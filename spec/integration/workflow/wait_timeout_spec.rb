# frozen_string_literal: true

require_relative "support"

# Issue #541, step 4a: wait_timeout:/on_timeout: on a wait_for: step, applied
# by a real sweep against a real database.
RSpec.describe "Workflow wait timeouts (integration)", :integration do
  include WorkflowIntegration
  include Pgbus::Workflow::TestHelpers

  let(:log) { Queue.new }

  def logged
    entries = []
    entries << log.pop until log.empty?
    entries
  end

  def define(name, on_timeout:)
    calls = log
    stub_const(name, Class.new(Pgbus::Workflow) do
      step :await_payment, wait_for: :paid, wait_timeout: 3600, on_timeout: on_timeout do
        calls << [received_signal&.name, timed_out?]
      end

      step(:after) { calls << :after }
    end)
  end

  def park(klass)
    run_until(klass.start!, statuses: %w[waiting])
  end

  # The wait's deadline passed an hour ago.
  def expire!(workflow)
    steps_of(workflow).last.update_columns(timeout_at: 1.minute.ago, waiting_since: 61.minutes.ago)
  end

  def sweep = Pgbus::Workflow::Sweep.repair

  it "sets timeout_at to waiting_since + wait_timeout when the step parks" do
    wf = park(define("PauseOnTimeout", on_timeout: :pause!))

    step = steps_of(wf).last
    expect(step.timeout_at - step.waiting_since).to be_within(1).of(3600)
    expect(wf.events.last.details).to include("wait_for" => "paid", "timeout_at" => step.timeout_at.utc.iso8601(3))
  end

  it "pauses with WaitTimedOut by default, and leaves a signal that arrives after the timeout pending" do
    wf = park(define("PauseOnTimeout", on_timeout: :pause!))
    expire!(wf)

    expect(sweep).to include(timed_out: 1)
    expect(wf.reload.status).to eq("paused")
    expect(steps_of(wf).last).to have_attributes(status: "failed", outcome: "paused", error_class: "Pgbus::Workflow::WaitTimedOut")
    expect(steps_of(wf).last.error_message).to include("await_payment waited for paid")
    timed_out = wf.events.find { |event| event.event == "wait_timed_out" }
    expect([timed_out.actor, timed_out.details["action"]]).to eq(%w[system pause!])

    late = wf.signal!(:paid)
    expect(late.reload.status).to eq("pending")
  end

  it "cancels the run with on_timeout: :cancel!" do
    wf = park(define("CancelOnTimeout", on_timeout: :cancel!))
    expire!(wf)
    sweep

    expect(wf.reload.status).to eq("cancelled")
    expect(steps_of(wf).last.status).to eq("cancelled")
  end

  it "skips the step and moves on with on_timeout: :skip!" do
    wf = park(define("SkipOnTimeout", on_timeout: :skip!))
    expire!(wf)
    sweep
    run_until(wf, statuses: %w[finished])

    expect(steps_of(wf).map(&:status)).to eq(%w[skipped completed])
    expect(logged).to eq([:after])
  end

  it "runs the body timed out, with no signal, under on_timeout: :run" do
    wf = park(define("RunOnTimeout", on_timeout: :run))
    expire!(wf)
    sweep
    expect(wf.reload.status).to eq("ready")
    late = wf.signal!(:paid)

    run_until(wf, statuses: %w[finished])
    expect(logged).to eq([[nil, true], :after])
    expect(late.reload.status).to eq("pending")
  end

  describe "on_timeout: :notify (issue #565)" do
    # [[name, payload]] of the pgbus.workflow.step and wait_timed_out
    # notifications sent while the block runs.
    def notified(&)
      seen = []
      callback = ->(name, _start, _finish, _id, payload) { seen << [name, payload] }
      ActiveSupport::Notifications.subscribed(callback, /\Apgbus\.workflow\.(step|wait_timed_out)\z/, &)
      seen
    end

    it "reports the missed deadline, keeps the same attempt waiting and moves timeout_at one wait_timeout on" do
      wf = park(define("NotifyOnTimeout", on_timeout: :notify))
      expire!(wf)
      deadline = steps_of(wf).last.timeout_at

      seen = notified { expect(sweep).to include(timed_out: 1) }

      expect(wf.reload.status).to eq("waiting")
      expect(steps_of(wf).size).to eq(1)
      expect(steps_of(wf).last).to have_attributes(status: "waiting", attempt: 1, outcome: nil, wait_for: "paid")
      expect(steps_of(wf).last.timeout_at).to be_within(0.001).of(deadline + 3600)
      timed_out = wf.events.select { |event| event.event == "wait_timed_out" }
      expect(timed_out.map { |event| [event.actor, event.details["action"], event.attempt] }).to eq([["system", "notify", 1]])
      expect(seen.map(&:first)).to eq(["pgbus.workflow.wait_timed_out"])
      expect(seen.first.last).to include(workflow_class: "NotifyOnTimeout", step_name: "await_payment", attempt: 1, action: "notify")
      expect(logged).to be_empty
    end

    it "lets a later signal wake that same attempt, whose body runs with the signal and not timed out" do
      wf = park(define("NotifyThenSignal", on_timeout: :notify))
      expire!(wf)
      sweep

      wf.signal!(:paid)
      run_until(wf, statuses: %w[finished])

      expect(logged).to eq([["paid", false], :after])
      expect(steps_of(wf).map { |step| [step.step_name, step.attempt, step.status] })
        .to eq([["await_payment", 1, "completed"], ["after", 1, "completed"]])
    end

    it "notifies once per sweep however many deadlines it missed, and moves to the next one ahead of now" do
      wf = park(define("NotifyLate", on_timeout: :notify))
      steps_of(wf).last.update_columns(timeout_at: 150.minutes.ago, waiting_since: 210.minutes.ago)
      first = steps_of(wf).last.timeout_at

      expect(Pgbus::Workflow::WaitTimeout.expire_due(limit: 1, max_batches: 5)).to eq(1)

      # 150 minutes late on an hourly deadline: missed at -150, -90 and -30 minutes; next at +30.
      expect(steps_of(wf).last.timeout_at).to be_within(0.001).of(first + (3 * 3600))
      expect(events_of(wf).count("wait_timed_out")).to eq(1)
      expect(sweep).to include(timed_out: 0)
    end

    it "does not fire while the run is paused, and fires again once it is resumed and late" do
      wf = park(define("NotifyPaused", on_timeout: :notify))
      wf.pause!
      expire!(wf)
      expect(sweep).to include(timed_out: 0)
      expect(events_of(wf)).not_to include("wait_timed_out")

      wf.resume!
      expect(wf.status).to eq("waiting")
      expire!(wf)
      expect(sweep).to include(timed_out: 1)
      expect(wf.reload.status).to eq("waiting")
    end

    it "applies through expire_wait before the deadline too: timeout_at moves one wait_timeout on" do
      wf = park(define("NotifyInline", on_timeout: :notify))
      deadline = steps_of(wf).last.timeout_at

      expire_wait(wf)

      expect(wf.status).to eq("waiting")
      expect(steps_of(wf).last).to have_attributes(status: "waiting", attempt: 1)
      expect(steps_of(wf).last.timeout_at).to be_within(0.001).of(deadline + 3600)
      expect(events_of(wf).count("wait_timed_out")).to eq(1)
    end
  end

  it "pauses with WaitTimedOut when the waiting step was removed from the class while it waited" do
    wf = park(define("RemovedWhileWaiting", on_timeout: :notify))
    stub_const("RemovedWhileWaiting", Class.new(Pgbus::Workflow) do
      removed_step :await_payment
      step(:after) { nil }
    end)
    expire!(wf)

    expect(sweep).to include(timed_out: 1)
    expect(wf.reload.status).to eq("paused")
    expect(steps_of(wf).last).to have_attributes(status: "failed", outcome: "paused",
                                                 error_class: "Pgbus::Workflow::WaitTimedOut")
  end

  it "lets exactly one of a racing signal and timeout win" do
    wf = park(define("RacingTimeout", on_timeout: :run))
    expire!(wf)

    gate = Queue.new
    threads = [in_thread { gate.pop && wf.class.find(wf.workflow_id).signal!(:paid) },
               in_thread { gate.pop && sweep }]
    2.times { gate << true }
    threads.each(&:join)

    signal = Pgbus::WorkflowSignal.find_by!(workflow_id: wf.workflow_id)
    timed_out = events_of(wf).include?("wait_timed_out")
    expect([signal.status == "claimed", timed_out]).to contain_exactly(true, false)
    expect(steps_of(wf).last.status).to eq("scheduled")
    run_until(wf, statuses: %w[finished])
    expect(logged.first).to eq(timed_out ? [nil, true] : ["paid", false])
  end

  it "keeps timeout: for the run and wait_timeout: for the wait" do
    stub_const("BothTimeouts", Class.new(Pgbus::Workflow) do
      step(:await, wait_for: :paid, wait_timeout: 3600, timeout: 30) { nil }
    end)
    wf = park(BothTimeouts)

    parked = steps_of(wf).last
    expect(parked.stall_at).to be_nil
    expect(parked.timeout_at - parked.waiting_since).to be_within(1).of(3600)

    wf.signal!(:paid)
    read = read_step_message
    claimed_at = Time.current
    allow(Pgbus::Workflow::Signals).to receive(:gate).and_wrap_original do |original, *args, **kwargs|
      step = args[1]
      expect(step.stall_at).to be_within(2).of(claimed_at + 30)
      original.call(*args, **kwargs)
    end
    executor.execute(read, WorkflowIntegration::QUEUE)
    expect(wf.reload.status).to eq("finished")
  end

  it "does not time out a run that is paused, until it is resumed" do
    wf = park(define("PausedTimeout", on_timeout: :cancel!))
    wf.pause!
    expire!(wf)
    sweep
    expect(wf.reload.status).to eq("paused")

    wf.resume!
    expect(wf.status).to eq("waiting")
    sweep
    expect(wf.reload.status).to eq("cancelled")
  end

  it "does not count the time a run spent paused against its wait_timeout:" do
    wf = park(define("PausedLong", on_timeout: :cancel!))
    step = steps_of(wf).last
    deadline = step.timeout_at
    wf.pause!
    Pgbus::WorkflowEntry.where(workflow_id: wf.workflow_id).update_all(paused_at: 2.hours.ago)
    step.update_columns(waiting_since: 3.hours.ago)

    wf.resume!

    expect(step.reload.timeout_at).to be_within(5).of(deadline + 2.hours)
    expect(sweep).to include(timed_out: 0)
  end

  it "applies on_timeout inline through expire_wait" do
    wf = park(define("InlineTimeout", on_timeout: :run))

    expire_wait(wf)
    expect(wf.status).to eq("ready")
    perform_next_step(wf)

    expect(logged).to eq([[nil, true]])
  end
end
