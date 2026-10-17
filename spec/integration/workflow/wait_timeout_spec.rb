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
