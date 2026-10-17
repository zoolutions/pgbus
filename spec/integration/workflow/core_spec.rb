# frozen_string_literal: true

require_relative "support"

# Issue #540, step 11: the workflow engine end to end against a real database
# and real PGMQ queues, every StepJob through the real executor.
RSpec.describe "Workflow core (integration)", :integration do
  include WorkflowIntegration

  let(:log) { Queue.new }

  before do
    calls = log
    # `subject` below is Pgbus::Workflow#subject (the run's record), not RSpec's.
    # rubocop:disable-next RSpec/NamedSubject
    stub_const("SignupWorkflow", Class.new(Pgbus::Workflow) do
      cancel_if { subject.nil? || subject.closed? }

      step :welcome do
        calls << [:welcome, arguments[:plan]]
      end

      step :reminder, wait: 1, skip_if: -> { subject.onboarded? } do
        calls << :reminder
      end

      step :complete, wait: 1 do
        calls << :complete
      end
    end)
  end

  def logged
    entries = []
    entries << log.pop until log.empty?
    entries
  end

  it "runs the signup workflow end to end, waits included" do
    user = create_user
    wf = SignupWorkflow.start!(user, plan: "pro")

    expect(wf.status).to eq("ready")
    expect(wf.subject).to eq(user)
    run_until(wf, statuses: %w[finished])

    expect(logged).to eq([[:welcome, "pro"], :reminder, :complete])
    expect(steps_of(wf).map { |s| [s.step_name, s.status, s.attempt] })
      .to eq([["welcome", "completed", 1], ["reminder", "completed", 1], ["complete", "completed", 1]])
    reminder = steps_of(wf)[1]
    expect(reminder.started_at - reminder.created_at).to be >= 0.9
    expect(events_of(wf)).to start_with("started", "step_scheduled", "step_started", "step_completed")
    expect(events_of(wf).last).to eq("finished")
    expect(wf.record.finished_at).not_to be_nil
  end

  it "skips a step whose skip_if is true and cancels a run whose cancel_if is true" do
    onboarded = create_user(onboarded: true)
    wf = SignupWorkflow.start!(onboarded)
    run_until(wf, statuses: %w[finished])
    expect(steps_of(wf).map(&:status)).to eq(%w[completed skipped completed])

    closed = create_user(closed: true)
    cancelled = run_until(SignupWorkflow.start!(closed), statuses: %w[cancelled])
    expect(steps_of(cancelled).map(&:status)).to eq(%w[cancelled])
    expect(events_of(cancelled).last).to eq("cancelled")
  end

  describe "exceptions" do
    before do
      attempts = Concurrent::AtomicFixnum.new
      stub_const("FlakyWorkflow", Class.new(Pgbus::Workflow) do
        step :charge do
          raise ArgumentError, "gateway said no" if attempts.increment == 1
        end

        step(:receipt) { nil }
      end)
    end

    it "pauses with the error on the step row, reports it, and resume! runs attempt 2" do
      wf = run_until(FlakyWorkflow.start!)

      expect(wf.status).to eq("paused")
      failed = steps_of(wf).last
      expect(failed).to have_attributes(status: "failed", outcome: "paused", error_class: "ArgumentError",
                                        error_message: "gateway said no")
      expect(failed.error_backtrace).to include("core_spec.rb")
      expect(wf.record.error_class).to eq("ArgumentError")
      expect(Pgbus::ErrorReporter).to have_received(:report)
        .with(an_instance_of(ArgumentError), hash_including(workflow_class: "FlakyWorkflow", step_name: "charge", attempt: 1))

      wf.resume!(actor: "alice", reason: "gateway fixed")
      expect(wf.record.error_class).to be_nil
      run_until(wf, statuses: %w[finished])

      expect(steps_of(wf).map { |s| [s.step_name, s.attempt, s.status] })
        .to eq([["charge", 1, "failed"], ["charge", 2, "completed"], ["receipt", 1, "completed"]])
      resumed = wf.events.find { |e| e.event == "resumed" }
      expect(resumed).to have_attributes(actor: "alice", reason: "gateway fixed")
    end

    it "reattempts under a policy and applies terminal_action once max_reattempts is spent" do
      stub_const("TimeoutWorkflow", Class.new(Pgbus::Workflow) do
        on_exception Timeout::Error, action: :reattempt!, max_reattempts: 2, terminal_action: :cancel!

        step(:call_api) { raise Timeout::Error, "slow" }
      end)

      wf = run_until(TimeoutWorkflow.start!, statuses: %w[cancelled])

      expect(steps_of(wf).map { |s| [s.attempt, s.status, s.outcome] })
        .to eq([[1, "failed", "reattempted"], [2, "failed", "reattempted"], [3, "failed", "cancelled"]])
    end
  end

  it "resume! after a step paused itself runs the next step" do
    stub_const("ReviewWorkflow", Class.new(Pgbus::Workflow) do
      step(:review) { pause! }
      step(:publish) { output(published: true) }
    end)

    wf = run_until(ReviewWorkflow.start!)
    expect(wf.status).to eq("paused")
    expect(steps_of(wf).last).to have_attributes(status: "completed", outcome: "paused")

    wf.resume!
    run_until(wf, statuses: %w[finished])
    expect(steps_of(wf).map(&:step_name)).to eq(%w[review publish])
    expect(wf.output_of(:publish)).to eq("published" => true)
  end

  describe "enqueue after commit" do
    it "sends nothing while start! sits in an open application transaction" do
      user = create_user
      depth_inside = nil
      ActiveRecord::Base.transaction do
        SignupWorkflow.start!(user)
        depth_inside = queue_depth
      end

      expect(depth_inside).to eq(0)
      expect(queue_depth).to eq(1)
    end

    it "instruments an outside verb in an app transaction only once that transaction commits" do
      wf = SignupWorkflow.start!(create_user)
      seen = []
      subscription = ActiveSupport::Notifications.subscribe("pgbus.workflow_paused") { |*args| seen << args.last[:workflow_id] }

      ActiveRecord::Base.transaction do
        wf.pause!
        raise ActiveRecord::Rollback
      end
      expect(seen).to be_empty
      expect(wf.reload.status).to eq("ready")

      ActiveRecord::Base.transaction do
        wf.pause!
        expect(seen).to be_empty
      end
      expect(seen).to eq([wf.workflow_id])
    ensure
      ActiveSupport::Notifications.unsubscribe(subscription) if subscription
    end

    # Rails' transactional tests wrap each example in a non-joinable
    # transaction: the transition commits in a savepoint and its notes must go
    # out then, not wait for an outer commit that never comes.
    it "instruments and reports a transition under a non-joinable outer transaction" do
      stub_const("FailingWorkflow", Class.new(Pgbus::Workflow) { step(:boom) { raise ArgumentError, "boom" } })
      seen = []
      subscription = ActiveSupport::Notifications.subscribe("pgbus.workflow_paused") { |*args| seen << args.last[:error_class] }

      ActiveRecord::Base.transaction(joinable: false) do
        wf = FailingWorkflow.start!
        Pgbus::Workflow::StepJob.perform_now(steps_of(wf).first.id)

        expect(wf.reload.status).to eq("paused")
        expect(seen).to eq(["ArgumentError"])
        expect(Pgbus::ErrorReporter).to have_received(:report).with(an_instance_of(ArgumentError), hash_including(step_name: "boom"))
        raise ActiveRecord::Rollback
      end
    ensure
      ActiveSupport::Notifications.unsubscribe(subscription) if subscription
    end

    it "sends nothing and keeps nothing when that transaction rolls back" do
      user = create_user
      ActiveRecord::Base.transaction do
        SignupWorkflow.start!(user)
        raise ActiveRecord::Rollback
      end

      expect(queue_depth).to eq(0)
      expect(Pgbus::WorkflowEntry.count).to eq(0)
    end
  end

  describe "one ongoing run per subject" do
    it "raises AlreadyRunning naming the run that holds the slot, until that run finishes" do
      user = create_user
      first = SignupWorkflow.start!(user)

      expect { SignupWorkflow.start!(user) }.to raise_error(Pgbus::Workflow::AlreadyRunning) { |error|
        expect(error.workflow.workflow_id).to eq(first.workflow_id)
      }

      run_until(first, statuses: %w[finished])
      expect(SignupWorkflow.start!(user).workflow_id).not_to eq(first.workflow_id)
    end

    it "leaves the application transaction usable after AlreadyRunning" do
      user = create_user
      SignupWorkflow.start!(user)

      ActiveRecord::Base.transaction do
        expect { SignupWorkflow.start!(user) }.to raise_error(Pgbus::Workflow::AlreadyRunning)
        expect(WorkflowSpecUser.count).to eq(1)
      end
    end

    it "never limits subject-less runs" do
      stub_const("NightlyWorkflow", Class.new(Pgbus::Workflow) { step(:run) { nil } })

      expect { 2.times { NightlyWorkflow.start! } }.not_to raise_error
      expect(Pgbus::WorkflowEntry.where(workflow_class: "NightlyWorkflow").count).to eq(2)
    end
  end

  it "runs a step once when its StepJob is performed twice" do
    count = Concurrent::AtomicFixnum.new
    stub_const("OnceWorkflow", Class.new(Pgbus::Workflow) { step(:once) { count.increment } })
    wf = OnceWorkflow.start!
    step_id = steps_of(wf).first.id

    2.times { Pgbus::Workflow::StepJob.perform_now(step_id) }

    expect(count.value).to eq(1)
    expect(wf.reload.status).to eq("finished")
  end

  it "returns cleanly when the step row was deleted before its StepJob ran" do
    wf = SignupWorkflow.start!(create_user)
    step_id = steps_of(wf).first.id
    Pgbus::WorkflowEntry.where(workflow_id: wf.workflow_id).delete_all

    expect(Pgbus::WorkflowStep.where(id: step_id)).not_to exist
    expect { Pgbus::Workflow::StepJob.perform_now(step_id) }.not_to raise_error
  end
end
