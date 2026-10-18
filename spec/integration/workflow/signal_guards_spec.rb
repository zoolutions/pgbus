# frozen_string_literal: true

require_relative "support"

# Issue #572: inside a wait_for: step's guards, woken? (this attempt holds a
# signal) and signal_ready? (the gate would run it with one) tell a step a
# signal reached from one that has not waited yet, so a guard can skip a wait
# the record already passed without skipping a signalled step or stranding
# its signal as pending.
RSpec.describe "Workflow signal readers in guards (integration)", :integration do
  include WorkflowIntegration
  include Pgbus::Workflow::TestHelpers

  let(:log) { Queue.new }
  let(:woken_guard) { onboarding_workflow("WokenGuardWorkflow", :woken?) }
  let(:ready_guard) { onboarding_workflow("ReadyGuardWorkflow", :signal_ready?) }

  def logged
    entries = []
    entries << log.pop until log.empty?
    entries
  end

  def signals_of(workflow) = Pgbus::WorkflowSignal.where(workflow_id: workflow.workflow_id).order(:id).to_a

  def outcomes_of(workflow) = steps_of(workflow).map { |step| [step.step_name, step.attempt, step.outcome] }

  # The issue's OrderWorkflow, on the spec subject: skip the wait when the
  # user is already onboarded (the run started late, or a later signal came
  # first), with the guard reading +reader+.
  def onboarding_workflow(name, reader)
    stub_const(name, Class.new(Pgbus::Workflow) do
      step :await_onboarded, skip_if: -> { subject.onboarded? && !public_send(reader) }, wait_for: :onboarded do
        output(woken: woken?, signal_ready: signal_ready?, signal: received_signal.name)
      end

      step(:welcome) { nil }
    end)
  end

  describe "a step signal! woke after the record passed the state (the reproduction)" do
    { "woken?" => :woken_guard, "signal_ready?" => :ready_guard }.each do |reader, workflow|
      it "completes and runs its body when the guard reads #{reader}" do
        user = create_user
        wf = perform_next_step(public_send(workflow).start!(user))
        expect(wf.status).to eq("waiting")

        user.update!(onboarded: true)
        wf = deliver_signal(wf, :onboarded)
        perform_next_step(wf)

        expect(wf.reload.status).to eq("finished")
        expect(outcomes_of(wf)).to eq([["await_onboarded", 1, "completed"], ["welcome", 1, "completed"]])
        expect(wf.output_of(:await_onboarded)).to eq("woken" => true, "signal_ready" => true, "signal" => "onboarded")
        expect(signals_of(wf).map(&:status)).to eq(%w[consumed])
      end
    end
  end

  describe "a signal buffered before the step's first claim" do
    it "is taken when the guard reads signal_ready?: the step completes and no signal stays pending" do
      user = create_user
      wf = ready_guard.start!(user)
      user.update!(onboarded: true)
      signal = wf.signal!(:onboarded)
      expect(signal.status).to eq("pending")

      perform_next_step(wf)
      perform_next_step(wf)

      expect(wf.reload.status).to eq("finished")
      expect(outcomes_of(wf)).to eq([["await_onboarded", 1, "completed"], ["welcome", 1, "completed"]])
      expect(wf.output_of(:await_onboarded)).to eq("woken" => false, "signal_ready" => true, "signal" => "onboarded")
      expect(signals_of(wf).map(&:status)).to eq(%w[consumed])
      expect(Pgbus::WorkflowSignal.where(workflow_id: wf.workflow_id, status: "pending")).to be_empty
    end

    it "is not seen by woken?, which is false on a first claim: that guard skips and the signal stays pending" do
      user = create_user
      wf = woken_guard.start!(user)
      user.update!(onboarded: true)
      wf.signal!(:onboarded)

      perform_next_step(wf)

      expect(outcomes_of(wf).first).to eq(["await_onboarded", 1, "skipped"])
      expect(signals_of(wf).map(&:status)).to eq(%w[pending])
    end
  end

  describe "a run that starts after the record passed the state" do
    { "woken?" => :woken_guard, "signal_ready?" => :ready_guard }.each do |reader, workflow|
      it "skips the step without waiting when the guard reads #{reader}" do
        wf = perform_next_step(public_send(workflow).start!(create_user(onboarded: true)))
        perform_next_step(wf)

        expect(wf.reload.status).to eq("finished")
        expect(outcomes_of(wf)).to eq([["await_onboarded", 1, "skipped"], ["welcome", 1, "completed"]])
        expect(events_of(wf)).not_to include("step_waiting")
        expect(signals_of(wf)).to be_empty
      end
    end
  end

  describe "what the guard and the body read" do
    # Each guard call logs [:guard, attempt, woken?, signal_ready?]; the
    # body logs [:body, attempt, woken?, signal_ready?, received_signal name, timed_out?].
    def observing_workflow(name, **options, &extra)
      calls = log
      stub_const(name, Class.new(Pgbus::Workflow) do
        skip_if = lambda do
          calls << [:guard, Pgbus::Workflow::StepContext.attempt, woken?, signal_ready?]
          false
        end
        step :await, skip_if: skip_if, wait_for: :ready, **options do
          calls << [:body, Pgbus::Workflow::StepContext.attempt, woken?, signal_ready?, received_signal&.name, timed_out?]
          instance_exec(&extra) if extra
        end
      end)
    end

    it "is false on a first claim with no signal, and true on the claim after signal! woke the step" do
      wf = perform_next_step(observing_workflow("ObservedWake").start!)
      expect(wf.status).to eq("waiting")
      deliver_signal(wf, :ready)

      expect(logged).to eq([[:guard, 1, false, false], [:guard, 1, true, true], [:body, 1, true, true, "ready", false]])
    end

    it "stays true on the attempt after a reattempt of a woken attempt" do
      attempts = Concurrent::AtomicFixnum.new
      wf = perform_next_step(observing_workflow("ObservedReattempt") { reattempt! if attempts.increment == 1 }.start!)
      wf = deliver_signal(wf, :ready)
      perform_next_step(wf)

      expect(wf.reload.status).to eq("finished")
      expect(logged).to eq([[:guard, 1, false, false], [:guard, 1, true, true], [:body, 1, true, true, "ready", false],
                            [:guard, 2, true, true], [:body, 2, true, true, "ready", false]])
      expect(outcomes_of(wf)).to eq([["await", 1, "reattempted"], ["await", 2, "completed"]])
    end

    it "stays true on the attempt resume! runs after a woken attempt failed into a pause" do
      attempts = Concurrent::AtomicFixnum.new
      jam = -> { raise ArgumentError, "label printer jammed" if attempts.increment == 1 }
      wf = perform_next_step(observing_workflow("ObservedResume", &jam).start!)
      wf = deliver_signal(wf, :ready)
      expect(wf.status).to eq("paused")

      wf.resume!
      perform_next_step(wf)

      expect(wf.reload.status).to eq("finished")
      expect(logged).to eq([[:guard, 1, false, false], [:guard, 1, true, true], [:body, 1, true, true, "ready", false],
                            [:guard, 2, true, true], [:body, 2, true, true, "ready", false]])
      expect(outcomes_of(wf)).to eq([["await", 1, "paused"], ["await", 2, "completed"]])
    end

    it "reads woken? false and signal_ready? true when the gate takes a signal buffered before the claim" do
      wf = observing_workflow("ObservedBuffered").start!
      wf.signal!(:ready)
      perform_next_step(wf)

      expect(logged).to eq([[:guard, 1, false, true], [:body, 1, false, true, "ready", false]])
    end

    it "is false for both after an on_timeout: :run wake, even with a late signal pending the gate does not take" do
      wf = perform_next_step(observing_workflow("ObservedTimeout", wait_timeout: 3600, on_timeout: :run).start!)
      expire_wait(wf)
      late = wf.signal!(:ready)
      expect(late.status).to eq("pending")
      perform_next_step(wf)

      expect(logged).to eq([[:guard, 1, false, false], [:guard, 1, false, false], [:body, 1, false, false, nil, true]])
      expect(late.reload.status).to eq("pending")
    end
  end

  describe "a guard that ends the attempt" do
    it "leaves a pending signal pending, for a later wait_for: step that takes it" do
      calls = log
      stub_const("TwoApprovalsWorkflow", Class.new(Pgbus::Workflow) do
        step(:first_approval, skip_if: -> { true }, wait_for: :approved) { calls << :first }
        step(:second_approval, wait_for: :approved) { calls << [:second, received_signal.payload["by"]] }
      end)
      wf = TwoApprovalsWorkflow.start!
      signal = wf.signal!(:approved, payload: { by: "ops" })

      perform_next_step(wf)
      expect(signal.reload.status).to eq("pending")
      perform_next_step(wf)

      expect(wf.reload.status).to eq("finished")
      expect(logged).to eq([[:second, "ops"]])
      expect(signal.reload.status).to eq("consumed")
    end

    it "consumes the signal the step holds, on the attempt after a reattempt too" do
      attempts = Concurrent::AtomicFixnum.new
      stub_const("HeldSkipWorkflow", Class.new(Pgbus::Workflow) do
        step :await, skip_if: -> { attempts.value >= 1 }, wait_for: :ready do
          attempts.increment
          reattempt!
        end
      end)
      wf = perform_next_step(HeldSkipWorkflow.start!)
      wf = deliver_signal(wf, :ready)
      expect(signals_of(wf).map(&:status)).to eq(%w[claimed])

      perform_next_step(wf)

      expect(wf.reload.status).to eq("finished")
      expect(outcomes_of(wf)).to eq([["await", 1, "reattempted"], ["await", 2, "skipped"]])
      expect(signals_of(wf).map(&:status)).to eq(%w[consumed])
    end

    it "consumes the signal the step holds when cancel_if cancels the attempt after a reattempt" do
      attempts = Concurrent::AtomicFixnum.new
      stub_const("HeldCancelWorkflow", Class.new(Pgbus::Workflow) do
        cancel_if { attempts.value >= 1 }

        step :await, wait_for: :ready do
          attempts.increment
          reattempt!
        end
      end)
      wf = perform_next_step(HeldCancelWorkflow.start!)
      wf = deliver_signal(wf, :ready)

      perform_next_step(wf)

      expect(wf.reload.status).to eq("cancelled")
      expect(outcomes_of(wf)).to eq([["await", 1, "reattempted"], ["await", 2, "cancelled"]])
      expect(signals_of(wf).map(&:status)).to eq(%w[consumed])
    end

    it "consumes the signal the step holds when a removed_step skips the attempt after a reattempt" do
      stub_const("RetiredWaitWorkflow", Class.new(Pgbus::Workflow) do
        step(:await, wait_for: :ready) { reattempt! }
        step(:after) { nil }
      end)
      wf = perform_next_step(RetiredWaitWorkflow.start!)
      wf = deliver_signal(wf, :ready)
      stub_const("RetiredWaitWorkflow", Class.new(Pgbus::Workflow) do
        removed_step :await
        step(:after) { nil }
      end)

      perform_next_step(wf)
      perform_next_step(wf)

      expect(wf.reload.status).to eq("finished")
      expect(outcomes_of(wf)).to eq([["await", 1, "reattempted"], ["await", 2, "skipped"], ["after", 1, "completed"]])
      expect(signals_of(wf).map(&:status)).to eq(%w[consumed])
    end

    it "consumes the signal the step holds when a raising guard's policy skips the attempt after a reattempt" do
      attempts = Concurrent::AtomicFixnum.new
      stub_const("RaisingGuardWorkflow", Class.new(Pgbus::Workflow) do
        guard = -> { attempts.value >= 1 ? raise(ArgumentError, "lookup failed") : false }
        step :await, skip_if: guard, wait_for: :ready, on_exception: :skip! do
          attempts.increment
          reattempt!
        end
      end)
      wf = perform_next_step(RaisingGuardWorkflow.start!)
      wf = deliver_signal(wf, :ready)

      perform_next_step(wf)

      expect(wf.reload.status).to eq("finished")
      expect(steps_of(wf).last).to have_attributes(attempt: 2, outcome: "skipped", error_class: "ArgumentError")
      expect(signals_of(wf).map(&:status)).to eq(%w[consumed])
    end
  end
end
