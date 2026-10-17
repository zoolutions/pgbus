# frozen_string_literal: true

require_relative "support"

# Issue #540, step 5 against the database: start!, the subject and the
# arguments, the outside verbs and their guards, idempotent start, the audit
# trail, and the idempotency keys a step hands to external APIs.
RSpec.describe "Workflow start! and outside verbs (integration)", :integration do
  include WorkflowIntegration

  let(:keys) { Queue.new }

  before do
    seen = keys
    stub_const("OrderWorkflow", Class.new(Pgbus::Workflow) do
      step :charge do
        seen << [step_idempotency_key, attempt_idempotency_key]
        reattempt! if seen.size == 1
      end

      step :receipt, wait: 60 do
        nil
      end
    end)
  end

  describe "start!" do
    it "inserts the run and its first step row, and returns the workflow" do
      user = create_user
      wf = OrderWorkflow.start!(user, plan: "pro", ship_to: user)

      expect(wf).to be_a(OrderWorkflow)
      expect(wf.record).to have_attributes(status: "ready", current_step: "charge", subject_type: "WorkflowSpecUser",
                                           subject_id: user.id.to_s, definition_digest: OrderWorkflow.definition_digest)
      expect(steps_of(wf).map { |s| [s.step_name, s.status, s.attempt] }).to eq([["charge", "scheduled", 1]])
      expect(wf.events.map { |e| [e.event, e.actor] }).to eq([%w[started app], %w[step_scheduled app]])
    end

    it "round-trips the arguments through ActiveJob::Arguments, a GlobalID argument included" do
      user = create_user
      wf = OrderWorkflow.find(OrderWorkflow.start!(nil, plan: "pro", ship_to: user, count: 3).workflow_id)

      expect(wf.arguments[:plan]).to eq("pro")
      expect(wf.arguments["count"]).to eq(3)
      expect(wf.arguments[:ship_to]).to eq(user)
    end

    it "raises PayloadTooLarge over workflow_max_payload_size and inserts nothing" do
      allow(Pgbus.configuration).to receive(:workflow_max_payload_size).and_return(64)

      expect { OrderWorkflow.start!(nil, blob: "x" * 100) }.to raise_error(Pgbus::Workflow::PayloadTooLarge, /arguments/)
      expect(Pgbus::WorkflowEntry.count).to eq(0)
    end

    it "returns a nil subject once the record is gone" do
      user = create_user
      wf = OrderWorkflow.start!(user)
      user.destroy!

      expect(OrderWorkflow.find(wf.workflow_id).subject).to be_nil
    end

    it "returns the same run for the same idempotency_key, with duplicate_start?, and one step row" do
      first = OrderWorkflow.start!(nil, idempotency_key: "checkout-1")
      second = OrderWorkflow.start!(nil, idempotency_key: "checkout-1")

      expect(second.workflow_id).to eq(first.workflow_id)
      expect([first.duplicate_start?, second.duplicate_start?]).to eq([false, true])
      expect(Pgbus::WorkflowStep.count).to eq(1)
    end

    it "returns a finished run for its key too" do
      wf = OrderWorkflow.start!(nil, idempotency_key: "checkout-2")
      wf.cancel!

      again = OrderWorkflow.start!(nil, idempotency_key: "checkout-2")
      expect(again.workflow_id).to eq(wf.workflow_id)
      expect(again.status).to eq("cancelled")
      expect(again).to be_duplicate_start
    end

    it "still raises AlreadyRunning for the same subject under a different key" do
      user = create_user
      holder = OrderWorkflow.start!(user, idempotency_key: "a")

      expect { OrderWorkflow.start!(user, idempotency_key: "b") }
        .to raise_error(Pgbus::Workflow::AlreadyRunning) { |e| expect(e.workflow.workflow_id).to eq(holder.workflow_id) }
    end
  end

  describe "pause!, resume!, resume_at!, cancel!" do
    it "pauses a ready run, leaving its scheduled step to be sent again by resume!" do
      wf = OrderWorkflow.start!
      wf.pause!(actor: "alice", reason: "hold on")
      drain_visible

      expect(wf.reload.status).to eq("paused")
      expect(steps_of(wf).first.status).to eq("scheduled")

      wf.resume!
      expect(queue_depth).to eq(1)
      wait_for(timeout: 10) do
        drain_visible
        steps_of(wf).any? { |s| s.step_name == "receipt" }
      end
      expect(wf.events.find { |e| e.event == "paused" }).to have_attributes(actor: "alice", reason: "hold on")
    end

    it "resume_at! runs the named step next" do
      wf = OrderWorkflow.start!
      wf.pause!
      wf.resume_at!(:receipt, actor: "ops")

      expect(wf.status).to eq("ready")
      expect(steps_of(wf).map { |s| [s.step_name, s.status] }).to eq([%w[charge cancelled], %w[receipt scheduled]])
      expect(steps_of(wf).last.scheduled_at).to be <= Time.current
      expect { wf.reload.resume_at!(:unknown) }.to raise_error(Pgbus::Workflow::NotPaused)
    end

    it "rejects resume_at! of a step the class does not define" do
      wf = OrderWorkflow.start!
      wf.pause!

      expect { wf.resume_at!(:shipping) }.to raise_error(Pgbus::Workflow::StepNotDefined, /no step shipping/)
    end

    it "raises NotPaused when resuming a run that is not paused, and InvalidTransition for an illegal cancel" do
      wf = OrderWorkflow.start!
      expect { wf.resume! }.to raise_error(Pgbus::Workflow::NotPaused)

      wf.cancel!(reason: "customer asked")
      expect(steps_of(wf).map(&:status)).to eq(%w[cancelled])
      expect { wf.cancel! }.to raise_error(Pgbus::Workflow::InvalidTransition, /cannot cancel a cancelled workflow/)
      expect { wf.resume! }.to raise_error(Pgbus::Workflow::NotPaused)
      expect(wf.events.last).to have_attributes(event: "cancelled", actor: "app", reason: "customer asked")
    end
  end

  describe "run_now!, reschedule!, rerun!" do
    it "run_now! sends the waiting step now, leaving its delayed message a no-op" do
      stub_const("WaitingWorkflow", Class.new(Pgbus::Workflow) { step(:later, wait: 3600) { output(ran: true) } })
      wf = WaitingWorkflow.start!
      expect(queue_depth).to eq(1)

      wf.run_now!(actor: "ops", reason: "customer is waiting")

      run_until(wf, statuses: %w[finished], timeout: 5)
      expect(wf.output_of(:later)).to eq("ran" => true)
      expect(wf.events.find { |e| e.event == "run_now" }).to have_attributes(actor: "ops", reason: "customer is waiting")
    end

    it "reschedule! moves the scheduled step, and both refuse a run with nothing scheduled" do
      wf = OrderWorkflow.start!
      at = 2.hours.from_now
      wf.reschedule!(at: at)
      expect(steps_of(wf).first.scheduled_at).to be_within(1).of(at)

      wf.pause!
      expect { wf.run_now! }.to raise_error(Pgbus::Workflow::InvalidTransition)
      expect { wf.reschedule!(at: at) }.to raise_error(Pgbus::Workflow::InvalidTransition)
    end

    it "rerun! starts a new run with the same subject and arguments" do
      user = create_user
      wf = OrderWorkflow.start!(user, plan: "pro")
      expect { wf.rerun! }.to raise_error(Pgbus::Workflow::InvalidTransition, /rerun/)
      wf.cancel!

      rerun = wf.rerun!(actor: "ops", reason: "try again")

      expect(rerun.workflow_id).not_to eq(wf.workflow_id)
      expect(rerun.record).to have_attributes(rerun_of_workflow_id: wf.workflow_id, idempotency_key: nil, status: "ready")
      expect(rerun.subject).to eq(user)
      expect(rerun.arguments[:plan]).to eq("pro")
      expect(wf.events.last).to have_attributes(event: "rerun", actor: "ops", reason: "try again")
    end
  end

  describe "idempotency keys" do
    it "keeps step_idempotency_key across attempts and changes attempt_idempotency_key" do
      wf = OrderWorkflow.start!
      wait_for(timeout: 10) do
        drain_visible
        keys.size == 2
      end

      (step1, attempt1), (step2, attempt2) = Array.new(2) { keys.pop }
      expect(step1).to eq("pgbus-wf:#{wf.workflow_id}:charge")
      expect(step2).to eq(step1)
      expect([attempt1, attempt2]).to eq(["#{step1}:1", "#{step1}:2"])
    end
  end

  describe "the audit trail and the definition digest" do
    it "keeps events read-only once written" do
      event = OrderWorkflow.start!.events.first

      expect { event.update!(reason: "rewritten") }.to raise_error(ActiveRecord::ReadOnlyRecord)
    end

    it "marks a run that started on an older step list" do
      wf = OrderWorkflow.start!
      expect(wf.record).not_to be_definition_outdated

      stub_const("OrderWorkflow", Class.new(Pgbus::Workflow) do
        step(:charge) { nil }
        step(:fraud_check) { nil }
        step(:receipt) { nil }
      end)

      expect(Pgbus::Workflow.find(wf.workflow_id).record).to be_definition_outdated
    end
  end

  describe "available_actions" do
    it "follows the States table" do
      wf = OrderWorkflow.start!
      expect(wf.available_actions).to match_array(%i[pause cancel run_now reschedule])

      wf.pause!
      expect(wf.available_actions).to match_array(%i[resume resume_at cancel])
    end
  end
end
