# frozen_string_literal: true

require_relative "support"

# Issue #540, step 11: the failure scenarios, each against a real database
# and real PGMQ deliveries.
RSpec.describe "Workflow failure scenarios (integration)", :integration do
  include WorkflowIntegration

  describe "a worker killed mid-step" do
    it "is noticed when its message comes back, well before workflow_stall_threshold" do
      kill = Concurrent::AtomicBoolean.new(true)
      stub_const("ChargeWorkflow", Class.new(Pgbus::Workflow) do
        step(:charge) { throw :worker_killed if kill.make_false }
        step(:receipt) { nil }
      end)
      wf = ChargeWorkflow.start!

      first = read_step_message(vt: 1)
      # A caught throw unwinds the executor without its rescue clauses: the
      # row stays in_progress and the message is never archived, as when
      # the process dies.
      catch(:worker_killed) { executor.execute(first, WorkflowIntegration::QUEUE) }
      claimed = steps_of(wf).first
      expect(claimed).to have_attributes(status: "in_progress", msg_id: first.msg_id.to_i)

      sleep 1.5
      again = read_step_message
      expect(again.msg_id.to_i).to eq(first.msg_id.to_i)
      expect(again.read_ct.to_i).to eq(2)
      executor.execute(again, WorkflowIntegration::QUEUE)

      expect(wf.reload.status).to eq("paused")
      stalled = steps_of(wf).first
      expect(stalled).to have_attributes(status: "failed", error_class: "Pgbus::Workflow::StepStalled")
      expect(stalled.stall_at).to be > Time.current
      detected = wf.events.find { |e| e.event == "stall_detected" }
      expect(detected).to have_attributes(actor: "system", details: hash_including("detection" => "redelivery"))
    end
  end

  describe "a worker declared dead that was alive" do
    it "finds its row no longer in_progress, records late_completion_ignored and advances nothing" do
      gate = Queue.new
      stub_const("SlowWorkflow", Class.new(Pgbus::Workflow) do
        step :charge do
          gate.pop
          output(charged: true)
        end
        step(:receipt) { nil }
      end)
      wf = SlowWorkflow.start!
      message = read_step_message
      worker = in_thread { executor.execute(message, WorkflowIntegration::QUEUE) }
      wait_for { steps_of(wf).first.status == "in_progress" }

      steps_of(wf).first.update_columns(stall_at: 1.minute.ago)
      Pgbus::Workflow::Sweep.repair
      expect(wf.reload.status).to eq("paused")

      gate << :go
      worker.join

      charge = steps_of(wf).first
      expect(charge).to have_attributes(status: "failed", error_class: "Pgbus::Workflow::StepStalled", output: nil)
      expect(steps_of(wf).map(&:step_name)).to eq(%w[charge])
      expect(events_of(wf)).to include("stall_detected", "late_completion_ignored")
      expect(wf.reload.status).to eq("paused")
    end
  end

  describe "a duplicate delivery from two threads" do
    it "runs the body once" do
      count = Concurrent::AtomicFixnum.new
      stub_const("OnceWorkflow", Class.new(Pgbus::Workflow) { step(:once) { count.increment } })
      wf = OnceWorkflow.start!
      step_id = steps_of(wf).first.id

      Array.new(2) { in_thread { Pgbus::Workflow::StepJob.perform_now(step_id) } }.each(&:join)

      expect(count.value).to eq(1)
      expect(wf.reload.status).to eq("finished")
    end
  end

  describe "resume! and cancel! racing" do
    # Both orders are legal under the States table: cancel first makes
    # resume! raise NotPaused; resume first leaves a ready run that cancel!
    # then cancels. What holds either way: the run ends cancelled, exactly
    # once, and no step runs after the cancel.
    it "ends cancelled exactly once and runs no step after the cancel" do
      ran = Concurrent::AtomicFixnum.new
      stub_const("RacedWorkflow", Class.new(Pgbus::Workflow) do
        step(:review) { ran.increment == 1 ? pause! : nil }
        step(:publish) { ran.increment }
      end)
      wf = run_until(RacedWorkflow.start!)
      expect(wf.status).to eq("paused")
      ran_before = ran.value

      results = %i[resume! cancel!].map do |verb|
        in_thread do
          RacedWorkflow.find(wf.workflow_id).public_send(verb)
          :ok
        rescue Pgbus::Workflow::InvalidTransition => e
          e
        end
      end.map(&:value)
      drain_visible

      expect(wf.reload.status).to eq("cancelled")
      expect(events_of(wf).count("cancelled")).to eq(1)
      expect(events_of(wf).count("resumed")).to be <= 1
      expect(results.last).to eq(:ok)
      expect(results.first).to eq(:ok).or be_a(Pgbus::Workflow::NotPaused)
      expect(ran.value).to eq(ran_before)
      expect(steps_of(wf).map(&:status)).not_to include("scheduled", "in_progress")
    end
  end

  describe "a step renamed on a live run" do
    before do
      stub_const("RenamedWorkflow", Class.new(Pgbus::Workflow) do
        step(:old_name) { nil }
        step(:last) { nil }
      end)
    end

    it "pauses with StepNotDefined when nothing covers the old name" do
      wf = RenamedWorkflow.start!
      stub_const("RenamedWorkflow", Class.new(Pgbus::Workflow) do
        step(:new_name) { nil }
        step(:last) { nil }
      end)

      run_until(wf)

      expect(wf.status).to eq("paused")
      expect(steps_of(wf).first).to have_attributes(status: "failed", error_class: "Pgbus::Workflow::StepNotDefined")
      expect(steps_of(wf).first.error_message).to include("removed_step :old_name")
    end

    it "refuses to resume a run paused after a step that is no longer declared, changing nothing" do
      stub_const("ReviewedWorkflow", Class.new(Pgbus::Workflow) do
        step(:review) { pause! }
        step(:publish) { nil }
      end)
      wf = run_until(ReviewedWorkflow.start!)
      expect(wf.status).to eq("paused")
      stub_const("ReviewedWorkflow", Class.new(Pgbus::Workflow) do
        step(:approve) { nil }
        step(:publish) { nil }
      end)

      expect { wf.resume! }.to raise_error(Pgbus::Workflow::StepNotDefined, /no step review/)
      expect(wf.reload.status).to eq("paused")
      expect(steps_of(wf).map(&:step_name)).to eq(%w[review])
      expect(events_of(wf)).not_to include("resumed")
    end

    it "skips the old step and advances when a removed_step gravestone covers it" do
      wf = RenamedWorkflow.start!
      stub_const("RenamedWorkflow", Class.new(Pgbus::Workflow) do
        removed_step :old_name
        step(:last) { nil }
      end)
      expect(RenamedWorkflow.removed_steps_in_flight).to eq("old_name" => 1)

      run_until(wf, statuses: %w[finished])

      expect(steps_of(wf).map { |s| [s.step_name, s.status] }).to eq([%w[old_name skipped], %w[last completed]])
      expect(RenamedWorkflow.removed_steps_in_flight).to eq("old_name" => 0)
    end
  end

  describe "retention deleting a run mid-step" do
    it "lets the StepJob return cleanly" do
      gate = Queue.new
      stub_const("DeletedWorkflow", Class.new(Pgbus::Workflow) { step(:slow) { gate.pop } })
      wf = DeletedWorkflow.start!
      message = read_step_message
      worker = in_thread { executor.execute(message, WorkflowIntegration::QUEUE) }
      wait_for { steps_of(wf).first.status == "in_progress" }

      Pgbus::WorkflowEntry.where(workflow_id: wf.workflow_id).delete_all
      gate << :go

      expect(worker.value).to eq(:success)
      expect(Pgbus::WorkflowStep.where(workflow_id: wf.workflow_id)).not_to exist
    end
  end

  describe "reschedule! to a later time" do
    it "makes the early message a no-op and lets the later one run the step" do
      count = Concurrent::AtomicFixnum.new
      stub_const("LaterWorkflow", Class.new(Pgbus::Workflow) { step(:later) { count.increment } })
      wf = LaterWorkflow.start!
      wf.reschedule!(at: 3.seconds.from_now, actor: "ops")

      early = read_step_message
      executor.execute(early, WorkflowIntegration::QUEUE)
      expect(count.value).to eq(0)
      expect(steps_of(wf).first.status).to eq("scheduled")

      run_until(wf, statuses: %w[finished], timeout: 8)
      expect(count.value).to eq(1)
      expect(wf.events.find { |e| e.event == "rescheduled" }.actor).to eq("ops")
    end
  end

  describe "idempotent start under concurrency" do
    it "starts one run and hands it to both callers" do
      stub_const("CheckoutWorkflow", Class.new(Pgbus::Workflow) { step(:charge) { nil } })
      user = create_user

      runs = Array.new(2) { in_thread { CheckoutWorkflow.start!(user, idempotency_key: "checkout-1") } }.map(&:value)

      expect(runs.map(&:workflow_id).uniq.size).to eq(1)
      expect(runs.map(&:duplicate_start?)).to contain_exactly(true, false)
      expect(Pgbus::WorkflowStep.where(workflow_id: runs.first.workflow_id).count).to eq(1)
    end
  end
end
