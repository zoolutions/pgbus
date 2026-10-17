# frozen_string_literal: true

require_relative "support"

# Web::DataSource's workflow reads and actions (issue #542) against a real
# database: the list with its filters and latest-step join, the batch progress
# of a waiting fan-out step, the detail, the attention groups, and every
# action's real outcome, including the lost race.
RSpec.describe "Pgbus::Web::DataSource workflows", :integration do
  include WorkflowIntegration

  subject(:data_source) { Pgbus::Web::DataSource.new }

  let(:actor) { "dashboard:alice" }

  before do
    stub_const("GatewayDown", Class.new(StandardError))
    stub_const("CheckoutWorkflow", Class.new(Pgbus::Workflow) do
      step(:capture) { raise GatewayDown, "gateway down for #{arguments[:n]}" if arguments[:fail] }
      step(:receipt) { nil }
    end)
    stub_const("ShippingWorkflow", Class.new(Pgbus::Workflow) do
      step(:ship, wait_for: :shipped) { output(carrier: received_signal.payload[:carrier]) }
    end)
    stub_const("ReminderWorkflow", Class.new(Pgbus::Workflow) { step(:remind, wait: 2.days) { nil } })
  end

  def failed_checkout(n = 1) = run_until(CheckoutWorkflow.start!(n: n, fail: true))

  describe "#workflows" do
    it "lists ongoing runs newest first, with their current step, attempt and what they wait for" do
      paused = failed_checkout
      waiting = run_until(ShippingWorkflow.start!, statuses: %w[waiting])
      run_until(CheckoutWorkflow.start!(n: 2)) # finished: not ongoing
      reminder = ReminderWorkflow.start!

      rows = data_source.workflows(status: "ongoing")

      expect(rows.map { |r| r[:workflow_id] }).to eq([reminder.workflow_id, waiting.workflow_id, paused.workflow_id])
      expect(rows.last).to include(workflow_class: "CheckoutWorkflow", status: "paused", current_step: "capture",
                                   current_attempt: 1, step_status: "failed", error_class: "GatewayDown",
                                   error_message: "gateway down for 1")
      expect(rows[1]).to include(status: "waiting", step_status: "waiting", waiting_for: "shipped")
      expect(rows[1][:waiting_since]).to be_within(5).of(Time.current)
      expect(rows.first).to include(status: "ready", step_status: "scheduled")
      expect(rows.first[:scheduled_at]).to be_within(5).of(2.days.from_now)
    end

    it "filters by each state, and lists everything with all" do
      failed_checkout
      run_until(CheckoutWorkflow.start!(n: 2))

      expect(data_source.workflows(status: "paused").map { |r| r[:status] }).to eq(["paused"])
      expect(data_source.workflows(status: "finished").map { |r| r[:status] }).to eq(["finished"])
      expect(data_source.workflows(status: "all").size).to eq(2)
    end

    it "combines the filters with AND" do
      wanted = failed_checkout(1)
      failed_checkout(2).tap { |wf| Pgbus::WorkflowEntry.where(workflow_id: wf.workflow_id).update_all(created_at: 3.days.ago) }
      ReminderWorkflow.start!

      expect(data_source.workflows(status: "all", workflow_class: "CheckoutWorkflow", step: "capture",
                                   error_class: "GatewayDown", created_from: 1.day.ago)
                        .map { |r| r[:workflow_id] }).to eq([wanted.workflow_id])
      expect(data_source.workflows(status: "all", workflow_id: wanted.workflow_id).size).to eq(1)
      expect(data_source.workflows(status: "all", created_to: 1.day.ago).size).to eq(1)
    end

    it "filters by subject" do
      user = create_user
      stub_const("SignupWorkflow", Class.new(Pgbus::Workflow) { step(:welcome, wait: 1.day) { nil } })
      mine = SignupWorkflow.start!(user)
      SignupWorkflow.start!(create_user)

      rows = data_source.workflows(status: "all", subject_type: "WorkflowSpecUser", subject_id: user.id.to_s)

      expect(rows.map { |r| r[:workflow_id] }).to eq([mine.workflow_id])
      expect(rows.sole).to include(subject_type: "WorkflowSpecUser", subject_id: user.id.to_s)
    end

    it "pages, and counts with a bound" do
      3.times { ReminderWorkflow.start! }

      expect(data_source.workflows(status: "ongoing", page: 2, per_page: 2).size).to eq(1)
      expect(data_source.workflows_count(status: "ongoing")).to have_attributes(total: 3, capped?: false)
    end

    it "reads the batch progress of a waiting fan-out step in one query" do
      stub_const("NotifyJob", Class.new(ActiveJob::Base) { def perform(*) = nil })
      NotifyJob.queue_adapter = :pgbus
      stub_const("VendorWorkflow", Class.new(Pgbus::Workflow) do
        fan_out(:notify_vendors) { |b| b.add(NotifyJob.new(1), NotifyJob.new(2)) }
      end)
      wf = run_until(VendorWorkflow.start!, statuses: %w[waiting])
      batch_id = steps_of(wf).last.batch_id
      Pgbus::BatchEntry.where(batch_id: batch_id).update_all(completed_jobs: 1)

      queries = []
      subscription = ActiveSupport::Notifications.subscribe("sql.active_record") { |*, payload| queries << payload[:sql] }
      row = data_source.workflows(status: "waiting").sole
      ActiveSupport::Notifications.unsubscribe(subscription)

      expect(row).to include(batch_id: batch_id, batch_progress: { done: 1, total: 2 })
      expect(queries.count { |sql| sql.include?("pgbus_batches") }).to eq(1)
    end
  end

  describe "#workflow_detail" do
    it "returns the run with every attempt newest first, its events, signals and available actions" do
      wf = failed_checkout
      detail = data_source.workflow_detail(wf.workflow_id)

      expect(detail).to include(workflow_id: wf.workflow_id, status: "paused", definition_outdated: false,
                                available_actions: %i[resume resume_at cancel signal], resume_action: :retry,
                                resume_step: "capture", step_names: %w[capture receipt])
      expect(detail[:arguments]).to eq("n" => 1, "fail" => true)
      expect(detail[:steps].sole).to include(step_name: "capture", attempt: 1, status: "failed", error_class: "GatewayDown")
      expect(detail[:steps].sole[:error_backtrace]).to be_present
      expect(detail[:steps].sole[:durations]).to include(:waited, :queued, :ran)
      expect(detail[:events].first[:event]).to eq("paused")
      expect(detail[:events].map { |e| e[:event] }).to include("started", "step_failed")
      expect(detail[:timeline][:rows].sole[:label]).to eq("capture #1")
    end

    it "says a run paused after a completed step continues with the next one" do
      stub_const("HoldWorkflow", Class.new(Pgbus::Workflow) do
        step(:hold) { pause! }
        step(:after) { nil }
      end)
      wf = run_until(HoldWorkflow.start!)

      expect(data_source.workflow_detail(wf.workflow_id)).to include(resume_action: :continue, resume_step: "after")
    end

    it "offers the waiting step's signal name and lists the signals" do
      wf = run_until(ShippingWorkflow.start!, statuses: %w[waiting])
      wf.signal!(:early_bird, payload: { note: "kept" }, idempotency_key: "k1")

      detail = data_source.workflow_detail(wf.workflow_id)

      expect(detail[:signal_names]).to eq(["shipped"])
      expect(detail[:available_actions]).to include(:skip, :cancel, :signal)
      expect(detail[:signals].sole).to include(name: "early_bird", status: "pending", idempotency_key: "k1",
                                               payload: { "note" => "kept" })
    end

    it "links a rerun to its original and back" do
      original = failed_checkout.tap { |wf| wf.cancel!(actor: actor) }
      rerun = original.rerun!(actor: actor)

      expect(data_source.workflow_detail(rerun.workflow_id)[:rerun_of_workflow_id]).to eq(original.workflow_id)
      expect(data_source.workflow_detail(original.workflow_id)[:reruns].map { |r| r[:workflow_id] }).to eq([rerun.workflow_id])
    end

    it "is nil for an unknown run" do
      expect(data_source.workflow_detail("nope")).to be_nil
    end
  end

  describe "the home-page counts" do
    it "counts ongoing, paused and stuck runs" do
      failed_checkout
      ReminderWorkflow.start!

      expect(data_source.ongoing_workflows_count).to eq(2)
      expect(data_source.paused_workflows_count).to eq(1)
      expect(data_source.workflow_attention_count).to eq(1)
    end
  end

  describe "the attention view" do
    it "groups stuck runs with the status their actions follow" do
      failed_checkout(1)
      failed_checkout(2)

      group = data_source.workflow_attention_groups.sole

      expect(group).to include(workflow_class: "CheckoutWorkflow", step_name: "capture", reason: "paused_error",
                               error_class: "GatewayDown", count: 2, status: "paused",
                               available_actions: %i[resume resume_at cancel signal])
    end
  end

  describe "actions" do
    it "resumes a paused run and records the actor and reason" do
      wf = failed_checkout

      expect(data_source.resume_workflow(wf.workflow_id, actor: actor, reason: "gateway back")).to eq(ok: true)
      event = wf.reload.events.find { |e| e.event == "resumed" }
      expect([event.actor, event.reason]).to eq([actor, "gateway back"])
    end

    it "reports the lost race when the run moved on" do
      wf = failed_checkout
      wf.cancel!(actor: "someone")

      result = data_source.resume_workflow(wf.workflow_id, actor: actor)

      expect(result).to include(ok: false, code: :not_allowed, status: "cancelled")
      expect(result[:reason]).to include("cannot resume a cancelled workflow")
    end

    it "reports an unknown run" do
      expect(data_source.cancel_workflow("nope", actor: actor)).to include(ok: false, code: :not_found)
    end

    it "cancels, resumes at a chosen step, and skips a waiting step" do
      wf = failed_checkout
      expect(data_source.resume_workflow_at(wf.workflow_id, step: "receipt", actor: actor)).to eq(ok: true)
      expect(wf.reload.current_step).to eq("receipt")
      expect(data_source.cancel_workflow(wf.workflow_id, actor: actor)).to eq(ok: true)

      waiting = run_until(ShippingWorkflow.start!, statuses: %w[waiting])
      expect(data_source.skip_workflow_step(waiting.workflow_id, actor: actor)).to eq(ok: true)
      expect(steps_of(waiting).last.status).to eq("skipped")
    end

    it "rejects resume_at an unknown step" do
      wf = failed_checkout

      expect(data_source.resume_workflow_at(wf.workflow_id, step: "nope", actor: actor)).to include(ok: false, code: :invalid)
    end

    it "runs a scheduled step now" do
      wf = ReminderWorkflow.start!

      expect(data_source.run_workflow_step_now(wf.workflow_id, actor: actor, reason: "customer asked")).to eq(ok: true)
      expect(steps_of(wf).last.scheduled_at).to be_within(5).of(Time.current)
    end

    it "sends a signal once per idempotency key" do
      wf = run_until(ShippingWorkflow.start!, statuses: %w[waiting])
      args = { name: "shipped", payload: { "carrier" => "DHL" }, idempotency_key: "form-1", actor: actor }

      expect(data_source.signal_workflow(wf.workflow_id, **args)).to eq(ok: true, duplicate: false)
      expect(data_source.signal_workflow(wf.workflow_id, **args)).to eq(ok: true, duplicate: true)
      expect(Pgbus::WorkflowSignal.where(workflow_id: wf.workflow_id).count).to eq(1)
    end

    it "reruns a finished run and returns the new run's id" do
      wf = run_until(CheckoutWorkflow.start!(n: 3))

      result = data_source.rerun_workflow(wf.workflow_id, actor: actor)

      expect(result).to include(ok: true)
      expect(Pgbus::WorkflowEntry.find_by(workflow_id: result[:workflow_id]).rerun_of_workflow_id).to eq(wf.workflow_id)
    end

    it "bulk-resumes one group and says how many had already moved on" do
      runs = [failed_checkout(1), failed_checkout(2), failed_checkout(3)]
      group = { workflow_class: "CheckoutWorkflow", step_name: "capture", reason: "paused_error", error_class: "GatewayDown" }
      allow(Pgbus::WorkflowEntry).to receive(:attention_ids).and_wrap_original do |original, *args, **kwargs|
        original.call(*args, **kwargs).tap { runs.last.cancel!(actor: "someone") }
      end

      result = data_source.bulk_resume_workflows(group: group, actor: actor, reason: "fix deployed")

      expect(result).to eq(ok: true, done: 2, skipped: 1)
      expect(runs.first(2).map { |wf| wf.reload.status }).to all(eq("ready"))
    end

    it "bulk-cancels at most BULK_CAP runs per call" do
      stub_const("Pgbus::Web::DataSource::WorkflowActions::BULK_CAP", 1)
      failed_checkout(1)
      failed_checkout(2)
      group = { workflow_class: "CheckoutWorkflow", step_name: "capture", reason: "paused_error", error_class: "GatewayDown" }

      expect(data_source.bulk_cancel_workflows(group: group, actor: actor)).to eq(ok: true, done: 1, skipped: 0)
      expect(Pgbus::WorkflowEntry.where(status: "cancelled").count).to eq(1)
    end
  end
end
