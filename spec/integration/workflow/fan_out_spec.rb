# frozen_string_literal: true

require_relative "support"

# Issue #541, steps 2-3: fan_out steps against a real database: the batch
# opens through Pgbus.batch, members run through the real executor, and the
# batch's BatchFinishedJob resumes the run.
RSpec.describe "Workflow fan_out (integration)", :integration do
  include WorkflowIntegration

  let(:work_queue) { "fan_out_work" }
  let(:log) { Queue.new }
  let(:ran) { Queue.new }

  before do
    Pgbus.client.ensure_queue(work_queue)
    Pgbus.client.ensure_dead_letter_queue(work_queue)
    performed = ran
    queue = work_queue
    stub_const("FanOutMemberJob", Class.new(ActiveJob::Base) do
      self.queue_adapter = :pgbus
      queue_as queue
      def self.name = "FanOutMemberJob"

      define_method(:perform) do |id|
        raise ArgumentError, "vendor #{id} is down" if id == "down"

        performed << id
      end
    end)
    Pgbus::Workflow::BatchFinishedJob.queue_adapter = :pgbus

    calls = log
    stub_const("VendorWorkflow", Class.new(Pgbus::Workflow) do
      step(:authorize) { calls << :authorize }

      fan_out :notify_vendors do |b|
        b.add(*arguments[:vendors].map { |id| FanOutMemberJob.new(id) })
        output(vendors: arguments[:vendors].size)
      end

      step(:receipt) { calls << [:receipt, output_of(:notify_vendors)] }
    end)
  end

  def logged(queue = log)
    entries = []
    entries << queue.pop until queue.empty?
    entries
  end

  def drain_members
    while (message = Pgbus.client.read_message(work_queue, vt: 30))
      executor.execute(message, work_queue)
    end
  end

  # Runs the members on the work queue; one that raises is read again at
  # once until read_ct passes max_retries and it is dead-lettered.
  def run_members_dead_lettering_failures
    allow(Pgbus.configuration).to receive(:max_retries).and_return(1)
    while (message = Pgbus.client.read_message(work_queue, vt: 30))
      result = executor.execute(message, work_queue)
      Pgbus.client.set_visibility_timeout(work_queue, message.msg_id.to_i, vt: 0) if result == :failed
    end
  end

  def waiting_step(workflow) = steps_of(workflow).find { |step| step.step_name == "notify_vendors" }

  it "opens one batch for the members, parks the step on it and resumes the run once every member finished" do
    wf = run_until(VendorWorkflow.start!(vendors: %w[a b c]), statuses: %w[waiting])

    step = waiting_step(wf)
    expect(step).to have_attributes(status: "waiting", wait_for: nil)
    batch = Pgbus::BatchEntry.find_by(batch_id: step.batch_id)
    expect(batch).to have_attributes(description: "VendorWorkflow#notify_vendors #{wf.workflow_id}", total_jobs: 3)
    expect(JSON.parse(batch.properties)).to eq("workflow_id" => wf.workflow_id, "step_id" => step.id)
    expect(wf.events.find { |event| event.event == "fan_out_opened" }.details).to eq("batch_id" => step.batch_id, "size" => 3)

    drain_members
    expect(logged(ran)).to contain_exactly("a", "b", "c")
    callback = read_step_message
    payload = JSON.parse(callback.message)
    expect(payload["job_class"]).to eq("Pgbus::Workflow::BatchFinishedJob")
    expect(payload).not_to have_key(Pgbus::Batch::METADATA_KEY)
    executor.execute(callback, WorkflowIntegration::QUEUE)

    expect(waiting_step(wf).reload).to have_attributes(status: "completed", outcome: "completed")
    finished = wf.events.find { |event| event.event == "fan_out_finished" }
    expect(finished.details).to include("failed_jobs" => 0, "total_jobs" => 3)
    run_until(wf, statuses: %w[finished])
    expect(logged).to eq([:authorize, [:receipt, { "vendors" => 3 }]])
  end

  it "pauses with FanOutFailed when a member is dead-lettered, and resume! re-runs the whole fan_out on a fresh batch" do
    wf = run_until(VendorWorkflow.start!(vendors: %w[a down]), statuses: %w[waiting])
    first_batch = waiting_step(wf).batch_id

    run_members_dead_lettering_failures
    run_until(wf, statuses: %w[paused])
    expect(logged(ran)).to eq(%w[a])

    failed = steps_of(wf).find { |step| step.step_name == "notify_vendors" }
    expect(failed).to have_attributes(status: "failed", outcome: "paused", error_class: "Pgbus::Workflow::FanOutFailed")
    expect(failed.error_message).to start_with("1 of 2 jobs dead-lettered")
    expect(wf.record.error_class).to eq("Pgbus::Workflow::FanOutFailed")

    wf.resume!
    run_until(wf, statuses: %w[waiting])
    retried = steps_of(wf).select { |step| step.step_name == "notify_vendors" }.last
    expect(retried).to have_attributes(attempt: 2, status: "waiting")
    expect(retried.batch_id).not_to eq(first_batch)
    expect(Pgbus::BatchEntry.find_by(batch_id: first_batch).status).to eq("finished")
  end

  it "applies on_batch_failure: :skip! and moves on" do
    stub_const("SkippingVendorWorkflow", Class.new(Pgbus::Workflow) do
      fan_out(:notify, on_batch_failure: :skip!) { |b| b.add(FanOutMemberJob.new("down")) }
      step(:after) { nil }
    end)
    wf = run_until(SkippingVendorWorkflow.start!, statuses: %w[waiting])

    run_members_dead_lettering_failures
    run_until(wf, statuses: %w[finished])

    expect(steps_of(wf).map { |step| [step.step_name, step.status, step.outcome] })
      .to eq([%w[notify failed skipped], %w[after completed completed]])
  end

  it "resolves an empty fan_out at once, before its callback, which then finds nothing to resume" do
    wf = run_until(VendorWorkflow.start!(vendors: []), statuses: %w[finished])

    step = waiting_step(wf)
    expect(step).to have_attributes(status: "completed", outcome: "completed")
    expect(Pgbus::BatchEntry.find_by(batch_id: step.batch_id)).to have_attributes(status: "finished", total_jobs: 0)
    expect(logged).to eq([:authorize, [:receipt, { "vendors" => 0 }]])
    expect(events_of(wf).count("step_completed")).to eq(3)
  end

  it "creates no batch when the body skips, and none when it raises" do
    stub_const("ConditionalWorkflow", Class.new(Pgbus::Workflow) do
      fan_out :notify do |b|
        skip! if arguments[:skip]
        raise ArgumentError, "no vendors loaded" if arguments[:raise]

        b.add(FanOutMemberJob.new("a"))
      end
    end)

    skipped = run_until(ConditionalWorkflow.start!(skip: true), statuses: %w[finished])
    expect(steps_of(skipped).map(&:status)).to eq(%w[skipped])

    failed = run_until(ConditionalWorkflow.start!(raise: true), statuses: %w[paused])
    expect(steps_of(failed).last).to have_attributes(status: "failed", error_class: "ArgumentError")
    expect(Pgbus::BatchEntry.count).to eq(0)
  end

  it "rejects a callback set by the body: the workflow owns the batch's callbacks" do
    stub_const("CallbackWorkflow", Class.new(Pgbus::Workflow) do
      fan_out(:notify) { |b| b.on_success(FanOutMemberJob) }
    end)

    wf = run_until(CallbackWorkflow.start!, statuses: %w[paused])

    expect(steps_of(wf).last).to have_attributes(error_class: "ArgumentError")
    expect(steps_of(wf).last.error_message).to include("on_success: a fan_out step owns its batch's callbacks")
  end

  it "sends the StepJobs a callback or a batch member leads to outside any batch" do
    wf = run_until(VendorWorkflow.start!(vendors: %w[a]), statuses: %w[waiting])
    drain_members
    executor.execute(read_step_message, WorkflowIntegration::QUEUE)

    next_step = read_step_message
    expect(JSON.parse(next_step.message)).to include("job_class" => "Pgbus::Workflow::StepJob")
    expect(JSON.parse(next_step.message)).not_to have_key(Pgbus::Batch::METADATA_KEY)
    executor.execute(next_step, WorkflowIntegration::QUEUE)
    expect(wf.reload.status).to eq("finished")

    stub_const("SignalledWorkflow", Class.new(Pgbus::Workflow) { step(:wait, wait_for: :go) { nil } })
    parked = run_until(SignalledWorkflow.start!, statuses: %w[waiting])
    stub_const("SignallingMemberJob", Class.new(ActiveJob::Base) do
      self.queue_adapter = :pgbus
      def self.name = "SignallingMemberJob"
      def perform(workflow_id) = Pgbus::Workflow.find(workflow_id).signal!(:go)
    end)
    batch = Pgbus.batch { |b| b.add(SignallingMemberJob.new(parked.workflow_id).set(queue: work_queue)) }
    drain_members

    woken = read_step_message
    expect(JSON.parse(woken.message)).not_to have_key(Pgbus::Batch::METADATA_KEY)
    expect(Pgbus::BatchEntry.find_by(batch_id: batch.batch_id)).to have_attributes(total_jobs: 1, status: "finished")
  end

  it "has the sweep resolve a step whose batch finished but whose callback was lost" do
    wf = run_until(VendorWorkflow.start!(vendors: %w[a]), statuses: %w[waiting])
    drain_members
    lost = read_step_message
    Pgbus.client.delete_message(WorkflowIntegration::QUEUE, lost.msg_id.to_i)
    batch_id = waiting_step(wf).batch_id

    expect(Pgbus::Workflow::Sweep.repair).to include(fan_outs: 0)
    Pgbus::BatchEntry.where(batch_id: batch_id).update_all(finished_at: 2.hours.ago)
    expect(Pgbus::Workflow::Sweep.repair).to include(fan_outs: 1)

    expect(waiting_step(wf).reload.status).to eq("completed")
    run_until(wf, statuses: %w[finished])
  end

  it "pauses with FanOutFailed when the batch row is gone before the step resolved" do
    wf = run_until(VendorWorkflow.start!(vendors: %w[a]), statuses: %w[waiting])
    step = waiting_step(wf)
    Pgbus::BatchEntry.where(batch_id: step.batch_id).delete_all

    expect(Pgbus::Workflow::Sweep.repair).to include(fan_outs: 0)
    step.update_columns(waiting_since: 2.hours.ago)
    expect(Pgbus::Workflow::Sweep.repair).to include(fan_outs: 1)

    expect(wf.reload.status).to eq("paused")
    expect(waiting_step(wf).reload).to have_attributes(status: "failed", error_class: "Pgbus::Workflow::FanOutFailed")
    expect(waiting_step(wf).error_message).to include("no longer exists")
  end

  it "lets an operator skip a step waiting on its batch; the callback then leaves the run alone" do
    wf = run_until(VendorWorkflow.start!(vendors: %w[a]), statuses: %w[waiting])
    wf.skip!(actor: "ops")
    run_until(wf, statuses: %w[finished])

    drain_members
    drain_visible

    expect(waiting_step(wf)).to have_attributes(status: "skipped")
    expect(events_of(wf)).not_to include("fan_out_finished")
    expect(wf.reload.status).to eq("finished")
  end
end
