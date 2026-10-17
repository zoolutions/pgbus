# frozen_string_literal: true

require_relative "support"

# Issue #541, step 8: the epic's (#539) OrderWorkflow end to end on a real
# database: a fan-out whose members run through the real executor, a wait,
# a signal addressed by subject (before and after the step parks, and
# redelivered), a dead-lettered member, and a wait that times out through a
# real sweep.
RSpec.describe "Workflow example: OrderWorkflow (integration)", :integration do
  include WorkflowIntegration

  let(:work_queue) { "order_vendor_work" }
  let(:gateway) { Queue.new }
  let(:notified) { Queue.new }

  before do
    Pgbus.client.ensure_queue(work_queue)
    Pgbus.client.ensure_dead_letter_queue(work_queue)
    Pgbus::Workflow::BatchFinishedJob.queue_adapter = :pgbus

    calls = gateway
    stub_const("PaymentGateway", Module.new)
    stub_const("PaymentGateway::Transient", Class.new(StandardError))
    stub_const("PaymentGateway::FraudSuspected", Class.new(StandardError))
    PaymentGateway.define_singleton_method(:authorize) { |order| calls << [:authorize, order.id] }
    PaymentGateway.define_singleton_method(:capture) { |order| calls << [:capture, order.id] }
    PaymentGateway.define_singleton_method(:receipt) { |order| calls << [:receipt, order.id] }

    seen = notified
    queue = work_queue
    stub_const("NotifyVendorJob", Class.new(ActiveJob::Base) do
      self.queue_adapter = :pgbus
      queue_as queue
      def self.name = "NotifyVendorJob"

      define_method(:perform) do |vendor_id|
        raise ArgumentError, "vendor #{vendor_id} rejected the order" if vendor_id == "offline"

        seen << vendor_id
      end
    end)

    # `subject` below is Pgbus::Workflow#subject (the order), not RSpec's.
    # rubocop:disable-next RSpec/NamedSubject
    stub_const("OrderWorkflow", Class.new(Pgbus::Workflow) do
      cancel_if { subject.nil? || subject.closed? }
      on_exception PaymentGateway::Transient, action: :reattempt!, wait: 30.seconds, max_reattempts: 5

      step :authorize do
        PaymentGateway.authorize(subject)
      end

      fan_out :notify_vendors do |b|
        b.add(*arguments[:vendors].map { |vendor_id| NotifyVendorJob.new(vendor_id) })
      end

      step :capture, wait: 1.hour do
        PaymentGateway.capture(subject)
      rescue PaymentGateway::FraudSuspected
        pause!
      end

      step :await_shipment, wait_for: :shipped, wait_timeout: 2.days do
        subject.update!(name: "shipped at #{received_signal.payload["at"]}")
      end

      step :receipt do
        PaymentGateway.receipt(subject)
      end
    end)
  end

  def drained(queue)
    entries = []
    entries << queue.pop until queue.empty?
    entries
  end

  def run_members
    allow(Pgbus.configuration).to receive(:max_retries).and_return(1)
    while (message = Pgbus.client.read_message(work_queue, vt: 30))
      result = executor.execute(message, work_queue)
      Pgbus.client.set_visibility_timeout(work_queue, message.msg_id.to_i, vt: 0) if result == :failed
    end
  end

  # authorize, then the fan-out parks on its batch.
  def start_and_fan_out(order, vendors: %w[v1 v2 v3])
    run_until(OrderWorkflow.start!(order, vendors: vendors), statuses: %w[waiting])
  end

  # The members run, then the batch's BatchFinishedJob resumes the run, and
  # capture (an hour out) is pulled forward by an operator.
  def finish_fan_out(workflow)
    run_members
    callback = read_step_message
    expect(JSON.parse(callback.message)).to include("job_class" => "Pgbus::Workflow::BatchFinishedJob")
    expect(JSON.parse(callback.message)).not_to have_key(Pgbus::Batch::METADATA_KEY)
    executor.execute(callback, WorkflowIntegration::QUEUE)
    workflow.reload.run_now!(actor: "ops", reason: "captured early in the spec")
  end

  it "runs end to end, the shipment signal addressed by subject after the step parks, and a redelivery ignored" do
    order = create_user(name: "order")
    wf = start_and_fan_out(order)
    expect(steps_of(wf).last).to have_attributes(step_name: "notify_vendors", status: "waiting")

    finish_fan_out(wf)
    run_until(wf, statuses: %w[waiting])
    expect(wf.current_step).to eq("await_shipment")
    expect(drained(notified)).to contain_exactly("v1", "v2", "v3")

    first = OrderWorkflow.signal!(order, :shipped, payload: { at: "2026-10-17" }, idempotency_key: "evt-1", actor: "webhook")
    again = OrderWorkflow.signal!(order, :shipped, payload: { at: "2026-10-18" }, idempotency_key: "evt-1", actor: "webhook")
    expect(again).to have_attributes(id: first.id, duplicate_delivery?: true)
    run_until(wf, statuses: %w[finished])

    expect(drained(gateway)).to eq([[:authorize, order.id], [:capture, order.id], [:receipt, order.id]])
    expect(order.reload.name).to eq("shipped at 2026-10-17")
    expect(steps_of(wf).map { |step| [step.step_name, step.status] })
      .to eq([%w[authorize completed], %w[notify_vendors completed], %w[capture completed],
              %w[await_shipment completed], %w[receipt completed]])
    expect(Pgbus::WorkflowSignal.where(workflow_id: wf.workflow_id).pluck(:status)).to eq(%w[consumed])
    expect(events_of(wf)).to include("fan_out_opened", "fan_out_finished", "step_waiting", "signal_received", "signal_claimed")
  end

  it "takes a shipment signal that arrived before the step parked" do
    order = create_user(name: "order")
    wf = start_and_fan_out(order)
    OrderWorkflow.signal!(order, :shipped, payload: { at: "early" })

    finish_fan_out(wf)
    run_until(wf, statuses: %w[finished])

    expect(order.reload.name).to eq("shipped at early")
    expect(events_of(wf)).not_to include("wait_timed_out")
    expect(steps_of(wf).find { |step| step.step_name == "await_shipment" }.waiting_since).to be_nil
  end

  it "pauses with FanOutFailed when a vendor notification is dead-lettered" do
    wf = start_and_fan_out(create_user(name: "order"), vendors: %w[v1 offline v3])

    run_members
    run_until(wf, statuses: %w[paused])

    expect(steps_of(wf).last).to have_attributes(step_name: "notify_vendors", status: "failed",
                                                 error_class: "Pgbus::Workflow::FanOutFailed")
    expect(wf.record.error_message).to start_with("1 of 3 jobs dead-lettered")
    expect(drained(notified)).to contain_exactly("v1", "v3")
  end

  it "pauses with WaitTimedOut when the shipment never comes, through a real sweep" do
    order = create_user(name: "order")
    wf = start_and_fan_out(order)
    finish_fan_out(wf)
    run_until(wf, statuses: %w[waiting])
    steps_of(wf).last.update_columns(timeout_at: 1.minute.ago)

    expect(Pgbus::Workflow::Sweep.repair).to include(timed_out: 1)

    expect(wf.reload.status).to eq("paused")
    expect(steps_of(wf).last).to have_attributes(step_name: "await_shipment", error_class: "Pgbus::Workflow::WaitTimedOut")
    late = OrderWorkflow.signal!(order, :shipped, payload: { at: "too late" })
    expect(late.reload.status).to eq("pending")
  end

  it "cancels before the next step once the order is closed" do
    order = create_user(name: "order")
    wf = start_and_fan_out(order)
    order.update!(closed: true)

    finish_fan_out(wf)
    run_until(wf, statuses: %w[cancelled])

    expect(steps_of(wf).last).to have_attributes(step_name: "capture", status: "cancelled")
  end
end
