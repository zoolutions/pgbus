# frozen_string_literal: true

require_relative "support"

# Issue #541, step 4: wait_for: steps and signal! against a real database,
# every StepJob through the real executor.
RSpec.describe "Workflow signals (integration)", :integration do
  include WorkflowIntegration

  let(:log) { Queue.new }

  before do
    calls = log
    stub_const("ShipmentWorkflow", Class.new(Pgbus::Workflow) do
      step(:pack) { calls << :pack }

      step :await_shipment, wait_for: :shipped do
        calls << [:shipped, received_signal.payload["at"], received_signal.payload[:carrier], timed_out?]
      end

      step(:receipt) { calls << :receipt }
    end)
  end

  def logged
    entries = []
    entries << log.pop until log.empty?
    entries
  end

  def signals_of(workflow) = Pgbus::WorkflowSignal.where(workflow_id: workflow.workflow_id).order(:id).to_a

  # Runs StepJob messages until the run parks (or settles).
  def run_until_waiting(workflow) = run_until(workflow, statuses: %w[waiting finished cancelled paused])

  describe "the gate" do
    it "parks the step when no signal matches, sending nothing until one arrives" do
      wf = run_until_waiting(ShipmentWorkflow.start!)

      expect(wf.status).to eq("waiting")
      waiting = steps_of(wf).last
      expect(waiting).to have_attributes(step_name: "await_shipment", status: "waiting", wait_for: "shipped",
                                         timeout_at: nil, stall_at: nil)
      expect(waiting.waiting_since).not_to be_nil
      expect(queue_depth).to eq(0)
      expect(events_of(wf).last).to eq("step_waiting")
      expect(wf.events.last.details).to eq("wait_for" => "shipped")
    end

    it "claims the oldest pending signal that matches when the step reaches its gate" do
      wf = ShipmentWorkflow.start!
      wf.signal!(:refunded)
      first = wf.signal!(:shipped, payload: { at: "monday", carrier: "dhl" })
      wf.signal!(:shipped, payload: { at: "tuesday" })
      run_until(wf, statuses: %w[finished])

      expect(logged).to eq([:pack, [:shipped, "monday", "dhl", false], :receipt])
      expect(signals_of(wf).map { |signal| [signal.name, signal.status] })
        .to eq([%w[refunded pending], %w[shipped consumed], %w[shipped pending]])
      expect(steps_of(wf)[1].signal_id).to eq(first.id)
      expect(signals_of(wf)[1].claimed_by_step_id).to eq(steps_of(wf)[1].id)
    end
  end

  describe "signal!" do
    it "buffers a signal while no step waits for it" do
      wf = ShipmentWorkflow.start!
      signal = wf.signal!(:shipped, payload: { at: "early" })

      expect(signal).to have_attributes(name: "shipped", status: "pending", duplicate_delivery?: false)
      expect(signal.payload).to eq("at" => "early")
      expect(wf.status).to eq("ready")
      expect(events_of(wf)).to include("signal_received")
    end

    it "wakes a parked step: the step is scheduled, the run ready, and exactly one StepJob is sent" do
      wf = run_until_waiting(ShipmentWorkflow.start!)
      expect(queue_depth).to eq(0)

      signal = wf.signal!(:shipped, payload: { at: "late" }, actor: "webhook")

      expect(wf.status).to eq("ready")
      expect(signal.reload).to have_attributes(status: "claimed", claimed_by_step_id: steps_of(wf).last.id)
      expect(steps_of(wf).last).to have_attributes(status: "scheduled", signal_id: signal.id, attempt: 1)
      expect(queue_depth).to eq(1)
      received, claimed = wf.events.last(2)
      expect([received.event, received.actor, received.details["name"]]).to eq(%w[signal_received webhook shipped])
      expect([claimed.event, claimed.actor, claimed.step_name]).to eq(%w[signal_claimed webhook await_shipment])

      run_until(wf, statuses: %w[finished])
      expect(logged).to eq([:pack, [:shipped, "late", nil, false], :receipt])
      expect(signal.reload.status).to eq("consumed")
    end

    it "sends nothing until the application transaction around signal! commits" do
      wf = run_until_waiting(ShipmentWorkflow.start!)

      ActiveRecord::Base.transaction do
        wf.signal!(:shipped)
        expect(queue_depth).to eq(0)
      end
      expect(queue_depth).to eq(1)
    end

    it "treats a redelivery with the same idempotency key as a no-op that answers duplicate_delivery?" do
      wf = run_until_waiting(ShipmentWorkflow.start!)
      first = wf.signal!(:shipped, payload: { at: "once" }, idempotency_key: "evt-1")
      again = wf.signal!(:shipped, payload: { at: "twice" }, idempotency_key: "evt-1")

      expect(again).to have_attributes(id: first.id, duplicate_delivery?: true)
      expect(signals_of(wf).size).to eq(1)
      expect(queue_depth).to eq(1)
      duplicate = wf.events.reverse.find { |event| event.event == "signal_received" }
      expect(duplicate.details).to include("duplicate" => true, "idempotency_key" => "evt-1")
    end

    it "raises NotOngoing on a finished or cancelled run" do
      wf = ShipmentWorkflow.start!
      wf.cancel!

      expect { wf.signal!(:shipped) }.to raise_error(Pgbus::Workflow::NotOngoing, /cannot signal a cancelled workflow/)
      expect(signals_of(wf)).to be_empty
    end

    it "caps the payload at workflow_max_payload_size" do
      wf = ShipmentWorkflow.start!
      allow(Pgbus.configuration).to receive(:workflow_max_payload_size).and_return(64)

      expect { wf.signal!(:shipped, payload: { blob: "x" * 100 }) }
        .to raise_error(Pgbus::Workflow::PayloadTooLarge, /payload of signal shipped/)
      expect(signals_of(wf)).to be_empty
    end

    it "keeps the run paused when it wakes a step a pause! reached first; resume! then runs it" do
      wf = run_until_waiting(ShipmentWorkflow.start!)
      wf.pause!(actor: "ops")

      wf.signal!(:shipped, payload: { at: "while paused" })
      expect(wf.status).to eq("paused")
      expect(steps_of(wf).last.status).to eq("scheduled")
      drain_visible
      expect(steps_of(wf).last.status).to eq("scheduled")

      wf.resume!
      run_until(wf, statuses: %w[finished])
      expect(logged).to eq([:pack, [:shipped, "while paused", nil, false], :receipt])
    end

    it "sends a run paused while waiting back to waiting on resume!" do
      wf = run_until_waiting(ShipmentWorkflow.start!)
      wf.pause!
      wf.resume!

      expect(wf.status).to eq("waiting")
      expect(steps_of(wf).last.status).to eq("waiting")
      expect(queue_depth).to eq(0)
    end
  end

  describe "matchers" do
    before do
      calls = log
      stub_const("EuShipmentWorkflow", Class.new(Pgbus::Workflow) do
        step :await_eu, wait_for: Pgbus::Workflow::SignalMatcher.new(:shipped) { |payload| payload[:region] == "eu" } do
          calls << received_signal.payload["region"]
        end
      end)
    end

    it "wakes only for a signal the matcher's block takes, seeing the payload with indifferent access" do
      wf = run_until_waiting(EuShipmentWorkflow.start!)

      wf.signal!(:shipped, payload: { "region" => "us" })
      expect(wf.status).to eq("waiting")
      wf.signal!(:shipped, payload: { region: "eu" })
      run_until(wf, statuses: %w[finished])

      expect(logged).to eq(["eu"])
      expect(signals_of(wf).map(&:status)).to eq(%w[pending consumed])
    end
  end

  describe "the signal lifecycle across attempts" do
    before do
      calls = log
      attempts = Concurrent::AtomicFixnum.new
      stub_const("FlakyShipmentWorkflow", Class.new(Pgbus::Workflow) do
        step :ship, wait_for: :shipped do
          calls << [:attempt, received_signal.payload["at"]]
          reattempt! if attempts.increment == 1
          raise ArgumentError, "label printer jammed" if attempts.value == 2
        end
      end)
    end

    it "keeps the claim through reattempt! and a failure that pauses, and consumes it when the step completes" do
      wf = run_until_waiting(FlakyShipmentWorkflow.start!)
      signal = wf.signal!(:shipped, payload: { at: "noon" })
      run_until(wf, statuses: %w[paused])

      expect(signal.reload.status).to eq("claimed")
      expect(steps_of(wf).map { |step| [step.attempt, step.outcome, step.signal_id] })
        .to eq([[1, "reattempted", signal.id], [2, "paused", signal.id]])

      wf.resume!
      run_until(wf, statuses: %w[finished])
      expect(logged).to eq([[:attempt, "noon"], [:attempt, "noon"], [:attempt, "noon"]])
      expect(signal.reload.status).to eq("consumed")
      expect(steps_of(wf).last.signal_id).to eq(signal.id)
    end
  end

  describe "skip! on a waiting run" do
    it "skips the parked step, leaves the buffered signals pending, and moves on" do
      wf = run_until_waiting(ShipmentWorkflow.start!)
      wf.signal!(:refunded)

      wf.skip!(actor: "ops", reason: "picked up in store")
      expect(wf.status).to eq("ready")
      expect(steps_of(wf)[1]).to have_attributes(status: "skipped", outcome: "skipped")
      skipped = wf.events.reverse.find { |event| event.event == "step_skipped" }
      expect([skipped.actor, skipped.reason]).to eq(["ops", "picked up in store"])
      run_until(wf, statuses: %w[finished])

      expect(logged).to eq(%i[pack receipt])
      expect(signals_of(wf).map(&:status)).to eq(%w[pending])
    end

    it "is only legal while the run waits" do
      wf = ShipmentWorkflow.start!

      expect { wf.skip! }.to raise_error(Pgbus::Workflow::InvalidTransition, /cannot skip a ready workflow/)
    end
  end

  describe "a step body that skips while holding a signal" do
    before do
      stub_const("SkippingWorkflow", Class.new(Pgbus::Workflow) do
        step(:wait, wait_for: :shipped) { skip! }
      end)
    end

    it "consumes the signal it holds" do
      wf = SkippingWorkflow.start!
      signal = wf.signal!(:shipped)
      run_until(wf, statuses: %w[finished])

      expect(signal.reload.status).to eq("consumed")
      expect(steps_of(wf).last.status).to eq("skipped")
    end
  end

  describe ".signal! by subject" do
    before do
      stub_const("OrderShipmentWorkflow", Class.new(Pgbus::Workflow) do
        step(:await, wait_for: :shipped) { nil }
      end)
    end

    it "signals the subject's ongoing run" do
      user = create_user
      wf = run_until_waiting(OrderShipmentWorkflow.start!(user))

      signal = OrderShipmentWorkflow.signal!(user, :shipped, payload: { at: "now" }, idempotency_key: "evt-9", actor: "webhook")

      expect(signal).to have_attributes(workflow_id: wf.workflow_id, status: "claimed")
      expect(wf.reload.status).to eq("ready")
      expect(wf.events.find { |event| event.event == "signal_received" }.actor).to eq("webhook")
    end

    it "raises NotOngoing with missing: :raise and returns nil with missing: :ignore when no run is ongoing" do
      user = create_user

      expect { OrderShipmentWorkflow.signal!(user, :shipped) }
        .to raise_error(Pgbus::Workflow::NotOngoing, /no ongoing run for WorkflowSpecUser #{user.id}/)
      expect(OrderShipmentWorkflow.signal!(user, :shipped, missing: :ignore)).to be_nil

      wf = OrderShipmentWorkflow.start!(user)
      wf.cancel!
      expect(OrderShipmentWorkflow.signal!(user, :shipped, missing: :ignore)).to be_nil
    end

    it "raises ArgumentError without a subject, or with an unknown missing:" do
      expect { OrderShipmentWorkflow.signal!(nil, :shipped) }.to raise_error(ArgumentError, /needs a subject/)
      expect { OrderShipmentWorkflow.signal!(create_user, :shipped, missing: :skip) }
        .to raise_error(ArgumentError, /missing: must be :raise or :ignore/)
    end
  end
end
