# frozen_string_literal: true

require_relative "support"

# Issue #540, step 4a: durable step outputs.
RSpec.describe "Workflow step outputs (integration)", :integration do
  include WorkflowIntegration

  it "merges output calls, and later steps read the completed step's output" do
    user = create_user
    stub_const("CheckoutWorkflow", Class.new(Pgbus::Workflow) do
      step :charge do
        output(charge_id: "ch_1")
        output(amount: 1200, customer: subject) # rubocop:disable RSpec/NamedSubject -- the run's record
      end

      step :receipt do
        charge = output_of(:charge)
        output(seen: [charge[:charge_id], charge["amount"], charge[:customer].id])
      end
    end)

    wf = run_until(CheckoutWorkflow.start!(user), statuses: %w[finished])

    expect(wf.output_of(:receipt)).to eq("seen" => ["ch_1", 1200, user.id])
    expect(wf.output_of(:charge)[:customer]).to eq(user)
    expect(wf.outputs.keys).to eq(%w[charge receipt])
  end

  it "keeps a failed attempt's output on its row but never returns it" do
    attempts = Concurrent::AtomicFixnum.new
    stub_const("RetryWorkflow", Class.new(Pgbus::Workflow) do
      step :fetch do
        n = attempts.increment
        output(attempt: n)
        raise ArgumentError, "first try fails" if n == 1
      end
    end)

    wf = run_until(RetryWorkflow.start!)
    expect(wf.status).to eq("paused")
    expect(steps_of(wf).first.output).to be_present
    expect(wf.output_of(:fetch)).to be_nil

    wf.resume!
    run_until(wf, statuses: %w[finished])
    expect(wf.output_of(:fetch)).to eq("attempt" => 2)
  end

  it "sends an unserialisable value through the exception policies" do
    stub_const("BadOutputWorkflow", Class.new(Pgbus::Workflow) do
      on_exception ActiveJob::SerializationError, action: :cancel!

      step(:bad) { output(io: Object.new) }
    end)

    wf = run_until(BadOutputWorkflow.start!)

    expect(wf.status).to eq("cancelled")
    expect(steps_of(wf).first).to have_attributes(status: "failed", error_class: "ActiveJob::SerializationError", output: nil)
  end

  it "raises PayloadTooLarge inside the step when an output passes the cap" do
    allow(Pgbus.configuration).to receive(:workflow_max_payload_size).and_return(256)
    stub_const("HugeOutputWorkflow", Class.new(Pgbus::Workflow) { step(:huge) { output(blob: "x" * 300) } })

    wf = run_until(HugeOutputWorkflow.start!)

    expect(wf.status).to eq("paused")
    expect(steps_of(wf).first.error_class).to eq("Pgbus::Workflow::PayloadTooLarge")
  end

  it "ignores the body's return value" do
    stub_const("ReturnsRecordWorkflow", Class.new(Pgbus::Workflow) { step(:returns) { Object.new } })

    wf = run_until(ReturnsRecordWorkflow.start!, statuses: %w[finished])

    expect(steps_of(wf).first.output).to be_nil
  end

  it "raises NotInStep for output outside a step" do
    stub_const("PlainWorkflow", Class.new(Pgbus::Workflow) { step(:one) { nil } })

    expect { PlainWorkflow.start!.output(x: 1) }.to raise_error(Pgbus::Workflow::NotInStep, /output/)
  end
end
