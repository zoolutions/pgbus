# frozen_string_literal: true

require "spec_helper"

# Issue #683: a batch scope's hold row must look alive to the stalled-batch
# sweep for as long as its scope is open in a live process, whether or not
# the block keeps calling perform_later. HoldKeeper refreshes every
# registered hold on a timer.
RSpec.describe Pgbus::Batch::HoldKeeper do
  let(:relation) { double("relation", update_all: 1) }

  before do
    described_class.reset!
    stub_bus_record_pool
    allow(Pgbus::BatchExecution).to receive(:where).and_return(relation)
  end

  after { described_class.reset! }

  it "touches every registered hold, and none once it is unregistered" do
    allow(described_class).to receive(:start_timer)
    described_class.register("hold-a")
    described_class.register("hold-b")

    described_class.refresh!

    expect(Pgbus::BatchExecution).to have_received(:where).with(job_id: contain_exactly("hold-a", "hold-b"))
    expect(relation).to have_received(:update_all).with(created_at: an_instance_of(ActiveSupport::TimeWithZone).or(an_instance_of(Time)))

    described_class.unregister("hold-a")
    described_class.unregister("hold-b")

    expect(described_class.refresh!).to eq(0)
  end

  it "refreshes on its own timer while a hold is registered" do
    allow(described_class).to receive(:interval).and_return(0.05)
    ticks = Queue.new
    allow(described_class).to(receive(:refresh!).and_wrap_original { |original| ticks << :tick and original.call })

    described_class.register("hold-a")

    expect(ticks.pop(timeout: 2)).to eq(:tick)
  end

  it "logs a refresh that fails and keeps going" do
    allow(described_class).to receive(:start_timer)
    allow(relation).to receive(:update_all).and_raise(IOError, "db gone")
    allow(Pgbus.logger).to receive(:warn)
    described_class.register("hold-a")

    expect(described_class.refresh!).to eq(0)
    expect(Pgbus.logger).to have_received(:warn)
  end
end
