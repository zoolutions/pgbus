# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::EventBus::ClaimBeat do
  subject(:beat) { described_class.new }

  let(:event_id) { "evt-1" }
  let(:handler_class) { "OrderHandler" }
  let(:relation) { double("ActiveRecord::Relation", update_all: 1) }

  before do
    allow(Pgbus::ProcessedEvent).to receive_messages(completion_column?: true, where: relation)
  end

  it "starts empty" do
    expect(beat).to be_empty
    expect(beat.size).to eq(0)
  end

  describe "#touch!" do
    it "touches processed_at on every registered pending claim" do
      beat.register(event_id, handler_class)

      expect(beat.touch!).to eq(1)
      expect(Pgbus::ProcessedEvent).to have_received(:where)
        .with(event_id: event_id, handler_class: handler_class, completed_at: nil)
      expect(relation).to have_received(:update_all).with(processed_at: kind_of(Time))
    end

    it "touches each of several claims registered for one message" do
      beat.register("a", "H1")
      beat.register("b", "H2")

      expect(beat.touch!).to eq(2)
      expect(relation).to have_received(:update_all).twice
    end

    it "does nothing once the claim is released" do
      beat.register(event_id, handler_class)
      beat.release(event_id, handler_class)

      expect(beat.touch!).to eq(0)
      expect(relation).not_to have_received(:update_all)
    end

    it "registers a claim only once" do
      beat.register(event_id, handler_class)
      beat.register(event_id, handler_class)

      expect(beat.size).to eq(1)
    end

    it "is a no-op on a legacy schema without completed_at" do
      allow(Pgbus::ProcessedEvent).to receive(:completion_column?).and_return(false)
      beat.register(event_id, handler_class)

      expect(beat.touch!).to eq(0)
      expect(Pgbus::ProcessedEvent).not_to have_received(:where)
    end

    # The beat runs on the VisibilityHeartbeat ticker thread while the handler
    # runs on a pool thread: a claim released mid-iteration must not raise
    # there and take the ticker (and every other message's VT) down with it.
    it "survives a claim that fails to update" do
      allow(relation).to receive(:update_all).and_raise(ActiveRecord::StatementInvalid, "gone")
      beat.register(event_id, handler_class)

      expect { beat.touch! }.not_to raise_error
    end
  end
end
