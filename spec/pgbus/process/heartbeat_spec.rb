# frozen_string_literal: true

require "spec_helper"
require "json"
require "socket"

RSpec.describe Pgbus::Process::Heartbeat do
  let(:timer) { instance_double(Concurrent::TimerTask, execute: true, shutdown: true) }
  let(:process_record) { double("ProcessEntry", id: 42) }
  let(:heartbeat) { described_class.new(kind: "worker", metadata: { queues: %w[default] }) }

  before do
    allow(Concurrent::TimerTask).to receive(:new).and_return(timer)
  end

  describe "#start" do
    before do
      allow(Pgbus::ProcessEntry).to receive(:create!).and_return(process_record)
    end

    it "registers the process via ProcessEntry" do
      heartbeat.start

      expect(Pgbus::ProcessEntry).to have_received(:create!).with(
        hash_including(kind: "worker", pid: Process.pid)
      )
    end

    it "creates and executes a timer task" do
      heartbeat.start

      expect(Concurrent::TimerTask).to have_received(:new).with(execution_interval: described_class::INTERVAL)
      expect(timer).to have_received(:execute)
    end

    # The deploy-task rollover gate reads metadata["revision"] off every live
    # row (issue #536), so every kind stamps it from its first registration.
    it "stamps the app revision into the registered metadata" do
      allow(Pgbus::Deploy::Revision).to receive(:current).and_return("abc123")

      heartbeat.start

      expect(Pgbus::ProcessEntry).to have_received(:create!).with(
        hash_including(metadata: { queues: %w[default], revision: "abc123" })
      )
    end

    it "resolves the revision from the owning process's configuration" do
      config = Pgbus::Configuration.new
      config.app_revision = "from-process-config"

      described_class.new(kind: "worker", config: config).start

      expect(Pgbus::ProcessEntry).to have_received(:create!).with(
        hash_including(metadata: { revision: "from-process-config" })
      )
    end

    it "stores a nil revision rather than omitting the key" do
      allow(Pgbus::Deploy::Revision).to receive(:current).and_return(nil)

      heartbeat.start

      expect(Pgbus::ProcessEntry).to have_received(:create!) do |attrs|
        expect(attrs[:metadata]).to have_key(:revision)
        expect(attrs[:metadata][:revision]).to be_nil
      end
    end
  end

  describe "#beat" do
    context "when process_id is set" do
      let(:scope) { double("scope", update_all: 1) }

      before do
        allow(Pgbus::ProcessEntry).to receive(:create!).and_return(process_record)
        allow(Pgbus::ProcessEntry).to receive(:where).with(id: 42).and_return(scope)
        heartbeat.start
      end

      it "updates the heartbeat timestamp" do
        heartbeat.beat

        expect(scope).to have_received(:update_all) do |args|
          expect(args[:last_heartbeat_at]).to be_a(Time)
        end
      end
    end

    context "when process_id is nil" do
      it "does nothing" do
        expect { heartbeat.beat }.not_to raise_error
      end
    end

    # Issue #587: a pipeless child checks in with the supervisor only through
    # its row, so a registration that failed once (a DB blip at boot) is
    # retried on the next beat instead of leaving the process invisible.
    context "when the registration at start failed" do
      before do
        allow(Pgbus::ProcessEntry).to receive(:create!).and_invoke(
          ->(**) { raise StandardError, "db down" },
          ->(**) { process_record }
        )
        allow(Pgbus.logger).to receive(:warn)
        heartbeat.start
      end

      it "registers on the next beat" do
        heartbeat.beat

        expect(Pgbus::ProcessEntry).to have_received(:create!).twice
      end

      it "does not register after stop" do
        heartbeat.stop
        heartbeat.beat

        expect(Pgbus::ProcessEntry).to have_received(:create!).once
      end
    end

    # The row can vanish underneath a live process (stale-process reaper after
    # a heartbeat gap, manual cleanup, another host's clock skew). update_all
    # by id then matches 0 rows and raises nothing — the process must
    # re-register instead of staying invisible forever (issue #438).
    context "when the process row has been deleted" do
      let(:heartbeat) do
        described_class.new(kind: "worker", metadata: { queues: %w[default] }, loop_tick_supplier: -> { 99.5 })
      end
      let(:new_record) { double("ProcessEntry", id: 43) }
      let(:gone_scope) { double("gone scope", update_all: 0) }
      let(:new_scope) { double("new scope", update_all: 1) }
      let(:warnings) { [] }

      before do
        allow(Pgbus::ProcessEntry).to receive(:create!).and_return(process_record, new_record)
        allow(Pgbus::ProcessEntry).to receive(:where).with(id: 42).and_return(gone_scope)
        allow(Pgbus::ProcessEntry).to receive(:where).with(id: 43).and_return(new_scope)
        allow(Pgbus.logger).to receive(:warn) { |&blk| warnings << blk.call }
        heartbeat.start
      end

      it "re-registers the process" do
        heartbeat.beat

        expect(Pgbus::ProcessEntry).to have_received(:create!).twice
      end

      it "logs a warning naming the missing row" do
        heartbeat.beat

        expect(warnings.join).to include("id=42", "kind=worker", "pid=#{Process.pid}", "re-registering")
      end

      it "applies this beat's updates to the new row" do
        heartbeat.beat

        expect(new_scope).to have_received(:update_all) do |args|
          expect(args[:last_heartbeat_at]).to be_a(Time)
          expect(args[:metadata]).to include("loop_tick_at" => 99.5)
        end
      end

      it "beats against the new row from then on" do
        heartbeat.beat
        heartbeat.beat

        expect(gone_scope).to have_received(:update_all).once
        expect(new_scope).to have_received(:update_all).twice
      end

      it "deregisters the new row on stop" do
        allow(new_scope).to receive(:delete_all).and_return(1)
        heartbeat.beat
        heartbeat.stop

        expect(new_scope).to have_received(:delete_all)
      end

      context "when re-registration fails" do
        before do
          allow(Pgbus::ProcessEntry).to receive(:create!).and_invoke(
            ->(**) { process_record },
            ->(**) { raise StandardError, "db down" }
          )
        end

        it "logs and keeps the old id for the next beat" do
          expect { heartbeat.beat }.not_to raise_error
          heartbeat.beat

          expect(warnings.join).to include("Process registration failed")
          expect(gone_scope).to have_received(:update_all).twice
        end
      end

      context "when stop has already run" do
        before { allow(gone_scope).to receive(:delete_all).and_return(1) }

        it "does not resurrect the row" do
          heartbeat.stop
          heartbeat.beat

          expect(Pgbus::ProcessEntry).to have_received(:create!).once
          expect(gone_scope).not_to have_received(:update_all)
        end
      end
    end

    context "when update raises an error" do
      before do
        allow(Pgbus::ProcessEntry).to receive(:create!).and_return(process_record)
        allow(Pgbus::ProcessEntry).to receive(:where).and_raise(StandardError, "connection lost")
        heartbeat.start
      end

      it "logs a warning instead of raising" do
        expect { heartbeat.beat }.not_to raise_error
      end
    end

    context "with a metadata_supplier" do
      let(:scope) { double("scope", update_all: 1) }
      let(:heartbeat) do
        described_class.new(
          kind: "worker",
          metadata: { queues: %w[default] },
          metadata_supplier: -> { { "rates" => { "processed" => 12.4 } } }
        )
      end

      before do
        allow(Pgbus::ProcessEntry).to receive(:create!).and_return(process_record)
        allow(Pgbus::ProcessEntry).to receive(:where).with(id: 42).and_return(scope)
        heartbeat.start
      end

      it "merges the supplier hash into persisted metadata" do
        heartbeat.beat

        expect(scope).to have_received(:update_all) do |args|
          expect(args[:metadata]).to include(
            queues: %w[default],
            "rates" => { "processed" => 12.4 }
          )
        end
      end

      it "keeps the revision on every beat" do
        allow(Pgbus::Deploy::Revision).to receive(:current).and_return("abc123")
        heartbeat = described_class.new(
          kind: "worker", metadata: { queues: %w[default] },
          metadata_supplier: -> { { "rates" => { "processed" => 1.0 } } }
        )
        heartbeat.start
        heartbeat.beat

        expect(scope).to have_received(:update_all) do |args|
          expect(args[:metadata]).to include(revision: "abc123", "rates" => { "processed" => 1.0 })
        end
      end

      it "merges supplier metadata alongside loop_tick_at" do
        heartbeat = described_class.new(
          kind: "worker",
          metadata: { queues: %w[default] },
          metadata_supplier: -> { { "rates" => { "processed" => 1.0 } } },
          loop_tick_supplier: -> { 123.45 }
        )
        allow(Pgbus::ProcessEntry).to receive(:where).with(id: 42).and_return(scope)
        heartbeat.start
        heartbeat.beat

        expect(scope).to have_received(:update_all) do |args|
          expect(args[:metadata]).to include(
            "loop_tick_at" => 123.45,
            "rates" => { "processed" => 1.0 }
          )
        end
      end
    end

    context "when the metadata_supplier raises" do
      let(:scope) { double("scope", update_all: 1) }
      let(:heartbeat) do
        described_class.new(
          kind: "worker",
          metadata: { queues: %w[default] },
          metadata_supplier: -> { raise "supplier boom" }
        )
      end

      before do
        allow(Pgbus::ProcessEntry).to receive(:create!).and_return(process_record)
        allow(Pgbus::ProcessEntry).to receive(:where).with(id: 42).and_return(scope)
        heartbeat.start
      end

      it "continues without raising" do
        expect { heartbeat.beat }.not_to raise_error
      end

      it "logs a warning when the supplier fails" do
        allow(Pgbus.logger).to receive(:warn)
        heartbeat.beat

        expect(Pgbus.logger).to have_received(:warn)
      end
    end
  end

  describe "#stop" do
    let(:scope) { double("scope", delete_all: 1) }

    before do
      allow(Pgbus::ProcessEntry).to receive(:create!).and_return(process_record)
      allow(Pgbus::ProcessEntry).to receive(:where).with(id: 42).and_return(scope)
      heartbeat.start
    end

    it "shuts down the timer" do
      heartbeat.stop

      expect(timer).to have_received(:shutdown)
    end

    it "deregisters the process" do
      heartbeat.stop

      expect(scope).to have_received(:delete_all)
    end
  end
end
