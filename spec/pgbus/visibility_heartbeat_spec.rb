# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::VisibilityHeartbeat do
  let(:client) { instance_double(Pgbus::Client, set_visibility_timeout: nil) }
  let(:config) do
    Pgbus::Configuration.new.tap do |c|
      c.visibility_timeout = 30
      c.visibility_heartbeat = true
    end
  end

  after { described_class.reset! }

  def track(msg_id: 7, queue_name: "default", **, &)
    described_class.track(client: client, queue_name: queue_name, msg_id: msg_id, job_class: "SlowJob",
                          config: config, **, &)
  end

  describe ".track" do
    it "yields, keeps the message registered while the block runs and drops it afterwards" do
      inside = nil
      track { inside = described_class.tracked_count }

      expect(inside).to eq(1)
      expect(described_class.tracked_count).to eq(0)
    end

    it "drops the entry when the block raises" do
      expect { track { raise "boom" } }.to raise_error("boom")

      expect(described_class.tracked_count).to eq(0)
    end

    it "returns the block's value" do
      expect(track { :done }).to eq(:done)
    end

    it "does nothing when the heartbeat is disabled" do
      config.visibility_heartbeat = false
      inside = nil

      track { inside = described_class.tracked_count }

      expect(inside).to eq(0)
    end

    it "starts one ticker thread per process" do
      track { nil }

      thread = described_class.instance_variable_get(:@thread)
      expect(thread).to be_a(Thread)
      expect(thread.name).to eq("pgbus-visibility-heartbeat")
    end
  end

  describe ".tick!" do
    it "does not extend a message whose interval has not elapsed" do
      track do
        extended = described_class.tick!(now: 0, config: config)
        expect(extended).to eq(0)
      end

      expect(client).not_to have_received(:set_visibility_timeout)
    end

    it "re-arms the visibility timeout once the interval has elapsed" do
      track do
        extended = described_class.tick!(now: Process.clock_gettime(Process::CLOCK_MONOTONIC) + 10, config: config)
        expect(extended).to eq(1)
      end

      expect(client).to have_received(:set_visibility_timeout).with("default", 7, vt: 30, prefixed: true).once
    end

    it "honours a configured interval" do
      config.visibility_heartbeat_interval = 2
      start = Process.clock_gettime(Process::CLOCK_MONOTONIC)

      track do
        described_class.tick!(now: start + 1, config: config)
        described_class.tick!(now: start + 3, config: config)
        described_class.tick!(now: start + 4, config: config)
        described_class.tick!(now: start + 6, config: config)
      end

      expect(client).to have_received(:set_visibility_timeout).twice
    end

    it "passes prefixed: false through for a physical (priority sub-)queue name" do
      track(queue_name: "pgbus_default_p1", prefixed: false) do
        described_class.tick!(now: Process.clock_gettime(Process::CLOCK_MONOTONIC) + 10, config: config)
      end

      expect(client).to have_received(:set_visibility_timeout).with("pgbus_default_p1", 7, vt: 30, prefixed: false)
    end

    it "keeps a concurrency-limited job's semaphore alive on every extension" do
      allow(Pgbus::Concurrency::Semaphore).to receive(:touch)

      track(concurrency: ["ImportJob-42", 600]) do
        described_class.tick!(now: Process.clock_gettime(Process::CLOCK_MONOTONIC) + 10, config: config)
      end

      expect(Pgbus::Concurrency::Semaphore).to have_received(:touch).with("ImportJob-42", 600).once
    end

    # The touch runs between the VT extension and the instrumentation, and
    # extend!'s outer rescue would swallow an escaping raise — so asserting
    # only "set_visibility_timeout ran" passes even with the containment
    # removed. Assert what actually distinguishes the two paths: the
    # semaphore-specific warn, and that the beat still completes.
    it "contains a failing semaphore touch instead of aborting the beat" do
      allow(Pgbus::Concurrency::Semaphore).to receive(:touch).and_raise(StandardError, "pooler timeout")
      logger = instance_double(Logger, warn: nil, debug: nil, info: nil, error: nil)
      allow(Pgbus).to receive(:logger).and_return(logger)
      allow(ActiveSupport::Notifications).to receive(:instrument).and_call_original

      track(concurrency: ["ImportJob-42", 600]) do
        described_class.tick!(now: Process.clock_gettime(Process::CLOCK_MONOTONIC) + 10, config: config)
      end

      expect(client).to have_received(:set_visibility_timeout).once
      expect(logger).to have_received(:warn) do |&block|
        expect(block.call).to include("could not touch semaphore ImportJob-42")
      end
      # The beat ran to completion: instrumentation is emitted after the touch.
      expect(ActiveSupport::Notifications).to have_received(:instrument)
        .with("pgbus.job_visibility_extended", hash_including(extensions: 1))
    end

    it "does not touch any semaphore for a job without a concurrency key" do
      allow(Pgbus::Concurrency::Semaphore).to receive(:touch)

      track { described_class.tick!(now: Process.clock_gettime(Process::CLOCK_MONOTONIC) + 10, config: config) }

      expect(Pgbus::Concurrency::Semaphore).not_to have_received(:touch)
    end

    it "instruments every extension" do
      allow(ActiveSupport::Notifications).to receive(:instrument).and_call_original

      track do
        described_class.tick!(now: Process.clock_gettime(Process::CLOCK_MONOTONIC) + 10, config: config)
      end

      expect(ActiveSupport::Notifications).to have_received(:instrument).with(
        "pgbus.job_visibility_extended",
        queue: "default", job_class: "SlowJob", msg_id: 7, vt: 30, extensions: 1
      )
    end

    it "keeps ticking when an extension fails" do
      allow(client).to receive(:set_visibility_timeout).and_raise(StandardError, "pg gone")
      allow(Pgbus.logger).to receive(:warn)

      track do
        expect do
          described_class.tick!(now: Process.clock_gettime(Process::CLOCK_MONOTONIC) + 10, config: config)
        end.not_to raise_error
      end

      expect(Pgbus.logger).to have_received(:warn)
    end
  end

  describe ".stop" do
    it "stops the ticker thread and keeps tracked entries" do
      track do
        described_class.stop

        expect(described_class.instance_variable_get(:@thread)).to be_nil
        expect(described_class.tracked_count).to eq(1)
      end
    end

    it "is a no-op without a thread" do
      expect { described_class.stop }.not_to raise_error
    end
  end

  describe "fork safety" do
    it "forgets the parent's entries when the pid changes" do
      track do
        described_class.instance_variable_set(:@pid, -1)

        track(msg_id: 8) { expect(described_class.tracked_count).to eq(1) }
      end
    end
  end
end
