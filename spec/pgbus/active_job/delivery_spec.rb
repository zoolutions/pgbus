# frozen_string_literal: true

require "spec_helper"
require "json"
require "active_job"
require "async"

RSpec.describe Pgbus::ActiveJob::Delivery do
  describe ".with" do
    it "exposes the delivery inside the block and restores what was there after, also on a raise" do
      seen = described_class.with(msg_id: 7, read_ct: 2, queue_name: "default") { described_class.current }

      expect(seen).to eq(msg_id: 7, read_ct: 2, queue_name: "default")
      expect(described_class.current).to be_nil

      expect { described_class.with(msg_id: 1, read_ct: 1, queue_name: "q") { raise ArgumentError } }.to raise_error(ArgumentError)
      expect(described_class.current).to be_nil
    end

    it "is fiber-local: concurrent Async tasks each see their own delivery" do
      seen = {}
      Sync do |task|
        [1, 2].map do |msg_id|
          task.async do
            described_class.with(msg_id: msg_id, read_ct: 1, queue_name: "default") do
              task.yield
              seen[msg_id] = described_class.current[:msg_id]
            end
          end
        end.each(&:wait)
      end

      expect(seen).to eq(1 => 1, 2 => 2)
    end
  end

  describe "the executor" do
    subject(:executor) { Pgbus::ActiveJob::Executor.new(client: build_mock_client, config: Pgbus.configuration) }

    let(:job_id) { SecureRandom.uuid }
    let(:payload) { { "job_class" => "TestJob", "job_id" => job_id, "queue_name" => "default", "arguments" => [] } }
    let(:message) { build_message_double(msg_id: 42, message: JSON.generate(payload), read_ct: 3) }
    let(:job) { build_job_double(job_class: "TestJob", queue_name: "default", job_id: job_id) }

    before do
      allow(ActiveJob::Base).to receive(:deserialize).with(payload).and_return(job)
      allow(Pgbus::Concurrency).to receive(:extract_key).and_return(nil)
      stub_const("Pgbus::JobStat", Class.new)
      allow(Pgbus::JobStat).to receive_messages(record!: nil, table_exists?: true)
      allow(Pgbus::FailedEventRecorder).to receive_messages(record!: nil, clear!: nil, last_error: nil)
      allow(Pgbus::ErrorReporter).to receive(:report)
    end

    it "sets Delivery.current around perform_now and clears it after" do
      seen = nil
      allow(job).to receive(:perform_now) { seen = described_class.current }

      executor.execute(message, "default")

      expect(seen).to eq(msg_id: 42, read_ct: 3, queue_name: "default")
      expect(described_class.current).to be_nil
    end

    it "clears it when the job raises" do
      allow(job).to receive(:perform_now).and_raise(RuntimeError, "boom")

      expect(executor.execute(message, "default")).to eq(:failed)
      expect(described_class.current).to be_nil
    end
  end
end
