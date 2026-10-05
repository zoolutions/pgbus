# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::FailedEventRecorder do
  let(:mock_connection) { instance_double(ActiveRecord::ConnectionAdapters::AbstractAdapter) }

  before do
    allow(ActiveRecord::Base).to receive(:connection).and_return(mock_connection)
  end

  describe ".record!" do
    let(:error) { StandardError.new("API timeout") }

    before do
      error.set_backtrace(["app/jobs/test_job.rb:10:in `perform'", "pgbus/executor.rb:55"])
    end

    it "inserts a failed event with error details" do
      allow(mock_connection).to receive(:exec_query)

      described_class.record!(
        queue_name: "default",
        msg_id: 42,
        payload: { "job_class" => "TestJob" },
        headers: { "pgbus.recurring_key" => "test" },
        error: error,
        retry_count: 2
      )

      expect(mock_connection).to have_received(:exec_query).with(
        a_string_matching(/INSERT INTO pgbus_failed_events/),
        "FailedEvent Record",
        array_including("default", 42)
      )
    end

    it "does not raise on database errors" do
      allow(mock_connection).to receive(:exec_query).and_raise(ActiveRecord::StatementInvalid, "table missing")

      expect do
        described_class.record!(
          queue_name: "default", msg_id: 1, payload: "{}", headers: nil,
          error: error, retry_count: 0
        )
      end.not_to raise_error
    end

    it "truncates long error messages" do
      long_error = StandardError.new("x" * 20_000)
      long_error.set_backtrace([])
      allow(mock_connection).to receive(:exec_query)

      described_class.record!(
        queue_name: "default", msg_id: 1, payload: "{}", headers: nil,
        error: long_error, retry_count: 0
      )

      expect(mock_connection).to have_received(:exec_query).with(
        anything, anything,
        a_collection_including(a_string_matching(/\A.{1,10003}\z/))
      )
    end
  end

  describe ".exists?" do
    it "returns true when a failed event row exists" do
      allow(mock_connection).to receive(:select_value).and_return(1)

      result = described_class.exists?(queue_name: "default", msg_id: 42)

      expect(result).to be true
      expect(mock_connection).to have_received(:select_value).with(
        a_string_matching(/SELECT 1 FROM pgbus_failed_events/),
        "FailedEvent Exists",
        ["default", 42]
      )
    end

    it "returns false when no failed event row exists" do
      allow(mock_connection).to receive(:select_value).and_return(nil)

      result = described_class.exists?(queue_name: "default", msg_id: 42)

      expect(result).to be false
    end

    it "returns false on database errors" do
      allow(mock_connection).to receive(:select_value).and_raise(ActiveRecord::StatementInvalid, "table missing")

      expect(described_class.exists?(queue_name: "default", msg_id: 42)).to be false
    end
  end

  describe ".clear!" do
    it "deletes the failed event for the given queue and msg_id" do
      allow(mock_connection).to receive(:exec_delete)

      described_class.clear!(queue_name: "default", msg_id: 42)

      expect(mock_connection).to have_received(:exec_delete).with(
        a_string_matching(/DELETE FROM pgbus_failed_events/),
        "FailedEvent Clear",
        ["default", 42]
      )
    end

    it "does not raise on database errors" do
      allow(mock_connection).to receive(:exec_delete).and_raise(ActiveRecord::StatementInvalid, "table missing")

      expect do
        described_class.clear!(queue_name: "default", msg_id: 42)
      end.not_to raise_error
    end
  end

  describe ".clear_queue!" do
    it "deletes every failed event recorded under any of the given queue names" do
      allow(mock_connection).to receive(:exec_delete)

      described_class.clear_queue!(%w[pgbus_default default])

      expect(mock_connection).to have_received(:exec_delete).with(
        a_string_matching(/DELETE FROM pgbus_failed_events WHERE queue_name = ANY/),
        "FailedEvent Clear Queue",
        ["{pgbus_default,default}"]
      )
    end

    it "does not raise on database errors" do
      allow(mock_connection).to receive(:exec_delete).and_raise(ActiveRecord::StatementInvalid, "table missing")

      expect { described_class.clear_queue!(%w[default]) }.not_to raise_error
    end
  end
end
