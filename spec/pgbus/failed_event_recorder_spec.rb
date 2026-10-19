# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::FailedEventRecorder do
  let(:mock_connection) { instance_double(ActiveRecord::ConnectionAdapters::AbstractAdapter) }
  let(:source_column) { true }

  before do
    allow(ActiveRecord::Base).to receive(:connection).and_return(mock_connection)
    allow(mock_connection).to receive(:column_exists?).with(:pgbus_failed_events, :source_queue)
                                                      .and_return(source_column)
    described_class.reset_source_queue_column_check!
  end

  after { described_class.reset_source_queue_column_check! }

  # Issue #583: an upgraded gem may run before pgbus:add_failed_events_source_queue
  # is migrated, so the column is probed once and every query has a legacy shape.
  describe ".source_queue_column?" do
    it "is true when pgbus_failed_events has the source_queue column" do
      expect(described_class.source_queue_column?).to be(true)
    end

    it "probes the schema once per process" do
      2.times { described_class.source_queue_column? }

      expect(mock_connection).to have_received(:column_exists?).once
    end

    context "when the column has not been migrated yet" do
      let(:source_column) { false }

      it "is false and warns once, naming the generator" do
        allow(Pgbus.logger).to receive(:warn)

        2.times { expect(described_class.source_queue_column?).to be(false) }

        expect(Pgbus.logger).to have_received(:warn).once do |&block|
          expect(block.call).to include("pgbus:add_failed_events_source_queue")
        end
      end
    end

    it "does not memoize a probe that raised" do
      allow(mock_connection).to receive(:column_exists?).and_raise(ActiveRecord::ConnectionNotEstablished)

      expect { described_class.source_queue_column? }.to raise_error(ActiveRecord::ConnectionNotEstablished)

      allow(mock_connection).to receive(:column_exists?).and_return(true)
      expect(described_class.source_queue_column?).to be(true)
    end
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
        source_queue: "pgbus_default",
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

    # Issue #583: each _pN sub-queue numbers its messages from its own
    # sequence, so the row is keyed on the physical table as well.
    it "keys the row on the physical table the message was read from" do
      allow(mock_connection).to receive(:exec_query)

      described_class.record!(queue_name: "default", msg_id: 42, source_queue: "pgbus_default_p1",
                              payload: "{}", headers: nil, error: error, retry_count: 1)

      expect(mock_connection).to have_received(:exec_query).with(
        a_string_including("(queue_name, source_queue, msg_id,", "ON CONFLICT (queue_name, msg_id, source_queue)"),
        "FailedEvent Record",
        ["default", "pgbus_default_p1", 42, "{}", nil, "StandardError", "API timeout", anything, 1]
      )
    end

    context "when the source_queue column has not been migrated yet" do
      let(:source_column) { false }

      before { allow(Pgbus.logger).to receive(:warn) }

      it "writes the row the way the unmigrated table's index expects" do
        allow(mock_connection).to receive(:exec_query)

        described_class.record!(queue_name: "default", msg_id: 42, source_queue: "pgbus_default_p1",
                                payload: "{}", headers: nil, error: error, retry_count: 1)

        expect(mock_connection).to have_received(:exec_query).with(
          a_string_including("ON CONFLICT (queue_name, msg_id) DO UPDATE"),
          "FailedEvent Record",
          ["default", 42, "{}", nil, "StandardError", "API timeout", anything, 1]
        )
        expect(mock_connection).not_to have_received(:exec_query).with(a_string_including("source_queue"), any_args)
      end

      # A process that outlives the migration would otherwise keep sending
      # an ON CONFLICT target whose index the migration dropped.
      it "probes the schema again after the legacy write fails" do
        allow(mock_connection).to receive(:exec_query).and_raise(ActiveRecord::StatementInvalid, "no unique constraint")
        allow(Pgbus::ErrorReporter).to receive(:report)

        described_class.record!(queue_name: "default", msg_id: 42, source_queue: "pgbus_default",
                                payload: "{}", headers: nil, error: error, retry_count: 1)
        allow(mock_connection).to receive(:column_exists?).and_return(true)

        expect(described_class.source_queue_column?).to be(true)
      end
    end

    it "does not raise on database errors" do
      allow(mock_connection).to receive(:exec_query).and_raise(ActiveRecord::StatementInvalid, "table missing")

      expect do
        described_class.record!(
          queue_name: "default", msg_id: 1, source_queue: "pgbus_default", payload: "{}", headers: nil,
          error: error, retry_count: 0
        )
      end.not_to raise_error
    end

    it "truncates long error messages" do
      long_error = StandardError.new("x" * 20_000)
      long_error.set_backtrace([])
      allow(mock_connection).to receive(:exec_query)

      described_class.record!(
        queue_name: "default", msg_id: 1, source_queue: "pgbus_default", payload: "{}", headers: nil,
        error: long_error, retry_count: 0
      )

      expect(mock_connection).to have_received(:exec_query).with(
        anything, anything,
        a_collection_including(a_string_matching(/\A.{1,10003}\z/))
      )
    end
  end

  describe ".exists?" do
    it "returns true when the message's own row, or a row written before source_queue, exists" do
      allow(mock_connection).to receive(:select_value).and_return(1)

      result = described_class.exists?(queue_name: "default", msg_id: 42, source_queue: "pgbus_default_p1")

      expect(result).to be true
      expect(mock_connection).to have_received(:select_value).with(
        a_string_including("SELECT 1 FROM pgbus_failed_events",
                           "AND (source_queue = $3 OR source_queue IS NULL)"),
        "FailedEvent Exists",
        ["default", 42, "pgbus_default_p1"]
      )
    end

    it "returns false when no failed event row exists" do
      allow(mock_connection).to receive(:select_value).and_return(nil)

      result = described_class.exists?(queue_name: "default", msg_id: 42, source_queue: "pgbus_default")

      expect(result).to be false
    end

    it "returns false on database errors" do
      allow(mock_connection).to receive(:select_value).and_raise(ActiveRecord::StatementInvalid, "table missing")

      expect(described_class.exists?(queue_name: "default", msg_id: 42, source_queue: "pgbus_default")).to be false
    end

    context "when the source_queue column has not been migrated yet" do
      let(:source_column) { false }

      it "looks the row up by queue and msg_id" do
        allow(Pgbus.logger).to receive(:warn)
        allow(mock_connection).to receive(:select_value).and_return(1)

        described_class.exists?(queue_name: "default", msg_id: 42, source_queue: "pgbus_default")

        expect(mock_connection).to have_received(:select_value).with(
          "SELECT 1 FROM pgbus_failed_events WHERE queue_name = $1 AND msg_id = $2 LIMIT 1",
          "FailedEvent Exists", ["default", 42]
        )
      end
    end
  end

  describe ".last_error" do
    let(:row) do
      { "error_class" => "Stripe::CardError", "error_message" => "declined", "backtrace" => "a.rb:1\nb.rb:2",
        "retry_count" => 4, "failed_at" => Time.utc(2026, 10, 10, 11, 59) }
    end

    it "returns the recorded error for the message, its own row before one written without source_queue" do
      allow(mock_connection).to receive(:select_one).and_return(row)

      expect(described_class.last_error(queue_name: "default", msg_id: 42, source_queue: "pgbus_default_p1")).to eq(
        error_class: "Stripe::CardError", error_message: "declined", backtrace: "a.rb:1\nb.rb:2",
        retry_count: 4, failed_at: "2026-10-10T11:59:00.000000Z"
      )
      expect(mock_connection).to have_received(:select_one).with(
        a_string_including("FROM pgbus_failed_events WHERE queue_name = $1 AND msg_id = $2",
                           "AND (source_queue = $3 OR source_queue IS NULL)",
                           "ORDER BY source_queue IS NULL, failed_at DESC LIMIT 1"),
        "FailedEvent Last Error",
        ["default", 42, "pgbus_default_p1"]
      )
    end

    it "normalizes a string failed_at to UTC ISO 8601" do
      allow(mock_connection).to receive(:select_one).and_return(row.merge("failed_at" => "2026-10-10 13:59:00+02"))

      expect(described_class.last_error(queue_name: "default", msg_id: 42, source_queue: "pgbus_default")[:failed_at])
        .to eq("2026-10-10T11:59:00.000000Z")
    end

    it "returns nil when no failure was recorded" do
      allow(mock_connection).to receive(:select_one).and_return(nil)

      expect(described_class.last_error(queue_name: "default", msg_id: 42, source_queue: "pgbus_default")).to be_nil
    end

    it "returns nil and logs at debug when the query raises" do
      allow(mock_connection).to receive(:select_one).and_raise(ActiveRecord::StatementInvalid, "table missing")
      allow(Pgbus.logger).to receive(:debug)

      expect(described_class.last_error(queue_name: "default", msg_id: 42, source_queue: "pgbus_default")).to be_nil
      expect(Pgbus.logger).to have_received(:debug)
    end

    context "when the source_queue column has not been migrated yet" do
      let(:source_column) { false }

      it "reads the row by queue and msg_id" do
        allow(Pgbus.logger).to receive(:warn)
        allow(mock_connection).to receive(:select_one).and_return(row)

        described_class.last_error(queue_name: "default", msg_id: 42, source_queue: "pgbus_default")

        expect(mock_connection).to have_received(:select_one).with(
          a_string_ending_with("FROM pgbus_failed_events WHERE queue_name = $1 AND msg_id = $2 LIMIT 1"),
          "FailedEvent Last Error", ["default", 42]
        )
      end
    end
  end

  describe ".clear!" do
    # A sibling sub-queue's row with the same msg_id stays; a row written
    # before source_queue existed is the message's too and goes with it.
    it "deletes the message's own row and any written without source_queue" do
      allow(mock_connection).to receive(:exec_delete)

      described_class.clear!(queue_name: "default", msg_id: 42, source_queue: "pgbus_default_p1")

      expect(mock_connection).to have_received(:exec_delete).with(
        "DELETE FROM pgbus_failed_events WHERE queue_name = $1 AND msg_id = $2 " \
        "AND (source_queue = $3 OR source_queue IS NULL)",
        "FailedEvent Clear",
        ["default", 42, "pgbus_default_p1"]
      )
    end

    it "does not raise on database errors" do
      allow(mock_connection).to receive(:exec_delete).and_raise(ActiveRecord::StatementInvalid, "table missing")

      expect do
        described_class.clear!(queue_name: "default", msg_id: 42, source_queue: "pgbus_default")
      end.not_to raise_error
    end

    context "when the source_queue column has not been migrated yet" do
      let(:source_column) { false }

      it "deletes by queue and msg_id" do
        allow(Pgbus.logger).to receive(:warn)
        allow(mock_connection).to receive(:exec_delete)

        described_class.clear!(queue_name: "default", msg_id: 42, source_queue: "pgbus_default")

        expect(mock_connection).to have_received(:exec_delete).with(
          "DELETE FROM pgbus_failed_events WHERE queue_name = $1 AND msg_id = $2", "FailedEvent Clear", ["default", 42]
        )
      end
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
