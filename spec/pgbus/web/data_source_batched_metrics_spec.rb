# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Web::DataSource do
  subject(:data_source) { described_class.new(client: mock_client) }

  let(:mock_client) { build_mock_client }
  let(:conn) { double("ActiveRecord::Connection") }

  before do
    allow(data_source).to receive_messages(connection: conn, paused_queue_names: [])
    allow(conn).to receive(:transaction).with(requires_new: true).and_yield
  end

  describe "#queues_with_metrics" do
    it "fetches all queue metrics in a single batched query" do
      allow(conn).to receive(:select_values)
        .with(a_string_matching(/pgmq\.meta/))
        .and_return(%w[pgbus_default pgbus_mailers])

      allow(conn).to receive(:quote) { |v| "'#{v}'" }

      allow(conn).to receive(:select_all)
        .with(anything, "Pgbus Batched Queue Metrics")
        .and_return(double(to_a: [
                             { "queue_name" => "pgbus_default", "queue_length" => "5",
                               "queue_visible_length" => "3", "newest_msg_age_sec" => "10",
                               "oldest_msg_age_sec" => "100", "total_messages" => "500" },
                             { "queue_name" => "pgbus_mailers", "queue_length" => "2",
                               "queue_visible_length" => "2", "newest_msg_age_sec" => "5",
                               "oldest_msg_age_sec" => "50", "total_messages" => "200" }
                           ]))

      result = data_source.queues_with_metrics

      expect(result.length).to eq(2)
      expect(result.first[:name]).to eq("pgbus_default")
      expect(result.first[:queue_length]).to eq(5)
      expect(result.last[:name]).to eq("pgbus_mailers")
      expect(result.last[:total_messages]).to eq(200)
    end

    it "selects max(read_ct) and exposes it as :max_read_ct" do
      allow(conn).to receive(:select_values)
        .with(a_string_matching(/pgmq\.meta/))
        .and_return(%w[pgbus_default])

      allow(conn).to receive(:quote) { |v| "'#{v}'" }

      captured_sql = nil
      allow(conn).to receive(:select_all) do |sql, _label|
        captured_sql = sql
        double(to_a: [{ "queue_name" => "pgbus_default", "queue_length" => "5",
                        "queue_visible_length" => "3", "newest_msg_age_sec" => "10",
                        "oldest_msg_age_sec" => "100", "total_messages" => "500",
                        "max_read_ct" => "2", "visible_unread_length" => "0" }])
      end

      result = data_source.queues_with_metrics

      expect(captured_sql).to include("max(read_ct)")
      expect(captured_sql).to include("max_read_ct")
      expect(result.first[:max_read_ct]).to eq(2)
    end

    it "counts visible unread (read_ct=0) messages, scoped to visible rows only" do
      allow(conn).to receive(:select_values)
        .with(a_string_matching(/pgmq\.meta/))
        .and_return(%w[pgbus_default])

      allow(conn).to receive(:quote) { |v| "'#{v}'" }

      captured_sql = nil
      allow(conn).to receive(:select_all) do |sql, _label|
        captured_sql = sql
        double(to_a: [{ "queue_name" => "pgbus_default", "queue_length" => "5",
                        "queue_visible_length" => "5", "newest_msg_age_sec" => "10",
                        "oldest_msg_age_sec" => "100", "total_messages" => "500",
                        "max_read_ct" => "3", "visible_unread_length" => "4" }])
      end

      result = data_source.queues_with_metrics

      # The wedge signal must count only claimable (visible) never-claimed rows,
      # so an in-flight or retried message (read_ct>0, or invisible) cannot veto
      # the signal for a pile of visible read_ct=0 jobs (issue #367 review).
      expect(captured_sql).to include("read_ct = 0")
      expect(captured_sql).to include("vt <= NOW()")
      expect(captured_sql).to include("visible_unread_length")
      expect(result.first[:visible_unread_length]).to eq(4)
    end

    it "selects a vt-aware oldest_claimable_age_sec scoped to claimable rows" do
      allow(conn).to receive(:select_values)
        .with(a_string_matching(/pgmq\.meta/))
        .and_return(%w[pgbus_default])

      allow(conn).to receive(:quote) { |v| "'#{v}'" }

      captured_sql = nil
      allow(conn).to receive(:select_all) do |sql, _label|
        captured_sql = sql
        double(to_a: [{ "queue_name" => "pgbus_default", "queue_length" => "5",
                        "queue_visible_length" => "3", "newest_msg_age_sec" => "10",
                        "oldest_msg_age_sec" => "100", "total_messages" => "500",
                        "max_read_ct" => "2", "visible_unread_length" => "0",
                        "oldest_claimable_age_sec" => "42" }])
      end

      result = data_source.queues_with_metrics

      # Age of the oldest message eligible for pickup: min(vt) over claimable
      # rows, so a scheduled/backoff-parked message (future vt) contributes
      # nothing until it comes due (issue #389).
      expect(captured_sql).to include("min(vt)")
      expect(captured_sql).to include("oldest_claimable_age_sec")
      expect(result.first[:oldest_claimable_age_sec]).to eq(42)
      expect(result.first[:parked_length]).to eq(2)
    end

    it "selects oldest_unread_claimable_age_sec from min(vt) over visible read_ct=0 rows (issue #589)" do
      allow(conn).to receive(:select_values)
        .with(a_string_matching(/pgmq\.meta/))
        .and_return(%w[pgbus_default])

      allow(conn).to receive(:quote) { |v| "'#{v}'" }

      captured_sql = nil
      allow(conn).to receive(:select_all) do |sql, _label|
        captured_sql = sql
        double(to_a: [{ "queue_name" => "pgbus_default", "queue_length" => "5",
                        "queue_visible_length" => "3", "newest_msg_age_sec" => "10",
                        "oldest_msg_age_sec" => "100", "total_messages" => "500",
                        "max_read_ct" => "2", "visible_unread_length" => "1",
                        "oldest_claimable_age_sec" => "42", "oldest_unread_claimable_age_sec" => "7" }])
      end

      result = data_source.queues_with_metrics

      # min(vt), not min(enqueued_at): a delayed job's age starts when its wait
      # expires. read_ct = 0 in the same aggregate: a retried message on its
      # backoff must not make a fresh burst look old.
      expect(captured_sql).to include(
        "EXTRACT(epoch FROM (NOW() - min(vt) FILTER (WHERE vt <= NOW() AND read_ct = 0)))::int " \
        "AS oldest_unread_claimable_age_sec"
      )
      expect(result.first[:oldest_unread_claimable_age_sec]).to eq(7)
      expect(result.first[:oldest_claimable_age_sec]).to eq(42)
    end

    it "maps a NULL oldest_unread_claimable_age_sec (no visible unread row) to nil" do
      allow(conn).to receive(:select_values)
        .with(a_string_matching(/pgmq\.meta/))
        .and_return(%w[pgbus_default])

      allow(conn).to receive(:quote) { |v| "'#{v}'" }

      allow(conn).to receive(:select_all)
        .with(anything, "Pgbus Batched Queue Metrics")
        .and_return(double(to_a: [{ "queue_name" => "pgbus_default", "queue_length" => "1",
                                    "queue_visible_length" => "1", "newest_msg_age_sec" => "9",
                                    "oldest_msg_age_sec" => "9", "total_messages" => "1",
                                    "max_read_ct" => "1", "visible_unread_length" => "0",
                                    "oldest_claimable_age_sec" => "9", "oldest_unread_claimable_age_sec" => nil }]))

      expect(data_source.queues_with_metrics.first[:oldest_unread_claimable_age_sec]).to be_nil
    end

    it "reports nil claimable age for a queue holding only vt-parked messages" do
      allow(conn).to receive(:select_values)
        .with(a_string_matching(/pgmq\.meta/))
        .and_return(%w[pgbus_default])

      allow(conn).to receive(:quote) { |v| "'#{v}'" }

      # The issue #389 incident: one vt-parked message — an ActiveJob retry
      # re-enqueued with wait:, so read_ct is 0 and vt is hours in the future.
      # The raw age grows at wall-clock rate while nothing is eligible for pickup.
      allow(conn).to receive(:select_all)
        .with(anything, "Pgbus Batched Queue Metrics")
        .and_return(double(to_a: [{ "queue_name" => "pgbus_default", "queue_length" => "1",
                                    "queue_visible_length" => "0", "newest_msg_age_sec" => "17045",
                                    "oldest_msg_age_sec" => "17045", "total_messages" => "500",
                                    "max_read_ct" => "0", "visible_unread_length" => "0",
                                    "oldest_claimable_age_sec" => nil }]))

      result = data_source.queues_with_metrics

      expect(result.first[:oldest_msg_age_sec]).to eq(17_045)
      expect(result.first[:oldest_claimable_age_sec]).to be_nil
    end

    it "maps a NULL max_read_ct (empty queue) to nil, not 0" do
      allow(conn).to receive(:select_values)
        .with(a_string_matching(/pgmq\.meta/))
        .and_return(%w[pgbus_default])

      allow(conn).to receive(:quote) { |v| "'#{v}'" }

      allow(conn).to receive(:select_all)
        .with(anything, "Pgbus Batched Queue Metrics")
        .and_return(double(to_a: [{ "queue_name" => "pgbus_default", "queue_length" => "0",
                                    "queue_visible_length" => "0", "newest_msg_age_sec" => nil,
                                    "oldest_msg_age_sec" => nil, "total_messages" => "0",
                                    "max_read_ct" => nil }]))

      result = data_source.queues_with_metrics

      expect(result.first[:max_read_ct]).to be_nil
    end

    it "handles empty queue list" do
      allow(conn).to receive(:select_values)
        .with(a_string_matching(/pgmq\.meta/))
        .and_return([])

      result = data_source.queues_with_metrics
      expect(result).to eq([])
    end

    it "handles errors gracefully" do
      allow(conn).to receive(:select_values).and_raise(StandardError, "boom")

      result = data_source.queues_with_metrics
      expect(result).to eq([])
    end

    it "does not make N+1 queries" do
      allow(conn).to receive(:select_values)
        .with(a_string_matching(/pgmq\.meta/))
        .and_return(%w[q1 q2 q3])

      allow(conn).to receive(:quote) { |v| "'#{v}'" }

      allow(conn).to receive(:select_all)
        .with(anything, "Pgbus Batched Queue Metrics")
        .and_return(double(to_a: []))

      data_source.queues_with_metrics

      # select_values for queue names + select_all for batched metrics = 2 queries total
      expect(conn).to have_received(:select_values).once
      expect(conn).to have_received(:select_all).once
    end
  end

  # Issue #677: one scan per queue, chunked statements, failure isolation.
  describe "#queues_with_metrics at scale" do
    def metrics_row(name, length: 1)
      { "queue_name" => name, "queue_length" => length.to_s, "queue_visible_length" => length.to_s,
        "newest_msg_age_sec" => "3", "oldest_msg_age_sec" => "9", "total_messages" => "40",
        "max_read_ct" => "0", "visible_unread_length" => length.to_s,
        "oldest_claimable_age_sec" => "9", "oldest_unread_claimable_age_sec" => "9" }
    end

    # Answers each chunk statement with a row per queue it names, in reverse
    # order (the hashes still come back in pgmq.meta order), and raises for a
    # chunk that names any queue in +failing+.
    def answer_chunks(failing: [])
      statements = []
      allow(conn).to receive(:select_all).with(anything, "Pgbus Batched Queue Metrics") do |sql, _label|
        statements << sql
        names = sql.scan(/'([^']+)' AS queue_name/).flatten
        raise ActiveRecord::QueryCanceled, "canceling statement due to statement timeout" if names.intersect?(failing)

        double(to_a: names.reverse.map { |n| metrics_row(n) })
      end
      statements
    end

    before do
      allow(conn).to receive(:quote) { |v| "'#{v}'" }
      allow(conn).to receive(:select_values).with(a_string_matching(/pgmq\.meta/))
                                            .and_return(%w[q1 q2 q3 q4 q5])
      allow(Pgbus.configuration).to receive(:web_queue_metrics_chunk_size).and_return(2)
    end

    it "scans each queue table once per read, not once per metric" do
      allow(conn).to receive(:select_values).with(a_string_matching(/pgmq\.meta/)).and_return(%w[q1])
      statements = answer_chunks

      data_source.queues_with_metrics

      expect(statements.first.scan(/pgmq\.q_q1(?!_msg_id_seq)\b/).size).to eq(1)
      expect(statements.first.scan("pgmq.q_q1_msg_id_seq").size).to eq(1)
    end

    it "reads web_queue_metrics_chunk_size queues per statement and reports every queue" do
      statements = answer_chunks

      result = data_source.queues_with_metrics

      expect(statements.size).to eq(3)
      expect(result.map { |q| q[:name] }).to eq(%w[q1 q2 q3 q4 q5])
    end

    it "runs every chunk in its own savepoint" do
      answer_chunks

      data_source.queues_with_metrics

      expect(conn).to have_received(:transaction).with(requires_new: true).exactly(3).times
    end

    it "returns a healthy queue's hash with exactly the keys it had before chunking" do
      answer_chunks

      expect(data_source.queues_with_metrics.first).to eq(
        name: "q1", queue_length: 1, queue_visible_length: 1, parked_length: 0, oldest_msg_age_sec: 9,
        oldest_claimable_age_sec: 9, newest_msg_age_sec: 3, total_messages: 40, max_read_ct: 0,
        visible_unread_length: 1, oldest_unread_claimable_age_sec: 9, paused: false
      )
    end

    it "marks only the failed chunk's queues unavailable, with nil metrics" do
      answer_chunks(failing: %w[q3])

      result = data_source.queues_with_metrics.to_h { |q| [q[:name], q] }

      expect(result.keys).to eq(%w[q1 q2 q3 q4 q5])
      expect(result.values_at("q3", "q4")).to all(include(metrics_unavailable: true, queue_length: nil,
                                                          queue_visible_length: nil, total_messages: nil))
      expect(result.values_at("q1", "q2", "q5")).to all(include(queue_length: 1))
      expect(result.values_at("q1", "q2", "q5").map { |q| q.key?(:metrics_unavailable) }).to eq([false] * 3)
    end

    it "names the failed chunk and its error in queue_metrics_errors" do
      answer_chunks(failing: %w[q3])

      expect(data_source.queue_metrics_errors).to eq(
        [{ queues: %w[q3 q4], error: "ActiveRecord::QueryCanceled: canceling statement due to statement timeout" }]
      )
    end

    it "keeps the paused flag on an unavailable queue" do
      answer_chunks(failing: %w[q1])
      allow(data_source).to receive(:paused_queue_names).and_return(%w[q1])

      expect(data_source.queues_with_metrics.first).to include(name: "q1", metrics_unavailable: true, paused: true)
    end

    it "logs the failed chunk as an error" do
      answer_chunks(failing: %w[q5])
      allow(Pgbus.logger).to receive(:error)

      data_source.queues_with_metrics

      expect(Pgbus.logger).to have_received(:error) do |&block|
        expect(block.call).to include("q5").and include("ActiveRecord::QueryCanceled")
      end
    end

    it "reports no errors when every chunk answered" do
      answer_chunks

      expect(data_source.queue_metrics_errors).to eq([])
    end

    it "reports a failed pgmq.meta read as an error, not as an empty system" do
      allow(conn).to receive(:select_values).and_raise(ActiveRecord::QueryCanceled, "canceling statement")

      expect(data_source.queues_with_metrics).to eq([])
      expect(data_source.queue_metrics_errors).to eq(
        [{ queues: [], error: "ActiveRecord::QueryCanceled: canceling statement" }]
      )
    end

    it "forgets the errors on reset_cache!" do
      answer_chunks(failing: %w[q1])
      data_source.queue_metrics_errors
      answer_chunks

      expect(data_source.reset_cache!.queue_metrics_errors).to eq([])
    end
  end

  describe "#queue_names" do
    before do
      allow(conn).to receive(:select_values).with(a_string_matching(/pgmq\.meta/))
                                            .and_return(%w[pgbus_default pgbus_default_dlq])
      allow(conn).to receive(:select_all)
    end

    it "reads pgmq.meta only, without the metrics statement" do
      expect(data_source.queue_names).to eq(%w[pgbus_default pgbus_default_dlq])
      expect(conn).not_to have_received(:select_all)
    end

    it "is memoized until reset_cache!" do
      2.times { data_source.queue_names }
      data_source.reset_cache!
      data_source.queue_names

      expect(conn).to have_received(:select_values).twice
    end

    it "shares its read with queues_with_metrics" do
      allow(conn).to receive(:quote) { |v| "'#{v}'" }
      allow(conn).to receive(:select_all).and_return(double(to_a: []))

      data_source.queue_names
      data_source.queues_with_metrics

      expect(conn).to have_received(:select_values).once
    end

    it "leaves out a name the queue-name validator rejects, as the metrics read always did" do
      allow(conn).to receive(:select_values).with(a_string_matching(/pgmq\.meta/))
                                            .and_return(["pgbus_default", "x" * 60])

      expect(data_source.queue_names).to eq(%w[pgbus_default])
    end

    it "logs a failed read and answers []" do
      allow(conn).to receive(:select_values).and_raise(StandardError, "boom")
      allow(Pgbus.logger).to receive(:error)

      expect(data_source.queue_names).to eq([])
      expect(Pgbus.logger).to have_received(:error)
    end
  end
end
