# frozen_string_literal: true

require_relative "../integration_helper"
require "active_job"

# Issues #582 and #583: under priority routing a message sits in a `_pN`
# sub-queue, never in the logical queue's base table, and each sub-queue
# numbers its messages from its own sequence. The executor's failure path
# has to act on the sub-queue it read the message from: the retry backoff
# sets that table's vt, and the failed-event row is keyed on it, so two jobs
# failing with the same msg_id in two sub-queues keep a row each.
RSpec.describe "Executor failure path under priority routing (integration)", :integration do
  let(:queue) { "prio_executor_failures" }
  let(:client) { Pgbus.client }
  let(:executor) { Pgbus::ActiveJob::Executor.new(client: client) }
  let(:data_source) { Pgbus::Web::DataSource.new(client: client) }
  let(:conn) { ActiveRecord::Base.connection }
  let(:msg_id) { 9_100_001 }
  let(:failing) { Set.new }
  let(:job_class) do
    failures = failing
    Class.new(ActiveJob::Base) do
      self.queue_adapter = :inline
      def self.name = "ExecutorPriorityFailureSpec::FlakyJob"
      define_method(:perform) { |label| raise "boom #{label}" if failures.include?(label) }
    end
  end

  around do |example|
    original = Pgbus.configuration.priority_levels
    Pgbus.configuration.priority_levels = 3
    Pgbus.reset_client!
    example.run
  ensure
    Pgbus.client.physical_queue_names(queue).each { |name| Pgbus.client.drop_queue(name, prefixed: false) }
    Pgbus.configuration.priority_levels = original
    Pgbus.reset_client!
  end

  before do
    ActiveJob::Base.logger = Logger.new(IO::NULL)
    stub_const("ExecutorPriorityFailureSpec", Module.new)
    stub_const("ExecutorPriorityFailureSpec::FlakyJob", job_class)
    client.ensure_queue(queue)
  end

  def physical(level)
    client.physical_queue_names(queue).fetch(level)
  end

  # A job's message with a chosen msg_id, the way two sub-queues' sequences
  # collide. Visible now, so the next read claims it.
  def plant(level, label)
    conn.exec_insert(
      "INSERT INTO pgmq.q_#{physical(level)} (msg_id, vt, message) OVERRIDING SYSTEM VALUE " \
      "VALUES ($1, NOW(), $2::jsonb)",
      "spec", [msg_id, JSON.generate(job_class.new(label).serialize)]
    )
  end

  # Reads every visible message and runs each through the executor, the way
  # the worker hands it over: logical queue plus the sub-queue it came from.
  def run_visible(vt: 30)
    client.read_batch_prioritized(queue, qty: 10, vt: vt).map do |source, message|
      [source, executor.execute(message, queue, source_queue: source)]
    end
  end

  def release(level)
    client.set_visibility_timeout(physical(level), msg_id, vt: 0, prefixed: false)
  end

  def vt_of(level)
    Time.parse(conn.select_value("SELECT vt FROM pgmq.q_#{physical(level)} WHERE msg_id = $1", "spec", [msg_id]).to_s)
  end

  def failed_rows
    conn.select_all(
      "SELECT source_queue, payload->'arguments'->>0 AS label, error_message FROM pgbus_failed_events " \
      "WHERE queue_name = $1 ORDER BY source_queue", "spec", [queue]
    ).to_a
  end

  describe "retry backoff (#582)" do
    it "pushes the failed message's vt out in the sub-queue it was read from" do
      failing << "a"
      plant(1, "a")
      run_visible
      release(1)

      expect(run_visible(vt: 1)).to eq([[physical(1), :failed]])

      # read_ct 2 is the first retry: RetryBackoff's base delay (~5s), not the read's 1s.
      expect(vt_of(1)).to be > Time.current + 3
    end
  end

  describe "failed-event rows (#583)" do
    before do
      failing.merge(%w[a b])
      plant(0, "a")
      plant(1, "b")
    end

    it "keeps one row per sub-queue, each with its own job and error" do
      expect(run_visible).to contain_exactly([physical(0), :failed], [physical(1), :failed])

      expect(failed_rows).to eq([
                                  { "source_queue" => physical(0), "label" => "a", "error_message" => "boom a" },
                                  { "source_queue" => physical(1), "label" => "b", "error_message" => "boom b" }
                                ])
    end

    it "clears only the row of the sub-queue whose job succeeded" do
      run_visible
      failing.delete("a")
      release(0)

      expect(run_visible).to eq([[physical(0), :success]])

      expect(failed_rows.map { |row| row["source_queue"] }).to eq([physical(1)])
    end

    it "shows each message on the Jobs page with its own error, neither as an orphan" do
      run_visible

      rows = data_source.job_rows(queues: client.physical_queue_names(queue)).select { |r| r[:msg_id] == msg_id }

      expect(rows.map { |r| [r[:source], r[:queue_name], r[:error_message]] }).to contain_exactly(
        ["queue", physical(0), "boom a"], ["queue", physical(1), "boom b"]
      )
    end

    it "shows a row as an orphan once its own sub-queue's message is gone" do
      run_visible
      client.delete_message(physical(1), msg_id, prefixed: false)

      rows = data_source.job_rows(queues: client.physical_queue_names(queue)).select { |r| r[:msg_id] == msg_id }

      expect(rows.map { |r| [r[:source], r[:error_message]] }).to contain_exactly(
        ["queue", "boom a"], ["failed", "boom b"]
      )
    end

    it "discards one job's failure without touching the other's message or row" do
      run_visible
      row_b = data_source.failed_events.find { |e| e["source_queue"] == physical(1) }

      expect(data_source.discard_failed_event(row_b["id"])).to be(true)

      expect(conn.select_value("SELECT 1 FROM pgmq.q_#{physical(1)} WHERE msg_id = $1", "spec", [msg_id])).to be_nil
      expect(conn.select_value("SELECT 1 FROM pgmq.q_#{physical(0)} WHERE msg_id = $1", "spec", [msg_id])).to eq(1)
      expect(failed_rows.map { |row| row["label"] }).to eq(["a"])
    end
  end
end
