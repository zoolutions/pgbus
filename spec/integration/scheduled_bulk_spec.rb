# frozen_string_literal: true

require_relative "../integration_helper"
require "active_job"
require "active_job/queue_adapters/pgbus_adapter"
require "active_support/testing/time_helpers"

# Pgbus counts its sends on the driver: the prepared native statements and the
# pgmq functions, on PGMQ's own connection.
module ScheduledBulkSendRecorder
  def self.events = Thread.current[:scheduled_bulk_sends]

  def exec_prepared(name, *args, **kwargs, &)
    ScheduledBulkSendRecorder.events&.push(name.to_s)
    super
  end

  def exec_params(sql, *args, **kwargs, &)
    ScheduledBulkSendRecorder.events&.push(sql.to_s)
    super
  end
end

# Issue #686: jobs scheduled in the future go out in bulk, one send_batch per
# (queue, priority, delay) group, in enqueue_all, Pgbus.coalesce and a built
# batch's block, instead of one send each.
RSpec.describe "Bulk scheduled sends (integration)", :integration do
  include ActiveSupport::Testing::TimeHelpers

  let(:client) { Pgbus.client }
  let(:work_queue) { "scheduled_bulk_work" }
  let(:callback_queue) { "scheduled_bulk_callbacks" }
  let(:executor) { Pgbus::ActiveJob::Executor.new(client: client) }
  let(:owner) do
    Pgbus::ProcessEntry.create!(kind: "worker", hostname: "spec-host", pid: Process.pid, metadata: {},
                                last_heartbeat_at: Time.current)
  end

  let(:work_job) do
    queue = work_queue
    Class.new(ActiveJob::Base) do
      include Pgbus::ActiveJob::BatchId

      self.queue_adapter = :pgbus
      queue_as(queue)
      def self.name = "ScheduledBulkSpec::WorkJob"
      def perform(*); end
    end
  end

  let(:unique_job) do
    queue = work_queue
    Class.new(ActiveJob::Base) do
      include Pgbus::ActiveJob::BatchId
      include Pgbus::Uniqueness

      self.queue_adapter = :pgbus
      queue_as(queue)
      def self.name = "ScheduledBulkSpec::UniqueJob"
      def perform(*); end

      ensures_uniqueness strategy: :until_executed, key: ->(id) { "scheduled-bulk-#{id}" }
    end
  end

  let(:limited_job) do
    queue = work_queue
    Class.new(ActiveJob::Base) do
      include Pgbus::ActiveJob::BatchId
      include Pgbus::Concurrency

      self.queue_adapter = :pgbus
      queue_as(queue)
      def self.name = "ScheduledBulkSpec::LimitedJob"
      def perform(*); end

      limits_concurrency to: 2, key: ->(*) { "scheduled-bulk-limit" }, duration: 60
    end
  end

  let(:callback_job) do
    queue = callback_queue
    Class.new(ActiveJob::Base) do
      self.queue_adapter = :pgbus
      queue_as(queue)
      def self.name = "ScheduledBulkSpec::DoneJob"
      def perform(*); end
    end
  end

  before(:all) { PG::Connection.prepend(ScheduledBulkSendRecorder) unless PG::Connection <= ScheduledBulkSendRecorder }

  before do
    ActiveJob::Base.logger = Logger.new(IO::NULL)
    Pgbus::Batch.reset_executions_migrated_cache!
    stub_const("ScheduledBulkSpec", Module.new)
    stub_const("ScheduledBulkSpec::WorkJob", work_job)
    stub_const("ScheduledBulkSpec::UniqueJob", unique_job)
    stub_const("ScheduledBulkSpec::LimitedJob", limited_job)
    stub_const("ScheduledBulkSpec::DoneJob", callback_job)
    client.ensure_queue(work_queue)
    client.ensure_queue(callback_queue)
    client.send_batch(work_queue, [{ "warm" => true }])
    client.send_message(work_queue, { "warm" => true })
    client.purge_queue(work_queue)
  end

  after { travel_back }

  def sends
    Thread.current[:scheduled_bulk_sends] = []
    yield
    names = Thread.current[:scheduled_bulk_sends]
    batch = ->(n) { n.start_with?("pgbus_send_batch_") || n.include?("unnest($1::jsonb[])") || n.include?("pgmq.send_batch(") }
    single = ->(n) { n.match?(/\Apgbus_send_\h{12}\z/) || n.include?("VALUES (clock_timestamp()") || n.include?("pgmq.send(") }
    { send_batch: names.count(&batch), send: names.count(&single) }
  ensure
    Thread.current[:scheduled_bulk_sends] = nil
  end

  # Seconds until each message on the work queue becomes visible, sorted.
  def visible_in
    physical = Pgbus.configuration.queue_name(work_queue)
    ActiveRecord::Base.connection
                      .select_values("SELECT EXTRACT(EPOCH FROM (vt - clock_timestamp())) FROM pgmq.q_#{physical}")
                      .map(&:to_f).sort
  end

  it "sends a staggered fan-out (3 slices at 0, 10 and 20 s) as exactly 3 send_batch calls" do
    jobs = [0, 10, 20].flat_map { |wait| Array.new(10) { |i| work_job.new(i).set(wait: wait) } }

    counted = sends { ActiveJob.perform_all_later(jobs) }

    expect(counted).to eq(send_batch: 3, send: 0)
    expected = [0, 10, 20].flat_map { |wait| [wait] * 10 }
    visible_in.zip(expected).each { |seconds, wait| expect(seconds).to be_within(1).of(wait) }
  end

  it "sends 50 jobs that share one wait: as one send_batch" do
    jobs = Array.new(50) { |i| work_job.new(i).set(wait: 30) }

    counted = sends { ActiveJob.perform_all_later(jobs) }

    expect(counted).to eq(send_batch: 1, send: 0)
    expect(visible_in.size).to eq(50)
    expect(visible_in).to all(be_within(1).of(30))
  end

  it "holds a future job in Pgbus.coalesce and sends it with the delay left at the flush" do
    counted = sends do
      Pgbus.coalesce do
        work_job.set(wait: 60).perform_later(1)
        travel 20.seconds
      end
    end

    expect(counted).to eq(send_batch: 1, send: 0)
    # The message's clock is the database's, which did not travel: the delay
    # sent is what was left at the flush (40 s), not the original 60 s.
    expect(visible_in.first).to be_within(1).of(40)
  end

  it "keeps the single path for uniqueness and concurrency jobs with wait:, their leases covering the delay" do
    counted = sends do
      ActiveJob.perform_all_later([unique_job.new(1).set(wait: 90), limited_job.new.set(wait: 90)])
    end

    expect(counted).to eq(send_batch: 0, send: 2)
    lock = Pgbus::UniquenessKey.find_by(lock_key: "scheduled-bulk-1")
    expect(lock).to be_present
    slot = Pgbus::Semaphore.find_by(key: "scheduled-bulk-limit")
    expect(slot.expires_at).to be > 90.seconds.from_now
  end

  context "with a built batch" do
    before { Pgbus::Process::Heartbeat.note_registered(owner.id) }
    after { Pgbus::Process::Heartbeat.note_deregistered(owner.id) }

    it "counts and backfills scheduled members in bulk, and fires on_complete after the last one runs" do
      batch = nil
      counted = sends do
        batch = Pgbus.batch do |b|
          b.on_complete(callback_job)
          [1, 2].each { |wait| 5.times { |i| work_job.set(wait: wait).perform_later(i) } }
        end
      end

      expect(counted).to eq(send_batch: 2, send: 0)
      expect(batch.reload.total_jobs).to eq(10)
      expect(Pgbus::BatchExecution.where(batch_id: batch.batch_id, msg_id: nil).count).to eq(0)

      ActiveRecord::Base.connection.execute(
        "UPDATE pgmq.q_#{Pgbus.configuration.queue_name(work_queue)} SET vt = clock_timestamp()"
      )
      while (message = client.read_message(work_queue, vt: 30))
        executor.execute(message, work_queue)
      end

      expect(batch.reload).to have_attributes(status: "finished", completed_jobs: 10)
      physical = Pgbus.configuration.queue_name(callback_queue)
      expect(ActiveRecord::Base.connection.select_value("SELECT count(*) FROM pgmq.q_#{physical}").to_i).to eq(1)
    end

    it "keeps the statement count constant in N for scheduled members" do
      # Ruby's clock held still, so a slow runner cannot split 30 s into
      # 29 s and 30 s groups (that split is right, but not this spec's point).
      freeze_time
      counts = [10, 100, 1_000].map do |n|
        statements = 0
        callback = ->(*, payload) { statements += 1 unless payload[:name] == "SCHEMA" }
        counted = sends do
          ActiveSupport::Notifications.subscribed(callback, "sql.active_record") do
            Pgbus.batch { n.times { |i| work_job.set(wait: 30).perform_later(i) } }
          end
        end
        client.purge_queue(work_queue)
        statements + counted[:send_batch] + counted[:send]
      end

      expect(counts.uniq.size).to eq(1), "statement counts #{counts.inspect} grow with N"
    end
  end
end
