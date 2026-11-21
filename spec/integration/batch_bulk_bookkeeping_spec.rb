# frozen_string_literal: true

require_relative "../integration_helper"
require "active_job"
require "active_job/queue_adapters/pgbus_adapter"

# Every statement the pg driver runs on PGMQ's own connection while recording
# is on (sql.active_record never sees that connection): the SQL of an exec /
# exec_params, the statement name of an exec_prepared.
module BatchBulkSendRecorder
  def self.events = Thread.current[:batch_bulk_sends]

  %i[exec async_exec exec_params async_exec_params].each do |method|
    define_method(method) do |sql, *args, **kwargs, &block|
      BatchBulkSendRecorder.events&.push(sql.to_s)
      super(sql, *args, **kwargs, &block)
    end
  end

  %i[exec_prepared async_exec_prepared].each do |method|
    define_method(method) do |name, *args, **kwargs, &block|
      BatchBulkSendRecorder.events&.push("prepared:#{name}")
      super(name, *args, **kwargs, &block)
    end
  end
end

# Issue #683: a batch's per-job bookkeeping in a constant number of
# statements — one execution-row INSERT per track_enqueue, one backfill
# UPDATE per send group, one DELETE per failed group — and Pgbus.batch
# coalescing its block's perform_later calls into one send per group.
RSpec.describe "Batch bulk bookkeeping (integration)", :integration do
  let(:client) { Pgbus.client }
  let(:work_queue) { "bulk_book_work" }
  let(:callback_queue) { "bulk_book_callbacks" }
  let(:executor) { Pgbus::ActiveJob::Executor.new(client: client) }

  let(:work_job) do
    queue = work_queue
    Class.new(ActiveJob::Base) do
      include Pgbus::ActiveJob::BatchId

      self.queue_adapter = :pgbus
      queue_as(queue)
      def self.name = "BulkBookSpec::WorkJob"
      def perform(*); end
    end
  end

  let(:callback_job) do
    queue = callback_queue
    Class.new(ActiveJob::Base) do
      self.queue_adapter = :pgbus
      queue_as(queue)
      def self.name = "BulkBookSpec::DoneJob"
      def perform(*); end
    end
  end
  # A batch is built (status "building", its block coalesced) only by a
  # process with a heartbeat row (issue #683), as a pgbus worker has.
  let(:owner) do
    Pgbus::ProcessEntry.create!(kind: "worker", hostname: "spec-host", pid: Process.pid, metadata: {},
                                last_heartbeat_at: Time.current)
  end

  before(:all) { PG::Connection.prepend(BatchBulkSendRecorder) unless PG::Connection <= BatchBulkSendRecorder }

  after { Pgbus::Process::Heartbeat.note_deregistered(owner.id) }

  before do
    ActiveJob::Base.logger = Logger.new(IO::NULL)
    Pgbus::Batch.reset_executions_migrated_cache!
    Pgbus::Process::Heartbeat.note_registered(owner.id)
    stub_const("BulkBookSpec", Module.new)
    stub_const("BulkBookSpec::WorkJob", work_job)
    stub_const("BulkBookSpec::DoneJob", callback_job)
    client.ensure_queue(work_queue)
    client.ensure_queue(callback_queue)
    # Warm the queue's native statements and the batch schema probe, so the
    # measured blocks run only their own statements.
    client.send_batch(work_queue, [{ "warm" => true }])
    client.purge_queue(work_queue)
    Pgbus::Batch.executions_migrated?
  end

  # [ActiveRecord statements (name, sql), PGMQ send statements] the block ran.
  def statements_during(&block)
    sql = []
    callback = ->(*, payload) { sql << [payload[:name], payload[:sql]] unless payload[:name] == "SCHEMA" }
    Thread.current[:batch_bulk_sends] = []
    ActiveSupport::Notifications.subscribed(callback, "sql.active_record", &block)
    sends = Thread.current[:batch_bulk_sends].grep(/\Aprepared:pgbus_send|INSERT INTO pgmq\.|pgmq\.send/)
    [sql, sends]
  ensure
    Thread.current[:batch_bulk_sends] = nil
  end

  def execution_inserts(sql) = sql.count { |_, s| s.match?(/\AINSERT INTO "?pgbus_batch_executions/i) }
  def execution_updates(sql) = sql.count { |_, s| s.match?(/\AUPDATE "?pgbus_batch_executions/i) }
  def execution_deletes(sql) = sql.count { |_, s| s.match?(/\ADELETE FROM "?pgbus_batch_executions/i) }
  def batch_updates(sql) = sql.count { |_, s| s.match?(/\AUPDATE "?pgbus_batches/i) }

  def open_batch(**)
    Pgbus::BatchEntry.create!(batch_id: SecureRandom.uuid, status: "pending", properties: "{}", **)
  end

  def payloads_for(batch_id, count)
    Array.new(count) do
      { "job_id" => SecureRandom.uuid, "queue_name" => work_queue, Pgbus::Batch::METADATA_KEY => batch_id }
    end
  end

  def queue_depth(queue)
    physical = Pgbus.configuration.queue_name(queue)
    ActiveRecord::Base.connection.select_value("SELECT count(*) FROM pgmq.q_#{physical}").to_i
  end

  def run_all
    while (message = client.read_message(work_queue, vt: 30))
      executor.execute(message, work_queue)
    end
  end

  def invariant_holds?(batch_id)
    record = Pgbus::BatchEntry.find_by(batch_id: batch_id)
    outstanding = Pgbus::BatchExecution.where(batch_id: batch_id).count
    record.total_jobs == outstanding + record.completed_jobs + record.failed_jobs
  end

  describe "Batch.track_enqueue" do
    it "inserts 500 execution rows in one statement and keeps the invariant" do
      batch = open_batch

      sql, = statements_during { Pgbus::Batch.track_enqueue(payloads_for(batch.batch_id, 500)) }

      expect(execution_inserts(sql)).to eq(1)
      expect(batch.reload.total_jobs).to eq(500)
      expect(Pgbus::BatchExecution.where(batch_id: batch.batch_id).count).to eq(500)
      expect(invariant_holds?(batch.batch_id)).to be(true)
    end

    it "still raises AlreadyFinished for a finished batch, inserting nothing" do
      batch = open_batch(status: "finished")

      expect { Pgbus::Batch.track_enqueue(payloads_for(batch.batch_id, 3)) }.to raise_error(Pgbus::Batch::AlreadyFinished)
      expect(Pgbus::BatchExecution.where(batch_id: batch.batch_id).count).to eq(0)
    end

    it "keeps one row for a job id already counted (ON CONFLICT DO NOTHING)" do
      batch = open_batch
      payloads = payloads_for(batch.batch_id, 2)
      Pgbus::Batch.track_enqueue(payloads)

      Pgbus::Batch.track_retry(payloads.first)

      expect(Pgbus::BatchExecution.where(job_id: payloads.first["job_id"]).count).to eq(1)
    end
  end

  describe "the bulk send of 500 batch members" do
    it "backfills every execution row's msg_id in one UPDATE per group" do
      batch = nil

      sql, sends = statements_during do
        batch = Pgbus.batch { |b| b.add(*Array.new(500) { |i| work_job.new(i) }) }
      end

      expect(sends.size).to eq(1)
      expect(execution_updates(sql)).to eq(1)
      expect(Pgbus::BatchExecution.where(batch_id: batch.batch_id, msg_id: nil).count).to eq(0)
      expect(batch.reload.total_jobs).to eq(500)
    end

    it "leaves the messages live and raises nothing when the backfill fails" do
      allow(Pgbus::BatchExecution).to receive(:backfill_all!).and_raise(ActiveRecord::StatementInvalid, "backfill down")
      allow(Pgbus::BatchExecution).to receive(:backfill!).and_raise(ActiveRecord::StatementInvalid, "backfill down")
      batch = nil

      expect { batch = Pgbus.batch { |b| b.add(*Array.new(5) { |i| work_job.new(i) }) } }.not_to raise_error

      expect(queue_depth(work_queue)).to eq(5)
      expect(batch.reload.total_jobs).to eq(5)
    end
  end

  describe "a failed bulk send" do
    it "uncounts the whole group with one DELETE and one decrement" do
      batch = open_batch(status: "processing")
      Pgbus::BatchEntry.where(batch_id: batch.batch_id).update_all(total_jobs: 0)
      jobs = Array.new(4) { |i| work_job.new(i) }
      Pgbus::ActiveJob::BatchId.capture(jobs, batch.batch_id)
      allow(client).to receive(:send_batch).and_return([])
      allow(Pgbus).to receive(:client).and_return(client)

      sql, = statements_during do
        expect { Pgbus::ActiveJob::Adapter.new.enqueue_all(jobs) }.to raise_error(Pgbus::EnqueueError)
      end

      expect(execution_deletes(sql)).to eq(1)
      expect(batch_updates(sql)).to eq(2) # the count in, and one decrement for all four
      expect(batch.reload.total_jobs).to eq(0)
      expect(Pgbus::BatchExecution.where(batch_id: batch.batch_id).count).to eq(0)
    end
  end

  describe "perform_later inside Pgbus.batch" do
    it "sends 200 members as one send_batch with one execution insert, and fires on_complete after all 200" do
      batch = nil

      sql, sends = statements_during do
        batch = Pgbus.batch do |b|
          b.on_complete(callback_job)
          200.times { |i| work_job.perform_later(i) }
        end
      end

      expect(sends.size).to eq(1)
      expect(execution_inserts(sql)).to eq(1)
      expect(batch.reload.total_jobs).to eq(200)
      expect(queue_depth(callback_queue)).to eq(0)

      run_all

      expect(batch.reload).to have_attributes(status: "finished", completed_jobs: 200)
      expect(queue_depth(callback_queue)).to eq(1)
    end

    it "flushes 120 jobs in 3 chunks with coalesce_chunk_size 50" do
      Pgbus.configuration.coalesce_chunk_size = 50
      batch = nil

      _, sends = statements_during do
        batch = Pgbus.batch { 120.times { |i| work_job.perform_later(i) } }
      end

      expect(sends.size).to eq(3)
      expect(batch.reload.total_jobs).to eq(120)
      expect(queue_depth(work_queue)).to eq(120)
    ensure
      Pgbus.configuration.coalesce_chunk_size = 1_000
    end

    it "keeps chunk 1 live and counted when chunk 2 fails, then applies on_enqueue_failure" do
      Pgbus.configuration.coalesce_chunk_size = 50
      calls = 0
      allow(client).to receive(:send_batch).and_wrap_original do |original, *args, **kwargs|
        calls += 1
        raise PG::ConnectionBad, "chunk 2 lost" if calls == 2

        original.call(*args, **kwargs)
      end
      allow(client).to receive(:send_message).and_raise(PG::ConnectionBad, "still lost")
      allow(Pgbus).to receive(:client).and_return(client)
      failed = []

      batch = Pgbus.batch(on_enqueue_failure: ->(jobs, _error) { failed.concat(jobs) }) do
        120.times { |i| work_job.perform_later(i) }
      end

      expect(failed.size).to eq(50)
      expect(queue_depth(work_queue)).to eq(70)
      expect(Pgbus::BatchExecution.where(batch_id: batch.batch_id).where.not(msg_id: nil).count).to be >= 70
      expect(invariant_holds?(batch.batch_id)).to be(true)
    ensure
      Pgbus.configuration.coalesce_chunk_size = 1_000
    end

    it "raises Pgbus::EnqueueError by default when a member cannot be sent" do
      allow(client).to receive(:send_batch).and_raise(PG::ConnectionBad, "lost")
      allow(client).to receive(:send_message).and_raise(PG::ConnectionBad, "lost")
      allow(Pgbus).to receive(:client).and_return(client)

      expect { Pgbus.batch { work_job.perform_later(1) } }.to raise_error(Pgbus::EnqueueError)
    end

    it "keeps the invariant and finishes when a transaction in the block rolls back around the first member" do
      batch = Pgbus.batch do
        ActiveRecord::Base.transaction do
          work_job.perform_later(1)
          raise ActiveRecord::Rollback
        end
        work_job.perform_later(2)
      end

      expect(invariant_holds?(batch.batch_id)).to be(true)

      run_all

      expect(batch.reload).to have_attributes(status: "finished", completed_jobs: 2)
    end

    it "re-counts the members a short send_batch left uncounted, once each" do
      allow(client).to receive(:send_batch).and_return([])
      allow(Pgbus).to receive(:client).and_return(client)

      batch = Pgbus.batch { 3.times { |i| work_job.perform_later(i) } }

      expect(batch.reload.total_jobs).to eq(3)
      expect(Pgbus::BatchExecution.where(batch_id: batch.batch_id).count).to eq(3)
      expect(Pgbus::BatchExecution.where(batch_id: batch.batch_id, msg_id: nil).count).to eq(0)
      expect(queue_depth(work_queue)).to eq(3)
    end

    it "uncounts a re-counted member whose single send fails before any message exists" do
      allow(client).to receive(:send_batch).and_return([])
      allow(client).to receive(:send_message).and_raise(IOError, "not sent")
      allow(Pgbus).to receive(:client).and_return(client)
      failed = []

      batch = Pgbus.batch(on_enqueue_failure: ->(jobs, _error) { failed.concat(jobs) }) do
        3.times { |i| work_job.perform_later(i) }
      end

      expect(failed.size).to eq(3)
      expect(batch.reload.total_jobs).to eq(0)
      expect(Pgbus::BatchExecution.where(batch_id: batch.batch_id).count).to eq(0)
    end

    it "fails the held members with AlreadyFinished when the build was closed under it" do
      batch = Pgbus::Batch.new
      error = nil
      begin
        batch.enqueue do
          work_job.perform_later(1)
          Pgbus::BatchEntry.where(batch_id: batch.batch_id).update_all(status: "finished")
        end
      rescue Pgbus::EnqueueError => e
        error = e
      end

      expect(error.cause).to be_a(Pgbus::Batch::AlreadyFinished)
      expect(queue_depth(work_queue)).to eq(0)
    end

    it "keeps its members out of an outer Pgbus.coalesce scope and sends them at the batch block's end" do
      depth_after_batch = nil
      batch = nil

      Pgbus.coalesce do
        batch = Pgbus.batch { 3.times { |i| work_job.perform_later(i) } }
        depth_after_batch = queue_depth(work_queue)
      end

      expect(depth_after_batch).to eq(3)
      expect(batch.reload).to have_attributes(total_jobs: 3, status: "processing")
    end

    it "runs a constant number of statements for 10, 100 and 1 000 members" do
      counts = [10, 100, 1_000].map do |n|
        sql, sends = statements_during { Pgbus.batch { n.times { |i| work_job.perform_later(i) } } }
        client.purge_queue(work_queue)
        sql.size + sends.size
      end

      expect(counts.uniq.size).to eq(1), "statement counts #{counts.inspect} grow with N"
    end
  end
end
