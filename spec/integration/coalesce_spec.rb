# frozen_string_literal: true

require_relative "../integration_helper"
require "active_job"
require "active_job/queue_adapters/pgbus_adapter"

# Issue #682: Pgbus.coalesce against a real PGMQ — the sends counted at the
# driver, the commit gate of enqueue_after_transaction_commit, and batch
# members buffered by the scope.

# Every statement the pg driver runs while the block runs: the SQL of an
# exec / exec_params, the statement name of an exec_prepared. PGMQ runs on
# its own connection, which sql.active_record never sees.
module CoalesceStatementRecorder
  def self.events = Thread.current[:coalesce_spec_statements]

  %i[exec async_exec exec_params async_exec_params].each do |method|
    define_method(method) do |sql, *args, **kwargs, &block|
      CoalesceStatementRecorder.events&.push(sql.to_s)
      super(sql, *args, **kwargs, &block)
    end
  end

  %i[exec_prepared async_exec_prepared].each do |method|
    define_method(method) do |name, *args, **kwargs, &block|
      CoalesceStatementRecorder.events&.push("prepared:#{name}")
      super(name, *args, **kwargs, &block)
    end
  end
end

RSpec.describe "Pgbus.coalesce (integration)", :integration do
  let(:client) { Pgbus.client }
  let(:work_queue) { "coalesce_work" }
  let(:callback_queue) { "coalesce_callbacks" }
  let(:executor) { Pgbus::ActiveJob::Executor.new(client: client) }

  let(:event_job) { build_job("EventJob") }
  let(:deferring_job) { build_job("DeferringJob", defer: true) }

  let(:callback_job) do
    queue = callback_queue
    Class.new(ActiveJob::Base) do
      self.queue_adapter = :pgbus
      queue_as(queue)
      def self.name = "CoalesceIntSpec::OnCompleteJob"
      def perform(*); end
    end
  end

  before(:all) { PG::Connection.prepend(CoalesceStatementRecorder) unless PG::Connection <= CoalesceStatementRecorder }

  before do
    ActiveJob::Base.logger = Logger.new(IO::NULL)
    stub_const("CoalesceIntSpec", Module.new)
    stub_const("CoalesceIntSpec::EventJob", event_job)
    # Active Job 7.1 has no enqueue_after_transaction_commit to defer with.
    stub_const("CoalesceIntSpec::DeferringJob", deferring_job) if defined?(ActiveJob::EnqueueAfterTransactionCommit)
    stub_const("CoalesceIntSpec::OnCompleteJob", callback_job)
    client.ensure_queue(work_queue)
    client.ensure_queue(callback_queue)
  end

  def build_job(name, defer: nil)
    queue = work_queue
    Class.new(ActiveJob::Base) do
      if defer
        include ActiveJob::EnqueueAfterTransactionCommit

        self.enqueue_after_transaction_commit = true
      end
      include Pgbus::ActiveJob::BatchId

      self.queue_adapter = :pgbus
      queue_as(queue)
      define_singleton_method(:name) { "CoalesceIntSpec::#{name}" }
      def perform(*); end
    end
  end

  def statements_during
    Thread.current[:coalesce_spec_statements] = []
    yield
    Thread.current[:coalesce_spec_statements]
  ensure
    Thread.current[:coalesce_spec_statements] = nil
  end

  def sends(statements)
    batch = statements.count { |s| s.match?(/\Aprepared:pgbus_send_batch_|unnest\(\$1::jsonb\[\]\)|pgmq\.send_batch\(/) }
    single = statements.count { |s| s.match?(/\Aprepared:pgbus_send_\h{12}\z|VALUES \(clock_timestamp|pgmq\.send\(/) }
    { send_batch: batch, send: single }
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

  it "sends three perform_later calls as exactly one send_batch, and all three messages exist after" do
    # Warm the queue's native statements so the measured block runs only its sends.
    client.send_batch(work_queue, [{ "warm" => true }])
    client.purge_queue(work_queue)
    jobs = nil

    statements = statements_during do
      Pgbus.coalesce { jobs = Array.new(3) { |i| event_job.perform_later("e#{i}") } }
    end

    expect(sends(statements)).to eq(send_batch: 1, send: 0)
    expect(queue_depth(work_queue)).to eq(3)
    expect(jobs.map(&:provider_job_id)).to all(be_present)
  end

  it "sends three perform_later calls outside a scope as three sends" do
    client.send_message(work_queue, { "warm" => true })
    client.purge_queue(work_queue)

    statements = statements_during { Array.new(3) { |i| event_job.perform_later("e#{i}") } }

    expect(sends(statements)).to eq(send_batch: 0, send: 3)
  end

  context "with enqueue_after_transaction_commit" do
    before do
      skip "Active Job #{ActiveJob.version} has no enqueue_after_transaction_commit" unless
        defined?(ActiveJob::EnqueueAfterTransactionCommit)
    end

    it "never sends a job enqueued inside a transaction that rolls back" do
      Pgbus.coalesce do
        ActiveRecord::Base.transaction do
          deferring_job.perform_later(1)
          raise ActiveRecord::Rollback
        end
      end

      expect(queue_depth(work_queue)).to eq(0)
    end

    it "sends a job whose transaction commits inside the scope in the flush" do
      depth_at_commit = nil

      Pgbus.coalesce do
        ActiveRecord::Base.transaction { deferring_job.perform_later(1) }
        depth_at_commit = queue_depth(work_queue)
      end

      expect(depth_at_commit).to eq(0)
      expect(queue_depth(work_queue)).to eq(1)
    end
  end

  describe "a Pgbus.batch inside the scope" do
    def open_batch(job_class, count)
      Pgbus.batch(description: "coalesced") do |b|
        b.on_complete(callback_job)
        count.times { |i| job_class.perform_later(i) }
      end
    end

    it "counts the buffered members and stays pending until the flush has sent them" do
      client.send_batch(work_queue, [{ "warm" => true }])
      client.purge_queue(work_queue)
      batch = nil
      status_in_scope = nil

      statements = statements_during do
        Pgbus.coalesce do
          batch = open_batch(event_job, 3)
          status_in_scope = batch.reload.status
        end
      end

      expect(sends(statements)).to eq(send_batch: 1, send: 0)
      expect(status_in_scope).to eq("pending")
      expect(batch.reload).to have_attributes(total_jobs: 3, status: "processing")
      expect(queue_depth(work_queue)).to eq(3)
      expect(queue_depth(callback_queue)).to eq(0)
    end

    it "fires on_complete only after every member ran" do
      batch = nil
      Pgbus.coalesce { batch = open_batch(event_job, 3) }

      run_all

      expect(batch.reload).to have_attributes(status: "finished", total_jobs: 3, completed_jobs: 3)
      expect(queue_depth(callback_queue)).to eq(1)
    end

    it "counts members deferred to a commit inside the scope" do
      skip "Active Job #{ActiveJob.version} has no enqueue_after_transaction_commit" unless
        defined?(ActiveJob::EnqueueAfterTransactionCommit)

      client.send_batch(work_queue, [{ "warm" => true }])
      client.purge_queue(work_queue)
      batch = nil
      statements = statements_during do
        Pgbus.coalesce do
          batch = ActiveRecord::Base.transaction { open_batch(deferring_job, 2) }
        end
      end

      expect(sends(statements)).to eq(send_batch: 1, send: 0)
      expect(batch.reload).to have_attributes(total_jobs: 2, status: "processing")
      expect(queue_depth(work_queue)).to eq(2)
      expect(Pgbus::BatchExecution.where(batch_id: batch.batch_id).where.not(msg_id: nil).count).to eq(2)
    end

    it "sends an add to a batch started outside the scope at once" do
      batch = open_batch(event_job, 1)
      depth_in_scope = nil

      Pgbus.coalesce do
        batch.add(event_job.new(:late))
        depth_in_scope = queue_depth(work_queue)
      end

      expect(depth_in_scope).to eq(2)
      expect(batch.reload.total_jobs).to eq(2)
    end
  end
end
