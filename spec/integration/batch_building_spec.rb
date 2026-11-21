# frozen_string_literal: true

require_relative "../integration_helper"
require "active_job"
require "active_job/queue_adapters/pgbus_adapter"

# Issue #683: a batch opened in a process with a heartbeat row is built. It
# is created "building" with that row as its owner, its block's
# perform_later calls are held and counted in per chunk, and the block's end
# closes the build (building -> pending) in the transaction that counts the
# last chunk. While building, no sweep and no finish touch it; the sweep
# reaps a build only once its owner stops heartbeating.
RSpec.describe "Batch building (integration)", :integration do
  let(:client) { Pgbus.client }
  let(:work_queue) { "building_work" }
  let(:callback_queue) { "building_callbacks" }
  let(:executor) { Pgbus::ActiveJob::Executor.new(client: client) }

  let(:work_job) do
    queue = work_queue
    Class.new(ActiveJob::Base) do
      include Pgbus::ActiveJob::BatchId

      self.queue_adapter = :pgbus
      queue_as(queue)
      def self.name = "BuildingSpec::WorkJob"
      def perform(*); end
    end
  end

  # Sent at once even inside the block: uniqueness takes the single path.
  let(:unique_job) do
    queue = work_queue
    Class.new(ActiveJob::Base) do
      include Pgbus::ActiveJob::BatchId
      include Pgbus::Uniqueness

      self.queue_adapter = :pgbus
      queue_as(queue)
      def self.name = "BuildingSpec::UniqueJob"
      def perform(*); end

      ensures_uniqueness strategy: :until_executing, key: ->(id) { "building-#{id}" }
    end
  end
  let(:owner) do
    Pgbus::ProcessEntry.create!(kind: "worker", hostname: "spec-host", pid: Process.pid, metadata: {},
                                last_heartbeat_at: Time.current)
  end

  def callback(name)
    queue = callback_queue
    Class.new(ActiveJob::Base) do
      self.queue_adapter = :pgbus
      queue_as(queue)
      define_singleton_method(:name) { "BuildingSpec::#{name}" }
      def perform(*); end
    end
  end

  before do
    ActiveJob::Base.logger = Logger.new(IO::NULL)
    Pgbus::Batch.reset_executions_migrated_cache!
    stub_const("BuildingSpec", Module.new)
    stub_const("BuildingSpec::WorkJob", work_job)
    stub_const("BuildingSpec::UniqueJob", unique_job)
    %w[DoneJob OkJob FailJob].each { |name| stub_const("BuildingSpec::#{name}", callback(name)) }
    client.ensure_queue(work_queue)
    client.ensure_queue(callback_queue)
    Pgbus::Process::Heartbeat.note_registered(owner.id)
  end

  after { Pgbus::Process::Heartbeat.note_deregistered(owner.id) }

  def open_batch(&)
    Pgbus.batch do |b|
      b.on_complete(BuildingSpec::DoneJob)
      b.on_success(BuildingSpec::OkJob)
      b.on_failure(BuildingSpec::FailJob)
      yield
    end
  end

  def status_of(batch_id) = Pgbus::BatchEntry.find_by(batch_id: batch_id).status

  def callbacks
    classes = []
    while (message = client.read_message(callback_queue, vt: 30))
      classes << JSON.parse(message.message).fetch("job_class")
      client.delete_message(callback_queue, message.msg_id.to_i)
    end
    classes
  end

  def run_all
    while (message = client.read_message(work_queue, vt: 30))
      executor.execute(message, work_queue)
    end
  end

  def queue_depth(queue)
    physical = Pgbus.configuration.queue_name(queue)
    ActiveRecord::Base.connection.select_value("SELECT count(*) FROM pgmq.q_#{physical}").to_i
  end

  # The sweep runs in the dispatcher, outside the block's coalesce scope.
  def sweep_elsewhere(stalled_for: 300)
    Thread.new { Pgbus::Batch.sweep_stalled(stalled_for: stalled_for) }.join # rubocop:disable ThreadSafety/NewThread -- joined here
  end

  def backdate(batch_id)
    Pgbus::BatchEntry.where(batch_id: batch_id).update_all(created_at: 1.hour.ago)
    Pgbus::BatchExecution.where(batch_id: batch_id).update_all(created_at: 1.hour.ago)
  end

  it "builds the batch with this process as its owner, then closes the build at the block's end" do
    seen = nil
    batch = open_batch do
      work_job.perform_later(1)
      record = Pgbus::BatchEntry.find_by(batch_id: Thread.current[:pgbus_batch_id])
      seen = [record.status, record.owner_process_id]
    end

    expect(seen).to eq(["building", owner.id])
    expect(batch.reload).to have_attributes(status: "processing", total_jobs: 1)
  end

  it "is skipped by the stalled-batch sweep while building, however old" do
    open_batch do
      work_job.perform_later(1)
      batch_id = Thread.current[:pgbus_batch_id]
      backdate(batch_id)
      sweep_elsewhere

      expect(status_of(batch_id)).to eq("building")
      expect(callbacks).to be_empty
    end
  end

  it "is not finished by a member that finishes while building, and finishes right after the block" do
    statuses = []
    batch = open_batch do
      unique_job.perform_later(1) # sent and counted at once
      run_all
      statuses << status_of(Thread.current[:pgbus_batch_id])
      expect(callbacks).to be_empty
    end

    expect(statuses).to eq(["building"])
    expect(batch.reload).to have_attributes(status: "finished", total_jobs: 1, completed_jobs: 1)
    expect(callbacks).to contain_exactly("BuildingSpec::DoneJob", "BuildingSpec::OkJob")
  end

  describe "a transaction inside the block" do
    it "rolled back around the first held member cannot finish the batch early" do
      batch = open_batch do
        ActiveRecord::Base.transaction do
          work_job.perform_later(1)
          raise ActiveRecord::Rollback
        end
        work_job.perform_later(2)
        batch_id = Thread.current[:pgbus_batch_id]
        backdate(batch_id)
        sweep_elsewhere

        expect(status_of(batch_id)).to eq("building")
        expect(callbacks).to be_empty
      end

      run_all
      expect(batch.reload).to have_attributes(status: "finished", completed_jobs: 2)
      expect(callbacks).to contain_exactly("BuildingSpec::DoneJob", "BuildingSpec::OkJob")
    end

    it "committed, then one held open across the sweep, cannot finish the batch early" do
      Pgbus.configuration.coalesce_chunk_size = 1
      batch = open_batch do
        ActiveRecord::Base.transaction { work_job.perform_later(1) }
        batch_id = Thread.current[:pgbus_batch_id]
        ActiveRecord::Base.transaction do
          work_job.perform_later(2)
          backdate(batch_id)
          sweep_elsewhere
        end

        expect(status_of(batch_id)).to eq("building")
        expect(callbacks).to be_empty
      end

      run_all
      expect(batch.reload).to have_attributes(status: "finished", completed_jobs: 2)
    ensure
      Pgbus.configuration.coalesce_chunk_size = 1_000
    end
  end

  describe "an owner that stops heartbeating" do
    it "is reaped once its process row is gone: one failed job, on_complete and on_failure, never on_success" do
      error = nil
      batch = nil
      begin
        batch = open_batch do
          unique_job.perform_later(1)
          work_job.perform_later(2) # held, never sent: the process "dies" with it
          Pgbus::ProcessEntry.where(id: owner.id).delete_all
          sweep_elsewhere

          batch_id = Thread.current[:pgbus_batch_id]
          expect(Pgbus::BatchEntry.find_by(batch_id: batch_id))
            .to have_attributes(status: "processing", total_jobs: 2, failed_jobs: 1)
        end
      rescue Pgbus::EnqueueError => e
        error = e
      end

      # The block "returned" after its build was reaped: the held member fails.
      expect(error.cause).to be_a(Pgbus::Batch::AlreadyFinished)
      batch ||= Pgbus::Batch.find(Pgbus::BatchEntry.order(:id).last.batch_id)
      run_all
      expect(batch.reload).to have_attributes(status: "finished", completed_jobs: 1, failed_jobs: 1)
      expect(callbacks).to contain_exactly("BuildingSpec::DoneJob", "BuildingSpec::FailJob")
    end

    it "is reaped once its heartbeat is older than the dead-process threshold" do
      batch_id = nil
      begin
        open_batch do
          work_job.perform_later(1)
          batch_id = Thread.current[:pgbus_batch_id]
          Pgbus::ProcessEntry.where(id: owner.id)
                             .update_all(last_heartbeat_at: Time.current - Pgbus::Process::Heartbeat::ALIVE_THRESHOLD - 1)
          sweep_elsewhere
        end
      rescue Pgbus::EnqueueError
        nil
      end

      expect(Pgbus::BatchEntry.find_by(batch_id: batch_id).status).to eq("finished")
    end

    it "is not reaped while its owner heartbeats" do
      open_batch do
        work_job.perform_later(1)
        batch_id = Thread.current[:pgbus_batch_id]
        Pgbus::ProcessEntry.where(id: owner.id).update_all(last_heartbeat_at: Time.current)
        backdate(batch_id)
        sweep_elsewhere

        expect(status_of(batch_id)).to eq("building")
      end
    end

    # Heartbeat#write_beat re-registers a process whose row the stale-process
    # reaper deleted: pgbus already treated it as dead, so a build it owned
    # under the old row is reaped like any other.
    it "treats a build owned by a row the stale-process reaper deleted as abandoned" do
      batch_id = nil
      begin
        open_batch do
          work_job.perform_later(1)
          batch_id = Thread.current[:pgbus_batch_id]
          Pgbus::ProcessEntry.where(id: owner.id).delete_all
          replacement = Pgbus::ProcessEntry.create!(kind: "worker", hostname: "spec-host", pid: Process.pid,
                                                    metadata: {}, last_heartbeat_at: Time.current)
          Pgbus::Process::Heartbeat.note_registered(replacement.id)
          sweep_elsewhere
        end
      rescue Pgbus::EnqueueError
        nil
      end

      expect(Pgbus::BatchEntry.find_by(batch_id: batch_id).status).to eq("finished")
    end
  end

  describe "a process without a heartbeat row" do
    before { Pgbus::Process::Heartbeat.note_deregistered(owner.id) }

    it "takes the pending path: no build, members sent and counted at once" do
      depth_in_block = nil
      seen = nil
      batch = open_batch do
        work_job.perform_later(1)
        depth_in_block = queue_depth(work_queue)
        seen = Pgbus::BatchEntry.find_by(batch_id: Thread.current[:pgbus_batch_id]).slice(:status, :owner_process_id)
      end

      expect(seen).to eq("status" => "pending", "owner_process_id" => nil)
      expect(depth_in_block).to eq(1)
      expect(batch.reload.total_jobs).to eq(1)
    end
  end
end
