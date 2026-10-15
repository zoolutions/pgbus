# frozen_string_literal: true

require "spec_helper"
require "active_record"

# QUIT used to kill the execution pool outright: a job holding an
# ActiveRecord connection died with it checked out, and the pool stayed one
# connection short until the reaper noticed (issue #527). Now the jobs get
# config.immediate_shutdown_grace to finish, the rest are killed, and the dead
# jobs' connections are reclaimed.
RSpec.describe "immediate shutdown" do # rubocop:disable RSpec/DescribeClass
  let(:record_class) do
    Class.new(ActiveRecord::Base) do
      self.abstract_class = true

      def self.name = "ImmediateShutdownProbe"
    end
  end
  let(:ar_pool) { record_class.connection_pool }
  let(:checked_out) { -> { ar_pool.connections.count(&:in_use?) } }
  let(:started) { Queue.new }
  let(:finished) { Queue.new }

  before do
    record_class.establish_connection(adapter: "sqlite3", database: ":memory:", pool: 4)
    allow(Pgbus).to receive(:client).and_return(build_mock_client)
    allow(Pgbus::Process::Heartbeat).to receive(:new)
      .and_return(instance_double(Pgbus::Process::Heartbeat, start: true, stop: true))
  end

  after do
    Pgbus.stopping = false
    Pgbus.configuration.immediate_shutdown_grace = 2
    record_class.remove_connection
  end

  # A job that checks out a connection the way `Model.connection` does and
  # then blocks until `release` is fed.
  def post_job(pool, release)
    pool.post do
      ar_pool.lease_connection
      started << true
      release.pop
      finished << :done
    end
    started.pop
  end

  shared_examples "an immediate shutdown" do
    it "returns a killed job's connection to the pool" do
      Pgbus.configuration.immediate_shutdown_grace = 0
      2.times { post_job(pool, Queue.new) }
      expect(checked_out.call).to be_positive

      process.immediate_shutdown

      expect(checked_out.call).to eq(0)
      expect(finished).to be_empty
    end

    it "lets a job that finishes within the grace finish instead of killing it" do
      Pgbus.configuration.immediate_shutdown_grace = 30
      release = Queue.new
      post_job(pool, release)
      allow(pool).to receive(:shutdown).and_wrap_original do |original|
        original.call.tap { release << true }
      end
      allow(pool).to receive(:kill).and_call_original

      process.immediate_shutdown

      # Bounded so a killed job fails the example instead of hanging the run.
      expect(finished.pop(timeout: 5)).to eq(:done)
      expect(pool).not_to have_received(:kill)
    end
  end

  shared_context "with a worker" do
    let(:process) { Pgbus::Process::Worker.new(queues: %w[default], threads: 2, execution_mode: mode) }

    before do
      allow(Pgbus::ExecutionPools).to receive(:build).and_return(pool)
      process.lifecycle.transition_to!(:running)
    end
  end

  context "with a Worker on the thread pool" do
    let(:mode) { :threads }
    let(:pool) { Pgbus::ExecutionPools::ThreadPool.new(capacity: 2) }

    include_context "with a worker"
    it_behaves_like "an immediate shutdown"
  end

  context "with a Worker on the async pool" do
    let(:mode) { :async }
    let(:pool) { Pgbus::ExecutionPools::AsyncPool.new(capacity: 2) }

    include_context "with a worker"
    it_behaves_like "an immediate shutdown"
  end

  context "with a Consumer on the thread pool" do
    let(:pool) { Pgbus::ExecutionPools::ThreadPool.new(capacity: 2) }
    let(:process) { Pgbus::Process::Consumer.new(topics: ["orders.#"], threads: 2) }

    before do
      allow(Pgbus::ExecutionPools).to receive(:build).and_return(pool)
      allow(Pgbus::EventBus::Registry).to receive(:instance)
        .and_return(instance_double(Pgbus::EventBus::Registry, subscribers: [], queue_names_for_topics: []))
    end

    it_behaves_like "an immediate shutdown"
  end
end
