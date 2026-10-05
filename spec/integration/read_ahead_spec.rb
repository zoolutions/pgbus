# frozen_string_literal: true

require_relative "../integration_helper"

# Issue #486: a worker with read_ahead holds claimed messages beyond its free
# threads. When it starts draining, those buffered messages must be visible
# to other workers again at once, not after visibility_timeout.
RSpec.describe "Read-ahead (integration)", :integration do
  let(:client) { Pgbus.client }
  let(:queue) { "read_ahead_drain" }
  let(:slow_job) do
    Class.new(ActiveJob::Base) do
      self.queue_adapter = :inline
      def self.name = "ReadAheadSpec::SlowJob"
      def perform(*) = sleep(1.5) # outlives the drain window asserted below
    end
  end

  before do
    require "active_job"
    ActiveJob::Base.logger = Logger.new(IO::NULL)
    stub_const("ReadAheadSpec", Module.new)
    stub_const("ReadAheadSpec::SlowJob", slow_job)
    client.ensure_queue(queue)
    client.purge_queue(queue)
  end

  after do
    Pgbus.stopping = false
    Pgbus::VisibilityHeartbeat.reset!
  end

  def visible
    table = "pgmq.q_#{Pgbus.configuration.queue_name(queue)}"
    ActiveRecord::Base.connection.select_value("SELECT count(*) FROM #{table} WHERE vt <= clock_timestamp()").to_i
  end

  def wait_until(seconds = 5)
    deadline = Time.now + seconds
    sleep 0.01 until yield || Time.now > deadline
    yield
  end

  it "makes buffered messages visible again as soon as the worker drains" do
    client.send_batch(queue, Array.new(20) { slow_job.new.serialize })
    worker = Pgbus::Process::Worker.new(queues: [queue], threads: 2, read_ahead: 4)
    # graceful_shutdown is what the worker's TERM handler runs; keep the
    # spec process's own signal traps out of it.
    allow(worker).to receive_messages(setup_signals: nil, restore_signals: nil)

    runner = Thread.new { worker.run }
    expect(wait_until { worker.stats[:buffered] == 4 }).to be(true)
    expect(worker.stats[:in_flight]).to eq(6)
    expect(visible).to eq(14) # 2 running + 4 buffered are invisible

    worker.graceful_shutdown

    # in_flight drops by the returned count only once every return is written.
    expect(wait_until(1) { worker.stats[:in_flight] == 2 }).to be(true)
    expect(worker.stats[:buffered]).to eq(0)
    # Well inside visibility_timeout (30 s): the 4 buffered are claimable now.
    expect(visible).to eq(18)
    expect(runner.join(5)).to eq(runner)
    expect(worker.stats).to include(in_flight: 0, jobs_processed: 2, jobs_failed: 0)
  ensure
    runner&.kill
  end
end
