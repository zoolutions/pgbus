# frozen_string_literal: true

require_relative "../../integration_helper"
require "tmpdir"

# Process signals end-to-end (issue #674), against a real Postgres: two
# streamer Instances stand in for two web processes, each with its own signal
# delivery thread, so a block registered once runs once per Instance.
RSpec.describe "Process signals (issue #674)", :integration do
  let(:signal_name) { "sig_#{SecureRandom.hex(4)}" }
  let(:seen) { Queue.new }
  let(:gaps) { Queue.new }
  let(:instances) { [] }

  before do
    Pgbus::Signals.reset!
    @saved_scope = Pgbus.configuration.streams_listen_scope
    @saved_socket = ENV.fetch("PGBUS_STREAMS_HUB_SOCKET", nil)
    ENV.delete("PGBUS_STREAMS_HUB_SOCKET")
  end

  after do
    Pgbus::Web::Streamer.current = nil
    instances.each(&:shutdown!)
    Pgbus::Signals.reset!
    Pgbus.configuration.streams_listen_scope = @saved_scope
    @saved_socket ? ENV["PGBUS_STREAMS_HUB_SOCKET"] = @saved_socket : ENV.delete("PGBUS_STREAMS_HUB_SOCKET")
  end

  def start_instance
    Pgbus::Web::Streamer::Instance.new(
      client: Pgbus.client, config: Pgbus.configuration, logger: Logger.new(IO::NULL)
    ).tap(&:start).tap { |instance| instances << instance }
  end

  def listen_backend_pids
    ActiveRecord::Base.connection.select_values(<<~SQL)
      SELECT pid FROM pg_stat_activity
      WHERE application_name = 'pgbus-listen' AND datname = current_database()
    SQL
  end

  # Pops +count+ items, failing once +within+ seconds have passed in total.
  def pop_all(queue, count, within:)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + within
    Array.new(count) do
      remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
      item = queue.pop(timeout: [remaining, 0].max)
      raise "only #{count} item(s) expected within #{within}s; one never came" if item.nil?

      item
    end
  end

  def subscribe
    Pgbus.on_signal(signal_name, on_gap: -> { gaps << Thread.current }) do |payload|
      seen << [Thread.current, payload]
    end
  end

  describe "with a per-process listener (streams_listen_scope :process)" do
    before { Pgbus.configuration.streams_listen_scope = :process }

    it "runs the block in every process within a second, only after COMMIT, never on rollback" do
      subscribe
      start_instance
      start_instance

      ActiveRecord::Base.transaction do
        Pgbus.signal(signal_name, "id=42")
        expect(seen.pop(timeout: 0.3)).to be_nil
      end

      deliveries = pop_all(seen, 2, within: 1.0)
      expect(deliveries.map(&:last)).to eq(%w[id=42 id=42])
      expect(deliveries.map(&:first).uniq.size).to eq(2)

      ActiveRecord::Base.transaction do
        Pgbus.signal(signal_name, "rolled back")
        raise ActiveRecord::Rollback
      end
      Pgbus.signal(signal_name)

      expect(pop_all(seen, 2, within: 1.0).map(&:last)).to eq([nil, nil])
    end

    it "LISTENs on a signal registered after this process's streamer started" do
      Pgbus::Web::Streamer.current = start_instance

      subscribe
      Pgbus.signal(signal_name)

      expect(pop_all(seen, 1, within: 1.0).map(&:last)).to eq([nil])
    end

    it "runs every on_gap once after the LISTEN connection drops and reconnects, and keeps delivering" do
      baseline = listen_backend_pids
      subscribe
      start_instance
      listener_pids = listen_backend_pids - baseline
      expect(listener_pids.size).to eq(1)

      ActiveRecord::Base.connection.select_value("SELECT pg_terminate_backend(#{Integer(listener_pids.first)})")

      expect(pop_all(gaps, 1, within: 15).size).to eq(1)
      expect(gaps.pop(timeout: 0.5)).to be_nil

      Pgbus.signal(signal_name, "after the gap")
      expect(pop_all(seen, 1, within: 2.0).map(&:last)).to eq(["after the gap"])
    end

    it "reports a raising subscriber and still runs the others and wakes nothing else" do
      reported = Queue.new
      Pgbus.configuration.error_reporters << ->(error, context) { reported << [error.message, context[:action]] }
      Pgbus.on_signal(signal_name) { raise "subscriber boom" }
      subscribe
      start_instance

      Pgbus.signal(signal_name, "x")

      expect(pop_all(seen, 1, within: 1.0).map(&:last)).to eq(["x"])
      expect(pop_all(reported, 1, within: 1.0)).to eq([["subscriber boom", "signal_subscriber"]])
    ensure
      Pgbus.configuration.error_reporters.pop
    end
  end

  describe "through the master hub (streams_listen_scope :master)" do
    let(:tmpdir) { Dir.mktmpdir("pgbus-signals-hub") }
    let(:socket_path) { File.join(tmpdir, "hub.sock") }
    let(:hub) do
      Pgbus::Web::Streamer::MasterHub.new(
        config: Pgbus.configuration, socket_path: socket_path, status_interval: 0.5, logger: Logger.new(IO::NULL)
      )
    end

    before do
      Pgbus.configuration.streams_listen_scope = :master
      ENV["PGBUS_STREAMS_HUB_SOCKET"] = socket_path
    end

    after do
      hub.stop
      FileUtils.remove_entry(tmpdir) if File.directory?(tmpdir)
    end

    it "delivers to every worker over the hub's one LISTEN, and after a hub failover reports a gap and keeps delivering" do
      baseline = listen_backend_pids
      hub.start
      subscribe
      worker_a = start_instance
      worker_b = start_instance
      expect([worker_a.listener, worker_b.listener]).to all(be_a(Pgbus::Web::Streamer::FailoverListener))

      # No worker opened a LISTEN connection of its own.
      expect((listen_backend_pids - baseline).size).to eq(1)

      Pgbus.signal(signal_name, "via hub")
      deliveries = pop_all(seen, 2, within: 1.0)
      expect(deliveries.map(&:last)).to eq(["via hub", "via hub"])
      expect(deliveries.map(&:first).uniq.size).to eq(2)

      hub.stop
      expect(pop_all(gaps, 2, within: 10).map(&:object_id).uniq.size).to eq(2)

      Pgbus.signal(signal_name, "via fallback")
      expect(pop_all(seen, 2, within: 2.0).map(&:last)).to eq(["via fallback", "via fallback"])
      expect((listen_backend_pids - baseline).size).to eq(2)
    end
  end
end
