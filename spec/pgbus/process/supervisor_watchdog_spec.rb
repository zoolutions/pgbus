# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Process::Supervisor do
  describe "#check_stalled_workers" do
    let(:mock_heartbeat) { instance_double(Pgbus::Process::Heartbeat, start: nil, stop: nil) }
    let(:config) { Pgbus.configuration }
    let(:supervisor) { described_class.new }

    before do
      allow(Pgbus::Process::Heartbeat).to receive(:new).and_return(mock_heartbeat)
      # The TTIN-to-KILL grace is a real 2s sleep in production.
      allow(supervisor).to receive(:dump_grace_sleep)
      config.stall_threshold = 90
      config.boot_timeout = 120
    end

    after do
      config.stall_threshold = 90
      config.boot_timeout = 120
    end

    # Records Process.kill calls and the TTIN grace in the order they happen.
    def record_kill_sequence
      calls = []
      allow(Process).to receive(:kill) { |sig, pid| calls << [sig, pid] }
      allow(supervisor).to receive(:dump_grace_sleep) { calls << :grace }
      calls
    end

    describe "check_stalled_workers (private)" do
      let(:stalled_entry) do
        double("ProcessEntry",
               kind: "worker",
               pid: 1001,
               metadata: { "loop_tick_at" => (Time.now.to_f - 120) })
      end

      let(:healthy_entry) do
        double("ProcessEntry",
               kind: "worker",
               pid: 1002,
               metadata: { "loop_tick_at" => Time.now.to_f })
      end

      before do
        supervisor.forks = {
          1001 => { type: :worker, config: { queues: ["default"] } },
          1002 => { type: :worker, config: { queues: ["priority"] } }
        }
        supervisor.last_watchdog_at = 0.0
      end

      it "kills a worker whose loop_tick_at exceeds stall_threshold" do
        allow(Pgbus::ProcessEntry).to receive(:where)
          .with(hostname: Socket.gethostname, pid: [1001, 1002])
          .and_return(double(to_a: [stalled_entry, healthy_entry]))
        allow(Process).to receive(:kill)

        supervisor.send(:check_stalled_workers)

        expect(Process).to have_received(:kill).with("KILL", 1001)
        expect(Process).not_to have_received(:kill).with("KILL", 1002)
      end

      it "does not kill a worker within threshold" do
        allow(Pgbus::ProcessEntry).to receive(:where)
          .with(hostname: Socket.gethostname, pid: [1001, 1002])
          .and_return(double(to_a: [healthy_entry]))
        allow(Process).to receive(:kill)

        supervisor.send(:check_stalled_workers)

        expect(Process).not_to have_received(:kill)
      end

      it "skips the DB when stall_threshold is nil and every fork has checked in" do
        config.stall_threshold = nil
        supervisor.forks.each_value { |info| info[:checked_in] = true }
        allow(Pgbus::ProcessEntry).to receive(:where)
        allow(Process).to receive(:kill)

        supervisor.send(:check_stalled_workers)

        expect(Pgbus::ProcessEntry).not_to have_received(:where)
        expect(Process).not_to have_received(:kill)
      end

      it "handles Errno::ESRCH when process is already gone" do
        allow(Pgbus::ProcessEntry).to receive(:where)
          .with(hostname: Socket.gethostname, pid: [1001, 1002])
          .and_return(double(to_a: [stalled_entry]))
        allow(Process).to receive(:kill).and_raise(Errno::ESRCH)

        expect { supervisor.send(:check_stalled_workers) }.not_to raise_error
      end

      it "skips entries without loop_tick_at in metadata" do
        entry_no_tick = double("ProcessEntry",
                               kind: "worker", pid: 1001,
                               metadata: { "queues" => ["default"] })
        allow(Pgbus::ProcessEntry).to receive(:where)
          .with(hostname: Socket.gethostname, pid: [1001, 1002])
          .and_return(double(to_a: [entry_no_tick]))
        allow(Process).to receive(:kill)

        supervisor.send(:check_stalled_workers)

        expect(Process).not_to have_received(:kill)
      end

      it "respects the WATCHDOG_INTERVAL rate limiter" do
        supervisor.last_watchdog_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        allow(Pgbus::ProcessEntry).to receive(:where)

        supervisor.send(:check_stalled_workers)

        expect(Pgbus::ProcessEntry).not_to have_received(:where)
      end

      it "does not check non-worker forks once they have checked in" do
        supervisor.forks = {
          2001 => { type: :dispatcher, checked_in: true },
          2002 => { type: :scheduler, checked_in: true }
        }

        allow(Pgbus::ProcessEntry).to receive(:where)

        supervisor.send(:check_stalled_workers)

        expect(Pgbus::ProcessEntry).not_to have_received(:where)
      end

      it "rescues and logs DB errors" do
        allow(Pgbus::ProcessEntry).to receive(:where).and_raise(StandardError, "connection lost")

        expect { supervisor.send(:check_stalled_workers) }.not_to raise_error
      end
    end

    # Consumer operational parity (issue #274): consumer forks are now included
    # in the watchdog selection and the DB loop-tick query so a wedged consumer
    # is killed and restarted like a wedged worker.
    describe "consumer forks (issue #274)" do
      let(:stalled_consumer) do
        double("ProcessEntry", kind: "consumer", pid: 4001,
                               metadata: { "loop_tick_at" => (Time.now.to_f - 120) })
      end

      before do
        supervisor.forks = {
          4001 => { type: :consumer, config: { topics: ["orders.#"] } }
        }
        supervisor.last_watchdog_at = 0.0
      end

      it "queries this host's process rows for consumer forks" do
        allow(Pgbus::ProcessEntry).to receive(:where)
          .with(hostname: Socket.gethostname, pid: [4001])
          .and_return(double(to_a: []))
        allow(Process).to receive(:kill)

        supervisor.send(:check_stalled_workers)

        expect(Pgbus::ProcessEntry).to have_received(:where).with(hostname: Socket.gethostname, pid: [4001])
      end

      it "kills a consumer whose loop_tick_at exceeds stall_threshold" do
        allow(Pgbus::ProcessEntry).to receive(:where)
          .with(hostname: Socket.gethostname, pid: [4001])
          .and_return(double(to_a: [stalled_consumer]))
        allow(Process).to receive(:kill)

        supervisor.send(:check_stalled_workers)

        expect(Process).to have_received(:kill).with("KILL", 4001)
      end

      it "kills a wedged consumer via the pipe fallback when the DB read raises" do
        now = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        allow(Pgbus::ProcessEntry).to receive(:where).and_raise(StandardError, "connection lost")
        supervisor.forks = {
          4001 => { type: :consumer, config: { topics: ["orders.#"] },
                    liveness_reader: nil, last_pipe_tick_at: now - 120, pipe_seen: true, checked_in: true }
        }
        allow(Process).to receive(:kill)

        supervisor.send(:check_stalled_workers)

        expect(Process).to have_received(:kill).with("KILL", 4001)
      end
    end

    # Pipe-based liveness fallback: when the database is unreachable the
    # Heartbeat can't write loop_tick_at and the watchdog DB read raises.
    # The OS pipe channel keeps stall detection working during the outage.
    describe "pipe-liveness fallback (DB-independent)" do
      let(:now) { Process.clock_gettime(Process::CLOCK_MONOTONIC) }

      before do
        supervisor.last_watchdog_at = 0.0
        allow(Process).to receive(:kill)
      end

      def set_forks(pid, last_pipe_tick_at:, pipe_seen:)
        supervisor.forks = {
          pid => { type: :worker, config: { queues: ["default"] },
                   liveness_reader: nil,
                   last_pipe_tick_at: last_pipe_tick_at,
                   pipe_seen: pipe_seen, checked_in: pipe_seen }
        }
      end

      it "SIGKILLs a worker with a stale pipe tick when the DB read raises" do
        allow(Pgbus::ProcessEntry).to receive(:where).and_raise(StandardError, "connection lost")
        set_forks(1001, last_pipe_tick_at: now - 120, pipe_seen: true)

        supervisor.send(:check_stalled_workers)

        expect(Process).to have_received(:kill).with("KILL", 1001)
      end

      it "does NOT kill a worker with a fresh pipe tick when the DB read raises" do
        allow(Pgbus::ProcessEntry).to receive(:where).and_raise(StandardError, "connection lost")
        set_forks(1001, last_pipe_tick_at: now - 1, pipe_seen: true)

        supervisor.send(:check_stalled_workers)

        expect(Process).not_to have_received(:kill)
      end

      it "does NOT kill a worker whose pipe is unarmed (booting, no byte seen yet)" do
        allow(Pgbus::ProcessEntry).to receive(:where).and_raise(StandardError, "connection lost")
        # Seeded far in the past but never armed — a slow-booting worker.
        set_forks(1001, last_pipe_tick_at: now - 999, pipe_seen: false)

        supervisor.send(:check_stalled_workers)

        expect(Process).not_to have_received(:kill)
      end

      it "keeps a worker alive on a stale DB row when the pipe tick is fresh (fresh-wins)" do
        stale_db = double("ProcessEntry", kind: "worker", pid: 1001,
                                          metadata: { "loop_tick_at" => (Time.now.to_f - 300) })
        allow(Pgbus::ProcessEntry).to receive(:where)
          .with(hostname: Socket.gethostname, pid: [1001])
          .and_return(double(to_a: [stale_db]))
        set_forks(1001, last_pipe_tick_at: now - 1, pipe_seen: true)

        supervisor.send(:check_stalled_workers)

        expect(Process).not_to have_received(:kill)
      end

      it "kills a worker when BOTH the DB row and the pipe tick are stale" do
        stale_db = double("ProcessEntry", kind: "worker", pid: 1001,
                                          metadata: { "loop_tick_at" => (Time.now.to_f - 300) })
        allow(Pgbus::ProcessEntry).to receive(:where)
          .with(hostname: Socket.gethostname, pid: [1001])
          .and_return(double(to_a: [stale_db]))
        set_forks(1001, last_pipe_tick_at: now - 120, pipe_seen: true)

        supervisor.send(:check_stalled_workers)

        expect(Process).to have_received(:kill).with("KILL", 1001)
      end
    end

    # Issue #587: a child that forks and hangs before its first liveness signal
    # (no process row on this host, no pipe byte) used to be skipped forever.
    describe "boot deadline (issue #587)" do
      let(:now) { Process.clock_gettime(Process::CLOCK_MONOTONIC) }
      let(:log_output) { StringIO.new }

      before do
        supervisor.last_watchdog_at = 0.0
        allow(Process).to receive(:kill)
        allow(Pgbus::ProcessEntry).to receive(:where).and_return(double(to_a: []))
        allow(Pgbus).to receive(:logger).and_return(Logger.new(log_output))
      end

      def pending_fork(type, spawned_ago:)
        info = { type: type, spawned_at: now - spawned_ago, checked_in: false }
        if %i[worker consumer].include?(type)
          info.merge!(config: {}, liveness_reader: nil, last_pipe_tick_at: now - spawned_ago, pipe_seen: false)
        end
        info
      end

      %i[worker consumer dispatcher scheduler outbox_poller].each do |type|
        it "logs, sends TTIN, waits the grace, then SIGKILLs a #{type} that never checked in" do
          supervisor.forks = { 4242 => pending_fork(type, spawned_ago: 600) }
          calls = record_kill_sequence

          supervisor.send(:check_stalled_workers)

          expect(calls).to eq([["TTIN", 4242], :grace, ["KILL", 4242]])
          expect(log_output.string).to match(/ERROR.*#{type} pid=4242 boot timeout/)
        end
      end

      it "reads only this host's process rows" do
        supervisor.forks = { 4242 => pending_fork(:dispatcher, spawned_ago: 5) }

        supervisor.send(:check_stalled_workers)

        expect(Pgbus::ProcessEntry).to have_received(:where).with(hostname: Socket.gethostname, pid: [4242])
      end

      it "tolerates a slow-but-alive boot inside the window" do
        supervisor.forks = { 4242 => pending_fork(:worker, spawned_ago: 5) }

        supervisor.send(:check_stalled_workers)

        expect(Process).not_to have_received(:kill)
      end

      it "does nothing when boot_timeout is nil" do
        config.boot_timeout = nil
        supervisor.forks = { 4242 => pending_fork(:dispatcher, spawned_ago: 600) }

        supervisor.send(:check_stalled_workers)

        expect(Process).not_to have_received(:kill)
      end

      it "still marks forks checked in from their process row when both thresholds are nil" do
        config.boot_timeout = nil
        config.stall_threshold = nil
        supervisor.forks = { 4242 => pending_fork(:dispatcher, spawned_ago: 5) }
        row = double("ProcessEntry", kind: "dispatcher", pid: 4242, metadata: {})
        allow(Pgbus::ProcessEntry).to receive(:where).and_return(double(to_a: [row]))

        supervisor.send(:check_stalled_workers)

        expect(supervisor.forks[4242][:checked_in]).to be(true)
      end

      it "never kills a fork that checked in with a process row, past the window" do
        supervisor.forks = { 4242 => pending_fork(:dispatcher, spawned_ago: 600) }
        row = double("ProcessEntry", kind: "dispatcher", pid: 4242, metadata: {})
        allow(Pgbus::ProcessEntry).to receive(:where).and_return(double(to_a: [row]))

        supervisor.send(:check_stalled_workers)

        expect(Process).not_to have_received(:kill)
        expect(supervisor.forks[4242][:checked_in]).to be(true)
      end

      it "never kills a worker that checked in over the pipe, past the window" do
        supervisor.forks = {
          4242 => pending_fork(:worker, spawned_ago: 600).merge(pipe_seen: true, checked_in: true,
                                                                last_pipe_tick_at: now - 1)
        }

        supervisor.send(:check_stalled_workers)

        expect(Process).not_to have_received(:kill)
      end

      it "leaves a checked-in worker to the stall rules, whatever its age" do
        config.stall_threshold = nil
        supervisor.forks = {
          4242 => pending_fork(:worker, spawned_ago: 600).merge(pipe_seen: true, checked_in: true,
                                                                last_pipe_tick_at: now - 500)
        }

        supervisor.send(:check_stalled_workers)

        expect(Process).not_to have_received(:kill)
      end

      it "applies no boot deadline when the process-row read fails" do
        allow(Pgbus::ProcessEntry).to receive(:where).and_raise(StandardError, "connection lost")
        supervisor.forks = { 4242 => pending_fork(:dispatcher, spawned_ago: 600) }

        supervisor.send(:check_stalled_workers)

        expect(Process).not_to have_received(:kill)
      end

      it "ignores a fork with no spawned_at" do
        supervisor.forks = { 4242 => { type: :dispatcher, checked_in: false } }

        supervisor.send(:check_stalled_workers)

        expect(Process).not_to have_received(:kill)
      end

      it "waits one grace for every child killed in the same pass" do
        supervisor.forks = {
          4242 => pending_fork(:dispatcher, spawned_ago: 600),
          4243 => pending_fork(:scheduler, spawned_ago: 600)
        }

        supervisor.send(:check_stalled_workers)

        expect(supervisor).to have_received(:dump_grace_sleep).once
        expect(Process).to have_received(:kill).with("KILL", 4242)
        expect(Process).to have_received(:kill).with("KILL", 4243)
      end

      it "does not SIGKILL a child that is already gone at TTIN" do
        allow(Process).to receive(:kill).with("TTIN", 4242).and_raise(Errno::ESRCH)
        supervisor.forks = { 4242 => pending_fork(:dispatcher, spawned_ago: 600) }

        supervisor.send(:check_stalled_workers)

        expect(Process).not_to have_received(:kill).with("KILL", 4242)
      end

      it "re-forks the killed child through the restart path once it is reaped" do
        supervisor.forks = { 4242 => pending_fork(:dispatcher, spawned_ago: 600) }
        supervisor.send(:check_stalled_workers)
        killed = instance_double(Process::Status, success?: false, exitstatus: nil, signaled?: true, termsig: 9)
        allow(Process).to receive(:waitpid2).and_return([4242, killed], nil)
        allow(supervisor).to receive(:fork_dispatcher)

        supervisor.send(:reap_children)

        expect(supervisor).to have_received(:fork_dispatcher)
        expect(supervisor.forks).not_to have_key(4242)
      end
    end

    describe "stall kill sequence (issue #587)" do
      it "sends TTIN and waits the grace before the stall SIGKILL" do
        supervisor.forks = { 1001 => { type: :worker, config: {} } }
        supervisor.last_watchdog_at = 0.0
        stale = double("ProcessEntry", kind: "worker", pid: 1001,
                                       metadata: { "loop_tick_at" => (Time.now.to_f - 120) })
        allow(Pgbus::ProcessEntry).to receive(:where).and_return(double(to_a: [stale]))
        calls = record_kill_sequence

        supervisor.send(:check_stalled_workers)

        expect(calls).to eq([["TTIN", 1001], :grace, ["KILL", 1001]])
      end
    end

    # Issue #587: `kill -TTIN <child>` dumps every thread's backtrace to the
    # pgbus logger and the child keeps running. A real fork, because the
    # handler runs in trap context, where Logger cannot take its lock.
    describe "thread dump on TTIN (issue #587)" do
      let!(:log) { Tempfile.new("pgbus-ttin") } # created before the fork, so both sides share the path

      after { log.close! }

      # Fork a child that runs the real child setup, parks a named thread in
      # sleep, and reports ready. Never returns in the child.
      def fork_parked_child
        ready_r, ready_w = IO.pipe
        pid = fork do
          ready_r.close
          Pgbus.configuration.logger = Logger.new(log.path)
          supervisor.send(:setup_child_process)
          Thread.new { sleep }.name = "parked-helper" # rubocop:disable ThreadSafety/NewThread
          ready_w.write("1")
          loop { sleep 0.05 }
        ensure
          exit!(1)
        end
        ready_w.close
        ready_r.read(1)
        ready_r.close
        pid
      end

      def wait_for_log(text, timeout: 5)
        deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
        sleep 0.05 until File.read(log.path).include?(text) ||
                         Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
        File.read(log.path)
      end

      def kill_child(pid)
        Process.kill("KILL", pid)
        Process.waitpid(pid)
      rescue Errno::ESRCH, Errno::ECHILD
        nil
      end

      it "logs every thread's backtrace and keeps the child running" do
        pid = fork_parked_child

        Process.kill("TTIN", pid)
        output = wait_for_log("parked-helper")

        expect(output).to include("Thread dump pid=#{pid}")
        expect(output).to match(/ERROR.*parked-helper.*\n.*in 'Kernel#sleep'/)
        expect(Process.waitpid(pid, Process::WNOHANG)).to be_nil
      ensure
        kill_child(pid) if pid
      end
    end

    # The watchdog draining side: monitor_loop advances last_pipe_tick_at and
    # arms pipe_seen when a worker's pipe has readable bytes.
    describe "drain_liveness_pipes (private)" do
      it "advances last_pipe_tick_at and arms pipe_seen when bytes are readable" do
        reader, writer = IO.pipe
        writer.write("\0\0")
        supervisor.forks = {
          1001 => { type: :worker, liveness_reader: reader,
                    last_pipe_tick_at: 0.0, pipe_seen: false }
        }

        supervisor.send(:drain_liveness_pipes)

        info = supervisor.forks[1001]
        expect(info[:pipe_seen]).to be(true)
        expect(info[:checked_in]).to be(true)
        expect(info[:last_pipe_tick_at]).to be > 0.0
      ensure
        reader.close unless reader.closed?
        writer.close unless writer.closed?
      end

      it "does not arm pipe_seen when the pipe is empty" do
        reader, writer = IO.pipe
        supervisor.forks = {
          1001 => { type: :worker, liveness_reader: reader,
                    last_pipe_tick_at: 0.0, pipe_seen: false }
        }

        supervisor.send(:drain_liveness_pipes)

        expect(supervisor.forks[1001][:pipe_seen]).to be(false)
      ensure
        reader.close unless reader.closed?
        writer.close unless writer.closed?
      end

      it "does not raise when a reader was closed by a racing reap" do
        reader, writer = IO.pipe
        reader.close
        writer.close
        supervisor.forks = {
          1001 => { type: :worker, liveness_reader: reader,
                    last_pipe_tick_at: 0.0, pipe_seen: false }
        }

        expect { supervisor.send(:drain_liveness_pipes) }.not_to raise_error
      end
    end
  end
end
