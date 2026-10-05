# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Process::Supervisor do
  let(:mock_heartbeat) { instance_double(Pgbus::Process::Heartbeat, start: nil, stop: nil) }
  let(:config) { Pgbus.configuration }

  before do
    allow(Pgbus::Process::Heartbeat).to receive(:new).and_return(mock_heartbeat)
  end

  describe "#initialize" do
    it "stores config and initializes empty forks" do
      supervisor = described_class.new

      expect(supervisor.config).to eq(config)
      expect(supervisor.forks).to eq({})
      expect(supervisor.shutting_down?).to be false
    end
  end

  describe "#graceful_shutdown" do
    it "sets shutting_down and signals children with TERM" do
      supervisor = described_class.new(forks: { 1001 => { type: :worker }, 1002 => { type: :dispatcher } })

      allow(Process).to receive(:kill)
      supervisor.graceful_shutdown

      expect(supervisor.shutting_down?).to be true
      expect(Process).to have_received(:kill).with("TERM", 1001)
      expect(Process).to have_received(:kill).with("TERM", 1002)
    end
  end

  describe "#immediate_shutdown" do
    it "sets shutting_down and signals children with QUIT" do
      supervisor = described_class.new(forks: { 2001 => { type: :worker } })

      allow(Process).to receive(:kill)
      supervisor.immediate_shutdown

      expect(supervisor.shutting_down?).to be true
      expect(Process).to have_received(:kill).with("QUIT", 2001)
    end
  end

  describe "signal_children (private)" do
    it "handles Errno::ESRCH when a child process is already gone" do
      supervisor = described_class.new(forks: { 9999 => { type: :worker } })

      allow(Process).to receive(:kill).with("TERM", 9999).and_raise(Errno::ESRCH)

      expect { supervisor.send(:signal_children, "TERM") }.not_to raise_error
    end
  end

  describe "reap_children (private)" do
    let(:supervisor) do
      described_class.new(forks: { 3001 => { type: :worker, config: { queues: ["default"] } } })
    end
    let(:status_double) { instance_double(Process::Status, exitstatus: 1, success?: false) }

    it "restarts a child when not shutting down" do
      allow(Process).to receive(:waitpid2).with(-1, Process::WNOHANG).and_return([3001, status_double], nil)
      allow(supervisor).to receive(:fork).and_return(4001)

      supervisor.send(:reap_children)

      expect(supervisor).to have_received(:fork)
    end

    # A worker that hits a recycle limit exits 0 by design. Logging that as
    # "exited unexpectedly" buries real crashes (issue #438).
    describe "exit logging" do
      let(:infos) { [] }
      let(:warnings) { [] }

      before do
        allow(supervisor).to receive(:schedule_restart)
        allow(Pgbus.logger).to receive(:info) { |&blk| infos << blk.call }
        allow(Pgbus.logger).to receive(:warn) { |&blk| warnings << blk.call }
      end

      it "logs a clean exit at info as a recycle restart" do
        clean = instance_double(Process::Status, exitstatus: 0, success?: true)
        allow(Process).to receive(:waitpid2).with(-1, Process::WNOHANG).and_return([3001, clean], nil)

        supervisor.send(:reap_children)

        expect(infos.join).to include("pid=3001", "exited cleanly", "status=0", "restarting")
        expect(warnings.join).not_to include("exited unexpectedly")
        expect(supervisor).to have_received(:schedule_restart).with(anything, clean)
      end

      it "logs a non-zero exit as unexpected" do
        allow(Process).to receive(:waitpid2).with(-1, Process::WNOHANG).and_return([3001, status_double], nil)

        supervisor.send(:reap_children)

        expect(warnings.join).to include("pid=3001", "exited unexpectedly", "status=1")
        expect(infos.join).not_to include("exited cleanly")
      end

      it "names the signal for a signaled exit" do
        killed = instance_double(Process::Status, exitstatus: nil, success?: nil, signaled?: true, termsig: 9)
        allow(Process).to receive(:waitpid2).with(-1, Process::WNOHANG).and_return([3001, killed], nil)

        supervisor.send(:reap_children)

        expect(warnings.join).to include("pid=3001", "exited unexpectedly", "signal=9")
      end
    end

    it "does NOT restart a child when shutting_down is true" do
      supervisor = described_class.new(
        forks: { 3001 => { type: :worker, config: { queues: ["default"] } } },
        shutting_down: true
      )
      allow(Process).to receive(:waitpid2).with(-1, Process::WNOHANG).and_return([3001, status_double], nil)

      supervisor.send(:reap_children)

      expect(supervisor.forks).not_to have_key(3001)
    end
  end

  # A child that dies right after forking (bad config, unreachable DB,
  # crashing initializer) must not be re-forked in a tight loop — that
  # burns CPU and floods logs. Crashes within the stable-uptime window
  # get an exponentially backed-off restart; stable children and clean
  # exits (worker recycling) restart immediately.
  describe "restart backoff for crash-looping children" do
    # Each example seeds a distinct set of child forks; inject them via the
    # constructor and re-apply the standard fork/log stubs via build_supervisor.
    let(:crash_status) { instance_double(Process::Status, exitstatus: 1, success?: false) }
    let(:clean_status) { instance_double(Process::Status, exitstatus: 0, success?: true) }
    let(:worker_config) { { queues: ["default"], threads: 5 } }

    def build_supervisor(forks)
      described_class.new(forks: forks).tap do |s|
        allow(s).to receive(:fork).and_return(6001)
        allow(Pgbus.logger).to receive(:warn)
      end
    end

    def now
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end

    it "restarts immediately when the crashed child had a stable uptime" do
      supervisor = build_supervisor(3001 => { type: :worker, config: worker_config, spawned_at: now - 120 })
      allow(Process).to receive(:waitpid2).and_return([3001, crash_status], nil)

      supervisor.send(:reap_children)

      expect(supervisor).to have_received(:fork).once
    end

    it "restarts immediately on a clean exit even with short uptime (worker recycling)" do
      supervisor = build_supervisor(3001 => { type: :worker, config: worker_config, spawned_at: now - 5 })
      allow(Process).to receive(:waitpid2).and_return([3001, clean_status], nil)

      supervisor.send(:reap_children)

      expect(supervisor).to have_received(:fork).once
    end

    it "delays the restart when the child crashed shortly after forking" do
      base = now
      supervisor = build_supervisor(3001 => { type: :worker, config: worker_config, spawned_at: base - 1 })
      allow(supervisor).to receive(:monotonic_now).and_return(base)
      allow(Process).to receive(:waitpid2).and_return([3001, crash_status], nil)

      supervisor.send(:reap_children)
      supervisor.send(:process_pending_restarts)

      expect(supervisor).not_to have_received(:fork)
    end

    it "keeps separate crash streaks for identically-configured sibling workers" do
      messages = []
      base = now
      supervisor = build_supervisor(sibling_forks(base))
      allow(Pgbus.logger).to receive(:warn) { |&blk| messages << blk.call }
      allow(supervisor).to receive(:fork).and_return(6001, 6002, 6003)
      allow(supervisor).to receive(:monotonic_now).and_return(base)

      reap(supervisor, 3002, crash_status) # slot 1 crashes fast — crash #1, 1s backoff
      reap(supervisor, 3001, clean_status) # slot 0 recycles cleanly — must NOT reset slot 1's streak

      # Slot 1's restart fires, then it crashes fast again — crash #2, 2s backoff.
      allow(supervisor).to receive(:monotonic_now).and_return(base + 61)
      supervisor.send(:process_pending_restarts)
      restarted_pid = supervisor.forks.find { |_, i| i[:slot] == 1 }.first
      reap(supervisor, restarted_pid, crash_status)

      expect(messages.join("\n")).to include("restarting in 2s")
    end

    def sibling_forks(base)
      { 3001 => { type: :worker, config: worker_config, slot: 0, spawned_at: base },
        3002 => { type: :worker, config: worker_config, slot: 1, spawned_at: base } }
    end

    def reap(supervisor, pid, status)
      allow(Process).to receive(:waitpid2).and_return([pid, status], nil)
      supervisor.send(:reap_children)
    end

    it "fires due pending restarts from the monitor loop" do
      supervisor = described_class.new(pending_restarts: [{ info: { type: :dispatcher }, at: now - 1 }])
      allow(supervisor).to receive(:fork).and_return(7001)
      allow(Pgbus.logger).to receive(:warn)
      allow(Process).to receive(:waitpid2).and_return(nil)
      # Stop the loop after one pass: flip shutdown, and let reap_children clear
      # the just-restarted fork so the `@shutting_down && @forks.empty?` guard
      # trips (mirrors the real path where the exited child is reaped).
      allow(supervisor).to receive(:interruptible_sleep) do
        supervisor.graceful_shutdown
        allow(supervisor).to receive(:reap_children) { supervisor.forks.clear }
      end
      allow(supervisor).to receive(:check_stalled_workers)

      supervisor.send(:monitor_loop)

      expect(supervisor).to have_received(:fork).once
    end

    it "does not fire pending restarts once shutting down" do
      supervisor = described_class.new(
        forks: { 8001 => { type: :worker, config: worker_config, spawned_at: now } },
        shutting_down: true,
        pending_restarts: [{ info: { type: :dispatcher }, at: now - 1 }]
      )
      allow(supervisor).to receive(:fork)
      allow(Pgbus.logger).to receive(:warn)
      allow(Process).to receive(:waitpid2).and_return([8001, crash_status], nil)
      allow(supervisor).to receive(:interruptible_sleep)

      supervisor.send(:monitor_loop)

      expect(supervisor).not_to have_received(:fork)
    end

    it "runs the pending restart once its backoff has elapsed" do
      base = now
      supervisor = build_supervisor(3001 => { type: :worker, config: worker_config, spawned_at: base })
      allow(supervisor).to receive(:monotonic_now).and_return(base)
      allow(Process).to receive(:waitpid2).and_return([3001, crash_status], nil)

      supervisor.send(:reap_children)
      expect(supervisor).not_to have_received(:fork)

      allow(supervisor).to receive(:monotonic_now).and_return(base + 61)
      supervisor.send(:process_pending_restarts)

      expect(supervisor).to have_received(:fork)
    end

    it "escalates the backoff on consecutive rapid crashes" do
      messages = []
      base = now
      supervisor = build_supervisor(3001 => { type: :worker, config: worker_config, spawned_at: base })
      allow(Pgbus.logger).to receive(:warn) { |&blk| messages << blk.call }
      allow(supervisor).to receive(:monotonic_now).and_return(base)
      allow(Process).to receive(:waitpid2).and_return([3001, crash_status], nil)
      supervisor.send(:reap_children)

      # First backoff elapses; the child is restarted, then crashes again fast.
      allow(supervisor).to receive(:monotonic_now).and_return(base + 61)
      supervisor.send(:process_pending_restarts)
      allow(Process).to receive(:waitpid2).and_return([6001, crash_status], nil)
      supervisor.send(:reap_children)

      joined = messages.join("\n")
      expect(joined).to include("restarting in 1s")
      expect(joined).to include("restarting in 2s")
    end

    it "resets the crash streak after a stable run" do
      messages = []
      base = now
      supervisor = build_supervisor(3001 => { type: :worker, config: worker_config, spawned_at: base })
      allow(Pgbus.logger).to receive(:warn) { |&blk| messages << blk.call }
      allow(supervisor).to receive(:monotonic_now).and_return(base)
      allow(Process).to receive(:waitpid2).and_return([3001, crash_status], nil)
      supervisor.send(:reap_children)

      # Backoff elapses, child restarts and then runs well past the stable window.
      allow(supervisor).to receive(:monotonic_now).and_return(base + 61)
      supervisor.send(:process_pending_restarts)

      # Stable crash: restarts immediately and clears the streak...
      allow(supervisor).to receive(:monotonic_now).and_return(base + 200)
      allow(Process).to receive(:waitpid2).and_return([6001, crash_status], nil)
      supervisor.send(:reap_children)

      # ...so the next rapid crash starts back at the base backoff.
      allow(supervisor).to receive(:monotonic_now).and_return(base + 201)
      allow(Process).to receive(:waitpid2).and_return([6001, crash_status], nil)
      supervisor.send(:reap_children)

      expect(messages.join("\n").scan("restarting in 1s").size).to eq(2)
    end
  end

  describe "restart_child (private)" do
    let(:supervisor) { described_class.new }

    before do
      allow(supervisor).to receive(:fork).and_return(5001)
    end

    it "routes :worker to fork_worker" do
      info = { type: :worker, config: { queues: ["default"], threads: 5 } }
      supervisor.send(:restart_child, info)

      expect(supervisor.forks).to have_key(5001)
      expect(supervisor.forks[5001][:type]).to eq(:worker)
    end

    it "routes :dispatcher to fork_dispatcher" do
      info = { type: :dispatcher }
      supervisor.send(:restart_child, info)

      expect(supervisor.forks).to have_key(5001)
      expect(supervisor.forks[5001][:type]).to eq(:dispatcher)
    end

    it "routes :consumer to fork_consumer" do
      info = { type: :consumer, config: { topics: ["orders.#"], threads: 3 } }
      supervisor.send(:restart_child, info)

      expect(supervisor.forks).to have_key(5001)
      expect(supervisor.forks[5001][:type]).to eq(:consumer)
    end

    it "routes :scheduler to fork_scheduler" do
      info = { type: :scheduler }
      supervisor.send(:restart_child, info)

      expect(supervisor.forks).to have_key(5001)
      expect(supervisor.forks[5001][:type]).to eq(:scheduler)
    end
  end

  describe "worker liveness pipe (private)" do
    let(:supervisor) { described_class.new }
    let(:worker_config) { { queues: ["default"], threads: 5 } }

    # With fork stubbed to return a pid, the child block never runs, so the
    # parent path (create pipe, close writer, store reader) is exercised.
    before do
      allow(supervisor).to receive(:fork).and_return(7001)
    end

    describe "fork_worker" do
      it "stores a liveness_reader IO for the forked worker" do
        supervisor.send(:fork_worker, worker_config, slot: 0)

        info = supervisor.forks[7001]
        expect(info[:liveness_reader]).to be_a(IO)
        expect(info[:liveness_reader]).not_to be_closed
      end

      it "seeds last_pipe_tick_at (monotonic) and leaves pipe_seen unarmed" do
        supervisor.send(:fork_worker, worker_config, slot: 0)

        info = supervisor.forks[7001]
        expect(info[:last_pipe_tick_at]).to be_a(Float)
        expect(info[:pipe_seen]).to be(false)
      end

      it "closes both pipe ends and stores no reader when the fork fails (nil pid)" do
        allow(supervisor).to receive(:fork).and_return(nil)
        created = []
        allow(IO).to receive(:pipe).and_wrap_original do |orig|
          pair = orig.call
          created.concat(pair)
          pair
        end

        supervisor.send(:fork_worker, worker_config, slot: 0)

        expect(supervisor.forks).not_to have_key(7001)
        expect(created).to all(be_closed)
      end

      it "closes both pipe ends when the fork raises Errno::EAGAIN" do
        allow(supervisor).to receive(:fork).and_raise(Errno::EAGAIN)
        created = []
        allow(IO).to receive(:pipe).and_wrap_original do |orig|
          pair = orig.call
          created.concat(pair)
          pair
        end

        expect { supervisor.send(:fork_worker, worker_config, slot: 0) }.not_to raise_error
        expect(created).to all(be_closed)
      end
    end

    # Consumer forks get the same OS-pipe liveness channel as workers (issue
    # #274) so the watchdog can detect a wedged consumer without the database.
    describe "fork_consumer (issue #274)" do
      let(:consumer_config) { { topics: ["orders.#"], threads: 3 } }

      it "stores an open liveness_reader IO for the forked consumer" do
        supervisor.send(:fork_consumer, consumer_config, slot: 0)

        info = supervisor.forks[7001]
        expect(info[:type]).to eq(:consumer)
        expect(info[:liveness_reader]).to be_a(IO)
        expect(info[:liveness_reader]).not_to be_closed
      end

      it "seeds last_pipe_tick_at (monotonic) and leaves pipe_seen unarmed" do
        supervisor.send(:fork_consumer, consumer_config, slot: 0)

        info = supervisor.forks[7001]
        expect(info[:last_pipe_tick_at]).to be_a(Float)
        expect(info[:pipe_seen]).to be(false)
      end

      it "closes both pipe ends and stores no reader when the fork fails (nil pid)" do
        allow(supervisor).to receive(:fork).and_return(nil)
        created = []
        allow(IO).to receive(:pipe).and_wrap_original do |orig|
          pair = orig.call
          created.concat(pair)
          pair
        end

        supervisor.send(:fork_consumer, consumer_config, slot: 0)

        expect(supervisor.forks).not_to have_key(7001)
        expect(created).to all(be_closed)
      end
    end

    describe "reap_children closes the liveness reader" do
      let(:status_double) { instance_double(Process::Status, exitstatus: 0, success?: true) }

      it "closes the reader and drops the fork on reap" do
        reader, writer = IO.pipe
        writer.close
        supervisor = described_class.new(
          forks: { 3001 => { type: :worker, config: worker_config, spawned_at: 0.0,
                             liveness_reader: reader, last_pipe_tick_at: 0.0, pipe_seen: true } },
          shutting_down: true
        )
        allow(Process).to receive(:waitpid2).with(-1, Process::WNOHANG).and_return([3001, status_double], nil)

        supervisor.send(:reap_children)

        expect(reader).to be_closed
        expect(supervisor.forks).not_to have_key(3001)
      end
    end

    describe "shutdown closes remaining liveness readers" do
      it "closes any un-reaped worker readers" do
        reader, writer = IO.pipe
        writer.close
        supervisor = described_class.new(
          forks: { 3001 => { type: :worker, config: worker_config, liveness_reader: reader } }
        )
        allow(supervisor).to receive(:reap_children)
        allow(supervisor).to receive(:interruptible_sleep)
        allow(Process).to receive(:kill)

        supervisor.send(:shutdown)

        expect(reader).to be_closed
      end
    end
  end

  describe "bootstrap_queues (private)" do
    let(:supervisor) { described_class.new }
    let(:mock_client) { build_mock_client }

    before do
      allow(Pgbus).to receive(:client).and_return(mock_client)
      allow(mock_client).to receive(:ensure_all_queues)
    end

    it "calls ensure_all_queues on the client" do
      supervisor.send(:bootstrap_queues)

      expect(mock_client).to have_received(:ensure_all_queues).once
    end

    it "rescues errors and logs them" do
      allow(mock_client).to receive(:ensure_all_queues).and_raise(StandardError, "connection failed")
      allow(Pgbus.logger).to receive(:error)

      expect { supervisor.send(:bootstrap_queues) }.not_to raise_error
      expect(Pgbus.logger).to have_received(:error).at_least(:once)
    end

    it "swallows SchemaNotReady so the parent survives a missing schema at boot" do
      allow(mock_client).to receive(:ensure_all_queues)
        .and_raise(Pgbus::SchemaNotReady, "PGMQ schema installation failed. Ensure migrations have been run.")
      allow(Pgbus.logger).to receive(:error)

      expect { supervisor.send(:bootstrap_queues) }.not_to raise_error
    end
  end

  describe "bootstrap_queues! (private)" do
    let(:supervisor) { described_class.new }
    let(:mock_client) { build_mock_client }

    before do
      allow(Pgbus).to receive(:client).and_return(mock_client)
      allow(mock_client).to receive(:ensure_all_queues)
    end

    it "calls ensure_all_queues on the client" do
      supervisor.send(:bootstrap_queues!)

      expect(mock_client).to have_received(:ensure_all_queues).once
    end

    it "logs one actionable error and re-raises on SchemaNotReady" do
      message = "PGMQ schema installation failed. Ensure the pgbus database exists and migrations have been run."
      allow(mock_client).to receive(:ensure_all_queues).and_raise(Pgbus::SchemaNotReady, message)
      allow(Pgbus.logger).to receive(:error)

      expect { supervisor.send(:bootstrap_queues!) }.to raise_error(Pgbus::SchemaNotReady, message)
      expect(Pgbus.logger).to have_received(:error).once
    end

    it "reports and swallows a non-schema StandardError, as the lenient variant does" do
      allow(mock_client).to receive(:ensure_all_queues).and_raise(StandardError, "connection failed")
      allow(Pgbus.logger).to receive(:error)

      expect { supervisor.send(:bootstrap_queues!) }.not_to raise_error
    end
  end

  describe "pre-fork bootstrap" do
    let(:supervisor) { described_class.new }
    let(:mock_client) { build_mock_client }

    before do
      allow(Pgbus).to receive(:client).and_return(mock_client)
      # run now emits log_boot_banner, which reads the installed PGMQ version.
      allow(mock_client).to receive_messages(
        ensure_all_queues: nil, verify_connection!: true, pgmq_schema_version: "1.5.0"
      )
    end

    it "calls bootstrap_queues before boot_processes" do
      call_order = []
      allow(supervisor).to receive(:bootstrap_queues) { call_order << :bootstrap }
      allow(supervisor).to receive(:boot_processes) { call_order << :boot }
      allow(supervisor).to receive(:setup_signals)
      allow(supervisor).to receive(:start_heartbeat)
      allow(supervisor).to receive(:monitor_loop)
      allow(supervisor).to receive(:shutdown)

      supervisor.run

      expect(call_order).to eq(%i[bootstrap boot])
    end

    it "verifies the database connection once before bootstrapping queues" do
      call_order = []
      allow(mock_client).to receive(:verify_connection!) { call_order << :verify }
      allow(supervisor).to receive(:bootstrap_queues) { call_order << :bootstrap }
      allow(supervisor).to receive(:boot_processes) { call_order << :boot }
      allow(supervisor).to receive(:setup_signals)
      allow(supervisor).to receive(:start_heartbeat)
      allow(supervisor).to receive(:monitor_loop)
      allow(supervisor).to receive(:shutdown)

      supervisor.run

      expect(call_order).to eq(%i[verify bootstrap boot])
      expect(mock_client).to have_received(:verify_connection!).once
    end

    it "propagates a ConfigurationError from verification without booting children" do
      allow(mock_client).to receive(:verify_connection!)
        .and_raise(Pgbus::ConfigurationError, "Database connection failed via database_url: nope")
      allow(supervisor).to receive(:bootstrap_queues)
      allow(supervisor).to receive(:boot_processes)
      allow(supervisor).to receive(:setup_signals)
      allow(supervisor).to receive(:start_heartbeat)
      allow(supervisor).to receive(:monitor_loop)
      allow(supervisor).to receive(:shutdown)

      expect { supervisor.run }.to raise_error(Pgbus::ConfigurationError, /database_url/)

      expect(supervisor).not_to have_received(:bootstrap_queues)
      expect(supervisor).not_to have_received(:boot_processes)
    end

    it "rescues bootstrap errors in the parent without aborting" do
      allow(supervisor).to receive(:bootstrap_queues).and_call_original
      allow(mock_client).to receive(:ensure_all_queues).and_raise(StandardError, "pg down")
      allow(supervisor).to receive(:boot_processes)
      allow(supervisor).to receive(:setup_signals)
      allow(supervisor).to receive(:start_heartbeat)
      allow(supervisor).to receive(:monitor_loop)
      allow(supervisor).to receive(:shutdown)
      allow(Pgbus.logger).to receive(:error)

      expect { supervisor.run }.not_to raise_error
    end
  end

  # Issue #347: doctor preflight runs inside the booting supervisor (one Rails
  # boot instead of two), between bootstrap_queues and boot_processes.
  describe "doctor-on-boot preflight" do
    let(:config) { Pgbus::Configuration.new }
    let(:supervisor) { described_class.new(config: config) }
    let(:mock_client) { build_mock_client }
    let(:doctor) { instance_double(Pgbus::Doctor) }

    before do
      allow(Pgbus).to receive(:client).and_return(mock_client)
      allow(mock_client).to receive_messages(
        ensure_all_queues: nil, verify_connection!: true, pgmq_schema_version: "1.5.0"
      )
      # Isolate the full boot path from real forking/signals/loops.
      allow(supervisor).to receive(:setup_signals)
      allow(supervisor).to receive(:start_heartbeat)
      allow(supervisor).to receive(:start_health_server)
      allow(supervisor).to receive(:boot_processes)
      allow(supervisor).to receive(:monitor_loop)
      allow(supervisor).to receive(:shutdown)
      allow(Pgbus.logger).to receive(:info)
      allow(Pgbus.logger).to receive(:error)
    end

    context "when doctor_on_boot is off (default nil)" do
      it "never constructs a Doctor and boots normally" do
        allow(Pgbus::Doctor).to receive(:new)
        supervisor.run
        expect(Pgbus::Doctor).not_to have_received(:new)
        expect(supervisor).to have_received(:boot_processes)
      end
    end

    context "when doctor_on_boot is :report" do
      before do
        config.doctor_on_boot = :report
        allow(Pgbus::Doctor).to receive(:new).and_return(doctor)
        allow(doctor).to receive_messages(boot_report: "REPORT", boot_ok?: false)
      end

      it "runs the boot preflight and logs the report, then boots regardless of findings" do
        supervisor.run
        expect(doctor).to have_received(:boot_report)
        expect(supervisor).to have_received(:boot_processes)
      end

      it "builds the Doctor with the supervisor's own config and the shared client" do
        supervisor.run
        expect(Pgbus::Doctor).to have_received(:new).with(config: config, client: mock_client)
      end

      it "runs the preflight after bootstrap_queues and before boot_processes" do
        call_order = []
        allow(supervisor).to receive(:bootstrap_queues) { call_order << :bootstrap }
        allow(supervisor).to receive(:run_doctor_preflight) { call_order << :preflight }
        allow(supervisor).to receive(:boot_processes) { call_order << :boot }

        supervisor.run

        expect(call_order).to eq(%i[bootstrap preflight boot])
      end
    end

    context "when doctor_on_boot is :strict" do
      before do
        config.doctor_on_boot = :strict
        allow(Pgbus::Doctor).to receive(:new).and_return(doctor)
        allow(doctor).to receive(:boot_report).and_return("REPORT")
      end

      it "boots when the strict preflight passes" do
        allow(doctor).to receive(:boot_ok?).and_return(true)
        supervisor.run
        expect(supervisor).to have_received(:boot_processes)
      end

      it "raises ConfigurationError and forks NOTHING when a strict-fatal check fails" do
        allow(doctor).to receive(:boot_ok?).and_return(false)

        expect { supervisor.run }.to raise_error(Pgbus::ConfigurationError, /doctor/i)
        expect(supervisor).not_to have_received(:boot_processes)
      end

      it "logs the report before raising so the operator sees which check failed" do
        allow(doctor).to receive(:boot_ok?).and_return(false)

        supervisor.run
      rescue Pgbus::ConfigurationError
        expect(doctor).to have_received(:boot_report)
      end

      it "tears down the heartbeat and health server on a strict abort (ensure shutdown)" do
        allow(doctor).to receive(:boot_ok?).and_return(false)

        expect { supervisor.run }.to raise_error(Pgbus::ConfigurationError)
        expect(supervisor).to have_received(:shutdown)
        expect(supervisor.forks).to be_empty
      end
    end
  end

  describe "boot diagnostics banner (private)" do
    let(:supervisor) { described_class.new(config: config) }
    let(:config) { Pgbus::Configuration.new }
    let(:messages) { [] }

    before do
      allow(Pgbus.logger).to receive(:info) { |&blk| messages << blk.call if blk }
    end

    # Capture only the banner text: every banner line is prefixed "[Pgbus] boot:".
    def banner
      messages.select { |m| m.to_s.start_with?("[Pgbus] boot:") }.join("\n")
    end

    it "logs one banner block with version, pool size, pgmq mode, notify flags, and roles" do
      config.database_url = "postgres://u:sekret@db:5432/app"
      allow(Pgbus.client).to receive(:pgmq_schema_version).and_return("1.5.0")

      supervisor.send(:log_boot_banner)

      expect(banner).to include("pgbus #{Pgbus::VERSION}")
      expect(banner).to include("pool=#{config.resolved_pool_size}")
      expect(banner).to include("pgmq_schema_mode=auto")
      expect(banner).to include("pgmq_version=1.5.0")
      expect(banner).to include("listen_notify=true")
      expect(banner).to include("worker_notify_wakeup=true")
    end

    # Issue #484: a production host that lost YJIT shows it in the first log lines.
    it "names the Ruby JIT on the version line" do
      allow(Pgbus.client).to receive(:pgmq_schema_version).and_return(nil)
      allow(Pgbus::RubyJit).to receive(:label).and_return("yjit")

      supervisor.send(:log_boot_banner)

      expect(banner).to match(/boot: pgbus \S+ pid=\d+ jit=yjit/)
    end

    it "renders the connection target as host + dbname without the password" do
      config.database_url = "postgres://u:sekret@db:5432/app"
      allow(Pgbus.client).to receive(:pgmq_schema_version).and_return(nil)

      supervisor.send(:log_boot_banner)

      expect(banner).to include("db")
      expect(banner).to include("app")
      expect(banner).not_to include("sekret")
    end

    it "redacts the password from a connection_params hash" do
      config.connection_params = { host: "pghost", dbname: "myapp", user: "u", password: "sekret" }
      allow(Pgbus.client).to receive(:pgmq_schema_version).and_return(nil)

      supervisor.send(:log_boot_banner)

      expect(banner).to include("pghost")
      expect(banner).to include("myapp")
      expect(banner).not_to include("sekret")
    end

    it "degrades the pgmq version to 'unknown' when the lookup raises, without aborting" do
      config.database_url = "postgres://u:sekret@db:5432/app"
      allow(Pgbus.client).to receive(:pgmq_schema_version).and_raise(StandardError, "pg down")

      expect { supervisor.send(:log_boot_banner) }.not_to raise_error
      expect(banner).to include("pgmq_version=unknown")
    end

    # Distinct from the raising case above: PGMQ is reachable but nothing is
    # tracked yet, so pgmq_schema_version returns nil. Locks in the `|| "unknown"`
    # branch separately from the rescue branch.
    it "renders the pgmq version as 'unknown' when the lookup returns nil (not installed)" do
      config.database_url = "postgres://u:sekret@db:5432/app"
      allow(Pgbus.client).to receive(:pgmq_schema_version).and_return(nil)

      supervisor.send(:log_boot_banner)

      expect(banner).to include("pgmq_version=unknown")
    end

    it "reflects a narrowed roles list" do
      config.database_url = "postgres://u:sekret@db:5432/app"
      config.roles = [:workers]
      allow(Pgbus.client).to receive(:pgmq_schema_version).and_return(nil)

      supervisor.send(:log_boot_banner)

      expect(banner).to include("workers")
      expect(banner).not_to match(/roles=.*dispatcher/)
    end

    it "lists worker capsules with queues, threads, and execution mode" do
      config.database_url = "postgres://u:sekret@db:5432/app"
      config.workers = [
        { name: "critical", queues: %w[critical], threads: 5 },
        { name: "default", queues: %w[default low], threads: 3 }
      ]
      allow(Pgbus.client).to receive(:pgmq_schema_version).and_return(nil)

      supervisor.send(:log_boot_banner)

      expect(banner).to include("critical")
      expect(banner).to include("queues=critical")
      expect(banner).to include("threads=5")
      expect(banner).to include("queues=default,low")
      expect(banner).to include("threads=3")
    end

    it "names an unnamed capsule 'anonymous'" do
      config.database_url = "postgres://u:sekret@db:5432/app"
      config.workers = [{ queues: %w[default], threads: 5 }]
      allow(Pgbus.client).to receive(:pgmq_schema_version).and_return(nil)

      supervisor.send(:log_boot_banner)

      expect(banner).to include("anonymous")
    end

    it "lists event consumers with topics and threads" do
      config.database_url = "postgres://u:sekret@db:5432/app"
      config.event_consumers = [{ topics: %w[orders.# payments.#], threads: 4 }]
      allow(Pgbus.client).to receive(:pgmq_schema_version).and_return(nil)

      supervisor.send(:log_boot_banner)

      expect(banner).to include("orders.#,payments.#")
      expect(banner).to include("threads=4")
    end

    it "renders under the JSON log formatter without raising" do
      config.database_url = "postgres://u:sekret@db:5432/app"
      config.log_format = :json
      allow(Pgbus.client).to receive(:pgmq_schema_version).and_return("1.5.0")

      formatted = []
      formatter = config.logger.formatter
      allow(Pgbus.logger).to receive(:info) do |&blk|
        line = blk.call
        formatted << formatter.call("INFO", Time.now, nil, line)
      end

      expect { supervisor.send(:log_boot_banner) }.not_to raise_error
      json_lines = formatted.select { |l| l.include?("boot:") }
      expect(json_lines).not_to be_empty
      json_lines.each { |l| expect { JSON.parse(l) }.not_to raise_error }
      expect(formatted.join).not_to include("sekret")
    end
  end

  describe "banner is emitted during #run" do
    let(:supervisor) { described_class.new(config: config) }
    let(:config) { Pgbus::Configuration.new }
    let(:mock_client) { build_mock_client }

    before do
      config.database_url = "postgres://u:sekret@db:5432/app"
      allow(Pgbus).to receive(:client).and_return(mock_client)
      allow(mock_client).to receive_messages(
        verify_connection!: true, ensure_all_queues: nil, pgmq_schema_version: "1.5.0"
      )
    end

    it "logs the banner after start_heartbeat and before bootstrap_queues" do
      call_order = []
      allow(supervisor).to receive(:setup_signals)
      allow(supervisor).to receive(:start_heartbeat) { call_order << :heartbeat }
      allow(supervisor).to receive(:log_boot_banner) { call_order << :banner }
      allow(supervisor).to receive(:bootstrap_queues) { call_order << :bootstrap }
      allow(supervisor).to receive(:boot_processes)
      allow(supervisor).to receive(:monitor_loop)
      allow(supervisor).to receive(:shutdown)

      supervisor.run

      expect(call_order).to eq(%i[heartbeat banner bootstrap])
    end
  end

  describe "standalone health server" do
    let(:supervisor) { described_class.new(config: config) }
    let(:config) { Pgbus::Configuration.new }
    let(:mock_client) { build_mock_client }
    let(:health_server) { instance_double(Pgbus::Web::HealthServer, start: nil, stop: nil) }

    before do
      config.database_url = "postgres://u:sekret@db:5432/app"
      allow(Pgbus).to receive(:client).and_return(mock_client)
      allow(mock_client).to receive_messages(
        verify_connection!: true, ensure_all_queues: nil, pgmq_schema_version: "1.5.0"
      )
      allow(Pgbus::Web::HealthServer).to receive(:new).and_return(health_server)
      allow(supervisor).to receive(:setup_signals)
      allow(supervisor).to receive(:log_boot_banner)
      allow(supervisor).to receive(:boot_processes)
      allow(supervisor).to receive(:monitor_loop)
    end

    it "does not build a health server when health_port is nil" do
      config.health_port = nil

      supervisor.run

      expect(Pgbus::Web::HealthServer).not_to have_received(:new)
    end

    it "starts a health server with the configured port and bind when health_port is set" do
      config.health_port = 9394
      config.health_bind = "0.0.0.0"

      supervisor.run

      expect(Pgbus::Web::HealthServer).to have_received(:new)
        .with(port: 9394, bind: "0.0.0.0", app: an_instance_of(Pgbus::Web::HealthApp))
      expect(health_server).to have_received(:start)
    end

    it "wires /readyz to the supervisor's container-local readiness (issue #386)" do
      config.health_port = 9394
      captured_app = nil
      allow(Pgbus::Web::HealthServer).to receive(:new) do |**kwargs|
        captured_app = kwargs[:app]
        health_server
      end

      supervisor.run
      status, _headers, body = captured_app.call("REQUEST_METHOD" => "GET", "PATH_INFO" => "/readyz")

      expect(status).to eq(200)
      expect(JSON.parse(body.join)).to include("status" => "OK", "expected" => 0, "live" => 0)
    end

    it "starts the health server after the heartbeat" do
      config.health_port = 9394
      call_order = []
      allow(supervisor).to receive(:start_heartbeat) { call_order << :heartbeat }
      allow(health_server).to receive(:start) { call_order << :health }

      supervisor.run

      expect(call_order).to eq(%i[heartbeat health])
    end

    it "stops the health server on shutdown" do
      config.health_port = 9394

      supervisor.run

      expect(health_server).to have_received(:stop)
    end
  end

  describe "readiness snapshot (issue #386)" do
    it "starts BOOTING and not ready" do
      snapshot = described_class.new.readiness_snapshot

      expect(snapshot.ready?).to be false
      expect(snapshot.status).to eq("BOOTING")
    end

    it "becomes ready once boot completes with all forked children counted" do
      supervisor = described_class.new(forks: { 101 => { type: :worker }, 102 => { type: :dispatcher } })

      supervisor.send(:mark_booted)
      snapshot = supervisor.readiness_snapshot

      expect(snapshot.ready?).to be true
      expect(snapshot.expected).to eq(2)
      expect(snapshot.live).to eq(2)
    end

    it "degrades when a child dies and has not been restarted" do
      supervisor = described_class.new(forks: { 101 => { type: :worker }, 102 => { type: :worker } })
      supervisor.send(:mark_booted)

      supervisor.forks.delete(102)
      supervisor.send(:refresh_readiness)
      snapshot = supervisor.readiness_snapshot

      expect(snapshot.ready?).to be false
      expect(snapshot.status).to eq("DEGRADED")
      expect(snapshot.live).to eq(1)
    end

    it "flips to DRAINING on graceful_shutdown" do
      supervisor = described_class.new(forks: { 101 => { type: :worker } })
      supervisor.send(:mark_booted)
      allow(Process).to receive(:kill)

      supervisor.graceful_shutdown

      expect(supervisor.readiness_snapshot.status).to eq("DRAINING")
      expect(supervisor.readiness_snapshot.ready?).to be false
    end

    it "flips to DRAINING on immediate_shutdown" do
      supervisor = described_class.new(forks: { 101 => { type: :worker } })
      supervisor.send(:mark_booted)
      allow(Process).to receive(:kill)

      supervisor.immediate_shutdown

      expect(supervisor.readiness_snapshot.status).to eq("DRAINING")
    end

    it "fails the readiness gate when a boot-time fork failed (intended > live)" do
      supervisor = described_class.new
      allow(supervisor).to receive(:fork).and_return(nil)

      supervisor.send(:fork_worker, { queues: ["default"] }, slot: 0)
      supervisor.send(:mark_booted)
      snapshot = supervisor.readiness_snapshot

      expect(snapshot.expected).to eq(1)
      expect(snapshot.live).to eq(0)
      expect(snapshot.status).to eq("DEGRADED")
    end

    it "does not inflate the baseline when a child is re-forked after boot" do
      supervisor = described_class.new
      allow(supervisor).to receive(:fork).and_return(7001)

      supervisor.send(:fork_worker, { queues: ["default"] }, slot: 0)
      supervisor.send(:mark_booted)
      allow(supervisor).to receive(:fork).and_return(7002)
      supervisor.send(:fork_worker, { queues: ["default"] }, slot: 0)
      supervisor.send(:refresh_readiness)

      expect(supervisor.readiness_snapshot.expected).to eq(1)
    end

    it "is marked booted by #run after boot_processes, before monitor_loop runs" do
      supervisor = described_class.new
      mock_client = build_mock_client
      allow(Pgbus).to receive(:client).and_return(mock_client)
      allow(mock_client).to receive_messages(
        verify_connection!: true, ensure_all_queues: nil, pgmq_schema_version: "1.5.0"
      )
      allow(supervisor).to receive_messages(setup_signals: nil, log_boot_banner: nil, boot_processes: nil)
      seen = nil
      allow(supervisor).to receive(:monitor_loop) { seen = supervisor.readiness_snapshot }

      supervisor.run

      expect(seen.booted).to be true
    end
  end

  describe "shutdown deadline (issue #386)" do
    it "waits config.shutdown_timeout for children before escalating to SIGKILL" do
      config = Pgbus::Configuration.new
      config.shutdown_timeout = 42
      supervisor = described_class.new(config: config, forks: { 5001 => { type: :worker } })
      allow(supervisor).to receive(:interruptible_sleep)
      allow(Process).to receive(:waitpid2).and_return(nil)
      allow(Process).to receive(:kill)
      t0 = Time.now
      allow(Time).to receive(:now).and_return(t0, t0 + 41, t0 + 43)

      supervisor.send(:shutdown)

      expect(Process).to have_received(:kill).with("KILL", 5001)
    end
  end

  describe "recurring_tasks_configured? (private)" do
    let(:supervisor) { described_class.new }

    it "returns true when recurring_tasks are set in config" do
      config.recurring_tasks = { "task1" => { "class" => "MyJob", "schedule" => "0 * * * *" } }
      expect(supervisor.send(:recurring_tasks_configured?)).to be true
      config.recurring_tasks = nil
    end

    it "returns false when nothing is configured" do
      config.recurring_tasks = nil
      config.recurring_tasks_file = nil
      expect(supervisor.send(:recurring_tasks_configured?)).to be false
    end
  end

  describe "NotifyHub wiring (issue #381)" do
    let(:hub) do
      instance_double(Pgbus::Process::NotifyHub,
                      start: nil, stop: nil, tick: nil,
                      register_fork: nil, deregister_fork: nil)
    end

    # worker_notify_scope / roles / workers live on the global config;
    # snapshot and restore so these examples can't leak into the rest of
    # the suite.
    around do |example|
      original_scope = config.worker_notify_scope
      original_roles = config.roles
      original_workers = config.workers
      example.run
      config.worker_notify_scope = original_scope
      config.roles = original_roles
      config.workers = original_workers
    end

    describe "start_notify_hub (private)" do
      before { allow(Pgbus::Process::NotifyHub).to receive(:new).and_return(hub) }

      it "builds and starts the hub under :supervisor scope" do
        config.worker_notify_scope = :supervisor
        supervisor = described_class.new

        supervisor.send(:start_notify_hub)

        expect(hub).to have_received(:start)
        expect(supervisor.notify_hub).to be(hub)
      end

      it "skips the hub under :fork scope" do
        config.worker_notify_scope = :fork
        supervisor = described_class.new

        supervisor.send(:start_notify_hub)

        expect(supervisor.notify_hub).to be_nil
      end

      it "skips the hub when worker_notify_wakeup? is off" do
        config.worker_notify_scope = :supervisor
        allow(config).to receive(:worker_notify_wakeup?).and_return(false)
        supervisor = described_class.new

        supervisor.send(:start_notify_hub)

        expect(supervisor.notify_hub).to be_nil
      end

      it "skips the hub when neither workers nor consumers roles are enabled" do
        config.worker_notify_scope = :supervisor
        config.roles = [:dispatcher]
        supervisor = described_class.new

        supervisor.send(:start_notify_hub)

        expect(supervisor.notify_hub).to be_nil
      end

      it "degrades to no hub (forks poll) when hub start raises" do
        config.worker_notify_scope = :supervisor
        allow(hub).to receive(:start).and_raise(StandardError, "boom")
        supervisor = described_class.new

        expect { supervisor.send(:start_notify_hub) }.not_to raise_error
        expect(supervisor.notify_hub).to be_nil
      end
    end

    describe "wake pipes on fork" do
      let(:supervisor) do
        described_class.new(notify_hub: hub).tap do |s|
          allow(s).to receive(:fork).and_return(4001)
        end
      end

      it "registers a worker fork with its physical queues and the pipe write end" do
        supervisor.send(:fork_worker, { queues: %w[critical], threads: 1 }, slot: 0)

        expect(hub).to have_received(:register_fork).with(
          pid: 4001,
          queues: [config.queue_name("critical")],
          wildcard: false,
          pipe: an_instance_of(IO)
        )
        expect(supervisor.forks[4001][:wake_writer]).to be_an(IO)
      end

      it "registers a wildcard capsule with wildcard: true" do
        supervisor.send(:fork_worker, { queues: %w[*], threads: 1 }, slot: 0)

        expect(hub).to have_received(:register_fork).with(
          pid: 4001, queues: [], wildcard: true, pipe: an_instance_of(IO)
        )
      end

      it "registers a consumer fork with registry-derived physical queues" do
        registry = instance_double(Pgbus::EventBus::Registry)
        allow(Pgbus::EventBus::Registry).to receive(:instance).and_return(registry)
        allow(registry).to receive(:queue_names_for_topics)
          .with(["orders.#"]).and_return(%w[orders_handler])

        supervisor.send(:fork_consumer, { topics: ["orders.#"], threads: 1 }, slot: 0)

        expect(hub).to have_received(:register_fork).with(
          pid: 4001,
          queues: [config.queue_name("orders_handler")],
          wildcard: false,
          pipe: an_instance_of(IO)
        )
      end

      it "creates no wake pipe without a hub" do
        bare = described_class.new
        allow(bare).to receive(:fork).and_return(4002)

        bare.send(:fork_worker, { queues: %w[critical], threads: 1 }, slot: 0)

        expect(bare.forks[4002][:wake_writer]).to be_nil
      end
    end

    describe "reap deregistration" do
      it "deregisters a reaped fork from the hub" do
        supervisor = described_class.new(
          notify_hub: hub,
          forks: { 3001 => { type: :worker, config: { queues: ["default"] } } },
          shutting_down: true
        )
        status = instance_double(Process::Status, exitstatus: 0, success?: true)
        allow(Process).to receive(:waitpid2).and_return([3001, status], nil)

        supervisor.send(:reap_children)

        expect(hub).to have_received(:deregister_fork).with(3001)
      end
    end

    describe "monitor tick" do
      it "beats the hub once per monitor pass" do
        supervisor = described_class.new(notify_hub: hub)
        allow(Process).to receive(:waitpid2).and_return(nil)
        allow(supervisor).to receive(:check_stalled_workers)
        allow(supervisor).to receive(:interruptible_sleep) do
          supervisor.graceful_shutdown
          allow(supervisor).to receive(:reap_children) { supervisor.forks.clear }
        end
        allow(Process).to receive(:kill)

        supervisor.send(:monitor_loop)

        expect(hub).to have_received(:tick).at_least(:once)
      end
    end

    describe "shutdown" do
      it "stops the hub" do
        supervisor = described_class.new(notify_hub: hub)
        allow(supervisor).to receive(:interruptible_sleep)
        allow(Process).to receive(:waitpid2).and_raise(Errno::ECHILD)

        supervisor.send(:shutdown)

        expect(hub).to have_received(:stop)
      end
    end

    describe "close_inherited_parent_resources (runs in EVERY forked child)" do
      # Called from setup_child_process, so dispatcher/scheduler/outbox
      # children release sibling pipe ends and the hub's socket copy too — a
      # dispatcher holding a sibling worker's wake WRITER would keep that
      # sibling's pipe from ever reaching EOF after the supervisor dies.
      it "closes sibling liveness readers, sibling wake writers, and the hub copy" do
        allow(hub).to receive(:close_inherited!)
        liveness_r, liveness_w = IO.pipe
        wake_r, wake_w = IO.pipe
        supervisor = described_class.new(
          notify_hub: hub,
          forks: { 3001 => { type: :worker, liveness_reader: liveness_r, wake_writer: wake_w } }
        )

        supervisor.send(:close_inherited_parent_resources)

        expect(liveness_r).to be_closed
        expect(wake_w).to be_closed
        expect(hub).to have_received(:close_inherited!)
        expect(supervisor.notify_hub).to be_nil
        [liveness_w, wake_r].each(&:close)
      end
    end
  end
end
