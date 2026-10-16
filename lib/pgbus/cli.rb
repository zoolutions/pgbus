# frozen_string_literal: true

require "optparse"

module Pgbus
  module CLI
    module_function

    def start(args)
      command = args.first || "help"

      case command
      when "start"
        start_supervisor(args[1..] || [])
      when "status"
        show_status
      when "queues"
        list_queues
      when "dlq"
        DLQ.start(args[1..] || [])
      when "dashboard"
        Dashboard.start(args[1..] || [])
      when "mcp"
        start_mcp_server
      when "doctor"
        run_doctor
      when "version"
        puts "pgbus #{Pgbus::VERSION}"
      when "help", "--help", "-h"
        print_help
      else
        puts "Unknown command: #{command}"
        print_help
        exit 1
      end
    end

    def start_supervisor(args = [])
      apply_start_options(args)
      # The flags go through setters, after boot finalized the configuration:
      # finalize their values too before the supervisor forks.
      Pgbus.finalize!
      Pgbus.logger.info { "[Pgbus] Starting Pgbus #{Pgbus::VERSION}..." }

      supervisor = Process::Supervisor.new
      supervisor.run
    end

    # Parses CLI flags for `pgbus start` and applies them to the global
    # configuration before the supervisor boots. Designed to override the
    # initializer config without requiring a redeploy.
    def apply_start_options(args)
      options = parse_start_options(args)

      Pgbus.configuration.workers = options[:queues] if options[:queues]
      Pgbus.configuration.execution_mode = options[:execution_mode].to_sym if options[:execution_mode]
      apply_capsule_filter(options[:capsule]) if options[:capsule]
      apply_role_filter(options)
      apply_doctor_flag(options)
    end

    # --doctor runs the boot preflight in report mode; --doctor-strict aborts
    # the boot on a fatal finding. Both set config.doctor_on_boot, which the
    # supervisor reads at boot (issue #347). Strict wins if both are passed — it
    # is the stronger gate, so a user asking for both clearly wants strict.
    def apply_doctor_flag(options)
      Pgbus.configuration.doctor_on_boot = :report if options[:doctor]
      Pgbus.configuration.doctor_on_boot = :strict if options[:doctor_strict]
    end

    def parse_start_options(args)
      options = {}
      OptionParser.new do |opts|
        opts.banner = "Usage: pgbus start [options]"

        opts.on("--queues STRING", "Override worker capsules (e.g. \"critical: 5; default: 10\")") do |v|
          options[:queues] = v
        end

        opts.on("--capsule NAME", "Run only the named capsule from the configured workers") do |v|
          options[:capsule] = v
        end

        opts.on("--workers-only", "Run only the worker processes (no scheduler/dispatcher/consumers/outbox)") do
          options[:workers_only] = true
        end

        opts.on("--scheduler-only", "Run only the recurring scheduler (the cron pod pattern)") do
          options[:scheduler_only] = true
        end

        opts.on("--dispatcher-only", "Run only the dispatcher (the maintenance pod pattern)") do
          options[:dispatcher_only] = true
        end

        opts.on("--execution-mode MODE", "Execution mode: threads (default) or async") do |v|
          options[:execution_mode] = v
        end

        opts.on("--doctor", "Run doctor preflight checks at boot and log the report (issue #347)") do
          options[:doctor] = true
        end

        opts.on("--doctor-strict", "Run doctor preflight and refuse to boot on a fatal finding") do
          options[:doctor_strict] = true
        end
      end.parse!(args.dup)
      options
    end

    ROLE_FLAG_TO_ROLE = {
      workers_only: :workers,
      scheduler_only: :scheduler,
      dispatcher_only: :dispatcher
    }.freeze
    private_constant :ROLE_FLAG_TO_ROLE

    # Translates --workers-only / --scheduler-only / --dispatcher-only into
    # the corresponding +Pgbus.configuration.roles+ array. Mutually exclusive —
    # passing more than one of the three flags raises ArgumentError.
    def apply_role_filter(options)
      role_flags = options.slice(*ROLE_FLAG_TO_ROLE.keys).compact
      return if role_flags.empty?

      if role_flags.size > 1
        raise ArgumentError,
              "--workers-only, --scheduler-only, and --dispatcher-only are mutually exclusive " \
              "(got: #{role_flags.keys.map { |k| "--#{k.to_s.tr("_", "-")}" }.join(", ")})"
      end

      Pgbus.configuration.roles = [ROLE_FLAG_TO_ROLE.fetch(role_flags.keys.first)]
    end

    def apply_capsule_filter(name)
      capsule = Pgbus.configuration.capsule_named(name)
      unless capsule
        available = (Pgbus.configuration.workers || []).filter_map { |c| c[:name] }
        raise ArgumentError,
              "no capsule named #{name.inspect} (available: #{available.join(", ")})"
      end

      # Go through the public setter so any future normalization/validation
      # in workers= is applied consistently to the CLI override path too.
      Pgbus.configuration.workers = [capsule]
    end

    def show_status
      processes = ProcessEntry.order(:kind, :created_at)
                              .select(:kind, :hostname, :pid, :metadata, :last_heartbeat_at)

      if processes.none?
        puts "No Pgbus processes running."
        return
      end

      puts "KIND         HOST                 PID      HEARTBEAT                      METADATA"
      puts "-" * 100
      processes.each do |p|
        puts format("%-12s %-20s %-8s %-30s %s",
                    p.kind, p.hostname, p.pid, p.last_heartbeat_at, p.metadata)
      end
    end

    # Boots the read-only MCP diagnostic server over stdio. Loaded lazily so
    # the optional `mcp` gem is only required when this command is invoked.
    def start_mcp_server
      Pgbus::MCP.load!
      Pgbus::MCP::Runner.run
    end

    # Runs the deployment preflight diagnostics and prints the report. Exits 1
    # on any failed check so it can gate a deploy or CI run; exits 0 when all
    # checks pass (warnings do not fail). Doctor itself never raises.
    def run_doctor
      doctor = Pgbus::Doctor.new
      puts doctor.report
      exit 1 unless doctor.success?
    end

    def list_queues
      Pgbus.client.list_queues
      metrics = Pgbus.client.metrics
      claimable_ages = Pgbus.client.oldest_claimable_ages

      puts "QUEUE                                    DEPTH      VISIBLE    OLDEST (s)      CLAIMABLE (s)   TOTAL          "
      puts "-" * 111

      Array(metrics).each do |m|
        puts format("%-40s %-10s %-10s %-15s %-15s %-15s",
                    m.queue_name, m.queue_length, m.queue_visible_length,
                    m.oldest_msg_age_sec || "-", claimable_ages[m.queue_name] || "-",
                    m.total_messages)
      end
    end

    def print_help
      puts <<~HELP
        Usage: pgbus <command> [options]

        Commands:
          start    Start the Pgbus supervisor (workers + dispatcher)
          status   Show running Pgbus processes
          queues   List queues with metrics
          dlq      Inspect and drain dead-letter queues
                   (list/show/retry/retry-all/purge)
          dashboard  Print an AppSignal dashboard as import-ready JSON
                     (main/health/streams/throughput; --list to enumerate)
          mcp      Start the read-only MCP diagnostic server over stdio
          doctor   Run environment diagnostics (config, DB, PGMQ, queues,
                   LISTEN/NOTIFY, process liveness); exits 1 on any failure
          version  Show version
          help     Show this help

        Options for `start`:
          --queues STRING    Override worker capsules from the CLI
                             (e.g. "critical: 5; default: 10")
          --capsule NAME     Run only the named capsule from the configured
                             workers (useful for one-capsule-per-process
                             deployments)
          --workers-only     Run only worker processes (no scheduler/
                             dispatcher/consumers/outbox — for worker-only
                             containers)
          --scheduler-only   Run only the recurring scheduler (the cron pod
                             pattern — exactly one of these per deployment)
          --dispatcher-only  Run only the dispatcher (the maintenance pod
                             pattern)
          --execution-mode   Execution mode: threads (default) or async
                             (fiber-based, lower connection usage)
          --doctor           Run doctor preflight checks in the booting
                             supervisor and log the report (one Rails boot
                             instead of `pgbus doctor` + `pgbus start`)
          --doctor-strict    Like --doctor, but refuse to boot (exit non-zero,
                             fork nothing) if a fatal check fails (bad config
                             or an absent PGMQ schema)

        Environment for `mcp`:
          PGBUS_MCP_TOKEN          If set, PGBUS_MCP_AUTH_TOKEN must match for
                                   the server to start (optional token gate).
          PGBUS_MCP_ALLOW_PAYLOADS Truthy to let tools return raw message
                                   bodies/headers (off by default; redacted).
      HELP
    end
  end
end
