# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Doctor do
  subject(:doctor) { described_class.new(config: config, client: client, data_source: data_source) }

  let(:config) do
    Pgbus::Configuration.new.tap do |c|
      c.database_url = "postgres://user:s3cret@localhost:5432/pgbus_test"
      c.queue_prefix = "pgbus"
      c.default_queue = "default"
      c.listen_notify = true
      c.pgmq_schema_mode = :auto
    end
  end

  let(:client) do
    build_mock_client.tap do |c|
      allow(c).to receive_messages(
        ping: true,
        pgmq_installed?: true,
        pgmq_schema_version: Pgbus::PgmqSchema.latest_version,
        configured_queues: %w[default],
        list_queues: [{ queue_name: "pgbus_default" }],
        notify_enabled?: true,
        in_recovery?: false
      )
      # Mirror the real Client: resolve a logical name to its physical PGMQ
      # table name(s). Default (non-priority) maps 1:1 with the prefix.
      allow(c).to receive(:physical_queue_names) { |name| ["pgbus_#{name}"] }
    end
  end

  let(:data_source) { instance_double(Pgbus::Web::DataSource) }
  let(:ok_verdict) { { status: "OK", reasons: [], summary: { queues: 1, workers: 1 } } }

  before do
    allow(data_source).to receive_messages(queues_with_metrics: [], processes: [], queue_health_stats: {})
    health = instance_double(Pgbus::MCP::HealthAnalyzer, verdict: ok_verdict)
    allow(Pgbus::MCP::HealthAnalyzer).to receive(:new).and_return(health)
    # The dedicated-connections check opens real PG connections through
    # DedicatedConnection (deliberately NOT through the mocked client —
    # that's the whole point of the check). Stub it healthy by default;
    # the check-specific describe re-stubs it per scenario.
    allow(Pgbus::DedicatedConnection).to receive(:connect)
      .and_return(double("dedicated PG connection", exec: nil, close: nil))
  end

  describe "#run" do
    it "runs twelve checks" do
      expect(doctor.run.size).to eq(12)
    end

    it "returns hashes with :name, :status, :detail keys" do
      doctor.run.each do |check|
        expect(check).to include(:name, :status, :detail)
        expect(check[:status]).to(satisfy { |status| %i[ok warn fail].include?(status) })
      end
    end

    it "reports all checks as :ok in a healthy environment" do
      expect(doctor.run.map { |c| c[:status] }).to all(eq(:ok))
    end

    it "includes a configuration check" do
      names = doctor.run.map { |c| c[:name] }
      expect(names).to include(a_string_matching(/config/i))
    end

    it "includes checks for db, pgmq, queues, notify, and processes" do
      names = doctor.run.map { |c| c[:name].downcase }.join(" ")
      expect(names).to match(/database|connect/)
      expect(names).to match(/pgmq|schema/)
      expect(names).to include("queue")
      expect(names).to include("notify")
      expect(names).to match(/process|liveness|health/)
    end
  end

  describe "#success?" do
    it "is true when every check passes" do
      expect(doctor.success?).to be(true)
    end

    it "is true when only warnings are present (no failures)" do
      allow(client).to receive(:notify_enabled?).and_return(false)
      config.listen_notify = false

      expect(doctor.success?).to be(true)
    end

    it "is false when any check fails" do
      allow(client).to receive(:ping).and_raise(PG::Error.new("could not connect")) if defined?(PG::Error)
      stub_const("PG::Error", Class.new(StandardError)) unless defined?(PG::Error)
      allow(client).to receive(:ping).and_raise(PG::Error.new("could not connect"))

      expect(doctor.success?).to be(false)
    end
  end

  describe "the configuration check" do
    it "fails with the validate! message when the config is invalid" do
      config.pool_timeout = -1

      check = config_check(doctor.run)
      expect(check[:status]).to eq(:fail)
      expect(check[:detail]).to include("pool_timeout")
    end

    it "renders the bare validate! message (no ClassName: prefix) for a ConfigurationError" do
      # validate! raises Pgbus::ConfigurationError (#282); the check must catch
      # it in the clean-message rescue, not fall through to the StandardError
      # fallback that prefixes the error class.
      config.pool_timeout = -1

      detail = config_check(doctor.run)[:detail]
      expect(detail).not_to include("Pgbus::ConfigurationError")
      expect(detail).to start_with("pool_timeout")
    end
  end

  describe "the database connectivity check" do
    it "fails when ping raises, and success? is false" do
      stub_const("PG::Error", Class.new(StandardError)) unless defined?(PG::Error)
      allow(client).to receive(:ping).and_raise(PG::Error.new("could not connect to server"))

      check = db_check(doctor.run)
      expect(check[:status]).to eq(:fail)
      expect(check[:detail]).to include("could not connect to server")
      expect(doctor.success?).to be(false)
    end
  end

  describe "the PGMQ schema check" do
    it "warns when the installed version is older than the vendored version" do
      allow(client).to receive(:pgmq_schema_version).and_return("0.0.1")

      check = pgmq_check(doctor.run)
      expect(check[:status]).to eq(:warn)
      expect(check[:detail]).to match(/upgrade|update/i)
    end

    it "fails when the PGMQ schema is genuinely absent (no meta table)" do
      allow(client).to receive_messages(pgmq_schema_version: nil, pgmq_installed?: false)

      check = pgmq_check(doctor.run)
      expect(check[:status]).to eq(:fail)
      expect(check[:detail]).to match(/not installed/i)
    end

    it "warns (not fails) when PGMQ is installed but has no version tracking row" do
      # Extension install / an install predating version tracking: pgmq.meta
      # exists and works, but pgbus_pgmq_schema_versions was never populated.
      allow(client).to receive_messages(pgmq_schema_version: nil, pgmq_installed?: true)

      check = pgmq_check(doctor.run)
      expect(check[:status]).to eq(:warn)
      expect(check[:detail]).to match(/tracking|upgrade_pgmq/i)
    end
  end

  describe "the queue existence check" do
    it "fails and names the missing queue" do
      allow(client).to receive_messages(configured_queues: %w[default critical], list_queues: [{ queue_name: "pgbus_default" }])

      check = queue_check(doctor.run)
      expect(check[:status]).to eq(:fail)
      expect(check[:detail]).to include("pgbus_critical")
    end

    it "accepts list_queues rows that respond to queue_name" do
      row = double("QueueRow", queue_name: "pgbus_default")
      allow(client).to receive_messages(configured_queues: %w[default], list_queues: [row])

      expect(queue_check(doctor.run)[:status]).to eq(:ok)
    end

    it "resolves priority sub-queues so a priority deployment is not falsely failed" do
      # Priority mode creates _p0.._pN physical tables, never the bare name.
      allow(client).to receive(:physical_queue_names)
        .with("default").and_return(%w[pgbus_default_p0 pgbus_default_p1 pgbus_default_p2])
      allow(client).to receive_messages(
        configured_queues: %w[default],
        list_queues: [{ queue_name: "pgbus_default_p0" }, { queue_name: "pgbus_default_p1" },
                      { queue_name: "pgbus_default_p2" }]
      )

      expect(queue_check(doctor.run)[:status]).to eq(:ok)
    end
  end

  describe "the LISTEN/NOTIFY check" do
    it "is ok when notify is enabled on all configured queues" do
      expect(notify_check(doctor.run)[:status]).to eq(:ok)
    end

    it "warns (not fails) when listen_notify is configured but a trigger is missing" do
      allow(client).to receive(:notify_enabled?).and_return(false)

      check = notify_check(doctor.run)
      expect(check[:status]).to eq(:warn)
    end

    it "is ok when listen_notify is disabled in config" do
      config.listen_notify = false
      allow(client).to receive(:notify_enabled?).and_return(false)

      expect(notify_check(doctor.run)[:status]).to eq(:ok)
    end
  end

  describe "the process liveness check" do
    it "fails on a STALLED verdict" do
      stalled = { status: "STALLED", reasons: ["worker wedged"], summary: {} }
      health = instance_double(Pgbus::MCP::HealthAnalyzer, verdict: stalled)
      allow(Pgbus::MCP::HealthAnalyzer).to receive(:new).and_return(health)

      check = process_check(doctor.run)
      expect(check[:status]).to eq(:fail)
      expect(check[:detail]).to include("worker wedged")
      expect(doctor.success?).to be(false)
    end

    it "warns on a DEGRADED verdict" do
      degraded = { status: "DEGRADED", reasons: ["1 process(es) stale"], summary: {} }
      health = instance_double(Pgbus::MCP::HealthAnalyzer, verdict: degraded)
      allow(Pgbus::MCP::HealthAnalyzer).to receive(:new).and_return(health)

      expect(process_check(doctor.run)[:status]).to eq(:warn)
    end

    it "never raises when the HealthAnalyzer blows up" do
      allow(Pgbus::MCP::HealthAnalyzer).to receive(:new).and_raise(StandardError, "boom")

      check = process_check(doctor.run)
      expect(check[:status]).to eq(:fail)
      expect(check[:detail]).to include("boom")
    end
  end

  describe "the allowed_global_id_models check (security)" do
    it "is :ok outside production even when allowed_global_id_models is nil" do
      config.allowed_global_id_models = nil
      allow(doctor).to receive(:production?).and_return(false)

      expect(gid_check(doctor.run)[:status]).to eq(:ok)
    end

    it "warns in production when allowed_global_id_models is nil (allow-all)" do
      config.allowed_global_id_models = nil
      allow(doctor).to receive(:production?).and_return(true)

      check = gid_check(doctor.run)
      expect(check[:status]).to eq(:warn)
      expect(check[:detail]).to include("allowed_global_id_models")
      # Issue #368: the warning must name both surfaces the allowlist now guards.
      expect(check[:detail]).to match(/job arguments/i)
      expect(check[:detail]).to match(/EventBus/i)
    end

    it "does not fail the run — a warning never blocks a deploy" do
      config.allowed_global_id_models = nil
      allow(doctor).to receive(:production?).and_return(true)

      expect(doctor.success?).to be(true)
    end

    it "is :ok in production when an allowlist is configured" do
      # allowed_global_id_models is a Class/Module allowlist at runtime
      # (see Serializer), not strings — model the valid happy path.
      config.allowed_global_id_models = [stub_const("Order", Class.new)]
      allow(doctor).to receive(:production?).and_return(true)

      expect(gid_check(doctor.run)[:status]).to eq(:ok)
    end
  end

  describe "the streams broadcast-queue isolation check (#311)" do
    before do
      config.streams_enabled = true
      stub_const("Turbo::Broadcastable", Module.new)
    end

    it "warns in production when streams are on, Turbo is loaded, and no dedicated broadcast queue is set" do
      config.streams_broadcast_queue = nil
      allow(doctor).to receive(:production?).and_return(true)

      check = broadcast_queue_check(doctor.run)
      expect(check[:status]).to eq(:warn)
      expect(check[:detail]).to include("streams_broadcast_queue")
    end

    it "is :ok when the broadcast queue is set AND a worker capsule drains it" do
      config.streams_broadcast_queue = "realtime"
      config.workers = [{ queues: %w[realtime], threads: 3 }, { queues: %w[default], threads: 5 }]
      allow(doctor).to receive(:production?).and_return(true)

      expect(broadcast_queue_check(doctor.run)[:status]).to eq(:ok)
    end

    it "is :ok when a wildcard worker drains the broadcast queue" do
      config.streams_broadcast_queue = "realtime"
      config.workers = [{ queues: %w[*], threads: 5 }]
      allow(doctor).to receive(:production?).and_return(true)

      expect(broadcast_queue_check(doctor.run)[:status]).to eq(:ok)
    end

    it "warns when the broadcast queue is set but NO worker capsule drains it (the footgun)" do
      # Broadcasts route to `realtime` but nothing reads it — they pile up
      # unread and the browser never updates. This is worse than the nil case.
      config.streams_broadcast_queue = "realtime"
      config.workers = [{ queues: %w[default], threads: 5 }]
      allow(doctor).to receive(:production?).and_return(true)

      check = broadcast_queue_check(doctor.run)
      expect(check[:status]).to eq(:warn)
      expect(check[:detail]).to include("realtime")
      expect(check[:detail]).to match(/no worker|not drained|capsule/i)
    end

    it "warns about an undrained broadcast queue even outside production (broadcasts silently pile up)" do
      config.streams_broadcast_queue = "realtime"
      config.workers = [{ queues: %w[default], threads: 5 }]
      allow(doctor).to receive(:production?).and_return(false)

      expect(broadcast_queue_check(doctor.run)[:status]).to eq(:warn)
    end

    it "is :ok outside production even without a dedicated broadcast queue" do
      config.streams_broadcast_queue = nil
      allow(doctor).to receive(:production?).and_return(false)

      expect(broadcast_queue_check(doctor.run)[:status]).to eq(:ok)
    end

    it "is :ok when turbo-rails is not loaded (no broadcast jobs to isolate)" do
      hide_const("Turbo::Broadcastable")
      config.streams_broadcast_queue = nil
      allow(doctor).to receive(:production?).and_return(true)

      expect(broadcast_queue_check(doctor.run)[:status]).to eq(:ok)
    end

    it "does not fail the run — a warning never blocks a deploy" do
      config.streams_broadcast_queue = nil
      allow(doctor).to receive(:production?).and_return(true)

      expect(doctor.success?).to be(true)
    end
  end

  describe "the primary-affinity check (pooler safety, #332)" do
    it "is :ok when the job connection is on the primary" do
      allow(client).to receive(:in_recovery?).and_return(false)
      check = primary_check(doctor.run)
      expect(check[:status]).to eq(:ok)
    end

    it "warns (never fails) when the job connection is on a read-only replica" do
      allow(client).to receive(:in_recovery?).and_return(true)
      check = primary_check(doctor.run)
      expect(check[:status]).to eq(:warn)
      expect(check[:detail]).to match(/replica|recovery/i)
      # Names the actionable remediation (direct-port overrides).
      expect(check[:detail]).to match(/direct|worker_notify|require_primary/i)
      # A warning must never fail the run.
      expect(doctor.success?).to be(true)
    end

    it "does not crash when the recovery probe raises" do
      allow(client).to receive(:in_recovery?).and_raise(StandardError, "boom")
      expect { doctor.run }.not_to raise_error
    end
  end

  # Issue #484: the worker is GVL-bound when Postgres is close, so YJIT is worth
  # ~15-20% jobs/s there. Rails turns it on through config.yjit (load_defaults 7.2+
  # in every env, 8.0+ outside local envs). The check reports; it never enables.
  describe "the Ruby JIT check (issue #484)" do
    let(:yjit) { Module.new }

    before do
      stub_const("RubyVM::YJIT", yjit)
      hide_const("RubyVM::ZJIT")
    end

    def stub_yjit(enabled)
      allow(yjit).to receive(:enabled?).and_return(enabled)
    end

    def stub_rails_env(local:)
      env = double("Rails.env", local?: local, production?: !local, to_s: local ? "development" : "production")
      stub_const("Rails", Module.new)
      allow(Rails).to receive(:env).and_return(env)
    end

    it "is :ok and names YJIT when it is enabled" do
      stub_yjit(true)
      check = jit_check(doctor.run)
      expect(check[:status]).to eq(:ok)
      expect(check[:detail]).to include("YJIT enabled")
    end

    it "is :ok when YJIT is off in a local Rails environment" do
      stub_yjit(false)
      stub_rails_env(local: true)
      check = jit_check(doctor.run)
      expect(check[:status]).to eq(:ok)
      expect(check[:detail]).to match(/local/i)
    end

    it "warns (never fails) when YJIT is off outside a local environment" do
      stub_yjit(false)
      stub_rails_env(local: false)
      check = jit_check(doctor.run)
      expect(check[:status]).to eq(:warn)
      expect(check[:detail]).to include("config.yjit")
      expect(check[:detail]).to include("RUBY_YJIT_ENABLE")
      expect(doctor.success?).to be(true)
    end

    it "is :ok when this Ruby has no JIT at all" do
      hide_const("RubyVM::YJIT")
      check = jit_check(doctor.run)
      expect(check[:status]).to eq(:ok)
      expect(check[:detail]).to match(/no YJIT/i)
    end

    it "reports and never enables the JIT" do
      stub_yjit(false)
      allow(yjit).to receive(:enable)
      stub_rails_env(local: false)
      doctor.run
      expect(yjit).not_to have_received(:enable)
    end

    it "runs right after the configuration check and is neither boot-skipped nor strict-fatal" do
      expect(described_class::CHECKS.keys.first(2)).to eq(["Configuration", "Ruby JIT"])
      expect(described_class::BOOT_SKIP).not_to include("Ruby JIT")
      expect(described_class::STRICT_FATAL).not_to include("Ruby JIT")
    end
  end

  describe "the dedicated-connections check (issue #352)" do
    def dedicated_check(results)
      results.find { |c| c[:name] == "Dedicated connections" }
    end

    it "is :ok when the streamer and notify-listener connections open and answer" do
      expect(dedicated_check(doctor.run)[:status]).to eq(:ok)
    end

    it "opens each configured dedicated connection the way the runtime does, probes it, and closes it" do
      conn = double("dedicated PG connection", exec: nil, close: nil)
      allow(Pgbus::DedicatedConnection).to receive(:connect).and_return(conn)

      doctor.run

      # streams + worker notify — both default to the base connection options.
      expect(Pgbus::DedicatedConnection).to have_received(:connect)
        .with(config.streams_connection_options).at_least(:once)
      expect(Pgbus::DedicatedConnection).to have_received(:connect)
        .with(config.worker_notify_connection_options).at_least(:once)
      expect(conn).to have_received(:exec).with("SELECT 1").twice
      expect(conn).to have_received(:close).twice
    end

    it "fails with the connect error and the affected path label" do
      # The exact failure this check exists for: :session GUC mode leaves a
      # :variables key that libpq rejects — only the dedicated paths break,
      # every Client-pooled path works, so probing via the client misses it.
      allow(Pgbus::DedicatedConnection).to receive(:connect)
        .and_raise(StandardError.new('invalid connection option "variables"'))

      check = dedicated_check(doctor.run)

      expect(check[:status]).to eq(:fail)
      expect(check[:detail]).to include("streams", "worker notify", 'invalid connection option "variables"')
    end

    it "closes the connection even when the probe query raises" do
      conn = double("dedicated PG connection", close: nil)
      allow(conn).to receive(:exec).and_raise(StandardError.new("boom"))
      allow(Pgbus::DedicatedConnection).to receive(:connect).and_return(conn)

      check = dedicated_check(doctor.run)

      expect(check[:status]).to eq(:fail)
      expect(conn).to have_received(:close).twice
    end

    it "is :ok/disabled when streams and notify wakeup are both off" do
      config.streams_enabled = false
      config.listen_notify = false # worker_notify_wakeup? defaults to listen_notify
      allow(Pgbus::DedicatedConnection).to receive(:connect).and_raise("must not connect")

      check = dedicated_check(doctor.run)

      expect(check[:status]).to eq(:ok)
      expect(check[:detail]).to match(/disabled/i)
    end

    it "probes only the notify path when streams are disabled" do
      config.streams_enabled = false
      conn = double("dedicated PG connection", exec: nil, close: nil)
      allow(Pgbus::DedicatedConnection).to receive(:connect).and_return(conn)

      doctor.run

      expect(Pgbus::DedicatedConnection).to have_received(:connect).once
    end

    it "is not strict-fatal — a failing dedicated connection never aborts a :strict boot" do
      # Same reasoning as the Database check: a transient DB blip at boot
      # must not lockstep-abort a fleet; the failure still shows loud in the
      # boot report.
      allow(Pgbus::DedicatedConnection).to receive(:connect)
        .and_raise(StandardError.new('invalid connection option "variables"'))

      expect(dedicated_check(doctor.boot_checks)[:status]).to eq(:fail)
      expect(doctor.boot_ok?).to be(true)
    end
  end

  describe "the connection-budget check (issue #381)" do
    # Informational, config-only, always :ok — it exists so operators can do
    # direct-connection capacity math from the doctor output alone.
    def budget_check
      doctor.run.find { |c| c[:name] == "Connection budget" }
    end

    before do
      config.workers = [
        { queues: %w[critical], threads: 1 },
        { queues: %w[default], threads: 1 },
        { queues: %w[mailers], threads: 1 }
      ]
      config.event_consumers = [{ topics: ["orders.#"], threads: 1 }, { topics: ["payments.#"], threads: 1 }]
    end

    it "reports 1 pinned connection under :supervisor scope" do
      config.worker_notify_scope = :supervisor

      expect(budget_check[:status]).to eq(:ok)
      expect(budget_check[:detail]).to match(/\A1 direct LISTEN connection pinned \(scope=supervisor/)
      expect(budget_check[:detail]).to include("3 capsules + 2 consumers")
    end

    it "reports one connection per fork under :fork scope" do
      config.worker_notify_scope = :fork

      expect(budget_check[:detail]).to match(/\A5 direct LISTEN connections pinned \(scope=fork/)
    end

    it "reports 0 when worker notify wakeup is off" do
      config.listen_notify = false

      expect(budget_check[:detail]).to start_with("0 direct LISTEN connections pinned")
    end

    it "honors a narrowed role set" do
      config.worker_notify_scope = :fork
      config.roles = [:workers]

      expect(budget_check[:detail]).to match(/\A3 direct LISTEN connections pinned/)
    end

    it "singularizes capsule/consumer counts of exactly 1" do
      config.workers = [{ queues: %w[default], threads: 1 }]
      config.event_consumers = [{ topics: ["orders.#"], threads: 1 }]

      expect(budget_check[:detail]).to include("1 capsule + 1 consumer ")
      expect(budget_check[:detail]).not_to include("1 capsules")
    end

    it "notes one streams connection per web host under :master scope (the default)" do
      allow(config).to receive(:streams_enabled).and_return(true)

      expect(budget_check[:detail]).to include("+ 1 per web host (streams master hub")
    end

    it "notes the per-web-process streams listener under :process scope" do
      allow(config).to receive(:streams_enabled).and_return(true)
      config.streams_listen_scope = :process

      expect(budget_check[:detail]).to include("+ 1 per web-server process (streams)")
    end

    it "omits the streams clause when streams are disabled" do
      config.streams_enabled = false

      expect(budget_check[:detail]).not_to include("streams")
    end
  end

  describe "#report" do
    it "renders one line per check" do
      report = doctor.report
      doctor.run.each { |c| expect(report).to include(c[:name]) }
    end

    it "includes a configuration summary" do
      report = doctor.report
      expect(report).to include("queue_prefix")
      expect(report).to include("pgmq_schema_mode")
    end

    it "redacts the password in the database_url" do
      expect(doctor.report).not_to include("s3cret")
    end
  end

  describe "#config_summary" do
    it "redacts the password from a database_url string" do
      expect(doctor.config_summary[:database_url]).not_to include("s3cret")
      expect(doctor.config_summary[:database_url]).to include("localhost")
    end

    it "redacts a password that itself contains an @ (regex must not stop at the first @)" do
      config.database_url = "postgres://user:p@ss:w0rd@localhost:5432/pgbus_test"

      redacted = doctor.config_summary[:database_url]
      expect(redacted).not_to include("p@ss:w0rd")
      expect(redacted).to include("localhost")
    end

    it "redacts a password= parameter in a conninfo-style database_url" do
      config.database_url = "host=localhost dbname=pgbus password=topsecret sslmode=require"

      expect(doctor.config_summary[:database_url]).not_to include("topsecret")
    end

    it "redacts the password from connection_params hash form" do
      config.database_url = nil
      config.connection_params = { host: "localhost", dbname: "pgbus", password: "hunter2" }

      summary = doctor.config_summary
      expect(summary[:connection_params].to_s).not_to include("hunter2")
    end

    it "redacts other secret-bearing connection_params keys (sslpassword)" do
      config.database_url = nil
      config.connection_params = { host: "localhost", sslpassword: "keysecret", passfile: "/etc/pgpass" }

      summary = doctor.config_summary
      expect(summary[:connection_params].to_s).not_to include("keysecret")
    end

    it "exposes resolved runtime knobs" do
      summary = doctor.config_summary
      expect(summary).to include(:queue_prefix, :default_queue, :pgmq_schema_mode, :resolved_pool_size)
    end
  end

  describe "resilience" do
    it "never raises from #run on a fully broken environment" do
      stub_const("PG::Error", Class.new(StandardError)) unless defined?(PG::Error)
      allow(client).to receive(:ping).and_raise(PG::Error.new("dead"))
      allow(client).to receive(:pgmq_schema_version).and_raise(StandardError, "nope")
      allow(client).to receive(:list_queues).and_raise(StandardError, "nope")
      allow(client).to receive(:notify_enabled?).and_raise(StandardError, "nope")
      allow(Pgbus::MCP::HealthAnalyzer).to receive(:new).and_raise(StandardError, "nope")

      expect { doctor.run }.not_to raise_error
      expect(doctor.success?).to be(false)
    end
  end

  # Supervisor-integrated preflight (issue #347): a subset of checks safe to run
  # inside the booting supervisor BEFORE workers are forked.
  describe "#boot_checks" do
    it "excludes the process-liveness check (workers aren't forked yet)" do
      names = doctor.boot_checks.map { |c| c[:name] }
      expect(names).not_to include(a_string_matching(/process|liveness/i))
    end

    it "runs the other eleven checks" do
      expect(doctor.boot_checks.size).to eq(11)
    end

    it "never invokes the HealthAnalyzer — the excluded check does zero work" do
      # The whole point of excluding it: no pre-fork DataSource/HealthAnalyzer
      # round-trip against pgbus_processes (which has no rows for this generation
      # yet, and could carry stale prior-generation rows).
      allow(Pgbus::MCP::HealthAnalyzer).to receive(:new).and_call_original
      doctor.boot_checks
      expect(Pgbus::MCP::HealthAnalyzer).not_to have_received(:new)
    end

    it "returns the same hash shape as #run" do
      doctor.boot_checks.each do |check|
        expect(check).to include(:name, :status, :detail)
        expect(check[:status]).to(satisfy { |status| %i[ok warn fail].include?(status) })
      end
    end
  end

  describe "#boot_ok?" do
    it "is true in a healthy environment" do
      expect(doctor.boot_ok?).to be(true)
    end

    it "is false when the Configuration check fails (a real, non-transient config bug)" do
      allow(config).to receive(:validate!).and_raise(Pgbus::ConfigurationError, "bad workers")
      expect(doctor.boot_ok?).to be(false)
    end

    it "is false when the PGMQ schema is absent (deploy-fatal)" do
      allow(client).to receive_messages(pgmq_schema_version: nil, pgmq_installed?: false)
      expect(doctor.boot_ok?).to be(false)
    end

    it "stays true when only the Queues check fails — a transient the lenient bootstrap tolerates" do
      # bootstrap_queues deliberately swallows a boot-time DB blip so children
      # crash-and-backoff; a Queues :fail must NOT hard-abort a fleet cold boot.
      allow(client).to receive(:list_queues).and_return([])
      queues = doctor.boot_checks.find { |c| c[:name] == "Queues" }
      expect(queues[:status]).to eq(:fail)          # it does fail…
      expect(doctor.boot_ok?).to be(true)           # …but boot is not blocked
    end

    it "stays true on a warning (e.g. an outdated but present PGMQ schema)" do
      allow(client).to receive(:pgmq_schema_version).and_return("0.0.1")
      expect(doctor.boot_ok?).to be(true)
    end
  end

  describe "#boot_report" do
    it "renders the preflight subset and omits the process-liveness line" do
      report = doctor.boot_report
      expect(report).to include("Configuration")
      expect(report).to include("PGMQ schema")
      expect(report).not_to match(/Process liveness/i)
    end

    it "still redacts the password in the config summary" do
      expect(doctor.boot_report).not_to include("s3cret")
    end
  end

  # --- helpers to pluck a check out of the results by keyword ---

  def config_check(checks)  = checks.find { |c| c[:name].match?(/config/i) }
  def db_check(checks)      = checks.find { |c| c[:name].match?(/database|connect/i) }
  def pgmq_check(checks)    = checks.find { |c| c[:name].match?(/pgmq|schema/i) }
  def queue_check(checks)   = checks.find { |c| c[:name].match?(/queue/i) && !c[:name].match?(/notify/i) }
  def notify_check(checks)  = checks.find { |c| c[:name].match?(/notify/i) }
  def process_check(checks) = checks.find { |c| c[:name].match?(/process|liveness|health/i) }
  def gid_check(checks)     = checks.find { |c| c[:name].match?(/globalid|allowlist/i) }
  def broadcast_queue_check(checks) = checks.find { |c| c[:name].match?(/broadcast queue/i) }
  def primary_check(checks)         = checks.find { |c| c[:name].match?(/primary/i) }
  def jit_check(checks)             = checks.find { |c| c[:name] == "Ruby JIT" }
end
