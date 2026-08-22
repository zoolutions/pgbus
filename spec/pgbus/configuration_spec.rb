# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Configuration do
  subject(:config) { described_class.new }

  describe "defaults" do
    it "has default queue prefix" do
      expect(config.queue_prefix).to eq("pgbus")
    end

    it "has default queue name" do
      expect(config.default_queue).to eq("default")
    end

    it "leaves pool_size unset by default (auto-tuned at read time)" do
      expect(config.pool_size).to be_nil
    end

    it "leaves health_port unset by default (standalone health server disabled)" do
      expect(config.health_port).to be_nil
    end

    it "binds the health server to localhost by default" do
      expect(config.health_bind).to eq("127.0.0.1")
    end

    it "has default visibility timeout" do
      expect(config.visibility_timeout).to eq(30)
    end

    it "has default max retries" do
      expect(config.max_retries).to eq(5)
    end

    it "leaves metrics_backend off by default" do
      expect(config.metrics_backend).to be_nil
    end

    it "has default statsd host and port" do
      expect(config.statsd_host).to eq("127.0.0.1")
      expect(config.statsd_port).to eq(8125)
    end

    it "has default polling interval" do
      expect(config.polling_interval).to eq(0.1)
    end

    it "enables listen/notify by default" do
      expect(config.listen_notify).to be true
    end

    it "leaves worker_notify_wakeup unset by default so it inherits listen_notify" do
      # The raw attribute stays nil; the worker_notify_wakeup? resolver
      # falls back to listen_notify. This is what makes NOTIFY wakeups
      # on by default without forcing operators who already turned
      # listen_notify off to also flip a second flag.
      expect(config.worker_notify_wakeup).to be_nil
    end

    it "leaves worker_notify_host/port/database_url unset by default" do
      expect(config.worker_notify_host).to be_nil
      expect(config.worker_notify_port).to be_nil
      expect(config.worker_notify_database_url).to be_nil
    end

    it "has no worker recycling limits by default" do
      expect(config.max_jobs_per_worker).to be_nil
      expect(config.max_memory_mb).to be_nil
      expect(config.max_worker_lifetime).to be_nil
    end

    it "has no prefetch_limit by default" do
      expect(config.prefetch_limit).to be_nil
    end

    it "has circuit breaker enabled by default" do
      expect(config.circuit_breaker_enabled).to be true
    end

    it "exposes circuit breaker tuning as constants on Pgbus::CircuitBreaker" do
      # The threshold/backoff values were silent settings nobody tuned and
      # were culled from configuration. They live on the CircuitBreaker
      # class as documented constants.
      expect(Pgbus::CircuitBreaker::THRESHOLD).to eq(5)
      expect(Pgbus::CircuitBreaker::BASE_BACKOFF).to eq(30)
      expect(Pgbus::CircuitBreaker::MAX_BACKOFF).to eq(600)
    end

    it "has no priority levels by default" do
      expect(config.priority_levels).to be_nil
      expect(config.default_priority).to eq(1)
    end

    it "has no group_mode by default" do
      expect(config.group_mode).to be_nil
    end

    it "has default archive retention of 7 days" do
      expect(config.archive_retention).to eq(7 * 24 * 3600)
    end

    it "has default batch retention of 7 days and sweep interval of 5 minutes" do
      expect(config.batch_retention).to eq(7 * 24 * 3600)
      expect(config.batch_sweep_interval).to eq(300)
    end

    it "exposes archive compaction tuning as constants on Pgbus::Process::Dispatcher" do
      # The compaction interval and batch size were silent settings; they
      # live on the dispatcher class as constants now.
      expect(Pgbus::Process::Dispatcher::ARCHIVE_COMPACTION_INTERVAL).to eq(3600)
      expect(Pgbus::Process::Dispatcher::ARCHIVE_COMPACTION_BATCH_SIZE).to eq(1000)
    end

    it "has stats enabled with 30 day retention by default" do
      expect(config.stats_enabled).to be true
      expect(config.stats_retention).to eq(30 * 24 * 3600)
    end

    it "has default stats flush thresholds matching StatBuffer defaults" do
      expect(config.stats_flush_size).to eq(100)
      expect(config.stats_flush_interval).to eq(5)
    end

    it "has streams_stats_enabled disabled by default (opt-in)" do
      expect(config.streams_stats_enabled).to be false
    end

    it "has zombie_detection enabled by default" do
      expect(config.zombie_detection).to be true
    end

    it "has insights_default_minutes of 1 hour" do
      expect(config.insights_default_minutes).to eq(60)
    end

    it "has outbox disabled by default" do
      expect(config.outbox_enabled).to be false
      expect(config.outbox_poll_interval).to eq(1.0)
      expect(config.outbox_batch_size).to eq(100)
      expect(config.outbox_retention).to eq(24 * 3600)
    end

    it "has default recurring schedule interval" do
      expect(config.recurring_schedule_interval).to eq(1.0)
    end

    it "has no recurring tasks by default" do
      expect(config.recurring_tasks).to be_nil
    end

    it "does not skip recurring by default" do
      expect(config.skip_recurring).to be false
    end

    it "has default recurring execution retention of 7 days" do
      expect(config.recurring_execution_retention).to eq(7 * 24 * 3600)
    end

    it "defaults execution_mode to :threads" do
      expect(config.execution_mode).to eq(:threads)
    end

    it "has default stall_threshold of 90 seconds" do
      expect(config.stall_threshold).to eq(90)
    end

    it "has default read_timeout of 30 seconds" do
      expect(config.read_timeout).to eq(30)
    end

    it "has default drain_timeout of 30 seconds" do
      expect(config.drain_timeout).to eq(30)
    end

    it "defaults shutdown_timeout to drain_timeout + 5" do
      expect(config.shutdown_timeout).to eq(35)
    end

    it "keeps shutdown_timeout above a raised drain_timeout" do
      config.drain_timeout = 60

      expect(config.shutdown_timeout).to eq(65)
    end

    it "respects an explicit shutdown_timeout over the derived default" do
      config.shutdown_timeout = 120

      expect(config.shutdown_timeout).to eq(120)
    end

    it "enables eager_validation by default" do
      expect(config.eager_validation).to be(true)
    end
  end

  describe "#execution_mode_for" do
    it "returns the global default when worker has no override" do
      expect(config.execution_mode_for({})).to eq(:threads)
    end

    it "returns the worker-level override when present" do
      expect(config.execution_mode_for(execution_mode: :async)).to eq(:async)
    end

    it "normalizes :fiber to :async" do
      expect(config.execution_mode_for(execution_mode: :fiber)).to eq(:async)
    end

    it "falls back to global execution_mode" do
      config.execution_mode = :async
      expect(config.execution_mode_for({})).to eq(:async)
    end
  end

  describe "#queue_name" do
    it "prefixes the queue name" do
      expect(config.queue_name("critical")).to eq("pgbus_critical")
    end

    it "normalizes hyphens to underscores" do
      expect(config.queue_name("hotwire-livereload")).to eq("pgbus_hotwire_livereload")
    end

    it "normalizes dots to underscores" do
      expect(config.queue_name("my.queue")).to eq("pgbus_my_queue")
    end
  end

  describe "#dead_letter_queue_name" do
    it "appends dlq suffix to prefixed name" do
      expect(config.dead_letter_queue_name("critical")).to eq("pgbus_critical_dlq")
    end
  end

  describe "#priority_queue_name" do
    it "returns the priority sub-queue name" do
      expect(config.priority_queue_name("critical", 0)).to eq("pgbus_critical_p0")
      expect(config.priority_queue_name("critical", 2)).to eq("pgbus_critical_p2")
    end
  end

  describe "#priority_queue_names" do
    it "returns single queue name when priority_levels is nil" do
      expect(config.priority_queue_names("default")).to eq(["pgbus_default"])
    end

    it "returns single queue name when priority_levels is 1" do
      config.priority_levels = 1
      expect(config.priority_queue_names("default")).to eq(["pgbus_default"])
    end

    it "returns sub-queue names when priority_levels > 1" do
      config.priority_levels = 3
      expect(config.priority_queue_names("default")).to eq(%w[pgbus_default_p0 pgbus_default_p1 pgbus_default_p2])
    end
  end

  describe "#resolved_pool_size" do
    context "when pool_size is explicitly set" do
      it "returns the explicit value (overrides auto-tune)" do
        config.pool_size = 17
        expect(config.resolved_pool_size).to eq(17)
      end

      it "returns the explicit value even when it's smaller than the auto-tuned value" do
        config.pool_size = 1
        config.workers = [{ queues: %w[default], threads: 50 }]
        expect(config.resolved_pool_size).to eq(1)
      end
    end

    context "when pool_size is nil (auto-tune)" do
      it "returns total worker threads + 2 (one for dispatcher, one for scheduler)" do
        config.pool_size = nil
        config.workers = [{ queues: %w[default], threads: 5 }]
        expect(config.resolved_pool_size).to eq(7)
      end

      it "sums threads across multiple worker entries (capsules)" do
        config.pool_size = nil
        config.workers = [
          { queues: %w[critical], threads: 5 },
          { queues: %w[default mailers], threads: 10 }
        ]
        expect(config.resolved_pool_size).to eq(17)
      end

      it "accepts string keys (YAML form)" do
        config.pool_size = nil
        config.workers = [{ "queues" => %w[default], "threads" => 8 }]
        expect(config.resolved_pool_size).to eq(10)
      end

      it "uses 5 as the default per-worker thread count when threads is missing" do
        config.pool_size = nil
        config.workers = [{ queues: %w[default] }]
        expect(config.resolved_pool_size).to eq(7)
      end

      it "includes event_consumers thread counts" do
        config.pool_size = nil
        config.workers = [{ queues: %w[default], threads: 5 }]
        config.event_consumers = [{ topics: %w[orders.#], threads: 3 }]
        expect(config.resolved_pool_size).to eq(10)
      end

      it "uses 3 as the default per-consumer thread count when threads is missing" do
        config.pool_size = nil
        config.workers = nil
        config.event_consumers = [{ topics: %w[orders.#] }]
        expect(config.resolved_pool_size).to eq(5)
      end

      it "treats nil workers as zero" do
        config.pool_size = nil
        config.workers = nil
        config.event_consumers = nil
        expect(config.resolved_pool_size).to eq(2)
      end

      it "treats empty workers as zero" do
        config.pool_size = nil
        config.workers = []
        config.event_consumers = []
        expect(config.resolved_pool_size).to eq(2)
      end

      it "warns when the auto-tuned pool exceeds the sanity threshold" do
        config.pool_size = nil
        config.workers = [{ queues: %w[default], threads: 60 }]
        warned_message = nil
        allow(Pgbus.logger).to receive(:warn) { |&block| warned_message = block.call }
        config.resolved_pool_size
        expect(warned_message).to match(/pool_size .* 62/)
      end

      it "uses fewer connections for async workers (fibers share connections)" do
        config.pool_size = nil
        config.workers = [{ queues: %w[webhooks], threads: 100, execution_mode: :async }]
        # Async workers need ~3 connections (reactor + polling + headroom),
        # not 100 (one per fiber). Total: 3 + 1 dispatcher + 1 scheduler = 5
        expect(config.resolved_pool_size).to eq(5)
      end

      it "mixes async and thread workers correctly" do
        config.pool_size = nil
        config.workers = [
          { queues: %w[webhooks], threads: 50, execution_mode: :async },
          { queues: %w[default], threads: 5 }
        ]
        # 3 (async) + 5 (threads) + 1 dispatcher + 1 scheduler = 10
        expect(config.resolved_pool_size).to eq(10)
      end

      it "uses fewer connections for fiber mode (alias for async)" do
        config.pool_size = nil
        config.workers = [{ queues: %w[llm], threads: 200, execution_mode: :fiber }]
        # 3 + 1 + 1 = 5
        expect(config.resolved_pool_size).to eq(5)
      end

      it "honors global execution_mode when workers have no per-entry override" do
        config.pool_size = nil
        config.execution_mode = :async
        config.workers = [{ queues: %w[default], threads: 50 }]
        # Global async: 3 + 1 dispatcher + 1 scheduler = 5
        expect(config.resolved_pool_size).to eq(5)
      end

      it "does not warn for normal sizes" do
        config.pool_size = nil
        config.workers = [{ queues: %w[default], threads: 5 }]
        allow(Pgbus.logger).to receive(:warn)
        config.resolved_pool_size
        expect(Pgbus.logger).not_to have_received(:warn)
      end

      it "rejects non-integer thread counts (e.g. string)" do
        config.pool_size = nil
        config.workers = [{ queues: %w[default], threads: "5" }]
        expect { config.resolved_pool_size }.to raise_error(
          Pgbus::ConfigurationError,
          /worker.*threads.*positive integer/
        )
      end

      it "rejects float thread counts" do
        config.pool_size = nil
        config.workers = [{ queues: %w[default], threads: 0.5 }]
        expect { config.resolved_pool_size }.to raise_error(
          Pgbus::ConfigurationError,
          /worker.*threads.*positive integer/
        )
      end

      it "rejects zero thread counts" do
        config.pool_size = nil
        config.workers = [{ queues: %w[default], threads: 0 }]
        expect { config.resolved_pool_size }.to raise_error(
          Pgbus::ConfigurationError,
          /worker.*threads.*positive integer/
        )
      end

      it "rejects negative thread counts" do
        config.pool_size = nil
        config.workers = [{ queues: %w[default], threads: -1 }]
        expect { config.resolved_pool_size }.to raise_error(
          Pgbus::ConfigurationError,
          /worker.*threads.*positive integer/
        )
      end

      it "rejects bad event_consumer thread counts with the right group label" do
        config.pool_size = nil
        config.workers = nil
        config.event_consumers = [{ topics: %w[orders.#], threads: "abc" }]
        expect { config.resolved_pool_size }.to raise_error(
          Pgbus::ConfigurationError,
          /event_consumer.*threads.*positive integer/
        )
      end

      it "includes the offending value in the error message" do
        config.pool_size = nil
        config.workers = [{ queues: %w[default], threads: "abc" }]
        expect { config.resolved_pool_size }.to raise_error(Pgbus::ConfigurationError, /"abc"/)
      end
    end
  end

  describe "#workers=" do
    context "when given an Array (explicit form)" do
      it "stores the array unchanged" do
        config.workers = [{ queues: %w[default], threads: 5 }]
        expect(config.workers).to eq([{ queues: %w[default], threads: 5 }])
      end

      it "preserves additional keys like single_active_consumer" do
        config.workers = [{ queues: %w[critical], threads: 3, single_active_consumer: true }]
        expect(config.workers.first[:single_active_consumer]).to be(true)
      end
    end

    context "when given an Array with string keys (YAML form)" do
      it "symbolizes every top-level option key" do
        config.workers = [{
          "queues" => %w[critical],
          "threads" => 2,
          "single_active_consumer" => true,
          "consumer_priority" => 1,
          "group_mode" => "fifo",
          "execution_mode" => "async",
          "name" => "critical"
        }]

        entry = config.workers.first
        expect(entry).to eq(
          queues: %w[critical],
          threads: 2,
          single_active_consumer: true,
          consumer_priority: 1,
          group_mode: "fifo",
          execution_mode: "async",
          name: "critical"
        )
      end

      it "makes string-keyed options readable via symbols" do
        config.workers = [{ "queues" => %w[critical], "threads" => 2, "single_active_consumer" => true }]
        entry = config.workers.first
        expect(entry[:queues]).to eq(%w[critical])
        expect(entry[:threads]).to eq(2)
        expect(entry[:single_active_consumer]).to be(true)
      end

      it "leaves symbol-keyed entries unchanged" do
        config.workers = [{ queues: %w[default], threads: 5 }]
        expect(config.workers).to eq([{ queues: %w[default], threads: 5 }])
      end

      it "normalizes a mix of string- and symbol-keyed entries" do
        config.workers = [
          { "queues" => %w[critical], "threads" => 2 },
          { queues: %w[default], threads: 5 }
        ]
        expect(config.workers).to eq([
                                       { queues: %w[critical], threads: 2 },
                                       { queues: %w[default], threads: 5 }
                                     ])
      end

      it "raises Pgbus::ConfigurationError when an entry is not a Hash" do
        expect { config.workers = [%w[not a hash]] }.to raise_error(
          Pgbus::ConfigurationError, /worker entry must be a Hash/
        )
      end
    end

    context "when given a String (new DSL form)" do
      it "parses the wildcard form to an anonymous capsule (no :name)" do
        # Wildcards never get auto-named — see the long comment on
        # Configuration#workers= for the rationale. Anonymous capsules
        # let the user run multiple identical forks without colliding
        # with the named-capsule overlap check.
        config.workers = "*: 5"
        expect(config.workers).to eq([{ queues: ["*"], threads: 5 }])
        expect(config.workers.first).not_to have_key(:name)
      end

      it "auto-names each capsule whose first-queue is unique and not the wildcard" do
        config.workers = "critical, default: 5; mailers: 2"
        names = config.workers.map { |c| c[:name] }
        expect(names).to eq(%w[critical mailers])
      end

      it "leaves duplicate-first-queue capsules anonymous (the 'N forks' pattern)" do
        # Restores the legacy YAML pattern: 5 × {queues: ["*"], threads: 3}.
        # Each capsule becomes its own forked process at boot time.
        config.workers = "*: 3; *: 3; *: 3"
        expect(config.workers.size).to eq(3)
        expect(config.workers).to all(eq(queues: ["*"], threads: 3))
        config.workers.each { |c| expect(c).not_to have_key(:name) }
      end

      it "leaves duplicate non-wildcard first-queues anonymous too" do
        # Same logic — if naming would collide, neither side gets a name.
        config.workers = "default: 5; default: 3"
        expect(config.workers).to eq([
                                       { queues: ["default"], threads: 5 },
                                       { queues: ["default"], threads: 3 }
                                     ])
        config.workers.each { |c| expect(c).not_to have_key(:name) }
      end

      it "raises CapsuleDSL::ParseError for invalid strings" do
        expect { config.workers = "default: 0" }.to raise_error(
          Pgbus::Configuration::CapsuleDSL::ParseError,
          /positive integer/
        )
      end
    end

    context "when given nil" do
      it "allows nil (no workers configured — used by scheduler-only / dispatcher-only deployments)" do
        expect { config.workers = nil }.not_to raise_error
        expect(config.workers).to be_nil
      end
    end

    context "when given anything else" do
      it "raises Pgbus::ConfigurationError for an Integer" do
        expect { config.workers = 5 }.to raise_error(Pgbus::ConfigurationError, /String.*Array|Array.*String/)
      end

      it "raises Pgbus::ConfigurationError for a Hash" do
        expect { config.workers = { queues: %w[default] } }.to raise_error(Pgbus::ConfigurationError, /String.*Array|Array.*String/)
      end
    end
  end

  describe "#event_consumers=" do
    it "symbolizes string-keyed entries" do
      config.event_consumers = [{ "topics" => %w[orders.*], "threads" => 4 }]
      entry = config.event_consumers.first
      expect(entry).to eq(topics: %w[orders.*], threads: 4)
      expect(entry[:topics]).to eq(%w[orders.*])
      expect(entry[:threads]).to eq(4)
    end

    it "leaves symbol-keyed entries unchanged" do
      config.event_consumers = [{ topics: %w[orders.#], threads: 3 }]
      expect(config.event_consumers).to eq([{ topics: %w[orders.#], threads: 3 }])
    end

    it "passes nil through" do
      config.event_consumers = nil
      expect(config.event_consumers).to be_nil
    end

    it "raises Pgbus::ConfigurationError when an entry is not a Hash" do
      expect { config.event_consumers = ["not a hash"] }.to raise_error(
        Pgbus::ConfigurationError, /event_consumer entry must be a Hash/
      )
    end
  end

  describe "#capsule" do
    it "appends a named capsule to workers (with name normalized to string)" do
      config.workers = nil
      config.capsule(:critical, queues: %w[critical], threads: 5)
      expect(config.workers).to eq([
                                     { name: "critical", queues: %w[critical], threads: 5 }
                                   ])
    end

    it "preserves additional keys (single_active_consumer, consumer_priority)" do
      config.workers = nil
      config.capsule(
        :gated,
        queues: %w[gated],
        threads: 1,
        single_active_consumer: true,
        consumer_priority: 10
      )
      capsule = config.workers.first
      expect(capsule[:single_active_consumer]).to be(true)
      expect(capsule[:consumer_priority]).to eq(10)
    end

    it "appends to an existing workers list set via the string DSL" do
      config.workers = "default: 5"
      config.capsule(:critical, queues: %w[critical], threads: 3)
      names = config.workers.map { |c| c[:name] }
      expect(names).to eq(%w[default critical])
    end

    it "appends to an existing workers list set via the legacy array form" do
      config.workers = [{ queues: %w[default], threads: 5 }]
      config.capsule(:reports, queues: %w[reports], threads: 2)
      expect(config.workers.size).to eq(2)
      expect(config.workers.last[:name]).to eq("reports")
    end

    it "raises if the same name is registered twice" do
      config.workers = nil
      config.capsule(:critical, queues: %w[critical], threads: 5)
      expect do
        config.capsule(:critical, queues: %w[urgent], threads: 3)
      end.to raise_error(Pgbus::ConfigurationError, /:critical.*already defined/)
    end

    it "rejects nil queues" do
      expect do
        config.capsule(:bad, queues: nil, threads: 5)
      end.to raise_error(Pgbus::ConfigurationError, /queues/)
    end

    it "rejects empty queues" do
      expect do
        config.capsule(:bad, queues: [], threads: 5)
      end.to raise_error(Pgbus::ConfigurationError, /queues/)
    end

    it "rejects non-positive threads" do
      expect do
        config.capsule(:bad, queues: %w[a], threads: 0)
      end.to raise_error(Pgbus::ConfigurationError, /threads/)
    end

    it "raises if a queue overlaps with an already-defined capsule" do
      config.workers = nil
      config.capsule(:a, queues: %w[shared], threads: 5)
      expect do
        config.capsule(:b, queues: %w[shared], threads: 5)
      end.to raise_error(Pgbus::ConfigurationError, /shared.*already.*capsule/i)
    end
  end

  describe "#capsule_named" do
    before do
      config.workers = nil
      config.capsule(:critical, queues: %w[critical], threads: 5)
      config.capsule(:default, queues: %w[default], threads: 10)
    end

    it "returns the matching capsule by symbol name" do
      expect(config.capsule_named(:critical)).to include(name: "critical", threads: 5)
    end

    it "returns the matching capsule by string name" do
      expect(config.capsule_named("critical")).to include(name: "critical", threads: 5)
    end

    it "returns nil when no capsule matches" do
      expect(config.capsule_named(:missing)).to be_nil
    end

    it "returns nil when workers is nil" do
      config.workers = nil
      expect(config.capsule_named(:any)).to be_nil
    end
  end

  describe "capsule name normalization" do
    it "stores symbol-named capsules as strings internally" do
      config.workers = nil
      config.capsule(:critical, queues: %w[critical], threads: 5)
      expect(config.workers.first[:name]).to eq("critical")
    end

    it "stores string-named capsules as strings internally" do
      config.workers = nil
      config.capsule("critical", queues: %w[critical], threads: 5)
      expect(config.workers.first[:name]).to eq("critical")
    end

    it "rejects symbol/string name collision (treats them as the same name)" do
      config.workers = nil
      config.capsule(:critical, queues: %w[critical], threads: 5)
      expect do
        config.capsule("critical", queues: %w[urgent], threads: 3)
      end.to raise_error(Pgbus::ConfigurationError, /already defined/)
    end

    it "string DSL stores auto-generated names as strings" do
      config.workers = "critical: 5"
      expect(config.workers.first[:name]).to eq("critical")
    end
  end

  describe "wildcard overlap detection" do
    # Anonymous capsules (parsed from "*: N" or duplicate-first-queue
    # strings) are intentionally invisible to overlap detection — they
    # represent "N forks of the same pool" rather than addressable units.
    # Only named capsules trigger the overlap rule.
    it "allows adding a named capsule when an existing anonymous '*' capsule exists" do
      config.workers = "*: 5" # anonymous (wildcard never gets auto-named)
      expect do
        config.capsule(:critical, queues: %w[critical], threads: 3)
      end.not_to raise_error
    end

    it "allows adding a wildcard capsule on top of anonymous wildcards" do
      config.workers = "*: 3; *: 3"
      expect do
        config.capsule(:catch_all, queues: ["*"], threads: 5)
      end.not_to raise_error
    end

    it "rejects adding a wildcard capsule when an existing NAMED capsule exists" do
      config.workers = nil
      config.capsule(:critical, queues: %w[critical], threads: 3)
      expect do
        config.capsule(:catch_all, queues: ["*"], threads: 5)
      end.to raise_error(Pgbus::ConfigurationError, /already.*capsule|wildcard/i)
    end

    it "rejects adding a named capsule when an existing NAMED wildcard capsule exists" do
      config.workers = nil
      config.capsule(:catch_all, queues: ["*"], threads: 5)
      expect do
        config.capsule(:critical, queues: %w[critical], threads: 3)
      end.to raise_error(Pgbus::ConfigurationError, /already.*capsule|wildcard/i)
    end
  end

  describe "anonymous N-forks pattern (legacy YAML compatibility)" do
    # The pattern that triggered the v0.5.1 regression fix: legacy YAML
    # could declare 5 × {queues: ["*"], threads: 3} to mean "5 forked
    # processes each running 3 threads, all reading every queue". The
    # PR-3 capsule DSL initially rejected this as a queue overlap. The
    # fix: anonymous capsules can overlap freely, named capsules cannot.
    it "produces N capsules from N-fork wildcard syntax" do
      config.workers = "*: 3; *: 3; *: 3; *: 3; *: 3"
      expect(config.workers.size).to eq(5)
      expect(config.workers.map { |c| c[:threads] }.sum).to eq(15)
    end

    it "still rejects two named capsules with the same explicit queue" do
      config.workers = nil
      config.capsule(:foo, queues: %w[shared], threads: 5)
      expect do
        config.capsule(:bar, queues: %w[shared], threads: 5)
      end.to raise_error(Pgbus::ConfigurationError, /shared.*already.*capsule/i)
    end
  end

  describe "#role_enabled?" do
    context "when roles is nil (the default — boot everything)" do
      it "returns true for every role" do
        config.roles = nil
        %i[workers dispatcher scheduler consumers outbox].each do |role|
          expect(config.role_enabled?(role)).to be(true)
        end
      end
    end

    context "when roles is set to a subset" do
      it "returns true only for roles in the subset" do
        config.roles = %i[workers dispatcher]
        expect(config.role_enabled?(:workers)).to be(true)
        expect(config.role_enabled?(:dispatcher)).to be(true)
        expect(config.role_enabled?(:scheduler)).to be(false)
        expect(config.role_enabled?(:consumers)).to be(false)
      end

      it "accepts string role names" do
        config.roles = %i[workers]
        expect(config.role_enabled?("workers")).to be(true)
        expect(config.role_enabled?("scheduler")).to be(false)
      end
    end

    context "when roles is an empty array" do
      it "returns false for every role (effectively disables the supervisor)" do
        config.roles = []
        %i[workers dispatcher scheduler consumers outbox].each do |role|
          expect(config.role_enabled?(role)).to be(false)
        end
      end
    end
  end

  describe "#roles=" do
    it "stores nil unchanged" do
      config.roles = nil
      expect(config.roles).to be_nil
    end

    it "normalizes string roles to symbols" do
      config.roles = %w[workers dispatcher]
      expect(config.roles).to eq(%i[workers dispatcher])
    end

    it "lowercases mixed-case role names" do
      config.roles = %w[WORKERS Dispatcher]
      expect(config.roles).to eq(%i[workers dispatcher])
    end

    it "wraps a single non-array value into an array" do
      config.roles = :workers
      expect(config.roles).to eq([:workers])
    end

    it "deduplicates" do
      config.roles = %i[workers workers dispatcher]
      expect(config.roles).to eq(%i[workers dispatcher])
    end

    it "raises Pgbus::ConfigurationError for an unknown role (typo protection)" do
      expect { config.roles = [:workres] }.to raise_error(Pgbus::ConfigurationError, /invalid role.*workres/i)
    end

    it "lists valid roles in the error message" do
      expect { config.roles = [:bogus] }.to raise_error(Pgbus::ConfigurationError, /workers.*dispatcher.*scheduler/i)
    end

    it "does not raise for any of the supported roles" do
      %i[workers dispatcher scheduler consumers outbox].each do |role|
        expect { config.roles = [role] }.not_to raise_error
      end
    end
  end

  describe "#resolved_pool_size with role filtering" do
    context "when roles is nil (default — boot everything)" do
      it "includes worker + event_consumer + dispatcher + scheduler thread counts" do
        config.pool_size = nil
        config.roles = nil
        config.workers = [{ queues: %w[default], threads: 5 }]
        config.event_consumers = [{ topics: %w[orders.#], threads: 3 }]
        # 5 workers + 3 consumers + 1 dispatcher + 1 scheduler = 10
        expect(config.resolved_pool_size).to eq(10)
      end
    end

    context "when running --workers-only" do
      it "excludes dispatcher and scheduler overhead from the pool size" do
        config.pool_size = nil
        config.roles = [:workers]
        config.workers = [{ queues: %w[default], threads: 5 }]
        config.event_consumers = [{ topics: %w[orders.#], threads: 3 }]
        # only workers: 5 threads, no overhead, no consumers
        expect(config.resolved_pool_size).to eq(5)
      end
    end

    context "when running --scheduler-only" do
      it "needs only the scheduler's connection slot" do
        config.pool_size = nil
        config.roles = [:scheduler]
        config.workers = [{ queues: %w[default], threads: 50 }]
        # workers are configured but not booted by this process
        expect(config.resolved_pool_size).to eq(1)
      end
    end

    context "when running --dispatcher-only" do
      it "needs only the dispatcher's connection slot" do
        config.pool_size = nil
        config.roles = [:dispatcher]
        config.workers = [{ queues: %w[default], threads: 50 }]
        expect(config.resolved_pool_size).to eq(1)
      end
    end

    context "when explicitly set" do
      it "ignores roles and returns the explicit value" do
        config.pool_size = 3
        config.roles = [:scheduler]
        config.workers = [{ queues: %w[default], threads: 5 }]
        expect(config.resolved_pool_size).to eq(3)
      end
    end
  end

  describe "duration coercion on assignment" do
    let(:duration_settings) do
      %i[
        visibility_timeout
        archive_retention
        batch_retention
        batch_sweep_interval
        idempotency_ttl
        outbox_retention
        stats_retention
        recurring_execution_retention
      ]
    end

    it "accepts a Numeric (interpreted as seconds, existing behavior)" do
      duration_settings.each do |setting|
        config.public_send("#{setting}=", 60)
        expect(config.public_send(setting)).to eq(60)
      end
    end

    it "accepts an ActiveSupport::Duration and stores as integer seconds" do
      config.visibility_timeout = 10.minutes
      expect(config.visibility_timeout).to eq(600)

      config.archive_retention = 7.days
      expect(config.archive_retention).to eq(7 * 24 * 3600)

      config.idempotency_ttl = 7.days
      expect(config.idempotency_ttl).to eq(7 * 24 * 3600)

      config.outbox_retention = 1.day
      expect(config.outbox_retention).to eq(24 * 3600)

      config.stats_retention = 30.days
      expect(config.stats_retention).to eq(30 * 24 * 3600)

      config.recurring_execution_retention = 7.days
      expect(config.recurring_execution_retention).to eq(7 * 24 * 3600)
    end

    it "raises Pgbus::ConfigurationError immediately when assigned a negative number" do
      duration_settings.each do |setting|
        expect { config.public_send("#{setting}=", -1) }.to raise_error(
          Pgbus::ConfigurationError, /#{setting}.*positive/
        )
      end
    end

    it "raises Pgbus::ConfigurationError immediately when assigned zero" do
      duration_settings.each do |setting|
        expect { config.public_send("#{setting}=", 0) }.to raise_error(
          Pgbus::ConfigurationError, /#{setting}.*positive/
        )
      end
    end

    it "raises Pgbus::ConfigurationError immediately when assigned a non-numeric value" do
      duration_settings.each do |setting|
        expect { config.public_send("#{setting}=", "five seconds") }.to raise_error(
          Pgbus::ConfigurationError, /#{setting}.*Numeric.*Duration/
        )
      end
    end

    it "accepts nil as a valid sentinel for 'feature disabled'" do
      # archive_retention, idempotency_ttl, recurring_execution_retention all
      # use nil to skip the corresponding maintenance task in the dispatcher.
      # batch_sweep_interval is required (> 0) — the dispatcher always runs it.
      nullable = duration_settings - %i[batch_sweep_interval]
      nullable.each do |setting|
        expect { config.public_send("#{setting}=", nil) }.not_to raise_error
        expect(config.public_send(setting)).to be_nil
      end
    end

    it "stores Duration values as a plain Integer (downstream code reads seconds)" do
      config.visibility_timeout = 30.seconds
      # ActiveSupport::Duration overrides BOTH is_a? AND instance_of? to return
      # true for Integer, so we have to check the actual class identity to
      # confirm the Duration was coerced rather than stored as-is.
      expect(config.visibility_timeout.class).to eq(Integer)
      expect(config.visibility_timeout).to eq(30)
    end

    it "preserves Float values for sub-second settings" do
      # Numerics that happen to be float should pass through unchanged
      config.visibility_timeout = 0.5
      expect(config.visibility_timeout).to eq(0.5)
    end
  end

  describe "#validate!" do
    it "rejects invalid prefetch_limit" do
      config.prefetch_limit = 0
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /prefetch_limit/)
    end

    it "accepts nil health_port (standalone server disabled)" do
      config.health_port = nil
      expect { config.validate! }.not_to raise_error
    end

    it "accepts a valid health_port" do
      config.health_port = 9394
      expect { config.validate! }.not_to raise_error
    end

    it "rejects a non-integer health_port" do
      config.health_port = "9394"
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /health_port/)
    end

    it "rejects an out-of-range health_port" do
      config.health_port = 70_000
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /health_port/)
    end

    it "accepts valid prefetch_limit" do
      config.prefetch_limit = 10
      expect { config.validate! }.not_to raise_error
    end

    it "rejects non-numeric stall_threshold" do
      config.stall_threshold = "90"
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /stall_threshold/)
    end

    it "rejects zero stall_threshold" do
      config.stall_threshold = 0
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /stall_threshold/)
    end

    it "accepts nil stall_threshold (disabled)" do
      config.stall_threshold = nil
      expect { config.validate! }.not_to raise_error
    end

    it "rejects false stall_threshold" do
      config.stall_threshold = false
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /stall_threshold/)
    end

    it "rejects non-numeric read_timeout" do
      config.read_timeout = "30"
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /read_timeout/)
    end

    it "rejects zero read_timeout" do
      config.read_timeout = 0
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /read_timeout/)
    end

    it "accepts nil read_timeout (disabled)" do
      config.read_timeout = nil
      expect { config.validate! }.not_to raise_error
    end

    it "rejects false read_timeout" do
      config.read_timeout = false
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /read_timeout/)
    end

    it "rejects non-numeric drain_timeout" do
      config.drain_timeout = "30"
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /drain_timeout/)
    end

    it "rejects zero drain_timeout" do
      config.drain_timeout = 0
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /drain_timeout/)
    end

    it "rejects negative drain_timeout" do
      config.drain_timeout = -5
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /drain_timeout/)
    end

    it "accepts a positive drain_timeout" do
      config.drain_timeout = 60
      expect { config.validate! }.not_to raise_error
    end

    it "rejects non-numeric shutdown_timeout" do
      config.shutdown_timeout = "45"
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /shutdown_timeout/)
    end

    it "rejects zero shutdown_timeout" do
      config.shutdown_timeout = 0
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /shutdown_timeout/)
    end

    it "accepts nil shutdown_timeout (derived default)" do
      config.shutdown_timeout = nil
      expect { config.validate! }.not_to raise_error
    end

    it "rejects an infinite shutdown_timeout" do
      config.shutdown_timeout = Float::INFINITY
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /shutdown_timeout/)
    end

    it "rejects a NaN shutdown_timeout" do
      config.shutdown_timeout = Float::NAN
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /shutdown_timeout/)
    end

    it "rejects a non-real shutdown_timeout without crashing" do
      config.shutdown_timeout = Complex(45, 1)
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /shutdown_timeout/)
    end

    it "warns when an explicit shutdown_timeout is below drain_timeout" do
      allow(Pgbus.logger).to receive(:warn)
      config.drain_timeout = 60
      config.shutdown_timeout = 45

      config.validate!

      expect(Pgbus.logger).to have_received(:warn)
    end

    it "does not warn when shutdown_timeout covers drain_timeout" do
      allow(Pgbus.logger).to receive(:warn)
      config.shutdown_timeout = 45

      config.validate!

      expect(Pgbus.logger).not_to have_received(:warn)
    end

    it "rejects zero stats_flush_size" do
      config.stats_flush_size = 0
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /stats_flush_size/)
    end

    it "rejects negative stats_flush_size" do
      config.stats_flush_size = -1
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /stats_flush_size/)
    end

    it "rejects non-integer stats_flush_size" do
      config.stats_flush_size = 100.5
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /stats_flush_size/)
    end

    it "accepts a positive stats_flush_size" do
      config.stats_flush_size = 500
      expect { config.validate! }.not_to raise_error
    end

    it "rejects zero stats_flush_interval" do
      config.stats_flush_interval = 0
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /stats_flush_interval/)
    end

    it "rejects negative stats_flush_interval" do
      config.stats_flush_interval = -1
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /stats_flush_interval/)
    end

    it "accepts a positive stats_flush_interval" do
      config.stats_flush_interval = 2.5
      expect { config.validate! }.not_to raise_error
    end

    it "rejects invalid priority_levels" do
      config.priority_levels = 0
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /priority_levels/)
    end

    it "rejects priority_levels > 10" do
      config.priority_levels = 11
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /priority_levels/)
    end

    it "accepts valid priority_levels" do
      config.priority_levels = 3
      expect { config.validate! }.not_to raise_error
    end

    it "accepts :fifo group_mode" do
      config.group_mode = :fifo
      expect(config.group_mode).to eq(:fifo)
    end

    it "accepts :round_robin group_mode" do
      config.group_mode = :round_robin
      expect(config.group_mode).to eq(:round_robin)
    end

    it "accepts nil group_mode (disabled)" do
      config.group_mode = nil
      expect(config.group_mode).to be_nil
    end

    it "rejects invalid group_mode" do
      expect { config.group_mode = :invalid }.to raise_error(Pgbus::ConfigurationError, /group_mode/)
    end

    it "coerces string group_mode to symbol" do
      config.group_mode = "fifo"
      expect(config.group_mode).to eq(:fifo)
    end

    it "accepts :options and :session for connection_guc_mode" do
      config.connection_guc_mode = :session
      expect { config.validate! }.not_to raise_error
      config.connection_guc_mode = :options
      expect { config.validate! }.not_to raise_error
    end

    it "rejects an invalid connection_guc_mode at assignment" do
      expect { config.connection_guc_mode = :bogus }.to raise_error(Pgbus::ConfigurationError, /connection_guc_mode/)
    end

    it "rejects a non-boolean require_primary" do
      config.require_primary = "yes"
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /require_primary/)
    end

    it "accepts a boolean require_primary (default false)" do
      expect(config.require_primary).to be(false)
      config.require_primary = true
      expect { config.validate! }.not_to raise_error
    end

    # Pre-1.0 surface-freeze: close the validation gaps on core job-path keys so
    # a malformed value fails loud at boot instead of deep in a worker thread,
    # per-enqueue, or by silently corrupting queue names (issue #335).
    context "with the #335 config-gap validations" do
      # Worker recycling trio: nil = disabled, positive Numeric when set.
      %i[max_jobs_per_worker max_memory_mb max_worker_lifetime].each do |key|
        it "rejects a non-numeric #{key} (recycling limit)" do
          config.public_send("#{key}=", "10")
          expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /#{key}/)
        end

        # Body is AST-identical to the interval loop's zero-rejection but tests a
        # different key/rule; RuboCop can't tell them apart, hence the disable.
        it "rejects a zero #{key} (recycling limit)" do # rubocop:disable RSpec/RepeatedExample
          config.public_send("#{key}=", 0)
          expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /#{key}/)
        end

        it "accepts nil #{key} (recycling disabled)" do
          config.public_send("#{key}=", nil)
          expect { config.validate! }.not_to raise_error
        end

        it "accepts a positive #{key} (recycling limit)" do
          config.public_send("#{key}=", 500)
          expect { config.validate! }.not_to raise_error
        end
      end

      # Interval knobs: positive Numeric, never nil.
      %i[dispatch_interval outbox_poll_interval recurring_schedule_interval].each do |key|
        # AST-identical to the recycling loop's zero-rejection (different key/rule).
        it "rejects a zero #{key} (interval)" do # rubocop:disable RSpec/RepeatedExample
          config.public_send("#{key}=", 0)
          expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /#{key}/)
        end

        it "rejects a negative #{key} (interval)" do
          config.public_send("#{key}=", -1)
          expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /#{key}/)
        end

        it "accepts a positive #{key} (interval)" do
          config.public_send("#{key}=", 2.5)
          expect { config.validate! }.not_to raise_error
        end
      end

      it "rejects a zero outbox_batch_size" do
        config.outbox_batch_size = 0
        expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /outbox_batch_size/)
      end

      it "rejects a non-integer outbox_batch_size" do
        config.outbox_batch_size = 1.5
        expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /outbox_batch_size/)
      end

      it "rejects a negative default_priority" do
        config.default_priority = -1
        expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /default_priority/)
      end

      it "accepts a zero default_priority (0 is a valid priority level)" do
        config.default_priority = 0
        expect { config.validate! }.not_to raise_error
      end

      it "rejects a non-integer default_priority" do
        config.default_priority = "high"
        expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /default_priority/)
      end

      %i[queue_prefix default_queue].each do |key|
        it "rejects an empty #{key}" do
          config.public_send("#{key}=", "")
          expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /#{key}/)
        end

        it "rejects a nil #{key}" do
          config.public_send("#{key}=", nil)
          expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /#{key}/)
        end

        it "rejects a non-String #{key}" do
          config.public_send("#{key}=", :sym)
          expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /#{key}/)
        end
      end

      it "rejects a non-callable web_auth" do
        config.web_auth = "yes"
        expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /web_auth/)
      end

      it "accepts a nil web_auth (dashboard open)" do
        config.web_auth = nil
        expect { config.validate! }.not_to raise_error
      end

      it "accepts a callable web_auth" do
        config.web_auth = ->(_req) { true }
        expect { config.validate! }.not_to raise_error
      end

      it "rejects a non-Array error_reporters" do
        config.error_reporters = ->(_e, _ctx) {}
        expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /error_reporters/)
      end

      it "accepts an Array error_reporters" do
        config.error_reporters = [->(_e, _ctx) {}]
        expect { config.validate! }.not_to raise_error
      end

      it "rejects a non-Hash connects_to" do
        config.connects_to = :pgbus
        expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /connects_to/)
      end

      it "accepts a nil connects_to (primary database)" do
        config.connects_to = nil
        expect { config.validate! }.not_to raise_error
      end

      it "accepts a Hash connects_to" do
        config.connects_to = { database: { writing: :pgbus } }
        expect { config.validate! }.not_to raise_error
      end
    end

    it "raises Pgbus::ConfigurationError (not NoMethodError) for non-string/symbol types" do
      expect { config.group_mode = 1 }.to raise_error(Pgbus::ConfigurationError, /type/)
      expect { config.group_mode = true }.to raise_error(Pgbus::ConfigurationError, /type/)
    end

    it "rejects non-positive insights_default_minutes" do
      config.insights_default_minutes = 0
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /insights_default_minutes/)
    end

    it "rejects negative insights_default_minutes" do
      config.insights_default_minutes = -1
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /insights_default_minutes/)
    end

    it "accepts nil workers (workerless modes like scheduler-only or dispatcher-only)" do
      config.workers = nil
      expect { config.validate! }.not_to raise_error
    end

    it "rejects fractional insights_default_minutes" do
      config.insights_default_minutes = 90.5
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /insights_default_minutes/)
    end

    it "rejects negative retry_backoff" do
      config.retry_backoff = -1
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /retry_backoff must be > 0/)
    end

    it "rejects nil retry_backoff_max" do
      config.retry_backoff_max = nil
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /retry_backoff_max must be > 0/)
    end

    it "rejects jitter > 1" do
      config.retry_backoff_jitter = 1.5
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /retry_backoff_jitter must be between 0 and 1/)
    end

    it "accepts valid backoff settings" do
      config.retry_backoff = 10
      config.retry_backoff_max = 600
      config.retry_backoff_jitter = 0.2
      expect { config.validate! }.not_to raise_error
    end

    it "rejects invalid global execution_mode" do
      config.execution_mode = :bogus
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /execution_mode/i)
    end

    it "accepts valid execution_mode values" do
      %i[threads async fiber].each do |mode|
        config.execution_mode = mode
        expect { config.validate! }.not_to raise_error
      end
    end

    it "rejects invalid per-worker execution_mode" do
      config.workers = [{ queues: %w[default], threads: 5, execution_mode: :bogus }]
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /execution_mode/i)
    end

    it "accepts per-worker execution_mode override" do
      config.workers = [{ queues: %w[default], threads: 50, execution_mode: :async }]
      expect { config.validate! }.not_to raise_error
    end

    it "accepts nil metrics_backend (default)" do
      config.metrics_backend = nil
      expect { config.validate! }.not_to raise_error
    end

    it "accepts :prometheus metrics_backend" do
      config.metrics_backend = :prometheus
      expect { config.validate! }.not_to raise_error
    end

    it "accepts :statsd metrics_backend" do
      config.metrics_backend = :statsd
      expect { config.validate! }.not_to raise_error
    end

    it "accepts a Pgbus::Metrics::Backend instance" do
      config.metrics_backend = Pgbus::Metrics::Backend::Null.new
      expect { config.validate! }.not_to raise_error
    end

    it "rejects an unknown metrics_backend symbol" do
      config.metrics_backend = :bogus
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /metrics_backend/)
    end

    it "rejects a non-backend metrics_backend object" do
      config.metrics_backend = "prometheus"
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /metrics_backend/)
    end

    it "accepts the default statsd_host, statsd_port, and health_bind" do
      expect { config.validate! }.not_to raise_error
    end

    it "rejects a non-String statsd_host" do
      config.statsd_host = 123
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /statsd_host/)
    end

    it "rejects an empty statsd_host" do
      config.statsd_host = ""
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /statsd_host/)
    end

    it "rejects a nil statsd_host" do
      config.statsd_host = nil
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /statsd_host/)
    end

    it "accepts a valid statsd_host" do
      config.statsd_host = "statsd.internal"
      expect { config.validate! }.not_to raise_error
    end

    it "rejects a non-Integer statsd_port" do
      config.statsd_port = "8125"
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /statsd_port/)
    end

    it "rejects a zero statsd_port" do
      config.statsd_port = 0
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /statsd_port/)
    end

    it "rejects a negative statsd_port" do
      config.statsd_port = -1
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /statsd_port/)
    end

    it "rejects an out-of-range statsd_port" do
      config.statsd_port = 70_000
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /statsd_port/)
    end

    it "rejects a nil statsd_port" do
      config.statsd_port = nil
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /statsd_port/)
    end

    it "accepts a valid statsd_port" do
      config.statsd_port = 9125
      expect { config.validate! }.not_to raise_error
    end

    it "rejects a non-String health_bind" do
      config.health_bind = 123
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /health_bind/)
    end

    it "rejects an empty health_bind" do
      config.health_bind = ""
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /health_bind/)
    end

    it "rejects a nil health_bind" do
      config.health_bind = nil
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /health_bind/)
    end

    it "accepts a valid health_bind" do
      config.health_bind = "0.0.0.0"
      expect { config.validate! }.not_to raise_error
    end
  end

  describe "#connection_options" do
    it "returns database_url when set" do
      config.database_url = "postgres://localhost/test"
      expect(config.connection_options).to eq("postgres://localhost/test")
    end

    it "returns connection_params when set" do
      params = { host: "localhost", dbname: "test" }
      config.connection_params = params
      expect(config.connection_options).to eq(params)
    end

    it "raises when no connection configured and no ActiveRecord" do
      hide_const("ActiveRecord::Base") if defined?(ActiveRecord::Base)
      expect { config.connection_options }.to raise_error(Pgbus::ConfigurationError)
    end

    context "with connects_to configured" do
      let(:db_config) do
        double("db_config", configuration_hash: {
                 host: "pgbus-host", port: 5433, database: "pgbus_db",
                 username: "pgbus_user", password: "secret"
               })
      end

      before do
        config.connects_to = { database: { writing: :pgbus } }
        stub_const("Pgbus::BusRecord", Class.new)
        allow(Pgbus::BusRecord).to receive(:connection_db_config).and_return(db_config)
      end

      it "returns a connection hash extracted from BusRecord config" do
        result = config.connection_options
        expect(result).to be_a(Hash)
        expect(result[:host]).to eq("pgbus-host")
        expect(result[:dbname]).to eq("pgbus_db")
        expect(result[:user]).to eq("pgbus_user")
      end
    end

    context "without connects_to" do
      let(:db_config) do
        double("db_config", configuration_hash: {
                 host: "localhost", port: 5432, database: "myapp_dev",
                 username: "dev_user", password: nil
               })
      end

      before do
        allow(ActiveRecord::Base).to receive(:connection_db_config).and_return(db_config)
      end

      it "returns a connection hash extracted from ActiveRecord config" do
        result = config.connection_options
        expect(result).to be_a(Hash)
        expect(result[:host]).to eq("localhost")
        expect(result[:dbname]).to eq("myapp_dev")
        expect(result[:user]).to eq("dev_user")
      end
    end

    context "with a socket-based (host-less) database.yml (issue #343)" do
      # A local-dev database.yml with no `host:`/`port:` is a Unix-socket
      # connection. ActiveRecord itself connects via libpq's default socket
      # (PGHOST / default socket dir). pgmq's raw connections must match — they
      # must NOT be silently rewritten to TCP localhost:5432, which points at a
      # different server on machines where the socket dir isn't localhost.
      let(:db_config) do
        double("db_config", configuration_hash: {
                 database: "myapp_dev", username: "dev_user"
               })
      end

      before do
        allow(ActiveRecord::Base).to receive(:connection_db_config).and_return(db_config)
      end

      it "omits :host so libpq uses its socket default, matching AR" do
        result = config.connection_options
        expect(result).to be_a(Hash)
        expect(result).not_to have_key(:host)
        expect(result).not_to have_key(:port)
        expect(result[:dbname]).to eq("myapp_dev")
        expect(result[:user]).to eq("dev_user")
      end
    end

    context "when AR config extraction fails" do
      before do
        allow(ActiveRecord::Base).to receive(:connection_db_config)
          .and_raise(StandardError, "no connection established")
      end

      it "falls back to Proc with a warning" do
        allow(Pgbus.logger).to receive(:warn)
        result = config.connection_options
        expect(result).to be_a(Proc)
        expect(Pgbus.logger).to have_received(:warn)
      end
    end

    context "when database.yml carries a :variables block (GUC forwarding, issue #332)" do
      let(:db_config) do
        double("db_config", configuration_hash: {
                 host: "localhost", port: 5432, database: "myapp",
                 username: "u", variables: { "client_min_messages" => "warning" }
               })
      end

      before do
        allow(ActiveRecord::Base).to receive(:connection_db_config).and_return(db_config)
      end

      it "forwards :variables into the libpq options param by default (:options mode)" do
        # Previously :variables was dropped entirely, so client_min_messages
        # never reached pgmq's raw connections (NOTICE flooding).
        result = config.connection_options
        expect(result).to be_a(Hash)
        expect(result[:options]).to include("-c client_min_messages=warning")
      end

      it "keeps the raw :variables hash available for :session mode" do
        config.connection_guc_mode = :session
        result = config.connection_options
        expect(result).to be_a(Hash)
        expect(result[:variables]).to eq("client_min_messages" => "warning")
      end
    end
  end

  describe "#worker_notify_wakeup?" do
    # Resolver lives at lib/pgbus/configuration.rb:775 and gates the
    # NotifyListener startup at lib/pgbus/process/worker.rb:430-452.
    # Flipping it to false must demonstrably keep the listener from
    # being constructed; nil must inherit listen_notify so a user who
    # already turned listen_notify off doesn't accidentally still pay
    # for a dedicated PG connection per worker fork.

    it "inherits from listen_notify when unset (default-on)" do
      expect(config.worker_notify_wakeup).to be_nil
      expect(config.listen_notify).to be true
      expect(config.worker_notify_wakeup?).to be true
    end

    it "inherits 'off' from listen_notify when listen_notify is false" do
      config.listen_notify = false
      expect(config.worker_notify_wakeup?).to be false
    end

    it "honors an explicit true even if listen_notify is false" do
      config.listen_notify = false
      config.worker_notify_wakeup = true
      expect(config.worker_notify_wakeup?).to be true
    end

    it "honors an explicit false even if listen_notify is true" do
      config.listen_notify = true
      config.worker_notify_wakeup = false
      expect(config.worker_notify_wakeup?).to be false
    end
  end

  describe "#worker_notify_scope" do
    # Where the LISTEN connection lives (issue #381): :supervisor (default)
    # runs ONE shared NotifyListener in the supervisor and wakes forks over
    # pipes; :fork restores the previous one-listener-per-fork behavior.

    it "defaults to :supervisor" do
      expect(config.worker_notify_scope).to eq(:supervisor)
    end

    it "accepts :fork" do
      config.worker_notify_scope = :fork
      expect(config.worker_notify_scope).to eq(:fork)
    end

    it "coerces a String" do
      config.worker_notify_scope = "fork"
      expect(config.worker_notify_scope).to eq(:fork)
    end

    it "rejects an unknown scope with an actionable error" do
      expect { config.worker_notify_scope = :hosted }
        .to raise_error(Pgbus::ConfigurationError, /worker_notify_scope.*:supervisor.*:fork/m)
    end

    it "rejects a non-symbolizable value" do
      expect { config.worker_notify_scope = 42 }
        .to raise_error(Pgbus::ConfigurationError, /worker_notify_scope/)
    end
  end

  describe "#streams_listen_scope" do
    # Where the streams LISTEN connection lives (issue #382): :master (default)
    # runs ONE shared listener in the preforking master (workers connect over
    # a Unix socket); :process keeps one listener per web process.

    it "defaults to :master" do
      expect(config.streams_listen_scope).to eq(:master)
    end

    it "accepts :process" do
      config.streams_listen_scope = :process
      expect(config.streams_listen_scope).to eq(:process)
    end

    it "coerces a String" do
      config.streams_listen_scope = "process"
      expect(config.streams_listen_scope).to eq(:process)
    end

    it "rejects an unknown scope with an actionable error" do
      expect { config.streams_listen_scope = :hosted }
        .to raise_error(Pgbus::ConfigurationError, /streams_listen_scope.*:master.*:process/m)
    end

    it "rejects a non-symbolizable value" do
      expect { config.streams_listen_scope = 42 }
        .to raise_error(Pgbus::ConfigurationError, /streams_listen_scope/)
    end
  end

  describe "#worker_notify_connection_options" do
    # Mirrors streams_connection_options: defaults to connection_options,
    # overridable so the listener's persistent LISTEN connection can be
    # pinned past a transaction-pool PgBouncer (where LISTEN silently
    # unbinds on COMMIT). Precedence is database_url > host/port > base,
    # and both Hash and String base shapes have to compose cleanly.

    context "when no overrides are set" do
      it "returns the base connection_options Hash unchanged" do
        params = { host: "localhost", dbname: "test" }
        config.connection_params = params
        expect(config.worker_notify_connection_options).to eq(params)
      end

      it "returns the base connection_options String unchanged" do
        config.database_url = "postgres://localhost/test"
        expect(config.worker_notify_connection_options).to eq("postgres://localhost/test")
      end
    end

    context "with worker_notify_database_url set" do
      it "returns the override URL even when host/port overrides are also set" do
        config.database_url = "postgres://pool.example/test"
        config.worker_notify_database_url = "postgres://direct.example/test"
        config.worker_notify_host = "ignored"
        config.worker_notify_port = 9999
        expect(config.worker_notify_connection_options)
          .to eq("postgres://direct.example/test")
      end
    end

    context "with worker_notify_host / worker_notify_port over a Hash base" do
      it "overrides only host" do
        config.connection_params = { host: "pool.example", port: 6432, dbname: "test" }
        config.worker_notify_host = "direct.example"
        result = config.worker_notify_connection_options
        expect(result).to eq(host: "direct.example", port: 6432, dbname: "test")
      end

      it "overrides only port" do
        config.connection_params = { host: "shared.example", port: 6432, dbname: "test" }
        config.worker_notify_port = 5432
        result = config.worker_notify_connection_options
        expect(result).to eq(host: "shared.example", port: 5432, dbname: "test")
      end

      it "overrides both host and port without mutating the base Hash" do
        base = { host: "pool.example", port: 6432, dbname: "test" }
        config.connection_params = base
        config.worker_notify_host = "direct.example"
        config.worker_notify_port = 5432
        result = config.worker_notify_connection_options
        expect(result).to eq(host: "direct.example", port: 5432, dbname: "test")
        # Mutating the override return value must NOT leak into the base hash.
        expect(base).to eq(host: "pool.example", port: 6432, dbname: "test")
      end
    end

    context "with worker_notify_host / worker_notify_port over a String base" do
      it "appends host=… as a libpq key=value pair" do
        config.database_url = "postgres://pool.example/test"
        config.worker_notify_host = "direct.example"
        expect(config.worker_notify_connection_options)
          .to eq("postgres://pool.example/test host=direct.example")
      end

      it "appends both host=… and port=…" do
        config.database_url = "postgres://pool.example/test"
        config.worker_notify_host = "direct.example"
        config.worker_notify_port = 5432
        expect(config.worker_notify_connection_options)
          .to eq("postgres://pool.example/test host=direct.example port=5432")
      end
    end
  end

  describe "#recurring_tasks_files" do
    it "returns array wrapping recurring_tasks_file when set" do
      config.recurring_tasks_file = "/app/config/recurring.yml"
      expect(config.recurring_tasks_files).to eq(["/app/config/recurring.yml"])
    end

    it "returns the explicitly set files when recurring_tasks_files is set" do
      config.recurring_tasks_files = ["/app/config/recurring.yml", "/app/config/recurring/bills.yml"]
      expect(config.recurring_tasks_files).to eq(["/app/config/recurring.yml", "/app/config/recurring/bills.yml"])
    end

    it "prefers recurring_tasks_files over recurring_tasks_file" do
      config.recurring_tasks_file = "/app/config/old.yml"
      config.recurring_tasks_files = ["/app/config/new.yml"]
      expect(config.recurring_tasks_files).to eq(["/app/config/new.yml"])
    end

    it "returns nil when neither is set" do
      expect(config.recurring_tasks_files).to be_nil
    end
  end

  describe "streams settings" do
    it "is enabled by default" do
      expect(config.streams_enabled).to be true
    end

    it "removed streams_queue_prefix in 1.0 (inert since #308, issue #335)" do
      # The accessor was an inert no-op since #308 and is fully removed in 1.0.
      # Stream queues are named like job queues via #queue_name.
      expect(config).not_to respond_to(:streams_queue_prefix)
      expect { config.streams_queue_prefix = "x" }.to raise_error(NoMethodError)
    end

    it "has no signed name secret by default (falls back to Turbo's key)" do
      expect(config.streams_signed_name_secret).to be_nil
    end

    it "has a 5 minute default retention" do
      expect(config.streams_default_retention).to eq(5 * 60)
    end

    it "has an empty per-stream retention map by default" do
      expect(config.streams_retention).to eq({})
    end

    it "has a 15 second heartbeat interval" do
      expect(config.streams_heartbeat_interval).to eq(15)
    end

    it "caps connections per worker at 2000" do
      expect(config.streams_max_connections).to eq(2_000)
    end

    it "has a 1 hour idle timeout" do
      expect(config.streams_idle_timeout).to eq(3_600)
    end

    it "has a 250ms LISTEN health check interval" do
      expect(config.streams_listen_health_check_ms).to eq(250)
    end

    it "has a 5 second write deadline" do
      expect(config.streams_write_deadline_ms).to eq(5_000)
    end

    it "has a 250ms fanout write deadline (short, to bound head-of-line blocking)" do
      expect(config.streams_fanout_write_deadline_ms).to eq(250)
    end

    it "leaves the dispatch queue unbounded by default (0 = unbounded)" do
      expect(config.streams_dispatch_queue_limit).to eq(0)
    end

    it "runs fanout writes inline by default (0 writer threads)" do
      expect(config.streams_writer_threads).to eq(0)
    end

    it "leaves the per-connection writer buffer unbounded by default (0 = unbounded)" do
      expect(config.streams_writer_buffer_limit).to eq(0)
    end

    it "sizes the dedicated streams DB pool at 5 by default" do
      expect(config.streams_pool_size).to eq(5)
    end

    it "has a 5 second streams pool checkout timeout by default" do
      expect(config.streams_pool_timeout).to eq(5)
    end

    it "disables streams-pool autoscaling by default" do
      expect(config.streams_pool_autoscale).to be false
    end

    it "leaves the autoscale hard cap unset by default (dynamic headroom is the ceiling)" do
      expect(config.streams_pool_max).to be_nil
    end

    it "runs the autoscale maintenance check every 5 minutes by default" do
      expect(config.streams_pool_autoscale_interval).to eq(300.0)
    end

    it "tags streams connections 'pgbus_streams' by default (peer inference)" do
      expect(config.streams_application_name).to eq("pgbus_streams")
    end

    it "does not opt into the Falcon streaming body code path by default" do
      expect(config.streams_falcon_streaming_body).to be false
    end

    it "has streams_test_mode disabled by default" do
      expect(config.streams_test_mode).to be false
    end
  end

  describe "#validate! with streams settings" do
    it "rejects negative streams_default_retention" do
      config.streams_default_retention = -1
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /streams_default_retention/)
    end

    it "rejects non-positive streams_max_connections" do
      config.streams_max_connections = 0
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /streams_max_connections/)
    end

    it "rejects non-positive streams_heartbeat_interval" do
      config.streams_heartbeat_interval = 0
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /streams_heartbeat_interval/)
    end

    it "rejects non-Hash streams_retention" do
      config.streams_retention = "nope"
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /streams_retention/)
    end

    it "rejects non-positive streams_idle_timeout" do
      config.streams_idle_timeout = 0
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /streams_idle_timeout/)
    end

    it "rejects non-positive streams_listen_health_check_ms" do
      config.streams_listen_health_check_ms = 0
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /streams_listen_health_check_ms/)
    end

    it "rejects non-positive streams_write_deadline_ms" do
      config.streams_write_deadline_ms = 0
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /streams_write_deadline_ms/)
    end

    it "rejects non-positive streams_fanout_write_deadline_ms" do
      config.streams_fanout_write_deadline_ms = 0
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /streams_fanout_write_deadline_ms/)
    end

    it "rejects a non-integer streams_fanout_write_deadline_ms" do
      config.streams_fanout_write_deadline_ms = 25.5
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /streams_fanout_write_deadline_ms/)
    end

    it "accepts streams_dispatch_queue_limit of 0 (the unbounded sentinel)" do
      config.streams_dispatch_queue_limit = 0
      expect { config.validate! }.not_to raise_error
    end

    it "accepts a positive streams_dispatch_queue_limit" do
      config.streams_dispatch_queue_limit = 5_000
      expect { config.validate! }.not_to raise_error
    end

    it "rejects a negative streams_dispatch_queue_limit" do
      config.streams_dispatch_queue_limit = -1
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /streams_dispatch_queue_limit/)
    end

    it "rejects a non-integer streams_dispatch_queue_limit" do
      config.streams_dispatch_queue_limit = 2.5
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /streams_dispatch_queue_limit/)
    end

    it "accepts streams_writer_threads of 0 (the inline sentinel)" do
      config.streams_writer_threads = 0
      expect { config.validate! }.not_to raise_error
    end

    it "accepts a positive streams_writer_threads" do
      config.streams_writer_threads = 2
      expect { config.validate! }.not_to raise_error
    end

    it "rejects a negative streams_writer_threads" do
      config.streams_writer_threads = -1
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /streams_writer_threads/)
    end

    it "rejects a non-integer streams_writer_threads" do
      config.streams_writer_threads = 1.5
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /streams_writer_threads/)
    end

    it "accepts streams_writer_buffer_limit of 0 (the unbounded sentinel)" do
      config.streams_writer_buffer_limit = 0
      expect { config.validate! }.not_to raise_error
    end

    it "accepts a positive streams_writer_buffer_limit" do
      config.streams_writer_buffer_limit = 1_000
      expect { config.validate! }.not_to raise_error
    end

    it "rejects a negative streams_writer_buffer_limit" do
      config.streams_writer_buffer_limit = -1
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /streams_writer_buffer_limit/)
    end

    it "rejects a non-integer streams_writer_buffer_limit" do
      config.streams_writer_buffer_limit = 2.5
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /streams_writer_buffer_limit/)
    end

    it "rejects non-positive streams_pool_size" do
      config.streams_pool_size = 0
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /streams_pool_size/)
    end

    it "rejects a non-integer streams_pool_size" do
      config.streams_pool_size = 2.5
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /streams_pool_size/)
    end

    it "rejects non-positive streams_pool_timeout" do
      config.streams_pool_timeout = 0
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /streams_pool_timeout/)
    end

    it "rejects a non-boolean streams_pool_autoscale" do
      config.streams_pool_autoscale = "yes"
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /streams_pool_autoscale/)
    end

    it "accepts nil streams_pool_max (no hard cap)" do
      config.streams_pool_max = nil
      expect { config.validate! }.not_to raise_error
    end

    it "accepts a streams_pool_max >= streams_pool_size" do
      config.streams_pool_size = 5
      config.streams_pool_max = 12
      expect { config.validate! }.not_to raise_error
    end

    it "rejects a streams_pool_max below streams_pool_size (inverted ceiling)" do
      config.streams_pool_size = 5
      config.streams_pool_max = 3
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /streams_pool_max/)
    end

    it "rejects a non-positive streams_pool_autoscale_interval" do
      config.streams_pool_autoscale_interval = 0
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /streams_pool_autoscale_interval/)
    end

    it "accepts a valid streams config" do
      expect { config.validate! }.not_to raise_error
    end
  end

  describe "#streams_connection_options" do
    context "when no streams override is set" do
      it "returns the base connection_options Hash" do
        config.connection_params = { host: "pooler.example", port: 6432, dbname: "app", user: "app" }
        expect(config.streams_connection_options).to eq(config.connection_options)
      end

      it "returns the base connection_options String" do
        config.database_url = "postgres://app@pooler.example:6432/app"
        expect(config.streams_connection_options).to eq(config.connection_options)
      end
    end

    context "when streams_port is set with a Hash base" do
      it "overrides only the port, preserves everything else" do
        config.connection_params = {
          host: "pooler.example", port: 6432, dbname: "app",
          user: "app", password: "secret", sslmode: "require"
        }
        config.streams_port = 5432

        expect(config.streams_connection_options).to eq(
          host: "pooler.example", port: 5432, dbname: "app",
          user: "app", password: "secret", sslmode: "require"
        )
      end

      it "does not mutate the base connection_params" do
        original = { host: "pooler.example", port: 6432, dbname: "app" }
        config.connection_params = original
        config.streams_port = 5432

        config.streams_connection_options
        expect(config.connection_params).to eq(host: "pooler.example", port: 6432, dbname: "app")
      end
    end

    context "when streams_host is set with a Hash base" do
      it "overrides only the host" do
        config.connection_params = { host: "pooler.example", port: 5432, dbname: "app" }
        config.streams_host = "direct.example"

        expect(config.streams_connection_options).to eq(
          host: "direct.example", port: 5432, dbname: "app"
        )
      end
    end

    context "when both streams_host and streams_port are set with a Hash base" do
      it "overrides both" do
        config.connection_params = { host: "pooler.example", port: 6432, dbname: "app" }
        config.streams_host = "direct.example"
        config.streams_port = 5432

        expect(config.streams_connection_options).to eq(
          host: "direct.example", port: 5432, dbname: "app"
        )
      end
    end

    context "when streams_port is set with a String base" do
      it "appends a port=... key=value override" do
        config.database_url = "host=pooler.example port=6432 dbname=app user=app"
        config.streams_port = 5432

        expect(config.streams_connection_options).to eq(
          "host=pooler.example port=6432 dbname=app user=app port=5432"
        )
      end

      it "works with a postgres:// URL too" do
        config.database_url = "postgres://app@pooler.example:6432/app"
        config.streams_port = 5432

        expect(config.streams_connection_options).to eq(
          "postgres://app@pooler.example:6432/app port=5432"
        )
      end
    end

    context "when streams_database_url is set" do
      it "wins over streams_host/streams_port" do
        config.connection_params = { host: "pooler.example", port: 6432, dbname: "app" }
        config.streams_host = "ignored"
        config.streams_port = 9999
        config.streams_database_url = "postgres://stream@direct.example:5432/app"

        expect(config.streams_connection_options).to eq(
          "postgres://stream@direct.example:5432/app"
        )
      end

      it "wins over the base connection_options entirely" do
        config.connection_params = { host: "pooler.example", port: 6432, dbname: "app" }
        config.streams_database_url = "postgres://stream@direct.example:5432/app"

        expect(config.streams_connection_options).to eq(
          "postgres://stream@direct.example:5432/app"
        )
      end
    end
  end

  describe "#streams_pool_connection_options" do
    context "when no streams_pool override is set" do
      it "follows streams_connection_options — a direct-port LISTEN pin carries the pool with it (0.12.0 default)" do
        config.connection_params = { host: "pooler.example", port: 6432, dbname: "app" }
        config.streams_port = 5432

        expect(config.streams_pool_connection_options).to eq(
          host: "pooler.example", port: 5432, dbname: "app"
        )
      end

      it "follows the base options when no streams override is set either" do
        config.connection_params = { host: "pooler.example", port: 6432, dbname: "app" }

        expect(config.streams_pool_connection_options).to eq(config.connection_options)
      end

      it "follows a separate streams database (streams_database_url)" do
        config.connection_params = { host: "pooler.example", port: 6432, dbname: "app" }
        config.streams_database_url = "postgres://stream@direct.example:5432/streamsdb"

        expect(config.streams_pool_connection_options).to eq(
          "postgres://stream@direct.example:5432/streamsdb"
        )
      end
    end

    context "when streams_pool_port is set (issue #358: pooler-bypass installs)" do
      it "routes the pool independently of the LISTEN connection" do
        config.connection_params = { host: "pooler.example", port: 6432, dbname: "app" }
        config.streams_port = 5432       # LISTEN bypasses the pooler
        config.streams_pool_port = 6432  # the pool stays on the pooler

        expect(config.streams_pool_connection_options).to eq(
          host: "pooler.example", port: 6432, dbname: "app"
        )
        expect(config.streams_connection_options).to eq(
          host: "pooler.example", port: 5432, dbname: "app"
        )
      end

      it "applies the override to the BASE options, not on top of the streams overrides" do
        config.connection_params = { host: "pooler.example", port: 6432, dbname: "app" }
        config.streams_host = "direct.example"
        config.streams_port = 5432
        config.streams_pool_port = 6432

        expect(config.streams_pool_connection_options).to eq(
          host: "pooler.example", port: 6432, dbname: "app"
        )
      end

      it "appends a port=... key=value override on a String base" do
        config.database_url = "postgres://app@pooler.example:6432/app"
        config.streams_port = 5432
        config.streams_pool_port = 6432

        expect(config.streams_pool_connection_options).to eq(
          "postgres://app@pooler.example:6432/app port=6432"
        )
      end
    end

    context "when streams_pool_host is set" do
      it "overrides only the host" do
        config.connection_params = { host: "pooler.example", port: 6432, dbname: "app" }
        config.streams_pool_host = "pool.example"

        expect(config.streams_pool_connection_options).to eq(
          host: "pool.example", port: 6432, dbname: "app"
        )
      end
    end

    context "when streams_pool_database_url is set" do
      it "wins over streams_pool_host/streams_pool_port and the streams overrides" do
        config.connection_params = { host: "pooler.example", port: 6432, dbname: "app" }
        config.streams_database_url = "postgres://stream@direct.example:5432/app"
        config.streams_pool_host = "ignored"
        config.streams_pool_port = 9999
        config.streams_pool_database_url = "postgres://pool@pooler.example:6432/app"

        expect(config.streams_pool_connection_options).to eq(
          "postgres://pool@pooler.example:6432/app"
        )
      end
    end
  end

  describe "Pgbus.configure eager validation" do
    after { Pgbus.reset! }

    it "raises Pgbus::ConfigurationError when an invalid value is set in the block" do
      expect do
        Pgbus.configure { |c| c.visibility_timeout = 0 }
      end.to raise_error(Pgbus::ConfigurationError, /visibility_timeout/)
    end

    it "raises for a value that only validate! catches (not caught by the setter)" do
      expect do
        Pgbus.configure { |c| c.polling_interval = 0 }
      end.to raise_error(Pgbus::ConfigurationError, /polling_interval/)
    end

    it "passes for a valid configure block" do
      expect do
        Pgbus.configure { |c| c.queue_prefix = "custom" }
      end.not_to raise_error
      expect(Pgbus.configuration.queue_prefix).to eq("custom")
    end

    it "succeeds across two sequential valid configure blocks" do
      expect do
        Pgbus.configure { |c| c.queue_prefix = "custom" }
        Pgbus.configure { |c| c.default_queue = "critical" }
      end.not_to raise_error
      expect(Pgbus.configuration.queue_prefix).to eq("custom")
      expect(Pgbus.configuration.default_queue).to eq("critical")
    end

    it "suppresses validation when eager_validation is disabled inside the block" do
      expect do
        Pgbus.configure do |c|
          c.eager_validation = false
          c.polling_interval = 0
        end
      end.not_to raise_error
    end

    it "suppresses validation when eager_validation is disabled beforehand" do
      Pgbus.configuration.eager_validation = false
      expect do
        Pgbus.configure { |c| c.polling_interval = 0 }
      end.not_to raise_error
    end

    it "still allows explicit validate! after opting out" do
      Pgbus.configuration.eager_validation = false
      Pgbus.configure { |c| c.polling_interval = 0 }
      expect { Pgbus.configuration.validate! }.to raise_error(Pgbus::ConfigurationError, /polling_interval/)
    end
  end

  describe "recurring_enabled (renamed from skip_recurring)" do
    it "defaults to true" do
      expect(config.recurring_enabled).to be(true)
    end

    it "controls recurring with positive polarity" do
      config.recurring_enabled = false
      expect(config.recurring_enabled).to be(false)
    end

    describe "deprecated skip_recurring alias" do
      it "maps skip_recurring = true to recurring_enabled = false (inverted polarity)" do
        allow(Pgbus.logger).to receive(:warn)
        config.skip_recurring = true
        expect(config.recurring_enabled).to be(false)
      end

      it "maps skip_recurring = false to recurring_enabled = true" do
        allow(Pgbus.logger).to receive(:warn)
        config.skip_recurring = false
        expect(config.recurring_enabled).to be(true)
      end

      it "reads back the inverted recurring_enabled value" do
        config.recurring_enabled = false
        expect(config.skip_recurring).to be(true)
      end

      it "warns once about the deprecation when the writer is used" do
        allow(Pgbus.logger).to receive(:warn)
        config.skip_recurring = true
        config.skip_recurring = false
        expect(Pgbus.logger).to have_received(:warn).once
      end

      it "names the replacement in the warning" do
        warned = nil
        allow(Pgbus.logger).to receive(:warn) { |&block| warned = block.call }
        config.skip_recurring = true
        expect(warned).to match(/skip_recurring.*recurring_enabled/)
      end
    end
  end

  describe "web_filter_parameters / web_filter_sensitive (renamed from dashboard_filter_*)" do
    it "defaults web_filter_sensitive to true and web_filter_parameters to nil" do
      expect(config.web_filter_sensitive).to be(true)
      expect(config.web_filter_parameters).to be_nil
    end

    it "stores web_filter_parameters and web_filter_sensitive" do
      config.web_filter_parameters = %w[ssn token]
      config.web_filter_sensitive = false
      expect(config.web_filter_parameters).to eq(%w[ssn token])
      expect(config.web_filter_sensitive).to be(false)
    end

    describe "deprecated dashboard_filter_* aliases" do
      it "routes dashboard_filter_parameters to web_filter_parameters" do
        allow(Pgbus.logger).to receive(:warn)
        config.dashboard_filter_parameters = %w[ssn]
        expect(config.web_filter_parameters).to eq(%w[ssn])
        expect(config.dashboard_filter_parameters).to eq(%w[ssn])
      end

      it "routes dashboard_filter_sensitive to web_filter_sensitive" do
        allow(Pgbus.logger).to receive(:warn)
        config.dashboard_filter_sensitive = false
        expect(config.web_filter_sensitive).to be(false)
        expect(config.dashboard_filter_sensitive).to be(false)
      end

      it "warns once per deprecated key" do
        allow(Pgbus.logger).to receive(:warn)
        config.dashboard_filter_parameters = %w[a]
        config.dashboard_filter_parameters = %w[b]
        expect(Pgbus.logger).to have_received(:warn).once
      end

      it "names the replacement in the warning" do
        warned = nil
        allow(Pgbus.logger).to receive(:warn) { |&block| warned = block.call }
        config.dashboard_filter_sensitive = false
        expect(warned).to match(/dashboard_filter_sensitive.*web_filter_sensitive/)
      end
    end
  end

  describe "recurring_tasks_file (singular) deprecation" do
    it "warns once and prefers the plural when both singular and plural are set" do
      warned = nil
      allow(Pgbus.logger).to receive(:warn) { |&block| warned = block.call }
      config.recurring_tasks_files = ["config/plural.yml"]
      config.recurring_tasks_file = "config/singular.yml"
      expect(config.recurring_tasks_files).to eq(["config/plural.yml"])
      expect(warned).to match(/recurring_tasks_file.*recurring_tasks_files/)
    end

    it "still wraps a lone singular value into the plural array without warning" do
      allow(Pgbus.logger).to receive(:warn)
      config.recurring_tasks_file = "config/only.yml"
      expect(config.recurring_tasks_files).to eq(["config/only.yml"])
      expect(Pgbus.logger).not_to have_received(:warn)
    end
  end

  describe "#log_format=" do
    it "installs the matching pgbus formatter when the logger has none" do
      logger = Logger.new(IO::NULL)
      logger.formatter = nil
      config.logger = logger

      config.log_format = :json

      expect(logger.formatter).to be_a(Pgbus::LogFormatter::JSON)
    end

    it "replaces a pgbus-installed formatter with the new format" do
      logger = Logger.new(IO::NULL)
      logger.formatter = Pgbus::LogFormatter::Text.new
      config.logger = logger

      config.log_format = :json

      expect(logger.formatter).to be_a(Pgbus::LogFormatter::JSON)
    end

    it "does not clobber a custom (non-pgbus) formatter on the logger" do
      custom = ->(_severity, _time, _progname, msg) { "custom: #{msg}\n" }
      logger = Logger.new(IO::NULL)
      logger.formatter = custom
      config.logger = logger

      config.log_format = :json

      expect(logger.formatter).to be(custom)
    end

    it "replaces a framework-default formatter (not a deliberate choice)" do
      logger = Logger.new(IO::NULL)
      logger.formatter = Logger::Formatter.new
      config.logger = logger

      config.log_format = :json

      expect(logger.formatter).to be_a(Pgbus::LogFormatter::JSON)
    end

    it "still validates the format regardless of the formatter guard" do
      expect { config.log_format = :xml }.to raise_error(Pgbus::ConfigurationError, /log_format/)
    end

    context "when the logger is an ActiveSupport::TaggedLogging logger (issue #334)" do
      it "preserves tags with the JSON formatter" do
        require "active_support/tagged_logging"
        io = StringIO.new
        logger = ActiveSupport::TaggedLogging.new(Logger.new(io))
        config.logger = logger

        config.log_format = :json
        logger.tagged("request-42") { logger.info("hello") }

        expect(io.string).to include("request-42")
        expect(io.string).to include("hello")
      end

      it "preserves tags with the Text formatter" do
        require "active_support/tagged_logging"
        io = StringIO.new
        logger = ActiveSupport::TaggedLogging.new(Logger.new(io))
        config.logger = logger

        config.log_format = :text
        logger.tagged("job-7") { logger.info("world") }

        expect(io.string).to include("job-7")
        expect(io.string).to include("world")
      end
    end
  end
end
