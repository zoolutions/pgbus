# frozen_string_literal: true

module Pgbus
  module Web
    # Turns queue_health_stats into what the home page's Queue Health cards
    # say (issue #492): each number's level, and the thresholds behind it, so
    # the words can state them instead of colouring a percentage unexplained.
    #
    # Pure like QueueSummary: no I/O. Each Entry carries a translation key
    # under pgbus.dashboard.queue_health.words and raw args (seconds, ratios,
    # counts) the helper formats.
    module HealthVerdict
      Entry = Data.define(:level, :key, :args)
      Result = Data.define(:dead_tuples, :bloat, :vacuum, :transaction)

      DEAD_TUPLES_WARN = 10_000
      BLOAT_WARN = 0.10
      BLOAT_CRITICAL = 0.20
      VACUUM_WARN_SEC = 3600
      VACUUM_CRITICAL_SEC = 86_400
      TXN_WARN_SEC = 30
      TXN_CRITICAL_SEC = 60
      # Where Pgbus::MCP::HealthAnalyzer turns the verdict DEGRADED (its MVCC
      # check); the card's own thresholds are earlier warnings.
      VERDICT_TXN_SEC = 300

      module_function

      def present(health)
        Result.new(dead_tuples: dead_tuples(health), bloat: bloat(health), vacuum: vacuum(health),
                   transaction: transaction(health))
      end

      def dead_tuples(health)
        count = health[:total_dead_tuples].to_i
        entry("dead_tuples", count > DEAD_TUPLES_WARN ? :warn : :ok, count: count, limit: DEAD_TUPLES_WARN)
      end

      def bloat(health)
        worst = Array(health[:tables]).max_by { |t| t[:bloat_ratio].to_f }
        return entry("bloat", :ok, :none) unless worst

        ratio = health[:worst_bloat_ratio].to_f
        entry("bloat", level(ratio, BLOAT_WARN, BLOAT_CRITICAL), ratio: ratio, table: worst[:table],
                                                                 warn: BLOAT_WARN, critical: BLOAT_CRITICAL,
                                                                 tables: health[:tables_needing_vacuum].to_i)
      end

      def vacuum(health)
        age = health[:oldest_vacuum_ago_sec]
        return entry("vacuum", :unknown) if age.nil?

        entry("vacuum", level(age, VACUUM_WARN_SEC, VACUUM_CRITICAL_SEC), age: age, warn: VACUUM_WARN_SEC,
                                                                          critical: VACUUM_CRITICAL_SEC)
      end

      def transaction(health)
        age = health[:oldest_transaction_age_sec]
        args = { warn: TXN_WARN_SEC, critical: TXN_CRITICAL_SEC, verdict: VERDICT_TXN_SEC }
        return entry("transaction", :ok, :none, **args) if age.nil?

        entry("transaction", level(age, TXN_WARN_SEC, TXN_CRITICAL_SEC), age: age, **args)
      end

      def level(value, warn, critical)
        return :critical if value > critical
        return :warn if value > warn

        :ok
      end

      # The key is the metric and the level, or a named case (none) that has
      # its own sentence.
      def entry(metric, level, variant = level, **args)
        Entry.new(level: level, key: "#{metric}.#{variant}", args: args)
      end
    end
  end
end
