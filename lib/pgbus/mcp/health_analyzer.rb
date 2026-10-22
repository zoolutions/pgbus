# frozen_string_literal: true

module Pgbus
  module MCP
    # Computes the top-level pgbus health verdict (OK / DEGRADED / STALLED)
    # from the existing DataSource read layer. This is the single signal that
    # catches the silent-worker-wedge class of incident (#179, #174, #181):
    # a queue with visible messages and no claim progress while a subscribing
    # worker is heart-beating with idle capacity.
    #
    # Verdict semantics (issue #180 acceptance criteria):
    #   STALLED  — backlog (visible > 0) AND at least one worker is heart-beating
    #              but its claim loop has stopped advancing (status :stalled),
    #              OR backlog with live-but-idle workers and zero claim progress.
    #   DEGRADED — something is wrong but not the wedge: stale processes, a
    #              paused queue holding a backlog, growing DLQ, or MVCC horizon
    #              pinned by a long-running transaction.
    #   OK       — draining normally / nothing actionable.
    class HealthAnalyzer
      # A worker is considered to have idle capacity unless its metadata
      # explicitly reports it is saturated. We treat the presence of any live
      # worker as "has capacity" because a wedged worker reports healthy
      # heartbeats while doing no work — exactly the case we must catch.
      WORKER_KIND = "worker"

      # Process kinds that claim from queues: workers drain job queues, consumers
      # drain EventBus handler queues. `drained_queue_names` unions both kinds'
      # queues, so the wedge signal must count both kinds as liveness evidence —
      # otherwise a consumer-only fleet with a wedged handler queue is missed
      # (the worker set is empty and the branch never runs).
      DRAINING_KINDS = %w[worker consumer].freeze

      # Cap on how many queue names a reason string lists before truncating to
      # "(+N more)" — keeps a flagged fleet of hundreds of queues from producing
      # a multi-KB log line (issue #367 minor).
      NAME_LIMIT = 10

      # +grace+ overrides Pgbus.configuration.health_unclaimed_grace (issue #589):
      # seconds a visible read_ct=0 message must have been claimable before it
      # counts as "never claimed". nil reads the config when the verdict runs.
      def initialize(data_source, grace: nil)
        @data_source = data_source
        @grace = grace
      end

      # Returns a machine-readable verdict hash suitable for both interactive
      # agent use and automated alerting.
      def verdict
        queues          = @data_source.queues_with_metrics
        processes       = @data_source.processes
        stream_names    = @data_source.stream_queue_names
        health, health_error = safe_queue_health

        # Partition queues once: the operational set excludes DLQs and
        # registered stream queues, and the backlog is the subset with
        # visible, claimable messages. Stream queues are excluded like DLQs
        # (issue #359): stream delivery is a non-consuming peek, so their
        # messages are permanently visible with read_ct=0 — exactly the wedge
        # signature — and counting them makes the verdict scream STALLED on
        # every streams-heavy install. Paused queues are removed from the
        # STALLED backlog (an intentional pause is not the wedge — it's
        # reported under DEGRADED), but kept in `non_dlq` for the summary.
        non_dlq = queues.reject { |q| dlq?(q) || stream_names.include?(q[:name].to_s) }
        backlog = non_dlq.select { |q| q[:queue_visible_length].to_i.positive? }
        active_backlog = backlog.reject { |q| q[:paused] }

        # Split the active backlog by whether some configured capsule or handler
        # drains the queue (issue #367 defect 2). Live workers say nothing about
        # a queue they can never claim from, so only the drained side can be the
        # silent-worker-wedge; the undrained side is its own DEGRADED hazard.
        drained, undrained = partition_by_drain(active_backlog)

        stalled  = stalled_reasons(active_backlog, drained, processes)
        degraded = degraded_reasons(queues, backlog, undrained, processes, health, health_error)

        status = if stalled.any?
                   "STALLED"
                 elsif degraded.any?
                   "DEGRADED"
                 else
                   "OK"
                 end

        {
          status: status,
          reasons: stalled + degraded,
          checked_at: Time.now.utc.iso8601,
          summary: build_summary(queues, non_dlq, processes, health).merge(
            oldest_unread_claimable_age_sec: oldest_unread_age(drained)
          )
        }
      end

      private

      def dlq?(queue)
        queue[:name].to_s.end_with?(Pgbus::DEAD_LETTER_SUFFIX)
      end

      # Partition a backlog into [drained, undrained] using the set of queues
      # some configured capsule or EventBus handler drains. A nil drained set
      # means a wildcard capsule drains everything, so nothing is undrained.
      def partition_by_drain(backlog)
        return [backlog, []] if backlog.empty?

        drained_names = @data_source.drained_queue_names
        return [backlog, []] if drained_names.nil?

        backlog.partition { |q| drained_names.include?(q[:name].to_s) }
      end

      # Returns [health_hash, error_or_nil]. We never raise — pgbus_health
      # must always produce a verdict — but we DO surface the failure so the
      # caller knows the verdict was built from partial data, and we log it
      # for operators (per the project's no-silent-errors rule).
      def safe_queue_health
        [@data_source.queue_health_stats, nil]
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus::MCP] queue_health_stats failed: #{e.class}: #{e.message}" }
        [{}, e]
      end

      # STALLED detection. Two distinct wedge signatures, with different scopes:
      #
      #   * A worker self-reporting :stalled (heart-beating but claim loop frozen)
      #     is a wedge regardless of WHICH queue holds the backlog — the worker is
      #     broken. So this branch reasons against the FULL active backlog. This
      #     is the specific #179 signature and stays worker-scoped.
      #   * Live-but-idle draining processes with a never-claimed backlog is only
      #     a wedge for queues those processes can actually claim from — so this
      #     branch reasons against the DRAINED backlog only (issue #367 defect 2).
      #     Liveness is the evidence, and `drained_queue_names` covers both
      #     worker-drained job queues and consumer-drained handler queues, so a
      #     live process of either draining kind counts (a consumer-only fleet
      #     must still catch a wedged handler queue).
      def stalled_reasons(active_backlog, drained_backlog, processes)
        stalled_workers = processes.select { |p| p[:kind] == WORKER_KIND && p[:status].to_s == "stalled" }
        live_drainers   = processes.select do |p|
          DRAINING_KINDS.include?(p[:kind].to_s) && %w[healthy stalled].include?(p[:status].to_s)
        end

        reasons = []
        if stalled_workers.any? && active_backlog.any?
          reasons << "#{stalled_workers.size} worker(s) stalled (heart-beating but claim loop not advancing) " \
                     "while #{active_backlog.size} queue(s) have visible backlog: #{backlog_names(active_backlog)}"
        elsif live_drainers.any? && drained_backlog.any? && any_unread?(drained_backlog)
          unread = drained_backlog.select { |q| unread?(q) }
          reasons << "#{unread.size} queue(s) have visible messages with read_ct=0 " \
                     "(never claimed#{unread_age_clause(unread)}) " \
                     "while #{live_drainers.size} draining process(es) are alive: #{backlog_names(unread)}"
        end
        reasons
      end

      # DEGRADED detection: conditions worth surfacing that are not the wedge.
      # +undrained+ is the active backlog on queues no configured capsule/handler
      # drains (issue #367 defect 2) — a real hazard (nothing will empty them),
      # but not the worker wedge, so it is DEGRADED rather than STALLED.
      def degraded_reasons(queues, backlog, undrained, processes, health, health_error)
        reasons = []

        reasons << "queue health stats unavailable: #{health_error.class}: #{health_error.message}" if health_error

        stale = processes.select { |p| p[:status].to_s == "stale" }
        reasons << "#{stale.size} process(es) stale (no recent heartbeat)" if stale.any?

        paused_backlog = backlog.select { |q| q[:paused] }
        reasons << "#{paused_backlog.size} paused queue(s) holding a backlog: #{backlog_names(paused_backlog)}" if paused_backlog.any?

        # Every active undrained queue is the "nobody drains this" hazard: it
        # holds a visible backlog and no configured capsule/handler will empty
        # it. Past reads don't make it safe — if nothing drains it now, whatever
        # read those messages is gone. (`undrained` is already the visible,
        # non-paused backlog on queues no capsule drains.)
        if undrained.any?
          reasons << "#{undrained.size} queue(s) hold messages but no capsule is " \
                     "configured to drain them: #{backlog_names(undrained)}"
        end

        dlq = queues.select { |q| dlq?(q) && q[:queue_length].to_i.positive? }
        reasons << "#{dlq.size} dead-letter queue(s) hold messages" if dlq.any?

        age = health[:oldest_transaction_age_sec]
        reasons << "oldest open transaction is #{age}s old (MVCC horizon pinning risk)" if age && age > 300

        unregistered_hint = unregistered_stream_hint
        reasons << unregistered_hint if unregistered_hint

        reasons
      end

      # Issue #366: fingerprint-matched streams excluded from the wedge signal
      # still need the registry for orphan sweep bookkeeping and durable
      # wildcard safety. Surface a DEGRADED hint so operators run the backfill.
      def unregistered_stream_hint
        return unless @data_source.respond_to?(:unregistered_stream_queue_count)

        count = @data_source.unregistered_stream_queue_count
        return if count.to_i <= 0

        "#{count} unregistered stream queue(s) detected (dormant pre-registry durable " \
          "streams). Run: rake pgbus:streams:backfill_registry"
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::MCP] unregistered stream hint failed: #{e.class}: #{e.message}" }
        nil
      end

      # True when any queue in the backlog holds a visible, never-claimed
      # message — the wedge signal.
      def any_unread?(backlog)
        backlog.any? { |q| unread?(q) }
      end

      # True when a queue has at least one visible (claimable) message with
      # read_ct=0. This counts per visible message (issue #367 review), so one
      # retried or in-flight message (read_ct>0) can no longer veto the signal
      # for a pile of genuinely-unclaimed jobs the way max(read_ct) did.
      #
      # When metrics don't expose the count (an old hash / a stubbed test) we
      # conservatively treat the queue as unread so we never miss the wedge.
      #
      # Issue #589: the oldest such row must also have been claimable for at
      # least the grace. A fresh enqueue, or a delayed job whose wait just
      # expired, is visible with read_ct=0 until a worker's next poll, so
      # without the grace a healthy fleet read STALLED (and /readyz 503) on a
      # large share of checks. An unknown age (key missing or nil) stays
      # conservative and counts.
      def unread?(queue)
        return true unless queue.key?(:visible_unread_length)
        return false unless queue[:visible_unread_length].to_i.positive?

        age = unread_age(queue)
        age.nil? || age >= grace
      end

      def grace
        @grace || Pgbus.configuration.health_unclaimed_grace
      end

      def unread_age(queue)
        queue[:oldest_unread_claimable_age_sec]
      end

      # Oldest known unread claimable age over +queues+, nil when none is known.
      def oldest_unread_age(queues)
        queues.filter_map { |q| unread_age(q) }.max
      end

      # ", oldest claimable for 74s" so an operator can tell a wedge from a
      # spike; empty when no flagged queue reports an age.
      def unread_age_clause(queues)
        age = oldest_unread_age(queues)
        age ? ", oldest claimable for #{age}s" : ""
      end

      # Join backlog queue names for a reason string, truncated so a flagged
      # fleet of hundreds of queues doesn't produce a multi-KB log line
      # (issue #367 minor). Lists the first NAME_LIMIT, then "(+N more)".
      def backlog_names(queues)
        names = queues.map { |q| q[:name] }
        shown = names.first(NAME_LIMIT).join(", ")
        overflow = names.size - NAME_LIMIT
        overflow.positive? ? "#{shown} (+#{overflow} more)" : shown
      end

      def build_summary(queues, non_dlq, processes, health)
        workers = stale = stalled = 0
        processes.each do |p|
          status = p[:status].to_s
          stale += 1 if status == "stale"
          next unless p[:kind] == WORKER_KIND

          workers += 1
          stalled += 1 if status == "stalled"
        end

        {
          queues: queues.size,
          total_visible: non_dlq.sum { |q| q[:queue_visible_length].to_i },
          total_depth: non_dlq.sum { |q| q[:queue_length].to_i },
          workers: workers,
          stalled_workers: stalled,
          stale_processes: stale,
          oldest_transaction_age_sec: health[:oldest_transaction_age_sec]
        }
      end
    end
  end
end
