# frozen_string_literal: true

module Pgbus
  module Web
    class DataSource
      # The Jobs page's unified list (issue #489): every job the dashboard can
      # see, one row each, whatever table it lives in, with its state derived
      # in SQL so tabs, counts and paging all cut on the same value.
      #
      # Sources, one UNION ALL fragment each:
      # - every non-DLQ queue table, LEFT JOINed to its pgbus_failed_events row
      #   (failed_events.queue_name is the LOGICAL name; the physical one is
      #   matched too for rows older code wrote)
      # - failed rows whose message is no longer in its queue ("orphans")
      # - pgbus_blocked_executions (jobs parked by limits_concurrency)
      #
      # Queue names reach SQL only through sanitize_name; the state filter and
      # paging are bound parameters. Each fragment carries its own ORDER BY +
      # LIMIT so it stays index-backed and bounded; msg_ids from different
      # queues are not comparable, so the outer sort is by time.
      module JobList
        # A failure recorded after the read that ran the attempt means the job
        # is backing off; a later read (last_read_at past failed_at) means the
        # retry attempt is running now.
        STATE_CASE = <<~SQL.gsub(/\s+/, " ").strip.freeze
          CASE
            WHEN m.read_ct = 0 AND m.vt > now() THEN 'scheduled'
            WHEN m.read_ct = 0 THEN 'ready'
            WHEN f.failed_at IS NOT NULL AND f.failed_at >= m.last_read_at THEN 'retrying'
            WHEN m.vt > now() THEN 'running'
            ELSE 'ready'
          END
        SQL

        # Per-fragment ceiling for the tab counts, so a million-row queue never
        # forces a full scan on every auto-refresh. A fragment that reaches it
        # makes its states' counts lower bounds.
        COUNT_CAP = 10_000

        QUEUE_STATES = %w[ready scheduled running retrying].freeze

        # A predicate on m that each state branch of STATE_CASE already implies.
        # Without it a filtered tab walks the whole pkey and probes the LATERAL for
        # every row before LIMIT bites (300k-row queue, no matches: ~350 ms); with it
        # only candidates reach the CASE. The exact `q.state = $1` still applies.
        STATE_PREFILTER = {
          "scheduled" => "m.read_ct = 0 AND m.vt > now()",
          "ready" => "m.vt <= now()",
          "running" => "m.read_ct > 0 AND m.vt > now()",
          "retrying" => "m.read_ct > 0"
        }.freeze

        INTEGER_COLUMNS = %i[id msg_id read_ct failed_event_id slots_held slots_max].freeze

        StateCounts = Data.define(:counts, :capped) do
          def [](state) = counts.fetch(state.to_s, 0)
          def capped?(state) = capped.include?(state.to_s)
        end

        def job_rows(state: nil, queue_name: nil, page: 1, per_page: 25)
          scope = job_list_scope(queue_name)
          return [] unless scope

          binds = []
          state_param = nil
          if state && state != "blocked" && scope[:queues].any?
            binds << state
            state_param = "$1"
          end

          fetch = (page * per_page).to_i
          fragments = job_row_fragments(scope, state, state_param, fetch)
          return [] if fragments.empty?

          sql = <<~SQL
            SELECT * FROM (#{fragments.join("\nUNION ALL\n")}) AS jobs
            ORDER BY sort_at DESC NULLS LAST, id DESC
            LIMIT $#{binds.size + 1} OFFSET $#{binds.size + 2}
          SQL
          binds.push(per_page, (page - 1) * per_page)

          connection.select_all(sql, "Pgbus Job List", binds).to_a.map { |row| format_job_row(row) }
        rescue StandardError => e
          Pgbus.logger.error { "[Pgbus::Web] Error reading the job list: #{e.class}: #{e.message}" }
          []
        end

        def job_state_counts(queue_name: nil)
          scope = job_list_scope(queue_name)
          return empty_state_counts unless scope

          kinds = []
          fragments = []
          count_fragments(scope).each do |kind, sql|
            fragments << "(SELECT #{kinds.size} AS frag, state FROM (#{sql}) c LIMIT #{COUNT_CAP + 1})"
            kinds << kind
          end

          rows = connection.select_all(<<~SQL, "Pgbus Job State Counts").to_a
            SELECT frag, state, COUNT(*) AS n FROM (#{fragments.join("\nUNION ALL\n")}) AS counted
            GROUP BY frag, state
          SQL
          tally_state_counts(rows, kinds)
        rescue StandardError => e
          Pgbus.logger.error { "[Pgbus::Web] Error counting job states: #{e.class}: #{e.message}" }
          empty_state_counts
        end

        # { [physical_queue, msg_id] => jobs_ahead } for the ready queue rows
        # on one page: one bounded COUNT per distinct queue, never per row.
        def jobs_ahead(rows)
          ready = rows.select { |r| r[:source] == "queue" && r[:state] == "ready" }
          ready.group_by { |r| r[:queue_name] }.each_with_object({}) do |(queue, queue_rows), out|
            out.merge!(jobs_ahead_in(queue, queue_rows.map { |r| r[:msg_id].to_i }.sort))
          end
        end

        def job_list_context(now: Time.now)
          alive = ->(kind) { processes.any? { |p| p[:kind].to_s == kind && p[:healthy] } }
          JobState::Context.new(now: now, max_retries: Pgbus.configuration.max_retries,
                                paused: paused_queue_names.to_set, drained: drained_queue_names,
                                workers_alive: alive.call("worker"), handler_queues: handler_queue_physical_names.to_set,
                                consumers_alive: alive.call("consumer"))
        end

        private

        # The physical non-DLQ queues in view, grouped under their logical
        # name, or nil when the requested queue does not exist.
        def job_list_scope(queue_name)
          names = queues_with_metrics.map { |q| q[:name] }.reject { |n| n.end_with?(Pgbus::DEAD_LETTER_SUFFIX) }
          if queue_name
            return nil unless names.include?(queue_name)

            names = [queue_name]
          end

          queues = names.map { |n| [sanitize_name(n), logical_queue_name(n)] }
          { queues: queues, by_logical: queues.group_by(&:last), filtered: !queue_name.nil? }
        end

        def job_row_fragments(scope, state, state_param, fetch)
          fragments = []
          unless state == "blocked"
            fragments.concat(scope[:queues].map { |(qtable, logical)| queue_row_fragment(qtable, logical, state, state_param, fetch) })
          end
          fragments.concat(orphan_fragments(scope, fetch)) if state.nil? || state == "retrying"
          fragments << blocked_fragment(scope, fetch) if state.nil? || state == "blocked"
          fragments
        end

        def queue_row_fragment(qtable, logical, state, state_param, fetch)
          where = state_param ? "WHERE q.state = #{state_param}" : ""
          prefilter = state_param && STATE_PREFILTER[state] ? "WHERE #{STATE_PREFILTER[state]}" : ""
          <<~SQL.strip
            (SELECT * FROM (
              SELECT 'queue'::text AS source, m.msg_id AS id, '#{qtable}'::text AS queue_name,
                     '#{logical}'::text AS logical_queue, m.msg_id, m.message->>'job_class' AS job_class,
                     m.read_ct, m.enqueued_at, m.last_read_at, m.vt, #{STATE_CASE} AS state,
                     f.error_class, f.error_message, f.id AS failed_event_id,
                     NULL::text AS concurrency_key, NULL::integer AS slots_held, NULL::integer AS slots_max,
                     m.message::text AS payload, m.headers::text AS headers, m.enqueued_at AS sort_at
              FROM pgmq.q_#{qtable} m
              #{failed_join(qtable, logical)}
              #{prefilter}
            ) q #{where} ORDER BY q.msg_id DESC LIMIT #{fetch})
          SQL
        end

        def failed_join(qtable, logical)
          <<~SQL.strip
            LEFT JOIN LATERAL (
              SELECT f.id, f.error_class::text AS error_class, f.error_message,
                     f.failed_at::timestamptz AS failed_at
              FROM pgbus_failed_events f
              WHERE f.queue_name IN ('#{logical}', '#{qtable}') AND f.msg_id = m.msg_id
              ORDER BY f.failed_at DESC LIMIT 1
            ) f ON true
          SQL
        end

        # Failed rows whose message left its queue: one fragment per logical
        # queue, plus (unfiltered) one for rows whose queue no longer exists.
        def orphan_fragments(scope, fetch)
          fragments = scope[:by_logical].map do |logical, tables|
            missing = tables.map { |(qtable, _)| "NOT EXISTS (SELECT 1 FROM pgmq.q_#{qtable} m WHERE m.msg_id = f.msg_id)" }
            names = quoted_list([logical, *tables.map(&:first)])
            orphan_fragment("'#{logical}'::text", "f.queue_name IN (#{names}) AND #{missing.join(" AND ")}", fetch)
          end
          return fragments if scope[:filtered]

          known = scope[:by_logical].flat_map { |logical, tables| [logical, *tables.map(&:first)] }
          where = known.empty? ? "true" : "NOT (f.queue_name = ANY(ARRAY[#{quoted_list(known)}]))"
          fragments << orphan_fragment("f.queue_name::text", where, fetch)
        end

        def orphan_fragment(logical_sql, where, fetch)
          <<~SQL.strip
            (SELECT 'failed'::text AS source, f.id, f.queue_name::text AS queue_name, #{logical_sql} AS logical_queue,
                    f.msg_id, f.payload->>'job_class' AS job_class, (f.retry_count + 1) AS read_ct,
                    NULL::timestamptz AS enqueued_at, NULL::timestamptz AS last_read_at, NULL::timestamptz AS vt,
                    'retrying'::text AS state, f.error_class::text AS error_class, f.error_message,
                    f.id AS failed_event_id, NULL::text AS concurrency_key, NULL::integer AS slots_held,
                    NULL::integer AS slots_max, f.payload::text AS payload, f.headers::text AS headers,
                    f.failed_at::timestamptz AS sort_at
             FROM pgbus_failed_events f WHERE #{where} ORDER BY f.id DESC LIMIT #{fetch})
          SQL
        end

        def blocked_fragment(scope, fetch)
          <<~SQL.strip
            (SELECT 'blocked'::text AS source, b.id, b.queue_name::text AS queue_name,
                    b.queue_name::text AS logical_queue, NULL::bigint AS msg_id, b.payload->>'job_class' AS job_class,
                    NULL::integer AS read_ct, b.created_at::timestamptz AS enqueued_at,
                    NULL::timestamptz AS last_read_at, NULL::timestamptz AS vt, 'blocked'::text AS state,
                    NULL::text AS error_class, NULL::text AS error_message, NULL::bigint AS failed_event_id,
                    b.concurrency_key::text AS concurrency_key, s.value AS slots_held, s.max_value AS slots_max,
                    b.payload::text AS payload, NULL::text AS headers, b.created_at::timestamptz AS sort_at
             FROM pgbus_blocked_executions b
             LEFT JOIN pgbus_semaphores s ON s.key = b.concurrency_key
             #{blocked_where(scope)} ORDER BY b.created_at DESC LIMIT #{fetch})
          SQL
        end

        def blocked_where(scope)
          return "" unless scope[:filtered]

          qtable, logical = scope[:queues].first
          "WHERE b.queue_name IN (#{quoted_list([logical, qtable])})"
        end

        # [kind, sql selecting a `state` column] per fragment, uncapped.
        def count_fragments(scope)
          queue = scope[:queues].map do |(qtable, logical)|
            [:queue, "SELECT #{STATE_CASE} AS state FROM pgmq.q_#{qtable} m #{failed_join(qtable, logical)}"]
          end
          orphans = orphan_fragments(scope, COUNT_CAP + 1).map { |sql| [:orphan, "SELECT state FROM #{sql} o"] }
          queue + orphans + [[:blocked, "SELECT state FROM #{blocked_fragment(scope, COUNT_CAP + 1)} b2"]]
        end

        def tally_state_counts(rows, kinds)
          counts = Hash.new(0)
          per_fragment = Hash.new(0)
          rows.each do |row|
            n = row["n"].to_i
            counts[row["state"]] += n
            per_fragment[row["frag"].to_i] += n
          end

          capped = Set.new
          per_fragment.each do |frag, total|
            next if total <= COUNT_CAP

            capped.merge(capped_states_for(kinds[frag]))
          end
          counts["all"] = counts.values.sum
          StateCounts.new(counts: counts.to_h, capped: capped)
        end

        def capped_states_for(kind)
          states = { queue: QUEUE_STATES, orphan: %w[retrying], blocked: %w[blocked] }.fetch(kind)
          [*states, "all"]
        end

        def empty_state_counts
          StateCounts.new(counts: {}, capped: Set.new)
        end

        def jobs_ahead_in(queue, msg_ids)
          @jobs_ahead_memo ||= {}
          @jobs_ahead_memo[[queue, msg_ids]] ||= begin
            rows = connection.select_all(<<~SQL, "Pgbus Jobs Ahead", ["{#{msg_ids.join(",")}}"])
              SELECT x.msg_id, (
                SELECT COUNT(*) FROM (
                  SELECT 1 FROM pgmq.q_#{sanitize_name(queue)} m
                  WHERE m.vt <= now() AND m.msg_id < x.msg_id LIMIT #{JobState::AHEAD_CAP}
                ) s
              ) AS ahead
              FROM unnest($1::bigint[]) AS x(msg_id)
            SQL
            rows.to_a.to_h { |r| [[queue, r["msg_id"].to_i], r["ahead"].to_i] }
          end
        rescue StandardError => e
          Pgbus.logger.error { "[Pgbus::Web] Error counting jobs ahead in #{queue}: #{e.class}: #{e.message}" }
          {}
        end

        # Values are sanitized queue names (word characters only), so quoting
        # them as literals is safe.
        def quoted_list(names)
          names.uniq.map { |n| "'#{sanitize_name(n)}'" }.join(", ")
        end

        def format_job_row(row)
          row.to_h.each_with_object({}) do |(key, value), out|
            sym = key.to_sym
            out[sym] = INTEGER_COLUMNS.include?(sym) && !value.nil? ? value.to_i : value
          end
        end
      end
    end
  end
end
