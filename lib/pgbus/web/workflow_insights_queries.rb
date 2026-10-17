# frozen_string_literal: true

module Pgbus
  module Web
    # The SQL behind the workflow insights (issue #549): one query per block
    # of the page, each bounded by the window and served by an index #540
    # created. The step stats read idx_pgbus_workflow_steps_insights
    # (workflow_class, step_name, finished_at); the run counts read
    # idx_pgbus_workflows_class_status (workflow_class, status, created_at),
    # with every status listed so the created_at range applies under each;
    # the ended runs read idx_pgbus_workflows_status (status, updated_at): a
    # run ends in the UPDATE that sets its finished_at/cancelled_at, so
    # updated_at >= the ended-at bound keeps every row the window wants.
    #
    # Status lists are the States constants, inlined; every value that comes
    # from outside (a class name, a time, a bucket length) is a bind.
    module WorkflowInsightsQueries
      ALL = Workflow::States::WORKFLOW
      ONGOING = Workflow::States::ONGOING
      # Upper bounds (ms) of the end-to-end duration histogram: < 1 s, 10 s,
      # 1 min, 10 min, 1 h, 6 h, 1 d, 7 d, and the open last bin.
      DURATION_BOUNDS_MS = [1_000, 10_000, 60_000, 600_000, 3_600_000, 21_600_000, 86_400_000, 604_800_000].freeze
      REVISIONS = 5

      module_function

      # Every workflow_class with a row, by a loose index scan (one probe per
      # class) instead of a DISTINCT over every run.
      def class_names
        sql = <<~SQL
          WITH RECURSIVE classes AS (
            (SELECT workflow_class FROM #{runs} ORDER BY workflow_class LIMIT 1)
            UNION ALL
            SELECT (SELECT w.workflow_class FROM #{runs} w WHERE w.workflow_class > c.workflow_class
                    ORDER BY w.workflow_class LIMIT 1)
            FROM classes c WHERE c.workflow_class IS NOT NULL
          )
          SELECT workflow_class FROM classes WHERE workflow_class IS NOT NULL
        SQL
        select(sql, "Classes").rows.map(&:first)
      end

      def class_known?(workflow_class)
        select("SELECT 1 FROM #{runs} WHERE workflow_class = $1 LIMIT 1", "Class Known", [workflow_class]).rows.any?
      end

      # The ongoing runs by (class, status, current step).
      def ongoing(workflow_class: nil)
        sql = <<~SQL
          SELECT workflow_class, status, current_step, COUNT(*)
          FROM #{runs}
          WHERE status IN (#{list(ONGOING)})#{" AND workflow_class = $1" if workflow_class}
          GROUP BY workflow_class, status, current_step
        SQL
        select(sql, "Ongoing", [workflow_class].compact).cast_values.map do |klass, status, step, runs|
          { workflow_class: klass, status: status, step: step, runs: runs.to_i }
        end
      end

      # Per class: the runs that finished or were cancelled since +since+,
      # and the end-to-end percentiles (created → finished) of the finished.
      def ended(since:, workflow_class: nil)
        sql = <<~SQL
          SELECT workflow_class, COUNT(*) FILTER (WHERE status = 'finished'), COUNT(*) FILTER (WHERE status = 'cancelled'),
                 COUNT(e2e_ms), #{percentiles("e2e_ms", 0.5, 0.95, 0.99)}
          FROM (#{ended_runs(workflow_class)}) ended
          GROUP BY workflow_class
        SQL
        select(sql, "Ended", [since, workflow_class].compact).cast_values.map do |klass, finished, cancelled, samples, *pcts|
          { workflow_class: klass, finished: finished.to_i, cancelled: cancelled.to_i,
            e2e_ms: percentile_hash(%i[p50 p95 p99], pcts, samples) }
        end
      end

      # { bin => runs } of one class's finished runs over DURATION_BOUNDS_MS
      # (bin 0 is under the first bound, the last bin is past the last one).
      def duration_histogram(workflow_class, since:)
        sql = <<~SQL
          SELECT width_bucket(e2e_ms::float8, ARRAY[#{DURATION_BOUNDS_MS.join(", ")}]::float8[]), COUNT(*)
          FROM (#{ended_runs(workflow_class)}) ended
          WHERE status = 'finished'
          GROUP BY 1
        SQL
        select(sql, "Durations", [since, workflow_class]).rows.to_h { |bin, runs| [bin.to_i, runs.to_i] }
      end

      # { bucket start (epoch seconds) => [started, finished, cancelled] }.
      # Buckets start at multiples of +bucket+ in a zone +offset+ seconds
      # from UTC (a day bucket at the zone's midnight).
      def throughput(workflow_class, since:, bucket:, offset: 0)
        sql = <<~SQL
          SELECT bucket, SUM(started), SUM(finished), SUM(cancelled) FROM (
            SELECT FLOOR((EXTRACT(EPOCH FROM created_at) + $4) / $3)::bigint AS bucket, 1 AS started, 0 AS finished,
                   0 AS cancelled
            FROM #{runs}
            WHERE workflow_class = $2 AND status IN (#{list(ALL)}) AND created_at >= $1
            UNION ALL
            SELECT FLOOR((EXTRACT(EPOCH FROM ended_at) + $4) / $3)::bigint, 0, (status = 'finished')::int,
                   (status = 'cancelled')::int
            FROM (#{ended_runs(workflow_class)}) ended
          ) buckets
          GROUP BY bucket
        SQL
        select(sql, "Throughput", [since, workflow_class, bucket, offset]).rows.to_h do |start, started, finished, cancelled|
          [(start.to_i * bucket) - offset, [started.to_i, finished.to_i, cancelled.to_i]]
        end
      end

      # Per step: the attempts that finished since +since+.
      def step_stats(workflow_class, since:)
        sql = <<~SQL
          SELECT step_name, #{stat_columns}, MIN(created_at)
          FROM (#{attempt_rows}) attempts
          GROUP BY step_name
        SQL
        select(sql, "Steps", [workflow_class, since]).cast_values.to_h do |name, *stats, first_seen_at|
          [name, attempt_stats(stats).merge(first_seen_at: first_seen_at)]
        end
      end

      # Per step, over the runs started since +since+: how many have a row
      # for it (reached) and its mean position in their runs (the step order
      # of a class that no longer loads). Partitioned by the run's bigint id:
      # sorting 50k workflow_id strings under a locale collation costs ~4x.
      def funnel_reached(workflow_class, since:)
        sql = <<~SQL
          SELECT step_name, COUNT(*), AVG(position) FROM (
            SELECT s.step_name, RANK() OVER (PARTITION BY w.id ORDER BY MIN(s.id)) AS position
            FROM #{runs} w JOIN #{steps} s ON s.workflow_id = w.workflow_id
            WHERE w.workflow_class = $1 AND w.status IN (#{list(ALL)}) AND w.created_at >= $2
            GROUP BY w.id, s.step_name
          ) reached
          GROUP BY step_name
        SQL
        select(sql, "Funnel", [workflow_class, since]).rows.to_h do |name, reached, position|
          [name, { reached: reached.to_i, position: position.to_f }]
        end
      end

      # Where each run started since +since+ is now: { [step, status] => runs }.
      def funnel_stopped(workflow_class, since:)
        sql = <<~SQL
          SELECT current_step, status, COUNT(*)
          FROM #{runs}
          WHERE workflow_class = $1 AND status IN (#{list(ALL)}) AND created_at >= $2
          GROUP BY current_step, status
        SQL
        select(sql, "Stopped", [workflow_class, since]).rows.to_h { |step, status, runs| [[step, status], runs.to_i] }
      end

      # The step stats of the REVISIONS app revisions that finished an
      # attempt last, per (revision, step) and per revision (step nil).
      def by_revision(workflow_class, since:)
        sql = <<~SQL
          WITH revisions AS (
            SELECT app_revision, MIN(finished_at) AS first_seen_at, MAX(finished_at) AS last_seen_at
            FROM #{steps}
            WHERE workflow_class = $1 AND finished_at >= $2 AND app_revision IS NOT NULL
            GROUP BY app_revision
            ORDER BY last_seen_at DESC
            LIMIT #{REVISIONS}
          )
          SELECT attempts.app_revision, revisions.first_seen_at, revisions.last_seen_at, attempts.step_name, #{stat_columns}
          FROM (#{attempt_rows}) attempts
          JOIN revisions ON revisions.app_revision = attempts.app_revision
          GROUP BY GROUPING SETS ((attempts.app_revision, revisions.first_seen_at, revisions.last_seen_at, attempts.step_name),
                                  (attempts.app_revision, revisions.first_seen_at, revisions.last_seen_at))
          ORDER BY revisions.last_seen_at DESC, attempts.step_name NULLS FIRST
        SQL
        select(sql, "Revisions", [workflow_class, since]).cast_values.map do |revision, first_seen, last_seen, name, *stats|
          { revision: revision, first_seen_at: first_seen, last_seen_at: last_seen, step: name, stats: attempt_stats(stats) }
        end
      end

      # The finished and cancelled runs that ended since $1 (of class $2).
      def ended_runs(workflow_class)
        <<~SQL
          SELECT workflow_class, status, COALESCE(finished_at, cancelled_at) AS ended_at,
                 CASE WHEN status = 'finished' THEN GREATEST(EXTRACT(EPOCH FROM (finished_at - created_at)) * 1000, 0) END AS e2e_ms
          FROM #{runs}
          WHERE status IN ('finished', 'cancelled') AND updated_at >= $1
            AND COALESCE(finished_at, cancelled_at) >= $1#{" AND workflow_class = $2" if workflow_class}
        SQL
      end

      # The attempts of class $1 that finished since $2. run_ms: completed or
      # failed attempts that started; pickup_ms (scheduled → started, never
      # negative): every attempt that started.
      def attempt_rows
        <<~SQL
          SELECT step_name, status, outcome, error_class, app_revision, created_at,
                 CASE WHEN status IN ('completed', 'failed') AND started_at IS NOT NULL
                      THEN GREATEST(EXTRACT(EPOCH FROM (finished_at - started_at)) * 1000, 0) END AS run_ms,
                 CASE WHEN started_at IS NOT NULL
                      THEN GREATEST(EXTRACT(EPOCH FROM (started_at - scheduled_at)) * 1000, 0) END AS pickup_ms
          FROM #{steps}
          WHERE workflow_class = $1 AND finished_at >= $2
        SQL
      end

      STAT_KEYS = %i[attempts completed failed reattempted skipped cancelled].freeze

      def stat_columns
        <<~SQL.squish
          COUNT(*), COUNT(*) FILTER (WHERE status = 'completed'), COUNT(*) FILTER (WHERE error_class IS NOT NULL),
          COUNT(*) FILTER (WHERE outcome = 'reattempted'), COUNT(*) FILTER (WHERE status = 'skipped'),
          COUNT(*) FILTER (WHERE status = 'cancelled'),
          COUNT(run_ms), #{percentiles("run_ms", 0.5, 0.95, 0.99)},
          COUNT(pickup_ms), #{percentiles("pickup_ms", 0.5, 0.95)}
        SQL
      end

      def attempt_stats(values)
        counts = STAT_KEYS.zip(values.first(STAT_KEYS.size).map(&:to_i)).to_h
        run_samples, run50, run95, run99, pickup_samples, pickup50, pickup95 = values.drop(STAT_KEYS.size)
        counts.merge(run_ms: percentile_hash(%i[p50 p95 p99], [run50, run95, run99], run_samples),
                     pickup_ms: percentile_hash(%i[p50 p95], [pickup50, pickup95], pickup_samples))
      end

      def percentiles(column, *fractions)
        fractions.map { |f| "percentile_cont(#{f}) WITHIN GROUP (ORDER BY #{column})" }.join(", ")
      end

      def percentile_hash(keys, values, samples)
        keys.zip(values.map { |v| v&.to_f&.round(1) }).to_h.merge(samples: samples.to_i)
      end

      def list(statuses) = statuses.map { |status| connection.quote(status) }.join(", ")

      def runs = WorkflowEntry.quoted_table_name

      def steps = WorkflowStep.quoted_table_name

      def connection = WorkflowEntry.connection

      def select(sql, name, binds = []) = connection.exec_query(sql, "WorkflowInsights #{name}", binds)
    end
  end
end
