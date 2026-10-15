# frozen_string_literal: true

module Pgbus
  class JobStat < BusRecord
    self.table_name = "pgbus_job_stats"

    scope :since, ->(time) { where("created_at >= ?", time) }
    scope :successful, -> { where(status: "success") }
    scope :failed, -> { where(status: "failed") }
    scope :dead_lettered, -> { where(status: "dead_lettered") }

    # Record a job execution stat. Called by the executor after each job.
    def self.record!(job_class:, queue_name:, status:, duration_ms:, enqueue_latency_ms: nil, retry_count: 0)
      return unless table_exists?

      attrs = {
        job_class: job_class,
        queue_name: queue_name,
        status: status,
        duration_ms: duration_ms
      }
      attrs[:enqueue_latency_ms] = enqueue_latency_ms if latency_columns?
      attrs[:retry_count] = retry_count if latency_columns?

      create!(attrs)
    rescue StandardError => e
      Pgbus.logger.debug { "[Pgbus] Failed to record job stat: #{e.message}" }
    end

    # Memoized — intentionally never invalidated at runtime. If the
    # pgbus_job_stats migration runs while the app is already running,
    # a restart is required for stat recording to begin.
    #
    # We only memoize a *successful* probe. A transient error (PG
    # hiccup during boot, connection refused during a failover) is
    # treated as "don't know yet" — the next call retries. Caching
    # false on the first hiccup would permanently disable job stats
    # for the process lifetime, which is a worse failure mode than
    # a few retries. See issue #98 / PR #91 (StreamStat fix).
    # An idempotent schema memo: concurrent first callers probe the same
    # schema and store the same Boolean, so the unlocked read/write is safe.
    # rubocop:disable ThreadSafety/ClassInstanceVariable
    def self.table_exists?
      return @table_exists if defined?(@table_exists)

      @table_exists = connection.table_exists?(table_name)
    rescue StandardError => e
      Pgbus.logger.debug { "[Pgbus] Failed to check job stat table: #{e.message}" }
      false
    end

    # Memoized — checks if the latency migration has been applied.
    # Same transient-error handling as `table_exists?`: a failed
    # probe is not cached, so a later successful probe can still
    # enable latency recording.
    def self.latency_columns?
      return @latency_columns if defined?(@latency_columns)
      return false unless table_exists?

      @latency_columns = column_names.include?("enqueue_latency_ms")
    rescue StandardError => e
      Pgbus.logger.debug { "[Pgbus] Failed to check job stat latency columns: #{e.message}" }
      false
    end
    # rubocop:enable ThreadSafety/ClassInstanceVariable

    # Throughput: jobs per minute bucketed by minute for the last N minutes
    def self.throughput(minutes: 60)
      since(minutes.minutes.ago)
        .group("date_trunc('minute', created_at)")
        .order(Arel.sql("date_trunc('minute', created_at)"))
        .count
    end

    # Average duration by job class
    def self.avg_duration_by_class(minutes: 60)
      since(minutes.minutes.ago)
        .group(:job_class)
        .order(Arel.sql("AVG(duration_ms) DESC"))
        .average(:duration_ms)
    end

    # Success/fail/DLQ counts
    def self.status_counts(minutes: 60)
      since(minutes.minutes.ago).group(:status).count
    end

    # Top N slowest job classes by average duration
    def self.slowest_classes(limit: 10, minutes: 60)
      since(minutes.minutes.ago)
        .group(:job_class)
        .order(Arel.sql("AVG(duration_ms) DESC"))
        .limit(limit)
        .pluck(:job_class, Arel.sql("COUNT(*)"), Arel.sql("ROUND(AVG(duration_ms))"), Arel.sql("MAX(duration_ms)"))
        .map { |cls, count, avg, max| { job_class: cls, count: count.to_i, avg_ms: avg.to_i, max_ms: max.to_i } }
    end

    # Single-query aggregate summary using conditional counts.
    def self.summary(minutes: 60)
      cols = [
        "COUNT(*)",
        "COUNT(*) FILTER (WHERE status = 'success')",
        "COUNT(*) FILTER (WHERE status = 'failed')",
        "COUNT(*) FILTER (WHERE status = 'dead_lettered')",
        "ROUND(AVG(duration_ms)::numeric, 1)",
        "MAX(duration_ms)"
      ]
      if latency_columns?
        cols.push(
          "ROUND(AVG(enqueue_latency_ms) FILTER (WHERE enqueue_latency_ms IS NOT NULL)::numeric, 1)",
          "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY enqueue_latency_ms) " \
          "FILTER (WHERE enqueue_latency_ms IS NOT NULL)",
          "PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY enqueue_latency_ms) " \
          "FILTER (WHERE enqueue_latency_ms IS NOT NULL)",
          "PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY enqueue_latency_ms) " \
          "FILTER (WHERE enqueue_latency_ms IS NOT NULL)",
          "ROUND(AVG(retry_count) FILTER (WHERE retry_count IS NOT NULL)::numeric, 2)"
        )
      end

      row = since(minutes.minutes.ago).pick(*cols.map { |c| Arel.sql(c) })

      result = {
        total: row[0].to_i,
        success: row[1].to_i,
        failed: row[2].to_i,
        dead_lettered: row[3].to_i,
        avg_duration_ms: row[4]&.to_f || 0,
        max_duration_ms: row[5].to_i
      }

      if latency_columns?
        result.merge!(
          avg_latency_ms: row[6]&.to_f || 0,
          p50_latency_ms: row[7]&.to_f || 0,
          p95_latency_ms: row[8]&.to_f || 0,
          p99_latency_ms: row[9]&.to_f || 0,
          avg_retries: row[10]&.to_f || 0
        )
      end

      result
    end

    # Latency trend: average enqueue latency per minute bucketed
    def self.latency_trend(minutes: 60)
      return [] unless latency_columns?

      since(minutes.minutes.ago)
        .where.not(enqueue_latency_ms: nil)
        .group("date_trunc('minute', created_at)")
        .order(Arel.sql("date_trunc('minute', created_at)"))
        .pluck(
          Arel.sql("date_trunc('minute', created_at)"),
          Arel.sql("ROUND(AVG(enqueue_latency_ms))"),
          Arel.sql("ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY enqueue_latency_ms))")
        )
        .map { |time, avg, p95| { time: time, avg_ms: avg.to_i, p95_ms: p95.to_i } }
    end

    # Average latency by queue
    def self.avg_latency_by_queue(minutes: 60)
      return [] unless latency_columns?

      since(minutes.minutes.ago)
        .where.not(enqueue_latency_ms: nil)
        .group(:queue_name)
        .order(Arel.sql("AVG(enqueue_latency_ms) DESC"))
        .pluck(
          :queue_name,
          Arel.sql("ROUND(AVG(enqueue_latency_ms))"),
          Arel.sql("ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY enqueue_latency_ms))"),
          Arel.sql("COUNT(*)")
        )
        .map { |q, avg, p95, count| { queue_name: q, avg_ms: avg.to_i, p95_ms: p95.to_i, count: count.to_i } }
    end

    # Cleanup old stats
    def self.cleanup!(older_than:)
      where("created_at < ?", older_than).delete_all
    end
  end
end
