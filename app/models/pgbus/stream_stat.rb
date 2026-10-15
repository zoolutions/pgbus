# frozen_string_literal: true

module Pgbus
  # Records one row per stream event (broadcast / connect / disconnect)
  # when `config.streams_stats_enabled` is true. Mirrors JobStat in
  # shape and aggregation API so the Insights controller can query
  # both with the same patterns.
  #
  # Writes are fire-and-forget: any error is swallowed so a stat
  # recording failure cannot affect the dispatcher's hot path.
  class StreamStat < BusRecord
    self.table_name = "pgbus_stream_stats"

    EVENT_TYPES = %w[broadcast connect disconnect].freeze

    scope :since, ->(time) { where("created_at >= ?", time) }
    # NOTE: scope names intentionally avoid `broadcasts` / `connects` /
    # `disconnects`. `turbo-rails` auto-includes `Turbo::Broadcastable`
    # into every AR model, which defines a class method `broadcasts`.
    # AR's dangerous_class_method? guard then rejects a colliding
    # scope name at load time, crashing eager_load. See issue #92.
    scope :broadcast_events, -> { where(event_type: "broadcast") }
    scope :connect_events, -> { where(event_type: "connect") }
    scope :disconnect_events, -> { where(event_type: "disconnect") }

    # Records a stream event. Called from the Dispatcher when the
    # `streams_stats_enabled` flag is set. Errors are swallowed so a
    # missing table or a connection blip cannot kill the dispatcher.
    def self.record!(stream_name:, event_type:, duration_ms: 0, fanout: nil)
      return unless table_exists?

      create!(
        stream_name: stream_name,
        event_type: event_type,
        duration_ms: duration_ms.to_i,
        fanout: fanout
      )
    rescue StandardError => e
      Pgbus.logger.debug { "[Pgbus] Failed to record stream stat: #{e.message}" }
    end

    # Memoized — intentionally never invalidated at runtime. If the
    # pgbus_stream_stats migration runs while the app is already
    # running, a restart is required for stat recording to begin.
    #
    # We only memoize a *successful* probe. A transient error (PG
    # hiccup during boot, connection refused during a failover) is
    # treated as "don't know yet" — the next call retries. Caching
    # false on the first hiccup would permanently disable stream
    # stats for the process lifetime, which is a worse failure mode
    # than a few retries.
    # An idempotent schema memo: concurrent first callers probe the same
    # schema and store the same Boolean, so the unlocked read/write is safe.
    # rubocop:disable-next ThreadSafety/ClassInstanceVariable
    def self.table_exists?
      return @table_exists if defined?(@table_exists)

      @table_exists = connection.table_exists?(table_name)
    rescue StandardError => e
      Pgbus.logger.debug { "[Pgbus] Failed to check stream stat table: #{e.message}" }
      false
    end

    # Single-query aggregate summary over the given window.
    # Returns totals by event type, average fanout for broadcasts,
    # and an "active" estimate (connects − disconnects in window).
    def self.summary(minutes: 60)
      row = since(minutes.minutes.ago).pick(
        Arel.sql("COUNT(*) FILTER (WHERE event_type = 'broadcast')"),
        Arel.sql("COUNT(*) FILTER (WHERE event_type = 'connect')"),
        Arel.sql("COUNT(*) FILTER (WHERE event_type = 'disconnect')"),
        Arel.sql("ROUND(AVG(fanout) FILTER (WHERE event_type = 'broadcast')::numeric, 1)"),
        Arel.sql("ROUND(AVG(duration_ms) FILTER (WHERE event_type = 'broadcast')::numeric, 1)"),
        Arel.sql("ROUND(AVG(duration_ms) FILTER (WHERE event_type = 'connect')::numeric, 1)")
      )

      {
        broadcasts: row[0].to_i,
        connects: row[1].to_i,
        disconnects: row[2].to_i,
        active_estimate: [row[1].to_i - row[2].to_i, 0].max,
        avg_fanout: row[3]&.to_f || 0,
        avg_broadcast_ms: row[4]&.to_f || 0,
        avg_connect_ms: row[5]&.to_f || 0
      }
    end

    # Top N streams by broadcast count in the window, with avg fanout.
    def self.top_streams(limit: 10, minutes: 60)
      broadcast_events
        .since(minutes.minutes.ago)
        .group(:stream_name)
        .order(Arel.sql("COUNT(*) DESC"))
        .limit(limit)
        .pluck(
          :stream_name,
          Arel.sql("COUNT(*)"),
          Arel.sql("ROUND(AVG(fanout)::numeric, 1)"),
          Arel.sql("ROUND(AVG(duration_ms)::numeric, 1)")
        )
        .map do |name, count, avg_fanout, avg_ms|
          {
            stream_name: name,
            count: count.to_i,
            avg_fanout: avg_fanout&.to_f || 0,
            avg_ms: avg_ms&.to_f || 0
          }
        end
    end

    # Throughput: broadcast events per minute bucketed by minute.
    def self.throughput(minutes: 60)
      broadcast_events
        .since(minutes.minutes.ago)
        .group("date_trunc('minute', created_at)")
        .order(Arel.sql("date_trunc('minute', created_at)"))
        .count
    end

    # Cleanup old stats. Called from Pgbus::Process::Dispatcher on the
    # same cadence as JobStat.cleanup! using the shared stats_retention.
    def self.cleanup!(older_than:)
      where("created_at < ?", older_than).delete_all
    end
  end
end
