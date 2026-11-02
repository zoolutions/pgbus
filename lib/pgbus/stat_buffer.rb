# frozen_string_literal: true

module Pgbus
  # Thread-safe buffer that accumulates job stats in memory and flushes
  # them to the database in batches. This avoids one INSERT per job
  # execution, replacing it with periodic bulk inserts.
  class StatBuffer
    DEFAULT_FLUSH_SIZE = 100
    DEFAULT_FLUSH_INTERVAL = 5 # seconds
    BASE_COLUMNS = "job_class, queue_name, status, duration_ms"
    ALL_COLUMNS = "#{BASE_COLUMNS}, enqueue_latency_ms, retry_count".freeze

    attr_reader :flush_size, :flush_interval

    def initialize(flush_size: DEFAULT_FLUSH_SIZE, flush_interval: DEFAULT_FLUSH_INTERVAL)
      @flush_size = flush_size
      @flush_interval = flush_interval
      @buffer = []
      @mutex = Mutex.new
      @last_flush_at = monotonic_now
      @stopped = false
    end

    # Append a stat entry to the buffer. Flushes automatically when
    # the buffer reaches flush_size.
    def push(attrs)
      should_flush = false

      @mutex.synchronize do
        @buffer << attrs
        should_flush = @buffer.size >= @flush_size
      end

      flush if should_flush
    end

    # Flush buffered stats to the database. Safe to call from any thread.
    def flush
      entries = nil

      @mutex.synchronize do
        return if @buffer.empty?

        entries = @buffer.dup
        @buffer.clear
        @last_flush_at = monotonic_now
      end

      write_to_database(entries) if entries&.any?
    end

    # Flush if the interval has elapsed since the last flush.
    # Called by the dispatcher on its maintenance tick.
    def flush_if_due
      due = @mutex.synchronize { monotonic_now - @last_flush_at >= @flush_interval }
      flush if due
    end

    def stop
      @stopped = true
      flush
    end

    def size
      @mutex.synchronize { @buffer.size }
    end

    private

    # One multi-row INSERT per flush, built here rather than by
    # JobStat.insert_all: ActiveRecord's per-value attribute check and type
    # cast and its RETURNING "id" cost ~1 ms of Ruby CPU per 100 rows, 12-15 %
    # of a no-op job's on-CPU time (issue #622). The integer columns are
    # written through Integer(); the strings (job_class comes from the
    # message) go through the connection's quoting, once per distinct value.
    #
    # Always attempt all 6 columns. If the latency migration hasn't run, the
    # statement fails, and we fall back to base columns. This avoids relying
    # on the memoized latency_columns? flag which can be stuck at false if
    # evaluated before the connection is ready.
    def write_to_database(entries)
      return unless JobStat.table_exists?

      JobStat.connection_pool.with_connection do |conn|
        conn.execute(insert_sql(conn, entries, latency: true), "Pgbus::JobStat Bulk Insert")
      rescue ActiveRecord::StatementInvalid => e
        raise unless missing_latency_column?(e)

        write_base_columns(conn, entries)
      end
    rescue StandardError => e
      Pgbus.logger.warn { "[Pgbus] Stat buffer flush failed: #{e.message}" }
    end

    # PostgreSQL's undefined_column (42703, PG::UndefinedColumn: the cause
    # ActiveRecord keeps) naming a latency column in its first line. The
    # class, not the wording, says a column is missing (the wording follows
    # the server's lc_messages); only the first line names which, because the
    # "LINE 1:" excerpt of the statement below it shows both latency columns
    # whatever went wrong. pg is loaded once PostgreSQL has raised anything.
    def missing_latency_column?(error)
      return false unless defined?(::PG::UndefinedColumn) && error.cause.is_a?(::PG::UndefinedColumn)

      primary = error.cause.message.lines.first.to_s
      primary.include?("enqueue_latency_ms") || primary.include?("retry_count")
    end

    def write_base_columns(conn, entries)
      Pgbus.logger.warn { "[Pgbus] Latency columns missing — recording stats without latency data" } unless @degraded_warned
      @degraded_warned = true
      conn.execute(insert_sql(conn, entries, latency: false), "Pgbus::JobStat Bulk Insert")
    rescue StandardError => e
      Pgbus.logger.warn { "[Pgbus] Stat buffer fallback flush failed: #{e.message}" }
    end

    def insert_sql(conn, entries, latency:)
      quoted = Hash.new { |memo, value| memo[value] = conn.quote(value) }
      columns = latency ? ALL_COLUMNS : BASE_COLUMNS
      values = entries.map do |e|
        row = "(#{quoted[e[:job_class]]}, #{quoted[e[:queue_name]]}, #{quoted[e[:status]]}, #{sql_integer(e[:duration_ms])}"
        row << ", #{sql_integer(e[:enqueue_latency_ms])}, #{sql_integer(e[:retry_count])}" if latency
        row << ")"
      end
      "INSERT INTO #{JobStat.quoted_table_name} (#{columns}) VALUES #{values.join(", ")}"
    end

    def sql_integer(value)
      value.nil? ? "NULL" : Integer(value).to_s
    end

    def monotonic_now
      ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
    end
  end
end
