# frozen_string_literal: true

# Fleet cell of the worker profiling bench (issue #602): the pure pieces.
#
# The fleet cell drains one queue with N worker processes on one Postgres, the
# production shape (`processes: N`). This module holds what carries no database
# or subprocess dependency (the WP_BENCH_PROCESSES knob, the connection budget,
# the throughput and pg_stat_statements arithmetic and the table) so
# spec/benchmarks/fleet_profile_harness_spec.rb pins it without a run.
module FleetProfileHarness
  DEFAULT_PROCESSES = "1,2,4"
  # Each drain child holds its pgmq pool plus its ActiveRecord pool, capped at
  # this size (stats flushes, its pgbus_processes row; a further checkout waits
  # instead of connecting), so the budget is a bound, not an estimate. Measured
  # peak: 16 connections for one child at pool_size 14 (the parent's
  # included), 58-62 for four.
  AR_PER_PROCESS = 2
  # Room for the parent's own connections and anything else that connects.
  BUDGET_HEADROOM = 10
  TOP_STATEMENTS = 5
  QUERY_WIDTH = 60
  WAIT_KINDS = 6

  module_function

  def parse_processes(value)
    counts = value.to_s.split(",", -1).map(&:strip)
    raise ArgumentError, "WP_BENCH_PROCESSES must list positive integers, like 1,2,4 (got #{value.inspect})" if
      counts.empty? || counts.any? { |c| !c.match?(/\A[1-9]\d*\z/) }

    counts.map { |c| Integer(c, 10) }
  end

  def pg_stat_preload_reason(shared_preload_libraries)
    return if shared_preload_libraries.to_s.split(",").map(&:strip).include?("pg_stat_statements")

    "add it to shared_preload_libraries"
  end

  def budgeted_connections(processes:, pool_size:) = (processes * (pool_size + AR_PER_PROCESS)) + BUDGET_HEADROOM

  def free_connections(max_connections:, reserved:, in_use:)
    max_connections - reserved - in_use
  end

  # nil when the children's connections (plus headroom) fit the free
  # connections, else the reason the row is skipped.
  def budget_skip_reason(processes:, pool_size:, free_connections:)
    needed = budgeted_connections(processes: processes, pool_size: pool_size)
    return if needed <= free_connections

    "skipped: needs #{processes} × (#{pool_size} + #{AR_PER_PROCESS}) + #{BUDGET_HEADROOM} = #{needed} connections, " \
      "#{free_connections} free"
  end

  # children: each child's {"processed", "started_at", "finished_at"}, CLOCK_REALTIME
  # seconds. go_at is the parent's CLOCK_REALTIME stamp just before it released
  # the start barrier; finished_at is the moment a child saw its last job finish.
  def throughput(children, go_at:)
    processed = children.sum { |c| c.fetch("processed") }
    wall_s = children.map { |c| c.fetch("finished_at") }.max - go_at
    starts = children.map { |c| c.fetch("started_at") }
    { processed: processed, wall_s: wall_s, jobs_per_s: processed / wall_s,
      start_skew_ms: (starts.max - starts.min) * 1000 }
  end

  # rows: pg_stat_statements rows ({"query", "calls", "total_exec_time", and
  # "total_plan_time" in ms}). A statement's time is execution plus planning:
  # a plpgsql function plans its dynamic EXECUTE inside its own execution
  # time, while a top-level statement's planning is total_plan_time, recorded
  # only with pg_stat_statements.track_planning on (0 otherwise). Without it,
  # pgbus's own SQL (issue #605) would look cheaper by its planning.
  # busy_backends is Σ that time over the wall: how many backends were
  # executing pgbus statements at any moment, on average.
  def summarize_statements(rows, jobs:, wall_s:)
    total_ms = rows.sum { |r| statement_ms(r) }
    calls = rows.sum { |r| r.fetch("calls").to_i }
    top = rows.max_by(TOP_STATEMENTS) { |r| statement_ms(r) }.map do |r|
      { "query" => r.fetch("query"), "calls" => r.fetch("calls").to_i,
        "ms_per_call" => statement_ms(r) / [r.fetch("calls").to_i, 1].max }
    end
    { available: true, ms_per_job: total_ms / jobs, statements_per_job: calls.to_f / jobs,
      busy_backends: total_ms / 1000 / wall_s, top: top }
  end

  def statement_ms(row)
    row.fetch("total_exec_time").to_f + row.fetch("total_plan_time", 0).to_f
  end

  # waits: wait kind (wait_event_type:wait_event) => count of active backends of this database seen
  # in that state, summed over `samples` polls of pg_stat_activity ("CPU" when
  # the backend was active with no wait event).
  def wait_shares(waits, samples:)
    total = waits.values.sum
    return { "avg_active" => 0.0, "shares" => {} } if total.zero? || samples.zero?

    shares = waits.sort_by { |_, n| -n }.to_h { |type, n| [type, n.fdiv(total)] }
    { "avg_active" => total.fdiv(samples), "shares" => shares }
  end

  def label(cell) = "#{cell["role"]} / #{cell["location"]} / #{cell["jit"]}"

  def fleet_table(cells)
    lines = [
      "| cell | processes | jobs/s | jobs/s per process | ms/job | PG ms/job | statements/job | " \
      "PG busy backends | start skew | top statement |",
      "|---|---:|---:|---:|---:|---:|---:|---:|---:|---|"
    ]
    lines + cells.map { |c| fleet_row(c) }
  end

  def fleet_row(cell)
    head = "| #{label(cell)} | #{cell["processes"]} |"
    return "#{head} #{cell["skip_reason"]} | | | | | | | |" if cell["skip_reason"]

    per_process = cell["jobs_per_s"] / cell["processes"]
    ms_per_job = cell["wall_s"] * 1000 / cell["jobs"]
    top = cell["pg_top"]&.first
    "#{head} #{cell["jobs_per_s"].round} | #{per_process.round} | #{format("%.3f", ms_per_job)} | " \
      "#{num(cell["pg_ms_per_job"], "%.3f")} | #{num(cell["pg_statements_per_job"], "%.2f")} | " \
      "#{num(cell["pg_busy_backends"], "%.2f")} | #{format("%.1f", cell["start_skew_ms"])} ms | " \
      "#{top ? short_query(top["query"]) : "n/a"} |"
  end

  def statement_lines(cells)
    cells.reject { |c| c["skip_reason"] }.flat_map do |c|
      title = "#{label(c)}, #{c["processes"]} process(es):#{connection_note(c)}#{wait_note(c["pg_waits"])}"
      next ["#{title} pg_stat_statements: n/a (#{c["pg_unavailable_reason"]})"] if c["pg_unavailable_reason"]

      [title] + planning_note(c) + c["pg_top"].map do |s|
        "  #{format("%.3f", s["ms_per_call"])} ms/call × #{s["calls"]}  #{short_query(s["query"])}"
      end
    end
  end

  def planning_note(cell)
    return [] unless cell["pg_planning_tracked"] == false

    ["  planning not counted: pg_stat_statements.track_planning is off, so PG ms/job leaves out " \
     "top-level statements' planning"]
  end

  def connection_note(cell)
    return "" unless cell["peak_connections"]

    " peak #{cell["peak_connections"]} connections (budgeted #{cell["budgeted_connections"]})" \
      "#{"," if cell["pg_waits"] && !cell["pg_waits"]["shares"].empty?}"
  end

  def wait_note(waits)
    return "" if waits.nil? || waits["shares"].empty?

    split = waits["shares"].first(WAIT_KINDS).map { |type, share| "#{type} #{(share * 100).round}%" }.join(", ")
    " #{format("%.1f", waits["avg_active"])} active backends sampled, #{split}"
  end

  def num(value, fmt) = value.nil? ? "n/a" : format(fmt, value)

  def short_query(query)
    flat = query.to_s.gsub(/\s+/, " ").strip
    flat.length > QUERY_WIDTH ? "#{flat[0, QUERY_WIDTH]}…" : flat
  end
end
