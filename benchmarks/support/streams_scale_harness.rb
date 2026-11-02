# frozen_string_literal: true

require "json"

# Streams scale bench (issue #620): the pure pieces.
#
# The bench connects M SSE clients, spread over spawned client processes, to
# one Puma web process and times the streamer at that size. This module holds
# what carries no database, Puma or subprocess dependency (the knobs, the child
# result line, the percentile and fan-out arithmetic, the fd-limit clamp, the
# wall-time prepend and the table), so spec/benchmarks/streams_scale_bench_spec.rb
# pins it without a run. The client script (streams_scale_client.rb) requires
# this file too: it must never load pgbus or ActiveRecord.
module StreamsScaleHarness
  CHILD_READY = "STREAMS_SCALE_CHILD_READY"
  CHILD_RESULT = "STREAMS_SCALE_CHILD_RESULT "
  # One (M, mode) cell runs in its own subprocess of the bench and prints this.
  CELL_RESULT = "STREAMS_SCALE_CELL_RESULT "
  MODES = %i[ephemeral durable].freeze
  DEFAULTS = {
    "STREAMS_SCALE_CLIENTS" => "100,1000", "STREAMS_SCALE_CLIENT_PROCESSES" => "4",
    "STREAMS_SCALE_BROADCASTS" => "20", "STREAMS_SCALE_MODES" => "ephemeral,durable",
    "STREAMS_SCALE_WRITERS" => "0", "STREAMS_SCALE_SLOW" => "0", "STREAMS_SCALE_PROFILE" => "",
    "STREAMS_SCALE_STREAMS" => "1"
  }.freeze
  # The frames the #617 Ractor gate asks about, plus the rest of a wake's
  # per-connection work (issue #621): each is the share of the profiled
  # thread's running samples whose stack holds a matching frame.
  PROFILE_MARKERS = {
    "io_writer.rb" => ->(f) { f.filename.to_s.end_with?("streamer/io_writer.rb") },
    "write_nonblock" => ->(f) { f.label.to_s.include?("write_nonblock") },
    "framing (envelope.rb)" => ->(f) { f.filename.to_s.end_with?("streams/envelope.rb") },
    "audience filter" => ->(f) { f.label.to_s.include?("visible_envelopes_for") },
    "JSON.parse" => ->(f) { f.label.to_s.include?("JSON") && f.label.to_s.include?("parse") },
    "read_after" => ->(f) { f.label.to_s.include?("read_after") },
    "ActiveRecord" => ->(f) { f.filename.to_s.include?("/activerecord-") }
  }.freeze
  PROFILE_CATEGORIES = { 1 => :idle, 2 => :stalled }.freeze
  # Descriptors per client: the server's socket and, counted conservatively,
  # the client's (the clients live in other processes today).
  FD_PER_CLIENT = 2
  # The listener, the Postgres connections, the child pipes, Ruby's own.
  FD_HEADROOM = 64
  # macOS: getrlimit reports an infinite hard limit and setrlimit to infinity
  # raises Errno::EINVAL; OPEN_MAX (sys/syslimits.h) is what it accepts.
  OPEN_MAX = 10_240
  DARWIN = RUBY_PLATFORM.include?("darwin")
  # A broadcast a client has not received this long after it was sent is a miss.
  MISS_DEADLINE_S = 5.0
  NA = "n/a"
  COLUMNS = [
    "M", "streams", "connect all (s)", "connect p99 (ms)", "fan-out first p50 (ms)", "fan-out last p50/p99 (ms)",
    "dispatcher wake p50/p99 (ms)", "heartbeat tick (ms)", "watcher build (ms)", "RSS Δ/conn (KB)",
    "CPU/wall", "misses / errors / 503s", "retried connects"
  ].freeze

  module_function

  def knobs(env)
    value = ->(name) { env.fetch(name, DEFAULTS.fetch(name)) }
    {
      clients: parse_counts(value.call("STREAMS_SCALE_CLIENTS"), name: "STREAMS_SCALE_CLIENTS"),
      client_processes: parse_integer(value.call("STREAMS_SCALE_CLIENT_PROCESSES"),
                                      name: "STREAMS_SCALE_CLIENT_PROCESSES", min: 1),
      broadcasts: parse_integer(value.call("STREAMS_SCALE_BROADCASTS"), name: "STREAMS_SCALE_BROADCASTS", min: 1),
      modes: parse_modes(value.call("STREAMS_SCALE_MODES")),
      writers: parse_integer(value.call("STREAMS_SCALE_WRITERS"), name: "STREAMS_SCALE_WRITERS", min: 0),
      slow: parse_integer(value.call("STREAMS_SCALE_SLOW"), name: "STREAMS_SCALE_SLOW", min: 0),
      profile: value.call("STREAMS_SCALE_PROFILE").to_s.strip.then { |dir| dir.empty? ? nil : dir },
      streams: parse_integer(value.call("STREAMS_SCALE_STREAMS"), name: "STREAMS_SCALE_STREAMS", min: 1)
    }
  end

  def parse_counts(value, name:)
    counts = value.to_s.split(",", -1).map(&:strip)
    raise ArgumentError, "#{name} must list positive integers, like 100,1000 (got #{value.inspect})" if
      counts.empty? || counts.any? { |c| !c.match?(/\A[1-9]\d*\z/) }

    counts.map { |c| Integer(c, 10) }
  end

  def parse_integer(value, name:, min:)
    text = value.to_s.strip
    raise ArgumentError, "#{name} must be an integer >= #{min} (got #{value.inspect})" unless
      text.match?(/\A\d+\z/) && Integer(text, 10) >= min

    Integer(text, 10)
  end

  def parse_modes(value)
    modes = value.to_s.split(",", -1).map(&:strip)
    valid = modes.any? && modes.uniq.size == modes.size && modes.all? { |m| MODES.map(&:to_s).include?(m) }
    raise ArgumentError, "STREAMS_SCALE_MODES must list #{MODES.join(" and/or ")} once each (got #{value.inspect})" unless
      valid

    modes.map(&:to_sym)
  end

  # M clients over `processes` children, the remainder on the first ones; a
  # child with no clients is never spawned.
  def split_clients(clients, processes)
    base, extra = clients.divmod(processes)
    Array.new(processes) { |i| base + (i < extra ? 1 : 0) }.select(&:positive?)
  end

  # Each child's first global client index, so its slots continue the
  # round-robin where the previous child's ended (issue #630).
  def client_offsets(counts)
    counts.each_index.map { |i| counts.first(i).sum }
  end

  # The stream a child's client at `slot` joins: clients are dealt over the
  # streams round-robin by global index (issue #630).
  def stream_index(slot, offset:, streams:) = (offset + slot) % streams

  # Nearest-rank percentile; nil for no samples.
  def percentile(values, pct)
    return nil if values.empty?

    sorted = values.sort
    sorted[[(pct / 100.0 * sorted.size).ceil - 1, 0].max]
  end

  def parse_child_result(output, prefix: CHILD_RESULT)
    line = output.lines.reverse.find { |l| l.start_with?(prefix) }
    raise "no #{prefix.strip} line in the subprocess's output:\n#{output}" unless line

    JSON.parse(line.delete_prefix(prefix))
  end

  def fd_needed(clients) = (FD_PER_CLIENT * clients) + FD_HEADROOM

  # The soft limit to set, or nil when the current one already fits `needed`
  # or the cap would not raise it (a soft limit is never lowered). An infinite
  # hard limit caps at OPEN_MAX on macOS only; elsewhere it allows `needed`.
  def fd_target(soft:, hard:, needed:, infinity: Process::RLIM_INFINITY, darwin: DARWIN)
    return nil if soft >= needed

    target = if hard != infinity then hard
             elsif darwin then OPEN_MAX
             else needed
             end
    target > soft ? target : nil
  end

  # Raises this process's soft fd limit as far as fd_target allows and
  # returns the soft limit in force afterwards. A kernel that refuses the raise
  # (macOS with kern.maxfilesperproc under OPEN_MAX) leaves the current limit,
  # so the caller's skip message names it.
  def raise_fd_limit!(needed)
    soft, hard = Process.getrlimit(:NOFILE)
    target = fd_target(soft: soft, hard: hard, needed: needed)
    return soft unless target

    Process.setrlimit(:NOFILE, target, hard)
    Process.getrlimit(:NOFILE).first
  rescue Errno::EINVAL, Errno::EPERM
    soft
  end

  def fd_skip_reason(clients, cap:)
    needed = fd_needed(clients)
    return if cap >= needed

    "skipped: M=#{clients} needs #{FD_PER_CLIENT} × #{clients} + #{FD_HEADROOM} = #{needed} file descriptors, " \
      "the limit is #{cap}; raise it with `ulimit -n #{[needed, OPEN_MAX].max}` " \
      "(macOS: `sudo launchctl limit maxfiles #{OPEN_MAX} unlimited`, check with `launchctl limit maxfiles`)"
  end

  # connected_ats: each client's CLOCK_REALTIME stamp of its pgbus:connected
  # frame; go_at: the parent's stamp just before it released the barrier.
  def connect_stats(connected_ats, go_at:)
    ms = connected_ats.map { |t| (t - go_at) * 1000 }
    { p50_ms: percentile(ms, 50), p99_ms: percentile(ms, 99) }
  end

  # broadcasts: [{"tag", "at"}], `at` the parent's CLOCK_REALTIME stamp right
  # before the broadcast call. clients: [{"events" => {tag => received_at}}].
  # Per broadcast, the time to its first and its last client among those that
  # received it within the deadline; a (client, broadcast) pair that did not
  # is a miss.
  def fanout_stats(broadcasts, clients, deadline_s: MISS_DEADLINE_S)
    firsts = []
    lasts = []
    misses = 0
    broadcasts.each do |broadcast|
      latencies = clients.filter_map do |client|
        received = client.fetch("events")[broadcast.fetch("tag")]
        latency = received && (received - broadcast.fetch("at"))
        next latency if latency && latency <= deadline_s

        misses += 1
        nil
      end
      next if latencies.empty?

      firsts << (latencies.min * 1000)
      lasts << (latencies.max * 1000)
    end
    { first_p50_ms: percentile(firsts, 50), last_p50_ms: percentile(lasts, 50),
      last_p99_ms: percentile(lasts, 99), misses: misses }
  end

  # The one-minute load from `sysctl -n vm.loadavg` ("{ 6.60 5.95 5.80 }") or
  # /proc/loadavg ("0.52 0.58 0.59 1/482 12345").
  def parse_loadavg(text)
    match = text.to_s.match(/(\d+(?:\.\d+)?)/)
    match && Float(match[1])
  end

  def load_average
    text = if File.readable?("/proc/loadavg")
             File.read("/proc/loadavg")
           else
             IO.popen(%w[sysctl -n vm.loadavg], err: File::NULL, &:read)
           end
    parse_loadavg(text)
  rescue SystemCallError
    nil
  end

  # Wraps `method` on this one object (its singleton class, so other instances
  # and the library's code stay untouched) and pushes each call's wall time in
  # ms onto `sink` (a Queue: the timed threads and the bench thread share it).
  # A private method stays private.
  def time_calls(object, method, sink)
    was_private = object.private_methods.include?(method)
    timer = Module.new do
      define_method(method) do |*args, **kwargs, &block|
        started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        begin
          super(*args, **kwargs, &block)
        ensure
          sink << ((Process.clock_gettime(Process::CLOCK_MONOTONIC) - started) * 1000)
        end
      end
      private method if was_private
    end
    object.singleton_class.prepend(timer)
  end

  def drain(queue)
    values = []
    values << queue.pop until queue.empty?
    values
  end

  def table(rows)
    ["| #{COLUMNS.join(" | ")} |", "|#{"---:|" * COLUMNS.size}"] + rows.map { |row| table_row(row) }
  end

  def table_row(row)
    return "| #{row["m"]} | #{row["skip_reason"]} |#{" |" * (COLUMNS.size - 2)}" if row["skip_reason"]

    failures = "#{row["misses"]} / #{row["errors"]} / #{row["over_capacity"]}"
    failures = "**#{failures}**" if row["misses"].to_i.positive? || row["errors"].to_i.positive?
    cells = [
      row["m"], row.fetch("streams", 1), num(row["connect_all_s"], "%.2f"), num(row["connect_p99_ms"], "%.1f"),
      num(row["fanout_first_p50_ms"], "%.1f"),
      "#{num(row["fanout_last_p50_ms"], "%.1f")} / #{num(row["fanout_last_p99_ms"], "%.1f")}",
      "#{num(row["wake_p50_ms"], "%.1f")} / #{num(row["wake_p99_ms"], "%.1f")}",
      num(row["heartbeat_ms"], "%.1f"), num(row["watcher_ms"], "%.2f"), num(row["rss_kb_per_conn"], "%.1f"),
      row["cpu_ratio"].nil? ? NA : "#{(row["cpu_ratio"] * 100).round} %", failures, row["retries"]
    ]
    "| #{cells.join(" | ")} |"
  end

  def num(value, fmt) = value.nil? ? NA : format(fmt, value)

  # Yields [frames (leaf first), category, weight] for every sample a
  # Vernier::Result took on the thread named `thread_name`.
  def each_profile_sample(result, thread_name)
    result.threads.each_value do |thread|
      next unless thread[:name].to_s == thread_name

      categories = thread[:sample_categories] || []
      thread[:samples].each_with_index do |stack_idx, i|
        yield result.stack(stack_idx).frames, PROFILE_CATEGORIES.fetch(categories[i], :running), thread[:weights][i]
      end
    end
  end

  # samples: [frames (leaf first, each with #filename and #label), category
  # (:running, :idle: blocked in a wait, :stalled: waiting for the GVL),
  # weight]. Running samples only feed the shares: the top self frames (the
  # leaf, "file.rb:label") and PROFILE_MARKERS' inclusive shares.
  def profile_summary(samples, top: 8)
    totals = Hash.new(0.0)
    self_weights = Hash.new(0.0)
    marker_weights = PROFILE_MARKERS.keys.to_h { |name| [name, 0.0] }
    samples.each do |frames, category, weight|
      totals[category] += weight
      next unless category == :running

      leaf = frames.first
      self_weights["#{File.basename(leaf.filename.to_s)}:#{leaf.label}"] += weight if leaf
      PROFILE_MARKERS.each { |name, match| marker_weights[name] += weight if frames.any?(&match) }
    end
    running = totals[:running]
    share = ->(w) { running.zero? ? 0.0 : w / running }
    {
      "running" => running, "idle" => totals[:idle], "stalled" => totals[:stalled],
      "top_self" => self_weights.sort_by { |label, w| [-w, label] }.first(top).map { |label, w| [label, share.call(w)] },
      "inclusive" => marker_weights.transform_values(&share)
    }
  end

  def profile_lines(summary)
    pct = ->(v) { format("%.1f %%", v * 100) }
    stalled = summary["running"] + summary["stalled"]
    [
      "running #{summary["running"].round} samples, stalled (GVL) " \
      "#{pct.call(stalled.zero? ? 0.0 : summary["stalled"] / stalled)} of running + stalled",
      "inclusive: #{summary["inclusive"].map { |name, v| "#{name} #{pct.call(v)}" }.join(", ")}",
      "top self: #{summary["top_self"].map { |label, v| "#{label} #{pct.call(v)}" }.join(", ")}"
    ]
  end
end
