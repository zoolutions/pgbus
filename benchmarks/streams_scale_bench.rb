# frozen_string_literal: true

# Streams scale bench (issue #620): M SSE subscribers on ONE web process.
#
#   PGBUS_DATABASE_URL=postgres://user@host/db bundle exec rake bench:streams_scale
#
# Each (M, mode) cell is its own spawned subprocess of this file
# (STREAMS_SCALE_CELL=M:mode), so no cell inherits another's heap growth or JIT
# warm-up. In it one Puma::Server (PumaTestHarness) runs with one streamer
# (streams_listen_scope = :process: no master hub), over real PGMQ and real
# LISTEN/NOTIFY. The M clients run in spawned client processes
# (benchmarks/support/streams_scale_client.rb), never forked and never in the
# server's process: 1 000 SSE-parsing threads under its GVL would make the bench
# time itself instead of the streamer.
#
# Per M and per broadcast mode (ephemeral: the NOTIFY payload is the data;
# durable: read_after from the stream queue) it prints:
#
#   connect all     barrier → registry.size == M (every client registered); with
#                   slow clients, barrier → the last fast client's pgbus:connected
#   connect p99     barrier → a client's pgbus:connected frame, p99 over clients
#   fan-out         broadcast call → the first / the last client's receipt,
#                   p50/p99 over STREAMS_SCALE_BROADCASTS broadcasts, 250 ms apart
#   dispatcher wake handle_durable_wake / handle_ephemeral_wake wall time
#   heartbeat tick  Heartbeat#tick wall time (one forced after the broadcasts)
#   watcher build   PeerCloseWatcher#watched_sockets wall time, p50: the O(M)
#                   part of a watcher pass; its IO.select wait is not counted
#   RSS Δ/conn      the cell's RSS after the connects minus before, per client
#   CPU/wall        the cell's utime + stime over the broadcast window
#   retried connects connects the kernel reset (accept queue full) and the
#                   client process retried after 2 s, as an EventSource would
#
# The server's methods are timed by prepending on the instances this bench
# owns (StreamsScaleHarness.time_calls); nothing under lib/ changes. RSS and CPU
# are the whole cell process's (the server runs in it), so only their deltas
# mean anything. Clock: CLOCK_REALTIME, the one clock every process on the machine
# shares; load average is printed before and after because sibling work on the
# machine moves every number.
#
# Knobs: STREAMS_SCALE_CLIENTS (default 100,1000; 2000 opt-in),
# STREAMS_SCALE_CLIENT_PROCESSES (4), STREAMS_SCALE_BROADCASTS (20),
# STREAMS_SCALE_MODES (ephemeral,durable), STREAMS_SCALE_WRITERS (0:
# streams_writer_threads, the durable fan-out pump), STREAMS_SCALE_SLOW (0: K
# extra clients in one extra process that sleep 1 s after each event; excluded
# from the fan-out and connect columns).
#
# The fd soft limit is raised first thing in every cell and client process
# (to the hard limit, or OPEN_MAX when that is infinite, as on macOS); an M
# whose 2 × M + 64 descriptors do not fit is skipped with the cap and the
# `ulimit -n` / `launchctl limit maxfiles` hint. Run-and-report, never a CI gate.

require "English"
require "etc"
require "json"
require "logger"
require "rbconfig"
require "securerandom"
require "stringio"
require "uri"
require "active_record"
require "pgbus"
require_relative "support/streams_scale_harness"
require_relative "../spec/support/puma_test_harness"

DATABASE_URL = ENV.fetch("PGBUS_DATABASE_URL") do
  abort "PGBUS_DATABASE_URL not set. Example: postgres://user@host/db"
end

KNOBS = StreamsScaleHarness.knobs(ENV)
CLIENT_SCRIPT = File.expand_path("support/streams_scale_client.rb", __dir__)
# A storm of M connects queues in the kernel's accept backlog (somaxconn caps
# Puma's 1 024; 128 on macOS, which resets the overflow: the client process
# reconnects those after 2 s, as an EventSource would, and the table counts
# them). A long connect timeout shows a slow accept as latency, not an error.
CONNECT_TIMEOUT_S = 60
# A hijacked client gets no byte until the dispatcher registers it (no
# heartbeat before that), and connects register one at a time, so the reader
# must outwait the whole storm.
READ_TIMEOUT_S = 1_800
# The registry wait gives up only when no client has registered for this long:
# each connect is serial on the dispatcher, so the total scales with M.
REGISTER_STALL_S = 30
BROADCAST_GAP_S = 0.25
PUMA_THREADS = 8

def realtime = Process.clock_gettime(Process::CLOCK_REALTIME)
def monotonic = Process.clock_gettime(Process::CLOCK_MONOTONIC)

def rss_kb = Integer(IO.popen(["ps", "-o", "rss=", "-p", Process.pid.to_s], &:read).strip, 10)

def somaxconn
  value = IO.popen(%w[sysctl -n kern.ipc.somaxconn], err: File::NULL, &:read).strip
  value = File.read("/proc/sys/net/core/somaxconn").strip if value.empty? && File.readable?("/proc/sys/net/core/somaxconn")
  value.empty? ? "unknown" : value
rescue SystemCallError
  "unknown"
end

def configure!
  url = URI.parse(DATABASE_URL)
  params = URI.decode_www_form(url.query || "").to_h
  params["pool"] = "20"
  url.query = URI.encode_www_form(params)
  ActiveRecord::Base.establish_connection(url.to_s)

  Pgbus.configure do |c|
    c.database_url = DATABASE_URL
    c.queue_prefix = "pgbus_sscale"
    c.default_queue = "default"
    c.logger = Logger.new(IO::NULL)
    c.pgmq_schema_mode = :embedded
    c.listen_notify = true
    c.stats_enabled = false
    c.streams_stats_enabled = false
    c.streams_signed_name_secret = "a" * 64
    c.streams_listen_scope = :process
    c.streams_max_connections = 2 * (KNOBS[:clients].max + KNOBS[:slow])
    c.streams_writer_threads = KNOBS[:writers]
    # streams_heartbeat_interval stays at its default (15 s) so the heartbeat
    # ticks under load; one tick is forced after the broadcasts as well.
  end
end

# The streamer's own log, at :warn, per cell: a select over > 1 024 fds, a
# failed write or an EMFILE goes there, and the cell prints it.
def boot_server(log)
  logger = Logger.new(log, level: :warn)
  streamer = Pgbus::Web::Streamer::Instance.new(client: Pgbus.client, config: Pgbus.configuration, logger: logger)
  streamer.start
  app = Pgbus::Web::StreamApp.new(streamer: streamer, config: Pgbus.configuration, logger: logger)
  harness = SseTestSupport::PumaTestHarness.boot(rack_app: app, max_threads: PUMA_THREADS)
  [streamer, harness]
end

def instrument(streamer)
  sinks = { connect: Queue.new, wake: Queue.new, heartbeat: Queue.new, watcher: Queue.new }
  StreamsScaleHarness.time_calls(streamer.dispatcher, :handle_connect, sinks[:connect])
  StreamsScaleHarness.time_calls(streamer.dispatcher, :handle_durable_wake, sinks[:wake])
  StreamsScaleHarness.time_calls(streamer.dispatcher, :handle_ephemeral_wake, sinks[:wake])
  StreamsScaleHarness.time_calls(streamer.heartbeat, :tick, sinks[:heartbeat])
  StreamsScaleHarness.time_calls(streamer.peer_close_watcher, :watched_sockets, sinks[:watcher]) if
    streamer.peer_close_watcher
  sinks
end

def spawn_client(url, count, slow:)
  out_r, out_w = IO.pipe
  in_r, in_w = IO.pipe
  args = [RbConfig.ruby, CLIENT_SCRIPT, url, count.to_s, CONNECT_TIMEOUT_S.to_s, READ_TIMEOUT_S.to_s, slow ? "1" : "0"]
  pid = Process.spawn({ "RUBY_YJIT_ENABLE" => nil }, *args, in: in_r, out: out_w)
  out_w.close
  in_r.close
  child = { pid: pid, stdin: in_w, stdout: out_r, output: +"", slow: slow }
  await_ready(child)
  # The result line of a 250-client child is ~100 KB, more than a pipe holds:
  # read it from a thread so the child never blocks on a full pipe.
  child[:reader] = Thread.new { out_r.read } # rubocop:disable ThreadSafety/NewThread -- joined in #collect
  child
end

def await_ready(child)
  while (line = child[:stdout].gets)
    child[:output] << line
    return if line.chomp == StreamsScaleHarness::CHILD_READY
  end
  Process.wait(child[:pid])
  abort "streams scale client exited before it was ready:\n#{child[:output]}"
end

def signal(children, word)
  children.each do |child|
    child[:stdin].puts(word)
    child[:stdin].flush
  end
end

def collect(child)
  child[:output] << child[:reader].value
  child[:stdout].close
  child[:stdin].close
  _, status = Process.wait2(child[:pid])
  raise "streams scale client failed (#{status}):\n#{child[:output]}" unless status.success?

  StreamsScaleHarness.parse_child_result(child[:output])
end

def wait_for_registry(streamer, expected)
  size = streamer.registry.size
  moved_at = monotonic
  while size < expected && monotonic - moved_at < REGISTER_STALL_S
    sleep 0.005
    current = streamer.registry.size
    moved_at = monotonic if current != size
    size = current
  end
  size
end

def broadcast_html(tag)
  %(<turbo-stream action="append" target="scale"><template><p data-scale-tag="#{tag}">#{tag}</p></template></turbo-stream>)
end

def cpu_seconds
  t = Process.times
  t.utime + t.stime
end

def run_broadcasts(stream)
  Array.new(KNOBS[:broadcasts]) do |i|
    sleep BROADCAST_GAP_S unless i.zero?
    at = realtime
    stream.broadcast(broadcast_html(i.to_s))
    { "tag" => i.to_s, "at" => at }
  end
end

def client_failures(clients)
  over = clients.count { |c| c["status"] == "503" }
  errors = clients.count { |c| c["status"] != "503" && (c["status"] != "200" || c["connected_at"].nil?) }
  [over, errors]
end

def run_cell(clients, mode)
  cap = StreamsScaleHarness.raise_fd_limit!(StreamsScaleHarness.fd_needed(clients + KNOBS[:slow]))
  reason = StreamsScaleHarness.fd_skip_reason(clients + KNOBS[:slow], cap: cap)
  return { "m" => clients, "skip_reason" => reason } if reason

  stream_name = "ss_#{mode}_#{SecureRandom.hex(4)}"
  Pgbus.client.ensure_stream_queue(stream_name)
  stream = Pgbus.stream(stream_name, durable: mode == :durable)
  log = StringIO.new
  streamer, harness = boot_server(log)
  sinks = instrument(streamer)
  url = "#{harness.url("/#{Pgbus::Streams::SignedName.sign(stream_name)}")}?since=#{stream.current_msg_id}"

  GC.start
  rss_before = rss_kb
  children = StreamsScaleHarness.split_clients(clients, KNOBS[:client_processes]).map do |n|
    spawn_client(url, n, slow: false)
  end
  children << spawn_client(url, KNOBS[:slow], slow: true) if KNOBS[:slow].positive?
  expected = clients + KNOBS[:slow]

  go_at = realtime
  signal(children, "go")
  registered = wait_for_registry(streamer, expected)
  registered_s = realtime - go_at
  rss_connected = rss_kb

  connects = StreamsScaleHarness.drain(sinks[:connect])
  stream.broadcast(broadcast_html("warmup"))
  sleep 0.5
  sinks.each_value { |q| StreamsScaleHarness.drain(q) }
  cpu0 = cpu_seconds
  wall0 = monotonic
  broadcasts = run_broadcasts(stream)
  streamer.heartbeat.tick
  cpu_ratio = (cpu_seconds - cpu0) / (monotonic - wall0)
  rss_after = rss_kb
  sleep [StreamsScaleHarness::MISS_DEADLINE_S - (realtime - broadcasts.last["at"]), 0].max
  # After the miss deadline, so the last broadcast's wake is in.
  timings = sinks.transform_values { |q| StreamsScaleHarness.drain(q) }

  signal(children, "stop")
  results = children.map { |child| collect(child) }
  fast = results.reject { |r| r["slow"] }.flat_map { |r| r["clients"] }
  every = results.flat_map { |r| r["clients"] }
  raise "clients reported #{every.size} of #{expected}" unless every.size == expected

  over, errors = client_failures(every)
  fast_connected = fast.filter_map { |c| c["connected_at"] }
  connect = StreamsScaleHarness.connect_stats(fast_connected, go_at: go_at)
  # Slow clients are left out of the connect columns: with any, "connect all"
  # is the last fast client's pgbus:connected; without, the registry wait.
  connect_all_s = KNOBS[:slow].positive? && fast_connected.any? ? fast_connected.max - go_at : registered_s
  fanout = StreamsScaleHarness.fanout_stats(broadcasts, fast)
  wakes = timings[:wake]

  {
    "m" => clients, "registered" => registered, "connect_all_s" => connect_all_s,
    "connect_p50_ms" => connect[:p50_ms], "connect_p99_ms" => connect[:p99_ms],
    "fanout_first_p50_ms" => fanout[:first_p50_ms], "fanout_last_p50_ms" => fanout[:last_p50_ms],
    "fanout_last_p99_ms" => fanout[:last_p99_ms], "misses" => fanout[:misses],
    "wake_count" => wakes.size,
    "handle_connect_p50_ms" => StreamsScaleHarness.percentile(connects, 50),
    "handle_connect_p99_ms" => StreamsScaleHarness.percentile(connects, 99),
    "wake_p50_ms" => StreamsScaleHarness.percentile(wakes, 50),
    "wake_p99_ms" => StreamsScaleHarness.percentile(wakes, 99),
    "heartbeat_ms" => timings[:heartbeat].max, "heartbeat_ticks" => timings[:heartbeat].size,
    "watcher_ms" => StreamsScaleHarness.percentile(timings[:watcher], 50),
    "rss_before_kb" => rss_before, "rss_connected_kb" => rss_connected, "rss_after_kb" => rss_after,
    "rss_kb_per_conn" => (rss_connected - rss_before).fdiv(expected), "cpu_ratio" => cpu_ratio,
    "errors" => errors, "over_capacity" => over, "retries" => every.sum { |c| c["retries"] },
    # The error that ended each unconnected client's last attempt.
    "error_kinds" => every.select { |c| c["connected_at"].nil? }.filter_map { |c| c["error"]&.sub(/ - .*/, "") }.tally,
    "log" => log.string, "puma_log" => harness.server.log_writer.stderr.string
  }
ensure
  streamer&.shutdown!
  harness&.shutdown
  # A fresh queue per cell; dropping it keeps repeated runs from piling up.
  Pgbus.client.drop_queue(stream_name) if stream_name
end

def print_cell_notes(mode, row)
  return if row["skip_reason"]

  printf "  %s M=%d: registered %d; connect p50 %.1f ms; dispatcher handle_connect p50/p99 %.1f / %.1f ms; " \
         "%d wakes for %d broadcasts; %d heartbeat tick(s); RSS %d → %d → %d KB\n",
         mode, row["m"], row["registered"], row["connect_p50_ms"] || Float::NAN,
         row["handle_connect_p50_ms"] || Float::NAN, row["handle_connect_p99_ms"] || Float::NAN,
         row["wake_count"], KNOBS[:broadcasts], row["heartbeat_ticks"],
         row["rss_before_kb"], row["rss_connected_kb"], row["rss_after_kb"]
  row["error_kinds"].each { |kind, n| puts "  client errors: #{n} × #{kind}" }
  { "streamer log" => row["log"], "puma log" => row["puma_log"] }.each do |name, text|
    next if text.to_s.strip.empty?

    puts "  #{name} (last 20 lines):"
    text.lines.last(20).each { |l| puts "    #{l}" }
  end
end

PUMA_POOL_ERROR = "Error reached top of thread-pool"

def yjit? = defined?(RubyVM::YJIT) && RubyVM::YJIT.enabled?

# Parent side: one cell subprocess. Echoes its notes as they come, counts Puma's
# per-connection thread-pool errors (a write to a socket the kernel reset)
# instead of echoing each, and returns the cell's result row.
def run_cell_process(clients, mode)
  args = [RbConfig.ruby]
  args << "--yjit" if yjit?
  args << __FILE__
  output = +""
  pool_errors = Hash.new(0)
  IO.popen({ "STREAMS_SCALE_CELL" => "#{clients}:#{mode}" }, args, err: %i[child out]) do |io|
    io.each_line do |line|
      output << line
      next if line.start_with?(StreamsScaleHarness::CELL_RESULT)
      next pool_errors[line.split(": ", 2).last.strip] += 1 if line.start_with?(PUMA_POOL_ERROR)

      puts line unless line.start_with?("NOTICE:")
    end
  end
  raise "cell M=#{clients} #{mode} failed (#{$CHILD_STATUS}):\n#{output}" unless $CHILD_STATUS.success?

  pool_errors.each { |error, n| puts "  puma (#{mode} M=#{clients}): #{n} × #{PUMA_POOL_ERROR}: #{error}" }
  StreamsScaleHarness.parse_child_result(output, prefix: StreamsScaleHarness::CELL_RESULT)
end

def print_header
  cap = StreamsScaleHarness.raise_fd_limit!(StreamsScaleHarness.fd_needed(KNOBS[:clients].max + KNOBS[:slow]))
  puts "=" * 78
  puts "Pgbus streams scale bench (issue #620): M SSE clients on one Puma web process"
  puts "  DB: #{DATABASE_URL.sub(%r{//[^@]*@}, "//***@")}"
  puts "  Ruby #{RUBY_VERSION}, #{yjit? ? "YJIT" : "no JIT"}, " \
       "Puma #{Puma::Const::PUMA_VERSION} (in-process Puma::Server, max_threads #{PUMA_THREADS}), " \
       "#{Etc.nprocessors} cores, one subprocess per cell"
  puts "  streams_listen_scope :process (one process, no master hub), streams_writer_threads #{KNOBS[:writers]}, " \
       "heartbeat #{Pgbus.configuration.streams_heartbeat_interval} s, " \
       "peer-close watcher #{Pgbus.configuration.streams_peer_close_interval.inspect} s, " \
       "listen health check #{Pgbus.configuration.streams_listen_health_check_ms} ms"
  puts "  fd soft limit #{cap}, somaxconn #{somaxconn}, #{KNOBS[:client_processes]} client processes, " \
       "#{KNOBS[:broadcasts]} broadcasts #{(BROADCAST_GAP_S * 1000).round} ms apart, slow clients #{KNOBS[:slow]}"
  puts "  load average before: #{StreamsScaleHarness.load_average || "unknown"}"
  puts "=" * 78
end

configure!

if (cell = ENV.fetch("STREAMS_SCALE_CELL", nil))
  clients, mode = cell.split(":")
  row = run_cell(Integer(clients, 10), mode.to_sym)
  print_cell_notes(mode, row)
  # One write, so no Puma stderr line can land between the JSON and its newline.
  $stdout.write("#{StreamsScaleHarness::CELL_RESULT}#{JSON.generate(row.except("log", "puma_log", "error_kinds"))}\n")
  $stdout.flush
  exit
end

print_header
KNOBS[:modes].each do |mode|
  rows = KNOBS[:clients].map { |clients| run_cell_process(clients, mode) }
  puts "\n#{mode} broadcasts:\n\n"
  StreamsScaleHarness.table(rows).each { |line| puts line }
  puts
end

puts "load average after: #{StreamsScaleHarness.load_average || "unknown"}"
