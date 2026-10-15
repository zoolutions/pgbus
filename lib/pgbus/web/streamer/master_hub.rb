# frozen_string_literal: true

require "socket"
require "fileutils"

module Pgbus
  module Web
    module Streamer
      # Master-process streams hub (issue #382): ONE LISTEN connection per web
      # host instead of one per Puma worker. Runs in the Puma master (started
      # by the pgbus_streams plugin), owns a single Web::Streamer::Listener on
      # the refcounted union of every worker's stream channels, and fans wakes
      # (including ephemeral payloads) out to workers over a Unix domain
      # socket using HubProtocol frames.
      #
      # Workers are CLIENTS: they connect lazily to +socket_path+ on first SSE
      # use (HubClient). Nothing is inherited across fork, so there is no FD
      # hygiene for this transport, and a server that never starts a hub (no
      # preload_app!, single mode, hub crash) simply has no socket — every
      # worker falls back to its own per-worker Listener (FailoverListener),
      # trading connections for unchanged semantics (settled on #382).
      #
      # The no-lost-wake ack contract, cross-process: a worker's sub is
      # registered in the routing table BEFORE the hub executes LISTEN, and
      # the ack is sent only AFTER ensure_listening returns — so from the
      # moment LISTEN is active every wake reaches the subscribing worker.
      # Over-delivery before the ack is harmless; under-delivery is the only
      # failure mode that matters (same principle as Process::NotifyHub).
      #
      # Backpressure (per-worker outbound queue + writer thread):
      #   - durable wakes (payload nil) are droppable beyond durable_queue_limit
      #     — the next durable wake re-reads from the min cursor, so they
      #     self-heal (mirrors dispatch_queue_limit semantics);
      #   - ephemeral wakes are NEVER dropped: they push past the durable cap,
      #     and a worker whose queue exceeds hard_queue_limit is EVICTED
      #     (socket severed) — which triggers that worker's own fallback
      #     listener. A wedged worker degrades itself, never its siblings.
      #
      # Threading: accept thread + fanout thread + status thread, plus one
      # reader and one writer thread per connected worker. The routing table
      # is guarded by @table_mutex; each worker's outbox by its own mutex.
      # All socket WRITES go through that worker's writer thread (frames must
      # never interleave).
      class MasterHub
        DEFAULT_DURABLE_QUEUE_LIMIT = 256
        DEFAULT_HARD_QUEUE_LIMIT = 1024
        # Status is rebroadcast every REBROADCAST_TICKS status intervals even
        # unchanged, so a worker that connected mid-outage converges.
        REBROADCAST_TICKS = 5

        attr_reader :socket_path

        def initialize(config:, socket_path:, listener_factory: nil, status_interval: 1.0,
                       durable_queue_limit: DEFAULT_DURABLE_QUEUE_LIMIT,
                       hard_queue_limit: DEFAULT_HARD_QUEUE_LIMIT, logger: Pgbus.logger)
          @config = config
          @socket_path = socket_path
          @status_interval = status_interval
          @durable_queue_limit = durable_queue_limit
          @hard_queue_limit = hard_queue_limit
          @logger = logger
          @listener_factory = listener_factory || default_listener_factory
          @dispatch_queue = Queue.new
          # Serializes the FULL start and stop sequences: a stop racing an
          # in-progress start (blocked in the listener factory's PG connect)
          # must WAIT for it and then tear everything down — otherwise stop
          # returns having cleaned nothing and start finishes building a live
          # hub afterwards. Loop threads never take this mutex (they read
          # @running via @table_mutex), so holding it across the blocking
          # startup cannot deadlock them.
          @lifecycle_mutex = Mutex.new
          @table_mutex = Mutex.new
          @workers = {}
          # Plain Hash, entries created ONLY at subscribe time — a default
          # proc here would leak one empty Set per wake that arrives for an
          # already-unsubscribed channel (in-flight NOTIFYs after the last
          # unsub, per-record stream names → unbounded, review on #384).
          @queue_refs = {}
          @stop_signal = Queue.new
          @next_id = 0
          @dropped_durable_wakes = 0
          @evicted_workers = 0
          @running = false
        end

        def dropped_durable_wakes
          @table_mutex.synchronize { @dropped_durable_wakes }
        end

        def evicted_workers
          @table_mutex.synchronize { @evicted_workers }
        end

        # The factory must return a STARTED listener wired to +dispatch_queue+.
        # If any step after the listener exists fails (bad socket path, chmod,
        # thread spawn), the listener — and its dedicated LISTEN connection,
        # the exact resource this hub conserves — is stopped before the error
        # propagates; MasterHubBoot's rescue never sees a leaked connection.
        def start
          @lifecycle_mutex.synchronize { locked_start }
        end

        def stop
          @lifecycle_mutex.synchronize { locked_stop }
        end

        private

        def locked_start
          @table_mutex.synchronize { @running = true }
          @listener = @listener_factory.call(dispatch_queue: @dispatch_queue)
          FileUtils.rm_f(@socket_path)
          # Owner-only: the socket carries every stream wake including
          # ephemeral HTML payloads, and there is no peer authentication —
          # the filesystem mode IS the access control. The umask covers the
          # bind itself so there is no window in which the socket exists with
          # wider permissions; the chmod stays as the second guarantee.
          # File.umask is process-wide, but this runs once at hub start in
          # the Puma master and is restored in the ensure.
          old_umask = File.umask(0o177)
          begin
            @server = UNIXServer.new(@socket_path)
          ensure
            File.umask(old_umask)
          end
          File.chmod(0o600, @socket_path)
          # The hub's three loops, owned by this hub: #start runs once at hub
          # start in the Puma master, under @lifecycle_mutex; #stop signals
          # each loop and joins all three.
          @accept_thread = Thread.new { accept_loop } # rubocop:disable ThreadSafety/NewThread
          @fanout_thread = Thread.new { fanout_loop } # rubocop:disable ThreadSafety/NewThread
          @status_thread = Thread.new { status_loop } # rubocop:disable ThreadSafety/NewThread
          self
        rescue StandardError
          @table_mutex.synchronize { @running = false }
          close_quietly(@server)
          @listener&.stop
          @listener = nil
          raise
        end

        def locked_stop
          @table_mutex.synchronize do
            return self unless @running

            @running = false
          end
          @stop_signal << :stop
          close_quietly(@server)
          @dispatch_queue << :stop
          worker_ids = @table_mutex.synchronize { @workers.keys }
          worker_ids.each { |id| cleanup_worker(id) }
          [@accept_thread, @fanout_thread, @status_thread].each { |t| t&.join(2) }
          @listener&.stop
          FileUtils.rm_f(@socket_path)
          self
        end

        def default_listener_factory
          lambda do |dispatch_queue:|
            build_connection = -> { Pgbus::DedicatedConnection.connect(@config.streams_connection_options) }
            conn = build_connection.call
            Pgbus::Process::PrimaryValidator.validate_primary!(conn)
            Listener.new(
              pg_connection: conn,
              dispatch_queue: dispatch_queue,
              health_check_ms: @config.streams_listen_health_check_ms,
              connection_factory: build_connection,
              dispatch_queue_limit: @config.streams_dispatch_queue_limit,
              logger: @logger
            ).tap(&:start)
          end
        end

        def running?
          @table_mutex.synchronize { @running }
        end

        def accept_loop
          loop do
            begin
              sock = @server.accept
            rescue IOError, Errno::EBADF, Errno::EINVAL
              # server closed during stop
              break
            end
            begin
              register_worker(sock)
            rescue StandardError => e
              # One bad connection must not stop the hub accepting others.
              @logger.warn { "[Pgbus::Streamer::MasterHub] failed to register a worker: #{e.class}: #{e.message}" }
              close_quietly(sock)
            end
          end
        end

        def register_worker(sock)
          entry = {
            sock: sock, subs: Set.new, outbox: [], durable_count: 0, open: true,
            outbox_mutex: Mutex.new, outbox_cond: ConditionVariable.new
          }
          id = @table_mutex.synchronize do
            @next_id += 1
            @workers[@next_id] = entry
            @next_id
          end
          # One writer and one reader per connected Puma worker, owned by its
          # @workers entry: both end when cleanup_worker closes the entry's
          # socket (a worker hangs up, or #stop cleans up every worker).
          entry[:writer] = Thread.new { writer_loop(id, entry) } # rubocop:disable ThreadSafety/NewThread
          entry[:reader] = Thread.new { reader_loop(id, entry) } # rubocop:disable ThreadSafety/NewThread
          id
        end

        def reader_loop(id, entry)
          loop do
            frame = HubProtocol.read_frame(entry[:sock])
            break if frame.nil?

            handle_frame(id, entry, frame)
          end
        rescue HubProtocol::ProtocolError => e
          @logger.warn { "[Pgbus::Streamer::MasterHub] worker #{id} protocol error: #{e.message}" }
        rescue IOError, Errno::EBADF, Errno::ECONNRESET
          # severed by eviction or stop
        rescue StandardError => e
          # e.g. ensure_listening raising inside handle_sub — the ensure still
          # severs this worker (its fallback takes over), but never silently.
          @logger.warn { "[Pgbus::Streamer::MasterHub] reader for worker #{id} failed: #{e.class}: #{e.message}" }
        ensure
          cleanup_worker(id)
        end

        def handle_frame(id, entry, frame)
          case frame["t"]
          when "sub" then handle_sub(id, entry, frame["q"])
          when "unsub" then handle_unsub(id, frame["q"])
          else
            @logger.warn { "[Pgbus::Streamer::MasterHub] worker #{id} sent unknown frame: #{frame["t"].inspect}" }
          end
        end

        # Register FIRST, LISTEN second, ack LAST — the ordering the no-lost-
        # wake contract rests on (see class comment). Runs on this worker's
        # reader thread; ensure_listening blocks bounded by the listener's own
        # ack budget.
        def handle_sub(id, entry, queue)
          @table_mutex.synchronize do
            entry[:subs].add(queue)
            (@queue_refs[queue] ||= Set.new).add(id)
          end
          @listener.ensure_listening(queue)
          enqueue_frame(id, entry, { "t" => "ack", "q" => queue }, droppable: false)
        end

        def handle_unsub(id, queue)
          release_queue_refs(id, [queue])
          @table_mutex.synchronize { @workers[id]&.[](:subs)&.delete(queue) }
        end

        def fanout_loop
          loop do
            message = @dispatch_queue.pop
            break if message == :stop

            begin
              deliver(message)
            rescue StandardError => e
              # One bad message must not stop wake delivery for the host.
              @logger.error { "[Pgbus::Streamer::MasterHub] wake delivery failed: #{e.class}: #{e.message}" }
            end
          end
        end

        def deliver(message)
          frame = { "t" => "wake", "q" => message.queue_name, "p" => message.payload }
          droppable = message.payload.nil?
          targets = @table_mutex.synchronize do
            refs = @queue_refs[message.queue_name]
            refs ? refs.filter_map { |id| [id, @workers[id]] if @workers[id] } : []
          end
          targets.each { |id, entry| enqueue_frame(id, entry, frame, droppable: droppable) }
        end

        # Non-blocking enqueue with the drop/evict policy. Never blocks the
        # fanout thread on one slow worker (the head-of-line lesson from
        # issue #315 item 3, applied cross-process).
        def enqueue_frame(id, entry, frame, droppable:)
          evict = false
          entry[:outbox_mutex].synchronize do
            return unless entry[:open]

            if droppable && entry[:durable_count] >= @durable_queue_limit
              @table_mutex.synchronize { @dropped_durable_wakes += 1 }
              return
            end

            entry[:outbox] << [frame, droppable]
            entry[:durable_count] += 1 if droppable
            evict = entry[:outbox].size > @hard_queue_limit
            entry[:outbox_cond].signal
          end
          evict_worker(id, entry) if evict
        end

        # Sever a worker that stopped draining. Closing the socket unblocks
        # its writer (IOError) and its reader (EOF on the client side makes
        # the worker's HubClient fail over to a local listener) — the wedged
        # worker degrades itself, never its siblings.
        def evict_worker(id, entry)
          already = false
          entry[:outbox_mutex].synchronize do
            already = !entry[:open]
            entry[:open] = false
            entry[:outbox_cond].broadcast
          end
          return if already

          @table_mutex.synchronize { @evicted_workers += 1 }
          @logger.warn do
            "[Pgbus::Streamer::MasterHub] evicting worker #{id}: outbound queue exceeded " \
              "#{@hard_queue_limit} frames (worker not draining) — it falls back to its own listener"
          end
          close_quietly(entry[:sock])
        end

        def writer_loop(id, entry)
          loop do
            frame = nil
            entry[:outbox_mutex].synchronize do
              entry[:outbox_cond].wait(entry[:outbox_mutex]) while entry[:outbox].empty? && entry[:open]
              return unless entry[:open]

              frame, droppable = entry[:outbox].shift
              entry[:durable_count] -= 1 if droppable
            end
            entry[:sock].write(HubProtocol.encode(frame))
          end
        rescue IOError, Errno::EPIPE, Errno::ECONNRESET, Errno::EBADF
          # severed / worker died
        rescue StandardError => e
          # e.g. a ProtocolError from encode — never die silently; sever this
          # worker so its fallback takes over.
          @logger.warn { "[Pgbus::Streamer::MasterHub] writer for worker #{id} failed: #{e.class}: #{e.message}" }
        ensure
          cleanup_worker(id)
        end

        def status_loop
          last_status = nil
          ticks_since_broadcast = 0
          loop do
            # A stop-signal wait instead of sleep, so #stop wakes the thread
            # immediately even with a long status_interval.
            break if @stop_signal.pop(timeout: @status_interval)
            break unless running?

            begin
              healthy = listener_healthy?
              ticks_since_broadcast += 1
              next unless healthy != last_status || ticks_since_broadcast >= REBROADCAST_TICKS

              broadcast_status(healthy)
              last_status = healthy
              ticks_since_broadcast = 0
            rescue StandardError => e
              @logger.warn { "[Pgbus::Streamer::MasterHub] status tick failed: #{e.class}: #{e.message}" }
            end
          end
        end

        def listener_healthy?
          listener = @listener
          !!(listener&.alive? && listener.connected?)
        end

        def broadcast_status(healthy)
          frame = { "t" => "status", "healthy" => healthy }
          entries = @table_mutex.synchronize { @workers.to_a }
          entries.each { |id, entry| enqueue_frame(id, entry, frame, droppable: false) }
        end

        # Idempotent teardown for one worker — reachable from its reader's
        # ensure, an eviction, and stop.
        def cleanup_worker(id)
          entry = @table_mutex.synchronize { @workers.delete(id) }
          return unless entry

          entry[:outbox_mutex].synchronize do
            entry[:open] = false
            entry[:outbox_cond].broadcast
          end
          close_quietly(entry[:sock])
          release_queue_refs(id, entry[:subs].to_a)
        end

        # Decrement refcounts; UNLISTEN queues that hit zero (async — no
        # correctness path waits on unlisten, mirroring remove_listening).
        def release_queue_refs(id, queues)
          released = @table_mutex.synchronize do
            queues.select do |q|
              refs = @queue_refs[q]
              next false unless refs

              refs.delete(id)
              @queue_refs.delete(q) if refs.empty?
            end
          end
          released.each { |q| @listener.remove_listening(q) }
        end

        def close_quietly(io)
          io.close if io && !io.closed?
        rescue IOError, Errno::EBADF
          nil
        end
      end
    end
  end
end
