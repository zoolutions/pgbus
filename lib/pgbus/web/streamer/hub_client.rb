# frozen_string_literal: true

require "socket"

module Pgbus
  module Web
    module Streamer
      # Worker-side client for the MasterHub (issue #382). Presents the same
      # surface the Dispatcher consumes from a Listener — synchronous
      # `ensure_listening` (the no-lost-broadcast ack contract, now crossing
      # the process boundary), async `remove_listening` — while wakes arrive
      # as HubProtocol frames and are re-materialized into the worker's
      # dispatch queue as WakeMessages.
      #
      # Failure model: this class never retries. Connect refusal, an ack
      # deadline, or transport EOF (master died / eviction) marks the client
      # dead, fails every pending sub, and fires +on_failure+ exactly once —
      # the FailoverListener's cue to swap in a per-worker Listener. One-way:
      # once a worker has fallen back it stays local until it recycles
      # (settled on #382 — no flap-back complexity).
      class HubClient
        class HubUnavailableError < StandardError; end

        # Optimistic before the first status broadcast, mirroring WakePipe /
        # NotifyListener: a just-connected worker isn't treated as degraded
        # before the hub has said anything.
        def initialize(socket_path:, dispatch_queue:, ack_timeout: 2.0,
                       on_failure: nil, logger: Pgbus.logger)
          @socket_path = socket_path
          @dispatch_queue = dispatch_queue
          @ack_timeout = ack_timeout
          @on_failure = on_failure
          @logger = logger
          @write_mutex = Mutex.new
          @ack_mutex = Mutex.new
          @pending_acks = Hash.new { |h, k| h[k] = [] }
          @hub_healthy = true
          @dead = false
          @stopping = false
          @sock = nil
          @reader = nil
        end

        def connect
          @sock = UNIXSocket.new(@socket_path)
          # The client's reader thread, owned by this object: #connect starts
          # it once per socket; it ends when the socket closes, and #stop
          # closes the socket and joins it.
          @reader = Thread.new { reader_loop } # rubocop:disable ThreadSafety/NewThread
          self
        rescue SystemCallError, IOError, ArgumentError, ThreadError => e
          # ArgumentError: a socket path over the platform sun_path limit;
          # IOError: a path that exists but is not a socket; ThreadError: the
          # reader thread could not spawn. All must fall back exactly like a
          # refused connect, never abort worker boot — and never leak the
          # half-opened socket.
          close_quietly(@sock)
          @sock = nil
          raise HubUnavailableError, "cannot reach master hub at #{@socket_path}: #{e.class}: #{e.message}"
        end

        def hub_healthy?
          @hub_healthy
        end

        def dead?
          @dead
        end

        # Synchronous, bounded: returns :done once the master has confirmed
        # LISTEN is active for +queue+. Raises HubUnavailableError on a dead
        # transport or an expired ack deadline (which also kills the
        # transport — a hub that can't ack in time can't be trusted with the
        # no-lost-broadcast contract either).
        def ensure_listening(queue)
          raise HubUnavailableError, "master hub transport is dead" if @dead

          waiter = Queue.new
          @ack_mutex.synchronize { @pending_acks[queue] << waiter }
          write_frame({ "t" => "sub", "q" => queue })

          result = waiter.pop(timeout: @ack_timeout)
          if result.nil?
            discard_waiter(queue, waiter)
            mark_dead("sub ack for #{queue} not received within #{@ack_timeout}s")
            raise HubUnavailableError, "master hub ack timeout for #{queue}"
          end
          raise HubUnavailableError, "master hub died while awaiting ack for #{queue}" if result == :dead

          :done
        end

        # Lazy GC, fire-and-forget — no correctness path waits on UNLISTEN
        # (mirrors Listener#remove_listening). A dead transport is a no-op:
        # the master's EOF cleanup already released this worker's refs.
        def remove_listening(queue)
          return if @dead

          write_frame({ "t" => "unsub", "q" => queue })
        rescue HubUnavailableError
          nil
        end

        # Snapshot of this component's live thread(s). Instance#shutdown! captures
        # it BEFORE calling #stop so a join that timed out is still observable
        # after #stop has cleared the reference (issue #443).
        def threads
          [@reader].compact
        end

        def stop
          @stopping = true
          close_quietly(@sock)
          @reader&.join(2)
          @reader = nil
          self
        end

        private

        def reader_loop
          loop do
            frame = HubProtocol.read_frame(@sock)
            break if frame.nil?

            handle_frame(frame)
          end
          mark_dead("master hub closed the transport") unless @stopping
        rescue HubProtocol::ProtocolError => e
          mark_dead("master hub protocol error: #{e.message}") unless @stopping
        rescue IOError, Errno::EBADF, Errno::ECONNRESET
          mark_dead("master hub transport error") unless @stopping
        rescue StandardError => e
          # The reader thread is the ONLY detector of hub death — an
          # unexpected error must not let it exit with the client still
          # reporting healthy, or the worker goes silently deaf.
          mark_dead("master hub reader crashed: #{e.class}: #{e.message}") unless @stopping
        end

        def handle_frame(frame)
          case frame["t"]
          when "wake"
            @dispatch_queue << Listener::WakeMessage.new(queue_name: frame["q"], payload: frame["p"])
          when "ack"
            @ack_mutex.synchronize { @pending_acks[frame["q"]].shift }&.push(:ack)
          when "status"
            @hub_healthy = frame["healthy"]
          else
            @logger.warn { "[Pgbus::Streamer::HubClient] unknown frame from master: #{frame["t"].inspect}" }
          end
        end

        # Frames must never interleave — all writes go through one mutex
        # (writers: dispatcher thread via ensure/remove; no writer thread
        # needed client-side, sub/unsub frames are tiny). Bounded: a master
        # that stopped draining its input would otherwise block this write
        # forever, and the ack deadline only starts ticking AFTER the write
        # returns — so a stalled write is itself a failover trigger.
        def write_frame(message)
          data = HubProtocol.encode(message)
          deadline = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) + @ack_timeout
          @write_mutex.synchronize do
            until data.empty?
              begin
                written = @sock.write_nonblock(data)
                data = data.byteslice(written..)
              rescue IO::WaitWritable
                remaining = deadline - ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
                raise Errno::ETIMEDOUT, "write stalled" if remaining <= 0 || !@sock.wait_writable(remaining)
              end
            end
          end
        rescue IOError, Errno::EPIPE, Errno::EBADF, Errno::ECONNRESET, Errno::ETIMEDOUT => e
          mark_dead("write to master hub failed: #{e.class}")
          raise HubUnavailableError, "master hub transport is dead"
        end

        # Idempotent: first caller flips @dead, fails every waiter, fires
        # on_failure once. Reachable from the reader (EOF/protocol error) and
        # from ack timeouts / failed writes on caller threads.
        def mark_dead(reason)
          waiters = @ack_mutex.synchronize do
            return if @dead

            @dead = true
            drained = @pending_acks.values.flatten
            @pending_acks.clear
            drained
          end
          @hub_healthy = false
          waiters.each { |w| w << :dead }
          close_quietly(@sock)
          @logger.warn { "[Pgbus::Streamer::HubClient] #{reason} — falling back to a per-worker listener" }
          @on_failure&.call
        end

        def discard_waiter(queue, waiter)
          @ack_mutex.synchronize { @pending_acks[queue].delete(waiter) }
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
