# frozen_string_literal: true

module Pgbus
  module Web
    module Streamer
      # Off-thread durable-stream fanout writer (issue #321).
      #
      # The dispatcher must never block on a slow client's socket write. When
      # streams_writer_threads > 0, StreamEventDispatcher#handle_durable_wake
      # hands each (connection, filtered envelopes, batch_max) to this pump
      # instead of writing inline. The pump owns N worker threads; each
      # connection is pinned to ONE worker by `id.hash % N`, so a connection's
      # frames stay strictly ordered and its per-io mutex is never contended
      # across workers.
      #
      # The pump calls the UNCHANGED Connection#enqueue on a worker thread (the
      # blocking write, last_msg_id_sent advance, and mark_dead! all move off
      # the dispatcher), then reports back:
      #   - success  → a WriteAckMessage(connection, accepted_max) onto ack_queue.
      #                accepted_max is the highest msg_id actually written (or
      #                the batch_max for a fully-filtered empty batch, so the
      #                dispatcher's scan cursor still advances past the hidden
      #                window). The dispatcher — the SOLE owner of @scanned_cursor
      #                — applies it on its own thread. This keeps the lockless
      #                single-owner invariant intact: the writer only reports a
      #                value, it never mutates dispatcher state.
      #   - failure  → the injected on_dead callback (which posts a
      #                DisconnectMessage), NOT an ack. A failed write must never
      #                advance the cursor past a frame that never reached the
      #                socket (issue #321 B2). The dispatcher then scrubs the
      #                connection's state deterministically (B4), even on an
      #                otherwise-quiet stream.
      #
      # EPHEMERAL frames are NEVER routed here — they have no archive to replay,
      # so an async drop would be unrecoverable (issue #321 B1). #post raises
      # ArgumentError on a negative msg_id as a defense-in-depth guard against a
      # future refactor accidentally offloading one.
      class OutboundPump
        WriteJob = Data.define(:connection, :envelopes, :batch_max, :deadline_ms)
        private_constant :WriteJob

        DRAIN = :__drain__

        def initialize(threads:, ack_queue:, on_dead:, buffer_limit: 0, logger: Pgbus.logger)
          raise ArgumentError, "threads must be positive" unless threads.positive?

          @ack_queue = ack_queue
          @on_dead = on_dead
          @buffer_limit = buffer_limit
          @logger = logger
          # One partition per worker. Each is a Partition wrapping a bounded
          # per-connection buffer so the drop-oldest policy is per connection,
          # not per partition (a fast connection can't be starved by a slow one
          # sharing its worker beyond ordering).
          @partitions = Array.new(threads) { Partition.new(buffer_limit, @logger) }
          @threads = []
          @started = false
        end

        def start
          return self if @started

          @started = true
          @partitions.each do |partition|
            # One writer per partition, owned by this pump: Instance#start
            # starts them once (the @started check above); #stop signals each
            # partition and joins every thread.
            @threads << Thread.new { run_worker(partition) } # rubocop:disable ThreadSafety/NewThread
          end
          self
        end

        # True while any writer thread is still alive. Lets callers assert the
        # pump's OWN threads stopped after #stop without inspecting the global
        # Thread.list (which is noisy and can't distinguish a pump leak from
        # unrelated thread churn).
        def alive?
          @threads.any?(&:alive?)
        end

        # Snapshot of the pump's live writer threads — for test introspection.
        def worker_threads
          @threads.dup
        end

        # Hand a durable fanout write to the pump. Returns immediately — the
        # dispatcher does not block. Raises on a negative (ephemeral) msg_id.
        def post(connection, envelopes, batch_max, deadline_ms:)
          if envelopes.any? { |e| e.msg_id.negative? }
            raise ArgumentError,
                  "OutboundPump received an ephemeral (negative msg_id) envelope; " \
                  "ephemeral fanout must stay inline on the dispatcher thread (issue #321 B1)"
          end

          partition_for(connection).push(
            WriteJob.new(connection: connection, envelopes: envelopes,
                         batch_max: batch_max, deadline_ms: deadline_ms)
          )
        end

        # Graceful drain: signal every partition to flush what it holds, then
        # join each worker bounded by the write deadline. Never Thread#kill — a
        # kill mid write_nonblock corrupts IO state (mirrors
        # StreamEventDispatcher#stop). Idempotent.
        # Snapshot of this component's live thread(s). Instance#shutdown! captures
        # it BEFORE calling #stop so a join that timed out is still observable
        # after #stop has cleared the reference (issue #443).
        def threads
          @threads.dup
        end

        def stop
          return self unless @started

          @started = false
          @partitions.each { |p| p.push(DRAIN) }
          @threads.each do |t|
            next if t.join(join_timeout_seconds)

            @logger.warn { "[Pgbus::Streamer::OutboundPump] writer thread did not drain within #{join_timeout_seconds}s" }
          end
          @threads.clear
          self
        end

        private

        def partition_for(connection)
          @partitions[connection.id.hash % @partitions.size]
        end

        def join_timeout_seconds
          5
        end

        def run_worker(partition)
          loop do
            job = partition.pop
            break if job == DRAIN

            process(job)
          end
        rescue StandardError => e
          @logger.error { "[Pgbus::Streamer::OutboundPump] worker crashed: #{e.class}: #{e.message}" }
          raise
        end

        def process(job)
          conn = job.connection
          return if conn.dead?

          written = conn.enqueue(job.envelopes, deadline_ms: job.deadline_ms)

          if conn.dead?
            @on_dead.call(conn)
          else
            @ack_queue << StreamEventDispatcher::WriteAckMessage.new(
              connection: conn,
              accepted_max: accepted_max_for(job, written)
            )
          end
        rescue StandardError => e
          @logger.error { "[Pgbus::Streamer::OutboundPump] write failed for #{job.connection.id}: #{e.class}: #{e.message}" }
          safe_mark_dead(job.connection)
        end

        # The scan cursor may advance to:
        #   - the highest msg_id actually written, or
        #   - the batch_max when the filtered batch was empty (advance past the
        #     audience-hidden window so read_after moves forward), or
        #   - the connection's current cursor when every frame was a dedup
        #     no-op (a max-guarded no-op on the dispatcher side).
        def accepted_max_for(job, written)
          return job.batch_max if job.envelopes.empty?

          written.map(&:msg_id).max || job.connection.last_msg_id_sent
        end

        def safe_mark_dead(connection)
          connection.mark_dead!
          @on_dead.call(connection)
        rescue StandardError => e
          @logger.debug { "[Pgbus::Streamer::OutboundPump] on_dead failed: #{e.class}: #{e.message}" }
        end

        # A worker's inbox: a control channel (for the DRAIN sentinel, always
        # delivered) plus a bounded per-connection buffer. When buffer_limit is
        # positive and a connection's pending frames exceed it, the OLDEST
        # durable frame for that connection is dropped — safe because durable
        # frames are archive-recoverable on reconnect (issue #321). A drop is
        # logged (throttled) so it isn't silent.
        class Partition
          # Total durable frames this partition has dropped under buffer_limit.
          # Monotonic; read by tests and folded into a throttled operator log.
          attr_reader :dropped

          def initialize(buffer_limit, logger)
            @buffer_limit = buffer_limit
            @logger = logger
            @jobs = []                 # FIFO of WriteJob (and the DRAIN sentinel)
            @pending = Hash.new(0)     # connection.id → buffered frame count
            @mutex = Mutex.new
            @cond = ConditionVariable.new
            @dropped = 0
          end

          def push(job)
            @mutex.synchronize do
              if job == DRAIN
                @jobs << job
              else
                enforce_limit(job)
                @jobs << job
                @pending[job.connection.id] += job.envelopes.size
              end
              @cond.signal
            end
          end

          def pop
            @mutex.synchronize do
              @cond.wait(@mutex) while @jobs.empty?
              job = @jobs.shift
              @pending[job.connection.id] -= job.envelopes.size unless job == DRAIN
              job
            end
          end

          private

          # Caller holds @mutex. Drop the oldest still-buffered frames for this
          # connection until adding `job`'s frames keeps it at/under the cap.
          def enforce_limit(job)
            return unless @buffer_limit.positive?

            cid = job.connection.id
            while @pending[cid] + job.envelopes.size > @buffer_limit
              dropped = drop_oldest_for(cid)
              break unless dropped # nothing left to drop → let it through
            end
          end

          # Caller holds @mutex. Remove the oldest queued WriteJob for cid,
          # decrementing the pending count. Returns the dropped job or nil.
          # A drop is logged (throttled to powers of two so a sustained
          # overflow can't flood the log) so lost frames aren't silent.
          def drop_oldest_for(cid)
            idx = @jobs.index { |j| j != DRAIN && j.connection.id == cid }
            return nil unless idx

            dropped = @jobs.delete_at(idx)
            @pending[cid] -= dropped.envelopes.size
            @dropped += 1
            log_drop(cid) if @dropped.nobits?(@dropped - 1) # power-of-two throttle
            dropped
          end

          def log_drop(cid)
            @logger.warn do
              "[Pgbus::Streamer::OutboundPump] dropped #{@dropped} durable frame(s) " \
                "under buffer_limit=#{@buffer_limit} (connection #{cid}) — the client " \
                "replays them from the archive on reconnect"
            end
          end
        end
        private_constant :Partition
      end
    end
  end
end
