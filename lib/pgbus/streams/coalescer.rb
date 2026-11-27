# frozen_string_literal: true

require "concurrent"

module Pgbus
  module Streams
    # Publish-side coalescing for high-frequency broadcasts (issue #171).
    #
    # A chatty reactive component — a live cursor, a typing indicator, a
    # progress bar — can fan out many small broadcasts per second. For
    # per-keystroke / per-frame updates that's wasteful: every frame
    # becomes a PGMQ insert (or a NOTIFY) and a fan-out to every connection.
    #
    # The Coalescer batches per (stream, target) within a short window and
    # flushes only the *latest* payload, so superseded frames never hit the
    # bus at all. This is last-write-wins and is only safe for idempotent
    # actions (replace / update of a stable target) — which is exactly the
    # high-frequency case. It is strictly opt-in (`coalesce:` on broadcast).
    #
    # Debounce semantics: the FIRST submit for a (stream, target) schedules
    # a flush `window_ms` later; subsequent submits within that window only
    # overwrite the buffered payload. So latency is bounded to one window
    # and a continuous stream of updates can't starve the flush (it is a
    # trailing-edge-with-max-wait debounce, not a resettable one).
    #
    # Thread-safe: many request threads may submit concurrently. The buffer
    # and the per-key pending-flush set are guarded by a single mutex; the
    # flush itself runs off the mutex on the scheduler's thread.
    class Coalescer
      # Default coalescing window when `coalesce: true` is passed without an
      # explicit millisecond value.
      DEFAULT_WINDOW_MS = 50

      Entry = Struct.new(:payload, :opts)

      # scheduler: responds to `schedule(delay_seconds) { ... }`. Defaults
      #   to `config.streams_coalesce_scheduler`, read when a window's flush
      #   is scheduled, else a Concurrent::ScheduledTask-backed timer.
      # flush: ->(stream_name:, target:, payload:, opts:) called once per
      #   window per key with the latest buffered frame.
      def initialize(flush:, scheduler: nil)
        @flush = flush
        @scheduler = scheduler || ConfiguredScheduler.new
        @mutex = Mutex.new
        @buffer = {}  # key => Entry (latest)
        @pending = {} # key => true while a flush is scheduled
      end

      # Buffers a frame for (stream_name, target). Overwrites any frame
      # already buffered for the same key within the current window. The
      # first submit per window schedules the flush.
      def submit(stream_name:, target:, payload:, opts:, window_ms: DEFAULT_WINDOW_MS)
        key = [stream_name, target]
        schedule = false

        entry = Entry.new(payload, opts)
        @mutex.synchronize do
          @buffer[key] = entry
          unless @pending[key]
            @pending[key] = true
            schedule = true
          end
        end

        return unless schedule

        begin
          @scheduler.schedule(window_ms / 1000.0) { flush_key(key, stream_name, target) }
        rescue StandardError => e
          unschedule(key, entry, e, stream_name, target)
          raise
        end
      end

      private

      # Nothing will flush this window: free the key, or no later submit would
      # schedule it again. A frame another submit buffered meanwhile found the
      # key pending and returned normally; this caller gets the error, that
      # one would lose its frame without a trace, so it is reported.
      def unschedule(key, entry, error, stream_name, target)
        dropped = @mutex.synchronize do
          @pending.delete(key)
          waiting = @buffer.delete(key)
          waiting && !waiting.equal?(entry)
        end
        return unless dropped

        ErrorReporter.report(error, { component: "streams.coalescer", stream: stream_name, target: target, dropped: true })
      end

      def flush_key(key, stream_name, target)
        entry = @mutex.synchronize do
          @pending.delete(key)
          @buffer.delete(key)
        end
        return unless entry

        @flush.call(stream_name: stream_name, target: target, payload: entry.payload, opts: entry.opts)
      rescue StandardError => e
        # The flush runs on the scheduler's thread — a raise here reaches no
        # caller, so a swallowed error is invisible to APM by construction
        # (issue #391: an oversized ephemeral frame died here without a
        # trace). Route through ErrorReporter so configured reporters see it.
        ErrorReporter.report(e, { component: "streams.coalescer", stream: stream_name, target: target })
      end

      # The timer: a Concurrent::ScheduledTask on the global IO executor.
      # Kept as a tiny adapter so the Coalescer can be unit-tested with a
      # synchronous fake scheduler (no real timers, no sleeps).
      class ScheduledTaskScheduler
        def schedule(delay_seconds, &)
          Concurrent::ScheduledTask.execute(delay_seconds, &)
        end
      end

      # Runs the flush at once, on the thread that submitted it (issue #694).
      # For tests: the flush then writes on that thread's own connection, with
      # no IO thread sharing a pinned connection across the test boundary.
      # Pgbus::Testing.fake! / inline! select it. It removes the coalescing
      # itself: every submit is broadcast.
      class InlineScheduler
        def schedule(_delay_seconds) = yield
      end

      # The default: `config.streams_coalesce_scheduler`, read when a window's
      # flush is scheduled (its first submit) so a scheduler set after boot (an
      # initializer, a test mode) takes effect on the process-wide coalescer,
      # else the timer.
      class ConfiguredScheduler
        def initialize
          @timer = ScheduledTaskScheduler.new
        end

        def schedule(delay_seconds, &)
          (Pgbus.configuration.streams_coalesce_scheduler || @timer).schedule(delay_seconds, &)
        end
      end
    end
  end
end
