# frozen_string_literal: true

module Pgbus
  module Web
    module Streamer
      # The worker-side seam between the two listening modes (issue #382):
      # starts on the master hub (HubClient) and fails over — once, one-way —
      # to a per-worker Listener when the hub transport dies (master gone,
      # ack deadline, eviction). The Dispatcher/Instance consume the same
      # ensure_listening/remove_listening/stop surface either way and never
      # learn which mode is active.
      #
      # Fallback direction is settled on #382: per-worker listener, not
      # poll-only — ephemeral broadcasts have no polling equivalent (their
      # payload exists only in the NOTIFY), so an outage trades connections
      # for unchanged semantics. Once fallen back, the worker stays local
      # until it recycles; no flap-back.
      #
      # The subscription set is recorded here so failover can rebuild the
      # exact LISTEN set on the fresh local connection before anything else
      # relies on it. ensure_listening NEVER raises to the dispatcher: on a
      # double failure (hub dead AND local build failing — e.g. DB down) it
      # logs and returns nil, matching the Listener's own ack-timeout
      # contract, which the dispatcher already tolerates, and a background
      # thread retries the local build every `rebuild_interval` seconds until
      # it succeeds or #stop (issue #674: a process signal has no later
      # ensure_listening call to retry it).
      #
      # Process signal channels (issue #674) are recorded and replayed the
      # same way. NOTIFYs sent between the hub's death and the local LISTEN
      # are lost, so once a failover has re-LISTENed a signal channel it calls
      # `on_gap` (the worker's signal delivery runs every on_gap block).
      class FailoverListener
        REBUILD_INTERVAL_SECONDS = 5
        REBUILD_JOIN_SECONDS = 2

        def initialize(hub_client:, local_listener_factory:, on_gap: nil,
                       rebuild_interval: REBUILD_INTERVAL_SECONDS, logger: Pgbus.logger)
          @hub_client = hub_client
          @local_listener_factory = local_listener_factory
          @on_gap = on_gap
          @rebuild_interval = rebuild_interval
          @logger = logger
          # #stop wakes the rebuild thread's wait through this queue.
          @stop_signal = Queue.new
          # @state_mutex guards the cheap shared state (@subscriptions, @impl,
          # @failed_over) and is only ever held for constant-time work — the
          # dispatcher's ensure/remove path must never wait behind a failover
          # build. @failover_mutex serializes the (blocking) build + replay:
          # a fresh PG connect + N re-LISTEN acks can stall for seconds when
          # the trigger IS a database problem (review on #384).
          @state_mutex = Mutex.new
          @failover_mutex = Mutex.new
          @subscriptions = Set.new
          @signal_subscriptions = Set.new
          @impl = hub_client
          @failed_over = false
          # Guarded by @state_mutex.
          @stopped = false
          @rebuild_thread = nil
        end

        # Interface parity with Listener for Instance#start: the hub client
        # connected at construction and the fallback starts itself on swap.
        def start
          self
        end

        def ensure_listening(queue)
          @state_mutex.synchronize { @subscriptions.add(queue) }
          current_impl.ensure_listening(queue)
        rescue HubClient::HubUnavailableError
          fail_over!
          begin
            current_impl.ensure_listening(queue)
          rescue HubClient::HubUnavailableError
            # fail_over! itself failed (factory raised) and @impl is still the
            # dead client — reported there; honor the nil-on-timeout contract.
            nil
          end
        end

        # Same failover contract as ensure_listening, for a signal channel.
        def ensure_signal_listening(channel)
          @state_mutex.synchronize { @signal_subscriptions.add(channel) }
          current_impl.ensure_signal_listening(channel)
        rescue HubClient::HubUnavailableError
          fail_over!
          begin
            current_impl.ensure_signal_listening(channel)
          rescue HubClient::HubUnavailableError
            nil
          end
        end

        def remove_listening(queue)
          @state_mutex.synchronize { @subscriptions.delete(queue) }
          current_impl.remove_listening(queue)
        rescue HubClient::HubUnavailableError => e
          @logger.debug do
            "[Pgbus::Streamer::FailoverListener] remove_listening on a dead hub client " \
              "(#{e.message}) — ignoring, unlisten GC is best-effort"
          end
          nil
        end

        # Idempotent, callable from the client's on_failure (reader thread)
        # and from a synchronous ensure failure (dispatcher thread).
        # @failover_mutex serializes concurrent callers — the second blocks
        # until the first finishes and then no-ops, so a synchronous retry
        # after fail_over! always lands on the swapped-in local listener.
        # The blocking build + replay runs OUTSIDE @state_mutex so concurrent
        # ensure/remove/stop calls never stall behind it.
        def fail_over!
          signals = nil
          @failover_mutex.synchronize do
            return if @state_mutex.synchronize { @failed_over }

            signals = swap_in_local!
          end
          report_gap if signals == true
        rescue StandardError => e
          # Hub dead AND the local listener can't be built (DB down, config
          # broken). Mark failed-over so callers stop rebuilding
          # synchronously; @impl stays on the dead client — every
          # ensure_listening resolves nil and the dispatcher rides its
          # existing timeout tolerance — while the rebuild thread retries.
          @state_mutex.synchronize { @failed_over = true }
          @logger.error do
            "[Pgbus::Streamer::FailoverListener] fallback listener failed to build " \
              "(#{e.class}: #{e.message}) — streams and process signals degraded; " \
              "retrying every #{@rebuild_interval}s"
          end
          start_rebuild
        end

        # Snapshot of this component's live thread(s). Instance#shutdown! captures
        # it BEFORE calling #stop so a join that timed out is still observable
        # after #stop has cleared the reference (issue #443).
        def threads
          rebuild = @state_mutex.synchronize { @rebuild_thread }
          rebuild&.alive? ? [*current_impl.threads, rebuild] : current_impl.threads
        end

        def stop
          rebuild = @state_mutex.synchronize do
            @stopped = true
            @rebuild_thread
          end
          @stop_signal << :stop
          rebuild&.join(REBUILD_JOIN_SECONDS)
          # Never waits on a connect in flight: a rebuild that finishes after
          # this sees @stopped and stops its own listener instead of swapping.
          current_impl.stop
        end

        private

        # Builds a started local listener, replays every subscription on it
        # and swaps it in. Returns true when it re-LISTENed a signal channel
        # (the caller reports the gap), false when it re-LISTENed none, and
        # :stopped when #stop came first (the listener is stopped, not
        # swapped). Raises when the build or the replay fails, after stopping
        # a listener that never swapped in (it would leak its thread and
        # LISTEN connection beside the dead client). Holds no lock across the
        # build or the replay.
        def swap_in_local!
          local = @local_listener_factory.call
          queues, signal_channels = @state_mutex.synchronize { [@subscriptions.dup, @signal_subscriptions.dup] }
          queues.each { |q| local.ensure_listening(q) }
          signal_channels.each { |channel| local.ensure_signal_listening(channel) }
          swapped = @state_mutex.synchronize do
            next false if @stopped

            @impl = local
            @failed_over = true
          end
          unless swapped
            local.stop
            return :stopped
          end
          # Subscriptions recorded during the replay went to the old impl;
          # replay them on the new one.
          late_queues, late_signals = @state_mutex.synchronize do
            [@subscriptions - queues, @signal_subscriptions - signal_channels]
          end
          late_queues.each { |q| local.ensure_listening(q) }
          late_signals.each { |channel| local.ensure_signal_listening(channel) }
          signal_channels.any? || late_signals.any?
        rescue StandardError
          begin
            local&.stop
          rescue StandardError
            nil
          end
          raise
        end

        def start_rebuild
          @state_mutex.synchronize do
            return if @stopped || @rebuild_thread&.alive?

            # The retry thread, owned by this object: it ends once a local
            # listener swaps in or #stop signals it; #stop joins it.
            @rebuild_thread = Thread.new { rebuild_loop } # rubocop:disable ThreadSafety/NewThread
          end
        end

        def rebuild_loop
          loop do
            break if @stop_signal.pop(timeout: @rebuild_interval)

            # No @failover_mutex here: a connect in flight must not hold up
            # #stop, nor fail_over! (which finds @failed_over set and returns).
            result = swap_in_local!
            break if result == :stopped

            report_gap if result
            @logger.info { "[Pgbus::Streamer::FailoverListener] fallback listener built; streams and process signals restored" }
            break
          rescue StandardError => e
            @logger.warn do
              "[Pgbus::Streamer::FailoverListener] fallback listener still failing to build (#{e.class}: #{e.message})"
            end
          end
        end

        # Signals sent while the hub was dying never reached this worker.
        def report_gap
          @on_gap&.call
        rescue StandardError => e
          @logger.error { "[Pgbus::Streamer::FailoverListener] on_gap raised: #{e.class}: #{e.message}" }
        end

        def current_impl
          @state_mutex.synchronize { @impl }
        end
      end
    end
  end
end
