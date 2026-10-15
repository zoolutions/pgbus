# frozen_string_literal: true

require "tmpdir"

module Pgbus
  module Web
    module Streamer
      # Deferred MasterHub startup for the Puma master (issue #382). The
      # pgbus_streams plugin's `start` runs BEFORE `preload_app!` loads the
      # Rails app (and with it the pgbus initializer), so the hub cannot be
      # built eagerly. This class splits the two halves:
      #
      #   1. The socket path is exported to ENV IMMEDIATELY — workers inherit
      #      it across fork and connect lazily on first SSE use.
      #   2. A poller thread waits for Pgbus.configuration to become ready
      #      (the initializer has run — with preload_app!, before the first
      #      fork), then builds and starts the MasterHub. Workers that race a
      #      still-booting hub simply fail to connect and fall back to their
      #      own listener until they recycle — degraded footprint, never
      #      degraded semantics.
      #
      # Without preload_app! the master never loads the app, the deadline
      # expires quietly, no socket is ever bound, and every worker keeps
      # today's per-worker listener — :master scope effectively requires
      # preload_app!, documented on the docs site.
      class MasterHubBoot
        def self.default_socket_path
          File.join(Dir.tmpdir, "pgbus-streams-hub-#{::Process.pid}.sock")
        end

        def initialize(socket_path: self.class.default_socket_path, hub_factory: nil,
                       poll_interval: 1.0, deadline: 120, logger: nil)
          @socket_path = socket_path
          @hub_factory = hub_factory || lambda do |socket_path:|
            MasterHub.new(config: Pgbus.configuration, socket_path: socket_path)
          end
          @poll_interval = poll_interval
          @deadline = deadline
          @logger = logger
          # Guards @hub and @running: written by the caller thread
          # (start/stop) and the background poller. A hub whose start
          # outlives stop's join budget is stopped by whichever side sees
          # the flag last, so teardown can never leave a live hub behind.
          @state_mutex = Mutex.new
          @hub = nil
          @running = false
          @thread = nil
        end

        def start
          ENV["PGBUS_STREAMS_HUB_SOCKET"] = @socket_path
          @state_mutex.synchronize { @running = true }
          # The boot thread, owned by this object: the Puma plugin starts it
          # once in the master; #stop clears @running and joins it.
          @thread = Thread.new { wait_and_start } # rubocop:disable ThreadSafety/NewThread
          self
        end

        def stop
          to_stop = @state_mutex.synchronize do
            @running = false
            hub = @hub
            @hub = nil
            hub
          end
          @thread&.join(2)
          @thread = nil
          to_stop&.stop
          self
        end

        private

        def running?
          @state_mutex.synchronize { @running }
        end

        def wait_and_start
          waited = 0.0
          until configuration_ready?
            return unless running?
            return give_up if waited >= @deadline

            sleep @poll_interval
            waited += @poll_interval
          end
          return unless running? && master_scope?

          hub = @hub_factory.call(socket_path: @socket_path)
          hub.start
          # Register-or-late-stop: if stop ran while the hub was building,
          # this thread owns the teardown of the hub stop never saw.
          late = @state_mutex.synchronize do
            if @running
              @hub = hub
              nil
            else
              hub
            end
          end
          late&.stop
          return if late

          log(:info) { "[Pgbus::Streamer::MasterHubBoot] master hub listening at #{@socket_path}" }
        rescue StandardError => e
          @state_mutex.synchronize { @hub = nil }
          # The method-scoped hub may have STARTED before a later step raised
          # (e.g. a failing logger after registration) — stop it here or its
          # LISTEN connection outlives the boot failure. MasterHub#stop is
          # idempotent and safe on a never-started hub.
          begin
            hub&.stop
          rescue StandardError
            nil
          end
          log(:error) do
            "[Pgbus::Streamer::MasterHubBoot] master hub failed to start " \
              "(#{e.class}: #{e.message}) — workers fall back to per-worker listeners"
          end
        end

        # Ready once the app's initializer has produced connection options a
        # dedicated LISTEN connection can be built from (String URL or libpq
        # Hash; the Proc fallback means "nothing configured yet").
        def configuration_ready?
          return false unless defined?(Pgbus) && Pgbus.configuration.streams_enabled

          options = Pgbus.configuration.streams_connection_options
          options.is_a?(String) || options.is_a?(Hash)
        rescue StandardError
          false
        end

        def master_scope?
          Pgbus.configuration.streams_listen_scope == :master
        end

        def give_up
          log(:info) do
            "[Pgbus::Streamer::MasterHubBoot] configuration never became ready within #{@deadline}s " \
              "(no preload_app!?) — no master hub; workers use per-worker listeners"
          end
        end

        def log(level, &)
          (@logger || Pgbus.logger).public_send(level, &)
        end
      end
    end
  end
end
