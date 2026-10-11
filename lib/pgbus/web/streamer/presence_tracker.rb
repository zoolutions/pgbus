# frozen_string_literal: true

module Pgbus
  module Web
    module Streamer
      # Connection-driven presence (issues #169, #515), run on the
      # StreamEventDispatcher thread, which owns AR connection release.
      #
      # On a seat table (Pgbus::Streams::Presence.seats_supported?) every SSE
      # connection is its own row, keyed by connection id and carrying the
      # client's seat; two tabs of one member are two rows. On an older
      # table it falls back to one member-keyed row, as before.
      #
      # The heartbeat posts a PresenceTouchMessage each tick; #touch refreshes
      # the live seats, re-joins any whose row was expired while the
      # connection stayed open, and expires silent seats across all streams
      # (rows of crashed processes included) at most every min_ttl / 2.
      #
      # Every failure is logged and swallowed: a presence DB hiccup must not
      # knock a live SSE connection out of the registry.
      class PresenceTracker
        def initialize(config:, presence_provider:, logger:, sweeper: nil, clock: nil)
          @config = config
          @presence_provider = presence_provider
          @logger = logger
          @sweeper = sweeper || method(:expire_seats)
          @clock = clock || -> { ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) }
          @last_sweep_at = nil
        end

        def join(connection, stream = connection.stream_name)
          return unless @config&.stream_presence?(stream)

          member = @config.presence_member_for(connection.context)
          return unless member

          join_member(@presence_provider.call(stream), connection, member)
          connection.presence_member = member[:id] if connection.respond_to?(:presence_member=)
        rescue StandardError => e
          log_failure("join", e)
        end

        def leave(connection, stream = connection.stream_name)
          member_id = presence_member_of(connection)
          return unless member_id

          # Cleared first: a heartbeat touch already queued behind this
          # disconnect must not re-join the seat it removes.
          connection.presence_member = nil if connection.respond_to?(:presence_member=)
          presence = @presence_provider.call(stream)
          if seats?(presence)
            presence.leave(connection_id: connection.id)
          else
            presence.leave(member_id: member_id)
          end
        rescue StandardError => e
          log_failure("leave", e)
        end

        def touch(connections)
          connections.select { |conn| presence_member_of(conn) }.group_by(&:stream_name).each do |stream, conns|
            touch_stream(stream, conns)
          rescue StandardError => e
            log_failure("touch", e)
          end
          sweep_if_due
        end

        private

        def join_member(presence, connection, member)
          metadata = member[:metadata] || {}
          if seats?(presence)
            presence.join(member_id: member[:id], metadata: metadata,
                          connection_id: connection.id, seat: seat_of(connection))
          else
            presence.join(member_id: member[:id], metadata: metadata)
          end
        end

        def touch_stream(stream, conns)
          presence = @presence_provider.call(stream)
          unless seats?(presence)
            conns.each { |conn| presence.touch(member_id: presence_member_of(conn)) }
            return
          end

          live = presence.touch(connection_id: conns.map(&:id))
          conns.reject { |conn| live.include?(conn.id) }.each { |conn| join(conn, stream) }
        end

        def sweep_if_due
          return if Array(@config.streams_presence_patterns).empty?

          at = now
          return if @last_sweep_at && at - @last_sweep_at < @config.presence_min_ttl / 2.0

          @last_sweep_at = at
          @sweeper.call
        rescue StandardError => e
          log_failure("expiry", e)
        end

        def expire_seats
          Pgbus::Streams::Presence.expire_seats!(
            ttl_for: ->(stream) { @config.presence_ttl_for(stream) }, min_ttl: @config.presence_min_ttl
          )
        end

        def seats?(presence)
          presence.respond_to?(:seats?) && presence.seats?
        end

        def seat_of(connection)
          connection.respond_to?(:seat) ? connection.seat : nil
        end

        def presence_member_of(connection)
          connection.respond_to?(:presence_member) ? connection.presence_member : nil
        end

        def now
          @clock.call
        end

        def log_failure(action, error)
          @logger.error { "[Pgbus::Streamer::PresenceTracker] presence #{action} failed: #{error.class}: #{error.message}" }
        end
      end
    end
  end
end
