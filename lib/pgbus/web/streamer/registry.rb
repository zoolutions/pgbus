# frozen_string_literal: true

module Pgbus
  module Web
    module Streamer
      # Worker-local in-memory registry of SSE connections indexed by stream
      # name and connection id.
      #
      # Reads are copy-free (issue #621): the per-stream index and the list of
      # every connection are frozen snapshots, replaced whole under the mutex
      # on register / unregister and never mutated in place (the rule
      # docs/concurrency.md sets for memos handed to other threads). A reader
      # loads the current snapshot with one reference read and gets either the
      # old or the new frozen object, never a half-built one, so the per-wake
      # (connections_for), per-tick (each_connection) and per-connect (size)
      # reads take no lock and copy nothing. Membership change pays instead:
      # one Array copy of the stream's connections and of every connection,
      # and one Hash copy of the stream index (microseconds at 2 000
      # connections, on a path that already waits on a LISTEN round trip).
      # The id index stays a mutable Hash under the mutex: lookup is rare.
      class Registry
        EMPTY = [].freeze

        def initialize
          @mutex = Mutex.new
          # stream name → frozen Array of connections, the Hash itself frozen.
          @by_stream = {}.freeze
          # Every registered connection, frozen.
          @all = EMPTY
          @by_id = {}
        end

        def register(connection)
          @mutex.synchronize do
            existing = @by_id[connection.id]
            return if existing.equal?(connection)

            # Registration of a new object under an existing id is a
            # defensive path — SecureRandom.hex(8) collisions are
            # astronomical, but the Registry's invariant is "@by_stream
            # only contains objects that are also in @by_id", so we
            # must scrub the old entry from its stream index before
            # overwriting. Otherwise connections_for(stream) would
            # return orphaned objects and writes would go nowhere.
            remove_from_snapshots(existing) if existing

            @by_id[connection.id] = connection
            add_to_snapshots(connection)
          end
        end

        # Returns true if a matching connection was removed, false if it
        # was not registered. Callers (e.g. StreamEventDispatcher) use the
        # return value to make per-connection bookkeeping idempotent on
        # duplicate DisconnectMessages — prune_dead can enqueue a
        # DisconnectMessage on every wake while the first one is still
        # waiting in the queue.
        def unregister(connection)
          @mutex.synchronize do
            existing = @by_id.delete(connection.id)
            return false unless existing

            remove_from_snapshots(existing)
            true
          end
        end

        def lookup(id)
          @mutex.synchronize { @by_id[id] }
        end

        # The frozen Array of connections on the given stream, the same
        # object until the stream's membership changes.
        def connections_for(stream_name)
          @by_stream[stream_name] || EMPTY
        end

        # Frozen list of stream names with at least one registered connection.
        def streams
          @by_stream.keys.freeze
        end

        def empty?(stream_name)
          !@by_stream.key?(stream_name)
        end

        def size
          @all.size
        end

        # Yields every registered connection across all streams. The iteration
        # walks the current frozen snapshot, so the block may safely call back
        # into register/unregister without risk of deadlock or skipped items.
        def each_connection(&)
          @all.each(&)
        end

        private

        # Must be called while holding @mutex. Builds new frozen snapshots;
        # the ones readers hold are never touched.
        def add_to_snapshots(connection)
          name = connection.stream_name
          current = @by_stream[name] || EMPTY
          @by_stream = @by_stream.merge(name => (current + [connection]).freeze).freeze unless current.any? { |c| c.equal?(connection) }
          @all = (@all + [connection]).freeze unless @all.any? { |c| c.equal?(connection) }
        end

        # Must be called while holding @mutex.
        def remove_from_snapshots(connection)
          name = connection.stream_name
          current = @by_stream[name]
          if current&.any? { |c| c.equal?(connection) }
            remaining = current.reject { |c| c.equal?(connection) }
            @by_stream = if remaining.empty?
                           @by_stream.except(name).freeze
                         else
                           @by_stream.merge(name => remaining.freeze).freeze
                         end
          end
          @all = @all.reject { |c| c.equal?(connection) }.freeze
        end
      end
    end
  end
end
