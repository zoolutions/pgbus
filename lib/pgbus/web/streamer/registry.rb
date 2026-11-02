# frozen_string_literal: true

module Pgbus
  module Web
    module Streamer
      # Worker-local in-memory registry of SSE connections indexed by stream
      # name and connection id. Thread-safe via a single mutex.
      #
      # Reads are copy-free (issue #621): connections_for and each_connection
      # hand out a frozen snapshot that is built on the first read after a
      # membership change and cached until the next one, never mutated in
      # place (the rule docs/concurrency.md sets for memos handed to other
      # threads). Every wake and every heartbeat tick on an unchanged stream
      # gets the same object back: one uncontended lock, no copy. The
      # snapshot is not rebuilt on register / unregister themselves, so a
      # connect storm of M clients does not copy the M-element list M times
      # (an eager copy-on-write registry measured +35 MB of RSS and 80 ms for
      # 2 000 registers, against 0.4 MB and 2.5 ms for this one). size reads
      # an Integer kept under the mutex, without the lock.
      class Registry
        EMPTY = [].freeze

        def initialize
          @mutex = Mutex.new
          # stream name → Array of its connections (mutated under @mutex only).
          @by_stream = {}
          @by_id = {}
          # stream name → the frozen snapshot of @by_stream[name]; an entry is
          # dropped when that stream's membership changes.
          @stream_snapshots = {}
          # The frozen snapshot of every connection, nil after a change.
          @all_snapshot = nil
          @size = 0
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
            evict_from_stream(existing) if existing

            # Not already a member: the same object under its id returned
            # above, and a connection's id never changes.
            @by_id[connection.id] = connection
            (@by_stream[connection.stream_name] ||= []) << connection
            membership_changed(connection.stream_name)
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

            evict_from_stream(existing)
            membership_changed(existing.stream_name)
            true
          end
        end

        def lookup(id)
          @mutex.synchronize { @by_id[id] }
        end

        # The frozen Array of connections on the given stream: the same
        # object until the stream's membership changes.
        def connections_for(stream_name)
          @mutex.synchronize do
            # No entry for a stream without members: a wake that lands after
            # the last subscriber left must not leave a slot behind for every
            # per-record stream name (order_42, …) the process ever served.
            members = @by_stream[stream_name]
            next EMPTY unless members

            @stream_snapshots[stream_name] ||= members.dup.freeze
          end
        end

        # Frozen list of stream names with at least one registered connection.
        def streams
          @mutex.synchronize { @by_stream.keys.freeze }
        end

        def empty?(stream_name)
          @mutex.synchronize { !@by_stream.key?(stream_name) }
        end

        # Read once per connect by the StreamApp's capacity check: an Integer
        # written under @mutex and read whole, so a reader sees the count
        # before or after a change, never a torn one.
        attr_reader :size

        # Yields every registered connection across all streams. The iteration
        # walks the current frozen snapshot, so the block may safely call back
        # into register/unregister without risk of deadlock or skipped items.
        def each_connection(&)
          snapshot = @mutex.synchronize { @all_snapshot ||= @by_id.values.freeze }
          snapshot.each(&)
        end

        private

        # Must be called while holding @mutex.
        def membership_changed(stream_name)
          @stream_snapshots.delete(stream_name)
          @all_snapshot = nil
          @size = @by_id.size
        end

        # Must be called while holding @mutex.
        def evict_from_stream(connection)
          name = connection.stream_name
          members = @by_stream[name]
          return unless members

          members.reject! { |c| c.equal?(connection) }
          @by_stream.delete(name) if members.empty?
          @stream_snapshots.delete(name)
        end
      end
    end
  end
end
