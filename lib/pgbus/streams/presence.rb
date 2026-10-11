# frozen_string_literal: true

module Pgbus
  module Streams
    # Presence tracking for SSE streams. A "presence member" is an
    # arbitrary identifier (typically a user id) marked as currently
    # subscribed to a given stream. Members live in pgbus_presence_members
    # with a (stream_name, member_id) primary key and a `last_seen_at`
    # timestamp; the table is the source of truth across all Puma workers
    # and Falcon reactors.
    #
    # Typical usage in a controller:
    #
    #   def show
    #     @room = Room.find(params[:id])
    #     Pgbus.stream(@room).presence.join(
    #       member_id: current_user.id.to_s,
    #       metadata: { name: current_user.name, avatar: current_user.avatar_url }
    #     )
    #   end
    #
    # And when the user leaves:
    #
    #   Pgbus.stream(@room).presence.leave(member_id: current_user.id.to_s)
    #
    # Reading the current member list:
    #
    #   Pgbus.stream(@room).presence.members
    #     # => [{ "id" => "7", "metadata" => {"name" => ..., "avatar" => ...},
    #     #      "joined_at" => "...", "last_seen_at" => "..." }]
    #
    # The join/leave operations also fire a stream broadcast (via the
    # caller-provided block) so connected clients see the change in real
    # time. The library emits the broadcast through the regular pgbus
    # stream pipeline, which means it's transactional, replayable, and
    # filterable just like any other broadcast.
    #
    # Seats (issue #515). A table migrated with `pgbus:add_presence_seats`
    # also keeps one row per SSE connection: `connection_id` (part of the
    # key), `seat` (the client's per-tab id, sent as `?seat=`) and a
    # `state` jsonb for ephemeral per-tab state. Connection-driven presence
    # (`streams_presence_patterns`) writes those rows, so two tabs of one
    # user are two rows and the user stays present until both are gone.
    # Member-keyed calls (no `connection_id:`) keep working on their own
    # row (connection_id IS NULL), exactly as before.
    #
    #   presence.join(member_id: "7", connection_id: conn_id, seat: seat)
    #   presence.set_state(connection_id: conn_id, state: { selected: 3 })
    #   presence.leave(connection_id: conn_id)
    #
    # Seat rows are expired by the streamer heartbeat after their stream's
    # ttl (`Pgbus.configuration.presence_ttl_for`) and every join, leave
    # and expiry is reported through `streams_presence_on_change`.
    #
    # Capability probe for a library building on seats:
    #   Pgbus::Streams::Presence.instance_method(:join).parameters
    #     .include?([:key, :connection_id])   # the gem has seats
    #   Pgbus::Streams::Presence.seats_supported?  # the table is migrated
    #
    # NOT included:
    #   - Built-in DOM events on the <pgbus-stream-source> element
    #     (the application's broadcast block decides the HTML).
    #   - Expiry of member-keyed rows (call #sweep! from a cron for those).
    class Presence
      RECORD_COLUMNS = "member_id, metadata, joined_at, last_seen_at"
      SEAT_RECORD_COLUMNS = "member_id, metadata, joined_at, last_seen_at, connection_id, seat, state"

      # How long a "no seats" answer is trusted before the schema is probed
      # again, so a table migrated under a running process is picked up.
      SCHEMA_RECHECK_SECONDS = 30

      @schema_mutex = Mutex.new
      @seats_supported = nil
      @probed_at = nil

      class << self
        # True when pgbus_presence_members has the seat columns (the
        # add_presence_seats migration ran). A true answer is kept for the
        # life of the process; a false one for SCHEMA_RECHECK_SECONDS.
        def seats_supported?
          return @seats_supported if schema_answer_fresh?

          @schema_mutex.synchronize do
            next @seats_supported if schema_answer_fresh?

            rows = connection.exec_params(<<~SQL, [])
              SELECT 1 AS present FROM information_schema.columns
              WHERE table_name = 'pgbus_presence_members' AND column_name = 'connection_id'
                AND table_schema = ANY (current_schemas(false))
            SQL
            @probed_at = monotonic_now
            @seats_supported = !rows.to_a.empty?
          end
        end

        # seats_supported?, but a cached "no seats" is probed again first.
        # For the destructive member-keyed statements: on a table migrated
        # under this process their SQL without `connection_id IS NULL`
        # succeeds silently and deletes seat rows.
        def seats_supported_now?
          return true if seats_supported?

          reset_schema_cache!
          seats_supported?
        end

        def reset_schema_cache!
          @schema_mutex.synchronize do
            @probed_at = nil
            @seats_supported = nil
          end
        end

        # Deletes every seat row (connection_id IS NOT NULL) that has not
        # been touched within its stream's ttl and returns the deleted
        # records, each with its "stream_name". Member-keyed rows are left
        # alone. `ttl_for` maps a stream name to seconds; `min_ttl` is the
        # smallest ttl configured, used to narrow the candidates in SQL.
        # The DELETE re-checks the cutoff (a touch between the two
        # statements keeps the row) and claims each row atomically, so
        # concurrent sweepers in other processes never report a row twice.
        def expire_seats!(ttl_for:, min_ttl:)
          return [] unless seats_supported?

          candidates = connection.exec_params(<<~SQL, [min_ttl])
            SELECT stream_name, connection_id FROM pgbus_presence_members
            WHERE connection_id IS NOT NULL AND last_seen_at < NOW() - make_interval(secs => $1)
          SQL
          candidates.to_a.group_by { |row| row["stream_name"] }.flat_map do |stream_name, rows|
            expire_stream_seats(stream_name, rows.map { |row| row["connection_id"] }, ttl_for.call(stream_name))
          end
        end

        def connection
          # Respect multi-database setups (connects_to). When
          # Pgbus.configuration.connects_to is set, the pgbus tables
          # live in a separate database accessed via Pgbus::BusRecord;
          # the default path uses ActiveRecord::Base. Matches the
          # canonical pattern in Pgbus::Process::QueueLock#connection
          # and Pgbus::Configuration's connection probe.
          unless defined?(::ActiveRecord::Base)
            raise Pgbus::ConfigurationError,
                  "Pgbus::Streams::Presence requires ActiveRecord (no AR connection available)"
          end

          if Pgbus.configuration.connects_to
            Pgbus::BusRecord.connection.raw_connection
          else
            ::ActiveRecord::Base.connection.raw_connection
          end
        end

        # Reports a seat join/leave to streams_presence_on_change (issue #515).
        def report_change(stream_name, type, record)
          Pgbus.configuration.notify_presence_change(
            stream_name,
            { type: type, connection_id: record["connection_id"], seat: record["seat"], member_id: record["id"] }
          )
        end

        # A Ruby Array of Strings as a Postgres text[] literal.
        def text_array(values)
          "{#{values.map { |v| %("#{v.to_s.gsub(/["\\]/) { |c| "\\#{c}" }}") }.join(",")}}"
        end

        def monotonic_now
          ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
        end

        private

        # Reads each ivar once: a concurrent reset_schema_cache! may nil them.
        def schema_answer_fresh?
          supported = @seats_supported
          probed_at = @probed_at
          return false if supported.nil? || probed_at.nil?

          supported || monotonic_now - probed_at < SCHEMA_RECHECK_SECONDS
        end

        def expire_stream_seats(stream_name, connection_ids, ttl)
          rows = connection.exec_params(<<~SQL, [stream_name, text_array(connection_ids), ttl])
            DELETE FROM pgbus_presence_members
            WHERE stream_name = $1 AND connection_id = ANY($2::text[])
              AND last_seen_at < NOW() - make_interval(secs => $3)
            RETURNING #{SEAT_RECORD_COLUMNS}
          SQL
          rows.to_a.map do |row|
            record = Record.build(row)
            report_change(stream_name, :leave, record)
            record.merge("stream_name" => stream_name)
          end
        end
      end

      # Turns a pgbus_presence_members row into the public record Hash.
      # Seat columns appear only when the row has them.
      module Record
        def self.build(row)
          record = {
            "id" => row["member_id"],
            "metadata" => parse_json(row["metadata"]),
            "joined_at" => row["joined_at"],
            "last_seen_at" => row["last_seen_at"]
          }
          return record unless row.key?("connection_id")

          record.merge("connection_id" => row["connection_id"], "seat" => row["seat"],
                       "state" => parse_json(row["state"]))
        end

        def self.parse_json(value)
          return value if value.is_a?(Hash)
          return {} if value.nil? || value.empty?

          JSON.parse(value)
        rescue JSON::ParserError => e
          # Resilience fallback: return an empty hash rather than
          # crashing the presence member list on one corrupt row. Debug
          # log so operators can still find the corruption without
          # impacting production throughput (debug is off by default).
          # Truncate the value so a huge metadata payload doesn't bloat
          # the log line.
          truncated = value.to_s[0, 200]
          Pgbus.logger&.debug do
            "[Pgbus::Streams::Presence] parse_metadata failed (#{e.class}: #{e.message}); raw: #{truncated.inspect}"
          end
          {}
        end
      end

      def initialize(stream)
        @stream = stream
      end

      # True when the table carries seats (see .seats_supported?).
      def seats?
        self.class.seats_supported?
      end

      # True when the table carries seats, re-probing a cached "no".
      def seats_now?
        self.class.seats_supported_now?
      end

      # Adds (or refreshes) a member on this stream. Idempotent: calling
      # join twice with the same member_id updates last_seen_at and
      # metadata without creating a duplicate row. Yields the member
      # hash to the optional block so the caller can render an
      # `<turbo-stream action="append">` to broadcast the join.
      #
      # With `connection_id:` (seat tables only) the row is the
      # connection's: one per SSE connection, carrying the client's `seat`.
      def join(member_id:, metadata: {}, connection_id: nil, seat: nil)
        member_id = member_id.to_s
        record = if connection_id
                   upsert_seat(member_id, metadata, connection_id.to_s, seat&.to_s)
                 else
                   upsert_member(member_id, metadata)
                 end
        self.class.report_change(@stream.name, :join, record) if connection_id

        if block_given?
          html = yield(record)
          @stream.broadcast(html) if html.is_a?(String) && !html.empty?
        end

        record
      end

      # Removes a member. Yields the (former) member hash to the
      # optional block so the caller can broadcast a `remove` action.
      # Returns the deleted record, or nil if the member wasn't present.
      # Pass `connection_id:` instead to remove one connection's seat row.
      def leave(member_id: nil, connection_id: nil)
        record = if connection_id && member_id.nil?
                   delete_seat(connection_id.to_s)
                 elsif member_id && connection_id.nil?
                   delete_member(member_id.to_s)
                 else
                   raise ArgumentError, "leave takes exactly one of member_id: or connection_id:"
                 end
        return nil unless record

        self.class.report_change(@stream.name, :leave, record) if connection_id

        if block_given?
          html = yield(record)
          @stream.broadcast(html) if html.is_a?(String) && !html.empty?
        end

        record
      end

      # Refreshes the last_seen_at timestamp without re-broadcasting.
      # Used as a heartbeat mechanism: clients ping touch periodically
      # to stay in the member list, and a sweeper expires anyone who
      # hasn't pinged in N seconds.
      #
      # With `connection_id:` (a String or an Array of them) it refreshes
      # those connections' seat rows in one statement and returns the ids
      # it found — a missing id means the row was expired while the
      # connection was still alive, and the caller can re-join it.
      def touch(member_id: nil, connection_id: nil)
        raise ArgumentError, "touch takes exactly one of member_id: or connection_id:" unless member_id.nil? ^ connection_id.nil?

        return touch_seats(Array(connection_id)) if connection_id

        member_id = member_id.to_s
        connection.exec_params(<<~SQL, [@stream.name, member_id])
          UPDATE pgbus_presence_members
          SET last_seen_at = NOW()
          WHERE stream_name = $1 AND member_id = $2#{" AND connection_id IS NULL" if seats_now?}
        SQL
      end

      # Merges `state` into one connection's seat row (keys not given keep
      # their value). Returns the updated record, or nil when the
      # connection has no row on this stream. Does not broadcast: the
      # caller renders whatever its viewers should see.
      def set_state(connection_id:, state:)
        raise ArgumentError, "set_state takes a Hash (got #{state.class})" unless state.is_a?(Hash)

        rows = connection.exec_params(<<~SQL, [@stream.name, connection_id.to_s, JSON.generate(state)])
          UPDATE pgbus_presence_members
          SET state = state || $3::jsonb, last_seen_at = NOW()
          WHERE stream_name = $1 AND connection_id = $2
          RETURNING #{SEAT_RECORD_COLUMNS}
        SQL
        row = rows.first
        row && Record.build(row)
      end

      # Returns the current list of members on this stream as an
      # array of hashes (id, metadata, joined_at, last_seen_at). On a seat
      # table each row is one seat and also carries connection_id, seat
      # and state; group by "id" for one entry per member.
      def members
        rows = connection.exec_params(<<~SQL, [@stream.name])
          SELECT #{seats? ? SEAT_RECORD_COLUMNS : RECORD_COLUMNS}
          FROM pgbus_presence_members
          WHERE stream_name = $1
          ORDER BY joined_at
        SQL
        rows.to_a.map { |row| Record.build(row) }
      end

      # Returns the count of current members. Faster than members.size
      # because it doesn't deserialize metadata.
      def count
        rows = connection.exec_params(<<~SQL, [@stream.name])
          SELECT COUNT(*) AS n FROM pgbus_presence_members WHERE stream_name = $1
        SQL
        rows.first["n"].to_i
      end

      # Removes members whose last_seen_at is older than the given cutoff.
      # Returns the number of expired rows. Use as a sweeper:
      #
      #   Pgbus.stream("room:42").presence.sweep!(older_than: 60.seconds.ago)
      #
      # Atomically claims the deletion via DELETE RETURNING so multiple
      # workers running the sweep concurrently won't double-emit leave
      # events. Yields each expired record when a block is given.
      def sweep!(older_than:)
        rows = connection.exec_params(<<~SQL, [@stream.name, older_than])
          DELETE FROM pgbus_presence_members
          WHERE stream_name = $1 AND last_seen_at < $2#{" AND connection_id IS NULL" if seats_now?}
          RETURNING #{RECORD_COLUMNS}
        SQL
        rows = rows.to_a
        rows.each { |row| yield Record.build(row) } if block_given?
        rows.size
      end

      private

      # A process that last probed the pre-seat schema sends the old conflict
      # target, which Postgres rejects once add_presence_seats has replaced
      # the unique index with a partial one. Re-probe and retry once.
      def upsert_member(member_id, metadata, retried: false)
        upsert_member_once(member_id, metadata)
      rescue PG::InvalidColumnReference
        raise if retried

        self.class.reset_schema_cache!
        upsert_member(member_id, metadata, retried: true)
      end

      def upsert_member_once(member_id, metadata)
        conflict = seats? ? "(stream_name, member_id) WHERE connection_id IS NULL" : "(stream_name, member_id)"
        rows = connection.exec_params(<<~SQL, [@stream.name, member_id, JSON.generate(metadata)])
          INSERT INTO pgbus_presence_members (stream_name, member_id, metadata, joined_at, last_seen_at)
          VALUES ($1, $2, $3::jsonb, NOW(), NOW())
          ON CONFLICT #{conflict}
          DO UPDATE SET metadata = EXCLUDED.metadata, last_seen_at = NOW()
          RETURNING #{RECORD_COLUMNS}
        SQL
        Record.build(rows.first)
      end

      def upsert_seat(member_id, metadata, connection_id, seat)
        rows = connection.exec_params(<<~SQL, [@stream.name, member_id, JSON.generate(metadata), connection_id, seat])
          INSERT INTO pgbus_presence_members
            (stream_name, member_id, metadata, connection_id, seat, joined_at, last_seen_at)
          VALUES ($1, $2, $3::jsonb, $4, $5, NOW(), NOW())
          ON CONFLICT (stream_name, connection_id) WHERE connection_id IS NOT NULL
          DO UPDATE SET member_id = EXCLUDED.member_id, metadata = EXCLUDED.metadata,
                        seat = EXCLUDED.seat, last_seen_at = NOW()
          RETURNING #{SEAT_RECORD_COLUMNS}
        SQL
        Record.build(rows.first)
      end

      def delete_member(member_id)
        rows = connection.exec_params(<<~SQL, [@stream.name, member_id])
          DELETE FROM pgbus_presence_members
          WHERE stream_name = $1 AND member_id = $2#{" AND connection_id IS NULL" if seats_now?}
          RETURNING #{RECORD_COLUMNS}
        SQL
        row = rows.first
        row && Record.build(row)
      end

      def delete_seat(connection_id)
        rows = connection.exec_params(<<~SQL, [@stream.name, connection_id])
          DELETE FROM pgbus_presence_members
          WHERE stream_name = $1 AND connection_id = $2
          RETURNING #{SEAT_RECORD_COLUMNS}
        SQL
        row = rows.first
        row && Record.build(row)
      end

      def touch_seats(connection_ids)
        return [] if connection_ids.empty?

        rows = connection.exec_params(<<~SQL, [@stream.name, self.class.text_array(connection_ids)])
          UPDATE pgbus_presence_members
          SET last_seen_at = NOW()
          WHERE stream_name = $1 AND connection_id = ANY($2::text[])
          RETURNING connection_id
        SQL
        rows.to_a.map { |row| row["connection_id"] }
      end

      def connection
        self.class.connection
      end
    end
  end
end
