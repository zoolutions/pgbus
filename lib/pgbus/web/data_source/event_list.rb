# frozen_string_literal: true

require "securerandom"

module Pgbus
  module Web
    class DataSource
      # The Events page (issue #494): the unified job list scoped to the
      # EventBus handler queues, the words EventState needs about consumers,
      # and Replay of a processed event from its archived message.
      #
      # Handler names and patterns come from the subscriber registry, in Ruby:
      # rows match on the physical queue (queue rows) or the logical one
      # (orphaned failed rows, which carry the name the consumer recorded).
      module EventList
        # Lower bound on archived_at below the oldest claim on a page: the
        # claim always precedes the archive, the margin absorbs clock skew.
        ARCHIVE_PROBE_MARGIN = 60

        # An archived envelope's routing key, where Publisher and the outbox
        # put it. Without one the consumer would archive a replay unrouted.
        ROUTING_KEY_SQL = "COALESCE(a.message->'headers'->>'routing_key', a.message->>'routing_key')"

        def event_rows(state: nil, queue_name: nil, page: 1, per_page: 25)
          queues = handler_queue_physical_names
          return [] if queues.empty?

          rows = job_rows(state: state, queue_name: queue_name, queues: queues, page: page, per_page: per_page)
          by_queue = subscribers_by_queue_name
          rows.map do |row|
            sub = by_queue[row[:queue_name]]
            row.merge(handler_class: sub&.dig(:handler_class), pattern: sub&.dig(:pattern))
          end
        end

        def event_state_counts(queue_name: nil)
          job_state_counts(queue_name: queue_name, queues: handler_queue_physical_names)
        end

        def events_ahead(rows) = jobs_ahead(rows)

        def event_list_context(now: Time.now)
          EventState::Context.new(jobs: job_list_context(now: now), covered_queues: covered_handler_queues,
                                  claim_window: EventBus::Handler.claim_ownership_window)
        end

        # { processed_event id => :replayable | :not_archived | :no_handler }
        # for one page of processed events: one bounded archive probe per
        # handler queue, never one per row.
        def event_replay_states(events)
          queues = registered_subscribers.to_h { |s| [s[:handler_class], s[:physical_queue_name]] }
          states = events.to_h { |e| [e["id"], queues[e["handler_class"]] ? :not_archived : :no_handler] }

          events.select { |e| queues[e["handler_class"]] }.group_by { |e| queues[e["handler_class"]] }
                .each do |queue, rows|
            found = archived_event_ids(queue, rows)
            rows.each { |e| states[e["id"]] = :replayable if found.include?(e["event_id"]) }
          end
          states
        end

        # Re-deliver a processed event's archived envelope to its handler's
        # queue, under a new event_id (so the idempotency claim is new) that
        # links back through replayed_from. Only that handler runs again: a
        # topic publish would re-run every subscriber of the routing key.
        def replay_event(event)
          sub = registered_subscribers.find { |s| s[:handler_class] == event["handler_class"] }
          return false unless sub

          archived = archived_event(sub[:physical_queue_name], event)
          return false unless archived

          message = JSON.parse(archived["message"])
          return false unless event_routing_key(message)

          replay = message.merge("event_id" => SecureRandom.uuid, "replayed_from" => event["event_id"])
          @client.transaction do |txn|
            txn.produce(sub[:physical_queue_name], JSON.generate(replay), headers: archived["headers"])
          end
          true
        rescue StandardError => e
          Pgbus.logger.error { "[Pgbus::Web] Error replaying event #{event["event_id"]}: #{e.class}: #{e.message}" }
          false
        end

        private

        def subscribers_by_queue_name
          registered_subscribers.each_with_object({}) do |s, out|
            out[s[:physical_queue_name]] ||= s
            out[s[:queue_name]] ||= s
          end
        end

        # Physical handler queues some healthy consumer reads: its topics
        # overlap the subscriber's pattern (Registry#queue_names_for_topics'
        # rule). nil when a healthy consumer does not report its topics.
        def covered_handler_queues
          consumers = processes.select { |p| p[:kind].to_s == "consumer" && p[:healthy] }
          topics = consumers.map { |p| p[:metadata].is_a?(Hash) ? p[:metadata]["topics"] : nil }
          return nil if topics.any?(&:nil?)

          registry = EventBus::Registry.instance
          topics = topics.flatten
          registered_subscribers.select { |s| topics.any? { |t| registry.pattern_overlaps?(t, s[:pattern]) } }
                                .to_set { |s| s[:physical_queue_name] }
        end

        def archived_event_ids(queue, events)
          ids = events.map { |e| e["event_id"].to_s }
          connection.select_values(<<~SQL, "Pgbus Archived Events", ["{#{ids.join(",")}}", archive_floor(events)]).to_set
            SELECT a.message->>'event_id' FROM pgmq.a_#{sanitize_name(queue)} a
            WHERE a.archived_at >= $2::timestamptz AND a.message->>'event_id' = ANY($1::text[])
              AND #{ROUTING_KEY_SQL} IS NOT NULL
          SQL
        rescue StandardError => e
          Pgbus.logger.error { "[Pgbus::Web] Error reading the archive of #{queue}: #{e.class}: #{e.message}" }
          Set.new
        end

        def archived_event(queue, event)
          connection.select_one(<<~SQL, "Pgbus Archived Event", [event["event_id"].to_s, archive_floor([event])])
            SELECT a.message::text AS message, a.headers::text AS headers FROM pgmq.a_#{sanitize_name(queue)} a
            WHERE a.message->>'event_id' = $1 AND a.archived_at >= $2::timestamptz
            ORDER BY a.archived_at DESC LIMIT 1
          SQL
        end

        def archive_floor(events)
          oldest = events.filter_map { |e| JobState.time(e["processed_at"]) }.min || Time.at(0)
          (oldest - ARCHIVE_PROBE_MARGIN).utc.iso8601(6)
        end

        def event_routing_key(message)
          headers = message["headers"]
          (headers["routing_key"] if headers.is_a?(Hash)).presence || message["routing_key"].presence
        end
      end
    end
  end
end
