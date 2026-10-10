# frozen_string_literal: true

module Pgbus
  module Web
    class DataSource
      # The facts behind a queue page's summary in words (issue #491), turned
      # into sentences by Pgbus::Web::QueueSummary. Both methods take the
      # physical queue name the page is for.
      module QueueSummary
        RUNNING = { paused: false, reason: nil, paused_at: nil, resumes_at: nil, trip_count: 0 }.freeze

        # Pause state is kept per logical queue, so every priority level and
        # the bare table share it.
        def queue_pause_state(name)
          state = QueueState.find_by(queue_name: logical_queue_name(name))
          return RUNNING unless state&.paused

          { paused: true, reason: state.paused_reason, paused_at: state.paused_at,
            resumes_at: state.circuit_breaker_resume_at, trip_count: state.circuit_breaker_trip_count.to_i }
        rescue StandardError => e
          Pgbus.logger.error { "[Pgbus::Web] Error reading pause state for #{name}: #{e.class}: #{e.message}" }
          RUNNING
        end

        def queue_drainers(name)
          table = name.to_s.delete_suffix(Pgbus::DEAD_LETTER_SUFFIX)
          level = priority_level_of(table)
          logical = level ? logical_queue_name(table) : table.delete_prefix("#{Pgbus.configuration.queue_prefix}_")
          capsules, wildcard = draining_capsules(name)
          healthy = processes.select { |p| p[:healthy] }
          {
            logical: logical, capsules: capsules, wildcard: wildcard,
            handler: handler_queue_physical_names.include?(name), stream: stream_queue_names.include?(name),
            live_workers: healthy.count { |p| p[:kind].to_s == "worker" && worker_drains?(p, logical) },
            live_consumers: healthy.count { |p| p[:kind].to_s == "consumer" && consumer_drains?(p, name) },
            siblings: level ? priority_siblings(name, logical) : [], priority_level: level
          }
        end

        private

        # The level of a priority sub-table, or nil. `_pN` is legal in an
        # ordinary queue name, so only a table the queue strategy creates for
        # the stripped logical name counts.
        def priority_level_of(table)
          level = table[Pgbus::Web::QueueSummary::PRIORITY_LEVEL, 1]
          return unless level && @client.physical_queue_names(logical_queue_name(table)).include?(table)

          level.to_i
        rescue StandardError => e
          Pgbus.logger.debug { "[Pgbus::Web] Error resolving priority level of #{table}: #{e.class}: #{e.message}" }
          nil
        end

        # [names of the capsules whose queues expand to this table, wildcard?].
        # An anonymous capsule goes by its first queue. Fails open to a
        # wildcard, like drained_queue_names: never a false "nothing drains it".
        def draining_capsules(name)
          capsules = Array(Pgbus.configuration.workers)
          wildcard = capsules.any? { |c| capsule_queue_list(c).include?("*") }
          names = capsules.filter_map do |c|
            queues = capsule_queue_list(c) - ["*"]
            next unless queues.any? { |q| @client.physical_queue_names(q).include?(name) }

            (c[:name] || c["name"] || queues.first).to_s
          end
          [names, wildcard]
        rescue StandardError => e
          Pgbus.logger.error { "[Pgbus::Web] Error resolving capsules for #{name}: #{e.class}: #{e.message}" }
          [[], true]
        end

        def capsule_queue_list(capsule)
          Array(capsule[:queues] || capsule["queues"]).map(&:to_s)
        end

        # A heartbeat lists queue names as configured (Array or comma-separated
        # String); tables are named after their normalized form ("bulk-imports"
        # → pgbus_bulk_imports), so compare that.
        def worker_drains?(process, logical)
          queues = Array((process[:metadata] || {})["queues"]).flat_map { |q| q.to_s.split(",") }.map(&:strip)
          queues.include?("*") || queues.any? { |q| normalized_queue(q) == logical }
        end

        # A consumer drains a handler queue when one of its topic filters
        # overlaps a subscription routed to it (the rule the consumer itself
        # uses to pick queues). A heartbeat without topics counts.
        def consumer_drains?(process, name)
          topics = Array((process[:metadata] || {})["topics"])
          return true if topics.empty?

          patterns = registered_subscribers.select { |s| s[:physical_queue_name] == name }.map { |s| s[:pattern] }
          registry = EventBus::Registry.instance
          topics.any? { |t| patterns.any? { |pattern| registry.pattern_overlaps?(t.to_s, pattern) } }
        end

        def normalized_queue(queue)
          Pgbus.configuration.queue_name(queue).delete_prefix("#{Pgbus.configuration.queue_prefix}_")
        rescue ArgumentError
          queue
        end

        def priority_siblings(name, logical)
          queues_with_metrics.map { |q| q[:name] }.select do |n|
            n != name && n.match?(Pgbus::Web::QueueSummary::PRIORITY_LEVEL) && logical_queue_name(n) == logical
          end
        end
      end
    end
  end
end
