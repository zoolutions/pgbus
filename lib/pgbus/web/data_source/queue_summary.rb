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
          logical = summary_logical_name(name)
          capsules, wildcard = draining_capsules(name)
          healthy = processes.select { |p| p[:healthy] }
          {
            logical: logical, capsules: capsules, wildcard: wildcard,
            handler: handler_queue_physical_names.include?(name), stream: stream_queue_names.include?(name),
            live_workers: healthy.count { |p| p[:kind].to_s == "worker" && worker_drains?(p, logical) },
            live_consumers: healthy.count { |p| p[:kind].to_s == "consumer" },
            siblings: priority_siblings(name, logical)
          }
        end

        private

        def summary_logical_name(name)
          logical_queue_name(name.to_s.delete_suffix(Pgbus::DEAD_LETTER_SUFFIX))
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

        # A heartbeat lists logical queue names, as an Array or a
        # comma-separated String.
        def worker_drains?(process, logical)
          queues = Array((process[:metadata] || {})["queues"]).flat_map { |q| q.to_s.split(",") }.map(&:strip)
          queues.include?("*") || queues.include?(logical)
        end

        def priority_siblings(name, logical)
          return [] unless name.match?(Pgbus::Web::QueueSummary::PRIORITY_LEVEL)

          queues_with_metrics.map { |q| q[:name] }.select do |n|
            n != name && n.match?(Pgbus::Web::QueueSummary::PRIORITY_LEVEL) && logical_queue_name(n) == logical
          end
        end
      end
    end
  end
end
