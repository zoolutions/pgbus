# frozen_string_literal: true

module Pgbus
  module Web
    # The Events page's words for one pending event (issue #494): JobState's
    # state and reason, re-keyed into event vocabulary with the handler named
    # in every reason, plus the one thing only events have: a pattern no
    # running consumer subscribes to.
    #
    # Rows come from DataSource#event_rows (JobList SQL scoped to the handler
    # queues) and carry :handler_class and :pattern. Pure, like JobState.
    module EventState
      STATES = %w[ready scheduled running retrying].freeze

      # Waiting reasons a missing consumer explains better.
      UNCOVERED = %w[waiting waiting_priority waiting_unknown lease_expired no_workers not_drained].freeze

      # Handler queues have no priority sub-tables and are always drained.
      RENAMED = {
        "running" => "handling", "no_workers" => "no_consumers",
        "waiting_priority" => "waiting", "not_drained" => "no_consumer_for_queue"
      }.freeze

      PROCESSED_TONES = { "completed" => :green, "handling" => :indigo, "abandoned" => :yellow }.freeze

      # jobs: the JobState::Context. covered_queues: Set of physical handler
      # queues some healthy consumer's topics overlap, or nil when unknown.
      # claim_window: EventBus::Handler.claim_ownership_window, in seconds.
      Context = Data.define(:jobs, :covered_queues, :claim_window)
      Result = Data.define(:state, :reason_key, :reason_args, :next_run_at, :badge_tone, :handler_class)
      ProcessedResult = Data.define(:state, :reason_key, :reason_args, :badge_tone)

      module_function

      def present(row, context, ahead: nil)
        job = JobState.present(row, context.jobs, ahead: ahead)
        key = event_reason_key(job.reason_key, row, context)
        handler = row[:handler_class] || row[:queue_name]
        args = key == "no_consumer_for_queue" ? { pattern: row[:pattern] } : job.reason_args
        Result.new(state: job.state, reason_key: key, reason_args: args.merge(handler: handler),
                   next_run_at: job.next_run_at, badge_tone: job.badge_tone, handler_class: row[:handler_class])
      end

      def event_reason_key(key, row, context)
        covered = context.covered_queues
        return "no_consumer_for_queue" if UNCOVERED.include?(key) && covered && !covered.include?(row[:queue_name])

        RENAMED.fetch(key, key)
      end

      # The audit state of one pgbus_processed_events row. processed_at is the
      # claim's last heartbeat (ClaimBeat refreshes it while the handler runs),
      # so a pending claim older than the ownership window went silent.
      def processed(row, now:, claim_window:)
        processed_at = JobState.time(row["processed_at"])
        return processed_result("completed", "completed_legacy", processed_at) unless row.key?("completed_at")

        completed_at = JobState.time(row["completed_at"])
        return processed_result("completed", "completed", completed_at) if completed_at
        return processed_result("handling", "handling", processed_at) if processed_at && now - processed_at < claim_window

        processed_result("abandoned", "abandoned", processed_at)
      end

      def processed_result(state, key, ago)
        ProcessedResult.new(state: state, reason_key: key, reason_args: { ago: ago },
                            badge_tone: PROCESSED_TONES.fetch(state))
      end
    end
  end
end
