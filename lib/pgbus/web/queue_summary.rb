# frozen_string_literal: true

module Pgbus
  module Web
    # Turns one queue's facts into the sentences an operator reads at the top
    # of its page: why it is paused, who drains it, what is waiting, and which
    # priority level it is (issue #491).
    #
    # Pure like JobState: it takes queue_detail, queue_pause_state and
    # queue_drainers as given and does no I/O. Each Line carries a translation
    # key under pgbus.queues.show.summary, raw args (Times under :ago/:time,
    # seconds under :age) and a tone the view maps to colours.
    module QueueSummary
      Line = Data.define(:key, :args, :tone)

      PRIORITY_LEVEL = /_p(\d+)\z/
      FAILURES = /(\d+) consecutive failures/

      module_function

      def present(detail, pause_state, drainers, max_retries:)
        name = detail[:name].to_s
        dlq = name.end_with?(Pgbus::DEAD_LETTER_SUFFIX)
        [
          pause_line(pause_state),
          drain_line(dlq, detail, drainers, max_retries),
          (backlog_line(detail) unless dlq || drainers[:stream]),
          priority_line(name, drainers)
        ].compact
      end

      def pause_line(state)
        return unless state[:paused]

        if state[:resumes_at]
          failures = state[:reason].to_s[FAILURES, 1].to_i
          return line("paused_circuit_breaker", :yellow, ago: state[:paused_at], failures: failures,
                                                         time: state[:resumes_at], trip: state[:trip_count].to_i)
        end
        return line("paused_operator_no_reason", :yellow, ago: state[:paused_at]) if state[:reason].to_s.strip.empty?

        line("paused_operator", :yellow, ago: state[:paused_at], reason: state[:reason])
      end

      # First match wins, as in JobState#ready_overlay: what the queue is
      # explains everything below it, then whether anything is configured to
      # drain it, and only then whether a worker is alive.
      def drain_line(dlq, detail, drainers, max_retries)
        return line("dlq", :gray, logical: drainers[:logical], max: max_retries) if dlq
        return line("stream", :gray) if drainers[:stream]
        return line("handler", :gray, count: drainers[:live_consumers].to_i) if drainers[:handler]

        capsules = Array(drainers[:capsules])
        return line("not_drained", :red) if capsules.empty? && !drainers[:wildcard]

        live = drainers[:live_workers].to_i
        return line("no_workers", :red, visible: detail[:queue_visible_length].to_i) if live.zero?
        return line("drained_wildcard", :gray, count: live) if capsules.empty?

        line("drained_by", :gray, capsules: capsules.join(", "), count: live)
      end

      def backlog_line(detail)
        visible = detail[:queue_visible_length].to_i
        parked = detail[:parked_length].to_i
        if visible.positive?
          line("backlog", :gray, visible: visible, age: detail[:oldest_claimable_age_sec], parked: parked)
        elsif parked.positive?
          line("backlog_parked_only", :gray, parked: parked)
        else
          line("empty", :gray)
        end
      end

      def priority_line(name, drainers)
        level = name[PRIORITY_LEVEL, 1]
        return unless level

        line("priority_level", :gray, level: level.to_i, logical: drainers[:logical])
      end

      def line(key, tone, **args)
        Line.new(key: key, args: args, tone: tone)
      end
    end
  end
end
