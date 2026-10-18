# frozen_string_literal: true

module Pgbus
  module Web
    # The Workflows dashboard's words for one run (issue #542): the badge
    # (its status, or "scheduled" for a ready run whose step waits for a
    # time still ahead) and what it is doing and what happens next, as a
    # pgbus.workflows.next.<key>_html key and its arguments. Arguments named
    # :step, :signal and :batch are names, :ago and :late past moments and
    # :time a future one; the helper renders each. Pure.
    module WorkflowState
      BADGE_TONES = { "ready" => :gray, "scheduled" => :gray, "performing" => :blue, "waiting" => :yellow,
                      "paused" => :yellow, "finished" => :green, "cancelled" => :red }.freeze

      Result = Data.define(:badge, :badge_tone, :next_key, :next_args)

      module_function

      # row – a DataSource#workflows row. threshold – how long a due step may
      # wait for a worker before it counts as overdue (workflow_stall_threshold).
      def present(row, threshold:, now: Time.current)
        badge, key, args = derive(row, threshold, now)
        Result.new(badge: badge, badge_tone: BADGE_TONES.fetch(badge, :gray), next_key: key, next_args: args)
      end

      def derive(row, threshold, now)
        step = row[:current_step]
        case row[:status]
        when "performing" then performing(row, step)
        when "waiting" then waiting(row, step)
        when "paused" then paused(row, step)
        when "ready" then ready(row, step, threshold, now)
        when "finished" then ["finished", "finished", { ago: row[:finished_at] || row[:updated_at] }]
        when "cancelled" then ["cancelled", "cancelled", { ago: row[:cancelled_at] || row[:updated_at] }]
        else [row[:status].to_s, "unknown", { status: row[:status].to_s }]
        end
      end

      def performing(row, step)
        attempt = row[:current_attempt].to_i
        return ["performing", "performing", { step: step }] if attempt <= 1

        ["performing", "performing_attempt", { step: step, attempt: attempt }]
      end

      def waiting(row, step)
        if row[:batch_id]
          progress = row[:batch_progress] || {}
          return ["waiting", "waiting_batch", { batch: batch_name(row), done: progress[:done].to_i, total: progress[:total].to_i }]
        end
        return ["waiting", "waiting_step", { step: step }] unless row[:waiting_for]

        args = { signal: row[:waiting_for], ago: row[:waiting_since] }
        return signal_deadline(row, args.merge(time: row[:timeout_at])) if row[:timeout_at]
        # A per-run wait_timeout: reported once under on_timeout: :notify,
        # then left without a deadline (issue #574).
        return ["waiting", "waiting_signal_late_no_deadline", args.merge(late: row[:late_since])] if row[:late_since]

        ["waiting", "waiting_signal", args]
      end

      # An on_timeout: :notify deadline sends a late notice and the wait goes
      # on (issue #565); late_since is the first deadline the wait missed.
      def signal_deadline(row, args)
        # A deploy that changed on_timeout since the last notice: the next
        # deadline does something else (nil: the class is not loaded here).
        late = row[:late_since] && [nil, "notify"].include?(row[:on_timeout]&.to_s)
        return ["waiting", "waiting_signal_late", args.merge(late: row[:late_since])] if late

        ["waiting", row[:on_timeout].to_s == "notify" ? "waiting_signal_notify" : "waiting_signal_timeout", args]
      end

      def paused(row, step)
        return ["paused", "paused_error", { step: step, error: row[:error_class] }] if row[:error_class]

        ["paused", "paused_manual", { step: step }]
      end

      def ready(row, step, threshold, now)
        due = JobState.time(row[:scheduled_at])
        return ["ready", "ready", { step: step }] unless due
        return ["scheduled", "scheduled", { step: step, time: due }] if due > now
        return ["ready", "overdue", { step: step, ago: due }] if threshold && due < now - threshold

        ["ready", "due", { step: step, ago: due }]
      end

      # The fan-out batch as its description names it: "Class#step".
      def batch_name(row) = "#{row[:workflow_class]}##{row[:current_step]}"
    end
  end
end
