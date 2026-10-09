# frozen_string_literal: true

require "time"

module Pgbus
  module Web
    # Turns one Jobs-list row into the words an operator reads: why the job is
    # sitting there and when it is expected to run (issue #489).
    #
    # The row's `state` is derived in SQL (DataSource::JobList::STATE_CASE) and
    # taken as given; this class never re-derives it. It only adds the reason
    # and the ETA, from facts the row and the request context already carry, so
    # it does no I/O and is fully unit-testable.
    #
    # reason_args carry raw values: Integers, Strings, and Times under :time
    # (a future moment) and :ago (a past one). The view helper formats them.
    module JobState
      STATES = %w[ready scheduled running retrying blocked].freeze
      AHEAD_CAP = 10_000
      PRIORITY_TABLE = /_p\d+\z/

      BADGE_TONES = {
        "ready" => :blue, "scheduled" => :gray, "running" => :indigo,
        "retrying" => :yellow, "blocked" => :purple
      }.freeze

      # paused: Set of logical queue names. drained: Set of physical queue
      # names, or nil when a wildcard capsule drains every queue.
      Context = Data.define(:now, :max_retries, :paused, :drained, :workers_alive)
      Result = Data.define(:state, :reason_key, :reason_args, :next_run_at, :badge_tone)

      module_function

      def present(row, context, ahead: nil)
        state = row[:state].to_s
        key, args, next_run_at = reason_for(state, row, context, ahead)
        Result.new(state: state, reason_key: key, reason_args: args, next_run_at: next_run_at,
                   badge_tone: BADGE_TONES.fetch(state, :gray))
      end

      def reason_for(state, row, context, ahead)
        case state
        when "scheduled"
          vt = time(row[:vt])
          ["scheduled", { time: vt }, vt]
        when "running" then ["running", { ago: time(row[:last_read_at]), time: time(row[:vt]) }, nil]
        when "retrying" then retrying_reason(row, context)
        when "blocked" then blocked_reason(row)
        else ready_reason(row, context, ahead)
        end
      end

      def ready_reason(row, context, ahead)
        overlay = ready_overlay(row, context)
        return [overlay, {}, nil] if overlay
        return ["lease_expired", { attempt: row[:read_ct].to_i + 1 }, nil] if row[:read_ct].to_i.positive?
        return ["waiting_unknown", {}, nil] if ahead.nil?

        key = row[:queue_name].to_s.match?(PRIORITY_TABLE) ? "waiting_priority" : "waiting"
        [key, { count: ahead_count(ahead) }, nil]
      end

      # First match wins: a paused queue explains everything below it, a queue
      # no capsule drains explains why live workers do not help, and only then
      # does the absence of any healthy worker matter.
      def ready_overlay(row, context)
        return "paused" if context.paused.include?(row[:logical_queue])
        return "not_drained" if context.drained && !context.drained.include?(row[:queue_name])

        "no_workers" unless context.workers_alive
      end

      def retrying_reason(row, context)
        error = row[:error_class]
        return ["orphaned", { error: error }, nil] if row[:source].to_s == "failed"

        attempt = row[:read_ct].to_i
        args = { attempt: attempt, max: context.max_retries, error: error }
        vt = time(row[:vt])
        return ["retrying_dlq", args, vt] if attempt >= context.max_retries
        return ["retrying_due", args, vt] if vt.nil? || vt <= context.now

        ["retrying", args.merge(time: vt), vt]
      end

      def blocked_reason(row)
        key = row[:concurrency_key]
        ago = time(row[:enqueued_at])
        return ["blocked_no_slots", { key: key, ago: ago }, nil] if row[:slots_max].nil?

        ["blocked", { key: key, held: row[:slots_held].to_i, limit: row[:slots_max].to_i, ago: ago }, nil]
      end

      def ahead_count(ahead)
        ahead.to_i >= AHEAD_CAP ? "10k+" : ahead.to_i
      end

      def time(value)
        case value
        when nil, "" then nil
        when String then Time.parse(value)
        else value
        end
      end
    end
  end
end
