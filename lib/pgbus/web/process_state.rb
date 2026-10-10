# frozen_string_literal: true

require "json"
require "time"

module Pgbus
  module Web
    # The Processes page's words for one pgbus_processes row (issue #493):
    # why its status is what it is and what happens next.
    #
    # Owns the status derivation too (DataSource#processes calls derive), so
    # the badge and the sentence "loop last ticked 45s ago (limit 90s)" come
    # from the same now and the same limits. present takes the row's status as
    # given, like JobState. Pure: no I/O, no config reads.
    #
    # reason_args/next_args carry raw values: Times under :ago, seconds under
    # :limit, :age and :within. The view helper formats them.
    module ProcessState
      # Kinds that stamp a loop beacon (loop_tick_at) on every iteration.
      LOOP_KINDS = %w[worker consumer dispatcher scheduler].freeze
      # Kinds the supervisor's watchdog SIGKILLs when their beacon stalls.
      WATCHED_KINDS = %w[worker consumer].freeze

      BADGE_TONES = { "healthy" => :green, "stalled" => :yellow, "stale" => :red }.freeze

      # Everything derive needs, captured once per request so every row is
      # judged against the same now.
      Thresholds = Data.define(:now, :alive_threshold, :stall_threshold, :dispatch_interval,
                               :recurring_schedule_interval, :polling_interval, :notify_fallback_poll,
                               :notify_wakeup, :reap_interval)

      # supervisors: { [hostname, pid] => status } for every supervisor row.
      # children: { [hostname, supervisor_pid] => { kind => count } }.
      # live_dispatcher: some dispatcher row is healthy (the reaper runs in it).
      Context = Data.define(:thresholds, :supervisors, :children, :live_dispatcher)

      Result = Data.define(:status, :badge_tone, :reason_key, :reason_args, :next_key, :next_args,
                           :activity, :recycle, :started_at, :uptime_seconds, :supervisor_pid)

      module_function

      def derive(kind:, metadata:, heartbeat_at:, thresholds:)
        return :stale if stale?(heartbeat_at, thresholds)
        return :healthy unless metadata.is_a?(Hash)

        limit = stall_limit_for(kind, thresholds)
        tick = metadata["loop_tick_at"]
        return :healthy unless limit && tick

        thresholds.now.to_f - tick.to_f > limit ? :stalled : :healthy
      end

      def stale?(heartbeat_at, thresholds)
        heartbeat = JobState.time(heartbeat_at)
        heartbeat ? thresholds.now - heartbeat > thresholds.alive_threshold : false
      end

      # Kind-aware stall limit. Workers run a tight loop, so the base
      # stall_threshold applies directly. The dispatcher and scheduler sleep
      # between iterations, so their beacon is naturally up to one sleep
      # interval old even when healthy. Consumers idle on an empty-read wait
      # that reaches notify_fallback_poll when a live listener drives wake-up,
      # or polling_interval otherwise. Kinds with no loop beacon: nil.
      def stall_limit_for(kind, thresholds)
        base = thresholds.stall_threshold
        return nil unless base&.positive?

        case kind.to_s
        when "worker" then base
        when "dispatcher" then base + thresholds.dispatch_interval
        when "scheduler" then base + thresholds.recurring_schedule_interval
        when "consumer" then base + consumer_slack(thresholds)
        end
      end

      def consumer_slack(thresholds)
        return thresholds.polling_interval unless thresholds.notify_wakeup

        [thresholds.polling_interval, thresholds.notify_fallback_poll].max
      end

      # The cross-row facts, from the already-derived rows; shared by
      # DataSource and the test stub.
      def context_for(rows, thresholds)
        supervisors = rows.select { |r| r[:kind].to_s == "supervisor" }
                          .to_h { |r| [[r[:hostname], r[:pid].to_i], status_of(r)] }
        children = Hash.new { |h, k| h[k] = Hash.new(0) }
        rows.each do |r|
          parent = parse_metadata(r[:metadata])["supervisor_pid"]
          children[[r[:hostname], parent.to_i]][r[:kind].to_s] += 1 if parent && r[:kind].to_s != "supervisor"
        end
        live = rows.any? { |r| r[:kind].to_s == "dispatcher" && status_of(r) == :healthy }
        Context.new(thresholds: thresholds, supervisors: supervisors,
                    children: children.transform_values(&:to_h), live_dispatcher: live)
      end

      # Host by host: each supervisor followed by its children, then the
      # standalone rows, each group in the order given.
      def order(rows, context)
        rows.group_by { |r| r[:hostname].to_s }.sort_by(&:first).flat_map do |_host, host_rows|
          supervisors, others = host_rows.partition { |r| r[:kind].to_s == "supervisor" }
          children = others.group_by { |r| supervisor_pid(r, parse_metadata(r[:metadata]), context) }
          supervisors.flat_map { |s| [s, *children.delete(s[:pid].to_i)] } + children.values.flatten
        end
      end

      def present(row, context)
        thresholds = context.thresholds
        metadata = parse_metadata(row[:metadata])
        status = status_of(row).to_s
        reason_key, reason_args = reason_for(status, row, metadata, thresholds)
        next_key, next_args = next_for(status, row, metadata, context)
        started_at = JobState.time(metadata["started_at"] && Time.at(metadata["started_at"].to_f))
        uptime = started_at && (thresholds.now - started_at).round
        Result.new(
          status: status, badge_tone: BADGE_TONES.fetch(status, :red),
          reason_key: reason_key, reason_args: reason_args, next_key: next_key, next_args: next_args,
          activity: ProcessActivity.activity(kind: row[:kind].to_s, metadata: metadata,
                                             children: context.children.fetch([row[:hostname], row[:pid].to_i], {}),
                                             thresholds: thresholds),
          recycle: ProcessActivity.recycle(metadata, uptime),
          started_at: started_at, uptime_seconds: uptime, supervisor_pid: supervisor_pid(row, metadata, context)
        )
      end

      def reason_for(status, row, metadata, thresholds)
        heartbeat = JobState.time(row[:last_heartbeat_at])
        alive = thresholds.alive_threshold
        limit = stall_limit_for(row[:kind], thresholds)
        tick = metadata["loop_tick_at"] && Time.at(metadata["loop_tick_at"].to_f)
        case status
        when "stale" then ["stale", { age: heartbeat && (thresholds.now - heartbeat).round, limit: alive }]
        when "stalled" then ["stalled", { ago: tick, limit: limit }]
        else healthy_reason(row[:kind].to_s, heartbeat, tick, limit, alive)
        end
      end

      def healthy_reason(kind, heartbeat, tick, limit, alive)
        return ["healthy_loop", { ago: tick, limit: limit }] if limit && tick

        key = LOOP_KINDS.include?(kind) && limit.nil? ? "healthy_detection_off" : "healthy_heartbeat"
        [key, { ago: heartbeat, limit: alive }]
      end

      def next_for(status, row, metadata, context)
        case status
        when "stale"
          context.live_dispatcher ? ["stale_reaped", { within: context.thresholds.reap_interval }] : ["stale_unreaped", {}]
        when "stalled" then [stalled_next(row, metadata, context), {}]
        else [nil, {}]
        end
      end

      def stalled_next(row, metadata, context)
        kind = row[:kind].to_s
        return { "dispatcher" => "stalled_maintenance", "scheduler" => "stalled_schedule" }[kind] unless WATCHED_KINDS.include?(kind)

        parent = supervisor_pid(row, metadata, context)
        parent && context.supervisors[[row[:hostname], parent]] == :healthy ? "stalled_supervised" : "stalled_standalone"
      end

      # The supervisor's pid, only when that supervisor's row is on the same
      # host: a standalone process's parent is a shell, foreman or systemd.
      def supervisor_pid(row, metadata, context)
        parent = metadata["supervisor_pid"]&.to_i
        parent if parent && context.supervisors.key?([row[:hostname], parent])
      end

      def status_of(row)
        status = row[:status]
        return status.to_sym if status

        row[:healthy] ? :healthy : :stale
      end

      def parse_metadata(metadata)
        case metadata
        when Hash then metadata
        when String then JSON.parse(metadata).then { |h| h.is_a?(Hash) ? h : {} }
        else {}
        end
      rescue JSON::ParserError
        {}
      end
    end
  end
end
