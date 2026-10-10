# frozen_string_literal: true

module Pgbus
  module Web
    # What a process is doing and how close it is to a recycle (issue #493),
    # as lists of [key, args] parts the view joins with " · ". A part shows up
    # only when its fact was reported, so a process from before a key existed
    # just says less. Pure, like ProcessState.
    #
    # args carry raw values: Integers and Floats, seconds under :interval,
    # :uptime and :limit (lifetime), Strings for queue and topic lists.
    module ProcessActivity
      KIND_ORDER = %w[worker consumer dispatcher scheduler outbox_poller].freeze
      RECYCLE_LIMITS = [%w[jobs max_jobs jobs_processed done], %w[memory max_memory_mb memory_mb used]].freeze

      module_function

      def activity(kind:, metadata:, children:, thresholds:)
        case kind
        when "worker" then worker(metadata)
        when "consumer" then consumer(metadata)
        when "dispatcher" then [["maintenance", { interval: thresholds.dispatch_interval }]]
        when "scheduler"
          [["schedule", { count: metadata["tasks"].to_i, interval: thresholds.recurring_schedule_interval }]]
        when "supervisor" then supervisor(children)
        when "outbox_poller" then [["outbox", {}]]
        else []
        end
      end

      def worker(metadata)
        queues = list(metadata["queues"])
        # A "*" anywhere puts the worker in wildcard mode (Worker#@wildcard).
        drains = if queues.include?("*") then ["drains_all", {}]
                 elsif queues.any? then ["drains", { queues: queues.join(", ") }]
                 end
        [drains, *pool(metadata), buffered(metadata), *rates(metadata["rates"])].compact
      end

      def rates(rates)
        return [] unless rates.is_a?(Hash)

        { "rate" => rates["processed"], "failed_rate" => rates["failed"] }
          .filter_map { |key, rate| [key, { rate: rate.to_f }] if rate.to_f.positive? }
      end

      def consumer(metadata)
        topics = list(metadata["topics"])
        handled = metadata["jobs_processed"]
        [(["consumes", { topics: topics.join(", ") }] if topics.any?), *pool(metadata), buffered(metadata),
         (["handled", { number: handled.to_i }] if handled)].compact
      end

      # "3/5 threads busy" from the pool's capacity/busy, or "5 threads" from
      # the registration's thread count when the pool was not reported.
      def pool(metadata)
        unit = metadata["execution_mode"].to_s == "async" ? "slots" : "threads"
        capacity = metadata["capacity"]
        return [["busy_#{unit}", { busy: metadata["busy"].to_i, capacity: capacity.to_i }]] if capacity

        metadata["threads"] ? [[unit, { count: metadata["threads"].to_i }]] : []
      end

      def buffered(metadata)
        count = metadata["buffered"].to_i
        ["buffered", { number: count }] if count.positive?
      end

      def supervisor(children)
        return [["supervises_none", {}]] if children.empty?

        sorted = children.sort_by { |kind, _| [KIND_ORDER.index(kind) || KIND_ORDER.size, kind] }
        [["supervises", { children: sorted }]]
      end

      # Each configured limit next to how far the process has come; nil when
      # the process reported no limit.
      def recycle(metadata, uptime_seconds)
        parts = RECYCLE_LIMITS.filter_map do |key, limit_key, value_key, arg|
          limit = metadata[limit_key]
          [key, { arg.to_sym => metadata[value_key]&.to_i, limit: limit.to_i }] if limit
        end
        lifetime = metadata["max_lifetime"]
        parts << ["lifetime", { uptime: uptime_seconds, limit: lifetime.to_i }] if lifetime
        parts.empty? ? nil : parts
      end

      def list(value)
        case value
        when Array then value.map(&:to_s)
        when String then value.split(",").map(&:strip).reject(&:empty?)
        else []
        end
      end
    end
  end
end
