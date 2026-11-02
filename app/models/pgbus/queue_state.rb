# frozen_string_literal: true

module Pgbus
  class QueueState < BusRecord
    self.table_name = "pgbus_queue_states"

    # One row per queue whatever the spelling (issue #633): the dashboard
    # names a queue from its pgmq.meta row, which PGMQ's lower-case table
    # names make lower case, while a worker names it as configured
    # ("Critical"). Applies to writes and to find_by/where.
    normalizes :queue_name, with: ->(name) { name.to_s.downcase }

    scope :paused, -> { where(paused: true) }

    def self.paused?(queue_name)
      where(queue_name: queue_name, paused: true).exists?
    end

    def self.pause!(queue_name, reason: nil)
      record = find_or_initialize_by(queue_name: queue_name)
      record.update!(paused: true, paused_reason: reason, paused_at: Time.current, circuit_breaker_resume_at: nil)
      record
    end

    def self.resume!(queue_name)
      record = find_by(queue_name: queue_name)
      return unless record

      record.update!(
        paused: false,
        paused_reason: nil,
        paused_at: nil,
        circuit_breaker_trip_count: 0,
        circuit_breaker_resume_at: nil
      )
      record
    end
  end
end
