# frozen_string_literal: true

module Pgbus
  # Lightweight instrumentation via ActiveSupport::Notifications.
  #
  # All events are prefixed with "pgbus." and carry timing information
  # automatically when used with the block form of AS::Notifications.instrument.
  #
  # Events emitted:
  #   pgbus.client.send_message    — single message enqueue
  #   pgbus.client.send_batch      — batch enqueue
  #   pgbus.client.read_batch      — batch dequeue
  #   pgbus.client.read_message    — single message dequeue
  #   pgbus.client.pool            — connection pool snapshot (size/available/pool_timeout); emitted per worker heartbeat
  #   pgbus.executor.execute       — full job execution (deserialize + perform + archive)
  #   pgbus.job_completed          — job archived successfully
  #   pgbus.job_failed             — job raised; carries :exception_object
  #   pgbus.job_dead_lettered      — job exceeded max_retries and was DLQ-routed
  #   pgbus.event_processed        — event handler succeeded
  #   pgbus.event_failed           — event handler raised; carries :exception_object
  #   pgbus.stream.broadcast       — stream broadcast (sync or deferred)
  #   pgbus.outbox.publish         — outbox row created
  #   pgbus.recurring.enqueue      — scheduler enqueued a due recurring task
  #   pgbus.worker.recycle         — worker hit a recycle threshold (reason/jobs_processed/memory_mb/lifetime_seconds)
  #   pgbus.consumer.recycle       — consumer hit a recycle threshold (same payload shape as pgbus.worker.recycle)
  #   pgbus.serializer.serialize   — job/event serialization
  #   pgbus.serializer.deserialize — job/event deserialization
  #   pgbus.batch_finished         — batch flipped to finished
  #                                  payload: batch_id, total_jobs, completed_jobs, failed_jobs
  #   pgbus.batch_sweep            — dispatcher stalled-batch sweep
  #                                  payload: stalled_for, stale_executions, orphan_rows,
  #                                  started_batches, finished_batches
  #
  module Instrumentation
    module_function

    def instrument(event, payload = {}, &block)
      if defined?(ActiveSupport::Notifications)
        ActiveSupport::Notifications.instrument(event, payload, &block)
      elsif block
        yield payload
      end
    end
  end
end
