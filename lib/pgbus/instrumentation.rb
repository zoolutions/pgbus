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
  #   pgbus.job_dead_lettered      — job exceeded max_retries and was DLQ-routed;
  #                                  :error is the last recorded error class (or nil)
  #   pgbus.job_visibility_extended — heartbeat re-armed a running job's visibility timeout
  #                                  payload: queue, job_class, msg_id, vt, extensions
  #   pgbus.event_processed        — event handler succeeded
  #   pgbus.event_failed           — event handler raised; carries :exception_object
  #   pgbus.event_dead_lettered    — a consumer moved an event that exceeded max_retries
  #                                  to its DLQ; fires once, after the move
  #                                  payload: queue_name, routing_key, handler, event_id,
  #                                  read_ct, msg_id, error (the class of the last
  #                                  recorded failure: a handler's error, or the
  #                                  consumer's own parse error for a body it could
  #                                  not route; nil when none was recorded). Any of
  #                                  routing_key/handler/event_id can be nil: all
  #                                  three when the body is not a JSON object, a
  #                                  field the envelope lacks, and handler when no
  #                                  single subscriber in this process owns the queue
  #   pgbus.event_unrouted         — a consumer read an event from a queue no
  #                                  subscriber in this process owns (stale topic
  #                                  binding); the message is archived
  #                                  payload: queue_name, routing_key
  #   pgbus.stream.broadcast       — stream broadcast (sync or deferred)
  #   pgbus.outbox.publish         — outbox row created
  #   pgbus.recurring.enqueue      — scheduler enqueued a due recurring task
  #   pgbus.deploy_task.enqueue    — a deploy task enqueue was attempted (after a rollover or by
  #                                  Run now); payload: task, version, revision, triggered_by, plus
  #                                  :exception/:exception_object when the enqueue raised
  #   pgbus.worker.recycle         — worker hit a recycle threshold (reason/jobs_processed/memory_mb/lifetime_seconds)
  #   pgbus.consumer.recycle       — consumer hit a recycle threshold (same payload shape as pgbus.worker.recycle)
  #   pgbus.serializer.serialize   — job/event serialization
  #   pgbus.serializer.deserialize — job/event deserialization
  #   pgbus.batch_finished         — batch flipped to finished
  #                                  payload: batch_id, total_jobs, completed_jobs, failed_jobs
  #   pgbus.blocked_execution_discarded — a parked job was discarded from the
  #                                  dashboard; it will never run
  #                                  payload: concurrency_key, job_class, job_id
  #   pgbus.batch_sweep            — dispatcher stalled-batch sweep
  #                                  payload: stalled_for, stale_executions, orphan_rows,
  #                                  started_batches, finished_batches
  #   pgbus.workflow.step          — a workflow step attempt ended (body, guard, stall)
  #                                  payload: workflow_class, workflow_id, step_name, attempt,
  #                                  outcome (completed | reattempted | skipped | paused |
  #                                  cancelled | finished_early), status (the step row's),
  #                                  error_class (nil without an error), duration (seconds
  #                                  the body ran; 0.0 for a guard; nil for a stall),
  #                                  pickup_latency (seconds from scheduled_at to the claim)
  #   pgbus.workflow_finished      — a workflow run finished
  #                                  payload: workflow_class, workflow_id
  #   pgbus.workflow_paused        — a workflow run paused (an exception, a stall, pause!)
  #                                  payload: workflow_class, workflow_id, step_name,
  #                                  error_class (nil for a manual pause)
  #   pgbus.workflow_cancelled     — a workflow run was cancelled
  #                                  payload: workflow_class, workflow_id, step_name
  #   pgbus.workflow.stall_detected — a step was stalled on the redelivery of the message that
  #                                  claimed it (detection: "redelivery": its worker died, or
  #                                  its heartbeat lapsed) or after it outlived its stall_at
  #                                  (detection: "sweep")
  #                                  payload: workflow_class, workflow_id, step_name, attempt,
  #                                  detection
  #   pgbus.workflow.gauge         — emitted by each dispatcher workflow sweep
  #                                  payload: statuses ([{ workflow_class:, status:, count: }]),
  #                                  stuck ([{ workflow_class:, reason:, count: }]), zero-filled
  #                                  across every known class
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
