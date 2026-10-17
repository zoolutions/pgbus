# frozen_string_literal: true

module Pgbus
  class Workflow
    # The dispatcher's workflow housekeeping.
    #
    # repair (every config.workflow_sweep_interval):
    # - an in_progress step past its stall_at (started_at + the step's
    #   timeout:, or workflow_stall_threshold) gets workflow_stall_action,
    #   with StepStalled recorded. A dead worker is usually caught sooner, on
    #   redelivery (StepExecution); this catches a live but hung step, and
    #   every crash when the visibility heartbeat is off.
    # - a scheduled step of a ready run, due more than workflow_stall_threshold
    #   ago and not sent within it, is sent again by bumping enqueued_at (the
    #   model callback re-fires). No queue inspection: a duplicate message is
    #   a no-op at the claim.
    # - pgbus.workflow.gauge: runs by class and status, and by class and stuck
    #   reason.
    #
    # cleanup (hourly): finished and cancelled runs older than
    # workflow_retention are deleted in batches; their steps and events go
    # with them (ON DELETE CASCADE).
    module Sweep
      BATCH_SIZE = 500

      # The schema probe's memo (a true answer is kept for the life of the
      # process), read and written under the lock; the probe itself runs
      # outside it.
      @schema_lock = Mutex.new
      @tables_ready = false

      class << self
        def repair(config: Pgbus.configuration, now: Time.current)
          return nil unless tables_ready?

          threshold = config.workflow_stall_threshold
          summary = { stalled: repair_stalled(now: now, config: config), resent: resend_overdue(now: now, threshold: threshold) }
          emit_gauge(now: now, threshold: threshold)
          summary
        end

        def cleanup(retention: Pgbus.configuration.workflow_retention, now: Time.current, batch_size: BATCH_SIZE)
          return 0 unless retention&.positive? && tables_ready?

          cutoff = now - retention
          deleted = 0
          loop do
            ids = WorkflowEntry.stale(before: cutoff).order(:id).limit(batch_size).pluck(:id)
            break if ids.empty?

            deleted += WorkflowEntry.where(id: ids).delete_all
          end
          deleted
        end

        # True once the three workflow tables exist (pgbus:add_workflows).
        def tables_ready?
          return true if @schema_lock.synchronize { @tables_ready }

          ready = begin
            [WorkflowEntry, WorkflowStep, WorkflowEvent].all?(&:table_exists?)
          rescue StandardError => e
            Pgbus.logger.debug { "[Pgbus] Workflow table probe failed: #{e.class}: #{e.message}" }
            false
          end
          @schema_lock.synchronize { @tables_ready = true } if ready
          ready
        end

        # Specs only.
        def reset_tables_ready!
          @schema_lock.synchronize { @tables_ready = false }
        end

        private

        def repair_stalled(now:, config:)
          WorkflowStep.stalled(now: now).order(:id).limit(BATCH_SIZE).pluck(:id, :workflow_id).count do |id, workflow_id|
            stall(id, workflow_id, now: now, config: config)
          end
        end

        # Re-checked under the run's lock: the step may have finished since
        # the query.
        def stall(id, workflow_id, now:, config:)
          Runner.locked(workflow_id) do |record, notes|
            step = record && WorkflowStep.find_by(id: id)
            next false unless step&.status == "in_progress" && step.stall_at && step.stall_at < now

            StepOutcome.stall!(record, step, detection: "sweep", notes: notes, config: config)
            true
          end
        end

        def resend_overdue(now:, threshold:)
          WorkflowStep.overdue(threshold: threshold, now: now).order(:id).limit(BATCH_SIZE).to_a.each do |step|
            step.update!(enqueued_at: now)
          end.size
        end

        def emit_gauge(now:, threshold:)
          statuses = WorkflowEntry.status_counts
          stuck = WorkflowEntry.stuck_reason_counts(threshold: threshold, now: now)
          classes = (statuses.keys.map(&:first) + stuck.keys.map(&:first) + Workflow.registered_classes.map(&:name)).uniq.sort
          Instrumentation.instrument(
            "pgbus.workflow.gauge",
            statuses: zero_filled(classes, States::WORKFLOW, statuses, :status),
            stuck: zero_filled(classes, States::STUCK_REASONS, stuck, :reason)
          )
        end

        # Every class × every value, so a count that drops to zero is
        # reported as zero instead of keeping its last value.
        def zero_filled(classes, values, counts, key)
          classes.product(values).map do |klass, value|
            { workflow_class: klass, key => value, count: counts.fetch([klass, value], 0) }
          end
        end
      end
    end
  end
end
