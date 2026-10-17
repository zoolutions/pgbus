# frozen_string_literal: true

module Pgbus
  class Workflow
    # fan_out steps (issue #541): a group of jobs the workflow waits for as
    # one batch.
    #
    #   fan_out :notify_vendors do |b|
    #     b.add(*subject.vendors.map { |vendor| NotifyVendorJob.new(vendor.id) })
    #   end
    #
    # 1. run (inside the StepJob, outside any transaction): the body gets a
    #    Builder that records its b.add calls. A flow-control verb or an
    #    exception in the body ends the step before any batch exists. When
    #    the body returns, Pgbus.batch opens with on_complete
    #    BatchFinishedJob.new(step_id) and the recorded adds.
    # 2. park (under the run's lock): the step waits on the batch. A batch
    #    that already finished (an empty one finishes at once, before
    #    Pgbus.batch returns) is resolved here and then, since its callback
    #    may have run before the step parked and found nothing to resume.
    # 3. finished (BatchFinishedJob, under the lock): no dead-lettered member
    #    completes the step and advances; any applies on_batch_failure with
    #    FanOutFailed recorded. A step no longer waiting (skipped, cancelled,
    #    already resolved by the park) is left alone.
    #
    # A reattempt or a resume! of a failed fan_out runs the whole body again
    # and opens a fresh batch: member jobs must be idempotent.
    module FanOut
      # The `b` a fan_out body gets. Records its adds; the batch opens once
      # the body returns. The workflow owns the batch's callbacks.
      class Builder
        CALLBACK_MESSAGE = "a fan_out step owns its batch's callbacks (on_complete resumes the workflow); " \
                           "do the follow-up work in the next step"

        def initialize
          @adds = []
        end

        # Adds job instances (MyJob.new(args), optionally .set(...)). A block
        # runs inside the batch, so a perform_later in it joins too.
        def add(*jobs, &block)
          jobs = Batch::Builder.job_instances(jobs)
          @adds << [jobs, block]
          jobs
        end

        def size = @adds.sum { |jobs, _block| jobs.size }

        %i[on_complete on_success on_failure on_finish].each do |slot|
          define_method(slot) { |*, **| raise ArgumentError, "#{slot}: #{CALLBACK_MESSAGE}" }
        end

        # Replays the recorded adds on a real batch builder.
        def replay(batch_builder)
          @adds.each { |jobs, block| batch_builder.add(*jobs, &block) }
        end
      end

      module_function

      def run(definition, workflow)
        builder = Builder.new
        definition.body.is_a?(Symbol) ? workflow.send(definition.body, builder) : workflow.instance_exec(builder, &definition.body)
        batch = open_batch(workflow, workflow.send(:step_context), builder)
        throw FlowControl::THROW_TAG, FlowControl::Outcome.fan_out(batch_id: batch.batch_id, size: batch.reload.total_jobs)
      end

      def open_batch(workflow, attempt, builder)
        queue = workflow.class.step_job_options_for(attempt.step_name)[:queue] || Pgbus.configuration.workflow_queue
        callback = BatchFinishedJob.new(attempt.step_id).set(queue: queue)
        Pgbus.batch(description: "#{workflow.workflow_class}##{attempt.step_name} #{workflow.workflow_id}",
                    properties: { workflow_id: workflow.workflow_id, step_id: attempt.step_id }) do |b|
          b.on_complete(callback)
          builder.replay(b)
        end
      end

      # The StepJob's outcome for a fan_out whose batch is open. What the body
      # recorded with output(...) is kept on the row until the step resolves.
      def park(record, step, outcome, notes:, output: nil)
        batch_id = outcome.details.fetch(:batch_id)
        Runner.event!(record, "fan_out_opened", actor: Runner::SYSTEM_ACTOR, step: step, details: outcome.details)
        unless Waiting.park!(record, step, now: Time.current, batch_id: batch_id, output: output)
          StepOutcome.ignore_late_completion(record, step, "waiting", "fan_out", nil)
          return
        end

        entry = BatchEntry.find_by(batch_id: batch_id)
        resolve(record, step, entry, notes) if entry&.status == "finished"
      end

      # BatchFinishedJob#perform (and the sweep, for a lost one). Truthy when
      # it resolved the step.
      def finished(step_id)
        step = WorkflowStep.find_by(id: step_id)
        return log("step #{step_id} no longer exists (its run was deleted)") unless step

        Runner.locked(step.workflow_id) do |record, notes|
          step = record && WorkflowStep.find_by(id: step_id)
          unless step&.status == "waiting" && step.batch_id
            next log("step #{step_id} is not waiting on its batch (#{step&.status || "gone"}): nothing to resume")
          end

          entry = BatchEntry.find_by(batch_id: step.batch_id)
          next log("batch #{step.batch_id} of step #{step_id} has not finished") unless entry&.status == "finished"

          resolve(record, step, entry, notes)
        end
      end

      # The sweep's repair for a lost callback: fan_out steps still waiting on
      # a batch that finished more than +grace+ ago. Returns how many it
      # resolved.
      def resolve_lost(now:, grace:, limit: Sweep::BATCH_SIZE)
        finished_batches = BatchEntry.finished.where(finished_at: ...(now - grace)).select(:batch_id)
        WorkflowStep.where(status: "waiting", batch_id: finished_batches).order(:id).limit(limit).pluck(:id)
                    .count { |step_id| finished(step_id) }
      end

      def resolve(record, step, entry, notes)
        batch = Batch.find(entry.batch_id)
        failed = batch.failed_jobs
        total = batch.total_jobs
        Runner.event!(record, "fan_out_finished", actor: Runner::SYSTEM_ACTOR, step: step,
                                                  details: { batch_id: entry.batch_id, failed_jobs: failed, total_jobs: total })
        return Waiting.resolve!(record, step, FlowControl::Outcome.complete, notes: notes) if failed.zero?

        error = FanOutFailed.new("#{failed} of #{total} jobs dead-lettered (batch #{entry.batch_id})")
        Waiting.resolve!(record, step, FlowControl::Outcome.for(failure_action(record, step)), notes: notes, error: error)
      end

      def failure_action(record, step)
        definition = record.workflow_klass&.workflow_steps&.fetch(step.step_name)
        definition.respond_to?(:on_batch_failure) ? definition.on_batch_failure : :pause!
      end

      def log(message)
        Pgbus.logger.info { "[Pgbus::Workflow] fan_out: #{message}" }
        nil
      end
    end
  end
end
