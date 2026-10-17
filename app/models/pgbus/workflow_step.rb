# frozen_string_literal: true

require "socket"

module Pgbus
  # One step attempt (pgbus_workflow_steps). The unique (workflow_id,
  # step_name, attempt) index makes a duplicate attempt impossible, and the
  # guarded claim (scheduled → in_progress) makes a duplicate StepJob a no-op.
  #
  # The after-commit callback below is the ONLY place a
  # Pgbus::Workflow::StepJob is enqueued: start!, an advance, a reattempt,
  # resume!, run_now!, and the sweep all write a row (a new scheduled row, or
  # a bumped enqueued_at) and let it fire once the write is visible, even
  # inside an application transaction.
  class WorkflowStep < BusRecord
    self.table_name = "pgbus_workflow_steps"

    after_save_commit :enqueue_step_job, if: :needs_enqueue?

    # Inserts the next attempt of +step_name+: one more than the highest
    # attempt so far (1 for a step that never ran).
    def self.insert_next!(workflow_id:, workflow_class:, step_name:, scheduled_at:)
      attempt = where(workflow_id: workflow_id, step_name: step_name).maximum(:attempt).to_i + 1
      create!(workflow_id: workflow_id, workflow_class: workflow_class, step_name: step_name, attempt: attempt,
              status: "scheduled", scheduled_at: scheduled_at, created_at: Time.current)
    end

    # scheduled → in_progress, only once the row is due (a second of slack
    # for clock skew between the sender and this worker). A message that
    # arrives early (left behind by reschedule! to a later time) claims
    # nothing; the later send does. Returns the number of rows claimed.
    def self.claim!(id, now:, stall_at:, msg_id:, worker:, app_revision:)
      where(id: id, status: "scheduled").where(scheduled_at: ..(now + 1))
                                        .update_all(status: "in_progress", started_at: now, stall_at: stall_at,
                                                    msg_id: msg_id, worker: worker, app_revision: app_revision)
    end

    # Records an attempt's outcome, only while the row is still in_progress.
    # 0 means the attempt was declared stalled (or the run cancelled) while
    # it was still running: its outcome is not applied.
    def self.finish!(id, **attrs)
      where(id: id, status: "in_progress").update_all(attrs)
    end

    # in_progress → waiting: the step parks on a signal or its fan-out batch
    # (Workflow::Waiting). Returns the number of rows moved.
    def self.park!(id, **attrs)
      where(id: id, status: "in_progress").update_all(attrs.merge(status: "waiting"))
    end

    # waiting → in_progress, so the outcome of a finished wait (the batch
    # finished, the wait timed out, an operator skipped it) is applied like
    # any attempt's. Returns the number of rows moved.
    def self.unpark!(id)
      where(id: id, status: "waiting").update_all(status: "in_progress")
    end

    # waiting rows of waiting runs past their timeout_at (the sweep applies
    # their on_timeout).
    def self.wait_expired(now: Time.current)
      where(status: "waiting").where(timeout_at: ...now)
                              .where(workflow_id: WorkflowEntry.where(status: "waiting").select(:workflow_id))
    end

    # The scheduled, waiting and in_progress rows of a run, cancelled (an
    # outside cancel!, a resume_at!).
    def self.cancel_open!(workflow_id, now: Time.current)
      where(workflow_id: workflow_id, status: Workflow::States::OPEN_STEP)
        .update_all(status: "cancelled", outcome: "cancelled", finished_at: now)
    end

    # The newest row of a run: the step it is on.
    def self.latest_for(workflow_id)
      where(workflow_id: workflow_id).order(id: :desc).first
    end

    # How many of +step_name+'s latest finished attempts in a row were
    # reattempted (what max_reattempts counts). Any other outcome, such as a
    # pause an operator resumed, ends the run of reattempts.
    def self.consecutive_reattempts(workflow_id, step_name)
      outcomes = where(workflow_id: workflow_id, step_name: step_name).where.not(outcome: nil)
                                                                      .order(attempt: :desc).pluck(:outcome)
      outcomes.take_while { |outcome| outcome == "reattempted" }.size
    end

    def self.completed_output(workflow_id, step_name)
      where(workflow_id: workflow_id, step_name: step_name.to_s, status: "completed").order(attempt: :desc).pick(:output)
    end

    # { step_name => output } of each step's latest completed attempt.
    def self.completed_outputs(workflow_id)
      where(workflow_id: workflow_id, status: "completed").order(:attempt).pluck(:step_name, :output).to_h
    end

    # in_progress rows past their stall_at.
    def self.stalled(now: Time.current)
      where(status: "in_progress").where(stall_at: ...now)
    end

    # scheduled rows of ready runs, due more than +threshold+ ago and not
    # sent within it: the sweep sends them again.
    def self.overdue(threshold:, now: Time.current)
      cutoff = now - threshold
      where(status: "scheduled").where(scheduled_at: ...cutoff)
                                .where("enqueued_at IS NULL OR enqueued_at < ?", cutoff)
                                .where(workflow_id: WorkflowEntry.where(status: "ready").select(:workflow_id))
    end

    # Fires on a new scheduled row, on a change to scheduled, and on a
    # bumped enqueued_at; never on any other save.
    def needs_enqueue?
      status == "scheduled" && (previously_new_record? || saved_change_to_status? || saved_change_to_enqueued_at?)
    end

    private

    # Runs after commit, so a failure cannot roll back the row: it is
    # reported, and the sweep sends the step again once it is
    # workflow_stall_threshold overdue.
    def enqueue_step_job
      job = Workflow::StepJob.set(**step_job_set_options).perform_later(id)
      update_columns(enqueued_at: Time.current, job_id: job.job_id) if job
    rescue StandardError => e
      Pgbus.logger.error do
        "[Pgbus] Workflow StepJob for step #{id} (#{step_name}) could not be enqueued: #{e.class}: #{e.message}"
      end
      ErrorReporter.report(e, { action: "workflow_step_enqueue", workflow_class: workflow_class,
                                workflow_id: workflow_id, step_name: step_name, attempt: attempt })
    end

    # queue/priority from the class and the step (step wins), sent at
    # scheduled_at (which already holds any job_options wait/wait_until, see
    # Workflow::Runner.schedule!).
    def step_job_set_options
      klass = workflow_class.safe_constantize
      options = klass.is_a?(Class) && klass < Workflow ? klass.step_job_options_for(step_name) : {}
      set = { queue: options[:queue] || Pgbus.configuration.workflow_queue, priority: options[:priority] }
      set[:wait_until] = scheduled_at if scheduled_at > Time.current
      set.compact
    end
  end
end
