# frozen_string_literal: true

module Pgbus
  module Web
    class DataSource
      # The Workflows dashboard's actions (issue #542). Each goes through the
      # run's own outside verb (Pgbus::Workflow#resume! and friends), so the
      # States table and the row lock decide, and returns the real outcome:
      #
      #   { ok: true }                                      it happened
      #   { ok: false, code:, reason:, status: }            it did not, and why
      #
      # code is :not_allowed (the run is no longer in a state that allows it,
      # usually a lost race: status says where it is now), :not_found,
      # :invalid (an unknown step, a payload over the size cap) or :failed
      # (anything else: logged). actor is who it is recorded as on the run's
      # audit trail; reason is the operator's note.
      module WorkflowActions
        # At most this many runs per bulk call; a bigger group takes another press.
        BULK_CAP = 500
        # The outcomes a bulk call counts as "had already moved on".
        MOVED_ON = %i[not_allowed not_found].freeze

        def resume_workflow(workflow_id, actor:, reason: nil)
          workflow_action(workflow_id) { |wf| wf.resume!(actor: actor, reason: reason) }
        end

        def cancel_workflow(workflow_id, actor:, reason: nil)
          workflow_action(workflow_id) { |wf| wf.cancel!(actor: actor, reason: reason) }
        end

        def skip_workflow_step(workflow_id, actor:, reason: nil)
          workflow_action(workflow_id) { |wf| wf.skip!(actor: actor, reason: reason) }
        end

        def resume_workflow_at(workflow_id, step:, actor:, reason: nil)
          workflow_action(workflow_id) { |wf| wf.resume_at!(step, actor: actor, reason: reason) }
        end

        def run_workflow_step_now(workflow_id, actor:, reason: nil)
          workflow_action(workflow_id) { |wf| wf.run_now!(actor: actor, reason: reason) }
        end

        # A redelivery with the same idempotency_key stores nothing: ok, with
        # duplicate: true. The signal itself carries no reason (signal! takes
        # none); the actor is on its signal_received event.
        def signal_workflow(workflow_id, name:, payload:, idempotency_key:, actor:, reason: nil) # rubocop:disable Lint/UnusedMethodArgument -- every action takes reason:
          duplicate = false
          result = workflow_action(workflow_id) do |wf|
            duplicate = wf.signal!(name, payload: payload, idempotency_key: idempotency_key, actor: actor).duplicate_delivery?
          end
          result[:ok] ? result.merge(duplicate: duplicate) : result
        end

        # The new run's workflow_id on success.
        def rerun_workflow(workflow_id, actor:, reason: nil)
          rerun = nil
          result = workflow_action(workflow_id) { |wf| rerun = wf.rerun!(actor: actor, reason: reason) }
          result[:ok] ? result.merge(workflow_id: rerun.workflow_id) : result
        end

        # +group+ is a needs-attention group (workflow_class, step_name,
        # reason, error_class). Each of its runs, at most BULK_CAP, through its
        # own guarded transition: one that moved on in the meantime is
        # skipped. { ok:, done:, skipped:, failed:, failure: }, or ok: false
        # with a reason when the group could not be read.
        def bulk_resume_workflows(group:, actor:, reason: nil)
          bulk_workflow_action(group) { |id| resume_workflow(id, actor: actor, reason: reason) }
        end

        def bulk_cancel_workflows(group:, actor:, reason: nil)
          bulk_workflow_action(group) { |id| cancel_workflow(id, actor: actor, reason: reason) }
        end

        private

        def workflow_action(workflow_id)
          workflow = Workflow.find(workflow_id)
          return { ok: false, code: :not_found, reason: "workflow #{workflow_id} not found" } unless workflow

          yield workflow
          { ok: true }
        rescue Workflow::InvalidTransition => e
          { ok: false, code: :not_allowed, reason: e.message, status: WorkflowEntry.where(workflow_id: workflow_id).pick(:status) }
        rescue ActiveRecord::RecordNotFound => e
          { ok: false, code: :not_found, reason: e.message }
        rescue Workflow::StepNotDefined, Workflow::PayloadTooLarge, ArgumentError => e
          { ok: false, code: :invalid, reason: e.message }
        rescue StandardError => e
          Pgbus.logger.error { "[Pgbus::Web] Workflow action on #{workflow_id} failed: #{e.class}: #{e.message}" }
          { ok: false, code: :failed, reason: "#{e.class}: #{e.message}" }
        end

        # done: changed. skipped: had moved on (no longer in a state that
        # allows it, or gone). failed: anything else, with the first reason.
        def bulk_workflow_action(group, &)
          ids = WorkflowEntry.attention_ids(group, **attention_thresholds, limit: BULK_CAP)
          outcomes = ids.map(&)
          moved_on, failed = outcomes.reject { |outcome| outcome[:ok] }.partition { |outcome| MOVED_ON.include?(outcome[:code]) }
          result = { ok: true, done: outcomes.size - moved_on.size - failed.size, skipped: moved_on.size, failed: failed.size }
          failed.empty? ? result : result.merge(failure: failed.first[:reason])
        rescue StandardError => e
          Pgbus.logger.error { "[Pgbus::Web] Bulk workflow action on #{group.inspect} failed: #{e.class}: #{e.message}" }
          { ok: false, code: :failed, reason: "#{e.class}: #{e.message}", done: 0, skipped: 0, failed: 0 }
        end
      end
    end
  end
end
