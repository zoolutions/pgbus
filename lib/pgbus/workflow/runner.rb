# frozen_string_literal: true

require "securerandom"
require "socket"

module Pgbus
  class Workflow
    # The transitions every caller shares: start!, the outside verbs, the
    # StepJob and the sweep. Each change happens under the workflow row lock
    # (WorkflowEntry.lock_for) as a guarded UPDATE whose WHERE status IN (...)
    # comes from the States table, and writes its WorkflowEvent in the same
    # transaction. Instrumentation and error reports wait until it commits.
    module Runner
      DEFAULT_ACTOR = "app"
      SYSTEM_ACTOR = "system"

      # Instrumentation events and error reports collected inside a
      # transaction, sent once it is over.
      class Notes
        def initialize
          @entries = []
        end

        def instrument(event, payload) = @entries << [:instrument, event, payload]

        def report(error, context) = @entries << [:report, error, context]

        def flush
          entries = @entries
          @entries = []
          entries.each do |kind, subject, detail|
            kind == :instrument ? Instrumentation.instrument(subject, detail) : ErrorReporter.report(subject, detail)
          end
        end
      end

      module_function

      # --- start! and rerun! --------------------------------------------

      def start(klass, subject:, arguments:, idempotency_key: nil)
        attrs = run_attributes(klass, idempotency_key: idempotency_key)
                .merge(Payload.subject_attributes(subject))
                .merge(arguments: Payload.dump(arguments, what: "arguments of #{klass.name}"))
        insert_run(klass, attrs, actor: DEFAULT_ACTOR)
      end

      # A finished or cancelled run started again: same subject and
      # arguments (copied as stored, not reloaded), no idempotency key.
      def rerun(workflow, actor:, reason: nil)
        WorkflowEntry.transaction do
          old = WorkflowEntry.lock_for(workflow.workflow_id) || raise(ActiveRecord::RecordNotFound,
                                                                      "workflow #{workflow.workflow_id} is gone")
          States.check!(:rerun, old.status)
          klass = old.workflow_klass || raise(StepNotDefined, "#{old.workflow_class} is not a Pgbus::Workflow class any more")
          attrs = run_attributes(klass, rerun_of: old.workflow_id)
                  .merge(old.attributes.symbolize_keys.slice(:subject_type, :subject_id, :subject_gid, :arguments))
          rerun = insert_run(klass, attrs, actor: actor, reason: reason)
          event!(old, "rerun", actor: actor, reason: reason, details: { rerun_workflow_id: rerun.workflow_id })
          rerun
        end
      end

      def run_attributes(klass, idempotency_key: nil, rerun_of: nil)
        raise ArgumentError, "an anonymous workflow class cannot start: give it a constant name" unless klass.name

        first = klass.workflow_steps.first
        raise ArgumentError, "#{klass.name} declares no steps" unless first

        now = Time.current
        { workflow_id: SecureRandom.uuid, workflow_class: klass.name, status: "ready", current_step: first.name,
          step_job_options: klass.step_job_options.transform_keys(&:to_s), idempotency_key: idempotency_key&.to_s,
          definition_digest: klass.definition_digest, rerun_of_workflow_id: rerun_of, created_at: now, updated_at: now }
      end

      # The run row and its first step row in one transaction (a savepoint
      # inside an app transaction, so a unique violation leaves the app's
      # transaction usable). The first StepJob goes out when it commits.
      def insert_run(klass, attrs, actor:, reason: nil)
        record, duplicate = WorkflowEntry.transaction(requires_new: true) do
          next [existing_run(klass, attrs), true] unless WorkflowEntry.insert_run!(attrs)

          record = WorkflowEntry.find_by!(workflow_id: attrs[:workflow_id])
          event!(record, "started", actor: actor, reason: reason, details: { rerun_of: attrs[:rerun_of_workflow_id] }.compact)
          first = klass.workflow_steps.first
          schedule!(record, first.name, at: Time.current + first.wait, actor: actor)
          [record, false]
        end
        klass.new(record, duplicate_start: duplicate)
      rescue ActiveRecord::RecordNotUnique
        # ON CONFLICT only absorbs the idempotency index. Two concurrent
        # starts with the same key and subject can instead trip the
        # one-ongoing index first; by then the winner has committed, so its
        # key is visible: that is still a duplicate start, not a second run.
        existing = attrs[:idempotency_key] && existing_run(klass, attrs, required: false)
        return klass.new(existing, duplicate_start: true) if existing

        holder = WorkflowEntry.ongoing.find_by(workflow_class: klass.name, subject_type: attrs[:subject_type],
                                               subject_id: attrs[:subject_id])
        raise AlreadyRunning.new("#{klass.name} already has an ongoing run for #{attrs[:subject_type]} " \
                                 "#{attrs[:subject_id]} (#{holder&.workflow_id})", workflow: holder && klass.new(holder))
      end

      def existing_run(klass, attrs, required: true)
        scope = WorkflowEntry.where(workflow_class: klass.name, idempotency_key: attrs[:idempotency_key])
        required ? scope.take! : scope.take
      end

      # --- outside verbs ------------------------------------------------

      # pause!, cancel!, resume!, resume_at!, run_now!, reschedule! on a run.
      # Returns the updated row; raises the States table's error when the
      # verb is not legal in the run's state.
      def apply(workflow, verb, actor:, reason: nil, **options)
        locked(workflow.workflow_id) do |record, notes|
          raise ActiveRecord::RecordNotFound, "workflow #{workflow.workflow_id} is gone" unless record

          States.check!(verb, record.status)
          OutsideVerbs.public_send(verb, record, notes, actor: actor, reason: reason, **options)
          record
        end
      end

      # --- shared helpers -----------------------------------------------

      # Runs the block in a transaction with the run's row locked (nil when
      # the run is gone), then sends what the block noted.
      def locked(workflow_id)
        notes = Notes.new
        result = WorkflowEntry.transaction { yield WorkflowEntry.lock_for(workflow_id), notes }
        notes.flush
        result
      end

      # The guarded UPDATE for +verb+: WHERE status IN (States.from(verb)).
      # Under the row lock it cannot lose once the caller checked the state;
      # 0 rows raises rather than letting a transition pass silently.
      def transition!(record, verb, **attrs)
        to = States.to(verb)
        updated = WorkflowEntry.transition!(record.workflow_id, from: States.from(verb), to: to, **attrs)
        raise InvalidTransition, "#{verb} found workflow #{record.workflow_id} outside #{States.from(verb).join("/")}" if updated.zero?

        record.assign_attributes(attrs.merge(status: to))
        to
      end

      def event!(record, event, actor:, reason: nil, step: nil, details: {})
        WorkflowEvent.record!(workflow_id: record.workflow_id, event: event, actor: actor, reason: reason, step: step,
                              details: details)
      end

      # A new attempt row of +step_name+ (its after-commit callback sends the
      # StepJob), with its step_scheduled event. A job_options wait:/wait_until:
      # (class or step) is folded into scheduled_at here, once, so the sweep
      # does not take a deliberately delayed step for a lost one and
      # run_now!/reschedule! move the one time that counts.
      def schedule!(record, step_name, at:, actor:) # rubocop:disable Naming/MethodParameterName
        at = job_delay_floor(record, step_name, at)
        step = WorkflowStep.insert_next!(workflow_id: record.workflow_id, workflow_class: record.workflow_class,
                                         step_name: step_name, scheduled_at: at)
        event!(record, "step_scheduled", actor: actor, step: step, details: { scheduled_at: at.utc.iso8601(3) })
        step
      end

      def job_delay_floor(record, step_name, at) # rubocop:disable Naming/MethodParameterName
        options = record.workflow_klass&.step_job_options_for(step_name) || {}
        [at, options[:wait] && (Time.current + options[:wait]), options[:wait_until]].compact.max
      end

      # "hostname:pid", stamped on every attempt a worker claims.
      def worker_tag = "#{Socket.gethostname}:#{::Process.pid}"

      def payload_for(record, **extra)
        { workflow_class: record.workflow_class, workflow_id: record.workflow_id }.merge(extra)
      end
    end
  end
end
