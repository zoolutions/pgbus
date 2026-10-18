# frozen_string_literal: true

require "active_support"
require "active_support/core_ext/class/attribute"
require "active_support/core_ext/hash/indifferent_access"

module Pgbus
  # A durable multi-step workflow (issue #540). Subclass it, declare steps,
  # start a run:
  #
  #   class SignupWorkflow < Pgbus::Workflow
  #     cancel_if { subject.nil? || subject.closed? }
  #     on_exception Net::OpenTimeout, action: :reattempt!, wait: 15.seconds, max_reattempts: 5
  #
  #     step :welcome do
  #       WelcomeMailer.with(user: subject).welcome.deliver_later
  #     end
  #
  #     step :reminder, wait: 2.days, skip_if: -> { subject.onboarded? } do
  #       ReminderMailer.with(user: subject).remind.deliver_later
  #     end
  #   end
  #
  #   wf = SignupWorkflow.start!(user, plan: "pro")
  #   wf.pause!(actor: "alice", reason: "SMTP down") / wf.resume! / wf.cancel!
  #
  # Each step attempt is a pgbus_workflow_steps row and one
  # Pgbus::Workflow::StepJob message, sent after the row commits. The step
  # body runs on a fresh instance of the class, with +subject+ and
  # +arguments+. Pgbus::WorkflowEntry is the run's row; this class is the
  # DSL and the handle.
  class Workflow
    class Error < Pgbus::Error; end

    # A second start! for a subject that already has an ongoing run of this
    # class. #workflow is the run that holds the slot.
    class AlreadyRunning < Error
      attr_reader :workflow

      def initialize(message = nil, workflow: nil)
        super(message)
        @workflow = workflow
      end
    end

    # A transition the States table does not allow from the workflow's state.
    class InvalidTransition < Error; end

    # resume!/resume_at! on a workflow that is not paused.
    class NotPaused < InvalidTransition; end

    # A row names a step the class does not define (and no removed_step
    # gravestone covers it), or resume_at! names an unknown step.
    class StepNotDefined < Error; end

    # Recorded on a step whose worker died or that outlived its stall_at.
    class StepStalled < Error; end

    # Arguments or a step output over config.workflow_max_payload_size.
    class PayloadTooLarge < Error; end

    # A step-only verb or reader (reattempt!, finished!, output,
    # step_idempotency_key, received_signal) called outside a step body.
    class NotInStep < Error; end

    # signal! on a finished or cancelled run, or by subject when the subject
    # has no ongoing run of the class (missing: :raise).
    class NotOngoing < InvalidTransition; end

    # Recorded on a fan_out step whose batch had dead-lettered members.
    class FanOutFailed < Error; end

    # Recorded on a waiting step whose wait_timeout: passed with on_timeout: :pause!.
    class WaitTimedOut < Error; end

    # start! of a class with a fan_out step before the batch callback-job
    # columns exist (rails generate pgbus:add_batch_callback_jobs).
    class BatchCallbacksUnmigrated < Error; end

    # start! of a class with a fan_out or wait_for: step before the
    # waiting-step schema exists (rails generate pgbus:add_workflow_signals).
    class WaitingStepsUnmigrated < Error; end

    # speedrun_workflow reached a step that waits for a signal or a batch.
    class StepWaiting < Error; end

    extend DSL
    include FlowControl
    include Instance

    # Class-level declarations: frozen values replaced whole by the DSL, set
    # while the class body loads and only read afterwards.
    # rubocop:disable ThreadSafety/ClassAndModuleAttributes -- written only by the class body at load, each a frozen value replaced whole
    class_attribute :_workflow_steps, instance_accessor: false, instance_predicate: false, default: Steps::EMPTY
    class_attribute :_cancel_conditions, instance_accessor: false, instance_predicate: false, default: [].freeze
    class_attribute :_exception_policies, instance_accessor: false, instance_predicate: false, default: [].freeze
    class_attribute :_step_job_options, instance_accessor: false, instance_predicate: false, default: {}.freeze
    # rubocop:enable ThreadSafety/ClassAndModuleAttributes

    class << self
      def inherited(subclass)
        super
        Registry.register(subclass)
      end

      # Every subclass that has a name, once (what the insights page lists).
      def registered_classes = Registry.classes

      # Start a run: the workflow row and its first step row, in one
      # transaction; the first StepJob is sent once that commits.
      #
      #   SignupWorkflow.start!(user, plan: "pro")    # arguments["plan"]
      #   CheckoutWorkflow.start!(order, idempotency_key: "checkout-#{order.id}")
      #   ReportWorkflow.start!(actor: "alice", reason: "monthly close")
      #
      # +idempotency_key+, +actor+ and +reason+ are reserved and never reach
      # the arguments. With an idempotency_key, a second start! with the same
      # key returns the first run (any status) with duplicate_start? true and
      # records nothing. +actor+ (default "app") and +reason+ go on the
      # started event, and the actor on the first step_scheduled, as on every
      # outside verb. Raises AlreadyRunning when the subject already has an
      # ongoing run.
      def start!(subject = nil, idempotency_key: nil, actor: nil, reason: nil, **arguments)
        Runner.start(self, subject: subject, arguments: arguments, idempotency_key: idempotency_key,
                           actor: actor || Runner::DEFAULT_ACTOR, reason: reason)
      end

      # The run with this workflow_id, as an instance of the class that
      # started it; nil when there is none, or when that class is not this
      # one or a subclass of it. Pgbus::Workflow.find finds any run (as a
      # bare Pgbus::Workflow when its class no longer exists, so it can still
      # be cancelled).
      def find(workflow_id)
        record = WorkflowEntry.find_by(workflow_id: workflow_id)
        return nil unless record

        klass = record.workflow_klass || Workflow
        klass <= self ? klass.new(record) : nil
      end

      # Signal the ongoing run of this class for +subject+ (a webhook that
      # knows the order, not the run):
      #
      #   OrderWorkflow.signal!(order, :payment_confirmed, payload: { amount: 42 }, idempotency_key: event.id,
      #                         missing: :ignore)
      #
      # The one-ongoing-per-subject index means at most one run matches. With
      # none (never started, or already over), missing: :raise raises
      # NotOngoing and missing: :ignore returns nil. A subject-less run is
      # signalled through its handle: find(workflow_id).signal!.
      def signal!(subject, name, payload: {}, idempotency_key: nil, missing: :raise, actor: nil, reason: nil)
        raise ArgumentError, "missing: must be :raise or :ignore, got #{missing.inspect}" unless %i[raise ignore].include?(missing)
        if subject.nil?
          raise ArgumentError, "#{self.name}.signal! needs a subject; signal a subject-less run through its handle " \
                               "(#{self.name}.find(workflow_id).signal!)"
        end

        attrs = Payload.subject_attributes(subject)
        record = WorkflowEntry.ongoing.find_by(workflow_class: self.name, subject_type: attrs[:subject_type],
                                               subject_id: attrs[:subject_id])
        raise NotOngoing, "#{self.name} has no ongoing run for #{attrs[:subject_type]} #{attrs[:subject_id]}" unless record

        new(record).signal!(name, payload: payload, idempotency_key: idempotency_key, actor: actor, reason: reason)
      rescue NotOngoing
        raise if missing == :raise

        nil
      end

      # How many ongoing runs still sit on each removed step:
      # { "reminder" => 3 }. A gravestone can go once its count is zero.
      def removed_steps_in_flight
        names = _workflow_steps.removed_names
        return {} if names.empty?

        counts = WorkflowEntry.ongoing.where(workflow_class: name, current_step: names).group(:current_step).count
        names.to_h { |step_name| [step_name, counts.fetch(step_name, 0)] }
      end
    end

    attr_reader :record

    def initialize(record, duplicate_start: false)
      @record = record
      @duplicate_start = duplicate_start
      @step_context = nil
    end
  end
end
