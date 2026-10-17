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

    # A step-only verb or reader (reattempt!, skip!, finished!, output,
    # step_idempotency_key) called outside a step body.
    class NotInStep < Error; end

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
      #
      # +idempotency_key+ is reserved: with one, a second start! with the same
      # key returns the first run (any status) with duplicate_start? true.
      # Raises AlreadyRunning when the subject already has an ongoing run.
      def start!(subject = nil, idempotency_key: nil, **arguments)
        Runner.start(self, subject: subject, arguments: arguments, idempotency_key: idempotency_key)
      end

      # The run with this workflow_id, as an instance of this class (or of
      # the class that started it, when called on Pgbus::Workflow); nil when
      # there is none.
      def find(workflow_id)
        record = WorkflowEntry.find_by(workflow_id: workflow_id)
        return nil unless record

        klass = self == Workflow ? (record.workflow_klass || self) : self
        klass.new(record)
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
