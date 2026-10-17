# frozen_string_literal: true

module Pgbus
  class Workflow
    # The class-body DSL. Every declaration builds a new frozen value and
    # stores it whole in a class_attribute, so a subclass extends what its
    # parent declared without changing the parent, and nothing is ever
    # mutated after the class body ran (docs/concurrency.md).
    module DSL
      # step :name, wait:, skip_if:, timeout:, job_options:, on_exception:,
      #      before_step:, after_step: do ... end
      # step { ... }              # anonymous: step_1, step_2, ...
      # step :name                # runs the instance method `name`
      # step def name = ...       # the same, declared in one line
      def step(name = nil, before_step: nil, after_step: nil, **options, &block)
        raise ArgumentError, "a step needs a name or a block" if name.nil? && block.nil?

        reject_unknown_options!(options)
        name ||= _workflow_steps.anonymous_name
        definition = StepDefinition.new(name: name, body: block || name.to_sym, **options)
        self._workflow_steps = _workflow_steps.add(definition, before: before_step, after: after_step)
        definition.name.to_sym
      end

      # fan_out :name, on_batch_failure: :pause!, **step_options do |b| ... end
      #
      # A step that adds a group of jobs with b.add and waits for them as one
      # batch: the step parks as waiting, and the batch's on_complete resumes
      # the run once every member finished. Any dead-lettered member applies
      # on_batch_failure (:pause!, :cancel! or :skip!) with
      # Pgbus::Workflow::FanOutFailed recorded. Takes every step option but
      # wait_for:, wait_timeout: and on_timeout:. Without a block it runs the
      # instance method of the same name, which gets the builder.
      def fan_out(name = nil, on_batch_failure: :pause!, before_step: nil, after_step: nil, **options, &block)
        raise ArgumentError, "a fan_out needs a name or a block" if name.nil? && block.nil?

        name ||= _workflow_steps.anonymous_name
        waits = options.keys & %i[wait_for wait_timeout on_timeout]
        if waits.any?
          raise ArgumentError, "fan_out #{name.to_s.to_sym.inspect} cannot take #{waits.first}: it waits for its batch, " \
                               "not for a signal"
        end

        reject_unknown_options!(options, kind: "fan_out", accepted: FanOutDefinition::OPTIONS)
        definition = FanOutDefinition.new(name: name, body: block || name.to_sym, on_batch_failure: on_batch_failure, **options)
        self._workflow_steps = _workflow_steps.add(definition, before: before_step, after: after_step)
        definition.name.to_sym
      end

      # A step that no longer exists. Declare it where the step used to be: a
      # run still scheduled on it skips it and moves on to the step after.
      def removed_step(*names)
        names.each { |name| self._workflow_steps = _workflow_steps.remove(name) }
      end

      # Runs before every step under the workflow lock; truthy cancels the
      # workflow. A block or a method name; several are OR-ed.
      def cancel_if(method_name = nil, &block)
        condition = block || method_name
        raise ArgumentError, "cancel_if needs a block or a method name" unless condition.is_a?(Proc) || condition.is_a?(Symbol)

        self._cancel_conditions = [*_cancel_conditions, condition].freeze
      end

      # A class-level exception policy (see ExceptionPolicy).
      def on_exception(*matching, action: nil, **, &)
        policy = ExceptionPolicy.new(action, matching: matching.flatten, **, &)
        self._exception_policies = [*_exception_policies, policy].freeze
      end

      # StepJob options for every step of this class: queue, priority, wait,
      # wait_until. A step's own job_options: win over these.
      def set_step_job_options(**options)
        self._step_job_options = _step_job_options.merge(StepDefinition.validate_job_options!(options)).freeze
      end

      def workflow_steps = _workflow_steps

      def cancel_conditions = _cancel_conditions

      def exception_policies = _exception_policies

      def step_job_options = _step_job_options

      # SHA256 of the ordered [name, kind] step list. Stored on every run at
      # start, so a run that began on an older definition can be told apart.
      def definition_digest = Registry.digest(_workflow_steps)

      # The StepJob options for one step: the class's, then the step's own.
      def step_job_options_for(step_name)
        own = _workflow_steps.fetch(step_name)&.job_options || {}
        _step_job_options.merge(own)
      end

      private

      def reject_unknown_options!(options, kind: "step", accepted: StepDefinition::OPTIONS)
        unknown = options.keys - accepted
        return if unknown.empty?

        raise ArgumentError, "unknown #{kind} option #{unknown.first.inspect} (accepted: #{accepted.join(", ")})"
      end
    end
  end
end
