# frozen_string_literal: true

module Pgbus
  class Workflow
    # A `fan_out` step (issue #541): a StepDefinition whose body adds jobs
    # to a builder instead of doing the work itself. FanOut runs the body,
    # opens the batch and parks the step on it.
    class FanOutDefinition < StepDefinition
      # What a batch with dead-lettered members does to the run.
      FAILURE_ACTIONS = %i[pause! cancel! skip!].freeze

      attr_reader :on_batch_failure

      def initialize(on_batch_failure: :pause!, **)
        @on_batch_failure = normalize_failure_action(on_batch_failure)
        super(**)
      end

      def kind = "fan_out"

      # Runs the body with a recording builder, opens the batch, and ends the
      # attempt with the :fan_out outcome (FanOut.run throws it).
      def invoke(workflow)
        FanOut.run(self, workflow)
      end

      private

      def normalize_failure_action(value)
        verb = value.nil? ? nil : :"#{value.to_s.delete_suffix("!")}!"
        return verb if FAILURE_ACTIONS.include?(verb)

        raise ArgumentError,
              "on_batch_failure: must be one of #{FAILURE_ACTIONS.map(&:inspect).join(", ")}, got #{value.inspect}"
      end
    end
  end
end
