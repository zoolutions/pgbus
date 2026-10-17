# frozen_string_literal: true

module Pgbus
  class Workflow
    # A removed_step gravestone: a step that existed once. A scheduled row
    # still naming it is skipped and the workflow moves on to the next step
    # declared after it.
    RemovedStep = Data.define(:name) do
      def kind = "removed"

      def removed? = true
    end
  end
end
