# frozen_string_literal: true

module Pgbus
  module Web
    # Workflow arguments, step outputs and signal payloads as the Workflows
    # dashboard and its MCP tools show them (issue #542). They are stored
    # through ActiveJob::Arguments; this reads that stored form without
    # deserializing it, so showing a run never loads its records (a GlobalID
    # shows as its URI) and never runs a custom serializer.
    module WorkflowPayload
      BOOKKEEPING = %w[_aj_symbol_keys _aj_ruby2_keywords _aj_hash_with_indifferent_access].freeze

      module_function

      def display(value)
        case value
        when nil then {}
        when Hash then display_hash(value)
        when Array then value.map { |element| display(element) }
        else value
        end
      end

      # display, then the dashboard's sensitive-key redaction (PayloadFilter).
      def filtered(value) = PayloadFilter.filter(display(value))

      def display_hash(hash)
        return hash["_aj_globalid"] if hash.size == 1 && hash.key?("_aj_globalid")
        return display(hash["value"]) if hash.key?("_aj_serialized")

        hash.except(*BOOKKEEPING).transform_values { |nested| nested.nil? ? nil : display(nested) }
      end
    end
  end
end
