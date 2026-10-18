# frozen_string_literal: true

require "json"

module Pgbus
  module MCP
    module Tools
      # Delivers a signal to an ongoing workflow run (issue #550).
      # DataSource#signal_workflow.
      class WorkflowSignalTool < WriteTool
        # Both columns are indexed; an index entry has a size limit.
        KEY_LIMIT = 255

        tool_name "pgbus_workflow_signal"
        title "Pgbus Workflow: Signal"
        description verb_description(
          :signal, "Send a named signal, with an optional JSON object payload, to an ongoing Pgbus::Workflow run. " \
                   "Pass an idempotency_key when the call may be retried: a redelivery with the same key stores " \
                   "nothing and answers duplicate: true, but only while the run is ongoing (once it finished or " \
                   "was cancelled, a retry answers ok: false: signal is not allowed)."
        )
        input_schema(
          **workflow_input_schema(
            properties: {
              name: { type: "string", minLength: 1, maxLength: KEY_LIMIT,
                      description: "The signal's name, e.g. payment_confirmed." },
              payload: {
                type: "object",
                description: "The signal's payload, a JSON object of at most workflow_max_payload_size bytes."
              },
              idempotency_key: { type: "string", maxLength: KEY_LIMIT,
                                 description: "Makes a retried call deliver the signal once. Blank means none." }
            },
            required: %w[name]
          )
        )

        def self.call(workflow_id:, reason:, name:, payload: nil, idempotency_key: nil, expected_status: nil,
                      server_context: nil)
          name = name.to_s.strip
          idempotency_key = idempotency_key.to_s.presence # blank is no key, as in Signals.deliver
          payload = payload_object(payload)
          perform(:signal, workflow_id: workflow_id, reason: reason, expected_status: expected_status,
                           server_context: server_context,
                           invalid: signal_refusal(name, idempotency_key, payload)) do |data_source, **who|
            data_source.signal_workflow(workflow_id, name: name, payload: payload, idempotency_key: idempotency_key, **who)
          end
        end

        # Why the signal cannot be sent as given, or nil.
        def self.signal_refusal(name, idempotency_key, payload)
          return "name must name a signal" if name.empty?
          return "name must be at most #{KEY_LIMIT} characters" if name.length > KEY_LIMIT
          return "idempotency_key must be at most #{KEY_LIMIT} characters" if idempotency_key && idempotency_key.length > KEY_LIMIT

          payload if payload.is_a?(String)
        end
        private_class_method :signal_refusal

        # The payload with string keys (as the dashboard's form sends it), or
        # why it cannot be sent. Workflow::Payload enforces the cap again on
        # the serialized form.
        def self.payload_object(payload)
          return {} if payload.nil?
          return "payload must be a JSON object" unless payload.is_a?(Hash)

          json = JSON.generate(payload)
          limit = Pgbus.configuration.workflow_max_payload_size
          return "payload is #{json.bytesize} bytes, over workflow_max_payload_size (#{limit})" if json.bytesize > limit

          JSON.parse(json)
        end
        private_class_method :payload_object
      end
    end
  end
end
