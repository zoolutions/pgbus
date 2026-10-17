# frozen_string_literal: true

module Pgbus
  module MCP
    # Base class for the opt-in write tools (issue #550). Deliberately not a
    # BaseTool subclass: no write tool can inherit the read-only annotations,
    # and no read tool can become a write tool by changing its superclass.
    # Server registers these only when config.mcp_write_tools is true.
    #
    # Every call (#perform): a reason (1-500 characters after trimming), the
    # run loaded (unknown: ok: false), expected_status checked when given, the
    # verb checked against the run's available_actions, then the same
    # Web::DataSource action the dashboard uses, recorded as actor "mcp" (or
    # "mcp:<client name>"). The answer is { ok:, reason:, workflow: } with the
    # run after the action, payloads always redacted.
    #
    # The two checks read the run before acting, so they are advisory: the
    # action's guarded transition under the run's row lock decides, and its
    # code (:not_allowed, with the status the run is in now) is passed on.
    class WriteTool < ::MCP::Tool
      ANNOTATIONS = { read_only_hint: false, destructive_hint: true, idempotent_hint: false, open_world_hint: false }.freeze
      REASON_LIMIT = 500
      CLIENT_NAME_LIMIT = 100
      ACTOR = "mcp"

      annotations(**ANNOTATIONS)

      class << self
        # MCP::Tool.inherited resets @annotations_value on every subclass, so
        # fall back to the nearest ancestor's (as BaseTool does).
        def annotations_value
          super || (superclass.respond_to?(:annotations_value) ? superclass.annotations_value : nil)
        end

        # For a verb whose second call changes nothing more (cancel, skip, resume at).
        def idempotent!
          annotations(**ANNOTATIONS, idempotent_hint: true)
        end

        # The input_schema every write tool takes, plus the tool's own fields.
        def workflow_input_schema(properties: {}, required: [])
          {
            properties: {
              workflow_id: { type: "string", description: "The run's workflow_id (a UUID)." },
              reason: { type: "string", minLength: 1, maxLength: REASON_LIMIT,
                        description: "Why, in 1-#{REASON_LIMIT} characters. Recorded on the run's audit trail." },
              expected_status: {
                type: "string", enum: Workflow::States::WORKFLOW,
                description: "The status you read. If the run is in another one when the call starts: ok: false, nothing done."
              },
              **properties
            },
            required: %w[workflow_id reason] + required
          }
        end

        # The tool description: what it does, the transition it makes, and the
        # opt-in. Frozen: it is class state (the conformance audit checks).
        def verb_description(verb, summary)
          transition = Workflow::States.fetch(verb)
          <<~DESC.freeze
            #{summary} Allowed from: #{transition.from.join(", ")}. What happens: #{transition.effect}. A write tool: it exists only when the server was built with config.mcp_write_tools = true. reason is required and recorded on the run's audit trail with actor "mcp"; pass expected_status (the status you read with pgbus_workflow): a run that has moved on by the time the call starts answers ok: false and nothing is done (the check runs just before the action, whose transition is guarded again under the run's row lock). Answers { ok, reason, workflow } with the run after the action (code and status when it was refused); arguments, outputs, signal payloads and error text are redacted.
          DESC
        end

        # Runs the checks, then yields (data_source, actor:, reason:) for the
        # DataSource action, whose { ok:, ... } becomes the answer.
        def perform(verb, workflow_id:, reason:, expected_status:, server_context:)
          note = reason.to_s.strip
          unless note.length.between?(1, REASON_LIMIT)
            return answer(ok: false, reason: "reason must be 1-#{REASON_LIMIT} characters after trimming whitespace")
          end

          data_source = BaseTool.data_source_from(server_context)
          detail = data_source.workflow_detail(workflow_id)
          return answer(ok: false, reason: "workflow #{workflow_id} not found", workflow: nil) unless detail

          refusal = guard_refusal(verb, detail, expected_status)
          return answer(**refusal, workflow: present(detail)) if refusal

          result = yield(data_source, actor: actor(server_context), reason: note)
          answer(ok: result[:ok], reason: result[:reason], **result, workflow: present(data_source.workflow_detail(workflow_id)))
        end

        def answer(**value) = BaseTool.json_response(value)

        private

        def guard_refusal(verb, detail, expected_status)
          status = detail[:status].to_s
          if expected_status && expected_status.to_s != status
            return { ok: false, reason: "workflow #{detail[:workflow_id]} is #{status}, not #{expected_status}" }
          end

          allowed = Array(detail[:available_actions]).map(&:to_s)
          return if allowed.include?(verb.to_s)

          { ok: false, reason: "cannot #{verb.to_s.tr("_", " ")} a #{status} workflow", available_actions: allowed }
        end

        def present(detail) = detail && WorkflowPayloads.detail(detail, allowed: false)

        # "mcp", or "mcp:<name>" when the transport knows the client (an
        # MCP::ServerContext on mcp >= 1.x; a plain Hash context has none).
        # The name is client-supplied: control characters dropped, length capped.
        def actor(server_context)
          info = server_context.client_info if server_context.respond_to?(:client_info)
          name = info.is_a?(Hash) ? (info[:name] || info["name"]) : nil
          name = name.to_s.gsub(/[^[:print:]]/, "").strip[0, CLIENT_NAME_LIMIT]
          name.empty? ? ACTOR : "#{ACTOR}:#{name}"
        end
      end
    end
  end
end
