# frozen_string_literal: true

module Pgbus
  module MCP
    module Tools
      # Inspect a single dead-letter message. Maps to DataSource#job_detail
      # against the named DLQ queue when +queue+ is supplied; falls back to a
      # cross-DLQ scan via DataSource#dlq_message_detail when it is not.
      # Payload redacted by default.
      class DlqDetailTool < BaseTool
        tool_name "pgbus_dlq_detail"
        title "Pgbus Dead-Letter Detail"
        description <<~DESC
          Inspect one dead-letter message: read_ct, vt, enqueued_at, source
          queue, and a dead_letter summary of why it died. Pass the full physical DLQ queue name (e.g.
          "pgbus_default_dlq") in +queue+ to disambiguate — msg_ids are only
          unique within a single DLQ table, so the same id can exist in more
          than one. Without +queue+ the tool scans every "_dlq" table and
          returns the FIRST match, which is ambiguous when multiple DLQs
          contain the id and is reported as an error. The message body and
          headers are redacted unless the server allows payloads and
          include_payloads is set.
        DESC

        input_schema(
          properties: {
            queue: {
              type: "string",
              description: "Full physical DLQ queue name (e.g. \"pgbus_default_dlq\"). Strongly recommended."
            },
            msg_id: { type: "integer", description: "Numeric PGMQ message id." },
            include_payloads: {
              type: "boolean",
              description: "Include the raw message body/headers. Only honored when the server allows payloads."
            }
          },
          required: %w[msg_id]
        )

        def self.call(msg_id:, queue: nil, include_payloads: false, server_context: nil)
          data_source = data_source_from(server_context)
          detail =
            if queue
              data_source.job_detail(queue, msg_id)
            else
              ambiguity_check(data_source, msg_id) || data_source.dlq_message_detail(msg_id)
            end
          return error_response("Dead-letter message #{msg_id} not found") unless detail
          return detail if detail.is_a?(::MCP::Tool::Response)

          summary = dead_letter_summary(detail, include_error: payloads_allowed?(server_context, include_payloads))
          json_response({ message: detail.merge(dead_letter: summary) },
                        server_context: server_context, include_payloads: include_payloads)
        end

        # When the caller didn't specify a queue, scan every DLQ for the id and
        # refuse to guess if more than one matches. Returns an error response
        # on ambiguity, nil otherwise (so the caller proceeds with the normal
        # first-match lookup).
        def self.ambiguity_check(data_source, msg_id)
          dlq_suffix = Pgbus::DEAD_LETTER_SUFFIX
          dlqs = data_source.queues_with_metrics.select { |q| q[:name].to_s.end_with?(dlq_suffix) }
          matches = dlqs.map { |q| q[:name] }.select { |name| data_source.job_detail(name, msg_id) }
          return nil if matches.size <= 1

          error_response(
            "Dead-letter message #{msg_id} is ambiguous — present in #{matches.size} DLQs " \
            "(#{matches.join(", ")}). Pass `queue:` to disambiguate."
          )
        end
      end
    end
  end
end
