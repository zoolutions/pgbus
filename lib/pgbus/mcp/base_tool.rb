# frozen_string_literal: true

require "json"

module Pgbus
  module MCP
    # Base class for every pgbus diagnostic tool. Provides the shared
    # read-only annotation, a JSON response helper, and access to the
    # DataSource the tool delegates to.
    #
    # The DataSource is pulled from +server_context[:data_source]+ so the
    # server can inject a configured instance (and tests can inject a double).
    # Every subclass is read-only by contract — see issue #180 security
    # requirements. Write/admin tools, if ever added, must live in a separate,
    # explicitly opt-in surface and never inherit from this class.
    class BaseTool < ::MCP::Tool
      # Marks the tool read-only and non-destructive so MCP clients can show
      # the right affordance and never treat a call as a mutation.
      annotations(
        read_only_hint: true,
        destructive_hint: false,
        idempotent_hint: true,
        open_world_hint: false
      )

      class << self
        # MCP::Tool.inherited resets @annotations_value to nil on every
        # subclass, so the read_only_hint set on BaseTool would not reach the
        # concrete tools. Fall back to the nearest ancestor that defined
        # annotations so all tools inherit the read-only contract without
        # repeating it. (annotations_value is what MCP::Tool#to_h reads.)
        def annotations_value
          super || (superclass.respond_to?(:annotations_value) ? superclass.annotations_value : nil)
        end

        # Pull the injected DataSource (or build a default one). Kept as a
        # class method because MCP tool entry points (`self.call`) are class
        # methods.
        # The server injects ONE DataSource for the life of the process, so its
        # per-request memos have to be dropped per tool call — otherwise every
        # call after the first replays the first call's snapshot.
        def data_source_from(server_context)
          data_source = (server_context && server_context[:data_source]) || Pgbus::Web::DataSource.new
          data_source.reset_cache! if data_source.respond_to?(:reset_cache!)
          data_source
        end

        # Whether payloads may be returned for this call. Honors a per-call
        # `include_payloads` argument only when the server was started with
        # payloads globally allowed (`server_context[:allow_payloads]`).
        # Defaults to false on both axes so nothing leaks by accident.
        def payloads_allowed?(server_context, include_payloads)
          return false unless server_context && server_context[:allow_payloads]

          !!include_payloads
        end

        # Wrap any Ruby value as a single text-content JSON response, applying
        # payload redaction at this boundary as a fail-safe. Redaction is the
        # default: a tool returns metadata, and any payload-bearing key (at any
        # depth) is stripped unless this call is explicitly allowed to include
        # payloads. A tool author who forgets about redaction therefore cannot
        # leak message bodies — they have to opt in.
        #
        # Pass +server_context+ and the tool's per-call +include_payloads+ flag
        # to enable payloads; both gates must be open (see #payloads_allowed?).
        def json_response(value, server_context: nil, include_payloads: false)
          allow = payloads_allowed?(server_context, include_payloads)
          redacted = Redactor.deep_redact(value, include_payloads: allow)
          ::MCP::Tool::Response.new([{ type: "text", text: JSON.generate(redacted) }])
        end

        # Wrap an error message as an MCP error response (isError: true) so the
        # client surfaces it as a tool failure rather than a normal result.
        def error_response(message)
          ::MCP::Tool::Response.new(
            [{ type: "text", text: message }],
            error: true
          )
        end
      end
    end
  end
end
