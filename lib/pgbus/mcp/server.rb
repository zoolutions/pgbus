# frozen_string_literal: true

module Pgbus
  module MCP
    # Assembles the read-only pgbus diagnostic MCP server: registers every
    # diagnostic tool and wires a DataSource (plus the payload gate) into the
    # server context so tools never touch the database directly or leak
    # payloads by default.
    #
    # This class only builds the +MCP::Server+ instance — transport is the
    # caller's concern (Runner drives stdio). Keeping them separate means the
    # same server can later be mounted over HTTP without changing tool code.
    module Server
      # The fixed, curated tool set. No arbitrary-SQL tool, no mutation tool —
      # adding one here would violate the issue #180 security contract.
      TOOLS = [
        Tools::HealthTool,
        Tools::QueuesTool,
        Tools::QueueDetailTool,
        Tools::ProcessesTool,
        Tools::JobsTool,
        Tools::JobDetailTool,
        Tools::DlqTool,
        Tools::DlqDetailTool,
        Tools::LocksTool,
        Tools::ConcurrencyTool,
        Tools::ThroughputTool,
        Tools::StatsTool,
        Tools::RecurringTool
      ].freeze

      INSTRUCTIONS = <<~TEXT
        Read-only diagnostic tools for a pgbus (PostgreSQL/PGMQ) deployment.
        Start with pgbus_health for a one-call OK/DEGRADED/STALLED verdict,
        then drill in with pgbus_queues, pgbus_processes, pgbus_jobs,
        pgbus_dlq, pgbus_locks, and pgbus_concurrency. No tool mutates state. Message payloads
        are redacted unless the server was started with payloads explicitly
        allowed.
      TEXT

      module_function

      # Build an MCP::Server with all diagnostic tools registered.
      #
      # @param data_source [Pgbus::Web::DataSource] read layer the tools query.
      # @param allow_payloads [Boolean] when true, tools honor a per-call
      #   include_payloads flag; otherwise payloads are always redacted.
      def build(data_source: Pgbus::Web::DataSource.new, allow_payloads: false)
        ::MCP::Server.new(
          name: "pgbus",
          title: "Pgbus Diagnostics",
          version: Pgbus::VERSION,
          instructions: INSTRUCTIONS,
          tools: TOOLS.dup,
          server_context: {
            data_source: data_source,
            allow_payloads: allow_payloads
          }
        )
      end
    end
  end
end
