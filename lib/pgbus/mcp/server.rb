# frozen_string_literal: true

module Pgbus
  module MCP
    # Assembles the read-only pgbus diagnostic MCP server: registers every
    # diagnostic tool and wires a DataSource (plus the payload gate) into the
    # server context so tools never touch the database directly or leak
    # payloads by default. The workflow write tools (WRITE_TOOLS, issue #550)
    # join only when config.mcp_write_tools is true when the server is built.
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
        Tools::RecurringTool,
        Tools::DeployTasksTool,
        Tools::WorkflowsTool,
        Tools::WorkflowDetailTool,
        Tools::WorkflowsAttentionTool,
        Tools::WorkflowDefinitionsTool,
        Tools::WorkflowStatsTool
      ].freeze

      # The opt-in write surface (issue #180's rule: separate, explicitly
      # opt-in, off by default). Never merged into TOOLS: with
      # config.mcp_write_tools false these are not constructed at all.
      WRITE_TOOLS = [
        Tools::WorkflowResumeTool,
        Tools::WorkflowCancelTool,
        Tools::WorkflowSkipStepTool,
        Tools::WorkflowResumeAtTool,
        Tools::WorkflowRunNowTool,
        Tools::WorkflowSignalTool
      ].freeze

      INSTRUCTIONS = <<~TEXT
        Read-only diagnostic tools for a pgbus (PostgreSQL/PGMQ) deployment.
        Start with pgbus_health for a one-call OK/DEGRADED/STALLED verdict,
        then drill in with pgbus_queues, pgbus_processes, pgbus_jobs,
        pgbus_dlq, pgbus_locks, and pgbus_concurrency; workflow runs with
        pgbus_workflows_attention, pgbus_workflows and pgbus_workflow, and how
        each workflow class performs with pgbus_workflow_definitions and
        pgbus_workflow_stats. No tool mutates state. Message payloads
        are redacted unless the server was started with payloads explicitly
        allowed.
      TEXT

      WRITE_INSTRUCTIONS = <<~TEXT
        The exception: this server was built with config.mcp_write_tools, so
        pgbus_workflow_resume, pgbus_workflow_cancel, pgbus_workflow_skip_step,
        pgbus_workflow_resume_at, pgbus_workflow_run_now and
        pgbus_workflow_signal change one workflow run each. Read the run with
        pgbus_workflow first, then pass a reason (recorded on the run's audit
        trail) and expected_status (the status you read), so a stale read
        answers ok: false instead of acting. There are no bulk write tools:
        loop over pgbus_workflows_attention results, one call per run.
      TEXT

      module_function

      # Build an MCP::Server with all diagnostic tools registered, plus the
      # write tools when config.mcp_write_tools is true (read here, once).
      #
      # @param data_source [Pgbus::Web::DataSource] read layer the tools query.
      # @param allow_payloads [Boolean] when true, tools honor a per-call
      #   include_payloads flag; otherwise payloads are always redacted.
      def build(data_source: Pgbus::Web::DataSource.new, allow_payloads: false)
        write = write_tools?
        ::MCP::Server.new(
          name: "pgbus",
          title: "Pgbus Diagnostics",
          version: Pgbus::VERSION,
          instructions: write ? INSTRUCTIONS + WRITE_INSTRUCTIONS : INSTRUCTIONS,
          tools: write ? TOOLS + WRITE_TOOLS : TOOLS.dup,
          server_context: {
            data_source: data_source,
            allow_payloads: allow_payloads
          }
        )
      end

      # Only a literal true turns the write tools on (validate! rejects
      # anything else, but an app may skip validation).
      def write_tools? = Pgbus.configuration.mcp_write_tools == true
    end
  end
end
