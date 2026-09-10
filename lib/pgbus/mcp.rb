# frozen_string_literal: true

module Pgbus
  # Optional read-only MCP (Model Context Protocol) diagnostic server.
  #
  # This namespace is deliberately kept out of pgbus's Zeitwerk loader because
  # its tool classes subclass `MCP::Tool` from the optional `mcp` gem.
  # Referencing that gem at autoload time would force every pgbus user to
  # install it. Instead the subsystem is loaded explicitly via {load!}, which
  # is called by the `pgbus mcp` CLI command (and by specs).
  module MCP
    # Files in dependency order: the gem first, then the base classes the
    # tools depend on, then the tools, then the server/runner.
    REQUIRES = %w[
      pgbus/mcp/redactor
      pgbus/mcp/base_tool
      pgbus/mcp/health_analyzer
      pgbus/mcp/tools/health_tool
      pgbus/mcp/tools/queues_tool
      pgbus/mcp/tools/queue_detail_tool
      pgbus/mcp/tools/processes_tool
      pgbus/mcp/tools/jobs_tool
      pgbus/mcp/tools/job_detail_tool
      pgbus/mcp/tools/dlq_tool
      pgbus/mcp/tools/dlq_detail_tool
      pgbus/mcp/tools/locks_tool
      pgbus/mcp/tools/concurrency_tool
      pgbus/mcp/tools/throughput_tool
      pgbus/mcp/tools/stats_tool
      pgbus/mcp/tools/recurring_tool
      pgbus/mcp/server
      pgbus/mcp/runner
      pgbus/mcp/rack_app
    ].freeze

    module_function

    # Require the `mcp` gem and the whole pgbus MCP subsystem. Idempotent.
    # Raises Pgbus::Error with an actionable message if the gem is missing.
    #
    # Only the `require "mcp"` is wrapped — a LoadError from an internal
    # `pgbus/mcp/...` path (typo, missing file) propagates verbatim so the
    # real broken require shows up in the message instead of being masked
    # as "add gem `mcp`".
    def load!
      begin
        require "mcp"
      rescue LoadError
        raise Pgbus::Error,
              "The pgbus MCP server requires the `mcp` gem. Add `gem \"mcp\"` to your Gemfile and run `bundle install`."
      end

      REQUIRES.each { |path| require path }
    end
  end
end
