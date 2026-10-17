# frozen_string_literal: true

require "json"
require_relative "spec_helper"

RSpec.describe Pgbus::MCP::Server do
  let(:data_source) { instance_double(Pgbus::Web::DataSource) }

  describe ".build" do
    subject(:server) { described_class.build(data_source: data_source) }

    it "names the server pgbus and stamps the gem version" do
      expect(server.name).to eq("pgbus")
      expect(server.version).to eq(Pgbus::VERSION)
    end

    it "registers exactly the curated diagnostic tool set" do
      expect(server.tools.keys).to match_array(described_class::TOOLS.map(&:tool_name))
    end

    it "injects the data source and defaults payloads to disallowed" do
      expect(server.server_context[:data_source]).to eq(data_source)
      expect(server.server_context[:allow_payloads]).to be(false)
    end

    it "honors allow_payloads when requested" do
      server = described_class.build(data_source: data_source, allow_payloads: true)
      expect(server.server_context[:allow_payloads]).to be(true)
    end

    it "every registered tool is read-only and non-destructive" do
      described_class::TOOLS.each do |tool|
        annotations = tool.annotations_value.to_h
        expect(annotations[:readOnlyHint]).to be(true), "#{tool.tool_name} is not read-only"
        expect(annotations[:destructiveHint]).to be(false), "#{tool.tool_name} is destructive"
      end
    end

    it "exposes no tool that accepts raw SQL or mutates state" do
      names = described_class::TOOLS.map(&:tool_name)
      expect(names).to all(start_with("pgbus_"))
      expect(names).not_to include(a_string_matching(/sql|query|exec|purge|retry|discard|delete|drop|pause|resume/))
    end
  end

  describe "the workflow write tools (issue #550)" do
    let(:write_names) do
      %w[pgbus_workflow_resume pgbus_workflow_cancel pgbus_workflow_skip_step pgbus_workflow_resume_at
         pgbus_workflow_run_now pgbus_workflow_signal]
    end

    def listed(server)
      response = JSON.parse(server.handle_json(JSON.generate(jsonrpc: "2.0", id: 1, method: "tools/list", params: {})))
      response.dig("result", "tools").to_h { |tool| [tool["name"], tool] }
    end

    after { Pgbus.configuration.mcp_write_tools = false }

    it "lists none of them while mcp_write_tools is off (the default)" do
      tools = listed(described_class.build(data_source: data_source))

      expect(tools.keys).to match_array(described_class::TOOLS.map(&:tool_name))
      expect(tools.keys & write_names).to be_empty
      expect(described_class.build(data_source: data_source).instructions).not_to include("pgbus_workflow_resume")
    end

    it "lists all six, destructive and not read-only, when mcp_write_tools is on" do
      Pgbus.configuration.mcp_write_tools = true
      tools = listed(described_class.build(data_source: data_source))

      expect(described_class::WRITE_TOOLS.map(&:tool_name)).to match_array(write_names)
      expect(tools.keys).to match_array(described_class::TOOLS.map(&:tool_name) + write_names)
      write_names.each do |name|
        expect(tools[name]["annotations"]).to include("readOnlyHint" => false, "destructiveHint" => true),
                                              "#{name} is not marked as a write"
      end
      expect(tools.values_at("pgbus_workflow_cancel", "pgbus_workflow_skip_step", "pgbus_workflow_resume_at")
                  .map { |tool| tool["annotations"]["idempotentHint"] }).to all(be(true))
      expect(tools.values_at("pgbus_workflow_resume", "pgbus_workflow_run_now", "pgbus_workflow_signal")
                  .map { |tool| tool["annotations"]["idempotentHint"] }).to all(be(false))
    end

    it "leaves every read tool unchanged when mcp_write_tools is on" do
      off = listed(described_class.build(data_source: data_source))
      Pgbus.configuration.mcp_write_tools = true
      on = listed(described_class.build(data_source: data_source))

      expect(on.slice(*off.keys)).to eq(off)
    end

    it "says in its instructions which tools write" do
      Pgbus.configuration.mcp_write_tools = true

      expect(described_class.build(data_source: data_source).instructions)
        .to include("pgbus_workflow_resume", "reason", "expected_status")
    end

    it "reads the flag when the server is built, not per call" do
      server = described_class.build(data_source: data_source)
      Pgbus.configuration.mcp_write_tools = true

      expect(listed(server).keys & write_names).to be_empty
    end

    it "never builds a write tool on the read-only base class" do
      expect(described_class::WRITE_TOOLS).to all(be < Pgbus::MCP::WriteTool)
      expect(described_class::WRITE_TOOLS.none? { |tool| tool <= Pgbus::MCP::BaseTool }).to be(true)
      expect(described_class::TOOLS.none? { |tool| tool <= Pgbus::MCP::WriteTool }).to be(true)
    end
  end

  describe "tool listing over the protocol" do
    subject(:server) { described_class.build(data_source: data_source) }

    it "lists all tools via a tools/list JSON-RPC request" do
      request = { jsonrpc: "2.0", id: 1, method: "tools/list", params: {} }
      response = JSON.parse(server.handle_json(JSON.generate(request)))

      tool_names = response.dig("result", "tools").map { |t| t["name"] }
      expect(tool_names).to include("pgbus_health", "pgbus_queues", "pgbus_processes")
    end

    it "calls pgbus_health end-to-end through the server" do
      allow(data_source).to receive_messages(queues_with_metrics: [], processes: [], queue_health_stats: {}, stream_queue_names: Set.new)

      request = {
        jsonrpc: "2.0", id: 2, method: "tools/call",
        params: { name: "pgbus_health", arguments: {} }
      }
      response = JSON.parse(server.handle_json(JSON.generate(request)))

      text = response.dig("result", "content").first["text"]
      expect(JSON.parse(text)["status"]).to eq("OK")
    end
  end
end
