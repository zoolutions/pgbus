# frozen_string_literal: true

require_relative "spec_helper"

RSpec.describe Pgbus::MCP::Runner do
  describe ".authorize!" do
    it "is a no-op when no token is configured" do
      expect { described_class.authorize!({}) }.not_to raise_error
    end

    it "is a no-op when PGBUS_MCP_TOKEN is empty" do
      expect { described_class.authorize!({ "PGBUS_MCP_TOKEN" => "" }) }.not_to raise_error
    end

    it "raises when the token is set but the auth token is missing" do
      expect do
        described_class.authorize!({ "PGBUS_MCP_TOKEN" => "s3cret" })
      end.to raise_error(Pgbus::Error, /authentication failed/)
    end

    it "raises when the auth token does not match" do
      expect do
        described_class.authorize!({ "PGBUS_MCP_TOKEN" => "s3cret", "PGBUS_MCP_AUTH_TOKEN" => "wrong" })
      end.to raise_error(Pgbus::Error, /authentication failed/)
    end

    it "passes when the auth token matches" do
      env = { "PGBUS_MCP_TOKEN" => "s3cret", "PGBUS_MCP_AUTH_TOKEN" => "s3cret" }
      expect { described_class.authorize!(env) }.not_to raise_error
    end
  end

  describe ".truthy?" do
    it "recognizes truthy strings" do
      %w[1 true yes on TRUE On].each do |v|
        expect(described_class.truthy?(v)).to be(true)
      end
    end

    it "treats everything else as false" do
      [nil, "", "0", "false", "no", "maybe"].each do |v|
        expect(described_class.truthy?(v)).to be(false)
      end
    end
  end

  describe ".secure_compare?" do
    it "is true for equal strings" do
      expect(described_class.secure_compare?("abc", "abc")).to be(true)
    end

    it "is false for differing strings of equal length" do
      expect(described_class.secure_compare?("abc", "abd")).to be(false)
    end

    it "is false for differing lengths" do
      expect(described_class.secure_compare?("abc", "abcd")).to be(false)
    end

    it "is false when the provided value is nil" do
      expect(described_class.secure_compare?("abc", nil)).to be(false)
    end
  end

  describe ".run" do
    it "enforces the token gate before opening a transport" do
      env = { "PGBUS_MCP_TOKEN" => "s3cret" }
      expect { described_class.run(env: env) }.to raise_error(Pgbus::Error, /authentication failed/)
    end

    it "builds a payload-redacted server and drives the stdio transport" do
      transport = instance_double(MCP::Server::Transports::StdioTransport, open: nil)
      allow(Pgbus::MCP::Server).to receive(:build).and_call_original
      allow(MCP::Server::Transports::StdioTransport).to receive(:new).and_return(transport)

      described_class.run(env: {})

      expect(Pgbus::MCP::Server).to have_received(:build).with(allow_payloads: false)
      expect(transport).to have_received(:open)
    end

    it "says in its start line whether the workflow write tools are on" do
      transport = instance_double(MCP::Server::Transports::StdioTransport, open: nil)
      allow(MCP::Server::Transports::StdioTransport).to receive(:new).and_return(transport)
      lines = []
      allow(Pgbus.logger).to receive(:info) { |&block| lines << block.call }

      described_class.run(env: {})
      Pgbus.configuration.mcp_write_tools = true
      described_class.run(env: {})

      expect(lines.first).to include("read-only")
      expect(lines.last).to include("workflow write tools ON")
    ensure
      Pgbus.configuration.mcp_write_tools = false
    end

    it "passes allow_payloads through when the env flag is truthy" do
      transport = instance_double(MCP::Server::Transports::StdioTransport, open: nil)
      allow(Pgbus::MCP::Server).to receive(:build).and_call_original
      allow(MCP::Server::Transports::StdioTransport).to receive(:new).and_return(transport)

      described_class.run(env: { "PGBUS_MCP_ALLOW_PAYLOADS" => "true" })

      expect(Pgbus::MCP::Server).to have_received(:build).with(allow_payloads: true)
    end
  end
end
