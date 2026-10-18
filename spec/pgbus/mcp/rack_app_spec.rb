# frozen_string_literal: true

require "json"
require "rack/mock"
require_relative "spec_helper"

RSpec.describe Pgbus::MCP::RackApp do
  let(:data_source) { instance_double(Pgbus::Web::DataSource) }

  let(:json_headers) do
    { "CONTENT_TYPE" => "application/json", "HTTP_ACCEPT" => "application/json" }
  end

  let(:health_call) do
    { jsonrpc: "2.0", id: 1, method: "tools/call", params: { name: "pgbus_health", arguments: {} } }.to_json
  end

  before do
    allow(data_source).to receive_messages(queues_with_metrics: [], processes: [], queue_health_stats: {}, stream_queue_names: Set.new)
  end

  def mock_for(**kwargs)
    Rack::MockRequest.new(described_class.new(data_source: data_source, **kwargs))
  end

  describe "responds to #call as a Rack app" do
    it "answers a tools/list POST with the curated tool set" do
      app = mock_for
      body = { jsonrpc: "2.0", id: 2, method: "tools/list", params: {} }.to_json

      response = app.post("/", input: body, **json_headers)
      names = JSON.parse(response.body).dig("result", "tools").map { |t| t["name"] }

      expect(response.status).to eq(200)
      expect(names).to include("pgbus_health", "pgbus_queues", "pgbus_processes")
    end

    it "answers a tools/call POST with a verdict from the injected data source" do
      app = mock_for
      response = app.post("/", input: health_call, **json_headers)

      inner = JSON.parse(JSON.parse(response.body).dig("result", "content", 0, "text"))
      expect(response.status).to eq(200)
      expect(inner["status"]).to eq("OK")
    end

    it "returns 406 when the client omits Accept: application/json (documented gotcha)" do
      app = mock_for
      response = app.post("/", input: health_call, "CONTENT_TYPE" => "application/json")

      expect(response.status).to eq(406)
    end
  end

  describe "token gate" do
    it "rejects requests with no Authorization header" do
      app = mock_for(token: "s3cret")
      response = app.post("/", input: health_call, **json_headers)

      expect(response.status).to eq(401)
      expect(JSON.parse(response.body).dig("error", "message")).to eq("Unauthorized")
    end

    it "rejects a wrong bearer token" do
      app = mock_for(token: "s3cret")
      response = app.post("/", input: health_call, "HTTP_AUTHORIZATION" => "Bearer nope", **json_headers)

      expect(response.status).to eq(401)
    end

    it "rejects a non-bearer Authorization scheme" do
      app = mock_for(token: "s3cret")
      response = app.post("/", input: health_call, "HTTP_AUTHORIZATION" => "Basic s3cret", **json_headers)

      expect(response.status).to eq(401)
    end

    it "allows a matching bearer token" do
      app = mock_for(token: "s3cret")
      response = app.post("/", input: health_call, "HTTP_AUTHORIZATION" => "Bearer s3cret", **json_headers)

      expect(response.status).to eq(200)
    end

    it "sends a WWW-Authenticate: Bearer challenge on 401" do
      app = mock_for(token: "s3cret")
      response = app.post("/", input: health_call, **json_headers)

      expect(response.headers["WWW-Authenticate"]).to eq("Bearer")
    end

    it "returns a fresh, mutable response array on each auth failure" do
      app = described_class.new(data_source: data_source, token: "s3cret")
      env = Rack::MockRequest.env_for("/", input: health_call, **json_headers)

      first = app.call(env)
      second = app.call(Rack::MockRequest.env_for("/", input: health_call, **json_headers))

      expect(first).not_to be_frozen
      expect(first[1]).not_to be_frozen
      expect(first).not_to equal(second)
    end

    # Regression for #304: the frozen UNAUTHORIZED triple raised FrozenError once
    # a downstream, response-mutating middleware ran — turning 401 into 500. Drive
    # the app through the real Rack::TempfileReaper (assigns response[2]) and
    # Rack::ETag (adds a header) to catch it the way Rails does.
    it "answers 401 through response-mutating middleware without a FrozenError" do
      require "rack/tempfile_reaper"
      require "rack/etag"
      stack = Rack::TempfileReaper.new(Rack::ETag.new(described_class.new(data_source: data_source, token: "s3cret")))

      response = Rack::MockRequest.new(stack).post("/", input: health_call, **json_headers)

      expect(response.status).to eq(401)
      expect(JSON.parse(response.body).dig("error", "message")).to eq("Unauthorized")
    end
  end

  describe "custom auth callable" do
    it "delegates to the auth proc, which receives a Rack::Request" do
      seen = nil
      auth = lambda do |req|
        seen = req
        req.get_header("HTTP_X_OK") == "yes"
      end
      app = mock_for(auth: auth)

      ok = app.post("/", input: health_call, "HTTP_X_OK" => "yes", **json_headers)
      denied = app.post("/", input: health_call, **json_headers)

      expect(ok.status).to eq(200)
      expect(denied.status).to eq(401)
      expect(seen).to be_a(Rack::Request)
    end

    it "takes precedence over token when both are given" do
      app = mock_for(token: "s3cret", auth: ->(_req) { true })
      # No bearer token sent, but the auth proc allows it.
      response = app.post("/", input: health_call, **json_headers)

      expect(response.status).to eq(200)
    end
  end

  describe "unauthenticated mounting" do
    it "warns when neither token nor auth is configured" do
      allow(Pgbus.logger).to receive(:warn)

      described_class.new(data_source: data_source)

      expect(Pgbus.logger).to have_received(:warn)
    end

    it "does not warn when a token is configured" do
      allow(Pgbus.logger).to receive(:warn)

      described_class.new(data_source: data_source, token: "s3cret")

      expect(Pgbus.logger).not_to have_received(:warn)
    end

    context "with the workflow write tools on" do
      before { Pgbus.configuration.mcp_write_tools = true }
      after { Pgbus.configuration.mcp_write_tools = false }

      it "says the unauthenticated endpoint can change workflow runs" do
        warnings = []
        allow(Pgbus.logger).to receive(:warn) { |&block| warnings << block.call }

        described_class.new(data_source: data_source)

        expect(warnings.join).to include("mcp_write_tools", "change workflow runs")
      end

      it "does not warn when a token is configured" do
        allow(Pgbus.logger).to receive(:warn)

        described_class.new(data_source: data_source, token: "s3cret")

        expect(Pgbus.logger).not_to have_received(:warn)
      end
    end
  end

  describe "payload redaction over HTTP" do
    let(:rows) { [{ msg_id: 1, read_ct: 0, message: "{\"pii\":\"x\"}", headers: "{}" }] }

    def jobs_call(include_payloads)
      {
        jsonrpc: "2.0", id: 9, method: "tools/call",
        params: { name: "pgbus_jobs", arguments: { queue: "pgbus_default", include_payloads: include_payloads } }
      }.to_json
    end

    before { allow(data_source).to receive(:jobs).and_return(rows) }

    it "redacts message bodies by default even when include_payloads is requested" do
      app = mock_for
      response = app.post("/", input: jobs_call(true), **json_headers)

      inner = JSON.parse(JSON.parse(response.body).dig("result", "content", 0, "text"))
      expect(inner["jobs"].first["message"]).to eq(Pgbus::MCP::Redactor::REDACTED)
    end

    it "returns payloads when the app allows them and include_payloads is set" do
      app = mock_for(allow_payloads: true)
      response = app.post("/", input: jobs_call(true), **json_headers)

      inner = JSON.parse(JSON.parse(response.body).dig("result", "content", 0, "text"))
      expect(inner["jobs"].first["message"]).to eq("{\"pii\":\"x\"}")
    end
  end

  # mcp >= 0.23 validates the Host (and Origin) header inside the transport —
  # DNS-rebinding protection, on by default, accepting only loopback hosts. A
  # pgbus app mounted at https://app.example.com/pgbus/mcp therefore answered
  # every request with 403 "Invalid Host header" until these options existed.
  describe "DNS rebinding protection" do
    let(:bearer) { { "HTTP_AUTHORIZATION" => "Bearer s3cret" } }
    let(:foreign_host) { { "HTTP_HOST" => "app.example.com" } }

    def error_message(response)
      JSON.parse(response.body).dig("error", "message")
    end

    context "when the app is gated (token: or auth:)" do
      it "accepts a non-loopback Host with a valid bearer token — the gate makes the check redundant" do
        app = mock_for(token: "s3cret")
        response = app.post("/", input: health_call, **bearer, **foreign_host, **json_headers)

        expect(response.status).to eq(200)
      end

      it "accepts a non-loopback Host through a custom auth callable" do
        app = mock_for(auth: ->(_req) { true })
        response = app.post("/", input: health_call, **foreign_host, **json_headers)

        expect(response.status).to eq(200)
      end

      it "still 401s an unauthenticated request before any Host check runs" do
        app = mock_for(token: "s3cret")
        response = app.post("/", input: health_call, **foreign_host, **json_headers)

        expect(response.status).to eq(401)
      end

      it "re-enables the transport check with dns_rebinding_protection: true" do
        app = mock_for(token: "s3cret", dns_rebinding_protection: true)
        response = app.post("/", input: health_call, **bearer, **foreign_host, **json_headers)

        expect(response.status).to eq(403)
        expect(error_message(response)).to include("Invalid Host header")
      end

      it "honours allowed_hosts alongside an explicitly enabled check" do
        app = mock_for(token: "s3cret", dns_rebinding_protection: true, allowed_hosts: ["app.example.com"])
        response = app.post("/", input: health_call, **bearer, **foreign_host, **json_headers)

        expect(response.status).to eq(200)
      end
    end

    context "when the app is unauthenticated" do
      before { allow(Pgbus.logger).to receive(:warn) }

      it "keeps the transport check on: a non-loopback Host is refused with 403" do
        app = mock_for
        response = app.post("/", input: health_call, **foreign_host, **json_headers)

        expect(response.status).to eq(403)
        expect(error_message(response)).to include("Invalid Host header")
      end

      it "accepts a loopback Host" do
        app = mock_for
        response = app.post("/", input: health_call, "HTTP_HOST" => "localhost:3000", **json_headers)

        expect(response.status).to eq(200)
      end

      it "widens the accepted hosts with allowed_hosts (bare name matches any port)" do
        app = mock_for(allowed_hosts: ["app.example.com"])
        response = app.post("/", input: health_call, "HTTP_HOST" => "app.example.com:8443", **json_headers)

        expect(response.status).to eq(200)
      end

      it "refuses a cross-origin browser request unless the Origin is in allowed_origins" do
        denied = mock_for.post("/", input: health_call, "HTTP_HOST" => "localhost",
                                    "HTTP_ORIGIN" => "https://tool.example.com", **json_headers)
        allowed = mock_for(allowed_origins: ["https://tool.example.com"])
                  .post("/", input: health_call, "HTTP_HOST" => "localhost",
                             "HTTP_ORIGIN" => "https://tool.example.com", **json_headers)

        expect(denied.status).to eq(403)
        expect(error_message(denied)).to include("Invalid Origin header")
        expect(allowed.status).to eq(200)
      end

      it "can be switched off explicitly with dns_rebinding_protection: false" do
        app = mock_for(dns_rebinding_protection: false)
        response = app.post("/", input: health_call, **foreign_host, **json_headers)

        expect(response.status).to eq(200)
      end
    end

    it "raises an actionable error on an mcp gem older than 0.23 (no DNS-rebinding options to pass)" do
      stub_const("MCP::VERSION", "0.22.0")

      expect { described_class.new(data_source: data_source, token: "s3cret") }
        .to raise_error(Pgbus::Error, /mcp >= 0\.23.*0\.22\.0/)
    end
  end

  describe "Pgbus::MCP.rack_app" do
    it "passes the DNS-rebinding options through to the RackApp" do
      allow(described_class).to receive(:new).and_call_original

      Pgbus::MCP.rack_app(data_source: data_source, token: "s3cret",
                          allowed_hosts: ["app.example.com"], allowed_origins: ["https://x.example"],
                          dns_rebinding_protection: true)

      expect(described_class).to have_received(:new).with(
        hash_including(allowed_hosts: ["app.example.com"], allowed_origins: ["https://x.example"],
                       dns_rebinding_protection: true)
      )
    end

    it "returns a RackApp instance" do
      app = Pgbus::MCP.rack_app(data_source: data_source, token: "s3cret")
      expect(app).to be_a(described_class)
    end
  end
end
