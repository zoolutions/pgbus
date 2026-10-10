# frozen_string_literal: true

require "json"
require "spec_helper"

# HealthAnalyzer lives in the MCP namespace (excluded from Zeitwerk). It has no
# `mcp` gem dependency, so require just that file — mirroring what HealthApp
# does lazily on the readiness path.
require "pgbus/mcp/health_analyzer"

RSpec.describe Pgbus::Web::HealthApp do
  subject(:app) { described_class.new(data_source: data_source) }

  let(:data_source) { instance_double(Pgbus::Web::DataSource) }
  let(:analyzer) { instance_double(Pgbus::MCP::HealthAnalyzer) }

  def env_for(method, path)
    { "REQUEST_METHOD" => method, "PATH_INFO" => path }
  end

  def get(path)
    app.call(env_for("GET", path))
  end

  # Rack response triple → parsed body String (bodies are arrays of strings).
  def body_of(response)
    response[2].join
  end

  before do
    allow(Pgbus::MCP::HealthAnalyzer).to receive(:new).with(data_source).and_return(analyzer)
    allow(data_source).to receive(:reset_cache!).and_return(data_source)
  end

  describe "GET /livez" do
    it "returns 200 text/plain ok" do
      status, headers, = get("/livez")

      expect(status).to eq(200)
      expect(headers["Content-Type"]).to eq("text/plain")
    end

    it "responds with the literal body 'ok'" do
      expect(body_of(get("/livez"))).to eq("ok")
    end

    it "never touches the database (no DataSource / analyzer calls)" do
      get("/livez")

      expect(Pgbus::MCP::HealthAnalyzer).not_to have_received(:new)
    end
  end

  describe "GET /readyz" do
    it "returns 200 and the verdict JSON when the verdict is OK" do
      allow(analyzer).to receive(:verdict).and_return(status: "OK", reasons: [])

      status, headers, = get("/readyz")

      expect(status).to eq(200)
      expect(headers["Content-Type"]).to eq("application/json")
    end

    it "includes the verdict in the body" do
      allow(analyzer).to receive(:verdict).and_return(status: "OK", reasons: [])

      expect(JSON.parse(body_of(get("/readyz")))["status"]).to eq("OK")
    end

    it "returns 200 when the verdict is DEGRADED (serving process still ready)" do
      allow(analyzer).to receive(:verdict).and_return(status: "DEGRADED", reasons: ["stale process"])

      status, = get("/readyz")

      expect(status).to eq(200)
    end

    it "returns 503 when the verdict is STALLED" do
      allow(analyzer).to receive(:verdict).and_return(status: "STALLED", reasons: ["wedged"])

      status, = get("/readyz")

      expect(status).to eq(503)
    end

    it "still returns the STALLED verdict JSON in the 503 body" do
      allow(analyzer).to receive(:verdict).and_return(status: "STALLED", reasons: ["wedged"])

      expect(JSON.parse(body_of(get("/readyz")))["status"]).to eq("STALLED")
    end

    context "when the DataSource / database is unreachable" do
      before do
        allow(analyzer).to receive(:verdict).and_raise(StandardError.new("connection refused"))
      end

      it "returns 503" do
        status, = get("/readyz")

        expect(status).to eq(503)
      end

      it "responds with an ERROR status body" do
        expect(JSON.parse(body_of(get("/readyz")))["status"]).to eq("ERROR")
      end

      it "logs the failure via Pgbus.logger (no swallowed errors)" do
        allow(Pgbus.logger).to receive(:error)

        get("/readyz")

        expect(Pgbus.logger).to have_received(:error)
      end
    end
  end

  describe "unknown routes and methods" do
    it "returns 404 for an unknown path" do
      status, = get("/nope")

      expect(status).to eq(404)
    end

    it "returns 405 for a non-GET method on a known path" do
      status, = app.call(env_for("POST", "/readyz"))

      expect(status).to eq(405)
    end

    it "returns 405 for a non-GET method on /livez" do
      status, = app.call(env_for("DELETE", "/livez"))

      expect(status).to eq(405)
    end
  end

  describe "container-local readiness (issue #386)" do
    subject(:app) { described_class.new(local_readiness: -> { snapshot }) }

    let(:snapshot) do
      Pgbus::Process::ReadinessSnapshot.new(booted: true, shutting_down: false, expected: 3, live: 3)
    end

    it "returns 200 with the local status when the snapshot is ready" do
      status, headers, = get("/readyz")

      expect(status).to eq(200)
      expect(headers["Content-Type"]).to eq("application/json")
    end

    it "includes status, expected, and live in the body" do
      body = JSON.parse(body_of(get("/readyz")))

      expect(body).to eq("status" => "OK", "expected" => 3, "live" => 3)
    end

    it "never builds the cluster analyzer in local mode" do
      get("/readyz")

      expect(Pgbus::MCP::HealthAnalyzer).not_to have_received(:new)
    end

    it "leaves /livez unconditional" do
      status, = get("/livez")

      expect(status).to eq(200)
    end

    context "when the container is still booting" do
      let(:snapshot) do
        Pgbus::Process::ReadinessSnapshot.new(booted: false, shutting_down: false, expected: 0, live: 0)
      end

      it "returns 503 BOOTING" do
        status, = get("/readyz")

        expect(status).to eq(503)
        expect(JSON.parse(body_of(get("/readyz")))["status"]).to eq("BOOTING")
      end
    end

    context "when the container is draining" do
      let(:snapshot) do
        Pgbus::Process::ReadinessSnapshot.new(booted: true, shutting_down: true, expected: 3, live: 3)
      end

      it "returns 503 DRAINING" do
        status, = get("/readyz")

        expect(status).to eq(503)
        expect(JSON.parse(body_of(get("/readyz")))["status"]).to eq("DRAINING")
      end
    end

    context "when a child is missing" do
      let(:snapshot) do
        Pgbus::Process::ReadinessSnapshot.new(booted: true, shutting_down: false, expected: 3, live: 2)
      end

      it "returns 503 DEGRADED with the counts" do
        status, = get("/readyz")
        body = JSON.parse(body_of(get("/readyz")))

        expect(status).to eq(503)
        expect(body).to eq("status" => "DEGRADED", "expected" => 3, "live" => 2)
      end
    end

    context "when the readiness callable raises" do
      subject(:app) { described_class.new(local_readiness: -> { raise StandardError, "boom" }) }

      it "returns 503 with an ERROR body and logs" do
        allow(Pgbus.logger).to receive(:error)

        status, = get("/readyz")

        expect(status).to eq(503)
        expect(JSON.parse(body_of(get("/readyz")))["status"]).to eq("ERROR")
        expect(Pgbus.logger).to have_received(:error).at_least(:once)
      end
    end
  end

  describe "injected DataSource" do
    # DataSource memoizes queues, processes, stream names and table stats per
    # instance; a long-lived injected one must not serve the first check's
    # facts forever (issue #492).
    it "drops its per-instance memos before every check" do
      allow(analyzer).to receive(:verdict).and_return(status: "OK", reasons: [])

      2.times { get("/readyz") }

      expect(data_source).to have_received(:reset_cache!).twice
    end
  end

  describe "default DataSource" do
    it "builds a fresh Pgbus::Web::DataSource when none is injected" do
      allow(Pgbus::Web::DataSource).to receive(:new).and_return(data_source)
      allow(analyzer).to receive(:verdict).and_return(status: "OK", reasons: [])

      described_class.new.call(env_for("GET", "/readyz"))

      expect(Pgbus::Web::DataSource).to have_received(:new)
    end
  end
end
