# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Metrics::Backends::Statsd do
  subject(:backend) { described_class.new(host: "127.0.0.1", port: 8125, socket: socket) }

  let(:socket) { instance_double(UDPSocket) }

  before { allow(socket).to receive(:send) }

  describe "#increment" do
    it "sends a counter datagram with a |c type" do
      backend.increment("pgbus_messages_sent", 1, { queue: "default" })

      expect(socket).to have_received(:send).with(
        "pgbus_messages_sent:1|c|#queue:default", 0, "127.0.0.1", 8125
      )
    end

    it "defaults the value to 1" do
      backend.increment("pgbus_worker_recycled")

      expect(socket).to have_received(:send).with("pgbus_worker_recycled:1|c", 0, "127.0.0.1", 8125)
    end
  end

  describe "#gauge" do
    it "sends a gauge datagram with a |g type" do
      backend.gauge("pgbus_queue_depth", 12, { queue: "default" })

      expect(socket).to have_received(:send).with(
        "pgbus_queue_depth:12|g|#queue:default", 0, "127.0.0.1", 8125
      )
    end
  end

  describe "#histogram" do
    it "sends a timing datagram with a |ms type" do
      backend.histogram("pgbus_job_duration_ms", 42, { queue: "default", job_class: "Foo" })

      expect(socket).to have_received(:send).with(
        "pgbus_job_duration_ms:42|ms|#queue:default,job_class:Foo", 0, "127.0.0.1", 8125
      )
    end
  end

  describe "#histogram of a _seconds metric" do
    it "sends it as a millisecond timer, named _ms, since StatsD reads |ms values as milliseconds" do
      backend.histogram("pgbus_workflow_step_duration_seconds", 0.25, { step: "welcome" })

      expect(socket).to have_received(:send).with(
        "pgbus_workflow_step_duration_ms:250.0|ms|#step:welcome", 0, "127.0.0.1", 8125
      )
    end
  end

  describe "tag formatting" do
    it "omits the tag section when there are no tags" do
      backend.increment("pgbus_messages_sent", 3)

      expect(socket).to have_received(:send).with("pgbus_messages_sent:3|c", 0, "127.0.0.1", 8125)
    end

    it "skips nil tag values" do
      backend.increment("pgbus_event_count", 1, { handler: "H", routing_key: nil })

      expect(socket).to have_received(:send).with(
        "pgbus_event_count:1|c|#handler:H", 0, "127.0.0.1", 8125
      )
    end
  end

  describe "error handling" do
    it "rescues socket errors and logs rather than raising" do
      allow(socket).to receive(:send).and_raise(SocketError, "down")
      allow(Pgbus.logger).to receive(:warn)

      expect { backend.increment("pgbus_messages_sent", 1) }.not_to raise_error
      expect(Pgbus.logger).to have_received(:warn)
    end
  end
end
