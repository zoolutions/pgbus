# frozen_string_literal: true

require "spec_helper"

require_relative "../../../lib/pgbus/web/data_source"

# Issue #631: a re-sent job payload takes its uniqueness key the way an
# enqueue does. The happy paths and the held key run against a real
# database in spec/integration/dashboard_resend_uniqueness_spec.rb; these
# cover the failures around the send.
RSpec.describe Pgbus::Web::DataSource::Resend do
  subject(:data_source) { Pgbus::Web::DataSource.new(client: mock_client) }

  let(:mock_client) { double("Pgbus::Client", pgmq: double("pgmq")) }
  let(:mock_connection) { double("ActiveRecord::Connection") }
  let(:payload) do
    { "job_class" => "OrderJob", "job_id" => "j-1", "executions" => 0,
      "pgbus_uniqueness_key" => "order-1", "pgbus_uniqueness_strategy" => "until_executed" }
  end
  let(:holder) { ["default", 9, "2026-11-02 10:00:00+00"] }

  before do
    allow(Pgbus::BusRecord).to receive(:connection).and_return(mock_connection)
    allow(Pgbus::UniquenessKey).to receive_messages(unbind!: nil, acquire!: true)
    allow(Pgbus::UniquenessKey).to receive(:release_if_unbound!)
    allow(Pgbus::UniquenessKey).to receive(:bind!)
    allow(Pgbus::UniquenessKey).to receive(:release!)
    allow(Pgbus::UniquenessKey).to receive(:clear_bind_stamp!)
    allow(Pgbus.logger).to receive(:warn)
  end

  def resend(payload, &) = data_source.send(:resend_holding_uniqueness, payload, lock_queue: "default", holder: holder, &)

  it "gives the key back and re-raises when the send fails" do
    expect { resend(payload) { raise StandardError, "produce failed" } }.to raise_error(StandardError, "produce failed")

    expect(Pgbus::UniquenessKey).to have_received(:acquire!).with("order-1", queue_name: "default", msg_id: 0)
    expect(Pgbus::UniquenessKey).to have_received(:release!).with("order-1")
    expect(Pgbus::UniquenessKey).not_to have_received(:bind!)
    expect(Pgbus::UniquenessKey).to have_received(:clear_bind_stamp!).with("order-1")
  end

  it "logs a failed bind and still reports the copy as sent" do
    allow(Pgbus::UniquenessKey).to receive(:bind!).and_raise(StandardError, "connection lost")

    expect(resend(payload) { "17" }).to be(true)

    expect(Pgbus::UniquenessKey).to have_received(:bind!).with("order-1", queue_name: "default", msg_id: 17,
                                                                          acquired_at: nil)
    expect(Pgbus.logger).to have_received(:warn) do |&block|
      expect(block.call).to include("Uniqueness bind of re-sent order-1 failed: connection lost")
    end
  end

  describe "the row the source message still holds" do
    let(:stamp) { "2026-11-02 10:05:00.123456" }

    before { allow(Pgbus::UniquenessKey).to receive(:unbind!).and_return(stamp) }

    it "is taken over for the copy and bound to it by its stamp, without acquiring" do
      expect(resend(payload) { "17" }).to be(true)

      expect(Pgbus::UniquenessKey).to have_received(:unbind!).with(
        "order-1", from_queue_name: "default", from_msg_id: 9, from_enqueued_at: "2026-11-02 10:00:00+00",
                   queue_name: "default"
      )
      expect(Pgbus::UniquenessKey).not_to have_received(:acquire!)
      expect(Pgbus::UniquenessKey).to have_received(:bind!).with("order-1", queue_name: "default", msg_id: 17,
                                                                            acquired_at: stamp)
    end

    it "goes by its stamp when the send fails" do
      expect { resend(payload) { raise StandardError, "produce failed" } }.to raise_error(StandardError)

      expect(Pgbus::UniquenessKey).to have_received(:release_if_unbound!).with("order-1", acquired_before: stamp)
      expect(Pgbus::UniquenessKey).not_to have_received(:release!)
    end
  end

  it "sends nothing, and says why, while another copy holds the key" do
    allow(Pgbus::UniquenessKey).to receive(:acquire!).and_return(false)
    sent = false

    expect(resend(payload) { sent = true }).to be(false)

    expect(sent).to be(false)
    expect(Pgbus.logger).to have_received(:warn) do |&block|
      expect(block.call).to include("Not re-sending OrderJob: uniqueness key order-1 is held by another copy")
    end
  end

  it "takes no key for a :while_executing payload, or one without a key" do
    [payload.merge("pgbus_uniqueness_strategy" => "while_executing"), payload.except("pgbus_uniqueness_key")].each do |other|
      expect(resend(other) { "1" }).to be(true)
    end

    expect(Pgbus::UniquenessKey).not_to have_received(:acquire!)
  end

  it "takes no key for an :until_executing retry_on re-enqueue (executions > 0), but does for a first attempt" do
    until_executing = payload.merge("pgbus_uniqueness_strategy" => "until_executing")

    resend(until_executing.merge("executions" => 2)) { "1" }
    expect(Pgbus::UniquenessKey).not_to have_received(:acquire!)

    resend(until_executing) { "2" }
    expect(Pgbus::UniquenessKey).to have_received(:acquire!).once
  end

  it "takes the key of an :until_executed retry (executions > 0): its copy releases it on success" do
    resend(payload.merge("executions" => 3)) { "1" }

    expect(Pgbus::UniquenessKey).to have_received(:acquire!).with("order-1", queue_name: "default", msg_id: 0)
  end

  it "takes a numeric key as text" do
    resend(payload.merge("pgbus_uniqueness_key" => 42)) { "1" }

    expect(Pgbus::UniquenessKey).to have_received(:acquire!).with("42", queue_name: "default", msg_id: 0)
  end
end
