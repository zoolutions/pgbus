# frozen_string_literal: true

require_relative "../integration_helper"

# Issue #589: the health verdict's grace is measured on the oldest visible,
# never-read message's claimable age, through Web::DataSource on real pgmq.
RSpec.describe "Unread claimable age metrics (integration)", :integration do
  let(:client) { Pgbus.client }
  let(:data_source) { Pgbus::Web::DataSource.new(client: client) }
  let(:queue_name) { client.config.queue_name("unread_age_test") }

  before do
    client.ensure_queue("unread_age_test")
    client.purge_queue("unread_age_test")
  end

  def batched_metrics
    data_source.queues_with_metrics.find { |q| q[:name] == queue_name }
  end

  def single_metrics
    data_source.queue_detail(queue_name)
  end

  it "reports the age of a message enqueued now and never read, in both SQL paths" do
    client.send_message("unread_age_test", { "now" => true })

    # Wall-clock relative: assert a lower bound, an exact 0 flakes on a
    # contended server.
    expect(batched_metrics[:oldest_unread_claimable_age_sec]).to be_an(Integer).and be >= 0
    expect(single_metrics[:oldest_unread_claimable_age_sec]).to be_an(Integer).and be >= 0
  end

  it "reports nil for a message whose vt is an hour ahead" do
    client.send_message("unread_age_test", { "delayed" => true }, delay: 3600)

    expect(batched_metrics[:oldest_unread_claimable_age_sec]).to be_nil
    expect(single_metrics[:oldest_unread_claimable_age_sec]).to be_nil
  end

  it "reports nil for a visible message that has been read, while the claimable age counts it" do
    client.send_message("unread_age_test", { "read" => true })
    client.read_batch("unread_age_test", qty: 1, vt: 0)

    batched = batched_metrics
    expect(batched[:queue_visible_length]).to eq(1)
    expect(batched[:oldest_claimable_age_sec]).to be_an(Integer)
    expect(batched[:oldest_unread_claimable_age_sec]).to be_nil
    expect(single_metrics[:oldest_unread_claimable_age_sec]).to be_nil
  end
end
