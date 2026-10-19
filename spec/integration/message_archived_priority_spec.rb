# frozen_string_literal: true

require_relative "../integration_helper"

# Client#message_archived? under priority routing (issue #569): a logical name
# expands to every _pN archive the strategy creates; a physical sub-queue name
# passed with prefixed: false probes its own archive only. Before, a logical
# name probed the base table, which priority routing never creates, and the
# answer was nil.
RSpec.describe "Client#message_archived? under priority routing", :integration do
  let(:queue) { "prio_archive" }
  let(:client) { Pgbus.client }
  let(:physical) { ->(level) { Pgbus.configuration.priority_queue_name(queue, level) } }

  around do |example|
    original = Pgbus.configuration.priority_levels
    Pgbus.configuration.priority_levels = 3
    Pgbus.reset_client!
    example.run
    Pgbus.configuration.priority_levels = original
    Pgbus.reset_client!
  end

  before { client.ensure_queue(queue) }

  it "finds a message archived from a sub-queue by its logical name" do
    msg_id = client.send_message(queue, { "n" => 1 }, priority: 1)
    client.archive_message(physical.call(1), msg_id, prefixed: false)

    expect(client.message_archived?(queue, msg_id: msg_id)).to be(true)
  end

  it "probes only the named sub-queue's archive with prefixed: false" do
    msg_id = client.send_message(queue, { "n" => 1 }, priority: 1)
    client.archive_message(physical.call(1), msg_id, prefixed: false)

    expect(client.message_archived?(physical.call(1), msg_id: msg_id, prefixed: false)).to be(true)
    # Each sub-queue table has its own msg_id sequence; _p2 holds nothing yet.
    expect(client.message_archived?(physical.call(2), msg_id: msg_id, prefixed: false)).to be(false)
  end

  it "answers false, not unknown, when no sub-queue archive holds the msg_id" do
    expect(client.message_archived?(queue, msg_id: 999_999_999)).to be(false)
  end
end
