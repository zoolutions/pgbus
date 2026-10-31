# frozen_string_literal: true

require_relative "../integration_helper"

RSpec.describe "Queue operations (integration)", :integration do
  let(:client) { Pgbus.client }

  describe "queue creation" do
    it "creates a queue idempotently" do
      client.ensure_queue("ops_test")
      client.ensure_queue("ops_test") # second call should not raise

      queues = client.list_queues.map { |q| q.respond_to?(:queue_name) ? q.queue_name : q.to_s }
      expect(queues).to include("pgbus_int_ops_test")
    end

    it "creates a dead letter queue" do
      client.ensure_dead_letter_queue("ops_test")

      queues = client.list_queues.map { |q| q.respond_to?(:queue_name) ? q.queue_name : q.to_s }
      expect(queues).to include("pgbus_int_ops_test_dlq")
    end
  end

  describe "batch operations" do
    before { client.ensure_queue("batch_test") }

    it "sends and reads a batch of messages" do
      payloads = 5.times.map { |i| { "index" => i, "job_class" => "BatchJob" } }
      msg_ids = client.send_batch("batch_test", payloads)

      expect(msg_ids).to be_an(Array)
      expect(msg_ids.size).to eq(5)

      messages = client.read_batch("batch_test", qty: 10, vt: 0)
      expect(messages.size).to eq(5)

      indices = messages.map { |m| JSON.parse(m.message)["index"] }
      expect(indices.sort).to eq([0, 1, 2, 3, 4])
    end

    it "respects qty limit on read" do
      5.times { |i| client.send_message("batch_test", { "index" => i }) }

      messages = client.read_batch("batch_test", qty: 2, vt: 0)
      expect(messages.size).to eq(2)
    end
  end

  describe "purge" do
    before { client.ensure_queue("purge_test") }

    it "removes all messages from a queue" do
      3.times { client.send_message("purge_test", { "data" => "x" }) }

      client.purge_queue("purge_test")

      messages = client.read_batch("purge_test", qty: 10)
      expect(messages || []).to be_empty
    end
  end

  describe "visibility timeout" do
    before { client.ensure_queue("vt_test") }

    it "makes message invisible for the specified duration" do
      client.send_message("vt_test", { "vt" => true })

      messages = client.read_batch("vt_test", qty: 1, vt: 30)
      expect(messages.size).to eq(1)

      again = client.read_batch("vt_test", qty: 1, vt: 30)
      expect(again || []).to be_empty
    end

    it "allows extending visibility timeout" do
      msg_id = client.send_message("vt_test", { "extend" => true })
      client.read_batch("vt_test", qty: 1, vt: 5)

      client.set_visibility_timeout("vt_test", msg_id, vt: 60)

      messages = client.read_batch("vt_test", qty: 1, vt: 5)
      expect(messages || []).to be_empty
    end
  end

  describe "delayed messages" do
    before { client.ensure_queue("delay_test") }

    it "sends a message with a delay" do
      client.send_message("delay_test", { "delayed" => true }, delay: 60)

      messages = client.read_batch("delay_test", qty: 1)
      expect(messages || []).to be_empty
    end

    # Each producer path against real PGMQ (issue #593): pgmq-ruby 0.8.0 raised
    # on an ActiveSupport::Duration (fixed in 0.8.1), and the native send binds
    # the delay as an integer, so a fractional one must not reach it.
    def delay_seconds(msg_id, table: "q_pgbus_int_delay_test")
      ActiveRecord::Base.connection.select_value(
        "SELECT EXTRACT(EPOCH FROM vt - enqueued_at) FROM pgmq.#{table} WHERE msg_id = #{msg_id.to_i}"
      ).to_f
    end

    it "takes an ActiveSupport::Duration delay on send_message" do
      msg_id = client.send_message("delay_test", { "delayed" => true }, delay: 5.minutes)

      expect(delay_seconds(msg_id)).to be_within(1).of(300)
    end

    it "takes an ActiveSupport::Duration delay on send_batch" do
      msg_ids = client.send_batch("delay_test", [{ "a" => 1 }, { "b" => 2 }], delay: 5.minutes)

      expect(msg_ids.map { |id| delay_seconds(id) }).to all(be_within(1).of(300))
    end

    it "keeps a fractional Duration fractional" do
      msg_id = client.send_message("delay_test", { "delayed" => true }, delay: 1.5.seconds)

      expect(delay_seconds(msg_id)).to be_within(0.25).of(1.5)
    end

    it "keeps a plain Float delay fractional" do
      msg_id = client.send_message("delay_test", { "delayed" => true }, delay: 1.5)

      expect(delay_seconds(msg_id)).to be_within(0.25).of(1.5)
    end

    it "keeps a fractional delay fractional on send_batch" do
      msg_ids = client.send_batch("delay_test", [{ "a" => 1 }, { "b" => 2 }], delay: 1.5.seconds)

      expect(msg_ids.map { |id| delay_seconds(id) }).to all(be_within(0.25).of(1.5))
    end

    it "takes an ActiveSupport::Duration delay on send_stream_message" do
      msg_id = client.send_stream_message("delay_stream", { "s" => 1 }, delay: 5.minutes)

      expect(delay_seconds(msg_id, table: "q_pgbus_int_delay_stream")).to be_within(1).of(300)
    end

    it "takes an ActiveSupport::Duration delay on publish_to_topic" do
      client.bind_topic("delay_test.#", "delay_test")
      client.publish_to_topic("delay_test.created", { "topic" => true }, delay: 5.minutes)
      msg_id = ActiveRecord::Base.connection.select_value("SELECT max(msg_id) FROM pgmq.q_pgbus_int_delay_test")

      expect(delay_seconds(msg_id)).to be_within(1).of(300)
    end

    # produce_topic sends a fractional delay through produce_batch_topic.
    it "keeps a fractional Duration fractional on publish_to_topic" do
      client.bind_topic("delay_test.#", "delay_test")
      client.publish_to_topic("delay_test.created", { "topic" => true }, delay: 1.5.seconds)
      msg_id = ActiveRecord::Base.connection.select_value("SELECT max(msg_id) FROM pgmq.q_pgbus_int_delay_test")

      expect(delay_seconds(msg_id)).to be_within(0.25).of(1.5)
    end
  end

  describe "message headers" do
    before { client.ensure_queue("header_test") }

    it "preserves headers through enqueue and read" do
      headers = { "x-trace-id" => "abc-123", "x-source" => "test" }
      client.send_message("header_test", { "data" => 1 }, headers: headers)

      messages = client.read_batch("header_test", qty: 1)
      expect(messages.size).to eq(1)

      msg_headers = messages.first.headers
      parsed = msg_headers.is_a?(String) ? JSON.parse(msg_headers) : msg_headers
      expect(parsed).to include("x-trace-id" => "abc-123")
    end
  end
end
