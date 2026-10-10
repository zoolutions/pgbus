# frozen_string_literal: true

require_relative "../integration_helper"
require "active_job"
require "active_job/queue_adapters/pgbus_adapter"

# Issue #495: a message that lands in a DLQ carries why it died in its PGMQ
# headers, a retry from the DLQ strips that block and counts the trip, and the
# count survives a second death.
RSpec.describe "Dead-letter reason headers (integration)", :integration do
  let(:client) { Pgbus.client }
  let(:config) { Pgbus.configuration }
  let(:queue) { "dlq_reason_work" }
  let(:dlq_name) { config.dead_letter_queue_name(queue) }
  let(:executor) { Pgbus::ActiveJob::Executor.new(client: client) }
  let(:data_source) { Pgbus::Web::DataSource.new(client: client) }

  let(:failing_job) do
    queue_name = queue
    Class.new(ActiveJob::Base) do
      self.queue_adapter = :pgbus
      queue_as(queue_name)
      def self.name = "DeadLetterReasonSpec::CardJob"
      def perform(*) = raise(ArgumentError, "card declined")
    end
  end

  around do |example|
    original = config.max_retries
    config.max_retries = 2
    example.run
  ensure
    config.max_retries = original
  end

  before do
    ActiveJob::Base.logger = Logger.new(IO::NULL)
    stub_const("DeadLetterReasonSpec", Module.new)
    stub_const("DeadLetterReasonSpec::CardJob", failing_job)
    client.ensure_queue(queue)
  end

  # Read → execute → make visible again, until the executor dead-letters it.
  def drive_to_dlq
    10.times do
      message = client.read_message(queue, vt: 30)
      raise "no message on #{queue}" unless message

      result = executor.execute(message, queue)
      return message if result == :dead_lettered

      client.set_visibility_timeout(queue, message.msg_id.to_i, vt: 0)
    end
    raise "never dead-lettered"
  end

  def dlq_rows
    data_source.reset_cache!
    data_source.dlq_messages(dlq: dlq_name)
  end

  def headers_of(row) = JSON.parse(row[:headers])

  it "writes the reason, the attempts and the last error into the DLQ copy's headers" do
    started = Time.now.utc
    DeadLetterReasonSpec::CardJob.perform_later
    source = drive_to_dlq

    rows = dlq_rows
    expect(rows.size).to eq(1)
    block = headers_of(rows.first).fetch("pgbus_dead_letter")
    expect(block).to include(
      "version" => 1, "reason" => "max_retries_exceeded", "source" => "worker",
      "source_queue" => config.queue_name(queue), "attempts" => 3, "max_retries" => 2,
      "error_class" => "ArgumentError", "error_message" => "card declined", "error_attempt" => 2
    )
    expect(block["backtrace"]).not_to be_empty
    expect(block["backtrace"].lines.size).to be <= 10
    expect(Time.iso8601(block["dead_lettered_at"])).to be_between(started - 1, Time.now.utc + 1)
    expect(Pgbus::FailedEventRecorder.exists?(queue_name: queue, msg_id: source.msg_id)).to be(false)
  end

  it "strips the block on retry and counts every trip out of the DLQ" do
    DeadLetterReasonSpec::CardJob.perform_later
    drive_to_dlq

    first = dlq_rows.first
    expect(data_source.retry_dlq_message(dlq_name, first[:msg_id])).to be(true)
    live = client.read_message(queue, vt: 0)
    expect(JSON.parse(live.headers)).to eq("pgbus_dlq_retries" => 1)

    drive_to_dlq
    second = headers_of(dlq_rows.first)
    expect(second).not_to have_key("pgbus_dlq_retries")
    expect(second.fetch("pgbus_dead_letter")["retries_from_dlq"]).to eq(1)

    data_source.retry_dlq_message(dlq_name, dlq_rows.first[:msg_id])
    expect(JSON.parse(client.read_message(queue, vt: 0).headers)).to eq("pgbus_dlq_retries" => 2)
  end

  it "keeps the message's own headers next to the block" do
    client.send_message(queue, { "job_class" => "DeadLetterReasonSpec::CardJob", "job_id" => SecureRandom.uuid,
                                 "arguments" => [] }, headers: { "trace_id" => "t-1" })
    drive_to_dlq

    expect(headers_of(dlq_rows.first)).to include("trace_id" => "t-1", "pgbus_dead_letter" => a_kind_of(Hash))
  end

  it "round-trips a consumer's block through Client#move_to_dead_letter" do
    client.send_message(queue, { "event_id" => "e-1", "payload" => {}, "headers" => { "routing_key" => "a.b" } })
    message = client.read_message(queue, vt: 30)
    headers = Pgbus::DeadLetterHeader.build(
      existing: message.headers, reason: Pgbus::DeadLetterHeader::REASON_MAX_RETRIES, source: "consumer",
      source_queue: config.queue_name(queue), attempts: message.read_ct.to_i, max_retries: 2
    )

    client.move_to_dead_letter(queue, message, headers: headers)

    block = Pgbus::DeadLetterHeader.parse(dlq_rows.first[:headers])
    expect(block).to include("source" => "consumer", "attempts" => 1)
    expect(block.keys.grep(/\Aerror_/)).to be_empty
  end
end
