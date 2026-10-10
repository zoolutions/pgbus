# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::DeadLetterHeader do
  let(:now) { Time.utc(2026, 10, 10, 12, 0, 0) }
  let(:error) do
    {
      error_class: "Stripe::CardError",
      error_message: "Your card was declined",
      backtrace: "app/jobs/pay.rb:12\napp/jobs/pay.rb:5",
      retry_count: 4,
      failed_at: "2026-10-10T11:59:00.000000Z"
    }
  end

  def build(existing: nil, error: nil, **overrides)
    described_class.build(
      existing: existing, reason: described_class::REASON_MAX_RETRIES, source: "worker",
      source_queue: "pgbus_default", attempts: 6, max_retries: 5, error: error, now: now, **overrides
    )
  end

  def block_of(json) = JSON.parse(json).fetch(described_class::KEY)

  describe ".build" do
    it "returns a JSON string with the dead-letter block under the namespaced key" do
      block = block_of(build)

      expect(block).to eq(
        "version" => 1,
        "reason" => "max_retries_exceeded",
        "source" => "worker",
        "source_queue" => "pgbus_default",
        "attempts" => 6,
        "max_retries" => 5,
        "dead_lettered_at" => "2026-10-10T12:00:00.000000Z"
      )
    end

    it "adds the error fields from the last recorded failure" do
      block = block_of(build(error: error))

      expect(block).to include(
        "error_class" => "Stripe::CardError",
        "error_message" => "Your card was declined",
        "backtrace" => "app/jobs/pay.rb:12\napp/jobs/pay.rb:5",
        "error_attempt" => 5,
        "error_recorded_at" => "2026-10-10T11:59:00.000000Z"
      )
    end

    it "serializes a Time failed_at as UTC ISO 8601" do
      block = block_of(build(error: error.merge(failed_at: Time.utc(2026, 10, 10, 11, 59))))

      expect(block["error_recorded_at"]).to eq("2026-10-10T11:59:00.000000Z")
    end

    it "has no error keys when no error was recorded" do
      expect(block_of(build).keys.grep(/\Aerror_|backtrace/)).to be_empty
    end

    it "truncates the message at 1000 chars and the backtrace at 10 lines / 2000 chars" do
      long = error.merge(error_message: "x" * 5_000, backtrace: (1..30).map { |i| "line #{i}" }.join("\n"))
      block = block_of(build(error: long))

      expect(block["error_message"].length).to eq(1_000)
      expect(block["backtrace"].lines.size).to eq(10)

      wide = error.merge(backtrace: Array.new(10) { "y" * 500 }.join("\n"))
      expect(block_of(build(error: wide))["backtrace"].length).to eq(2_000)
    end

    it "keeps every existing header key" do
      json = build(existing: '{"x-pgmq-group":"t1","trace_id":"abc"}')

      expect(JSON.parse(json)).to include("x-pgmq-group" => "t1", "trace_id" => "abc")
    end

    it "accepts existing headers as a Hash" do
      expect(JSON.parse(build(existing: { "trace_id" => "abc" }))).to include("trace_id" => "abc")
    end

    it "folds a live message's retry counter into the block and drops the top-level key" do
      parsed = JSON.parse(build(existing: '{"pgbus_dlq_retries":2}'))

      expect(parsed).not_to have_key(described_class::RETRIES_KEY)
      expect(parsed[described_class::KEY]["retries_from_dlq"]).to eq(2)
    end

    it "treats a non-numeric retry counter as zero instead of raising" do
      expect(block_of(build(existing: '{"pgbus_dlq_retries":{"x":1}}'))).not_to have_key("retries_from_dlq")
      expect(block_of(build(existing: '{"pgbus_dlq_retries":"3"}'))["retries_from_dlq"]).to eq(3)
      expect(block_of(build(existing: '{"pgbus_dlq_retries":"abc"}'))).not_to have_key("retries_from_dlq")
    end

    it "keeps non-object JSON headers under pgbus_original_headers, with a warning" do
      allow(Pgbus.logger).to receive(:warn)

      expect(JSON.parse(build(existing: "[1,2]"))).to include("pgbus_original_headers" => [1, 2])
      expect(Pgbus.logger).to have_received(:warn)
    end

    it "keeps malformed headers verbatim under pgbus_original_headers, with a warning" do
      allow(Pgbus.logger).to receive(:warn)

      expect(JSON.parse(build(existing: "{not json"))).to include("pgbus_original_headers" => "{not json")
      expect(Pgbus.logger).to have_received(:warn)
    end
  end

  describe ".parse" do
    it "returns the block as a string-keyed Hash" do
      expect(described_class.parse(build)).to include("reason" => "max_retries_exceeded", "attempts" => 6)
    end

    it "accepts a Hash" do
      expect(described_class.parse(JSON.parse(build))).to include("source" => "worker")
    end

    it "returns nil for nil, a header without the key, malformed JSON and a non-object block" do
      expect(described_class.parse(nil)).to be_nil
      expect(described_class.parse('{"trace_id":"abc"}')).to be_nil
      expect(described_class.parse("{not json")).to be_nil
      expect(described_class.parse('{"pgbus_dead_letter":"oops"}')).to be_nil
      expect(described_class.parse("[1]")).to be_nil
    end
  end

  describe ".strip_for_retry" do
    it "removes the block, sets the retry counter, keeps every other key" do
      parsed = JSON.parse(described_class.strip_for_retry(build(existing: '{"trace_id":"abc"}')))

      expect(parsed).to eq("trace_id" => "abc", "pgbus_dlq_retries" => 1)
    end

    it "restarts the counter when the block's retries_from_dlq is not a number" do
      expect(JSON.parse(described_class.strip_for_retry('{"pgbus_dead_letter":{"retries_from_dlq":[1]}}')))
        .to eq("pgbus_dlq_retries" => 1)
    end

    it "logs malformed headers without their content" do
      allow(Pgbus.logger).to receive(:warn) { |&blk| expect(blk.call).not_to include("s3cret") }

      build(existing: "{not json s3cret")
      expect(Pgbus.logger).to have_received(:warn)
    end

    it "starts the counter at 1 for nil or legacy headers" do
      expect(JSON.parse(described_class.strip_for_retry(nil))).to eq("pgbus_dlq_retries" => 1)
      expect(JSON.parse(described_class.strip_for_retry('{"trace_id":"t"}')))
        .to eq("trace_id" => "t", "pgbus_dlq_retries" => 1)
    end

    it "counts across the whole die → retry → die → retry lifecycle" do
      first_death = build(existing: "{}")
      expect(block_of(first_death)).not_to have_key("retries_from_dlq")

      first_retry = described_class.strip_for_retry(first_death)
      expect(JSON.parse(first_retry)).to eq("pgbus_dlq_retries" => 1)

      second_death = build(existing: first_retry)
      expect(JSON.parse(second_death)).not_to have_key("pgbus_dlq_retries")
      expect(block_of(second_death)["retries_from_dlq"]).to eq(1)

      second_retry = described_class.strip_for_retry(second_death)
      expect(JSON.parse(second_retry)).to eq("pgbus_dlq_retries" => 2)
    end
  end
end
