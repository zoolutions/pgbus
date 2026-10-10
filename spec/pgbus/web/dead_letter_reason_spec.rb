# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Web::DeadLetterReason do
  let(:died) { "2026-10-10T10:00:00Z" }
  let(:error) do
    { error_class: "Stripe::CardError", error_message: "Your card was declined",
      backtrace: "app/jobs/pay.rb:12\napp/jobs/pay.rb:5", retry_count: 4, failed_at: "2026-10-10T09:59:00.000000Z" }
  end

  def headers(source: "worker", error: nil, existing: nil, attempts: 6)
    Pgbus::DeadLetterHeader.build(existing: existing, reason: "max_retries_exceeded", source: source,
                                  source_queue: "pgbus_default_p2", attempts: attempts, max_retries: 5,
                                  error: error, now: Time.utc(2026, 10, 10, 10))
  end

  def row(headers) = { msg_id: 1, queue_name: "pgbus_default_dlq", enqueued_at: died, headers: headers }

  def present(headers) = described_class.present(row(headers))

  it "names the error from the last attempt" do
    result = present(headers(error: error))

    expect(result.reason_key).to eq("error")
    expect(result.reason_args).to eq(error_class: "Stripe::CardError", error_message: "Your card was declined",
                                     attempts: 6, max: 5)
    expect(result).to have_attributes(legacy?: false, source: "worker", source_queue: "pgbus_default_p2",
                                      attempts: 6, max_retries: 5, error_class: "Stripe::CardError",
                                      error_message: "Your card was declined", error_attempt: 5,
                                      backtrace: "app/jobs/pay.rb:12\napp/jobs/pay.rb:5")
  end

  it "truncates the message in the cell args but keeps it whole on the result" do
    result = present(headers(error: error.merge(error_message: "x" * 400)))

    expect(result.reason_args[:error_message].length).to be <= 160
    expect(result.error_message.length).to eq(400)
  end

  it "says when the recorded error came from an earlier attempt" do
    result = present(headers(error: error.merge(retry_count: 2)))

    expect(result.reason_key).to eq("error_earlier_attempt")
    expect(result.reason_args).to include(error_attempt: 3)
    expect(result.later_attempts).to eq(4..5)
  end

  it "has no later attempts when the error is from the last one" do
    expect(present(headers(error: error)).later_attempts).to be_nil
  end

  it "says no error was recorded for a worker without one" do
    result = present(headers)

    expect(result.reason_key).to eq("no_error_recorded")
    expect(result.reason_args).to eq(attempts: 6, max: 5)
  end

  it "says no handler error was recorded for a consumer without one" do
    result = present(headers(source: "consumer"))

    expect(result.reason_key).to eq("event_no_error")
    expect(result.reason_args).to eq(attempts: 6, max: 5)
  end

  it "names a consumer's recorded handler error like a job's" do
    expect(present(headers(source: "consumer", error: error)).reason_key).to eq("error")
  end

  it "carries how many times the message came back out of the DLQ" do
    result = present(headers(existing: '{"pgbus_dlq_retries":1}'))

    expect(result.retried_before).to eq(1)
  end

  it "has no retried_before for a first death" do
    expect(present(headers).retried_before).to be_nil
  end

  it "reads a row without the block as legacy" do
    result = present(nil)

    expect(result.reason_key).to eq("not_recorded")
    expect(result.reason_args).to eq(version: Pgbus::DeadLetterHeader::SINCE)
    expect(result).to have_attributes(legacy?: true, attempts: nil, source_queue: "pgbus_default", source: nil)
  end

  it "reads unrelated headers as legacy too" do
    expect(present('{"trace_id":"t"}')).to be_legacy
  end

  it "dates the death from the DLQ row's enqueued_at" do
    expect(present(headers).died_at).to eq(died)
    expect(present(nil).died_at).to eq(died)
  end
end
