# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::CLI::DLQ do
  let(:data_source) { instance_double(Pgbus::Web::DataSource) }

  let(:dlq_message) do
    {
      msg_id: 42,
      read_ct: 5,
      enqueued_at: "2026-01-01 00:00:00",
      last_read_at: "2026-01-02 00:00:00",
      vt: "2026-01-02 00:05:00",
      message: { "job_class" => "MyJob", "password" => "hunter2" },
      headers: {},
      queue_name: "pgbus_default_dlq"
    }
  end

  def capture(argv)
    original = $stdout
    $stdout = StringIO.new
    described_class.start(argv, data_source: data_source)
    $stdout.string
  ensure
    $stdout = original
  end

  describe "list" do
    it "renders rows with msg_id, DLQ queue, origin queue, read_ct, enqueued_at" do
      allow(data_source).to receive(:dlq_messages).with(page: 1, per_page: 25).and_return([dlq_message])
      allow(data_source).to receive(:dlq_total_count).and_return(1)

      output = capture(%w[list])

      expect(output).to include("42")
      expect(output).to include("pgbus_default_dlq")
      expect(output).to include("pgbus_default")
      expect(output).to include("5")
    end

    it "prints the total count footer from dlq_total_count" do
      allow(data_source).to receive_messages(dlq_messages: [dlq_message], dlq_total_count: 7)

      expect(capture(%w[list])).to include("7")
    end

    it "never prints payloads in list output" do
      allow(data_source).to receive_messages(dlq_messages: [dlq_message], dlq_total_count: 1)

      expect(capture(%w[list])).not_to include("hunter2")
    end

    it "honors --page and --per-page" do
      allow(data_source).to receive(:dlq_messages).with(page: 3, per_page: 50).and_return([])
      allow(data_source).to receive(:dlq_total_count).and_return(0)

      capture(%w[list --page 3 --per-page 50])

      expect(data_source).to have_received(:dlq_messages).with(page: 3, per_page: 50)
    end

    it "prints an empty-state message when there are no DLQ messages" do
      allow(data_source).to receive_messages(dlq_messages: [], dlq_total_count: 0)

      expect(capture(%w[list])).to match(/no dead-letter messages/i)
    end
  end

  describe "why a message died (issue #495)" do
    let(:dead_message) do
      dlq_message.merge(
        headers: Pgbus::DeadLetterHeader.build(
          existing: nil, reason: "max_retries_exceeded", source: "worker", source_queue: "pgbus_default_p2",
          attempts: 6, max_retries: 5,
          error: { error_class: "Stripe::CardError", error_message: "Your card was declined", retry_count: 4 }
        )
      )
    end

    it "adds a reason column to the list: the error class, or the reason key" do
      legacy = dlq_message.merge(msg_id: 43, headers: nil)
      no_error = dlq_message.merge(msg_id: 44, headers: Pgbus::DeadLetterHeader.build(
        existing: nil, reason: "max_retries_exceeded", source: "consumer", source_queue: "pgbus_events",
        attempts: 6, max_retries: 5
      ))
      allow(data_source).to receive_messages(dlq_messages: [dead_message, legacy, no_error], dlq_total_count: 3)

      output = capture(%w[list])

      expect(output).to include("REASON")
      expect(output.lines.find { |l| l.start_with?("42 ") }).to include("Stripe::CardError")
      expect(output.lines.find { |l| l.start_with?("43 ") }).to include("not_recorded")
      expect(output.lines.find { |l| l.start_with?("44 ") }).to include("event_no_error")
    end

    it "prints died, attempts, source queue, reason and error on show" do
      allow(data_source).to receive(:dlq_message_detail).with("42").and_return(dead_message)

      output = capture(%w[show 42])

      expect(output).to include("died:", "attempts:    6/5", "source:      pgbus_default_p2",
                                "reason:      max_retries_exceeded",
                                "error:       Stripe::CardError: Your card was declined (attempt 5)")
    end

    it "escapes control characters in the stored error message" do
      evil = dead_message.merge(headers: Pgbus::DeadLetterHeader.build(
        existing: nil, reason: "max_retries_exceeded", source: "worker", source_queue: "pgbus_default",
        attempts: 6, max_retries: 5,
        error: { error_class: "RuntimeError", error_message: "line1\nline2\e[31mred", retry_count: 4 }
      ))
      allow(data_source).to receive(:dlq_message_detail).with("42").and_return(evil)

      output = capture(%w[show 42])

      expect(output).to include('error:       RuntimeError: line1\x0Aline2\x1B[31mred (attempt 5)')
      expect(output).not_to include("\e[31m")
    end

    it "says the reason was not recorded for a legacy message" do
      allow(data_source).to receive(:dlq_message_detail).with("42").and_return(dlq_message.merge(headers: nil))

      expect(capture(%w[show 42])).to include("reason:      not recorded (dead-lettered before pgbus")
    end
  end

  describe "show" do
    it "prints the payload filtered by PayloadFilter" do
      allow(data_source).to receive(:dlq_message_detail).with("42").and_return(dlq_message)
      allow(Pgbus::Web::PayloadFilter).to receive(:filter_json)
        .with(dlq_message[:message]).and_return('{"job_class":"MyJob","password":"[FILTERED]"}')

      output = capture(%w[show 42])

      expect(Pgbus::Web::PayloadFilter).to have_received(:filter_json).with(dlq_message[:message])
      expect(output).to include("[FILTERED]")
      expect(output).not_to include("hunter2")
    end

    it "prints msg_id, queue, and read_ct metadata" do
      allow(data_source).to receive(:dlq_message_detail).and_return(dlq_message)
      allow(Pgbus::Web::PayloadFilter).to receive(:filter_json).and_return("{}")

      output = capture(%w[show 42])

      expect(output).to include("42")
      expect(output).to include("pgbus_default_dlq")
    end

    it "exits 1 for an unknown msg_id" do
      allow(data_source).to receive(:dlq_message_detail).with("999").and_return(nil)

      expect { described_class.start(%w[show 999], data_source: data_source) }
        .to raise_error(SystemExit) { |e| expect(e.status).to eq(1) }
    end
  end

  describe "retry" do
    it "resolves the DLQ queue via detail then retries with full queue name and msg_id" do
      allow(data_source).to receive(:dlq_message_detail).with("42").and_return(dlq_message)
      allow(data_source).to receive(:retry_dlq_message).with("pgbus_default_dlq", 42).and_return(true)

      capture(%w[retry 42])

      expect(data_source).to have_received(:retry_dlq_message).with("pgbus_default_dlq", 42)
    end

    it "exits 1 when the msg_id does not exist" do
      allow(data_source).to receive(:dlq_message_detail).with("999").and_return(nil)

      expect { described_class.start(%w[retry 999], data_source: data_source) }
        .to raise_error(SystemExit) { |e| expect(e.status).to eq(1) }
    end
  end

  describe "retry-all" do
    it "prints the re-enqueued count returned by retry_all_dlq" do
      allow(data_source).to receive(:retry_all_dlq).and_return(3)

      expect(capture(%w[retry-all])).to include("3")
    end
  end

  describe "purge MSG_ID" do
    it "discards a single message via discard_dlq_message" do
      allow(data_source).to receive(:dlq_message_detail).with("42").and_return(dlq_message)
      allow(data_source).to receive(:discard_dlq_message).with("pgbus_default_dlq", 42).and_return(true)

      capture(%w[purge 42])

      expect(data_source).to have_received(:discard_dlq_message).with("pgbus_default_dlq", 42)
    end

    it "exits 1 for an unknown msg_id" do
      allow(data_source).to receive(:dlq_message_detail).with("999").and_return(nil)

      expect { described_class.start(%w[purge 999], data_source: data_source) }
        .to raise_error(SystemExit) { |e| expect(e.status).to eq(1) }
    end
  end

  describe "purge --all" do
    it "makes no DataSource call and exits 1 without --yes" do
      allow(data_source).to receive(:discard_all_dlq)

      expect { described_class.start(%w[purge --all], data_source: data_source) }
        .to raise_error(SystemExit) { |e| expect(e.status).to eq(1) }

      expect(data_source).not_to have_received(:discard_all_dlq)
    end

    it "discards all and prints the count with --all --yes" do
      allow(data_source).to receive(:discard_all_dlq).and_return(4)

      expect(capture(%w[purge --all --yes])).to include("4")
    end
  end

  describe "unknown subcommand" do
    it "prints help and exits 1" do
      expect { described_class.start(%w[bogus], data_source: data_source) }
        .to raise_error(SystemExit) { |e| expect(e.status).to eq(1) }
    end
  end
end
