# frozen_string_literal: true

require "spec_helper"
require "action_view"
require "active_support/testing/time_helpers"

require_relative "../../../app/helpers/pgbus/application_helper"
require_relative "../../../app/helpers/pgbus/button_helper"
require_relative "../../../lib/pgbus/web/payload_filter"

RSpec.describe Pgbus::ApplicationHelper do
  include ActiveSupport::Testing::TimeHelpers

  let(:helper) do
    Class.new do
      include ActionView::Helpers::TagHelper
      include ActionView::Helpers::OutputSafetyHelper
      include ActionView::Helpers::TranslationHelper
      include Pgbus::ApplicationHelper
    end.new
  end

  before(:all) do # rubocop:disable RSpec/BeforeAfterAll
    I18n.load_path |= Dir[File.expand_path("../../../config/locales/*.yml", __dir__)]
    I18n.backend.reload!
  end

  around do |example|
    I18n.with_locale(:en) { example.run }
  end

  # Issue #497: one <time> family, always in Time.zone.
  describe "time helpers" do
    let(:now) { Time.utc(2026, 10, 10, 0, 8, 54) }

    around do |example|
      Time.use_zone("Europe/Stockholm") { travel_to(now) { example.run } }
    end

    describe "#pgbus_time" do
      it "renders the relative time with the UTC value and the absolute in Time.zone" do
        expect(helper.pgbus_time(now - 30))
          .to eq('<time datetime="2026-10-10T00:08:24Z" title="2026-10-10 02:08:24 CEST">30s ago</time>')
      end

      it "adds the clock time for a future moment with clock: true" do
        html = helper.pgbus_time(now + 5400, clock: true)
        expect(html).to include(">in 1h (03:38)</time>", 'datetime="2026-10-10T01:38:54Z"')
      end

      it "accepts an ISO string" do
        expect(helper.pgbus_time("2026-10-10T00:09:54Z")).to include(">in 1m</time>")
      end

      it "renders a dash for nil and a blank string" do
        expect(helper.pgbus_time(nil)).to eq("—")
        expect(helper.pgbus_time("")).to eq("—")
        expect(helper.pgbus_timestamp(" ")).to eq("—")
        expect(helper.pgbus_absolute_time("")).to eq("—")
      end

      it "shows an unparseable value as given, not marked safe (the view escapes it)" do
        expect(helper.pgbus_time("<b>soon</b>")).to eq("<b>soon</b>")
        expect(helper.pgbus_time("<b>soon</b>")).not_to be_html_safe
      end
    end

    describe "#pgbus_timestamp" do
      it "renders the absolute in a <time> followed by the relative" do
        html = helper.pgbus_timestamp(now - 300)
        expect(html).to eq('<time datetime="2026-10-10T00:03:54Z">2026-10-10 02:03:54 CEST</time> (5m ago)')
        expect(html).to be_html_safe
      end

      it "renders a dash for nil" do
        expect(helper.pgbus_timestamp(nil)).to eq("—")
      end
    end

    describe "#pgbus_absolute_time" do
      it "renders only the absolute in a <time>" do
        expect(helper.pgbus_absolute_time(now))
          .to eq('<time datetime="2026-10-10T00:08:54Z">2026-10-10 02:08:54 CEST</time>')
      end
    end

    describe "#pgbus_job_reason" do
      def result(key, args)
        Pgbus::Web::JobState::Result.new(state: "running", reason_key: key, reason_args: args,
                                         next_run_at: nil, badge_tone: :indigo)
      end

      it "renders both moments of a running job as <time> elements" do
        html = helper.pgbus_job_reason(result("running", { ago: now - 12, time: now + 48 }))
        expect(html).to be_html_safe
        expect(html.scan("<time ").size).to eq(2)
        expect(html).to include("Claimed ", ">12s ago</time>", ">in 48s (02:09)</time>")
      end

      it "escapes every argument that is not a time" do
        html = helper.pgbus_job_reason(result("retrying", { attempt: 2, max: 5, error: "<script>x</script>",
                                                            time: now + 60 }))
        expect(html).to include("&lt;script&gt;x&lt;/script&gt;")
        expect(html).not_to include("<script>")
      end
    end

    it "returns plain Strings for durations, for non-_html keys" do
      expect(helper.pgbus_duration(125)).to eq("2m 5s")
      expect(helper.pgbus_duration(125)).not_to be_html_safe
      expect(helper.pgbus_ms_duration(1500)).not_to be_html_safe
    end

    it "reads the range label from the locale" do
      I18n.with_locale(:sv) { expect(helper.pgbus_time_range_label(120)).to eq("2 timmar") }
    end
  end

  describe "#pgbus_number" do
    it "returns 0 for nil" do
      expect(helper.pgbus_number(nil)).to eq("0")
    end

    it "formats small numbers" do
      expect(helper.pgbus_number(42)).to eq("42")
    end

    it "formats thousands" do
      expect(helper.pgbus_number(1500)).to eq("1.5K")
    end

    it "formats millions" do
      expect(helper.pgbus_number(2_500_000)).to eq("2.5M")
    end
  end

  describe "#pgbus_worker_rates" do
    it "returns nil when the metadata carries no rates" do
      expect(helper.pgbus_worker_rates({ "queues" => %w[default] })).to be_nil
      expect(helper.pgbus_worker_rates(nil)).to be_nil
    end

    it "renders the known rate keys as a human-readable string" do
      rates = { "processed" => 12.4, "failed" => 0.2, "dequeued" => 10.1 }

      result = helper.pgbus_worker_rates({ "rates" => rates })

      expect(result).to include("12.4/s")
      expect(result).to include("0.2/s")
      expect(result).to include("10.1/s")
    end

    it "omits zero rates to keep the label compact" do
      rates = { "processed" => 3.0, "failed" => 0.0, "dequeued" => 0.0 }

      result = helper.pgbus_worker_rates({ "rates" => rates })

      expect(result).to include("3.0/s")
      expect(result).not_to include("0.0/s")
    end

    it "returns nil when all rates are zero" do
      rates = { "processed" => 0.0, "failed" => 0.0, "dequeued" => 0.0 }

      expect(helper.pgbus_worker_rates({ "rates" => rates })).to be_nil
    end
  end

  describe "#pgbus_display_metadata" do
    it "returns an empty hash for non-hash input" do
      expect(helper.pgbus_display_metadata(nil)).to eq({})
      expect(helper.pgbus_display_metadata("nope")).to eq({})
    end

    it "strips the throughput and internal keys, keeping the rest" do
      metadata = {
        "queues" => %w[default], "threads" => 5, "pid" => 1234,
        "rates" => { "processed" => 1.0 }, "jobs_processed" => 10,
        "jobs_failed" => 1, "in_flight" => 2, "loop_tick_at" => 123.4
      }

      result = helper.pgbus_display_metadata(metadata)

      expect(result).to eq("queues" => %w[default], "threads" => 5, "pid" => 1234)
    end

    it "handles symbol keys for the internal names" do
      metadata = { queues: %w[default], rates: { "processed" => 1.0 }, in_flight: 3 }

      expect(helper.pgbus_display_metadata(metadata)).to eq(queues: %w[default])
    end
  end

  describe "#pgbus_duration" do
    it "returns dash for nil" do
      expect(helper.pgbus_duration(nil)).to eq("—")
    end

    it "formats seconds" do
      expect(helper.pgbus_duration(45)).to eq("45s")
    end

    it "formats minutes and seconds" do
      expect(helper.pgbus_duration(125)).to eq("2m 5s")
    end

    it "formats hours and minutes" do
      expect(helper.pgbus_duration(3725)).to eq("1h 2m")
    end

    it "formats days and hours" do
      expect(helper.pgbus_duration(90_000)).to eq("1d 1h")
    end
  end

  describe "#pgbus_ms_duration" do
    it "returns dash for nil" do
      expect(helper.pgbus_ms_duration(nil)).to eq("—")
    end

    it "formats milliseconds" do
      expect(helper.pgbus_ms_duration(42)).to eq("42ms")
    end

    it "formats seconds" do
      expect(helper.pgbus_ms_duration(1500)).to eq("1.5s")
    end

    it "formats minutes" do
      expect(helper.pgbus_ms_duration(120_000)).to eq("2.0m")
    end

    it "formats exact second boundary" do
      expect(helper.pgbus_ms_duration(1000)).to eq("1.0s")
    end

    it "formats exact minute boundary" do
      expect(helper.pgbus_ms_duration(60_000)).to eq("1.0m")
    end

    it "rounds just below minute boundary" do
      expect(helper.pgbus_ms_duration(59_950)).to eq("60.0s")
    end
  end

  describe "#pgbus_time_range_label" do
    it "returns '1 hour' for 60 minutes" do
      expect(helper.pgbus_time_range_label(60)).to eq("1 hour")
    end

    it "returns '24 hours' for 1440 minutes" do
      expect(helper.pgbus_time_range_label(1440)).to eq("24 hours")
    end

    it "returns '7 days' for 10080 minutes" do
      expect(helper.pgbus_time_range_label(10_080)).to eq("7 days")
    end

    it "returns '30 days' for 43200 minutes" do
      expect(helper.pgbus_time_range_label(43_200)).to eq("30 days")
    end

    it "returns hours for sub-day ranges" do
      expect(helper.pgbus_time_range_label(360)).to eq("6 hours")
    end

    it "returns minutes for sub-hour ranges" do
      expect(helper.pgbus_time_range_label(15)).to eq("15 minutes")
    end

    it "returns singular minute for 1" do
      expect(helper.pgbus_time_range_label(1)).to eq("1 minute")
    end

    it "clamps zero to 1 minute" do
      expect(helper.pgbus_time_range_label(0)).to eq("1 minute")
    end

    it "clamps negative to 1 minute" do
      expect(helper.pgbus_time_range_label(-5)).to eq("1 minute")
    end

    it "keeps non-divisible hour values as minutes" do
      expect(helper.pgbus_time_range_label(90)).to eq("90 minutes")
    end

    it "keeps non-divisible day values as hours" do
      expect(helper.pgbus_time_range_label(1500)).to eq("25 hours")
    end

    it "falls back to minutes for values not divisible by 60 above a day" do
      expect(helper.pgbus_time_range_label(1530)).to eq("1530 minutes")
    end
  end

  describe "#pgbus_json_preview" do
    it "returns dash for nil" do
      expect(helper.pgbus_json_preview(nil)).to eq("—")
    end

    it "truncates long strings" do
      long = "a" * 200
      expect(helper.pgbus_json_preview(long, max_length: 50).length).to eq(53) # 50 + "..."
    end

    it "passes short strings through" do
      expect(helper.pgbus_json_preview("short")).to eq("short")
    end

    it "filters sensitive keys in JSON string input" do
      json = '{"password":"s3cret","name":"Alice"}'
      result = helper.pgbus_json_preview(json)
      expect(result).to include("[FILTERED]")
      expect(result).not_to include("s3cret")
      expect(result).to include("Alice")
    end

    it "filters sensitive keys in Hash input" do
      hash = { "password" => "s3cret", "name" => "Alice" }
      result = helper.pgbus_json_preview(hash)
      expect(result).to include("[FILTERED]")
      expect(result).not_to include("s3cret")
    end
  end

  describe "#pgbus_parse_message (filtering)" do
    it "filters sensitive keys in parsed hash" do
      message = '{"job_class":"MyJob","arguments":[{"password":"s3cret","user":"alice"}]}'
      result = helper.pgbus_parse_message(message)

      expect(result["job_class"]).to eq("MyJob")
      expect(result["arguments"].first["password"]).to eq("[FILTERED]")
      expect(result["arguments"].first["user"]).to eq("alice")
    end

    it "filters sensitive keys in hash input" do
      message = { "token" => "abc123", "name" => "visible" }
      result = helper.pgbus_parse_message(message)

      expect(result["token"]).to eq("[FILTERED]")
      expect(result["name"]).to eq("visible")
    end

    it "returns {} for nil without filtering" do
      expect(helper.pgbus_parse_message(nil)).to eq({})
    end

    it "returns {} for unparseable JSON" do
      expect(helper.pgbus_parse_message("not json")).to eq({})
    end

    context "when filtering is disabled" do
      before { Pgbus.configuration.dashboard_filter_sensitive = false }
      after { Pgbus.configuration.dashboard_filter_sensitive = true }

      it "does not filter sensitive keys" do
        message = { "password" => "s3cret" }
        result = helper.pgbus_parse_message(message)
        expect(result["password"]).to eq("s3cret")
      end
    end
  end

  describe "dead-letter helpers (issue #495)" do
    let(:link_helper) do
      Class.new do
        include ActionView::Helpers::TagHelper
        include ActionView::Helpers::OutputSafetyHelper
        include ActionView::Helpers::TranslationHelper
        include ActionView::Helpers::UrlHelper
        include Pgbus::ButtonHelper
        include Pgbus::ApplicationHelper
      end.new
    end
    let(:error) { { error_class: "Stripe::CardError", error_message: "<b>declined</b>", retry_count: 4 } }

    def reason(headers)
      Pgbus::Web::DeadLetterReason.present({ queue_name: "pgbus_default_dlq", enqueued_at: nil, headers: headers })
    end

    def dead(error: nil, existing: nil)
      Pgbus::DeadLetterHeader.build(existing: existing, reason: "max_retries_exceeded", source: "worker",
                                    source_queue: "pgbus_default", attempts: 6, max_retries: 5, error: error)
    end

    it "says what killed the job and escapes the error message" do
      html = helper.pgbus_dead_letter_reason(reason(dead(error: error)))

      expect(html).to be_html_safe
      expect(html).to include("Last error: Stripe::CardError: &lt;b&gt;declined&lt;/b&gt;")
    end

    it "links the error class to the filtered list when given a filter path" do
      path = ->(extra) { "/pgbus/dlq?error_class=#{extra[:error_class]}" }
      html = link_helper.pgbus_dead_letter_reason(reason(dead(error: error)), filter_path: path)

      expect(html).to include('href="/pgbus/dlq?error_class=Stripe::CardError"', ">Stripe::CardError</a>")
    end

    it "appends how often the message came back out of the DLQ" do
      expect(helper.pgbus_dead_letter_reason(reason(dead(existing: '{"pgbus_dlq_retries":2}'))))
        .to include("Retried from the DLQ 2 times before.")
    end

    it "explains a legacy row" do
      expect(helper.pgbus_dead_letter_reason(reason(nil)))
        .to eq("Reason not recorded (dead-lettered before pgbus #{Pgbus::DeadLetterHeader::SINCE})")
    end

    it "shows attempts against the retry limit, or a dash for a legacy row" do
      expect(helper.pgbus_dead_letter_attempts(reason(dead))).to eq("6/5")
      expect(helper.pgbus_dead_letter_attempts(reason(nil))).to eq("—")
    end

    it "names a job by class and an event by routing key, from the raw message" do
      expect(helper.pgbus_dead_letter_job('{"job_class":"PayJob"}')).to eq("PayJob")
      expect(helper.pgbus_dead_letter_job('{"headers":{"routing_key":"orders.created"}}')).to eq("orders.created")
      expect(helper.pgbus_dead_letter_job('{"routing_key":"orders.paid"}')).to eq("orders.paid")
      expect(helper.pgbus_dead_letter_job('{"headers":"x"}')).to eq("—")
      expect(helper.pgbus_dead_letter_job("not json")).to eq("—")
      expect(helper.pgbus_dead_letter_job(nil)).to eq("—")
    end
  end
end
