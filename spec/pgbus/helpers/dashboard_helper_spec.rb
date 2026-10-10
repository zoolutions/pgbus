# frozen_string_literal: true

require "spec_helper"
require "action_view"
require "active_support/testing/time_helpers"

require_relative "../../../app/helpers/pgbus/application_helper"
require_relative "../../../app/helpers/pgbus/dashboard_helper"

RSpec.describe Pgbus::DashboardHelper do
  include ActiveSupport::Testing::TimeHelpers

  let(:now) { Time.utc(2026, 10, 10, 12, 0, 0) }
  let(:helper) do
    Class.new do
      include ActionView::Helpers::TagHelper
      include ActionView::Helpers::OutputSafetyHelper
      include ActionView::Helpers::TranslationHelper
      include ActionView::Helpers::NumberHelper
      include Pgbus::ApplicationHelper
      include Pgbus::DashboardHelper
    end.new
  end

  before(:all) do # rubocop:disable RSpec/BeforeAfterAll
    I18n.load_path |= Dir[File.expand_path("../../../config/locales/*.yml", __dir__)]
    I18n.backend.reload!
  end

  around do |example|
    I18n.with_locale(:en) { Time.use_zone("UTC") { travel_to(now) { example.run } } }
  end

  def text(html) = Nokogiri::HTML.fragment(html.to_s).text

  describe "#pgbus_retrying_card_words" do
    let(:retrying) do
      { count: 3, capped: false, next_attempt_at: now + 40, due_now_count: 0, newest_error_class: "Net::ReadTimeout" }
    end

    it "says when the next attempt runs and which error came last" do
      expect(text(helper.pgbus_retrying_card_words(retrying)))
        .to eq("next attempt in 40s (12:00) · newest error Net::ReadTimeout")
    end

    it "counts the attempts already due" do
      words = helper.pgbus_retrying_card_words(retrying.merge(next_attempt_at: nil, due_now_count: 2))

      expect(text(words)).to eq("2 due now · newest error Net::ReadTimeout")
    end

    it "says nothing is retrying" do
      expect(helper.pgbus_retrying_card_words(retrying.merge(count: 0))).to eq("Nothing is retrying")
    end

    it "marks a capped count as a lower bound" do
      expect(helper.pgbus_retrying_count(retrying.merge(count: 10_000, capped: true))).to eq("10,000+")
    end
  end

  describe "#pgbus_dlq_card_words" do
    let(:message) { { job_class: "SendWelcomeEmailJob" }.to_json }

    def row(headers) = { msg_id: 7, queue_name: "pgbus_default_dlq", enqueued_at: now - 7200, headers: headers,
                         message: message }

    def block(error) = Pgbus::DeadLetterHeader.build(
      existing: nil, reason: Pgbus::DeadLetterHeader::REASON_MAX_RETRIES, source: "worker",
      source_queue: "pgbus_default", attempts: 6, max_retries: 5, now: now - 7200, error: error
    )

    it "names the newest error and job" do
      dlq = { depth: 2, newest: row(block(error_class: "RuntimeError", error_message: "boom", retry_count: 5)) }

      expect(text(helper.pgbus_dlq_card_words(dlq))).to eq("newest: RuntimeError in SendWelcomeEmailJob 2h ago")
    end

    it "says when no error was recorded" do
      expect(text(helper.pgbus_dlq_card_words({ depth: 1, newest: row(block(nil)) })))
        .to eq("newest: SendWelcomeEmailJob 2h ago — no error recorded")
    end

    it "says when the message predates the dead-letter reason" do
      expect(text(helper.pgbus_dlq_card_words({ depth: 1, newest: row(nil) })))
        .to eq("newest: SendWelcomeEmailJob 2h ago — no reason recorded")
    end

    it "says nothing died" do
      expect(helper.pgbus_dlq_card_words({ depth: 0, newest: nil })).to eq("Nothing dead-lettered")
    end
  end

  describe "#pgbus_health_words" do
    it "formats bloat thresholds as percentages and the rest as durations" do
      health = { total_dead_tuples: 12_000, worst_bloat_ratio: 0.215, oldest_vacuum_ago_sec: 7200,
                 oldest_transaction_age_sec: 45, tables: [{ table: "pgmq.q_pgbus_default", bloat_ratio: 0.215 }] }
      verdict = Pgbus::Web::HealthVerdict.present(health)

      expect(text(helper.pgbus_health_words(verdict.bloat))).to include("21.5% on pgmq.q_pgbus_default")
      expect(text(helper.pgbus_health_words(verdict.dead_tuples))).to start_with("Above 10,000")
      expect(text(helper.pgbus_health_words(verdict.vacuum))).to start_with("Over 1h 0m")
      expect(helper.pgbus_health_words(verdict.transaction)).to include("text-amber-700")
    end
  end

  describe "#pgbus_failure_job" do
    it "reads the job class from the payload, else the handler" do
      expect(helper.pgbus_failure_job({ "payload" => '{"job_class":"MailJob"}' })).to eq("MailJob")
      expect(helper.pgbus_failure_job({ "payload" => "{}", "handler_class" => "Billing::Handler" }))
        .to eq("Billing::Handler")
      expect(helper.pgbus_failure_job({ "payload" => "not json" })).to eq("—")
    end
  end
end
