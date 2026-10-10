# frozen_string_literal: true

require "spec_helper"
require "action_view"
require "active_support/testing/time_helpers"

require_relative "../../../app/helpers/pgbus/application_helper"
require_relative "../../../app/helpers/pgbus/events_helper"

RSpec.describe Pgbus::EventsHelper do
  include ActiveSupport::Testing::TimeHelpers

  let(:now) { Time.utc(2026, 10, 10, 12, 0, 0) }
  let(:helper) do
    Class.new do
      include ActionView::Helpers::TagHelper
      include ActionView::Helpers::OutputSafetyHelper
      include ActionView::Helpers::TranslationHelper
      include Pgbus::ApplicationHelper
      include Pgbus::EventsHelper
    end.new
  end

  before(:all) do # rubocop:disable RSpec/BeforeAfterAll
    I18n.load_path |= Dir[File.expand_path("../../../config/locales/*.yml", __dir__)]
    I18n.backend.reload!
  end

  around do |example|
    I18n.with_locale(:en) { Time.use_zone("UTC") { travel_to(now) { example.run } } }
  end

  def result(key, args, state: "running")
    Pgbus::Web::EventState::Result.new(state: state, reason_key: key, reason_args: args, next_run_at: nil,
                                       badge_tone: :indigo, handler_class: "OrderHandler")
  end

  describe "#pgbus_event_state_badge" do
    it "calls a running event Handling" do
      expect(helper.pgbus_event_state_badge("running")).to include("Handling", "bg-indigo-100")
    end
  end

  describe "#pgbus_event_reason" do
    it "renders both moments of a handled event as <time> and names the handler" do
      html = helper.pgbus_event_reason(result("handling", { handler: "OrderHandler", ago: now - 12, time: now + 48 }))

      expect(html).to be_html_safe
      expect(html.scan("<time ").size).to eq(2)
      expect(html).to include("Being handled by OrderHandler — claimed ", ">12s ago</time>", ">in 48s (12:00)</time>")
    end

    it "escapes the handler, the pattern and the error" do
      html = helper.pgbus_event_reason(result("no_consumer_for_queue", { pattern: "<b>x</b>", handler: "<i>H</i>" }))

      expect(html).to include("&lt;b&gt;x&lt;/b&gt;", "&lt;i&gt;H&lt;/i&gt;")
      expect(html).not_to include("<b>", "<i>")
    end
  end

  describe "processed events" do
    def processed(state, key, tone)
      Pgbus::Web::EventState::ProcessedResult.new(
        state: state, reason_key: key, reason_args: { ago: now - 300 }, badge_tone: tone
      )
    end

    it "badges a completed claim green" do
      expect(helper.pgbus_processed_event_badge(processed("completed", "completed", :green)))
        .to include("Completed", "bg-green-100")
    end

    it "says when a claim went silent" do
      expect(helper.pgbus_processed_event_reason(processed("abandoned", "abandoned", :yellow)))
        .to include("Claim went silent ", ">5m ago</time>", "the next delivery re-runs the handler")
    end
  end

  describe "#pgbus_event_routing_key" do
    it "reads the routing key unfiltered, from the headers or the envelope" do
      expect(helper.pgbus_event_routing_key('{"headers":{"routing_key":"orders.created"}}')).to eq("orders.created")
      expect(helper.pgbus_event_routing_key('{"routing_key":"orders.paid"}')).to eq("orders.paid")
    end

    it "is a dash for a payload without one" do
      expect(helper.pgbus_event_routing_key("{}")).to eq("—")
      expect(helper.pgbus_event_routing_key("{not json")).to eq("—")
      expect(helper.pgbus_event_routing_key(nil)).to eq("—")
    end
  end
end
