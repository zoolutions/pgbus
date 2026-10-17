# frozen_string_literal: true

require "spec_helper"
require "action_view"

require_relative "../../../app/helpers/pgbus/application_helper"
require_relative "../../../app/helpers/pgbus/button_helper"
require_relative "../../../app/helpers/pgbus/dashboard_helper"
require_relative "../../../app/helpers/pgbus/workflows_helper"
require_relative "../../../app/helpers/pgbus/workflow_insights_helper"

RSpec.describe Pgbus::WorkflowInsightsHelper do
  let(:helper) do
    Class.new do
      include ActionView::Helpers::TagHelper
      include ActionView::Helpers::UrlHelper
      include ActionView::Helpers::OutputSafetyHelper
      include ActionView::Helpers::TranslationHelper
      include ActionView::Helpers::NumberHelper
      include Pgbus::ApplicationHelper
      include Pgbus::ButtonHelper
      include Pgbus::DashboardHelper
      include Pgbus::WorkflowsHelper
      include Pgbus::WorkflowInsightsHelper
    end.new
  end

  before(:all) do # rubocop:disable RSpec/BeforeAfterAll
    I18n.load_path |= Dir[File.expand_path("../../../config/locales/*.yml", __dir__)]
    I18n.backend.reload!
  end

  around { |example| I18n.with_locale(:en) { example.run } }

  it "gives the chart each bucket on the dashboard zone's wall clock (the axis prints UTC)" do
    definition = { throughput: [{ from: Time.utc(2026, 10, 16, 22), started: 1, finished: 0, cancelled: 0 }],
                   duration_histogram: [] }

    payload = Time.use_zone("Europe/Stockholm") { helper.pgbus_insight_chart_payload(definition) }

    expect(payload[:throughput].sole[:time]).to eq("2026-10-17T00:00:00Z")
  end

  it "prints a duration in its largest unit under a minute, and in two units past it" do
    expect([helper.pgbus_insight_ms(820), helper.pgbus_insight_ms(48_200), helper.pgbus_insight_ms(5_400_000)])
      .to eq(["820ms", "48.2s", "1h 30m"])
  end

  it "notes few samples, and prints nothing without samples" do
    expect(helper.pgbus_insight_percentiles({ p50: 10.0, p95: 20.0, p99: 30.0, samples: 7 }))
      .to eq("p50 10ms · p95 20ms · p99 30ms (n=7, few samples)")
    expect(helper.pgbus_insight_percentiles({ p50: nil, p95: nil, p99: nil, samples: 0 })).to eq("—")
  end

  it "labels the duration bins" do
    expect([helper.pgbus_insight_bin_label({ from_ms: 0, to_ms: 1_000 }),
            helper.pgbus_insight_bin_label({ from_ms: 10_000, to_ms: 60_000 }),
            helper.pgbus_insight_bin_label({ from_ms: 604_800_000, to_ms: nil })]).to eq(["under 1s", "10s–1m", "7d or more"])
  end
end
