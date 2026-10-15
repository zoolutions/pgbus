# frozen_string_literal: true

require "spec_helper"
require "open3"

RSpec.describe "Pgbus::DashboardController" do
  it "renders the processes table under a custom base controller" do
    stdout, stderr, status = Open3.capture3(
      { "PGBUS_CUSTOM_BASE" => "1" },
      "bundle", "exec", "ruby", File.expand_path("../../support/pgbus/custom_base_dashboard.rb", __dir__)
    )

    expect(status).to be_success, "#{stdout}\n#{stderr}"
    expect(stdout).to include("SUPERCLASS=PgbusSpecBaseController")
    expect(stdout).to include("HOST=true")
    expect(stdout).to include("no supervisor to restart it")
    helpers = stdout.lines.map(&:strip).grep(/\APgbus::\w+Helper\z/)
    expect(helpers).to eq(%w[
      Pgbus::ApplicationHelper Pgbus::ButtonHelper Pgbus::DashboardHelper
      Pgbus::EventsHelper Pgbus::InsightsHelper Pgbus::ProcessesHelper Pgbus::StreamsHelper
    ].sort)
  end
end
