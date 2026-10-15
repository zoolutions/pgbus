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
    expect(stdout).to include("no supervisor to restart it")
    %w[ApplicationHelper ButtonHelper DashboardHelper EventsHelper InsightsHelper ProcessesHelper StreamsHelper].each do |name|
      expect(stdout).to include("Pgbus::#{name}")
    end
  end
end
