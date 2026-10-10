# frozen_string_literal: true

# Inject rich stub data for dashboard QA when PGBUS_STUB_DATA=1. The same
# sample data the accessibility gate audits (spec/system/accessibility_spec.rb),
# so the dummy server, Lighthouse and the PR screenshots show what CI checks.
if ENV["PGBUS_STUB_DATA"] == "1"
  Rails.application.config.after_initialize do
    require_relative "../../../support/pgbus/stub_data_source"
    Pgbus.configure do |c|
      c.web_data_source = Pgbus::Test::StubDataSource.new.tap(&:fill_sample_data!)
      c.web_refresh_interval = 5000
    end
    Rails.logger.info "[Pgbus Dummy] Stub data source loaded with sample data"
  end
end
