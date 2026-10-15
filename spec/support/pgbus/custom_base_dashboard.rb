# frozen_string_literal: true

# rubocop:disable RSpec/Output -- process entry point; stdout is the spec's result channel

# spec_helper requires this file. The boot has to stay behind the guard
# because ApplicationController freezes its superclass on first load.
if $PROGRAM_NAME == __FILE__
  ENV["RAILS_ENV"] = "test"
  ENV["PGBUS_CUSTOM_BASE"] = "1"

  require_relative "../../dummy/config/environment"
  require_relative "stub_data_source"

  source = Pgbus::Test::StubDataSource.new
  source.processes_list = source.sample_processes(Time.now)
  Pgbus.configure do |config|
    config.web_live_updates = false
    config.web_refresh_interval = 0
    config.web_data_source = source
  end

  puts "SUPERCLASS=#{Pgbus::ApplicationController.superclass.name}"
  puts "HOST=#{Pgbus::DashboardController._helpers.method_defined?(:pgbus_spec_host_helper)}"
  helpers = Pgbus::DashboardController._helpers.ancestors.filter_map(&:name).grep(/\APgbus::\w+Helper\z/)
  puts helpers.sort

  session = ActionDispatch::Integration::Session.new(Rails.application)
  session.get("/pgbus")
  puts "STATUS=#{session.response.status}"
  puts session.response.body
  exit(session.response.status == 200 ? 0 : 1)
end
# rubocop:enable RSpec/Output
