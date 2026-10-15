# frozen_string_literal: true

ENV["RAILS_ENV"] = "test"
ENV["PGBUS_CUSTOM_BASE"] = "1"

require_relative "../spec/dummy/config/environment"
require_relative "../spec/support/pgbus/stub_data_source"

source = Pgbus::Test::StubDataSource.new
source.processes_list = source.sample_processes(Time.now)
Pgbus.configure do |config|
  config.web_live_updates = false
  config.web_refresh_interval = 0
  config.web_data_source = source
end

puts "SUPERCLASS=#{Pgbus::ApplicationController.superclass.name}"
host = Pgbus::DashboardController.renderer.render(inline: "<%= pgbus_spec_host_helper %>")
puts "HOST=#{host}"
helpers = Pgbus::DashboardController._helpers.ancestors.filter_map(&:name).grep(/\APgbus::\w+Helper\z/)
puts helpers.sort

session = ActionDispatch::Integration::Session.new(Rails.application)
session.get("/pgbus")
puts "STATUS=#{session.response.status}"
puts session.response.body
exit(session.response.status == 200 ? 0 : 1)
