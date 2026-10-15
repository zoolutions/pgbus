# frozen_string_literal: true

# spec/pgbus/boot_finalize_spec.rb: boots pgbus in this fresh process, with
# Rails (the dummy app, configured from an initializer) or without (a plain
# `require "pgbus"` and Pgbus.configure), and prints KEY=value lines about the
# configuration the boot leaves. Nothing here calls Pgbus.finalize!.

# Values no Ractor can share, as an app may configure them: a lambda closing
# over app state, a Logger, a filter list holding a proc.
def configure_unshareable(config, seen)
  config.logger = Logger.new(IO::NULL)
  config.web_auth = lambda { |request|
    seen << request
    true
  }
  config.fair_share = ->(queues) { queues.to_h { |queue| [queue, seen.size] } }
  config.web_filter_parameters = [:token, ->(key, value) { value.replace("[FILTERED]") if key == "card" }]
  config.streams_durable_patterns = [+"orders"]
end

seen = []
configured = {}

case ARGV.first
when "rails"
  ENV["RAILS_ENV"] = "test"
  require_relative "../spec/dummy/config/application"

  # Where an app's config/initializers/pgbus.rb runs. Its own after_initialize
  # block still changes a collection in place: the engine finalizes after it.
  Dummy::Application.initializer "pgbus_spec.unshareable_config", before: :load_config_initializers do |app|
    Pgbus.configure { |c| configure_unshareable(c, seen) }
    configured.merge!(web_auth: Pgbus.configuration.web_auth, logger: Pgbus.configuration.logger,
                      filters: Pgbus.configuration.web_filter_parameters)
    app.config.after_initialize { Pgbus.configuration.streams_durable_patterns << +"late" }
  end
  Rails.application.initialize!
when "plain"
  require "logger"
  require "pgbus"
  Pgbus.configure { |c| configure_unshareable(c, seen) }
  configured.merge!(web_auth: Pgbus.configuration.web_auth, logger: Pgbus.configuration.logger,
                    filters: Pgbus.configuration.web_filter_parameters)
else
  abort "usage: boot_finalized.rb rails|plain"
end

config = Pgbus.configuration
puts "RAILS=#{defined?(Rails::Application) ? true : false}"
puts "FINALIZED=#{config.finalized?}"
puts "SHAREABLE=#{[config.streams_durable_patterns, config.workers, config.queue_prefix].all? { Ractor.shareable?(_1) }}"
puts "LATE_PATTERN=#{config.streams_durable_patterns.include?("late")}"
puts "CALLABLES_KEPT=#{config.web_auth.equal?(configured[:web_auth]) && config.fair_share.respond_to?(:call)}"
puts "AUTH_CALLED=#{config.web_auth.call(:request) && seen == [:request]}"
puts "LOGGER_KEPT=#{config.logger.equal?(configured[:logger])}"
puts "FILTERS_KEPT=#{config.web_filter_parameters.equal?(configured[:filters]) && !config.web_filter_parameters.frozen?}"
