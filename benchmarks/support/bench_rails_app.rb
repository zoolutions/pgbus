# frozen_string_literal: true

require "fileutils"
require "tmpdir"

# A minimal Rails application for the worker profile bench (issue #618),
# booted only with WP_BENCH_RAILS_APP=1. Without it Rails.application is nil
# and Pgbus::ActiveJob::Executor#execute_job runs `job.perform_now` bare;
# with it the job runs inside Rails.application.executor.wrap with the hooks
# ActiveRecord and ActiveSupport install there (query cache, connection
# return, CurrentAttributes reset), as in a host app. The pgbus engine is not
# loaded: the bench configures pgbus itself, before the app boots.
module BenchRailsApp
  module_function

  def boot!(database_url)
    return if defined?(::Rails.application) && ::Rails.application

    require "rails"
    require "active_record/railtie"
    require "active_job/railtie"

    # Rails reads the connection from DATABASE_URL when the app has no
    # config/database.yml; the root is an empty directory for that reason.
    ENV["DATABASE_URL"] = database_url
    root = Dir.mktmpdir("pgbus-wprof-app")
    at_exit { FileUtils.remove_entry(root, true) }
    app = Class.new(::Rails::Application) do
      config.load_defaults ::Rails::VERSION::STRING.to_f
      config.root = root
      config.eager_load = false
      config.enable_reloading = false if config.respond_to?(:enable_reloading=)
      config.cache_classes = true
      config.logger = Logger.new(IO::NULL)
      # Production's default: ActiveRecord's SQL lines stay unformatted, as in
      # a host app (with Rails out, ActiveRecord has no logger at all).
      config.log_level = :info
      config.secret_key_base = "pgbus-wprof"
    end
    Object.const_set(:PgbusWprofApp, app)
    app.initialize!
  end
end
