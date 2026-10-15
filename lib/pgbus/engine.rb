# frozen_string_literal: true

require "rails/engine"

module Pgbus
  class Engine < ::Rails::Engine
    isolate_namespace Pgbus

    # When pgbus was loaded before Rails, a separate Zeitwerk loader manages
    # app/models. Tear it down before Rails' autoloader claims the same paths.
    initializer "pgbus.release_models_loader", before: :set_autoload_paths do
      Pgbus.teardown_models_loader!
    end

    # config/pgbus.yml is no longer loaded (removed in 1.0). If an app still
    # ships one, it's now silently inert — warn once at boot so the leftover
    # file doesn't quietly diverge from the real (Ruby initializer) config.
    initializer "pgbus.legacy_yaml_warning" do |app|
      ActiveSupport.on_load(:after_initialize) do
        Pgbus::Engine.warn_if_legacy_yaml_config(app.root, Pgbus.logger)
      end
    end

    # Emits the legacy-YAML deprecation warning when +root+/config/pgbus.yml
    # exists. Extracted so it can be unit-tested without booting Rails. A nil
    # logger is a no-op (logger may be unset in a bare/embedded boot).
    def self.warn_if_legacy_yaml_config(root, logger)
      return if logger.nil?

      config_path = root.join("config", "pgbus.yml")
      return unless config_path.exist?

      logger.warn do
        "[Pgbus] config/pgbus.yml is no longer loaded (YAML config was removed in 1.0). " \
          "Port its settings into config/initializers/pgbus.rb as a Pgbus.configure block " \
          "and delete the YAML. See https://pgbus.zoolutions.llc/docs/configuration " \
          "(the pgbus MCP server can also help port it)."
      end
    end

    initializer "pgbus.recurring" do |app|
      next if Pgbus.configuration.recurring_tasks

      config = Pgbus.configuration
      files = config.recurring_tasks_files
      default_path = app.root.join("config", "recurring.yml")

      if files
        tasks = Pgbus::Recurring::ConfigLoader.load_all(files)
        if tasks.empty? && default_path.exist? && files.none? { |f| File.expand_path(f.to_s) == File.expand_path(default_path.to_s) }
          tasks = Pgbus::Recurring::ConfigLoader.load(default_path)
          config.recurring_tasks_file ||= default_path.to_s
        end
        config.recurring_tasks = tasks unless tasks.empty?
      elsif default_path.exist?
        config.recurring_tasks = Pgbus::Recurring::ConfigLoader.load(default_path)
        config.recurring_tasks_file ||= default_path.to_s
      end
    end

    initializer "pgbus.db" do
      ActiveSupport.on_load(:active_record) do
        Pgbus::BusRecord.connects_to(**Pgbus.configuration.connects_to) if Pgbus.configuration.connects_to

        # Disconnect the gem's own pools before any database purge/drop, so an
        # idle boot-time BusRecord session on a dedicated pgbus database can
        # never block that same process's DROP DATABASE (issue #409). No-op
        # when pgbus runs in the primary database (no BusRecord-owned pool).
        ActiveRecord::Tasks::DatabaseTasks.singleton_class.prepend(Pgbus::DatabaseTasksGuard)
      end
    end

    initializer "pgbus.active_job" do
      ActiveSupport.on_load(:active_job) do
        include Pgbus::Concurrency
        include Pgbus::Uniqueness
        include Pgbus::ActiveJob::BatchId
        include Pgbus::ActiveJob::CurrentAttributes
        include Pgbus::VisibilityHeartbeat::JobMixin
      end
    end

    initializer "pgbus.logger" do
      ActiveSupport.on_load(:after_initialize) do
        Pgbus.configuration.logger ||= Rails.logger
      end
    end

    rake_tasks do
      load File.expand_path("../tasks/pgbus_pgmq.rake", __dir__)
      load File.expand_path("../tasks/pgbus_streams.rake", __dir__)
      load File.expand_path("../tasks/pgbus_autovacuum.rake", __dir__)
      load File.expand_path("../tasks/pgbus_doctor.rake", __dir__)
    end

    initializer "pgbus.i18n" do
      config.i18n.load_path += Dir[root.join("config", "locales", "**", "*.yml")]
    end

    initializer "pgbus.web" do
      require "pgbus/web/authentication"
      require "pgbus/web/data_source"
    end

    # AppSignal is third-party and entirely optional. We require the
    # integration only when the host app has the appsignal gem loaded
    # AND hasn't disabled it via config.appsignal_enabled. AppSignal
    # itself loads early (it's typically required from config/environment.rb
    # before Rails finishes booting), so by the time `after_initialize`
    # fires the constant check is reliable.
    initializer "pgbus.integrations.appsignal", after: :load_config_initializers do
      ActiveSupport.on_load(:after_initialize) do
        next unless defined?(::Appsignal) && Pgbus.configuration.appsignal_enabled

        require "pgbus/integrations/appsignal"
        Pgbus::Integrations::Appsignal.install!
      end
    end

    # Generic metrics adapter. Opt-in via config.metrics_backend; nil (default)
    # installs no subscription, so there is zero overhead when unused. Runs
    # independently of AppSignal — both consume the same pgbus.* events and both
    # can be active at once.
    initializer "pgbus.metrics", after: :load_config_initializers do
      ActiveSupport.on_load(:after_initialize) do
        next if Pgbus.configuration.metrics_backend.nil?

        backend = Pgbus::Metrics.resolve_backend(Pgbus.configuration.metrics_backend)
        Pgbus::Metrics::Subscriber.install!(backend: backend)
        Pgbus.logger.info { "[Pgbus] Metrics subscriber installed (#{backend.class})" }
      end
    end

    # Install the watermark cache middleware ahead of the app's own
    # middleware so the thread-local cache is cleared between every
    # Rack request. Without this, repeated page renders served by the
    # same Puma thread would see stale current_msg_id values.
    initializer "pgbus.streams.middleware" do |app|
      app.middleware.use Pgbus::Streams::WatermarkCacheMiddleware if Pgbus.configuration.streams_enabled
    end

    # Make the <pgbus-stream-source> element available to the host app's
    # asset pipeline (Propshaft or Sprockets). The importmap pin points at
    # the minified build (rake frontend:js). Propshaft rewrites its
    # sourceMappingURL to the digested .min.js.map; Sprockets serves the
    # comment as written. The readable ES module source stays precompiled for
    # apps that pin or bundle it themselves. When importmap-rails is loaded,
    # auto-pin it so host apps get it without manual configuration.
    initializer "pgbus.streams.assets" do |app|
      if Pgbus.configuration.streams_enabled && app.config.respond_to?(:assets)
        app.config.assets.precompile += %w[
          pgbus/stream_source_element.min.js
          pgbus/stream_source_element.min.js.map
          pgbus/stream_source_element.js
        ]
      end
    end

    initializer "pgbus.streams.importmap" do
      if Pgbus.configuration.streams_enabled && defined?(::Importmap::Map)
        ActiveSupport.on_load(:after_initialize) do
          next unless Rails.application.respond_to?(:importmap)

          Rails.application.importmap.pin(
            "pgbus/stream_source_element", to: "pgbus/stream_source_element.min.js"
          )
        end
      end
    end

    # Install the Turbo::StreamsChannel patch after turbo-rails has been
    # loaded. The patch redirects broadcast_stream_to through Pgbus.stream
    # instead of ActionCable. When turbo-rails is not loaded, this is a
    # no-op and pgbus_stream_from still works via the explicit
    # Pgbus.stream(...).broadcast(...) API.
    initializer "pgbus.streams.turbo_broadcastable", after: :load_config_initializers do
      ActiveSupport.on_load(:after_initialize) do
        if Pgbus.configuration.streams_enabled
          # Touch the constant first so Zeitwerk autoloads
          # lib/pgbus/streams/turbo_broadcastable.rb. The file defines
          # `Pgbus::Streams::TurboBroadcastable` (the autoloaded const)
          # AND `Pgbus::Streams.install_turbo_broadcastable_patch!`
          # (a side-effect class method on the parent module). Without
          # the constant reference, Zeitwerk doesn't load the file and
          # the method call below raises NoMethodError. Assigning to
          # `_` keeps RuboCop's Lint/Void from deleting the line.
          _autoload_trigger = Pgbus::Streams::TurboBroadcastable
          Pgbus::Streams.install_turbo_broadcastable_patch!

          if defined?(::Turbo::Broadcastable)
            _autoload_trigger_broadcastable = Pgbus::Streams::BroadcastableOverride
            Pgbus::Streams::BroadcastableOverride.install!(::Turbo::Broadcastable)

            # Route turbo-rails' async broadcast jobs to a dedicated queue when
            # configured, so a broadcasts_to render+broadcast doesn't wait
            # behind long-running jobs on the default queue (#311). No-op when
            # streams_broadcast_queue is nil.
            Pgbus::Streams.install_broadcast_queue!(Pgbus.configuration.streams_broadcast_queue)
          end

          # Subscribe-side patch: override turbo_stream_from to render
          # <pgbus-stream-source> (SSE) instead of <turbo-cable-stream-source>
          # (ActionCable). Without this, third-party gems like
          # hotwire-livereload that call turbo_stream_from in their views
          # subscribe via ActionCable while the broadcast (patched above)
          # goes through PGMQ — the message never arrives.
          _autoload_trigger_override = Pgbus::Streams::TurboStreamOverride
          Pgbus::Streams.install_turbo_stream_override!
        end
      end
    end

    # Last: once the app's initializers have configured pgbus, freeze the
    # configuration's plain-data settings into Ractor-shareable values
    # (Pgbus.finalize!, issue #527). Registered after the app's initializer
    # files, so it runs after the after_initialize blocks they add.
    initializer "pgbus.finalize", after: :load_config_initializers do
      ActiveSupport.on_load(:after_initialize) { Pgbus.finalize! }
    end
  end
end
