# frozen_string_literal: true

require "json"
require "pgbus/integrations/appsignal/subscriber"
require "pgbus/integrations/appsignal/probe"

module Pgbus
  module Integrations
    # AppSignal integration for pgbus.
    #
    # Loaded automatically by Pgbus::Engine when the appsignal gem is present
    # and config.appsignal_enabled is true (default). To opt out:
    #
    #   Pgbus.configure do |c|
    #     c.appsignal_enabled = false
    #   end
    #
    # The integration:
    #   * Subscribes to pgbus.* ActiveSupport::Notifications and translates
    #     them into AppSignal background-job transactions and metrics.
    #   * Registers a minutely probe that reports queue depth, DLQ size,
    #     dead-tuple counts, MVCC horizon age, and stream stats from
    #     Pgbus::Web::DataSource.
    #
    # All metric names are prefixed `pgbus_` so they group cleanly in
    # AppSignal's custom-metrics view.
    module Appsignal
      DASHBOARD_PATH = File.expand_path("appsignal/dashboard.json", __dir__).freeze
      DASHBOARDS_DIR = File.expand_path("appsignal/dashboards", __dir__).freeze

      # Installed once at boot (the engine's after_initialize); @lock makes
      # the check-then-install atomic and guards the parsed-dashboard memos
      # (issue #527).
      @lock = Mutex.new
      @installed = false

      module_function

      def install!
        return false unless defined?(::Appsignal)

        installed_now = @lock.synchronize do
          next false if @installed

          Subscriber.install!
          Probe.install! if Pgbus.configuration.appsignal_probe_enabled
          @installed = true
        end
        Pgbus.logger.info { "[Pgbus] AppSignal integration installed" } if installed_now
        installed_now
      end

      def installed?
        @lock.synchronize { @installed }
      end

      def dashboard_definition
        @lock.synchronize { @dashboard_definition ||= JSON.parse(File.read(DASHBOARD_PATH)) }
      end

      def dashboard_definitions
        @lock.synchronize do
          @dashboard_definitions ||= Dir[File.join(DASHBOARDS_DIR, "*.json")].map { |path| JSON.parse(File.read(path)) }
        end
      end

      # Test hook: tear everything down so a fresh install! can run.
      def reset!
        Subscriber.reset! if defined?(Subscriber)
        Probe.reset! if defined?(Probe)
        @lock.synchronize do
          @installed = false
          @dashboard_definition = nil
          @dashboard_definitions = nil
        end
      end
    end
  end
end
