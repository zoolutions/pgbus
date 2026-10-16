# frozen_string_literal: true

module Pgbus
  module Deploy
    # The app revision this process runs (issue #536). Every heartbeat row
    # carries it in metadata["revision"]; the deploy-task rollover gate
    # compares those against the scheduler's own.
    #
    # First match wins, blank counts as absent:
    #   config.app_revision (String or callable) → PGBUS_REVISION → DASH_VERSION
    #   → KAMAL_VERSION → the REVISION file at Rails.root (Capistrano) → nil
    #
    # Computed per call, never memoized: it is read once per process
    # registration, and a module-level memo would be unguarded shared state.
    module Revision
      ENV_KEYS = %w[PGBUS_REVISION DASH_VERSION KAMAL_VERSION].freeze

      module_function

      def current(config = Pgbus.configuration)
        from_config(config) || from_env || from_file
      end

      def from_config(config)
        value = config.app_revision
        value = value.call if value.respond_to?(:call)
        presence(value)
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] app_revision raised #{e.class}: #{e.message}; trying the environment" }
        nil
      end

      def from_env
        ENV_KEYS.each do |key|
          value = presence(ENV.fetch(key, nil))
          return value if value
        end
        nil
      end

      def from_file
        return nil unless defined?(Rails) && Rails.respond_to?(:root) && Rails.root

        path = Rails.root.join("REVISION")
        File.exist?(path) ? presence(File.read(path)) : nil
      end

      def presence(value)
        string = value.to_s.strip
        string.empty? ? nil : string
      end
    end
  end
end
