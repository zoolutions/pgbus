# frozen_string_literal: true

module Pgbus
  module Web
    module Authentication
      extend ActiveSupport::Concern

      included do
        before_action :authenticate_pgbus!
      end

      # The "dashboard has no authentication" warning is logged once per
      # process. Request threads check and set the flag under @warn_lock, so
      # concurrent first requests cannot both log it (issue #527).
      @warn_lock = Mutex.new
      @auth_warned = false

      class << self
        def auth_warned
          @warn_lock.synchronize { @auth_warned }
        end

        def auth_warned=(value)
          @warn_lock.synchronize { @auth_warned = value }
        end

        # True for the one caller that flips the flag from false to true.
        def claim_auth_warning!
          @warn_lock.synchronize { @auth_warned ? false : (@auth_warned = true) }
        end
      end

      # Default base controller — anything else means the app deliberately
      # chose a controller (e.g. an AdminController) that already gates access,
      # so the dashboard isn't actually open and the warning is a false positive.
      DEFAULT_BASE_CONTROLLER = "::ActionController::Base"
      private_constant :DEFAULT_BASE_CONTROLLER

      private

      def authenticate_pgbus!
        auth_block = Pgbus.configuration.web_auth

        if auth_block.nil?
          warn_unauthenticated_dashboard
          return
        end

        return if auth_block.respond_to?(:call) && auth_block.call(request)

        head :unauthorized
      end

      def warn_unauthenticated_dashboard
        return if Pgbus::Web::Authentication.auth_warned
        # A non-default base_controller_class signals host-level auth is in
        # place; don't nag apps that gate the dashboard via a custom controller
        # (they had to set a redundant web_auth lambda just to silence us — #334).
        return unless default_base_controller?
        return unless Pgbus::Web::Authentication.claim_auth_warning!

        Pgbus.logger.warn do
          "[Pgbus] Dashboard is accessible without authentication. " \
            "Configure Pgbus.configuration.web_auth to restrict access. " \
            "See: https://github.com/zoolutions/pgbus#dashboard-authentication"
        end
      end

      # True when base_controller_class is (still) the default ActionController::Base.
      # Normalizes both sides so a Class value, the "::"-prefixed string, and the
      # bare "ActionController::Base" string all compare equal — otherwise the
      # warning would wrongly fire for an app that set the default in another form.
      def default_base_controller?
        normalize_controller_name(Pgbus.configuration.base_controller_class) ==
          normalize_controller_name(DEFAULT_BASE_CONTROLLER)
      end

      def normalize_controller_name(value)
        value.to_s.delete_prefix("::")
      end
    end
  end
end
