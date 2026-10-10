# frozen_string_literal: true

module Pgbus
  class ApplicationController < Pgbus.configuration.base_controller_class.constantize
    # Ensure core ActionController modules are available even when the
    # base controller is a slim subclass that may not include them all.
    ActionController::Base::MODULES.each do |mod|
      include mod unless self < mod
    end

    include Web::Authentication

    protect_from_forgery with: :exception
    before_action :set_locale
    after_action :set_security_headers

    layout "pgbus/application"

    helper Pgbus::ApplicationHelper unless self < Pgbus::ApplicationHelper
    helper Pgbus::ButtonHelper unless self < Pgbus::ButtonHelper

    # Make `pgbus` route proxy available in views (e.g. pgbus.root_path).
    # With isolate_namespace, the non-prefixed helpers (root_path) work inside
    # the engine, but the views use the pgbus.* proxy form for clarity.
    def pgbus
      @pgbus ||= Pgbus::Engine.routes.url_helpers
    end
    helper_method :pgbus

    private

    def set_security_headers
      response.headers["X-Frame-Options"] = "SAMEORIGIN"
      response.headers["X-Content-Type-Options"] = "nosniff"
      response.headers["X-XSS-Protection"] = "0"
      response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
      response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    end

    def set_locale
      I18n.locale = extract_locale || I18n.default_locale
    end

    def extract_locale
      locale = params[:locale] || cookies[:pgbus_locale] || preferred_locale_from_header
      locale if locale && available_locales.include?(locale.to_sym)
    end

    def preferred_locale_from_header
      return unless request.env["HTTP_ACCEPT_LANGUAGE"]

      request.env["HTTP_ACCEPT_LANGUAGE"]
             .scan(/([a-z]{2}(?:-[A-Z]{2})?)\s*;?\s*(?:q=([0-9.]+))?/i)
             .sort_by { |_, q| -(q&.to_f || 1.0) }
             .each do |lang, _|
               code = lang.downcase.to_sym
               return code.to_s if available_locales.include?(code)

               # Try base language (e.g., "de" from "de-AT")
               base = lang.split("-").first.downcase.to_sym
               return base.to_s if available_locales.include?(base)
             end
      nil
    end

    def available_locales
      @available_locales ||= begin
        pgbus_locales = Dir[Pgbus::Engine.root.join("config", "locales", "*.yml")]
                        .map { |f| File.basename(f, ".yml").to_sym }
        pgbus_locales & I18n.available_locales
      end
    end
    helper_method :available_locales

    def data_source
      @data_source ||= Pgbus.configuration.web_data_source || Web::DataSource.new
    end

    def page_param
      [params[:page].to_i, 1].max
    end

    def per_page
      Pgbus.configuration.web_per_page
    end

    def insights_minutes
      config = Pgbus.configuration
      default = config.insights_default_minutes.to_i
      value = (params[:minutes] || default).to_i
      value.clamp(1, [default, 43_200].max)
    end

    def turbo_frame_request?
      request.headers["Turbo-Frame"].present? || params[:frame].present?
    end

    def render_frame(partial)
      render partial: partial, layout: false
    end
  end
end
