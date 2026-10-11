# frozen_string_literal: true

module Pgbus
  class FrontendsController < ActionController::Base
    protect_from_forgery with: :exception
    skip_after_action :verify_same_origin_request, raise: false

    FRONTEND_ROOT = Engine.root.join("app", "frontend", "pgbus")

    STATIC_ASSETS = {
      css: {
        style: FRONTEND_ROOT.join("style.css")
      }
    }.freeze

    # Modules are served minified (rake frontend:js), each with its .min.js.map
    # beside it. ApexCharts ships minified already; charts.js imports it on
    # demand, so only Insights downloads it.
    MODULE_OVERRIDES = {
      application: FRONTEND_ROOT.join("application.min.js"),
      turbo: FRONTEND_ROOT.join("vendor", "turbo.min.js"),
      apexcharts: FRONTEND_ROOT.join("vendor", "apexcharts.js")
    }.freeze

    def self.js_modules
      @js_modules ||= FRONTEND_ROOT.join("modules").glob("*.min.js").sort
                                   .to_h { |f| [f.basename(".min.js").to_s.to_sym, f] }
                                   .merge(MODULE_OVERRIDES)
    end

    CURRENT_VERSION = Pgbus::VERSION.tr(".", "-").freeze

    before_action do
      expires_in 1.year, public: true
    end

    after_action :set_asset_security_headers

    def static
      validate_version!
      file = STATIC_ASSETS.dig(params[:format]&.to_sym, params[:id]&.to_sym)
      raise ActionController::RoutingError, "Not Found" unless file&.exist?

      render file: file
    end

    # GET modules/:version/<id>.js serves the module; <id>.map its sourcemap,
    # the URL each minified module's sourceMappingURL comment resolves to.
    def module
      validate_version!
      file = self.class.js_modules[params[:id]&.to_sym]

      case params[:format]
      when "js" then serve(file)
      when "map" then serve(sourcemap_for(file), content_type: "application/json")
      else raise ActionController::RoutingError, "Not Found"
      end
    end

    private

    def serve(file, **)
      raise ActionController::RoutingError, "Not Found" unless file&.exist?

      render(file: file, **)
    end

    def sourcemap_for(file)
      Pathname.new("#{file}.map") if file.to_s.end_with?(".min.js")
    end

    def validate_version!
      return if params[:version] == CURRENT_VERSION

      raise ActionController::RoutingError, "Not Found"
    end

    def set_asset_security_headers
      response.headers["X-Content-Type-Options"] = "nosniff"
    end
  end
end
