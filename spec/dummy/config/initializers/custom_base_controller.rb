# frozen_string_literal: true

if ENV["PGBUS_CUSTOM_BASE"] == "1"
  class PgbusSpecBaseController < ActionController::Base
    helper { def pgbus_spec_host_helper = "host-helper-ok" }
  end

  Pgbus.configuration.base_controller_class = "PgbusSpecBaseController"
end
