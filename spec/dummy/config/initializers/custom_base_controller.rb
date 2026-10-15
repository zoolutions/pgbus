# frozen_string_literal: true

# Lets one spec boot the dashboard under a host base controller. Rails only
# auto-loads helpers when the superclass is ActionController::Base.
if ENV["PGBUS_CUSTOM_BASE"] == "1"
  class PgbusSpecBaseController < ActionController::Base
    helper { def pgbus_spec_host_helper = "host-helper-ok" }
  end

  Pgbus.configuration.base_controller_class = "PgbusSpecBaseController"
end
