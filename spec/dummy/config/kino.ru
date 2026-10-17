# frozen_string_literal: true

# The report-only kino-smoke job (.github/workflows/main.yml, issue #527): the
# dummy app served by kino (script/kino_smoke.sh). kino has no rack.hijack, so
# streams answer with a streaming body (streams_falcon_streaming_body), which
# kino writes out chunk by chunk. KINO_SMOKE_MODE=ractor serves the app from
# worker Ractors through ractor-rails-shim; any other value serves it from
# threads, the control run that shows the app itself works.
ractor = ENV["KINO_SMOKE_MODE"] == "ractor"
if ractor
  require "ractor_rails_shim"
  RactorRailsShim.install
end

require_relative "environment"

Pgbus.configure do |c|
  c.database_url = ENV.fetch("PGBUS_DATABASE_URL")
  c.streams_signed_name_secret = ENV.fetch("PGBUS_STREAMS_SECRET")
  c.streams_falcon_streaming_body = true
end

if ractor
  RactorRailsShim.prepare_for_ractors!
  run RactorRailsShim.make_app_shareable!(Rails.application)
else
  run Rails.application
end
