# frozen_string_literal: true

Rails.application.routes.draw do
  # Rails' health check: what the report-only kino-smoke job (issue #527) GETs first.
  get "up" => "rails/health#show", as: :rails_health_check

  mount Pgbus::Engine => "/pgbus"
end
