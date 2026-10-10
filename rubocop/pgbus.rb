# frozen_string_literal: true

# Entry point for pgbus's custom RuboCop cops. Required from .rubocop.yml via
# `require:` so the cops are available when linting this repo.
require_relative "cop/pgbus/no_ruby_timeout"
require_relative "cop/pgbus/dashboard_time_formatting"
