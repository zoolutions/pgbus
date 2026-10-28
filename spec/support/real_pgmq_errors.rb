# frozen_string_literal: true

# Loads the genuine PGMQ::Errors::* classes from the pgmq-ruby runtime
# dependency so specs exercising Pgbus's rescue paths match the real error
# hierarchy — not a fake Class.new(StandardError) stub that would keep passing
# even if pgmq-ruby restructured its errors and every rescue silently stopped
# matching.
module RealPgmqErrors
  # Ensure PGMQ::Errors::ConnectionError (and its siblings) are the genuine
  # classes from pgmq-ruby, then return the ConnectionError class.
  #
  # pgmq-ruby autoloads PGMQ::Errors via Zeitwerk (see the gem's pgmq.rb). A
  # plain `require "pgmq"` populates it the first time, but this suite (and
  # Pgbus::Client#initialize itself) sometimes replaces the ::PGMQ constant with
  # a bare Module.new so construction works without a live database. On a bare
  # module the Zeitwerk autoload is gone and `require` is a no-op (the feature is
  # already in $LOADED_FEATURES), leaving PGMQ::Errors undefined. So `load` the
  # errors file directly onto whatever ::PGMQ currently is — this re-executes the
  # source and re-defines the real classes regardless of prior stubbing, and is
  # cheap (a handful of one-line class definitions).
  def real_pgmq_connection_error
    Object.const_set(:PGMQ, Module.new) unless defined?(::PGMQ)
    load(Gem.find_files("pgmq/errors.rb").first) unless defined?(PGMQ::Errors::ConnectionError)
    PGMQ::Errors::ConnectionError
  end

  # The genuine PGMQ::Metrics, loaded the same way, so a spec reading its
  # fields fails when pgmq-ruby lacks one (a double answers anything).
  def real_pgmq_metrics
    Object.const_set(:PGMQ, Module.new) unless defined?(::PGMQ)
    load(Gem.find_files("pgmq/metrics.rb").first) unless defined?(PGMQ::Metrics)
    PGMQ::Metrics
  end
end

RSpec.configure do |config|
  config.include RealPgmqErrors
end
