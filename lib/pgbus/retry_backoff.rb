# frozen_string_literal: true

module Pgbus
  # Computes exponential backoff delays for the VT-based retry path
  # (unhandled exceptions that fall through ActiveJob's retry_on).
  #
  # The formula mirrors the CircuitBreaker pattern already used
  # elsewhere in pgbus: base * 2^(attempt-1), capped at max,
  # with optional jitter to prevent thundering-herd retries.
  #
  # Jobs can override the global config via the pgbus_retry_backoff
  # class-level DSL (see JobMixin below).
  module RetryBackoff
    # Mixin for ActiveJob classes to declare per-job backoff config.
    #
    #   class ImportJob < ApplicationJob
    #     include Pgbus::RetryBackoff::JobMixin
    #     pgbus_retry_backoff base: 15, max: 120, jitter: 0.2
    #   end
    module JobMixin
      extend ActiveSupport::Concern

      # Class-body macro: it runs while the job class loads (at boot, or under the
      # autoloader's lock), stores a frozen Hash, and the reader only reads it.
      # rubocop:disable-next ThreadSafety/ClassInstanceVariable
      class_methods do
        def pgbus_retry_backoff(base: nil, max: nil, jitter: nil)
          raise ArgumentError, "retry_backoff base must be > 0" if !base.nil? && (!base.is_a?(Numeric) || base <= 0)
          raise ArgumentError, "retry_backoff max must be > 0" if !max.nil? && (!max.is_a?(Numeric) || max <= 0)
          if !jitter.nil? && (!jitter.is_a?(Numeric) || jitter.negative? || jitter > 1)
            raise ArgumentError, "retry_backoff jitter must be between 0 and 1"
          end

          @pgbus_retry_backoff = {
            base: base,
            max: max,
            jitter: jitter
          }.compact.freeze
        end

        def pgbus_retry_backoff_config
          @pgbus_retry_backoff
        end
      end
    end

    class << self
      # Compute delay for a specific job class, falling back to global
      # config for any options not overridden at the job level.
      def compute_delay_for_job(job_class, attempt:, jitter: nil)
        overrides = (job_class.respond_to?(:pgbus_retry_backoff_config) &&
                    job_class.pgbus_retry_backoff_config) || {}

        config = Pgbus.configuration
        compute_delay(
          attempt: attempt,
          base: overrides[:base] || config.retry_backoff,
          max: overrides[:max] || config.retry_backoff_max,
          jitter: jitter || overrides[:jitter] || config.retry_backoff_jitter
        )
      end

      # Core backoff computation.
      #
      # @param attempt [Integer] 1-based retry attempt number (read_ct - 1)
      # @param base [Numeric] base delay in seconds (default: config.retry_backoff)
      # @param max [Numeric] maximum delay cap (default: config.retry_backoff_max)
      # @param jitter [Numeric] jitter factor 0..1 (default: config.retry_backoff_jitter)
      # @return [Integer] delay in seconds
      def compute_delay(attempt:, base: nil, max: nil, jitter: nil)
        config = Pgbus.configuration
        base ||= config.retry_backoff
        max ||= config.retry_backoff_max
        jitter = config.retry_backoff_jitter if jitter.nil?

        exponent = [attempt - 1, 0].max
        delay = base * (2**exponent)
        delay = [delay, max].min

        [apply_jitter(delay, jitter), max].min
      end

      private

      def apply_jitter(delay, jitter)
        return delay.to_i if jitter.nil? || jitter.zero?

        spread = delay * jitter
        jittered = delay + rand(-spread..spread)
        [jittered.round, 0].max
      end
    end
  end
end
