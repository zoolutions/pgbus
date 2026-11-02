# frozen_string_literal: true

module Pgbus
  module ActiveJob
    # config.quiet_active_job_logs (issue #618): a worker process stops
    # ActiveJob's per-job "Performing …" / "Performed …" info lines by
    # unsubscribing ActiveJob::LogSubscriber from perform_start.active_job and
    # perform.active_job, and from nothing else. Every other ActiveJob line
    # (Enqueued, Retrying, Stopped retrying, Discarded) still logs as the host
    # configured it, and the web process is untouched. Swapping
    # ActiveJob::Base.logger instead would reach the web process too, and by
    # default that logger is Rails.logger.
    module QuietLogs
      EVENTS = %w[perform_start.active_job perform.active_job].freeze

      # Built at load; the applied notifiers are read and written under it.
      @mutex = Mutex.new
      @applied = {}.compare_by_identity

      class << self
        # Unsubscribes the LogSubscriber listeners once per notifier. Returns
        # true when it did, false when that notifier was already done.
        def apply!(notifier: ::ActiveSupport::Notifications.notifier)
          @mutex.synchronize do
            return false if @applied.key?(notifier)

            @applied[notifier] = true
            # all_listeners_for, not listeners_for: the latter skips a
            # silenced subscriber (its logger above info), which would come
            # back the moment the level drops.
            EVENTS.each do |name|
              notifier.all_listeners_for(name).each do |listener|
                notifier.unsubscribe(listener) if log_subscriber?(listener)
              end
            end
            true
          end
        end

        private

        # Fanout::Subscribers::Evented exposes #delegate on Rails 7.1 and later.
        def log_subscriber?(listener)
          listener.respond_to?(:delegate) && listener.delegate.is_a?(::ActiveJob::LogSubscriber)
        end
      end
    end
  end
end
