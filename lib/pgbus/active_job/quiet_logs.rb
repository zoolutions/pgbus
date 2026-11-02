# frozen_string_literal: true

module Pgbus
  module ActiveJob
    # config.quiet_active_job_logs (issue #618): a worker process stops
    # ActiveJob's per-job "Performing …" / "Performed …" info lines by
    # unsubscribing ActiveJob::LogSubscriber from perform_start.active_job and
    # perform.active_job. The perform listener also writes ActiveJob's
    # "Error performing …" error line, for a job that raised (with its
    # backtrace) and for one a before_perform callback halted, so those go too.
    # An exception that escapes ActiveJob's handlers is still logged by
    # ErrorReporter (class and message at error) and recorded in
    # pgbus_failed_events; one retry_on catches still logs ActiveJob's
    # "Retrying …" line (and "Stopped retrying …" at error when it gives up);
    # a halted job logs nothing.
    # The other ActiveJob events (Enqueued, Retrying, Stopped retrying,
    # Discarded) still log as the host configured them, and the web process is
    # untouched. Swapping
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
