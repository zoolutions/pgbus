# frozen_string_literal: true

require "time"

module Pgbus
  module Integrations
    module Appsignal
      # The AppSignal transaction of one unit of work: a job (JobSpan, on
      # pgbus.executor.execute) or an event handler (EventSpan, on
      # pgbus.event_processed).
      #
      # An ActiveSupport::Notifications listener that responds to #start and
      # #finish is called on both sides of the instrumented block (the Evented
      # subscriber). A finish-only block subscriber runs after the block has
      # returned, so the transaction it opened was never current while the job
      # ran: job code had nothing to attach to, and pgbus.job_failed, fired
      # later, found no transaction to put its error on (issue #552). Here
      # #start opens the transaction before the block runs, so job and handler
      # code, and the Rails error reporter, find it current; #finish records
      # the exception the block raised (the instrumenter puts it in
      # payload[:exception_object]) and completes the transaction.
      #
      # Like AppSignal's own ActiveJob hook, a transaction that is already
      # current is reused and left to its owner to complete.
      #
      # The frame lives in Thread.current[], which is fiber-local, so start and
      # finish pair up per job under execution_mode: :async, the same way
      # AppSignal keeps its current transaction. A span opened inside another
      # of its kind (a handler calling another handler's #process) keeps the
      # enclosing frame and restores it at finish, so the outer span still
      # completes its transaction. An instance holds no state of its own.
      # Nothing an AppSignal call raises reaches the job: it is logged at warn
      # level.
      #
      # Subclasses define #frame_key and #attached_key (the fiber-local slots),
      # #describe(transaction, payload) (action, queue start, tags) and
      # #finished(payload, duration_ms) (the span's metrics).
      class SpanSubscriber
        BACKGROUND_JOB = "background_job"

        # owned: this span created the transaction, so it completes it.
        # previous: the enclosing span's frame, restored at finish.
        Frame = Struct.new(:started_at, :owned, :previous)

        def start(_name, _id, payload)
          frame = Thread.current[frame_key] = Frame.new(monotonic_now, false, Thread.current[frame_key])
          guard do
            frame.owned = !::Appsignal::Transaction.current?
            transaction = frame.owned ? ::Appsignal::Transaction.create(BACKGROUND_JOB) : ::Appsignal::Transaction.current
            describe(transaction, payload)
          end
        end

        def finish(_name, _id, payload)
          frame = Thread.current[frame_key]
          guard do
            Thread.current[attached_key] = attach(payload[:exception_object])
            finished(payload, monotonic_now - frame.started_at)
          end
        ensure
          guard do
            Thread.current[frame_key] = frame.previous
            ::Appsignal::Transaction.complete_current! if frame.owned
          end
        end

        # pgbus.job_failed and pgbus.event_failed fire after #finish. An error
        # the span recorded is already on its transaction. One raised before
        # the span opened (a message body that is not JSON, an event that
        # cannot be built, a dead-letter move that failed) had no transaction,
        # so it is reported on one of its own, named and tagged like the span's
        # (or added to a transaction that is current, which is left as it is).
        def report_unattached(error, payload)
          attached = Thread.current[attached_key]
          Thread.current[attached_key] = nil
          return if error.nil? || error.equal?(attached)
          return ::Appsignal::Transaction.current.set_error(error) if ::Appsignal::Transaction.current?

          ::Appsignal.report_error(error) do |transaction|
            transaction.set_namespace(BACKGROUND_JOB)
            describe(transaction, payload)
          end
        end

        private

        # Returns the exception it recorded, nil when there was none.
        def attach(error)
          return unless error && ::Appsignal::Transaction.current?

          ::Appsignal::Transaction.current.set_error(error)
          error
        end

        # AppSignal expects queue-start as Unix epoch milliseconds. Pgbus
        # carries it as either an ISO-8601 String or a Time — both happen
        # in practice (executor passes the JSON string, handler passes a
        # parsed Time).
        def apply_queue_start(transaction, value)
          return unless value

          millis =
            case value
            when Time
              (value.to_f * 1_000).to_i
            when String
              (Time.parse(value).to_f * 1_000).to_i
            when Numeric
              value.to_i
            end
          transaction.set_queue_start(millis) if millis
        rescue ArgumentError
          # Unparseable timestamp — skip rather than blow up.
        end

        def guard
          yield
        rescue StandardError => e
          Pgbus.logger.warn { "[Pgbus::AppSignal] subscriber error: #{e.class}: #{e.message}" }
        end

        def monotonic_now
          ::Process.clock_gettime(::Process::CLOCK_MONOTONIC, :float_millisecond)
        end
      end
    end
  end
end
