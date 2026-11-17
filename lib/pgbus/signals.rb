# frozen_string_literal: true

module Pgbus
  # Process signals (issue #674): run a block in every web process when any
  # process sends a signal — the "drop your in-memory cache now" message.
  #
  #   # config/initializers/widgets.rb
  #   Pgbus.on_signal(:widgets_changed, on_gap: -> { WIDGETS.clear }) { |_payload| WIDGETS.clear }
  #
  #   # anywhere, after the change (inside a transaction it is sent at COMMIT)
  #   Pgbus.signal(:widgets_changed)
  #   Pgbus.signal(:widgets_changed, "id=42") # a String payload, under NOTIFY's 7999-byte cap
  #
  # Transport: a NOTIFY on one channel per signal, `<queue_prefix>_signal_<name>`,
  # LISTENed by the connection the streamer already owns: the per-worker
  # Listener under `streams_listen_scope :process`, the MasterHub's under
  # `:master` (fanned out to the workers over the hub socket). Subscribers run
  # on the streamer's signal delivery thread, never on the LISTEN thread, each
  # inside the Rails executor; one that raises is reported and the others run.
  #
  # The registry is process-local, like Pgbus::Streams.filters: register in an
  # initializer. With at least one subscriber, the first request a process
  # serves starts the streamer (Signals::Middleware), so a process that never
  # served an SSE connection still listens; with none, nothing changes.
  #
  # Delivery is at-most-once: a NOTIFY sent while a process was not LISTENing
  # (a reconnect, a hub failover) is lost. After such a gap every subscriber's
  # `on_gap` runs once, once the new LISTEN is in place, so it can assume it
  # missed signals.
  module Signals
    NAME_PATTERN = /\A[a-z0-9_]+\z/

    # How long a failed eager start waits before the next request tries again.
    START_RETRY_SECONDS = 30

    # One registered block, and the optional callable run after a LISTEN gap.
    Subscriber = Data.define(:name, :callback, :on_gap)

    # Built at load, never `||=` (AGENTS.md, Never Do 5). @lock guards
    # @subscribers (a frozen Hash of name → frozen Array of Subscriber,
    # replaced whole on write, so a reader iterates a snapshot). @start_lock guards the eager-start
    # bookkeeping and is held across Streamer.current, whose Instance#start
    # reads the registry: two locks, so that read never waits on itself.
    @lock = Mutex.new
    @start_lock = Mutex.new
    @subscribers = {}.freeze
    @started_pid = nil
    @failed_at = nil

    class << self
      # Pgbus.on_signal. Returns the Subscriber.
      def subscribe(name, on_gap: nil, &block)
        name = validate_name!(name)
        raise ArgumentError, "Pgbus.on_signal(#{name.inspect}) needs a block" unless block
        raise ArgumentError, "on_gap: must respond to #call (got #{on_gap.class})" if on_gap && !on_gap.respond_to?(:call)

        subscriber = Subscriber.new(name: name, callback: block, on_gap: on_gap)
        @lock.synchronize do
          @subscribers = @subscribers.merge(name => [*@subscribers[name], subscriber].freeze).freeze
        end
        # A streamer already running in this process LISTENs on the new
        # channel now; one started later subscribes every channel at start.
        Pgbus::Web::Streamer.listen_signal(channel_for(name))
        subscriber
      end

      # Pgbus.signal. Sends the NOTIFY now, or at the COMMIT of the caller's
      # open ActiveRecord transaction (nothing is sent on rollback). Returns nil.
      def publish(name, payload = nil)
        name = validate_name!(name)
        validate_payload!(payload)
        return testing_signal(name, payload) if testing?

        send_now = -> { Pgbus.client.notify_signal(name, payload) }
        send_now.call unless OpenTransaction.defer(&send_now)
        nil
      end

      def names
        @lock.synchronize { @subscribers.keys }
      end

      def subscribed?
        @lock.synchronize { @subscribers.any? }
      end

      def gap_handlers?
        @lock.synchronize { @subscribers.values.flatten.any?(&:on_gap) }
      end

      # The NOTIFY channel of +name+: `<queue_prefix>_signal_<name>`, through
      # config.queue_name, so it is bounded by the queue-name budget (47) and
      # always a valid, unquoted-safe identifier under PostgreSQL's 63.
      def channel_for(name)
        Pgbus.configuration.queue_name("signal_#{name}")
      end

      def channels
        names.map { |name| channel_for(name) }
      end

      # The registered signal whose channel is +channel+, or nil.
      def name_for(channel)
        names.find { |name| channel_for(name) == channel }
      end

      # Runs every subscriber of +name+ with +payload+, in registration order,
      # each in the Rails executor. Called on the streamer's signal delivery
      # thread (or inline by Pgbus::Testing). Never raises.
      def deliver(name, payload)
        subscribers_of(name).each do |subscriber|
          run_safely(action: "signal_subscriber", signal: name) { subscriber.callback.call(payload) }
        end
        nil
      end

      # Runs every subscriber's on_gap once. Never raises.
      def deliver_gap
        all = @lock.synchronize { @subscribers.values.flatten }
        all.each do |subscriber|
          next unless subscriber.on_gap

          run_safely(action: "signal_on_gap", signal: subscriber.name) { subscriber.on_gap.call }
        end
        nil
      end

      # The eager start Signals::Middleware runs on every request: with a
      # subscriber, start this process's streamer once, so it LISTENs before
      # the request reads anything a signal would invalidate. A no-op in tests
      # (streams_test_mode), with streams disabled, and in a schema rake task.
      # Never raises into the request: a failure is reported, and the next
      # request START_RETRY_SECONDS later tries again.
      def start_listening!
        return if started_here?
        return unless subscribed? && startable?

        pid = ::Process.pid
        @start_lock.synchronize do
          return if @started_pid == pid
          return if @failed_at && (monotonic_now - @failed_at) < START_RETRY_SECONDS

          begin
            Pgbus::Web::Streamer.current
            @started_pid = pid
            @failed_at = nil
          rescue StandardError => e
            @failed_at = monotonic_now
            Pgbus::ErrorReporter.report(e, { action: "signals_start" })
          end
        end
        nil
      end

      # A fresh registry. Tests only.
      def reset!
        @lock.synchronize { @subscribers = {}.freeze }
        @start_lock.synchronize do
          @started_pid = nil
          @failed_at = nil
        end
      end

      private

      def started_here?
        @start_lock.synchronize { @started_pid == ::Process.pid }
      end

      def startable?
        config = Pgbus.configuration
        config.streams_enabled && !config.streams_test_mode && !Pgbus.database_task?
      end

      def subscribers_of(name)
        @lock.synchronize { @subscribers[name.to_s] } || []
      end

      def validate_name!(name)
        string = name.to_s
        unless NAME_PATTERN.match?(string)
          raise ArgumentError,
                "Invalid signal name #{name.inspect}: use lowercase letters, digits and underscores only"
        end

        budget = QueueNameValidator::MAX_QUEUE_NAME_LENGTH
        channel = "#{Pgbus.configuration.queue_prefix}_signal_#{string}"
        if channel.length > budget
          raise ArgumentError,
                "Signal name #{string.inspect} is too long: its channel #{channel.inspect} is " \
                "#{channel.length} chars, the budget is #{budget}"
        end

        string
      end

      def validate_payload!(payload)
        return if payload.nil?
        raise ArgumentError, "A signal payload must be a String or nil (got #{payload.class})" unless payload.is_a?(String)

        limit = Client::NotifyStream::NOTIFY_PAYLOAD_LIMIT_BYTES
        return if payload.bytesize <= limit

        raise ArgumentError, "A signal payload is #{payload.bytesize} bytes; NOTIFY caps it at #{limit}"
      end

      def run_safely(context, &)
        with_executor(&)
      rescue StandardError => e
        Pgbus::ErrorReporter.report(e, context)
      end

      def with_executor(&)
        if defined?(::Rails) && ::Rails.respond_to?(:application) && ::Rails.application
          ::Rails.application.executor.wrap(&)
        else
          yield
        end
      end

      def testing?
        defined?(Pgbus::Testing) && !Pgbus::Testing.disabled?
      end

      def testing_signal(name, payload)
        Pgbus::Testing.record_signal(name, payload)
        nil
      end

      def monotonic_now
        ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
      end
    end
  end
end
