# frozen_string_literal: true

require "pgbus"

module Pgbus
  # Test helpers for Pgbus EventBus. Opt-in via explicit require — never
  # autoloaded by Zeitwerk so this code never leaks into production.
  #
  #   require "pgbus/testing"          # core only
  #   require "pgbus/testing/rspec"    # RSpec matchers + auto-config
  #   require "pgbus/testing/minitest" # Minitest assertions
  #
  # Three modes:
  #   :fake     — capture published events in an in-memory store (default for tests)
  #   :inline   — capture AND immediately dispatch to matching handlers
  #   :disabled — pass through to the real publisher (production behavior)
  module Testing
    MODES = %i[fake inline disabled].freeze
    MODE_KEY = :__pgbus_test_mode

    # Raised by `disabled!` when a live Pgbus::Web::Streamer had to be torn
    # down and some of its threads did not stop within their join budget —
    # a red spec with a diagnosis instead of a hung test process (issue #443).
    class StreamerLeakError < Pgbus::Error; end

    # Thread-safe in-memory store for events captured in fake/inline mode.
    class EventStore
      def initialize
        @mutex = Mutex.new
        @events = []
      end

      def push_event(event)
        @mutex.synchronize { @events << event }
      end

      def events(routing_key: nil)
        @mutex.synchronize do
          result = @events.dup
          result = result.select { |e| e.routing_key == routing_key } if routing_key
          result
        end
      end

      def size
        @mutex.synchronize { @events.size }
      end

      def clear!
        @mutex.synchronize { @events.clear }
      end

      # Dispatch all stored events to their matching handlers, then clear.
      # Events are removed one at a time after successful dispatch so that
      # a handler exception leaves unprocessed events in the store.
      def drain!
        loop do
          event = @mutex.synchronize { @events.first }
          break unless event

          Pgbus::EventBus::Registry.instance.subscribers_matching(event.routing_key).each do |subscriber|
            # Restore the publisher's Current (issue #431) like the consumer does.
            Pgbus::CurrentAttributes.restore(event.context) { subscriber.handler_class.new.handle(event) }
          end

          @mutex.synchronize { @events.shift }
        end
      end
    end

    # Eagerly initialize the store so concurrent access never races on creation.
    @store = EventStore.new

    # The process-wide mode (`fake!` with no block). A module ivar under a lock
    # built at load, not `Thread.main[]`: that reads the main thread's CURRENT
    # fiber only, so a mode set inside a fiber (an Async task) or read while the
    # main thread runs one was lost (issue #527). The block form stays
    # `Thread.current[MODE_KEY]`, which is fiber-local on every Ruby: a `fake!`
    # block inside one Async task must not leak to its siblings.
    @mode_lock = Mutex.new
    @default_mode = nil

    class << self
      def mode!(mode, &block)
        raise ArgumentError, "Unknown mode: #{mode}. Valid modes: #{MODES.join(", ")}" unless MODES.include?(mode)

        # Record the mode BEFORE syncing streams: a StreamerLeakError raised
        # by the teardown must still leave the process in the requested mode.
        unless block
          @mode_lock.synchronize { @default_mode = mode }
          sync_streams_test_mode!(mode)
          return
        end

        old = Thread.current[MODE_KEY]
        Thread.current[MODE_KEY] = mode
        sync_streams_test_mode!(mode)
        yield
      ensure
        if block
          Thread.current[MODE_KEY] = old
          # The mode in effect again: the outer block's, else the
          # process-wide one (not :disabled, which would let the streamer
          # into a fake-mode test's later SSE requests).
          sync_streams_test_mode!(old || default_mode)
        end
      end

      def mode
        Thread.current[MODE_KEY] || default_mode
      end

      def fake!(&) = mode!(:fake, &)
      def inline!(&) = mode!(:inline, &)
      def disabled!(&) = mode!(:disabled, &)

      def fake?     = mode == :fake
      def inline?   = mode == :inline
      def disabled? = mode == :disabled

      attr_reader :store

      private

      def default_mode
        @mode_lock.synchronize { @default_mode } || :disabled
      end

      # When entering fake/inline mode, enable streams_test_mode to prevent
      # rack.hijack from spawning background threads that acquire DB
      # connections outside the test transaction (see issue #133). When
      # returning to :disabled mode, restore the previous setting.
      def sync_streams_test_mode!(mode)
        return unless defined?(Pgbus.configuration)

        if mode == :disabled
          Pgbus.configuration.streams_test_mode = false
          reset_streamer!
        else
          Pgbus.configuration.streams_test_mode = true
        end
      end

      # A live streamer at this point is legitimate inside a
      # `disabled! do ... end` real-stream test, so a clean teardown only
      # warns. Threads that outlived shutdown! are a different matter: left
      # alone they share the test's pinned AR connection with the test thread
      # and the suite hangs with no diagnosis (issue #443) — so raise.
      def reset_streamer!
        return unless defined?(Pgbus::Web::Streamer)

        leaked = Pgbus::Web::Streamer.reset!
        return if leaked.nil?

        if leaked.empty?
          Pgbus.logger.warn { "[Pgbus::Testing] disabled! tore down a live Pgbus::Web::Streamer that was started during the test" }
          return
        end

        raise StreamerLeakError, <<~MSG
          Pgbus::Testing.disabled! shut down a live Pgbus::Web::Streamer but its #{leaked.join(", ")} thread(s) did not stop within their join budget.
          A live streamer inside the test process means an SSE request reached the real stream path while streams_test_mode was off — usually because Pgbus::Testing.disabled! ran in an RSpec `after` hook before Capybara reset the browser session (page still open, EventSource still reconnecting).
          Register the hook with `config.append_after { Pgbus::Testing.disabled! }` so it runs after Capybara.reset_sessions!. See the README section "SSE streams in tests".
        MSG
      end
    end
  end
end

require_relative "testing/assertions"
