# frozen_string_literal: true

require_relative "../testing"

module Pgbus
  module Testing
    # Minitest integration for Pgbus test helpers.
    #
    # Include in your test_helper.rb:
    #
    #   require "pgbus/testing/minitest"
    #
    #   class ActiveSupport::TestCase
    #     include Pgbus::Testing::MinitestHelpers
    #   end
    #
    # This provides:
    #   - Automatic fake mode + store clearing per test
    #   - assert_pgbus_published / assert_no_pgbus_published
    #   - perform_published_events
    #   - pgbus_published_events
    module MinitestHelpers
      include Pgbus::Testing::Assertions

      def before_setup
        Pgbus::Testing.fake!
        Pgbus::Testing.store.clear!
        Pgbus::Testing.clear_signals!
        super
      end
    end
  end
end
