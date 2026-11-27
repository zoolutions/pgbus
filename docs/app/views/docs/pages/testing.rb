# frozen_string_literal: true

# Testing apps that use pgbus: the opt-in test helpers, the fake/inline/disabled
# modes, the event-bus assertions and matchers, the SSE test caveat and
# coalesced broadcasts in tests.
class Views::Docs::Pages::Testing < DocsUI::Page
  title "Testing"
  eyebrow "Testing"

  def lead = "Opt-in RSpec and Minitest helpers capture published events without touching PGMQ."

  def content
    setup
    modes
    assertions
    matchers
    sse
    coalesced_broadcasts
  end

  private

  def setup
    DocsUI::Section("Set it up", description: "The testing module is never autoloaded — you require it.") do
      md <<~'MD'
        The testing helpers are never loaded by Zeitwerk, so they can't leak into
        production. Require them, activate a mode, and clear the store per test.
      MD
      DocsUI::Code(<<~RUBY, filename: "spec/rails_helper.rb")
        require "pgbus/testing/rspec"

        RSpec.configure do |config|
          config.before { Pgbus::Testing.fake! }
          config.append_after do
            Pgbus::Testing.disabled!
            Pgbus::Testing.store.clear!
            Pgbus::Testing.clear_signals!
          end
        end
      RUBY
      md <<~'MD'
        Use `append_after`, not `after`: config-level `after` hooks run in reverse
        registration order, so one registered after `capybara/rspec` fires before
        `Capybara.reset_sessions!` — while the browser page is still open and its
        stream is still reconnecting. `append_after` runs once the page is closed.

        Minitest is the same shape — require `pgbus/testing/minitest` and include
        `Pgbus::Testing::MinitestHelpers`, which hooks the lifecycle automatically.
      MD
    end
  end

  def modes
    DocsUI::Section("Testing modes") do
      md <<~'MD'
        Three modes control how `Publisher.publish` behaves in tests:
      MD
      DocsUI::Table(
        [ "Mode", "Behavior", "Use for" ],
        [
          [ [ :code, ":fake" ], "Captures events in memory; no PGMQ, no dispatch.", "Most unit/integration tests." ],
          [ [ :code, ":inline" ], "Captures AND dispatches to matching handlers.", "Testing handler side effects." ],
          [ [ :code, ":disabled" ], "Pass-through to the real publisher.", "Integration tests with real PGMQ." ]
        ]
      )
      DocsUI::Code(<<~RUBY)
        Pgbus::Testing.fake!     # global
        Pgbus::Testing.inline! do
          OrderService.create!(attrs) # handlers fire synchronously
        end                           # previous mode restored after the block
      RUBY
    end
  end

  def assertions
    DocsUI::Section("Event-bus assertions", description: "Shared by RSpec and Minitest.") do
      DocsUI::Code(<<~RUBY)
        assert_pgbus_published(count: 1, routing_key: "orders.created") do
          OrderService.create!(attrs)
        end

        assert_no_pgbus_published(routing_key: "orders.created") do
          OrderService.preview(attrs)
        end

        # Capture, then dispatch to handlers — for testing side effects:
        perform_published_events { OrderService.create!(attrs) }
      RUBY
    end
  end

  def matchers
    DocsUI::Section("The RSpec matcher") do
      md <<~'MD'
        `have_published_event` chains payload, header, and count constraints:
      MD
      DocsUI::Code(<<~RUBY)
        expect { publish_order(order) }
          .to have_published_event("orders.created")
          .with_payload(hash_including("id" => order.id))
          .with_headers(hash_including("x-tenant" => "acme"))
          .exactly(1)

        expect { publish_order(order) }.not_to have_published_event("orders.cancelled")
      RUBY
    end
  end

  def sse
    DocsUI::Section("SSE streams in tests") do
      md <<~'MD'
        SSE streams use `rack.hijack`, which spawns background threads that take
        their own database connections — incompatible with Rails'
        `use_transactional_fixtures`. pgbus detects the test environment and, under
        `Pgbus::Testing.fake!`/`.inline!`, auto-enables `streams_test_mode`: the
        streams endpoint returns a stub response with no hijack and no background
        threads, so your suite stays green without connection-pool surprises.
      MD
      DocsUI::Callout(:note) do
        plain "You rarely set "
        code { "streams_test_mode" }
        plain " yourself — activating a testing mode turns it on for you."
      end
      md <<~'MD'
        The stub also sends `retry: 86400000`, so an open page does not re-request
        it every few seconds. If `Pgbus::Testing.disabled!` runs while a page is
        still open (an `after` hook that fires before Capybara resets the session),
        a reconnect can start a real streamer inside the test process. Should that
        streamer's threads then outlive the bounded shutdown, `disabled!` raises
        `Pgbus::Testing::StreamerLeakError` naming them and the `append_after` fix,
        instead of letting the suite hang.
      MD
    end
  end

  def coalesced_broadcasts
    DocsUI::Section("Coalesced broadcasts in tests") do
      md <<~'MD'
        `broadcast(..., coalesce:)` normally flushes on a background timer thread,
        which could take the test's pinned connection. Unless you set
        `config.streams_coalesce_scheduler` yourself, `Pgbus::Testing.fake!` and
        `.inline!` flush each coalesced broadcast at once, on the thread that
        submits it, so every submit is broadcast. With no transaction open that
        happens before `broadcast` returns; a durable coalesced broadcast made
        inside an open transaction is submitted at its commit and flushes then.
        `disabled!` restores the timer.
      MD
    end
  end
end
