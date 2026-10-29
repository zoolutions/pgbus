# frozen_string_literal: true

# Zero to running: configure, enqueue a job, publish an event, boot workers,
# mount the dashboard. Each step links to the deeper guide.
class Views::Docs::Pages::QuickStart < DocsUI::Page
  title "Quick start"
  eyebrow "Getting started"

  def lead = "Configure, run an ActiveJob, publish an event, start workers, and mount the dashboard."

  def content
    configure
    activejob
    event_bus
    start_workers
    dashboard
  end

  private

  def configure
    DocsUI::Section("1. Configure (optional)", description: "pgbus works with zero config in Rails.") do
      md <<~'MD'
        pgbus uses your existing Active Record connection, so it runs with no
        configuration. For custom setups, drop a Ruby initializer:
      MD
      DocsUI::Code(<<~RUBY, filename: "config/initializers/pgbus.rb")
        Pgbus.configure do |c|
          c.queue_prefix       = "myapp"
          c.max_retries        = 5
          c.visibility_timeout = 30.seconds   # ActiveSupport::Duration accepted
          c.idempotency_ttl    = 7.days

          # Worker recycling — prevents long-lived processes from leaking memory
          c.max_jobs_per_worker = 10_000
          c.max_memory_mb       = 512
          c.max_worker_lifetime = 1.hour

          # Capsule string DSL — Sidekiq-style "queues: threads; queues: threads"
          c.workers = "default, mailers: 10; critical: 5"
        end
      RUBY
      md <<~'MD'
        The full set of knobs is in [Configuration](/docs/configuration) and the
        [Configuration reference](/docs/configuration-reference).
      MD
    end
  end

  def activejob
    DocsUI::Section("2. Use as the ActiveJob backend") do
      md <<~'MD'
        Point Active Job at pgbus:
      MD
      DocsUI::Code(<<~RUBY, filename: "config/application.rb")
        config.active_job.queue_adapter = :pgbus
      RUBY
      md <<~'MD'
        That's it — your existing jobs work unchanged:
      MD
      DocsUI::Code(<<~RUBY, filename: "app/jobs/order_confirmation_job.rb")
        class OrderConfirmationJob < ApplicationJob
          queue_as :mailers

          def perform(order)
            OrderMailer.confirmation(order).deliver_now
          end
        end

        # Enqueue
        OrderConfirmationJob.perform_later(order)

        # Schedule
        OrderConfirmationJob.set(wait: 5.minutes).perform_later(order)
      RUBY
      md <<~'MD'
        More in [ActiveJob adapter](/docs/active-job).
      MD
    end
  end

  def event_bus
    DocsUI::Section("3. Publish an event (optional)") do
      md <<~'MD'
        Publish with AMQP-style topic routing:
      MD
      DocsUI::Code(<<~RUBY)
        Pgbus.publish(
          "orders.created",
          { order_id: order.id, total: order.total }
        )
      RUBY
      md <<~'MD'
        Subscribe with an idempotent handler:
      MD
      DocsUI::Code(<<~RUBY, filename: "app/handlers/order_created_handler.rb")
        class OrderCreatedHandler < Pgbus::EventBus::Handler
          idempotent! # deduplicate by (event_id, handler_class)

          def handle(event)
            order_id = event["order_id"]
            Analytics.track_order(order_id)
          end
        end

        # Register in an initializer
        Pgbus::EventBus::Registry.instance.subscribe("orders.created", OrderCreatedHandler)

        # Wildcard patterns: orders.# matches orders.created, orders.shipped.confirmed, …
        Pgbus::EventBus::Registry.instance.subscribe("orders.#", OrderAuditHandler)
      RUBY
      md <<~'MD'
        The full routing and idempotency story is in [Event bus](/docs/event-bus).
      MD
    end
  end

  def start_workers
    DocsUI::Section("4. Start workers") do
      md <<~'MD'
        Boot the supervisor, which manages workers (ActiveJob queues), the
        dispatcher (maintenance), and event consumers:
      MD
      DocsUI::Code(<<~SHELL, lexer: :shell)
        bundle exec pgbus start
      SHELL
      md <<~'MD'
        See [Running workers](/docs/running-workers) for recycling, roles, and the
        CLI flags.
      MD
    end
  end

  def dashboard
    DocsUI::Section("5. Mount the dashboard") do
      md <<~'MD'
        Mount the engine to see queues, jobs, processes, failures, and dead
        letters — auto-refreshing over Turbo Frames, no WebSocket:
      MD
      DocsUI::Code(<<~RUBY, filename: "config/routes.rb")
        mount Pgbus::Engine => "/pgbus"
      RUBY

      DocsUI::Callout(:warning) do
        plain "Protect the dashboard in production — a "
        code { "web_auth" }
        plain " lambda or a "
        code { "base_controller_class" }
        plain " that inherits your app's authentication. See "
        a(href: "/docs/dashboard", class: "link") { "Dashboard" }
        plain "."
      end
    end
  end
end
