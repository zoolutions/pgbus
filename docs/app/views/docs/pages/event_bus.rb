# frozen_string_literal: true

# The pub/sub event bus: publishing topics, subscribing with patterns, and the
# idempotency that makes handlers safe to re-run. The fanout diagram anchors it.
class Views::Docs::Pages::EventBus < DocsUI::Page
  title "Event bus"
  eyebrow "Guide"

  def lead = "Publish a topic once; AMQP-style patterns fan it out to idempotent subscribers."

  def content
    publishing
    subscribing
    routing
    fair_share
    current_attributes
    idempotency
  end

  private

  def publishing
    DocsUI::Section("Publish an event") do
      md <<~'MD'
        An event is a routing key plus a payload. Publish it now, or schedule it
        with a delay:
      MD
      DocsUI::Code(<<~RUBY)
        Pgbus.publish(
          "orders.created",
          { order_id: order.id, total: order.total }
        )

        Pgbus.publish_later(
          "invoices.due",
          { invoice_id: invoice.id },
          delay: 30.days
        )
      RUBY
      DocsUI::Callout(:note) do
        plain "The payload is JSON. Keep it to data a subscriber can act on — ids, "
        plain "amounts, timestamps — not whole objects."
      end
      md <<~'MD'
        `Pgbus.publish` / `Pgbus.publish_later` are top-level shortcuts for
        `Pgbus::EventBus::Publisher.publish` / `.publish_later` (symmetric with
        `Pgbus.stream`). The long form still works if you prefer it.
      MD
    end
  end

  def subscribing
    DocsUI::Section("Subscribe with a handler") do
      md <<~'MD'
        A handler subclasses `Pgbus::EventBus::Handler` and implements `#handle`.
        Register it against a pattern in an initializer:
      MD
      DocsUI::Code(<<~RUBY, filename: "app/handlers/order_created_handler.rb")
        class OrderCreatedHandler < Pgbus::EventBus::Handler
          idempotent! # deduplicate by (event_id, handler_class)

          def handle(event)
            order_id = event.payload["order_id"]
            Analytics.track_order(order_id)
            InventoryService.reserve(order_id)
          end
        end
      RUBY
      DocsUI::Code(<<~RUBY, filename: "config/initializers/pgbus.rb")
        Pgbus::EventBus::Registry.instance.subscribe("orders.created", OrderCreatedHandler)
      RUBY
    end
  end

  def routing
    DocsUI::Section("Topic routing", description: "AMQP-style patterns: * for one segment, # for many.") do
      md <<~'MD'
        Patterns match dotted routing keys the way AMQP topic exchanges do: `*`
        matches exactly one segment, `#` matches zero or more. One publish fans out
        to every subscriber whose pattern matches.
      MD
      render Components::Diagrams::EventFanout.new
      DocsUI::Table(
        [ "Pattern", "Matches", "Doesn't match" ],
        [
          [ [ :code, "orders.created" ], [ :code, "orders.created" ], [ :code, "orders.updated" ] ],
          [ [ :code, "orders.*" ], [ :md, "`orders.created`, `orders.updated`" ], [ :code, "orders.line.added" ] ],
          [ [ :code, "orders.#" ], [ :md, "`orders.created`, `orders.line.added`" ], [ :code, "payments.captured" ] ]
        ]
      )
      DocsUI::Code(<<~RUBY)
        # Audit everything under the orders.* namespace, at any depth:
        Pgbus::EventBus::Registry.instance.subscribe("orders.#", OrderAuditHandler)
      RUBY
      md <<~'MD'
        Fanout is per queue, and a queue is consumed only by the handler(s)
        registered against it: one event matching N subscribers becomes N
        deliveries, one per handler, and each handler runs exactly once per
        event. Two handlers registered with the same explicit `queue_name:`
        share a queue and both run on its delivery. A message sitting in a queue
        no subscriber in this process owns — a stale topic binding from a
        renamed or removed handler — is archived, logged once per queue, and
        reported as `pgbus.event_unrouted`.
      MD
    end
  end

  def fair_share
    DocsUI::Section("Fair share across tenants", description: "One tenant's event burst must not starve everyone else's handlers.") do
      md <<~'MD'
        A subscriber queue is FIFO, so a bulk import emitting `orders.created`
        100 000 times for one tenant puts every other tenant's events behind it —
        in *every* handler subscribed to that topic. `config.event_fair_share`
        is the event twin of `config.fair_share` (jobs): a callable tags each
        event with a key (and an optional weight) at publish time, and the
        consumer's read interleaves across keys inside each subscriber queue —
        the same weighted, work-conserving round-robin workers use.
      MD
      DocsUI::Code(<<~RUBY, filename: "config/initializers/pgbus.rb")
        Pgbus.configure do |config|
          # Receives the Pgbus::Event (routing_key, payload as passed to publish, headers).
          # Return a key (String/Symbol/Integer), [key, weight], or nil to leave the event unkeyed.
          config.event_fair_share = ->(event) { Current.tenant&.id || event.payload["tenant_id"] }
        end
      RUBY
      md <<~'MD'
        - The key rides in the **event envelope** (`pgbus_fair_key` next to
          `event_id` / `published_at`), never inside your payload — handlers see
          `event.payload` unchanged, and the tag is copied to every subscriber
          queue the topic fans out to.
        - It is resolved where you publish — `Pgbus.publish`, `publish_later`,
          and `Pgbus::Outbox.publish_event` — so `Current.*` context is available,
          and the outbox relay carries it to the bus unchanged. A system writing
          `pgbus_outbox_entries` directly can set `"pgbus_fair_key"` in the
          envelope JSON itself.
        - Independent of `fair_share`: enable either side, or both with the same resolver shape.
        - Consumers keep strict list order *across* their subscriber queues and
          are fair *within* each; the circuit breaker still skips a paused queue.
        - The fair index is built on each subscriber queue by `setup_all!` (at
          creation) and by the consumer at boot (`CONCURRENTLY`). Split rule,
          weights, cost model and index details: [Routing & ordering](/docs/routing-ordering).
      MD
    end
  end

  def current_attributes
    DocsUI::Section("Current attributes", description: "Request context travels with the event.") do
      md <<~'MD'
        The same `config.current_attributes` switch that carries
        `Current.tenant` / `Current.user` / `Current.request_id` into jobs
        ([Active Job guide](/docs/active-job)) carries it into event handlers.
        With it on, `Pgbus.publish` snapshots the assigned attributes of each
        persisted class into the event envelope under `pgbus_current` — same
        filters (`only:` / `except:`), same `ActiveJob::Arguments` serialization
        — and the consumer restores them around `handle`:
      MD
      DocsUI::Code(<<~RUBY)
        # In the request: Current.tenant = tenant; then…
        Pgbus.publish("orders.created", { order_id: order.id })

        # In the handler, on the consumer:
        class OrderCreatedHandler < Pgbus::EventBus::Handler
          def handle(event)
            Current.tenant   # => the publisher's tenant
            event.context    # the raw stored form, if you want it explicitly
          end
        end
      RUBY
      md <<~'MD'
        - The context lives in the **envelope** (next to `event_id`), never in
          `event.payload`, and is copied to every subscriber queue the topic
          fans out to. Previous `Current` values come back after each `handle`.
        - `Pgbus::Outbox.publish_event` captures at write time — inside your
          transaction, where `Current` is set — and the relay carries it to the
          bus unchanged.
        - GlobalIDs inside the context are gated by `allowed_global_id_models`
          before anything is loaded, exactly like job arguments.
        - `Pgbus::Testing` round-trips it: fake-mode events expose `context`,
          and `drain!` / inline dispatch restore it around handlers.
        - The dashboard's pending-events rows show a **Context** card (through
          the same parameter filter as the payload).
      MD
    end
  end

  def idempotency
    DocsUI::Section("Idempotent handlers", description: "Safe to re-run — deduplicated by (event_id, handler).") do
      md <<~'MD'
        At-least-once delivery means a handler can be invoked more than once for the
        same event (a retry after a crash mid-handle). Declaring `idempotent!`
        records each `(event_id, handler_class)` in `pgbus_processed_events` with a
        unique index — a second delivery of the same event to the same handler is
        skipped, backed by an in-memory cache to avoid the round trip when it can.
      MD
      DocsUI::Callout(:tip) do
        plain "How long processed-event records are kept is "
        code { "idempotency_ttl" }
        plain " (default 7 days). The dispatcher purges older rows. "
        plain "Set it long enough to cover your worst-case retry window."
      end
    end
  end
end
