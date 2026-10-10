# frozen_string_literal: true

# One DataSource#event_rows hash for the Events page (issue #494): a job-list
# row scoped to a handler queue, named with its handler and pattern. The
# including spec defines `now`.
module EventRowBuilder
  def event_row(state, **attrs)
    { source: "queue", id: attrs[:msg_id] || 1, queue_name: "pgbus_orders_handler", logical_queue: "orders_handler",
      job_class: nil, read_ct: 0, enqueued_at: now - 60, last_read_at: nil, vt: now - 1, state: state,
      error_class: nil, error_message: nil, failed_event_id: nil, concurrency_key: nil, slots_held: nil,
      slots_max: nil, handler_class: "OrderHandler", pattern: "orders.#",
      payload: { event_id: "evt-#{state}-#{attrs[:msg_id] || 1}", payload: { order_id: 42 },
                 routing_key: "orders.created", published_at: (now - 120).iso8601 }.to_json,
      headers: nil }.merge(attrs)
  end

  def event_subscribers
    [{ pattern: "orders.#", handler_class: "OrderHandler", queue_name: "orders_handler",
       physical_queue_name: "pgbus_orders_handler" },
     { pattern: "webhook.*", handler_class: "WebhookHandler", queue_name: "webhook_handler",
       physical_queue_name: "pgbus_webhook_handler" }]
  end
end

RSpec.configure do |config|
  config.include EventRowBuilder, type: :system
  config.include EventRowBuilder, type: :request
end
