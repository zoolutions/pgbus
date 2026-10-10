# frozen_string_literal: true

module Pgbus
  module Test
    class StubDataSource
      # The Events page's sample data (issue #494): three subscribers, one of
      # them on a pattern no running consumer covers, a pending event in every
      # state and reason, and a processed-events audit past one page with a
      # claim in every state. Part of fill_sample_data!.
      module SampleEvents
        INVOICE_QUEUE = "pgbus_billing_invoice_handler"
        SLACK_QUEUE = "pgbus_notifications_slack_handler"
        WEBHOOK_QUEUE = "pgbus_webhooks_delivery_handler"

        private

        def fill_sample_events!(now)
          @subscribers_list = sample_subscribers
          @event_rows_list = sample_event_rows(now)
          @events_ahead_hash = { [INVOICE_QUEUE, 801] => 3 }
          @event_context = @event_context.with(covered_queues: Set[INVOICE_QUEUE, SLACK_QUEUE])
          # Newest claim first, as DataSource#processed_events orders them.
          @events_list = sample_processed_events(now).sort_by { |e| -Pgbus::Web::JobState.time(e["processed_at"]).to_f }
          @replay_states_hash = { 6 => :not_archived, 5 => :no_handler }
        end

        def sample_subscribers
          [
            { pattern: "invoice.*", handler_class: "Billing::InvoiceHandler", queue_name: "billing_invoice_handler",
              physical_queue_name: INVOICE_QUEUE },
            { pattern: "user.signed_up", handler_class: "Notifications::SlackHandler",
              queue_name: "notifications_slack_handler", physical_queue_name: SLACK_QUEUE },
            { pattern: "webhook.#", handler_class: "Webhooks::DeliveryHandler", queue_name: "webhooks_delivery_handler",
              physical_queue_name: WEBHOOK_QUEUE }
          ]
        end

        # One row per state and reason, plus an orphaned failed row whose
        # message left its queue.
        def sample_event_rows(now)
          invoice = { queue_name: INVOICE_QUEUE, logical_queue: "billing_invoice_handler",
                      handler_class: "Billing::InvoiceHandler", pattern: "invoice.*" }
          slack = { queue_name: SLACK_QUEUE, logical_queue: "notifications_slack_handler",
                    handler_class: "Notifications::SlackHandler", pattern: "user.signed_up" }
          webhook = { queue_name: WEBHOOK_QUEUE, logical_queue: "webhooks_delivery_handler",
                      handler_class: "Webhooks::DeliveryHandler", pattern: "webhook.#" }
          [
            sample_event(now, 801, "ready", "invoice.created", **invoice),
            sample_event(now, 802, "ready", "webhook.delivered", **webhook),
            sample_event(now, 803, "scheduled", "invoice.reminder", vt: now + 1800, **invoice),
            sample_event(now, 804, "running", "user.signed_up", read_ct: 1, last_read_at: now - 4, vt: now + 26, **slack),
            sample_event(now, 805, "retrying", "invoice.paid", read_ct: 2, last_read_at: now - 5, vt: now + 25,
                                                               failed_event_id: 11, error_class: "Net::ReadTimeout",
                                                               error_message: "execution expired", **invoice),
            sample_event(now, 806, "retrying", "user.signed_up", read_ct: 5, last_read_at: now - 20, vt: now + 10,
                                                                 failed_event_id: 12, error_class: "KeyError",
                                                                 error_message: "key not found: :email", **slack),
            sample_event(now, 790, "retrying", "invoice.voided", source: "failed", id: 13, read_ct: 3, vt: nil,
                                                                 enqueued_at: nil, failed_event_id: 13,
                                                                 error_class: "Stripe::InvalidRequestError",
                                                                 error_message: "No such invoice: in_xxx",
                                                                 **invoice, queue_name: "billing_invoice_handler"),
            sample_event(now, 807, "ready", "invoice.created", read_ct: 1, last_read_at: now - 400, vt: now - 100,
                                                               **invoice)
          ]
        end

        def sample_event(now, msg_id, state, routing_key, **attrs)
          { source: "queue", id: msg_id, msg_id: msg_id, job_class: nil, read_ct: 0, enqueued_at: now - (msg_id - 780),
            last_read_at: nil, vt: now - 1, state: state, error_class: nil, error_message: nil,
            failed_event_id: nil, concurrency_key: nil, slots_held: nil, slots_max: nil, headers: nil,
            payload: { event_id: "evt-pending-#{msg_id}", payload: { id: msg_id }, routing_key: routing_key,
                       published_at: (now - 900).utc.iso8601 }.to_json }.merge(attrs)
        end

        # A completed, a handling, an abandoned and a single-phase (legacy)
        # claim, then completed ones past one page.
        def sample_processed_events(now)
          special = [
            ["Billing::InvoiceHandler", now - 120, now - 119],
            ["Notifications::SlackHandler", now - 3, nil],
            ["Billing::InvoiceHandler", now - 600, nil],
            ["Notifications::SlackHandler", now - 900, :legacy],
            ["Legacy::RemovedHandler", now - 1200, now - 1199]
          ]
          rest = Array.new(SampleData::PAGED_ROWS - special.size) do |i|
            ["Billing::InvoiceHandler", now - (1800 + (i * 300)), now - (1799 + (i * 300))]
          end
          (special + rest).each_with_index.map do |(handler, processed_at, completed_at), i|
            row = { "id" => i + 1, "event_id" => "evt-processed-#{i + 1}", "handler_class" => handler,
                    "processed_at" => i.even? ? processed_at : processed_at.utc.iso8601 }
            completed_at == :legacy ? row : row.merge("completed_at" => completed_at)
          end
        end
      end
    end
  end
end
