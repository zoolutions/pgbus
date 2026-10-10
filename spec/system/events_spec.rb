# frozen_string_literal: true

require "system_helper"

# Issue #494: one pending-events list that says what state each event is in,
# why, and what happens next; a processed-events audit that says whether each
# claim completed; subscribers that say whether a consumer drains them.
RSpec.describe "Events", type: :system do
  let(:now) { Time.now.utc }

  it "shows an empty state for subscribers, pending and processed events" do
    visit "/pgbus/events"

    expect(page).to have_css("h1", text: "Events")
    expect(page).to have_text("No subscribers registered")
    expect(page).to have_text("No pending events")
    expect(page).to have_text("No events processed yet")

    within("[data-testid=event-state-tabs]") { click_link "Retrying" }
    expect(page).to have_text("No events waiting to retry")
  end

  context "with subscribers" do
    before { @stub_data_source.subscribers_list = event_subscribers }

    it "says whether a running consumer drains each pattern and links to its pending events" do
      @stub_data_source.event_context = @stub_data_source.event_context.with(covered_queues: Set["pgbus_orders_handler"])
      visit "/pgbus/events"

      within("[data-testid=subscribers] tbody tr", text: "orders.#") do
        expect(page).to have_text("OrderHandler")
        expect(page).to have_text("Drained by a running consumer")
      end
      within("[data-testid=subscribers] tbody tr", text: "webhook.*") do
        expect(page).to have_text("No running consumer covers this pattern")
        click_link "View pending"
      end

      expect(page).to have_current_path(%r{/pgbus/events\?queue=pgbus_webhook_handler})
      expect(@stub_data_source.calls[:event_rows].last.first).to include(queue_name: "pgbus_webhook_handler")
    end
  end

  context "with a pending event in every state" do
    before do
      @stub_data_source.subscribers_list = event_subscribers
      @stub_data_source.event_rows_list = [
        event_row("ready", msg_id: 11),
        event_row("ready", msg_id: 12, queue_name: "pgbus_webhook_handler", logical_queue: "webhook_handler",
                           handler_class: "WebhookHandler", pattern: "webhook.*"),
        event_row("scheduled", msg_id: 13, vt: now + 5400),
        event_row("running", msg_id: 14, read_ct: 1, last_read_at: now - 12, vt: now + 48),
        event_row("retrying", msg_id: 15, read_ct: 2, last_read_at: now - 30, vt: now + 40,
                              error_class: "Net::ReadTimeout", error_message: "execution expired", failed_event_id: 7),
        event_row("retrying", msg_id: 16, read_ct: 5, last_read_at: now - 30, vt: now + 40,
                              error_class: "KeyError", failed_event_id: 8),
        event_row("retrying", source: "failed", id: 9, msg_id: 99, queue_name: "orders_handler", read_ct: 3, vt: nil,
                              error_class: "RuntimeError", failed_event_id: 9),
        event_row("ready", msg_id: 17, read_ct: 1, last_read_at: now - 400, vt: now - 100)
      ]
      @stub_data_source.events_ahead_hash = { ["pgbus_orders_handler", 11] => 3 }
      @stub_data_source.event_context = @stub_data_source.event_context.with(
        covered_queues: Set["pgbus_orders_handler"]
      )
    end

    it "renders the state tabs with their counts" do
      visit "/pgbus/events"

      within("[data-testid=event-state-tabs]") do
        expect(page).to have_css("a[data-state=all]", text: "8")
        expect(page).to have_css("a[data-state=ready]", text: "3")
        expect(page).to have_css("a[data-state=running]", text: "Handling")
        expect(page).to have_css("a[data-state=retrying]", text: "3")
        expect(page).to have_css("a[aria-current=page][data-state=all]")
      end
    end

    it "explains waiting rows in words: handler, state, attempts and what happens next" do
      visit "/pgbus/events"

      within("tr[data-event-row][data-msg-id='11']") do
        expect(page).to have_text("orders.created")
        expect(page).to have_text("evt-ready-11")
        expect(page).to have_text("OrderHandler")
        expect(page).to have_text("Ready")
        expect(page).to have_text("Waiting — 3 ahead · next: OrderHandler")
        expect(page).to have_text("0/5")
      end
      within("tr[data-event-row][data-msg-id='12']") do
        expect(page).to have_text("No running consumer subscribes to webhook.* (WebhookHandler)")
      end
      within("tr[data-event-row][data-msg-id='13']") { expect(page).to have_text("Scheduled — handled by OrderHandler in 1h") }
    end

    it "explains handling, failed and orphaned rows in words" do
      visit "/pgbus/events"

      within("tr[data-event-row][data-msg-id='14']") do
        expect(page).to have_text("Handling")
        expect(page).to have_text(/Being handled by OrderHandler — claimed \d+s ago · lease expires in \d+s/)
      end
      within("tr[data-event-row][data-msg-id='15']") do
        expect(page).to have_text(%r{OrderHandler attempt 2/5 failed: Net::ReadTimeout — next attempt in \d+s})
        expect(page).to have_text("2/5")
      end
      within("tr[data-event-row][data-msg-id='16']") { expect(page).to have_text("next read moves it to the DLQ") }
      within("tr[data-event-row][data-msg-id='99']") do
        expect(page).to have_text("Failed with RuntimeError in OrderHandler — message no longer in queue")
      end
      within("tr[data-event-row][data-msg-id='17']") { expect(page).to have_text("consumer likely died") }
    end

    it "says the queue is paused" do
      @stub_data_source.event_context = @stub_data_source.event_context.with(
        jobs: @stub_data_source.event_context.jobs.with(paused: Set["orders_handler"])
      )
      visit "/pgbus/events"

      within("tr[data-event-row][data-msg-id='11']") { expect(page).to have_text("Queue paused") }
    end

    it "shows only the selected state on a tab" do
      visit "/pgbus/events"

      within("[data-testid=event-state-tabs]") { click_link "Retrying" }

      expect(page).to have_css("tr[data-event-row]", count: 3)
      expect(page).to have_css("a[aria-current=page][data-state=retrying]")
    end

    it "expands a row to its payload, metadata, error and actions with the cells aligned" do
      visit "/pgbus/events"

      expect(page).to have_no_css("tr[data-event-detail]", visible: :visible)
      within("tr[data-event-row][data-msg-id='15']") { find("details[data-job-toggle] summary").click }

      within("tr[data-event-detail]", visible: :visible) do
        expect(page).to have_text("Net::ReadTimeout: execution expired")
        expect(page).to have_text("order_id")
        expect(page).to have_text("Read count:")
        expect(page).to have_text("Full JSON Payload")
        expect(page).to have_text("Edit & Retry")
        expect(page).to have_link("View failure")
      end
      expect(page).to have_css("tr[data-event-row] > td", minimum: 8)
    end

    it "marks a pending event handled" do
      visit "/pgbus/events"

      within("tr[data-event-row][data-msg-id='11']") { click_button "Mark Handled" }
      accept_confirm_dialog

      expect(page).to have_toast("handled")
      expect(@stub_data_source.calls[:mark_event_handled].last).to eq(%w[pgbus_orders_handler 11 OrderHandler])
    end

    it "offers no Mark Handled while the handler is running" do
      visit "/pgbus/events"

      within("tr[data-event-row][data-msg-id='14']") do
        expect(page).to have_no_button("Mark Handled")
        expect(page).to have_button("Discard")
      end
    end

    it "discards a pending event" do
      visit "/pgbus/events"

      within("tr[data-event-row][data-msg-id='11']") { click_button "Discard" }
      accept_confirm_dialog

      expect(page).to have_toast("discarded")
      expect(@stub_data_source.calls[:discard_event].last).to eq(%w[pgbus_orders_handler 11])
    end

    it "reroutes an event to another handler" do
      visit "/pgbus/events"

      within("tr[data-event-row][data-msg-id='11']") { find("details[data-job-toggle] summary").click }
      within("tr[data-event-detail]", visible: :visible) do
        find("summary", text: "Reroute").click
        click_button "WebhookHandler"
      end
      accept_confirm_dialog

      expect(page).to have_toast("rerouted")
      expect(@stub_data_source.calls[:reroute_event].last)
        .to eq(%w[pgbus_orders_handler 11 pgbus_webhook_handler])
    end

    it "edits the payload and re-enqueues the event" do
      visit "/pgbus/events"

      within("tr[data-event-row][data-msg-id='11']") { find("details[data-job-toggle] summary").click }
      within("tr[data-event-detail]", visible: :visible) do
        find("summary", text: "Edit & Retry").click
        fill_in "payload", with: '{"event_id":"evt-ready-11","payload":{"order_id":43}}'
        click_button "Edit & Retry"
      end
      accept_confirm_dialog

      expect(page).to have_toast("Payload updated")
      expect(@stub_data_source.calls[:edit_event_payload].last)
        .to eq(["pgbus_orders_handler", "11", '{"event_id":"evt-ready-11","payload":{"order_id":43}}'])
    end

    it "discards an orphaned failed row through its failed event" do
      visit "/pgbus/events"

      within("tr[data-event-row][data-msg-id='99']") { click_button "Discard" }
      accept_confirm_dialog

      expect(page).to have_toast("discarded")
      expect(@stub_data_source.calls[:discard_failed_event].last).to eq(["9"])
    end

    it "bulk-discards the selected events" do
      visit "/pgbus/events"

      within("turbo-frame#events-list") do
        expect(page).to have_css("input[data-bulk-item]", count: 7)
        find("input[data-bulk-select-all]").click
      end
      click_button "Discard Selected"
      accept_confirm_dialog

      expect(page).to have_toast("Discarded 7 events")
      expect(@stub_data_source.calls[:discard_selected_events].last.first.size).to eq(7)
    end
  end

  it "keeps a corrupt payload visible and editable" do
    @stub_data_source.subscribers_list = event_subscribers
    @stub_data_source.event_rows_list = [event_row("ready", msg_id: 21, payload: "{not json")]
    visit "/pgbus/events"

    within("tr[data-event-row][data-msg-id='21']") { find("details[data-job-toggle] summary").click }
    within("tr[data-event-detail]", visible: :visible) do
      find("summary", text: "Edit & Retry").click
      expect(find_field("payload").value).to eq("{not json")
    end
  end

  context "with more pending events than one page" do
    before do
      allow(Pgbus.configuration).to receive(:web_per_page).and_return(2)
      @stub_data_source.subscribers_list = event_subscribers
      @stub_data_source.event_rows_list = Array.new(3) do |i|
        event_row("ready", msg_id: 30 + i, payload: { event_id: "evt-paged-#{i}", routing_key: "orders.p#{i}" }.to_json)
      end
    end

    it "pages through the list" do
      visit "/pgbus/events"

      within("turbo-frame#events-list") do
        expect(page).to have_text("Showing 1–2 of 3")
        click_link "Next"
      end

      expect(page).to have_text("orders.p2")
      expect(page).to have_text("Showing 3–3 of 3")
    end
  end

  context "with processed events" do
    before do
      @stub_data_source.subscribers_list = event_subscribers
      @stub_data_source.events_list = [
        { "id" => 1, "event_id" => "evt-done", "handler_class" => "OrderHandler",
          "processed_at" => now - 301, "completed_at" => now - 300 },
        { "id" => 2, "event_id" => "evt-busy", "handler_class" => "OrderHandler",
          "processed_at" => (now - 3).iso8601, "completed_at" => nil },
        { "id" => 3, "event_id" => "evt-silent", "handler_class" => "OrderHandler",
          "processed_at" => now - 120, "completed_at" => nil },
        { "id" => 4, "event_id" => "evt-legacy", "handler_class" => "WebhookHandler", "processed_at" => now - 60 }
      ]
      @stub_data_source.replay_states_hash = { 4 => :not_archived }
    end

    it "says whether each claim completed, is being handled or went silent" do
      visit "/pgbus/events"

      within("turbo-frame#processed-events") do
        within("tr", text: "evt-done") { expect(page).to have_text("Completed 5m ago") }
        within("tr", text: "evt-busy") { expect(page).to have_text(/Being handled — claim refreshed \d+s ago/) }
        within("tr", text: "evt-silent") do
          expect(page).to have_text("Claim went silent 2m ago — the next delivery re-runs the handler")
        end
        within("tr", text: "evt-legacy") { expect(page).to have_text("Recorded 1m ago") }
      end
    end

    it "replays a processed event after confirming" do
      visit "/pgbus/events"

      within("turbo-frame#processed-events tr", text: "evt-done") { click_button "Replay" }
      accept_confirm_dialog

      expect(page).to have_toast("replayed")
      expect(@stub_data_source.calls[:replay_event].last.first).to include("event_id" => "evt-done")
    end

    it "says why an event cannot be replayed instead of offering the action" do
      visit "/pgbus/events"

      within("turbo-frame#processed-events tr", text: "evt-legacy") do
        expect(page).to have_no_button("Replay")
        expect(page).to have_text("No longer in the archive")
      end
    end

    it "shows a processed event's state on its page" do
      visit "/pgbus/events/1"

      expect(page).to have_text("Completed 5m ago")
      expect(page).to have_button("Replay")
    end

    it "pages the processed events on their own page parameter" do
      allow(Pgbus.configuration).to receive(:web_per_page).and_return(2)
      visit "/pgbus/events"

      within("turbo-frame#processed-events") do
        expect(page).to have_text("Showing 1–2 of 4")
        click_link "Next"
      end

      expect(page).to have_current_path(/processed_page=2/)
      expect(@stub_data_source.calls[:processed_events].last.first).to include(page: 2)
      expect(@stub_data_source.calls[:event_rows].last.first).to include(page: 1)
    end
  end
end
