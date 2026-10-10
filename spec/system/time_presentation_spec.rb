# frozen_string_literal: true

require "system_helper"

# Issue #497: every timestamp is a <time datetime title> (relative, with the
# exact value in the tooltip) or, in expanded rows and on show pages, the
# absolute followed by the relative. Every age is a duration with units.
RSpec.describe "Time presentation", type: :system do
  let(:now) { Time.now.utc }
  let(:absolute) { /\d{4}-\d\d-\d\d \d\d:\d\d:\d\d UTC/ }
  let(:stamp) { "time[datetime][title]" }

  describe "Jobs" do
    def job_row(state, **attrs)
      { source: "queue", id: attrs[:msg_id], queue_name: "pgbus_default", logical_queue: "default",
        job_class: "#{state.capitalize}Job", read_ct: 0, enqueued_at: now - 60, last_read_at: nil,
        vt: now - 1, state: state, error_class: nil, error_message: nil, failed_event_id: nil,
        concurrency_key: nil, slots_held: nil, slots_max: nil,
        payload: { job_class: "#{state.capitalize}Job", job_id: "job-#{state}", arguments: [42] }.to_json,
        headers: nil }.merge(attrs)
    end

    before do
      @stub_data_source.job_rows_list = [
        job_row("ready", msg_id: 11),
        job_row("scheduled", msg_id: 12, vt: now + 5400),
        job_row("running", msg_id: 13, read_ct: 1, last_read_at: now - 12, vt: now + 48)
      ]
    end

    it "puts both moments of a running job's reason in <time> elements" do
      visit "/pgbus/jobs"

      within("tr[data-state=running] [data-testid=job-reason]") do
        expect(page).to have_css(stamp, count: 2)
        expect(page).to have_css(stamp, text: /\A\d+s ago\z/)
      end
    end

    it "gives a scheduled job its clock time" do
      visit "/pgbus/jobs"

      within("tr[data-state=scheduled] [data-testid=job-reason]") do
        expect(page).to have_css(stamp, text: /\Ain 1h \(\d\d:\d\d\)\z/)
      end
    end

    it "shows the exact enqueue time with the relative beside it in the expanded row" do
      visit "/pgbus/jobs"
      first("details[data-job-toggle] summary").click

      expect(page).to have_css("time[datetime]", text: absolute)
      expect(page).to have_text(/Enqueued: \d{4}-\d\d-\d\d \d\d:\d\d:\d\d UTC \(1m ago\)/)
    end

    it "shows a failed job's failure time as the absolute on its page" do
      @stub_data_source.failed_events_list = [
        { "id" => 7, "queue_name" => "pgbus_default", "error_class" => "RuntimeError", "error_message" => "boom",
          "failed_at" => (now - 120).iso8601, "retry_count" => 1, "payload" => "{}" }
      ]

      visit "/pgbus/jobs/7"

      expect(page).to have_css("time[datetime]", text: absolute)
      expect(page).to have_text("(2m ago)")
    end
  end

  describe "Queues" do
    it "renders the claimable and newest ages as durations, never bare seconds" do
      visit "/pgbus/queues"

      expect(page).to have_css("td[data-label='Oldest claimable']", text: "1m 30s")
      expect(page).to have_css("td[data-label='Newest']", text: "5s")
      expect(page).to have_css("td[data-label='Oldest claimable']", text: "1h 0m")
    end

    it "renders the queue header ages as durations" do
      visit "/pgbus/queues/pgbus_default"

      expect(page).to have_text("2m 0s")
      expect(page).to have_text("1m 30s")
    end

    context "with a message that becomes visible in the future" do
      before do
        @stub_data_source.jobs_list = [
          { msg_id: 42, queue_name: "pgbus_default", read_ct: 1, enqueued_at: (now - 60).iso8601,
            vt: (now + 3600).iso8601, last_read_at: (now - 30).iso8601,
            message: '{"job_class":"TestJob","job_id":"abc-123","arguments":[]}' }
        ]
      end

      it "says when it becomes visible instead of a negative age" do
        visit "/pgbus/queues/pgbus_default"

        expect(page).to have_css(stamp, text: /\Ain (59m|1h)\z/)
        expect(page).to have_no_text(/-\d+[smhd] ago/)
      end

      it "shows the exact visibility time in the expanded row" do
        visit "/pgbus/queues/pgbus_default"
        find("details.group summary").click

        expect(page).to have_text(/Visible at: \d{4}-\d\d-\d\d \d\d:\d\d:\d\d UTC \(in (59m|1h)\)/)
        expect(page).to have_css("time[datetime]", text: absolute, minimum: 2)
      end
    end
  end

  describe "Dashboard" do
    it "renders the queues table's oldest claimable age as a duration" do
      visit "/pgbus"

      expect(page).to have_css("td[data-label='Oldest claimable']", text: "1m 30s")
    end

    it "renders a recent failure's time as <time>" do
      @stub_data_source.failed_events_list = [
        { "id" => 1, "queue_name" => "pgbus_default", "error_class" => "RuntimeError",
          "error_message" => "Something went wrong", "failed_at" => (now - 300).iso8601 }
      ]

      visit "/pgbus"

      expect(page).to have_css("td[data-label=Time] #{stamp}", text: "5m ago")
    end
  end

  describe "Processes" do
    it "renders the heartbeat as <time> with the exact value in its tooltip" do
      visit "/pgbus/processes"

      expect(page).to have_css("td[data-label='Last Heartbeat'] #{stamp}", text: /\A(now|\d+s ago)\z/)
    end
  end

  describe "Recurring tasks" do
    before do
      @stub_data_source.recurring_tasks_list = [
        { id: 1, key: "capture_stats", class_name: "CaptureStatsJob", command: nil, schedule: "*/5 * * * *",
          human_schedule: "Every 5 minutes", queue_name: "default", priority: 2, description: nil,
          enabled: true, static: true, next_run_at: now + 330, last_run_at: now - 120,
          created_at: now - 86_400, updated_at: now - 3600 }
      ]
      @stub_data_source.recurring_executions_list = [{ run_at: now - 300, created_at: now - 300 }]
    end

    it "renders the last and next run as <time>" do
      visit "/pgbus/recurring_tasks"

      expect(page).to have_css(stamp, text: "2m ago")
      expect(page).to have_css(stamp, text: "in 5m")
    end

    it "leads the task page with the relative next run and shows the absolute below it" do
      visit "/pgbus/recurring_tasks/1"

      expect(page).to have_css(stamp, text: /\Ain 5m \(\d\d:\d\d\)\z/)
      expect(page).to have_css("time[datetime]:not([title])", text: absolute)
      expect(page).to have_text(/\d{4}-\d\d-\d\d \d\d:\d\d:\d\d UTC \(5m ago\)/)
    end
  end

  describe "Dead letter queue" do
    before do
      @stub_data_source.dlq_messages_list = [
        { msg_id: 501, queue_name: "pgbus_default_dlq", read_ct: 6, enqueued_at: (now - 7200).iso8601,
          vt: (now - 3600).iso8601, last_read_at: (now - 3600).iso8601, headers: nil,
          message: '{"job_class":"FailJob","job_id":"dlq-1","arguments":[]}' }
      ]
    end

    it "renders the enqueue time as <time> in the list" do
      visit "/pgbus/dlq"

      expect(page).to have_css(stamp, text: "2h ago")
    end

    it "shows the exact times on the message page" do
      visit "/pgbus/dlq/501"

      expect(page).to have_text(/\d{4}-\d\d-\d\d \d\d:\d\d:\d\d UTC \(2h ago\)/)
      expect(page).to have_text(/\d{4}-\d\d-\d\d \d\d:\d\d:\d\d UTC \(1h ago\)/)
    end
  end

  describe "Events" do
    it "renders a processed event's time as <time>, and the absolute on its page" do
      @stub_data_source.events_list = [
        { "id" => 1, "event_id" => "evt-1", "handler_class" => "OrderHandler",
          "processed_at" => (now - 600).iso8601 }
      ]

      visit "/pgbus/events"
      expect(page).to have_css(stamp, text: "10m ago")

      visit "/pgbus/events/1"
      expect(page).to have_text(/\d{4}-\d\d-\d\d \d\d:\d\d:\d\d UTC \(10m ago\)/)
    end
  end

  describe "Batches" do
    it "shows the exact created and finished times on the batch page" do
      @stub_data_source.batch_detail_hash = {
        batch_id: "b-1", description: "Backfill", status: "finished", total_jobs: 2, completed_jobs: 2,
        failed_jobs: 0, pending_jobs: 0, progress_pct: 100, properties: nil,
        created_at: now - 7200, finished_at: now - 3600
      }

      visit "/pgbus/batches/b-1"

      expect(page).to have_text(/\d{4}-\d\d-\d\d \d\d:\d\d:\d\d UTC \(2h ago\)/)
      expect(page).to have_text(/\d{4}-\d\d-\d\d \d\d:\d\d:\d\d UTC \(1h ago\)/)
    end
  end

  describe "Outbox" do
    it "renders the oldest unpublished age as a duration" do
      @stub_data_source.outbox_stats_hash = @stub_data_source.outbox_stats_hash.merge(oldest_unpublished_age: 3600)

      visit "/pgbus/outbox"

      expect(page).to have_text("1h 0m")
      expect(page).to have_no_text("3600s")
    end
  end
end
