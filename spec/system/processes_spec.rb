# frozen_string_literal: true

require "system_helper"

# Issue #493: every process row says what the process is doing, why its
# status is what it is, and what happens next.
RSpec.describe "Processes", type: :system do
  include ActiveSupport::Testing::TimeHelpers

  # Relative times are asserted: freeze the clock on a whole second.
  let(:now) { Time.utc(2026, 10, 10, 12, 0, 0) }

  around { |example| travel_to(now) { example.run } }

  def row(id) = find("tr#process-#{id}")

  it "shows the empty state across every column" do
    @stub_data_source.processes_list = []

    visit "/pgbus/processes"

    expect(page).to have_css("h1", text: "Processes")
    expect(page).to have_css("td[colspan='5']", text: "No processes running")
  end

  context "with sample processes" do
    before { @stub_data_source.processes_list = @stub_data_source.sample_processes(now) }

    it "says why a healthy worker is healthy, what it does and how close it is to a recycle", :aggregate_failures do
      visit "/pgbus/processes"

      within(row(2)) do
        expect(page).to have_text("worker")
        expect(page).to have_text("web-01.prod · 101")
        expect(page).to have_text("under supervisor 100")
        expect(page).to have_text("Healthy")
        expect(page).to have_text("Loop ticked 5s ago (limit 1m 30s)")
        expect(page).to have_text("Drains default, mailers · 3/5 threads busy · 2 buffered · 12.4 jobs/s")
        expect(page).to have_text("1,240 / 10,000 jobs · 310 / 512 MB · up 42m 0s / 1h 0m")
        expect(page).to have_text("up 42m 0s")
      end
    end

    it "says a stalled worker under a live supervisor gets restarted" do
      visit "/pgbus/processes"

      within(row(3)) do
        expect(page).to have_text("Stalled")
        expect(page).to have_text("Loop last ticked 2m ago (limit 1m 30s) — the supervisor's watchdog restarts it")
      end
    end

    it "says a stalled standalone consumer has no one to restart it", :aggregate_failures do
      visit "/pgbus/processes"

      within(row(7)) do
        expect(page).to have_text("Stalled")
        expect(page).to have_text("Loop last ticked 2m ago")
        expect(page).to have_text("no supervisor to restart it")
        expect(page).to have_text("Consumes orders.*, users.* · 1/3 threads busy · 5,230 handled")
        expect(page).to have_no_text("under supervisor")
      end
    end

    it "says a stale process is probably dead and when the dispatcher removes it" do
      visit "/pgbus/processes"

      within(row(6)) do
        expect(page).to have_text("Stale")
        expect(page).to have_text("No heartbeat for 7m 0s (limit 5m 0s) — probably dead; " \
                                  "removed at the dispatcher's next sweep (within 5m 0s)")
      end
    end

    it "says what the dispatcher and scheduler do", :aggregate_failures do
      visit "/pgbus/processes"

      within(row(4)) do
        expect(page).to have_text("Loop ticked 3s ago (limit 1m 31s)")
        expect(page).to have_text("Runs maintenance every 1s (scheduled jobs, stale processes, archives)")
      end
      within(row(5)) { expect(page).to have_text("Runs 14 recurring tasks, checks every 1s") }
    end

    it "says what the supervisor supervises and how long it has been up", :aggregate_failures do
      visit "/pgbus/processes"

      within(row(1)) do
        expect(page).to have_text("Heartbeat 12s ago (limit 5m 0s)")
        expect(page).to have_text("Supervises 2 workers, dispatcher, scheduler, outbox poller")
        expect(page).to have_text("up 3h 0m")
      end
    end

    it "lists each supervisor before its children, then standalone processes, host by host" do
      visit "/pgbus/processes"

      ids = all("tbody tr[id^='process-']").map { |tr| tr[:id].delete_prefix("process-").to_i }
      expect(ids).to eq([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    end

    it "names each fork's capsule and process slot (issue #503)" do
      visit "/pgbus/processes"

      expect(row(8)).to have_text("capsule render · process 1/2")
      expect(row(9)).to have_text("capsule render · process 2/2")
    end

    it "says less, without failing, for a worker from before the new heartbeat keys", :aggregate_failures do
      visit "/pgbus/processes"

      within(row(10)) do
        expect(page).to have_text("Drains default, mailers · 5 threads")
        expect(find("[data-testid=process-recycle]")).to have_text("—")
        expect(page).to have_no_text("under supervisor")
        expect(page).to have_no_text("up ")
      end
    end

    it "says stall detection is off when stall_threshold is nil" do
      @stub_data_source.process_thresholds_overrides = { stall_threshold: nil }

      visit "/pgbus/processes"

      expect(row(2)).to have_text("Heartbeat 5s ago (limit 5m 0s) · stall detection off")
    end

    it "expands a row to its start time and the metadata nothing else renders", :aggregate_failures do
      visit "/pgbus/processes"

      row(2).find("details[data-job-toggle] summary").click
      detail = find("tr[data-process-detail='2']", visible: :visible)

      expect(detail).to have_text("Started")
      expect(detail).to have_text("(42m ago)")
      expect(detail).to have_text("execution_mode: threads")
      expect(detail).to have_text("consumer_priority: 0")
      %w[supervisor_pid memory_mb started_at capacity busy max_jobs max_memory_mb max_lifetime loop_tick_at
         rates capsule].each do |key|
        expect(detail).to have_no_text("#{key}:")
      end
    end

    it "keeps the words readable in dark mode" do
      visit_dark "/pgbus/processes"

      expect(row(3)).to have_text("the supervisor's watchdog restarts it")
      expect(page).to be_accessible
    end
  end
end
