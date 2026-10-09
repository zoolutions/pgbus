# frozen_string_literal: true

require "system_helper"

# Covers security + pagination edge cases across the dashboard that
# previously had no system-level coverage. These are the scenarios
# the codebase audit flagged as most likely to go wrong: user-supplied
# strings rendered into admin views, empty lists, and pages requested
# beyond the end of the available data.
RSpec.describe "Dashboard edge cases", type: :system do
  describe "XSS safety" do
    # ERB's <%= %> auto-escapes by default, so every string from
    # user-supplied data (job payloads, error messages, queue names)
    # should render as literal text, not as active HTML. This test
    # confirms that by looking for the escaped bracket in the page
    # source and the absence of an actual <script> element.
    context "with a failed job containing a <script> payload in the error message" do
      before do
        @stub_data_source.failed_events_list = [
          {
            "id" => 1,
            "queue_name" => "pgbus_default",
            "msg_id" => 42,
            "job_class" => "ExploitJob",
            "error_class" => "RuntimeError",
            "error_message" => "<script>window.pwned=true</script>",
            "failed_at" => Time.now.utc.iso8601,
            "payload" => '{"job_class":"ExploitJob","arguments":["<img src=x onerror=alert(1)>"]}',
            "backtrace" => "line1\nline2",
            "retry_count" => 0
          }
        ]
        @stub_data_source.stats[:failed_count] = 1
      end

      it "escapes the error message on the recent failures card" do
        visit "/pgbus"

        # The literal angle bracket should appear in the rendered page
        # body (as text, escaped to &lt; in the DOM source). If the
        # template were NOT escaping, Playwright would see <script>
        # parsed as a tag and the text would not be findable.
        expect(page).to have_text("<script>")
        expect(page.evaluate_script("window.pwned")).to be_nil
      end

      it "escapes the error message on the failed job show page" do
        visit "/pgbus/jobs/1"

        expect(page).to have_text("<script>")
        expect(page).to have_text("<img src=x onerror=alert(1)>")
        # Sanity check: no alert / no attribute-based XSS fired.
        expect(page.evaluate_script("window.pwned")).to be_nil
      end
    end

    context "with a queue name containing angle brackets" do
      before do
        @stub_data_source.queues = [
          { name: "<script>alert(1)</script>", queue_length: 0, queue_visible_length: 0,
            oldest_msg_age_sec: nil, newest_msg_age_sec: nil, total_messages: 0 }
        ]
      end

      it "renders the queue name as literal text on the dashboard queues table" do
        visit "/pgbus"

        expect(page).to have_text("<script>alert(1)</script>")
        expect(page.evaluate_script("window.pwned")).to be_nil
      end
    end
  end

  describe "pagination edges" do
    context "when the failed jobs list is empty" do
      before do
        @stub_data_source.failed_events_list = []
        @stub_data_source.stats[:failed_count] = 0
      end

      it "renders the empty state without crashing" do
        visit "/pgbus/jobs"

        expect(page).to have_css("h1", text: "Jobs")
        # Empty state of the unified job list
        expect(page).to have_text("No jobs")
      end
    end

    context "when requesting a page past the end of the data" do
      before do
        @stub_data_source.failed_events_list = [
          { "id" => 1, "queue_name" => "pgbus_default", "msg_id" => 1,
            "error_class" => "RuntimeError", "error_message" => "boom",
            "failed_at" => Time.now.utc.iso8601, "retry_count" => 0 }
        ]
        @stub_data_source.stats[:failed_count] = 1
      end

      it "does not 500 on an off-the-end page number" do
        # page=999 is well past the end; the controller should clamp
        # or render empty without raising. The have_css assertion
        # doubles as a health check — if the server 500'd, the H1
        # would never render and the test would fail, regardless of
        # whether the Capybara driver surfaces the status code.
        visit "/pgbus/jobs?page=999"
        expect(page).to have_css("h1", text: "Jobs")
      end

      it "does not 500 on a negative page number" do
        visit "/pgbus/jobs?page=-1"
        expect(page).to have_css("h1", text: "Jobs")
      end

      it "does not 500 on a non-numeric page parameter" do
        visit "/pgbus/jobs?page=abc"
        expect(page).to have_css("h1", text: "Jobs")
      end
    end

    context "when requesting a non-existent failed job by id" do
      before { @stub_data_source.failed_events_list = [] }

      it "renders the not-found template without raising" do
        visit "/pgbus/jobs/99999"

        expect(page).to have_text("Job not found")
      end
    end
  end

  describe "failed job with a null/missing backtrace" do
    before do
      @stub_data_source.failed_events_list = [
        {
          "id" => 2,
          "queue_name" => "pgbus_default",
          "msg_id" => 50,
          "job_class" => "NoBacktraceJob",
          "error_class" => "ArgumentError",
          "error_message" => "invalid",
          "failed_at" => Time.now.utc.iso8601,
          "payload" => '{"job_class":"NoBacktraceJob","arguments":[]}',
          "backtrace" => nil,
          "retry_count" => 0
        }
      ]
    end

    it "renders the show page without crashing on missing backtrace" do
      visit "/pgbus/jobs/2"

      expect(page).to have_text("ArgumentError")
      expect(page).to have_text("invalid")
      # The backtrace section should simply be absent, not explode.
      expect(page).to have_no_text("undefined method")
    end
  end
end
