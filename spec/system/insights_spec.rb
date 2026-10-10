# frozen_string_literal: true

require "system_helper"

RSpec.describe "Insights", type: :system do
  describe "job stats (always visible)" do
    it "renders the page header and summary cards" do
      visit "/pgbus/insights"

      expect(page).to have_css("h1", text: "Insights")

      # Summary cards from default_insights_summary. Labels render with
      # text-transform: uppercase, so the *visible* text is "TOTAL JOBS"
      # not "Total Jobs"; have_text matches on rendered output.
      expect(page).to have_text("TOTAL JOBS")
      expect(page).to have_text("342")
      expect(page).to have_text("SUCCEEDED")
      expect(page).to have_text("335")
      expect(page).to have_text("FAILED")
      expect(page).to have_text("DEAD LETTERED")
    end

    it "shows the time range selector with 4h option" do
      visit "/pgbus/insights"

      within("[role='group'][aria-label='Time range']") do
        expect(page).to have_link("1h")
        expect(page).to have_link("4h")
        expect(page).to have_link("24h")
        expect(page).to have_link("7d")
        expect(page).to have_link("30d")
      end
    end

    it "shows an empty state for slowest job classes when there is no data" do
      visit "/pgbus/insights"

      expect(page).to have_text("Slowest Job Classes")
      expect(page).to have_text("No job stats yet")
    end

    context "with slowest job class rows" do
      before do
        @stub_data_source.insights_slowest = [
          { job_class: "SlowMailerJob", count: 42, avg_ms: 1250, max_ms: 4500 },
          { job_class: "BigReportJob", count: 11, avg_ms: 940, max_ms: 2100 }
        ]
      end

      it "renders the slowest job classes table" do
        visit "/pgbus/insights"

        expect(page).to have_text("SlowMailerJob")
        expect(page).to have_text("BigReportJob")
        expect(page).to have_text("42")
      end
    end
  end

  describe "stream stats (opt-in)" do
    context "when stream_stats_available? is false (default)" do
      it "does not render the Real-time Streams section" do
        visit "/pgbus/insights"

        expect(page).to have_no_text("Real-time Streams")
        expect(page).to have_no_text("Broadcasts")
        expect(page).to have_no_text("Top Streams by Broadcast Volume")
      end
    end

    context "when stream_stats_available? is true but no data" do
      before do
        @stub_data_source.stream_stats_available = true
      end

      it "renders the Real-time Streams section with zeroed cards" do
        visit "/pgbus/insights"

        expect(page).to have_text("Real-time Streams")
        # Labels are text-transform: uppercase in the rendered DOM.
        expect(page).to have_text("BROADCASTS")
        expect(page).to have_text("CONNECTS")
        expect(page).to have_text("DISCONNECTS")
        expect(page).to have_text("ACTIVE")
        expect(page).to have_text("AVG FANOUT")
      end

      it "shows an empty state in the top streams table" do
        visit "/pgbus/insights"

        expect(page).to have_text("Top Streams by Broadcast Volume")
        expect(page).to have_text("No stream activity recorded in the selected window")
      end
    end

    context "when stream_stats_available? is true with data" do
      before do
        @stub_data_source.stream_stats_available = true
        @stub_data_source.stream_summary = {
          broadcasts: 1_248, connects: 92, disconnects: 87,
          active_estimate: 5, avg_fanout: 7.3,
          avg_broadcast_ms: 4.1, avg_connect_ms: 12.6
        }
        @stub_data_source.top_streams_list = [
          { stream_name: "chat:lobby", count: 420, avg_fanout: 18.2, avg_ms: 3.1 },
          { stream_name: "orders:dashboard", count: 312, avg_fanout: 4.5, avg_ms: 5.8 }
        ]
      end

      it "renders the summary card values" do
        visit "/pgbus/insights"

        # pgbus_number formats 1_248 as "1.2K" (see pgbus_number helper).
        # Values under 1000 render as plain integers.
        expect(page).to have_text("1.2K")    # broadcasts
        expect(page).to have_text("92")      # connects
        expect(page).to have_text("87")      # disconnects
        expect(page).to have_text("7.3")     # avg_fanout
      end

      it "renders the top streams table rows" do
        visit "/pgbus/insights"

        expect(page).to have_text("chat:lobby")
        expect(page).to have_text("420")
        expect(page).to have_text("18.2")
        expect(page).to have_text("orders:dashboard")
        expect(page).to have_text("312")
      end
    end
  end
end
