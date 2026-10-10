# frozen_string_literal: true

require "rails_helper"

RSpec.describe "Pgbus::ProcessesController", type: :request do
  describe "GET /pgbus/processes" do
    it "renders the processes index" do
      get "/pgbus/processes"
      expect(response).to have_http_status(:ok)
    end

    it "renders the list turbo frame" do
      get "/pgbus/processes", params: { frame: "list" }
      expect(response).to have_http_status(:ok)
    end

    # Issue #493: the auto-refreshed frame carries the words, not just the badge.
    it "puts each process's reason and activity in the list frame" do
      @stub_data_source.processes_list = @stub_data_source.sample_processes(Time.now)

      get "/pgbus/processes", params: { frame: "list" }

      expect(response.body).to include("the supervisor&#39;s watchdog restarts it")
      expect(response.body).to include("Supervises 2 workers, dispatcher, scheduler, outbox poller")
      expect(response.body).not_to include("<html")
    end
  end
end
