# frozen_string_literal: true

require "rails_helper"

RSpec.describe "Pgbus::DashboardController", type: :request do
  describe "GET /pgbus" do
    it "renders the dashboard" do
      get "/pgbus"
      expect(response).to have_http_status(:ok)
    end

    %w[stats queues processes failures health].each do |frame|
      it "renders the #{frame} turbo frame without a layout" do
        get "/pgbus", params: { frame: frame }
        expect(response).to have_http_status(:ok)
      end
    end

    # Issue #493: the processes frame says why each process has its status.
    it "puts each process's reason in the processes frame" do
      @stub_data_source.processes_list = @stub_data_source.sample_processes(Time.now)

      get "/pgbus", params: { frame: "processes" }

      expect(response.body).to include("no supervisor to restart it")
    end
  end
end
