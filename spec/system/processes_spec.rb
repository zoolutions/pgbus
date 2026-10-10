# frozen_string_literal: true

require "system_helper"

RSpec.describe "Processes", type: :system do
  it "shows processes table" do
    visit "/pgbus/processes"

    expect(page).to have_css("h1", text: "Processes")
    expect(page).to have_text("worker")
    expect(page).to have_text("test-host")
    expect(page).to have_text("12345")
  end

  # Issue #503: sibling forks of one processes: N capsule are told apart.
  it "names each fork's capsule and process slot", :aggregate_failures do
    @stub_data_source.processes_list = [1, 2].map do |n|
      { id: n, kind: "worker", hostname: "worker-01.prod", pid: 40_000 + n,
        metadata: { "capsule" => "render", "process" => "#{n}/2", "queues" => ["render"], "threads" => 1 },
        last_heartbeat_at: Time.current, healthy: true, created_at: Time.current - 60 }
    end

    visit "/pgbus/processes"

    expect(page).to have_text("capsule: render", count: 2)
    expect(page).to have_text("process: 1/2")
    expect(page).to have_text("process: 2/2")
  end

  it "shows healthy status badge" do
    visit "/pgbus/processes"

    expect(page).to have_text("Healthy")
  end

  it "shows empty state" do
    @stub_data_source.processes_list = []

    visit "/pgbus/processes"

    expect(page).to have_text("No processes running")
  end
end
