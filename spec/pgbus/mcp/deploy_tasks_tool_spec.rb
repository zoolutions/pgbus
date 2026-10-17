# frozen_string_literal: true

require "json"
require_relative "spec_helper"

RSpec.describe Pgbus::MCP::Tools::DeployTasksTool do
  let(:data_source) { instance_double(Pgbus::Web::DataSource) }
  let(:context) { { data_source: data_source, allow_payloads: false } }
  let(:count_class) { Pgbus::Web::DataSource::ListCounts::Count }
  let(:total) { 1 }
  let(:capped) { false }
  let(:ran_at) { Time.utc(2026, 10, 10, 12, 0, 0) }
  let(:rollover) do
    { revision: "abc123", reason: :ready, lagging: [], workers_live: 2,
      live_by_revision: { "abc123" => 2 }, scheduler: true }
  end
  let(:tasks) { [task("backfill")] }

  def task(key, **overrides)
    { id: 1, key: key, class_name: "BackfillJob", command: nil, version: "1", every_deploy: false,
      queue_name: "default", priority: nil, description: nil, enabled: true, static: true,
      armed_at: nil, armed_for: nil, last_rollover_run: nil, last_manual_run: nil,
      created_at: ran_at, updated_at: ran_at }.merge(overrides)
  end

  before do
    allow(data_source).to receive(:list_count).with(:deploy_tasks)
                                              .and_return(count_class.new(total: total, capped: capped))
    allow(data_source).to receive_messages(deploy_tasks: tasks, deploy_rollover: rollover)
  end

  def result(**args)
    JSON.parse(described_class.call(server_context: context, **args).content.first[:text])
  end

  it "is named pgbus_deploy_tasks and is read-only" do
    expect(described_class.tool_name).to eq("pgbus_deploy_tasks")
    expect(described_class.annotations_value.to_h).to include(readOnlyHint: true)
  end

  it "has page and per_page integer properties with a minimum of 1" do
    props = described_class.input_schema.to_h[:properties]
    expect(props[:page]).to include(type: "integer", minimum: 1)
    expect(props[:per_page]).to include(type: "integer", minimum: 1)
  end

  context "with no arguments" do
    let(:tasks) { [task("a"), task("b")] }

    it "returns every task, unpaginated, without counting" do
      out = result

      expect(data_source).to have_received(:deploy_tasks).with(no_args)
      expect(data_source).not_to have_received(:list_count)
      expect(out["tasks"].size).to eq(2)
      expect(out).to include("page" => 1, "per_page" => nil, "total" => 2, "has_more" => false)
    end
  end

  it "passes page and per_page through" do
    result(page: 3, per_page: 20)

    expect(data_source).to have_received(:deploy_tasks).with(page: 3, per_page: 20)
  end

  it "defaults per_page to 100 when only page is given" do
    expect(result(page: 2)).to include("page" => 2, "per_page" => 100)
    expect(data_source).to have_received(:deploy_tasks).with(page: 2, per_page: 100)
  end

  it "clamps per_page to 100 and page to 1..1000" do
    expect(result(page: 99_999, per_page: 5_000)).to include("page" => 1_000, "per_page" => 100)
    expect(data_source).to have_received(:deploy_tasks).with(page: 1_000, per_page: 100)
  end

  it "clamps zero and negative values up to 1" do
    expect(result(page: 0, per_page: -5)).to include("page" => 1, "per_page" => 1)
  end

  context "when more rows exist beyond the page" do
    let(:total) { 250 }

    it "reports has_more and the total" do
      expect(result(page: 1, per_page: 100)).to include("total" => 250, "has_more" => true)
    end

    it "reports no more on the last page" do
      expect(result(page: 3, per_page: 100)).to include("has_more" => false)
    end
  end

  context "when the count is capped" do
    let(:total) { 10_000 }
    let(:capped) { true }

    it "stops reporting has_more at the page ceiling" do
      expect(result(page: described_class::MAX_PAGE, per_page: 1)).to include("has_more" => false)
    end

    it "stops reporting has_more on a short page past the cap" do
      expect(result(page: 102, per_page: 100)).to include("has_more" => false)
    end
  end

  describe "rollover" do
    let(:rollover) do
      { revision: "abc123", reason: :lagging,
        lagging: [{ kind: "worker", hostname: "web-1", pid: 42, revision: "old" }],
        workers_live: 3, live_by_revision: { "abc123" => 2, "old" => 1 }, scheduler: true }
    end

    it "is included with symbols as strings" do
      expect(result["rollover"]).to eq(
        "revision" => "abc123", "reason" => "lagging",
        "lagging" => [{ "kind" => "worker", "hostname" => "web-1", "pid" => 42, "revision" => "old" }],
        "workers_live" => 3, "live_by_revision" => { "abc123" => 2, "old" => 1 }, "scheduler" => true
      )
    end

    context "when it cannot be read" do
      let(:rollover) { nil }
      let(:tasks) { [task("a", armed_at: ran_at)] }

      it "is null and the armed task waits for an unknown reason" do
        out = result

        expect(out["rollover"]).to be_nil
        expect(out["tasks"].first).to include("state" => "waiting", "reason" => "waiting_unknown")
      end
    end
  end

  describe "task state" do
    let(:run) { { run_at: ran_at, revision: "abc123", version: "1", triggered_by: "rollover" } }
    let(:tasks) do
      [task("off", enabled: false), task("armed", armed_at: ran_at),
       task("done", last_rollover_run: run), task("fresh")]
    end

    it "carries state, reason and reason_args per task" do
      by_key = result["tasks"].to_h { |t| [t["key"], t] }

      expect(by_key["off"]).to include("state" => "disabled", "reason" => "disabled", "reason_args" => {})
      expect(by_key["armed"]).to include("state" => "due", "reason" => "due")
      expect(by_key["done"]).to include("state" => "ran", "reason" => "ran")
      expect(by_key["done"]["reason_args"]).to eq("ago" => "2026-10-10T12:00:00Z", "revision" => "abc123")
      expect(by_key["fresh"]).to include("state" => "never", "reason" => "never")
    end

    context "when the rollover is lagging" do
      let(:rollover) do
        { revision: "new", reason: :lagging, lagging: [{ pid: 1 }], workers_live: 1,
          live_by_revision: {}, scheduler: true }
      end

      it "reports waiting with the count and revision" do
        expect(result["tasks"][1]).to include("state" => "waiting", "reason" => "waiting_lagging",
                                              "reason_args" => { "count" => 1, "revision" => "new" })
      end
    end
  end

  describe "serialization" do
    let(:run) { { run_at: ran_at, revision: "abc123", version: "1", triggered_by: "manual" } }
    let(:tasks) { [task("a", armed_at: ran_at, last_manual_run: run)] }

    it "writes times as ISO 8601" do
      row = result["tasks"].first

      expect(row).to include("armed_at" => "2026-10-10T12:00:00Z", "created_at" => "2026-10-10T12:00:00Z")
      expect(row["last_manual_run"]).to include("run_at" => "2026-10-10T12:00:00Z", "triggered_by" => "manual")
    end

    it "never carries job arguments" do
      leaky = [task("a", arguments: [{ "token" => "secret" }])]
      allow(data_source).to receive(:deploy_tasks).and_return(leaky)

      out = JSON.generate(result)

      expect(result["tasks"].first).not_to have_key("arguments")
      expect(out).not_to include("secret")
    end
  end
end
