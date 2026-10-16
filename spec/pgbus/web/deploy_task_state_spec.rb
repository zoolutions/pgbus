# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Web::DeployTaskState do
  let(:now) { Time.utc(2026, 10, 16, 12, 0, 0) }
  let(:ready) { { revision: "rev-b", reason: :ready, lagging: [], workers_live: 2, live_by_revision: {}, scheduler: true } }

  def row(**overrides)
    { enabled: true, armed_at: nil, armed_for: nil, last_rollover_run: nil, last_manual_run: nil }.merge(overrides)
  end

  def rollover(**overrides) = ready.merge(overrides)

  it "is disabled when the task is disabled" do
    result = described_class.present(row(enabled: false), ready)

    expect(result).to have_attributes(state: "disabled", reason_key: "disabled", badge_tone: :gray)
  end

  it "says a disabled task still has a rollover run pending" do
    expect(described_class.present(row(enabled: false, armed_at: now), ready).reason_key).to eq("disabled_armed")
  end

  it "is due when armed and the gate is open" do
    result = described_class.present(row(armed_at: now), ready)

    expect(result).to have_attributes(state: "due", reason_key: "due", badge_tone: :indigo)
  end

  it "waits with the gate's reason when armed and the gate is closed" do
    lagging = [{ kind: "worker", hostname: "h", pid: 1, revision: "rev-a" }] * 2

    result = described_class.present(row(armed_at: now), rollover(reason: :lagging, lagging: lagging))

    expect(result).to have_attributes(state: "waiting", reason_key: "waiting_lagging", badge_tone: :yellow,
                                      reason_args: { count: 2, revision: "rev-b" })
  end

  it "names the revision a worker is missing on" do
    result = described_class.present(row(armed_at: now), rollover(reason: :no_workers))

    expect(result).to have_attributes(reason_key: "waiting_no_workers", reason_args: { revision: "rev-b" })
  end

  it "waits on a missing revision" do
    result = described_class.present(row(armed_at: now), rollover(reason: :no_revision, revision: nil))

    expect(result.reason_key).to eq("waiting_no_revision")
  end

  it "waits on a missing scheduler even when the web process's gate is open" do
    expect(described_class.present(row(armed_at: now), rollover(scheduler: false)).reason_key)
      .to eq("waiting_no_scheduler")
  end

  it "waits on an unknown gate when the rollover could not be read" do
    expect(described_class.present(row(armed_at: now), nil).reason_key).to eq("waiting_unknown")
  end

  it "ran when not armed and a rollover run exists" do
    run = { run_at: now - 300, revision: "rev-a", version: "2" }

    result = described_class.present(row(last_rollover_run: run), ready)

    expect(result).to have_attributes(state: "ran", reason_key: "ran", badge_tone: :green,
                                      reason_args: { ago: now - 300, revision: "rev-a" })
  end

  it "never ran when not armed and without a rollover run" do
    expect(described_class.present(row, ready)).to have_attributes(state: "never", reason_key: "never")
  end

  it "appends a manual run to any state" do
    manual = { run_at: now - 60, revision: "rev-b", version: "1" }

    result = described_class.present(row(armed_at: now, last_manual_run: manual), rollover(reason: :no_workers))

    expect(result.state).to eq("waiting")
    expect(result.manual_args).to eq(ago: now - 60)
  end

  it "has no manual note without a manual run" do
    expect(described_class.present(row, ready).manual_args).to be_nil
  end
end
