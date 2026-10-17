# frozen_string_literal: true

require_relative "support"

# The needs-attention groups (issue #542): the stuck-reason CASE the sweep's
# gauge counts, grouped by (class, step, reason, error class), and the run ids
# one group's bulk action walks.
RSpec.describe "Workflow needs-attention groups", :integration do
  include WorkflowIntegration

  let(:config) { Pgbus.configuration }
  let(:threshold) { config.workflow_stall_threshold }

  before do
    stub_const("TimeoutError", Class.new(StandardError))
    stub_const("CaptureWorkflow", Class.new(Pgbus::Workflow) do
      step(:capture) { raise TimeoutError, "gateway timed out for #{arguments[:n]}" }
      step(:receipt) { nil }
    end)
    stub_const("ManualWorkflow", Class.new(Pgbus::Workflow) { step(:hold) { pause! } })
  end

  def paused_on_error(n) = run_until(CaptureWorkflow.start!(n: n))

  it "groups stuck runs by class, step, reason and error class, with counts, first/last seen and the newest message" do
    first = paused_on_error(1)
    second = paused_on_error(2)
    Pgbus::WorkflowEntry.where(workflow_id: first.workflow_id).update_all(paused_at: 2.hours.ago)
    Pgbus::WorkflowEntry.where(workflow_id: second.workflow_id).update_all(paused_at: 10.minutes.ago)
    run_until(ManualWorkflow.start!)

    groups = Pgbus::WorkflowEntry.attention_groups(threshold: threshold)

    capture = groups.find { |g| g[:workflow_class] == "CaptureWorkflow" }
    expect(capture).to include(step_name: "capture", reason: "paused_error", error_class: "TimeoutError", count: 2,
                               error_message: "gateway timed out for 2")
    expect(capture[:first_seen_at]).to be_within(5).of(2.hours.ago)
    expect(capture[:last_seen_at]).to be_within(5).of(10.minutes.ago)
    expect(groups.find { |g| g[:workflow_class] == "ManualWorkflow" })
      .to include(step_name: "hold", reason: "paused_manual", error_class: nil, count: 1)
    expect(groups.first[:workflow_class]).to eq("CaptureWorkflow") # the biggest group first
  end

  it "counts the same runs as stuck_reason_counts" do
    paused_on_error(1)
    run_until(ManualWorkflow.start!)

    groups = Pgbus::WorkflowEntry.attention_groups(threshold: threshold)
    counts = Pgbus::WorkflowEntry.stuck_reason_counts(threshold: threshold)

    totals = groups.group_by { |g| [g[:workflow_class], g[:reason]] }.transform_values { |rows| rows.sum { |g| g[:count] } }
    expect(totals).to eq(counts)
  end

  it "bounds the number of groups" do
    paused_on_error(1)
    run_until(ManualWorkflow.start!)

    expect(Pgbus::WorkflowEntry.attention_groups(threshold: threshold, limit: 1).size).to eq(1)
  end

  it "returns one group's run ids, oldest stuck first, up to the limit" do
    first = paused_on_error(1)
    second = paused_on_error(2)
    Pgbus::WorkflowEntry.where(workflow_id: first.workflow_id).update_all(paused_at: 2.hours.ago)
    run_until(ManualWorkflow.start!)
    group = { workflow_class: "CaptureWorkflow", step_name: "capture", reason: "paused_error", error_class: "TimeoutError" }

    expect(Pgbus::WorkflowEntry.attention_ids(group, threshold: threshold)).to eq([first.workflow_id, second.workflow_id])
    expect(Pgbus::WorkflowEntry.attention_ids(group, threshold: threshold, limit: 1)).to eq([first.workflow_id])
  end

  it "matches a group without an error class" do
    manual = run_until(ManualWorkflow.start!)
    group = { workflow_class: "ManualWorkflow", step_name: "hold", reason: "paused_manual", error_class: nil }

    expect(Pgbus::WorkflowEntry.attention_ids(group, threshold: threshold)).to eq([manual.workflow_id])
  end

  it "includes waiting_overdue runs only with a waiting_threshold" do
    stub_const("ParkedWorkflow", Class.new(Pgbus::Workflow) { step(:forever, wait_for: :never) { nil } })
    wf = run_until(ParkedWorkflow.start!, statuses: %w[waiting])
    steps_of(wf).last.update_columns(waiting_since: 8.days.ago)

    expect(Pgbus::WorkflowEntry.attention_groups(threshold: threshold)).to be_empty
    groups = Pgbus::WorkflowEntry.attention_groups(threshold: threshold,
                                                   waiting_threshold: config.workflow_waiting_visibility_threshold)
    expect(groups.sole).to include(workflow_class: "ParkedWorkflow", step_name: "forever", reason: "waiting_overdue", count: 1)
    expect(groups.sole[:first_seen_at]).to be_within(5).of(8.days.ago)
  end
end
