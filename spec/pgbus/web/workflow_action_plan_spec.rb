# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Web::WorkflowActionPlan do
  let(:klass) do
    Class.new(Pgbus::Workflow) do
      step(:capture) { nil }
      step(:ship, wait_for: :shipped) { nil }
      step(:custom, wait_for: Object.new.tap { |o| o.define_singleton_method(:matches?) { |_| true } }) { nil }
      step(:receipt) { nil }
    end
  end
  # Plain doubles: the models' attribute readers come from the schema, which
  # a unit spec has no database to read.
  let(:paused) { double("Pgbus::WorkflowEntry", status: "paused") }

  def step(status, name = "capture", batch_id: nil)
    double("Pgbus::WorkflowStep", status: status, step_name: name, batch_id: batch_id, has_attribute?: true)
  end

  describe ".resume_plan" do
    {
      "failed" => [:retry, "capture"], "scheduled" => [:resend, "capture"], "waiting" => [:rewait, "capture"],
      "in_progress" => [:carry_on, "capture"], "completed" => [:continue, "ship"], "skipped" => [:continue, "ship"],
      "cancelled" => [nil, nil]
    }.each do |status, plan|
      it "plans #{plan.first.inspect} for a run paused on a #{status} step" do
        expect(described_class.resume_plan(paused, step(status), klass)).to eq(plan)
      end
    end

    it "finishes a run paused after its last step" do
      expect(described_class.resume_plan(paused, step("completed", "receipt"), klass)).to eq([:finish, nil])
    end

    it "plans nothing for a run that is not paused, has no steps, or whose class is gone" do
      expect(described_class.resume_plan(double("Pgbus::WorkflowEntry", status: "ready"), step("failed"), klass))
        .to eq([nil, nil])
      expect(described_class.resume_plan(paused, nil, klass)).to eq([nil, nil])
      expect(described_class.resume_plan(paused, step("completed"), nil)).to eq([nil, nil])
    end
  end

  describe ".signal_names" do
    it "names the signal a waiting step's matcher takes" do
      expect(described_class.signal_names(step("waiting", "ship"), klass)).to eq(["shipped"])
    end

    it "is empty for a custom matcher, a fan-out batch, a step that is not waiting or an unknown class" do
      expect(described_class.signal_names(step("waiting", "custom"), klass)).to eq([])
      expect(described_class.signal_names(step("waiting", "ship", batch_id: "b-1"), klass)).to eq([])
      expect(described_class.signal_names(step("scheduled", "ship"), klass)).to eq([])
      expect(described_class.signal_names(step("waiting", "ship"), nil)).to eq([])
    end
  end

  describe ".group_status" do
    it "maps each stuck reason to the status its runs are in" do
      expect(%w[stalled paused_error paused_manual overdue waiting_overdue].map { |r| described_class.group_status(r) })
        .to eq(%w[paused paused paused ready waiting])
    end
  end
end
