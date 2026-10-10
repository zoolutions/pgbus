# frozen_string_literal: true

require "rails_helper"

# The queue page summary sentences (issue #491). rails_helper, unlike the
# plain helper spec, loads the engine's locales and ActiveSupport time zones.
RSpec.describe Pgbus::ApplicationHelper do
  let(:helper) { Class.new { include Pgbus::ApplicationHelper }.new }

  describe "#pgbus_queue_summary_line" do
    def summary_line(key, **args) = Pgbus::Web::QueueSummary::Line.new(key: key, args: args, tone: :gray)

    it "formats a past moment, a wait and counts" do
      text = helper.pgbus_queue_summary_line(summary_line("backlog", visible: 3, age: 240, parked: 2))

      expect(text).to eq("3 claimable now, oldest waiting 4m 0s · 2 parked (scheduled or retrying)")
    end

    it "formats a pause with its reason" do
      text = helper.pgbus_queue_summary_line(summary_line("paused_operator", ago: Time.now - 300, reason: "deploy"))

      expect(text).to eq("Paused 5m ago — deploy")
    end

    it "formats a future resume time" do
      text = helper.pgbus_queue_summary_line(summary_line("paused_circuit_breaker", ago: Time.now - 30, failures: 5,
                                                                                    time: Time.now + 150, trip: 2))

      expect(text).to match(/\APaused automatically 30s ago after 5 consecutive failures — resumes in 2m \(\d\d:\d\d\) \(trip #2\)\z/)
    end

    it "pluralizes the healthy workers" do
      expect(helper.pgbus_queue_summary_line(summary_line("drained_by", capsules: "default", count: 1)))
        .to eq("Drained by default (1 healthy worker)")
      expect(helper.pgbus_queue_summary_line(summary_line("drained_by", capsules: "default, bulk", count: 2)))
        .to eq("Drained by default, bulk (2 healthy workers)")
    end
  end

  describe "plural sentences" do
    def summary_line(key, **args) = Pgbus::Web::QueueSummary::Line.new(key: key, args: args, tone: :gray)

    it "says job or jobs when no worker runs" do
      expect(helper.pgbus_queue_summary_line(summary_line("no_workers", count: 1)))
        .to eq("No healthy worker running — 1 job is claimable but nothing claims it")
      expect(helper.pgbus_queue_summary_line(summary_line("no_workers", count: 3)))
        .to eq("No healthy worker running — 3 jobs are claimable but nothing claims them")
    end

    it "credits live workers when no capsule config is visible" do
      expect(helper.pgbus_queue_summary_line(summary_line("drained_live", count: 1)))
        .to eq("Drained by 1 healthy worker listening on this queue")
    end
  end

  describe "#pgbus_queue_summary_classes" do
    it "gives every tone a dark partner and falls back to gray" do
      expect(helper.pgbus_queue_summary_classes(:red)).to include("text-red-800", "dark:text-red-200")
      expect(helper.pgbus_queue_summary_classes(:unknown)).to eq(helper.pgbus_queue_summary_classes(:gray))
    end
  end
end
