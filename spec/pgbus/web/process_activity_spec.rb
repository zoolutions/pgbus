# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Web::ProcessActivity do
  let(:thresholds) do
    Pgbus::Web::ProcessState::Thresholds.new(
      now: Time.utc(2026, 10, 10, 12), alive_threshold: 300, stall_threshold: 90, dispatch_interval: 1.0,
      recurring_schedule_interval: 2.0, polling_interval: 0.1, notify_fallback_poll: 15,
      notify_wakeup: false, reap_interval: 300
    )
  end

  def activity(kind, metadata = {}, children: {})
    described_class.activity(kind: kind, metadata: metadata, children: children, thresholds: thresholds)
  end

  describe ".activity" do
    context "with a worker" do
      let(:metadata) do
        { "queues" => %w[default mailers], "threads" => 5, "capacity" => 5, "busy" => 3, "buffered" => 2,
          "rates" => { "processed" => 12.4, "failed" => 0.0 } }
      end

      it "says what it drains, how busy it is, what it holds and how fast it goes" do
        expect(activity("worker", metadata)).to eq(
          [["drains", { queues: "default, mailers" }], ["busy_threads", { busy: 3, capacity: 5 }],
           ["buffered", { number: 2 }], ["rate", { rate: 12.4 }]]
        )
      end

      it "splits a comma String of queues" do
        expect(activity("worker", { "queues" => "default,mailers" }).first).to eq(["drains", { queues: "default, mailers" }])
      end

      it "says it drains every queue for a wildcard, alone or in a list" do
        expect(activity("worker", { "queues" => ["*"] }).first).to eq(["drains_all", {}])
        expect(activity("worker", { "queues" => "default,*" }).first).to eq(["drains_all", {}])
      end

      it "counts slots for an async worker" do
        parts = activity("worker", metadata.merge("execution_mode" => "async"))
        expect(parts).to include(["busy_slots", { busy: 3, capacity: 5 }])
      end

      it "falls back to the thread count without capacity/busy (a worker from before they were reported)" do
        expect(activity("worker", { "queues" => "default", "threads" => 5 }))
          .to eq([["drains", { queues: "default" }], ["threads", { count: 5 }]])
        expect(activity("worker", { "queues" => "default", "threads" => 2, "execution_mode" => "async" }))
          .to include(["slots", { count: 2 }])
      end

      it "adds the failure rate when jobs are failing" do
        parts = activity("worker", metadata.merge("rates" => { "processed" => 12.4, "failed" => 0.2 }))
        expect(parts.last(2)).to eq([["rate", { rate: 12.4 }], ["failed_rate", { rate: 0.2 }]])
      end

      it "leaves out an empty buffer and a zero or missing rate" do
        parts = activity("worker", metadata.merge("buffered" => 0, "rates" => { "processed" => 0.0 }))
        expect(parts.map(&:first)).to eq(%w[drains busy_threads])
        expect(activity("worker", metadata.merge("rates" => nil)).map(&:first)).not_to include("rate")
      end

      it "says nothing about queues it does not report" do
        expect(activity("worker", {})).to eq([])
      end
    end

    it "says what a consumer subscribes to and how much it has handled" do
      metadata = { "topics" => ["orders.*", "users.*"], "threads" => 3, "capacity" => 3, "busy" => 1,
                   "buffered" => 0, "jobs_processed" => 1240 }

      expect(activity("consumer", metadata)).to eq(
        [["consumes", { topics: "orders.*, users.*" }], ["busy_threads", { busy: 1, capacity: 3 }],
         ["handled", { number: 1240 }]]
      )
    end

    it "says only what a consumer reported" do
      expect(activity("consumer", { "threads" => 3 })).to eq([["threads", { count: 3 }]])
    end

    it "says how often the dispatcher runs maintenance" do
      expect(activity("dispatcher")).to eq([["maintenance", { interval: 1.0 }]])
    end

    it "says how many recurring tasks the scheduler runs and how often it checks" do
      expect(activity("scheduler", { "tasks" => 14 })).to eq([["schedule", { count: 14, interval: 2.0 }]])
      expect(activity("scheduler")).to eq([["schedule", { count: 0, interval: 2.0 }]])
    end

    it "lists a supervisor's children in kind order" do
      children = { "scheduler" => 1, "worker" => 2, "streamer" => 1, "dispatcher" => 1, "consumer" => 1 }

      expect(activity("supervisor", children: children)).to eq(
        [["supervises", { children: [["worker", 2], ["consumer", 1], ["dispatcher", 1], ["scheduler", 1],
                                     ["streamer", 1]] }]]
      )
    end

    it "says a supervisor has no children here" do
      expect(activity("supervisor")).to eq([["supervises_none", {}]])
    end

    it "says what the outbox poller does and nothing for an unknown kind" do
      expect(activity("outbox_poller")).to eq([["outbox", {}]])
      expect(activity("streamer")).to eq([])
    end
  end

  describe ".recycle" do
    let(:metadata) do
      { "jobs_processed" => 1240, "max_jobs" => 10_000, "memory_mb" => 310, "max_memory_mb" => 512,
        "max_lifetime" => 3600 }
    end

    it "pairs each configured limit with how far the process has come" do
      expect(described_class.recycle(metadata, 2520)).to eq(
        [["jobs", { done: 1240, limit: 10_000 }], ["memory", { used: 310, limit: 512 }],
         ["lifetime", { uptime: 2520, limit: 3600 }]]
      )
    end

    it "has no value for a limit whose counterpart was not reported" do
      expect(described_class.recycle({ "max_memory_mb" => 512 }, nil)).to eq([["memory", { used: nil, limit: 512 }]])
    end

    it "leaves out limits that are not configured" do
      expect(described_class.recycle({ "max_jobs" => 50, "jobs_processed" => 0 }, nil))
        .to eq([["jobs", { done: 0, limit: 50 }]])
    end

    it "is nil without any limit (a process from before limits were reported, or none configured)" do
      expect(described_class.recycle({ "jobs_processed" => 5 }, 10)).to be_nil
      expect(described_class.recycle({}, nil)).to be_nil
    end
  end
end
