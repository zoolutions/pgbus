# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Web::ProcessState do
  let(:now) { Time.utc(2026, 10, 10, 12, 0, 0) }
  let(:thresholds) do
    described_class::Thresholds.new(
      now: now, alive_threshold: 300, stall_threshold: 90, dispatch_interval: 1.0,
      recurring_schedule_interval: 1.0, polling_interval: 0.1, notify_fallback_poll: 15,
      notify_wakeup: false, reap_interval: 300
    )
  end

  def tick(seconds_ago) = { "loop_tick_at" => now.to_f - seconds_ago }

  def derive(kind, metadata, heartbeat_at: now - 10, with: {})
    described_class.derive(kind: kind, metadata: metadata, heartbeat_at: heartbeat_at,
                           thresholds: thresholds.with(**with))
  end

  def row(kind, pid:, hostname: "web-01", status: :healthy, metadata: {}, heartbeat_at: now - 10)
    { id: pid, kind: kind, hostname: hostname, pid: pid, metadata: metadata, last_heartbeat_at: heartbeat_at,
      status: status, healthy: status == :healthy }
  end

  def context_for(rows, with: {}) = described_class.context_for(rows, thresholds.with(**with))

  describe ".derive" do
    it "is :stale past the alive threshold, whatever the beacon says" do
      expect(derive("worker", tick(1), heartbeat_at: now - 301)).to eq(:stale)
    end

    it "is not stale without a heartbeat time" do
      expect(derive("worker", {}, heartbeat_at: nil)).to eq(:healthy)
    end

    it "accepts an ISO String heartbeat" do
      expect(derive("worker", {}, heartbeat_at: (now - 400).iso8601)).to eq(:stale)
    end

    {
      "worker" => [90, 91], "dispatcher" => [91, 92], "scheduler" => [91, 92]
    }.each do |kind, (healthy_age, stalled_age)|
      it "is :healthy for a #{kind} whose beacon is #{healthy_age}s old and :stalled at #{stalled_age}s" do
        expect(derive(kind, tick(healthy_age - 0.5))).to eq(:healthy)
        expect(derive(kind, tick(stalled_age))).to eq(:stalled)
      end
    end

    it "widens the dispatcher's window by dispatch_interval" do
      expect(derive("dispatcher", tick(92), with: { dispatch_interval: 5.0 })).to eq(:healthy)
    end

    it "widens the scheduler's window by recurring_schedule_interval" do
      expect(derive("scheduler", tick(92), with: { recurring_schedule_interval: 5.0 })).to eq(:healthy)
    end

    it "widens a notify-driven consumer's window by the fallback poll ceiling" do
      expect(derive("consumer", tick(100), with: { notify_wakeup: true })).to eq(:healthy)
      expect(derive("consumer", tick(106), with: { notify_wakeup: true })).to eq(:stalled)
    end

    it "widens a polling consumer's window by polling_interval only" do
      expect(derive("consumer", tick(92), with: { polling_interval: 5.0 })).to eq(:healthy)
      expect(derive("consumer", tick(100))).to eq(:stalled)
    end

    it "is never :stalled when stall detection is off" do
      %w[worker consumer dispatcher scheduler].each do |kind|
        expect(derive(kind, tick(999), with: { stall_threshold: nil })).to eq(:healthy)
      end
    end

    it "is never :stalled for kinds without a loop beacon" do
      %w[supervisor outbox_poller streamer].each { |kind| expect(derive(kind, tick(999))).to eq(:healthy) }
    end

    it "is :healthy without a beacon (a process from before the beacon existed) or without metadata" do
      expect(derive("dispatcher", { "pid" => 1 })).to eq(:healthy)
      expect(derive("worker", nil)).to eq(:healthy)
    end
  end

  describe ".stall_limit_for" do
    it "is nil for non-positive thresholds and beacon-less kinds" do
      expect(described_class.stall_limit_for("worker", thresholds.with(stall_threshold: 0))).to be_nil
      expect(described_class.stall_limit_for("supervisor", thresholds)).to be_nil
    end

    it "adds the larger of polling_interval and the fallback poll for a notify consumer" do
      limit = described_class.stall_limit_for("consumer", thresholds.with(notify_wakeup: true, polling_interval: 20))
      expect(limit).to eq(110)
    end
  end

  describe ".context_for" do
    let(:rows) do
      [
        row("supervisor", pid: 100),
        row("worker", pid: 101, metadata: { "supervisor_pid" => 100 }),
        row("worker", pid: 102, metadata: { "supervisor_pid" => 100 }),
        row("dispatcher", pid: 103, metadata: { "supervisor_pid" => 100 }, status: :stale),
        row("worker", pid: 201, hostname: "web-02", metadata: { "supervisor_pid" => 100 }),
        row("supervisor", pid: 300, hostname: "web-03", status: :stale)
      ]
    end

    it "indexes supervisors by host and pid with their status" do
      expect(context_for(rows).supervisors).to eq(["web-01", 100] => :healthy, ["web-03", 300] => :stale)
    end

    it "counts children per supervisor on the same host only" do
      expect(context_for(rows).children).to eq(["web-01", 100] => { "worker" => 2, "dispatcher" => 1 },
                                               ["web-02", 100] => { "worker" => 1 })
    end

    it "knows whether a healthy dispatcher exists" do
      expect(context_for(rows).live_dispatcher).to be(false)
      expect(context_for(rows + [row("dispatcher", pid: 400)]).live_dispatcher).to be(true)
    end

    it "treats a row without :status by its :healthy flag" do
      legacy = { kind: "dispatcher", hostname: "h", pid: 1, metadata: nil, healthy: true }
      expect(context_for([legacy]).live_dispatcher).to be(true)
    end
  end

  describe ".order" do
    it "puts each supervisor before its children, then standalone rows, host by host" do
      rows = [
        row("worker", pid: 900, hostname: "z-host"),
        row("worker", pid: 101, metadata: { "supervisor_pid" => 100 }),
        row("worker", pid: 500, metadata: { "supervisor_pid" => 1 }),
        row("supervisor", pid: 100),
        row("dispatcher", pid: 102, metadata: { "supervisor_pid" => 100 })
      ]

      ordered = described_class.order(rows, context_for(rows))

      expect(ordered.map { |r| r[:pid] }).to eq([100, 101, 102, 500, 900])
    end
  end

  describe ".present" do
    def present(the_row, rows: [the_row], with: {})
      described_class.present(the_row, context_for(rows, with: with))
    end

    it "says a healthy looping process ticked and against which limit" do
      result = present(row("worker", pid: 1, metadata: tick(5)))

      expect(result.status).to eq("healthy")
      expect(result.badge_tone).to eq(:green)
      expect(result.reason_key).to eq("healthy_loop")
      expect(result.reason_args).to eq(ago: Time.at(now.to_f - 5), limit: 90)
      expect(result.next_key).to be_nil
    end

    it "falls back to the heartbeat for a kind without a loop beacon" do
      result = present(row("supervisor", pid: 1))

      expect(result.reason_key).to eq("healthy_heartbeat")
      expect(result.reason_args).to eq(ago: now - 10, limit: 300)
    end

    it "falls back to the heartbeat for a looping kind that has not reported a beacon" do
      expect(present(row("worker", pid: 1)).reason_key).to eq("healthy_heartbeat")
    end

    it "says stall detection is off when stall_threshold is nil" do
      result = present(row("worker", pid: 1, metadata: tick(5)), with: { stall_threshold: nil })

      expect(result.reason_key).to eq("healthy_detection_off")
      expect(result.reason_args).to eq(ago: now - 10, limit: 300)
    end

    it "takes the row's status as given and falls back to :healthy for rows without one" do
      legacy = row("worker", pid: 1).except(:status)
      expect(present(legacy).status).to eq("healthy")
      expect(present(legacy.merge(healthy: false)).status).to eq("stale")
    end

    describe "a stale process" do
      let(:stale) { row("outbox_poller", pid: 105, status: :stale, heartbeat_at: now - 420) }

      it "says how long the heartbeat has been silent against the alive threshold" do
        result = present(stale)

        expect(result.badge_tone).to eq(:red)
        expect(result.reason_key).to eq("stale")
        expect(result.reason_args).to eq(age: 420, limit: 300)
      end

      it "is removed at the next sweep when a healthy dispatcher exists" do
        result = present(stale, rows: [stale, row("dispatcher", pid: 103)])

        expect(result.next_key).to eq("stale_reaped")
        expect(result.next_args).to eq(within: 300)
      end

      it "is never removed when no dispatcher is healthy, including when it is the only dispatcher" do
        dispatcher = row("dispatcher", pid: 103, status: :stale, heartbeat_at: now - 600)

        expect(present(stale, rows: [stale, dispatcher]).next_key).to eq("stale_unreaped")
        expect(present(dispatcher).next_key).to eq("stale_unreaped")
      end

      it "has no age without a heartbeat time" do
        expect(present(stale.merge(last_heartbeat_at: nil)).reason_args).to eq(age: nil, limit: 300)
      end
    end

    describe "a stalled process" do
      let(:supervisor) { row("supervisor", pid: 100) }
      let(:stalled) { row("worker", pid: 102, status: :stalled, metadata: tick(120).merge("supervisor_pid" => 100)) }

      it "says when the loop last ticked against the kind's limit" do
        result = present(stalled, rows: [supervisor, stalled])

        expect(result.badge_tone).to eq(:yellow)
        expect(result.reason_key).to eq("stalled")
        expect(result.reason_args).to eq(ago: Time.at(now.to_f - 120), limit: 90)
      end

      it "is restarted by a healthy supervisor's watchdog" do
        expect(present(stalled, rows: [supervisor, stalled]).next_key).to eq("stalled_supervised")
      end

      it "has no one to restart it under a stale supervisor or standalone" do
        stale_supervisor = supervisor.merge(status: :stale)
        expect(present(stalled, rows: [stale_supervisor, stalled]).next_key).to eq("stalled_standalone")
        expect(present(stalled).next_key).to eq("stalled_standalone")
      end

      it "names what stops advancing for a dispatcher or scheduler" do
        expect(present(row("dispatcher", pid: 1, status: :stalled, metadata: tick(120))).next_key)
          .to eq("stalled_maintenance")
        expect(present(row("scheduler", pid: 1, status: :stalled, metadata: tick(120))).next_key)
          .to eq("stalled_schedule")
      end

      it "has no next step for a kind that cannot stall" do
        expect(present(row("streamer", pid: 1, status: :stalled)).next_key).to be_nil
      end
    end

    describe "supervisor grouping" do
      let(:supervisor) { row("supervisor", pid: 100, status: :stale) }

      it "names the supervisor when its row exists on the same host, whatever its status" do
        child = row("worker", pid: 101, metadata: { "supervisor_pid" => 100 })
        expect(present(child, rows: [supervisor, child]).supervisor_pid).to eq(100)
      end

      it "does not group by a pid that belongs to another host or to no supervisor" do
        elsewhere = row("worker", pid: 101, hostname: "web-02", metadata: { "supervisor_pid" => 100 })
        shell = row("worker", pid: 102, metadata: { "supervisor_pid" => 1 })

        expect(present(elsewhere, rows: [supervisor, elsewhere]).supervisor_pid).to be_nil
        expect(present(shell, rows: [supervisor, shell]).supervisor_pid).to be_nil
      end
    end

    describe "uptime" do
      it "comes from started_at" do
        result = present(row("worker", pid: 1, metadata: { "started_at" => now.to_f - 2520 }))

        expect(result.started_at).to eq(Time.at(now.to_f - 2520))
        expect(result.uptime_seconds).to eq(2520)
      end

      it "is nil without started_at" do
        result = present(row("worker", pid: 1))
        expect([result.started_at, result.uptime_seconds]).to eq([nil, nil])
      end
    end

    it "parses String metadata and ignores unparseable metadata" do
      expect(present(row("worker", pid: 1, metadata: { "started_at" => now.to_f }.to_json)).uptime_seconds).to eq(0)
      expect(present(row("worker", pid: 1, metadata: "{nope")).uptime_seconds).to be_nil
    end

    it "carries the activity and recycle parts" do
      result = present(row("dispatcher", pid: 1))

      expect(result.activity).to eq([["maintenance", { interval: 1.0 }]])
      expect(result.recycle).to be_nil
    end
  end
end
