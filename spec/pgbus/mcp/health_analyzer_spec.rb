# frozen_string_literal: true

require_relative "spec_helper"

RSpec.describe Pgbus::MCP::HealthAnalyzer do
  subject(:analyzer) { described_class.new(data_source) }

  let(:data_source) { instance_double(Pgbus::Web::DataSource) }

  # drained: nil (default) means "a wildcard capsule drains every queue", which
  # keeps the legacy specs — written before the drain-intersection existed —
  # asserting on the full backlog. Pass an explicit array to restrict the
  # drained set (issue #367 defect 2). unregistered_streams stubs the #366
  # dormant-stream hint count.
  def stub_data_source(queues: [], processes: [], health: {}, stream_queues: [], drained: nil, unregistered_streams: 0)
    allow(data_source).to receive_messages(
      queues_with_metrics: queues, processes: processes,
      queue_health_stats: health, stream_queue_names: Set.new(stream_queues),
      drained_queue_names: drained.nil? ? nil : Set.new(drained),
      unregistered_stream_queue_count: unregistered_streams
    )
  end

  # visible_unread_length is the precise wedge signal: visible rows with
  # read_ct=0 (issue #367 review). When not given, it defaults from max_read_ct
  # so the legacy specs keep their meaning — a fully-unclaimed backlog
  # (max_read_ct 0 or absent) reports every visible row as unread; a claimed
  # one (max_read_ct > 0) reports none.
  #
  # unread_age sets oldest_unread_claimable_age_sec (issue #589) only when
  # given, so the legacy specs keep a hash without the key — the conservative
  # "age unknown, count it" path.
  def queue(name:, length: 0, visible: 0, paused: false, max_read_ct: nil, visible_unread: nil, unread_age: nil)
    h = { name: name, queue_length: length, queue_visible_length: visible, paused: paused }
    h[:max_read_ct] = max_read_ct unless max_read_ct.nil?
    h[:visible_unread_length] = visible_unread || (max_read_ct.to_i.zero? ? visible : 0)
    h[:oldest_unread_claimable_age_sec] = unread_age unless unread_age.nil?
    h
  end

  def worker(status:, kind: "worker")
    { kind: kind, status: status }
  end

  describe "#verdict OK path" do
    it "returns OK when there is no backlog and workers are healthy" do
      stub_data_source(
        queues: [queue(name: "pgbus_default", length: 0, visible: 0)],
        processes: [worker(status: :healthy)]
      )

      expect(analyzer.verdict[:status]).to eq("OK")
    end

    it "returns OK when a queue drains normally with a working worker" do
      stub_data_source(
        queues: [queue(name: "pgbus_default", length: 5, visible: 5, max_read_ct: 2)],
        processes: [worker(status: :healthy)]
      )

      verdict = analyzer.verdict
      expect(verdict[:status]).to eq("OK")
      expect(verdict[:reasons]).to be_empty
    end

    it "includes a machine-readable summary and ISO8601 checked_at" do
      stub_data_source(processes: [worker(status: :healthy)])

      verdict = analyzer.verdict
      expect(verdict[:summary]).to include(:queues, :workers, :total_visible)
      expect(verdict[:checked_at]).to match(/\A\d{4}-\d{2}-\d{2}T/)
    end
  end

  describe "#verdict STALLED path (silent wedge)" do
    it "is STALLED when a worker is stalled while a queue has visible backlog" do
      stub_data_source(
        queues: [queue(name: "pgbus_default", length: 10, visible: 10)],
        processes: [worker(status: :stalled)]
      )

      verdict = analyzer.verdict
      expect(verdict[:status]).to eq("STALLED")
      expect(verdict[:reasons].first).to include("stalled")
    end

    it "is STALLED when backlog is unread (read_ct 0) and workers are alive" do
      stub_data_source(
        queues: [queue(name: "pgbus_default", length: 10, visible: 10, max_read_ct: 0)],
        processes: [worker(status: :healthy)]
      )

      verdict = analyzer.verdict
      expect(verdict[:status]).to eq("STALLED")
      expect(verdict[:reasons].first).to include("never claimed")
    end

    it "treats backlog without read_ct metric as unread (conservative wedge default)" do
      stub_data_source(
        queues: [queue(name: "pgbus_default", length: 3, visible: 3)],
        processes: [worker(status: :healthy)]
      )

      expect(analyzer.verdict[:status]).to eq("STALLED")
    end

    it "does not flag STALLED when there are no workers at all" do
      stub_data_source(
        queues: [queue(name: "pgbus_default", length: 3, visible: 3)],
        processes: []
      )

      expect(analyzer.verdict[:status]).not_to eq("STALLED")
    end

    it "ignores DLQ backlog for the wedge signal" do
      stub_data_source(
        queues: [queue(name: "pgbus_default_dlq", length: 5, visible: 5)],
        processes: [worker(status: :healthy)]
      )

      expect(analyzer.verdict[:status]).not_to eq("STALLED")
    end

    it "does not flag STALLED when backlog has been read (read_ct > 0) and no worker is stalled" do
      stub_data_source(
        queues: [queue(name: "pgbus_default", length: 5, visible: 5, max_read_ct: 3)],
        processes: [worker(status: :healthy)]
      )

      expect(analyzer.verdict[:status]).to eq("OK")
    end
  end

  describe "#verdict DEGRADED path" do
    it "is DEGRADED when a process is stale" do
      stub_data_source(
        queues: [],
        processes: [worker(status: :stale)]
      )

      verdict = analyzer.verdict
      expect(verdict[:status]).to eq("DEGRADED")
      expect(verdict[:reasons].first).to include("stale")
    end

    it "is DEGRADED when a paused queue holds a backlog" do
      stub_data_source(
        queues: [queue(name: "pgbus_default", length: 5, visible: 5, paused: true, max_read_ct: 3)],
        processes: [worker(status: :healthy)]
      )

      verdict = analyzer.verdict
      expect(verdict[:status]).to eq("DEGRADED")
      expect(verdict[:reasons].join).to include("paused")
    end

    it "is DEGRADED when a dead-letter queue holds messages" do
      stub_data_source(
        queues: [queue(name: "pgbus_default_dlq", length: 4, visible: 0)],
        processes: [worker(status: :healthy)]
      )

      verdict = analyzer.verdict
      expect(verdict[:status]).to eq("DEGRADED")
      expect(verdict[:reasons].join).to include("dead-letter")
    end

    it "is DEGRADED when the oldest transaction pins the MVCC horizon" do
      stub_data_source(
        queues: [],
        processes: [worker(status: :healthy)],
        health: { oldest_transaction_age_sec: 600 }
      )

      verdict = analyzer.verdict
      expect(verdict[:status]).to eq("DEGRADED")
      expect(verdict[:reasons].join).to include("MVCC")
    end

    it "STALLED takes precedence over DEGRADED conditions" do
      stub_data_source(
        queues: [queue(name: "pgbus_default", length: 5, visible: 5, max_read_ct: 0),
                 queue(name: "pgbus_default_dlq", length: 2, visible: 0)],
        processes: [worker(status: :stalled), worker(status: :stale)]
      )

      expect(analyzer.verdict[:status]).to eq("STALLED")
    end
  end

  describe "#verdict resilience" do
    before { allow(Pgbus.logger).to receive(:warn) }

    it "still produces a verdict when queue_health_stats raises" do
      allow(data_source).to receive_messages(
        queues_with_metrics: [], processes: [], stream_queue_names: Set.new,
        drained_queue_names: nil, unregistered_stream_queue_count: 0
      )
      allow(data_source).to receive(:queue_health_stats).and_raise(StandardError, "boom")

      verdict = analyzer.verdict
      expect(verdict[:status]).to eq("DEGRADED")
      expect(verdict[:reasons].join).to include("queue health stats unavailable")
    end

    it "logs the queue_health_stats failure rather than swallowing it" do
      allow(data_source).to receive_messages(
        queues_with_metrics: [], processes: [], stream_queue_names: Set.new,
        drained_queue_names: nil, unregistered_stream_queue_count: 0
      )
      allow(data_source).to receive(:queue_health_stats).and_raise(StandardError, "boom")

      analyzer.verdict

      expect(Pgbus.logger).to have_received(:warn)
    end
  end

  describe "registered stream queues are excluded from the verdict (issue #359)" do
    it "does not read a stream queue's permanent read_ct=0 backlog as the wedge" do
      # Durable stream delivery is a non-consuming peek: visible messages with
      # read_ct=0 forever is the NORMAL state of a stream queue, not a wedge.
      stub_data_source(
        queues: [queue(name: "pgbus_chat_1_messages", length: 10, visible: 10, max_read_ct: 0)],
        processes: [worker(status: :healthy)],
        stream_queues: ["pgbus_chat_1_messages"]
      )

      verdict = analyzer.verdict
      expect(verdict[:status]).to eq("OK")
      expect(verdict[:reasons]).to be_empty
    end

    it "still detects a genuine job-queue wedge alongside noisy stream queues" do
      stub_data_source(
        queues: [queue(name: "pgbus_chat_1_messages", length: 10, visible: 10, max_read_ct: 0),
                 queue(name: "pgbus_default", length: 5, visible: 5, max_read_ct: 0)],
        processes: [worker(status: :healthy)],
        stream_queues: ["pgbus_chat_1_messages"]
      )

      verdict = analyzer.verdict
      expect(verdict[:status]).to eq("STALLED")
      expect(verdict[:reasons].first).to include("pgbus_default")
      expect(verdict[:reasons].first).not_to include("pgbus_chat_1_messages")
    end

    it "excludes stream backlog from the summary totals" do
      stub_data_source(
        queues: [queue(name: "pgbus_chat_1_messages", length: 10, visible: 10, max_read_ct: 0),
                 queue(name: "pgbus_default", length: 2, visible: 1, max_read_ct: 3)],
        processes: [worker(status: :healthy)],
        stream_queues: ["pgbus_chat_1_messages"]
      )

      summary = analyzer.verdict[:summary]
      expect(summary[:total_visible]).to eq(1)
      expect(summary[:total_depth]).to eq(2)
    end
  end

  describe "fingerprint-matched unregistered streams (issue #366)" do
    it "excludes dormant pre-registry stream queues from the STALLED wedge signal" do
      # known_names (via stream_queue_names) includes fingerprint matches even
      # when they are not yet in the registry — same permanent read_ct=0 shape.
      stub_data_source(
        queues: [queue(name: "pgbus_dormant_99", length: 10, visible: 10, max_read_ct: 0)],
        processes: [worker(status: :healthy)],
        stream_queues: ["pgbus_dormant_99"],
        unregistered_streams: 1
      )

      verdict = analyzer.verdict
      expect(verdict[:status]).to eq("DEGRADED")
      expect(verdict[:reasons].join).to include("unregistered stream")
      expect(verdict[:reasons].join).to include("backfill_registry")
      expect(verdict[:reasons].join).not_to include("never claimed")
    end

    it "does not hint when every fingerprint match is already registered" do
      stub_data_source(
        queues: [queue(name: "pgbus_chat_1", length: 10, visible: 10, max_read_ct: 0)],
        processes: [worker(status: :healthy)],
        stream_queues: ["pgbus_chat_1"],
        unregistered_streams: 0
      )

      expect(analyzer.verdict[:status]).to eq("OK")
    end
  end

  describe "STALLED excludes paused queues" do
    it "classifies a paused queue with read_ct=0 backlog as DEGRADED, not STALLED" do
      stub_data_source(
        queues: [queue(name: "pgbus_default", length: 5, visible: 5, paused: true, max_read_ct: 0)],
        processes: [worker(status: :healthy)]
      )

      verdict = analyzer.verdict
      expect(verdict[:status]).to eq("DEGRADED")
      expect(verdict[:reasons].join).to include("paused")
    end
  end

  # Issue #367 defect 2: the wedge verdict must only reason about queues some
  # configured capsule (or EventBus handler) can actually drain. Live workers
  # prove nothing about a queue they could never claim from.
  describe "#verdict drain-intersection (issue #367 defect 2)" do
    it "does NOT flag STALLED for a backlog queue no capsule drains" do
      stub_data_source(
        queues: [queue(name: "pgbus_adhoc", length: 5, visible: 5, max_read_ct: 0)],
        processes: [worker(status: :healthy)],
        drained: ["pgbus_default"]
      )

      expect(analyzer.verdict[:status]).not_to eq("STALLED")
    end

    it "reports an undrained backlog as DEGRADED with a 'no capsule' reason" do
      stub_data_source(
        queues: [queue(name: "pgbus_adhoc", length: 5, visible: 5, max_read_ct: 0)],
        processes: [worker(status: :healthy)],
        drained: ["pgbus_default"]
      )

      verdict = analyzer.verdict
      expect(verdict[:status]).to eq("DEGRADED")
      expect(verdict[:reasons].join).to include("no capsule")
      expect(verdict[:reasons].join).to include("pgbus_adhoc")
    end

    it "still flags STALLED for a drained queue while ignoring an undrained one" do
      stub_data_source(
        queues: [queue(name: "pgbus_default", length: 5, visible: 5, max_read_ct: 0),
                 queue(name: "pgbus_adhoc", length: 9, visible: 9, max_read_ct: 0)],
        processes: [worker(status: :healthy)],
        drained: ["pgbus_default"]
      )

      verdict = analyzer.verdict
      expect(verdict[:status]).to eq("STALLED")
      expect(verdict[:reasons].first).to include("pgbus_default")
      expect(verdict[:reasons].first).not_to include("pgbus_adhoc")
    end

    it "treats a wildcard capsule (drained_queue_names nil) as draining every queue" do
      stub_data_source(
        queues: [queue(name: "pgbus_adhoc", length: 5, visible: 5, max_read_ct: 0)],
        processes: [worker(status: :healthy)],
        drained: nil
      )

      expect(analyzer.verdict[:status]).to eq("STALLED")
    end

    it "counts a priority _pN sub-table as drained (drained set is physical-expanded)" do
      # A worker draining logical "critical" in priority mode drains the physical
      # pgbus_critical_p0.._pN tables; drained_queue_names returns those expanded
      # names, so a wedge on the priority sub-table is still caught as STALLED.
      stub_data_source(
        queues: [queue(name: "pgbus_critical_p0", length: 5, visible: 5, max_read_ct: 0)],
        processes: [worker(status: :healthy)],
        drained: %w[pgbus_critical_p0 pgbus_critical_p1]
      )

      verdict = analyzer.verdict
      expect(verdict[:status]).to eq("STALLED")
      expect(verdict[:reasons].join).not_to include("no capsule")
    end

    it "still reports an undrained-queue DEGRADED even when its messages were once claimed" do
      stub_data_source(
        queues: [queue(name: "pgbus_adhoc", length: 5, visible: 5, max_read_ct: 4, visible_unread: 0)],
        processes: [worker(status: :healthy)],
        drained: ["pgbus_default"]
      )

      # Past reads don't make an undrained queue safe: if no capsule drains it
      # now, whatever claimed those messages is gone and the backlog will sit
      # forever. It is DEGRADED (a config hazard), never STALLED (not the wedge).
      verdict = analyzer.verdict
      expect(verdict[:status]).to eq("DEGRADED")
      expect(verdict[:reasons].join).to include("no capsule")
      expect(verdict[:reasons].join).to include("pgbus_adhoc")
    end

    it "catches a wedged EventBus handler queue on a consumer-only fleet (no workers)" do
      # drained_queue_names unions consumer-drained handler queues; the wedge
      # branch must count consumer liveness, else a fleet with no workers never
      # runs (the old worker-only guard returned early).
      stub_data_source(
        queues: [queue(name: "pgbus_order_handler", length: 6, visible: 6, max_read_ct: 0)],
        processes: [{ kind: "consumer", status: :healthy }],
        drained: ["pgbus_order_handler"]
      )

      verdict = analyzer.verdict
      expect(verdict[:status]).to eq("STALLED")
      expect(verdict[:reasons].first).to include("draining process(es) are alive")
    end
  end

  # Issue #367 defect 1: max_read_ct is now populated, so read_ct > 0 must NOT
  # produce the "read_ct=0 (never claimed)" reason string.
  describe "#verdict does not assert 'never claimed' about claimed messages (issue #367 defect 1)" do
    it "is OK (not STALLED) when a drained queue's backlog has been read mid-burst" do
      stub_data_source(
        queues: [queue(name: "pgbus_default", length: 10, visible: 10, max_read_ct: 1)],
        processes: [worker(status: :healthy)]
      )

      verdict = analyzer.verdict
      expect(verdict[:status]).to eq("OK")
      expect(verdict[:reasons].join).not_to include("never claimed")
    end
  end

  # Issue #367 review: the wedge signal must count VISIBLE never-claimed rows,
  # not max(read_ct) over the whole table — one retried/in-flight message
  # (read_ct > 0) must not veto the signal for a pile of visible read_ct=0 jobs.
  describe "#verdict partial-wedge detection (issue #367 review)" do
    it "is STALLED when visible read_ct=0 jobs pile up despite a lingering retried message" do
      # 500 fresh unclaimed jobs (the wedge) + 1 message that retried once, so
      # max_read_ct=1 but 500 visible rows have never been claimed.
      stub_data_source(
        queues: [queue(name: "pgbus_default", length: 501, visible: 501,
                       max_read_ct: 1, visible_unread: 500)],
        processes: [worker(status: :healthy)]
      )

      verdict = analyzer.verdict
      expect(verdict[:status]).to eq("STALLED")
      expect(verdict[:reasons].first).to include("never claimed")
    end

    it "is OK when every visible message has been claimed (visible_unread=0) even if some are read once" do
      stub_data_source(
        queues: [queue(name: "pgbus_default", length: 5, visible: 5,
                       max_read_ct: 2, visible_unread: 0)],
        processes: [worker(status: :healthy)]
      )

      expect(analyzer.verdict[:status]).to eq("OK")
    end
  end

  # Issue #367 review: a heart-beating-but-stalled worker is a wedge regardless
  # of WHICH queue holds the visible backlog — the stalled-worker branch must
  # run against the full active backlog, not only the drained partition.
  describe "#verdict stalled-worker precedence across undrained backlog (issue #367 review)" do
    it "is STALLED (not DEGRADED) when a worker is stalled and the backlog is on an undrained queue" do
      stub_data_source(
        queues: [queue(name: "pgbus_y", length: 8, visible: 8, max_read_ct: 0)],
        processes: [worker(status: :stalled)],
        drained: ["pgbus_x"]
      )

      verdict = analyzer.verdict
      expect(verdict[:status]).to eq("STALLED")
      expect(verdict[:reasons].first).to include("stalled")
    end
  end

  # Issue #367 minor: a reason string listing hundreds of queues is a multi-KB
  # log line. Truncate to the first ~10 with a (+N more) suffix.
  describe "#verdict truncates long backlog name lists (issue #367 minor)" do
    it "lists at most 10 queue names and appends (+N more)" do
      queues = (1..25).map { |i| queue(name: "pgbus_q#{i}", length: 2, visible: 2, max_read_ct: 0) }
      stub_data_source(queues: queues, processes: [worker(status: :healthy)])

      reason = analyzer.verdict[:reasons].first
      expect(reason).to include("(+15 more)")
      expect(reason.scan(/pgbus_q\d+/).size).to eq(10)
    end
  end

  # Issue #589: a visible read_ct=0 message is the wedge only once it has been
  # claimable for longer than health_unclaimed_grace. Below it, a worker's next
  # poll is simply still coming, and /readyz must not flap 503.
  describe "#verdict unclaimed grace (issue #589)" do
    subject(:analyzer) { described_class.new(data_source, grace: 60) }

    it "is OK when the visible unread rows have been claimable for less than the grace" do
      stub_data_source(
        queues: [queue(name: "pgbus_default", length: 2, visible: 2, unread_age: 1)],
        processes: [worker(status: :healthy)]
      )

      verdict = analyzer.verdict
      expect(verdict[:status]).to eq("OK")
      expect(verdict[:reasons]).to be_empty
    end

    it "is STALLED once they have been claimable for the grace, and names the age" do
      stub_data_source(
        queues: [queue(name: "pgbus_default", length: 2, visible: 2, unread_age: 74)],
        processes: [worker(status: :healthy)]
      )

      verdict = analyzer.verdict
      expect(verdict[:status]).to eq("STALLED")
      expect(verdict[:reasons].first).to include("never claimed, oldest claimable for 74s")
    end

    it "counts an age exactly at the grace" do
      stub_data_source(
        queues: [queue(name: "pgbus_default", length: 2, visible: 2, unread_age: 60)],
        processes: [worker(status: :healthy)]
      )

      expect(analyzer.verdict[:status]).to eq("STALLED")
    end

    it "flags only the queues past the grace and names the oldest of them" do
      stub_data_source(
        queues: [queue(name: "pgbus_fresh", length: 1, visible: 1, unread_age: 2),
                 queue(name: "pgbus_old", length: 1, visible: 1, unread_age: 90),
                 queue(name: "pgbus_older", length: 1, visible: 1, unread_age: 300)],
        processes: [worker(status: :healthy)]
      )

      reason = analyzer.verdict[:reasons].first
      expect(reason).to start_with("2 queue(s)")
      expect(reason).to include("oldest claimable for 300s")
      expect(reason).to include("pgbus_old, pgbus_older")
      expect(reason).not_to include("pgbus_fresh")
    end

    it "keeps the conservative default when the hash has no age (older or stubbed data source)" do
      stub_data_source(
        queues: [queue(name: "pgbus_default", length: 3, visible: 3)],
        processes: [worker(status: :healthy)]
      )

      verdict = analyzer.verdict
      expect(verdict[:status]).to eq("STALLED")
      expect(verdict[:reasons].first).to include("never claimed")
      expect(verdict[:reasons].first).not_to include("oldest claimable for")
    end

    it "treats a present-but-nil age like a missing one" do
      q = queue(name: "pgbus_default", length: 3, visible: 3).merge(oldest_unread_claimable_age_sec: nil)
      stub_data_source(queues: [q], processes: [worker(status: :healthy)])

      expect(analyzer.verdict[:status]).to eq("STALLED")
    end

    it "with a grace of 0 reports any visible unread row, as before 0.22" do
      stub_data_source(
        queues: [queue(name: "pgbus_default", length: 1, visible: 1, unread_age: 0)],
        processes: [worker(status: :healthy)]
      )

      expect(described_class.new(data_source, grace: 0).verdict[:status]).to eq("STALLED")
    end

    it "leaves the worker-reported :stalled branch alone, whatever the backlog age" do
      stub_data_source(
        queues: [queue(name: "pgbus_default", length: 2, visible: 2, unread_age: 1)],
        processes: [worker(status: :stalled)]
      )

      verdict = analyzer.verdict
      expect(verdict[:status]).to eq("STALLED")
      expect(verdict[:reasons].first).to include("claim loop not advancing")
    end

    it "reports the oldest unread claimable age over the drained backlog in the summary, below the grace too" do
      stub_data_source(
        queues: [queue(name: "pgbus_a", length: 1, visible: 1, unread_age: 4),
                 queue(name: "pgbus_b", length: 1, visible: 1, unread_age: 12),
                 queue(name: "pgbus_undrained", length: 1, visible: 1, unread_age: 900)],
        processes: [worker(status: :healthy)],
        drained: %w[pgbus_a pgbus_b]
      )

      expect(analyzer.verdict[:summary][:oldest_unread_claimable_age_sec]).to eq(12)
    end

    it "reports a nil summary age when no drained queue has a known unread age" do
      stub_data_source(queues: [queue(name: "pgbus_default")], processes: [worker(status: :healthy)])

      expect(analyzer.verdict[:summary]).to include(oldest_unread_claimable_age_sec: nil)
    end

    it "reads Pgbus.configuration.health_unclaimed_grace when no grace is given" do
      original = Pgbus.configuration.health_unclaimed_grace
      Pgbus.configuration.health_unclaimed_grace = 600
      stub_data_source(
        queues: [queue(name: "pgbus_default", length: 2, visible: 2, unread_age: 120)],
        processes: [worker(status: :healthy)]
      )

      expect(described_class.new(data_source).verdict[:status]).to eq("OK")
    ensure
      Pgbus.configuration.health_unclaimed_grace = original
    end
  end
end
