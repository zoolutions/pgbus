# frozen_string_literal: true

require "spec_helper"
require_relative "../../benchmarks/support/streams_scale_harness"

# Pure-logic specs for the streams scale bench (issue #620): M SSE clients on
# one web process. No database, no Puma, no subprocess: the knob parsing, the
# child result line, the percentile and fan-out arithmetic, the fd-limit clamp
# and the table are fed hand-built values.
RSpec.describe StreamsScaleHarness do
  describe ".parse_counts" do
    it "parses a comma-separated list of client counts" do
      expect(described_class.parse_counts("100,1000", name: "STREAMS_SCALE_CLIENTS")).to eq([100, 1000])
      expect(described_class.parse_counts(" 100, 2000 ", name: "STREAMS_SCALE_CLIENTS")).to eq([100, 2000])
    end

    it "rejects zero, negatives, non-integers and an empty list, naming the knob" do
      ["0", "100,0", "-5", "many", "1.5", "", "100,,1000"].each do |value|
        expect { described_class.parse_counts(value, name: "STREAMS_SCALE_CLIENTS") }
          .to raise_error(ArgumentError, /STREAMS_SCALE_CLIENTS/), "expected #{value.inspect} to be rejected"
      end
    end
  end

  describe ".parse_integer" do
    it "accepts a positive integer when the minimum is 1" do
      expect(described_class.parse_integer("4", name: "STREAMS_SCALE_CLIENT_PROCESSES", min: 1)).to eq(4)
    end

    it "accepts 0 when the minimum is 0" do
      expect(described_class.parse_integer("0", name: "STREAMS_SCALE_WRITERS", min: 0)).to eq(0)
    end

    it "rejects 0 when the minimum is 1, and anything that is not an integer" do
      expect { described_class.parse_integer("0", name: "STREAMS_SCALE_BROADCASTS", min: 1) }
        .to raise_error(ArgumentError, /STREAMS_SCALE_BROADCASTS must be an integer >= 1 \(got "0"\)/)
      ["x", "2.5", "", "-1", "1e3"].each do |value|
        expect { described_class.parse_integer(value, name: "STREAMS_SCALE_SLOW", min: 0) }
          .to raise_error(ArgumentError, /STREAMS_SCALE_SLOW/), "expected #{value.inspect} to be rejected"
      end
    end
  end

  describe ".parse_modes" do
    it "parses the broadcast modes in the order given" do
      expect(described_class.parse_modes("ephemeral,durable")).to eq(%i[ephemeral durable])
      expect(described_class.parse_modes(" durable ")).to eq(%i[durable])
    end

    it "rejects an unknown mode, a duplicate and an empty list" do
      ["", "durable,durable", "persistent", "ephemeral,"].each do |value|
        expect { described_class.parse_modes(value) }
          .to raise_error(ArgumentError, /STREAMS_SCALE_MODES/), "expected #{value.inspect} to be rejected"
      end
    end
  end

  describe ".knobs" do
    it "has the documented defaults" do
      expect(described_class.knobs({})).to eq(
        clients: [100, 1000], client_processes: 4, broadcasts: 20,
        modes: %i[ephemeral durable], writers: 0, slow: 0, profile: nil
      )
    end

    it "reads every knob from the environment" do
      env = {
        "STREAMS_SCALE_CLIENTS" => "2000", "STREAMS_SCALE_CLIENT_PROCESSES" => "8",
        "STREAMS_SCALE_BROADCASTS" => "5", "STREAMS_SCALE_MODES" => "durable",
        "STREAMS_SCALE_WRITERS" => "2", "STREAMS_SCALE_SLOW" => "3", "STREAMS_SCALE_PROFILE" => " tmp/prof "
      }

      expect(described_class.knobs(env)).to eq(
        clients: [2000], client_processes: 8, broadcasts: 5, modes: %i[durable], writers: 2, slow: 3,
        profile: "tmp/prof"
      )
    end
  end

  describe ".profile_summary (issue #621)" do
    let(:frame) { Struct.new(:filename, :label) }
    let(:write) { frame.new("/gems/pgbus/lib/pgbus/web/streamer/io_writer.rb", "IO#write_nonblock") }
    let(:enqueue) { frame.new("/gems/pgbus/lib/pgbus/web/streamer/connection.rb", "Connection#enqueue") }
    let(:framing) { frame.new("/gems/pgbus/lib/pgbus/streams/envelope.rb", "Envelope.message") }
    let(:pop) { frame.new(nil, "Thread::Queue#pop") }

    it "shares running samples only, by leaf frame and by the gate's markers" do
      summary = described_class.profile_summary(
        [[[write, enqueue], :running, 3], [[framing, enqueue], :running, 1], [[pop], :idle, 50], [[enqueue], :stalled, 4]]
      )

      expect(summary).to include("running" => 4.0, "idle" => 50.0, "stalled" => 4.0)
      expect(summary["top_self"]).to eq([["io_writer.rb:IO#write_nonblock", 0.75], ["envelope.rb:Envelope.message", 0.25]])
      expect(summary["inclusive"]).to include("io_writer.rb" => 0.75, "write_nonblock" => 0.75,
                                              "framing (envelope.rb)" => 0.25, "audience filter" => 0.0)
    end

    it "reports zero shares when the thread never ran" do
      summary = described_class.profile_summary([[[pop], :idle, 10]])

      expect(summary["top_self"]).to eq([])
      expect(summary["inclusive"].values).to all(eq(0.0))
    end

    it "prints the running count, the GVL wait and both share lists" do
      lines = described_class.profile_lines(described_class.profile_summary([[[write, enqueue], :running, 3],
                                                                             [[enqueue], :stalled, 1]]))

      expect(lines[0]).to eq("running 3 samples, stalled (GVL) 25.0 % of running + stalled")
      expect(lines[1]).to start_with("inclusive: io_writer.rb 100.0 %, write_nonblock 100.0 %")
      expect(lines[2]).to eq("top self: io_writer.rb:IO#write_nonblock 100.0 %")
    end
  end

  describe ".split_clients" do
    it "spreads M over the client processes, the remainder on the first ones" do
      expect(described_class.split_clients(1000, 4)).to eq([250, 250, 250, 250])
      expect(described_class.split_clients(10, 4)).to eq([3, 3, 2, 2])
    end

    it "never spawns a process with no clients" do
      expect(described_class.split_clients(3, 4)).to eq([1, 1, 1])
    end
  end

  describe ".percentile" do
    it "is the nearest-rank percentile of the values" do
      values = (1..100).to_a.shuffle

      expect(described_class.percentile(values, 50)).to eq(50)
      expect(described_class.percentile(values, 99)).to eq(99)
      expect(described_class.percentile(values, 100)).to eq(100)
    end

    it "is the only value of a single sample and nil for none" do
      expect(described_class.percentile([7.5], 99)).to eq(7.5)
      expect(described_class.percentile([], 50)).to be_nil
    end
  end

  describe ".parse_child_result" do
    it "parses the last result line, ignoring anything else the child printed" do
      output = "warming up\n#{described_class::CHILD_RESULT}{\"index\":0}\n" \
               "#{described_class::CHILD_RESULT}{\"index\":1,\"clients\":[]}\nbye\n"

      expect(described_class.parse_child_result(output)).to eq("index" => 1, "clients" => [])
    end

    it "reads a cell subprocess's result line with the cell prefix" do
      output = "  ephemeral M=100: registered 100\n#{described_class::CELL_RESULT}{\"m\":100}\n"

      expect(described_class.parse_child_result(output, prefix: described_class::CELL_RESULT)).to eq("m" => 100)
    end

    it "raises with the child's output when there is no result line" do
      expect { described_class.parse_child_result("boom\n") }
        .to raise_error(RuntimeError, /no STREAMS_SCALE_CHILD_RESULT line.*boom/m)
    end
  end

  describe ".fd_target" do
    let(:infinity) { (2**63) - 1 }

    it "leaves a soft limit that already fits alone, even above OPEN_MAX" do
      expect(described_class.fd_target(soft: 1_048_576, hard: infinity, needed: 2_064, infinity: infinity))
        .to be_nil
    end

    it "raises the soft limit to the hard one when the hard limit is finite" do
      expect(described_class.fd_target(soft: 256, hard: 65_536, needed: 2_064, infinity: infinity)).to eq(65_536)
    end

    it "raises it to OPEN_MAX when the hard limit is infinite on macOS (it refuses setrlimit to infinity)" do
      expect(described_class.fd_target(soft: 256, hard: infinity, needed: 2_064, infinity: infinity, darwin: true))
        .to eq(described_class::OPEN_MAX)
    end

    it "raises it to what is needed when the hard limit is infinite elsewhere, past OPEN_MAX" do
      expect(described_class.fd_target(soft: 256, hard: infinity, needed: 20_064, infinity: infinity, darwin: false))
        .to eq(20_064)
    end

    it "never lowers a soft limit above OPEN_MAX that still falls short" do
      expect(described_class.fd_target(soft: 20_000, hard: infinity, needed: 20_064, infinity: infinity, darwin: true))
        .to be_nil
    end
  end

  describe ".raise_fd_limit!" do
    it "keeps the current soft limit when the kernel refuses the raise, so the skip hint can print" do
      allow(Process).to receive(:getrlimit).with(:NOFILE).and_return([256, Process::RLIM_INFINITY])
      allow(Process).to receive(:setrlimit).and_raise(Errno::EINVAL)

      expect(described_class.raise_fd_limit!(2_064)).to eq(256)
    end
  end

  describe ".fd_needed and .fd_skip_reason" do
    it "needs two descriptors per client plus headroom" do
      expect(described_class.fd_needed(1000)).to eq(2_064)
    end

    it "is nil when the cap fits" do
      expect(described_class.fd_skip_reason(1000, cap: 2_064)).to be_nil
    end

    it "says the cap and how to raise it when it does not" do
      reason = described_class.fd_skip_reason(1000, cap: 256)

      expect(reason).to include("skipped: M=1000 needs 2 × 1000 + 64 = 2064 file descriptors, the limit is 256")
      expect(reason).to include("ulimit -n")
      expect(reason).to include("launchctl limit maxfiles")
    end
  end

  describe ".connect_stats" do
    it "measures each client's connected_at from the start barrier" do
      stats = described_class.connect_stats([100.010, 100.020, 100.500], go_at: 100.0)

      expect(stats[:p50_ms]).to be_within(1e-6).of(20.0)
      expect(stats[:p99_ms]).to be_within(1e-6).of(500.0)
    end
  end

  describe ".fanout_stats" do
    let(:broadcasts) { [{ "tag" => "0", "at" => 10.0 }, { "tag" => "1", "at" => 11.0 }] }

    it "measures each broadcast to its first and its last client" do
      clients = [
        { "events" => { "0" => 10.004, "1" => 11.020 } },
        { "events" => { "0" => 10.010, "1" => 11.006 } }
      ]

      stats = described_class.fanout_stats(broadcasts, clients)

      expect(stats[:first_p50_ms]).to be_within(1e-6).of(4.0)
      expect(stats[:last_p50_ms]).to be_within(1e-6).of(10.0)
      expect(stats[:last_p99_ms]).to be_within(1e-6).of(20.0)
      expect(stats[:misses]).to eq(0)
    end

    it "counts a broadcast a client never got, or got after the deadline, as a miss" do
      clients = [
        { "events" => { "0" => 10.004 } },
        { "events" => { "0" => 16.0, "1" => 11.002 } }
      ]

      stats = described_class.fanout_stats(broadcasts, clients, deadline_s: 5.0)

      expect(stats[:misses]).to eq(2)
      expect(stats[:last_p99_ms]).to be_within(1e-6).of(4.0)
    end
  end

  describe ".parse_loadavg" do
    it "reads the one-minute load from sysctl vm.loadavg and /proc/loadavg" do
      expect(described_class.parse_loadavg("{ 6.60 5.95 5.80 }")).to eq(6.6)
      expect(described_class.parse_loadavg("0.52 0.58 0.59 1/482 12345\n")).to eq(0.52)
    end

    it "is nil when the text has no number" do
      expect(described_class.parse_loadavg("")).to be_nil
    end
  end

  describe ".time_calls" do
    let(:target_class) do
      Class.new do
        def tick(value) = value * 2

        private

        def work(value, step:) = value + step
      end
    end

    it "records each call's wall time in ms and returns what the method returned" do
      target = target_class.new
      sink = Queue.new
      described_class.time_calls(target, :tick, sink)

      expect(target.tick(21)).to eq(42)
      expect(sink.size).to eq(1)
      expect(sink.pop).to be_a(Float).and(be >= 0)
    end

    it "keeps a private method private and forwards keywords" do
      target = target_class.new
      sink = Queue.new
      described_class.time_calls(target, :work, sink)

      expect(target.respond_to?(:work)).to be(false)
      expect(target.send(:work, 1, step: 2)).to eq(3)
      expect(sink.size).to eq(1)
    end

    it "times only the instance it was given" do
      sink = Queue.new
      described_class.time_calls(target_class.new, :tick, sink)
      target_class.new.tick(1)

      expect(sink.size).to eq(0)
    end
  end

  describe ".table" do
    let(:row) do
      {
        "m" => 1000, "connect_all_s" => 2.346, "connect_p99_ms" => 1890.4,
        "fanout_first_p50_ms" => 3.21, "fanout_last_p50_ms" => 41.5, "fanout_last_p99_ms" => 77.26,
        "wake_p50_ms" => 38.1, "wake_p99_ms" => 70.9, "heartbeat_ms" => 45.6, "watcher_ms" => 0.81,
        "rss_kb_per_conn" => 23.4, "cpu_ratio" => 0.42, "misses" => 0, "errors" => 0, "over_capacity" => 0, "retries" => 412
      }
    end

    it "renders one markdown row per M under the column header" do
      lines = described_class.table([row])

      expect(lines[0]).to eq(
        "| M | connect all (s) | connect p99 (ms) | fan-out first p50 (ms) | fan-out last p50/p99 (ms) | " \
        "dispatcher wake p50/p99 (ms) | heartbeat tick (ms) | watcher build (ms) | RSS Δ/conn (KB) | CPU/wall | " \
        "misses / errors / 503s | retried connects |"
      )
      expect(lines[1]).to start_with("|---:|")
      expect(lines[2]).to eq(
        "| 1000 | 2.35 | 1890.4 | 3.2 | 41.5 / 77.3 | 38.1 / 70.9 | 45.6 | 0.81 | 23.4 | 42 % | 0 / 0 / 0 | 412 |"
      )
    end

    it "marks a row that missed broadcasts or failed connects" do
      line = described_class.table([row.merge("misses" => 3, "errors" => 1)]).last

      expect(line).to end_with("| **3 / 1 / 0** | 412 |")
    end

    it "prints a skipped row with its reason" do
      line = described_class.table([{ "m" => 2000, "skip_reason" => "skipped: M=2000 needs more" }]).last

      expect(line).to eq("| 2000 | skipped: M=2000 needs more | | | | | | | | | | |")
    end

    it "prints n/a for a value the cell did not measure" do
      line = described_class.table([row.merge("heartbeat_ms" => nil)]).last

      expect(line).to include("| n/a |")
    end
  end
end
