# frozen_string_literal: true

require "spec_helper"
require_relative "../../benchmarks/support/worker_profile_harness"

# Pure-logic specs for the worker profiling harness (issue #484). No database
# and no profiler run: the classifier and the share math are fed hand-built
# frame lists, so the bucket split the bench reports is pinned independently
# of any machine's timing.
RSpec.describe WorkerProfileHarness do
  def frame(filename, label = "m")
    described_class::Frame.new(filename: filename, label: label)
  end

  describe ".classify_frame" do
    it "maps pgbus source to :pgbus" do
      expect(described_class.classify_frame(frame("/app/vendor/pgbus-0.17.0/lib/pgbus/active_job/executor.rb"))).to eq(:pgbus)
      expect(described_class.classify_frame(frame("/src/pgbus/lib/pgbus/client.rb"))).to eq(:pgbus)
    end

    it "maps the pg, pgmq-ruby and connection_pool gems to :driver" do
      expect(described_class.classify_frame(frame("/gems/pg-1.5.9/lib/pg/connection.rb"))).to eq(:driver)
      expect(described_class.classify_frame(frame("/gems/pgmq-ruby-0.7.2/lib/pgmq/client/consumer.rb"))).to eq(:driver)
      expect(described_class.classify_frame(frame("/gems/connection_pool-2.5.0/lib/connection_pool.rb"))).to eq(:driver)
    end

    # libpq calls surface as C-function frames with no source path; ActiveRecord
    # queries reach them without passing through any pg-gem Ruby file.
    it "maps a PG:: C-function frame to :driver by its label" do
      expect(described_class.classify_frame(frame("<cfunc>", "PG::Connection#exec_params"))).to eq(:driver)
    end

    it "maps Rails framework gems to :rails" do
      %w[activejob activesupport activerecord railties globalid].each do |gem_name|
        expect(described_class.classify_frame(frame("/gems/#{gem_name}-8.1.3/lib/x.rb"))).to eq(:rails)
      end
    end

    it "maps everything else, including internal and nil frames, to :ruby" do
      expect(described_class.classify_frame(frame("<internal:kernel>"))).to eq(:ruby)
      expect(described_class.classify_frame(frame("/gems/concurrent-ruby-1.3.5/lib/x.rb"))).to eq(:ruby)
      expect(described_class.classify_frame(frame(nil, nil))).to eq(:ruby)
    end
  end

  describe ".classify_sample" do
    let(:pgmq_stack) do
      [frame("<cfunc>", "PG::Connection#exec_params"), frame("/gems/pgmq-ruby-0.7.2/lib/pgmq/x.rb"),
       frame("/src/lib/pgbus/client.rb")]
    end
    let(:active_record_stack) do
      [frame("<cfunc>", "PG::Connection#exec_params"),
       frame("/gems/activerecord-8.1/lib/database_statements.rb", "DatabaseStatements#perform_query"),
       frame("/src/lib/pgbus/failed_event_recorder.rb")]
    end
    let(:pool_wait_stack) do
      [frame("<cfunc>", "Thread::ConditionVariable#wait"),
       frame("/gems/connection_pool-2.5.0/lib/connection_pool/timed_stack.rb", "ConnectionPool::TimedStack#pop"),
       frame("/gems/connection_pool-2.5.0/lib/connection_pool.rb", "ConnectionPool#with")]
    end
    let(:ar_pool_wait_stack) do
      [frame("<cfunc>", "MonitorMixin::ConditionVariable#wait"),
       frame("/gems/activerecord-8.1/lib/queue.rb", "ActiveRecord::ConnectionAdapters::ConnectionPool::Queue#wait_poll")]
    end
    let(:work_wait_stack) do
      [frame("<internal:thread_sync>", "Thread::Queue#pop"),
       frame("/gems/concurrent-ruby-1.3.5/lib/pool.rb", "Concurrent::RubyThreadPoolExecutor::Worker#create_worker")]
    end

    it "attributes an idle sample blocked in libpq via pgmq-ruby to :db_wait" do
      expect(described_class.classify_sample(pgmq_stack, category: :idle)).to eq(:db_wait)
    end

    it "attributes an idle sample blocked in libpq via ActiveRecord to :db_wait" do
      expect(described_class.classify_sample(active_record_stack, category: :idle)).to eq(:db_wait)
    end

    it "attributes an idle sample waiting on a connection-pool checkout to :pool_wait" do
      expect(described_class.classify_sample(pool_wait_stack, category: :idle)).to eq(:pool_wait)
      expect(described_class.classify_sample(ar_pool_wait_stack, category: :idle)).to eq(:pool_wait)
    end

    it "attributes an idle sample with no driver or pool frame to :idle (waiting for work)" do
      expect(described_class.classify_sample(work_wait_stack, category: :idle)).to eq(:idle)
    end

    it "attributes a stalled sample to :gvl_wait" do
      expect(described_class.classify_sample(pgmq_stack, category: :stalled)).to eq(:gvl_wait)
    end

    it "attributes a running sample to the deepest non-:ruby frame (leaf first)" do
      stack = [frame("<internal:hash>"), frame("/src/lib/pgbus/active_job/executor.rb"), frame("/gems/activejob-8.1/lib/x.rb")]
      expect(described_class.classify_sample(stack, category: :running)).to eq(:pgbus)
    end

    it "attributes a running sample with only :ruby frames to :ruby" do
      expect(described_class.classify_sample(work_wait_stack, category: :running)).to eq(:ruby)
    end
  end

  describe ".shares" do
    it "returns every bucket, summing to 1.0" do
      result = described_class.shares([[:pgbus, 2], [:db_wait, 6], [:rails, 2]])

      expect(result.keys).to eq(described_class::BUCKETS)
      expect(result.values.sum).to be_within(1e-9).of(1.0)
      expect(result[:db_wait]).to be_within(1e-9).of(0.6)
      expect(result[:idle]).to eq(0.0)
    end

    it "returns all zeros for an empty sample set instead of dividing by zero" do
      expect(described_class.shares([]).values).to all(eq(0.0))
    end

    # The CPU composition: of the samples that were actually on-CPU, whose code
    # was running? Wait buckets are dropped before normalising.
    it "restricts to the given buckets and renormalises over them" do
      result = described_class.shares([[:pgbus, 1], [:rails, 3], [:db_wait, 96]],
                                      buckets: described_class::CPU_BUCKETS)

      expect(result.keys).to eq(described_class::CPU_BUCKETS)
      expect(result[:pgbus]).to be_within(1e-9).of(0.25)
      expect(result[:rails]).to be_within(1e-9).of(0.75)
    end
  end

  describe ".sample_category" do
    it "maps vernier's sample category ids to symbols" do
      expect(described_class.sample_category(0)).to eq(:running)
      expect(described_class.sample_category(1)).to eq(:idle)
      expect(described_class.sample_category(2)).to eq(:stalled)
      expect(described_class.sample_category(nil)).to eq(:running)
    end
  end

  # Issue #486: the bench runs the same harness over a Worker and an event
  # Consumer, so each result row says which loop it measured.
  describe "Cell" do
    it "round-trips role through to_h" do
      cell = described_class::Cell.new(role: "consumer", location: "local", jit: "yjit", jobs: 10)

      expect(cell.to_h).to include("role" => "consumer", "location" => "local")
    end

    # Issue #602: a fleet row carries its process count and the Postgres cost.
    it "round-trips the fleet fields through to_h, nil when not set" do
      cell = described_class::Cell.new(role: "fleet", location: "local", jit: "none", jobs: 10, processes: 2,
                                       pg_ms_per_job: 0.1, pg_statements_per_job: 1.5, pg_top: [])

      expect(cell.to_h).to include("processes" => 2, "pg_ms_per_job" => 0.1, "pg_statements_per_job" => 1.5,
                                   "pg_top" => [], "skip_reason" => nil)
    end
  end

  describe ".completion_batching?" do
    it "is on for 1 and off when unset or 0" do
      expect(described_class.completion_batching?("WP_BENCH_COMPLETION_BATCHING" => "1")).to be(true)
      expect(described_class.completion_batching?("WP_BENCH_COMPLETION_BATCHING" => "0")).to be(false)
      expect(described_class.completion_batching?({})).to be(false)
    end

    it "rejects anything else" do
      expect { described_class.completion_batching?("WP_BENCH_COMPLETION_BATCHING" => "yes") }
        .to raise_error(ArgumentError, /WP_BENCH_COMPLETION_BATCHING must be 0 or 1/)
    end
  end

  describe ".completed_jobs" do
    it "is nil (not set) when unset, else the Symbol" do
      expect(described_class.completed_jobs({})).to be_nil
      expect(described_class.completed_jobs("WP_BENCH_COMPLETED_JOBS" => "delete")).to eq(:delete)
      expect(described_class.completed_jobs("WP_BENCH_COMPLETED_JOBS" => "archive")).to eq(:archive)
    end

    it "rejects anything else" do
      expect { described_class.completed_jobs("WP_BENCH_COMPLETED_JOBS" => "drop") }
        .to raise_error(ArgumentError, /WP_BENCH_COMPLETED_JOBS must be archive or delete/)
    end
  end

  # Issue #618: the rails bucket split by who owns the work. The leaf rails
  # frame names it; a generic leaf (callbacks, notifications, the executor
  # wrap, ActiveRecord) takes the nearest owner frame toward the root.
  describe ".classify_rails_frame" do
    let(:aj) { "/gems/activejob-8.1.4/lib/active_job" }
    let(:as) { "/gems/activesupport-8.1.4/lib/active_support" }
    let(:ar) { "/gems/activerecord-8.1.4/lib/active_record" }
    let(:executor_frame) { frame("/src/pgbus/lib/pgbus/active_job/executor.rb") }

    def classify(*frames) = described_class.classify_rails_frame(frames)

    it "names ActiveJob's log subscriber and the loggers aj_log" do
      expect(classify(frame("<internal:kernel>"), frame("#{aj}/log_subscriber.rb"), executor_frame)).to eq("aj_log")
      expect(classify(frame("#{aj}/logging.rb"))).to eq("aj_log")
      expect(classify(frame("#{as}/log_subscriber.rb"))).to eq("aj_log")
      expect(classify(frame("#{as}/tagged_logging.rb"))).to eq("aj_log")
      expect(classify(frame("#{as}/logger_thread_safe_level.rb"))).to eq("aj_log")
      expect(classify(frame("#{as}/broadcast_logger.rb"))).to eq("aj_log")
    end

    it "names argument deserialization and GlobalID aj_arguments" do
      expect(classify(frame("#{aj}/arguments.rb"), executor_frame)).to eq("aj_arguments")
      expect(classify(frame("#{aj}/serializers/object_serializer.rb"))).to eq("aj_arguments")
      expect(classify(frame("/gems/globalid-1.2.1/lib/global_id/locator.rb"))).to eq("aj_arguments")
    end

    it "names a callbacks leaf owned by ActiveJob aj_callbacks" do
      expect(classify(frame("#{as}/callbacks.rb"), frame("#{aj}/execution.rb"), executor_frame)).to eq("aj_callbacks")
    end

    it "names a notifications leaf by its nearest owner" do
      activejob_nearer = [frame("#{as}/notifications/instrumenter.rb"), frame("#{aj}/instrumentation.rb"),
                          frame("/src/pgbus/lib/pgbus/instrumentation.rb")]
      pgbus_nearer = [frame("#{as}/notifications/fanout.rb"), frame("#{as}/notifications.rb"),
                      frame("/src/pgbus/lib/pgbus/instrumentation.rb"), frame("#{aj}/execution.rb")]

      expect(classify(*activejob_nearer)).to eq("notif:activejob")
      expect(classify(*pgbus_nearer)).to eq("notif:pgbus")
    end

    it "names the executor wrap's own frames and the hooks it runs executor_wrap" do
      wrapper = frame("#{as}/execution_wrapper.rb")

      expect(classify(wrapper, executor_frame)).to eq("executor_wrap")
      expect(classify(frame("#{as}/current_attributes.rb"), wrapper, executor_frame)).to eq("executor_wrap")
      expect(classify(frame("#{ar}/query_cache.rb"), wrapper, executor_frame)).to eq("executor_wrap")
      expect(classify(frame("#{as}/callbacks.rb"), wrapper, executor_frame)).to eq("executor_wrap")
    end

    # A job's own ActiveRecord work runs inside perform_now, inside the wrap:
    # ActiveJob is the nearer owner, and it is not the wrap's cost.
    it "names an ActiveRecord leaf with ActiveJob nearer than the wrap aj_other" do
      stack = [frame("#{ar}/relation.rb"), frame("#{aj}/execution.rb"), frame("#{as}/execution_wrapper.rb"), executor_frame]

      expect(classify(*stack)).to eq("aj_other")
    end

    # pgbus's own calls into Rails (the stat buffer's insert_all, the
    # heartbeat's process row) are pgbus's cost, not ActiveJob's.
    it "names a rails leaf whose nearest owner is pgbus, outside notifications, pgbus_rails" do
      stat_flush = frame("/src/pgbus/lib/pgbus/stat_buffer.rb", "Pgbus::StatBuffer#write_to_database")

      expect(classify(frame("#{ar}/insert_all.rb"), stat_flush)).to eq("pgbus_rails")
      expect(classify(frame("#{as}/core_ext/hash/keys.rb"), stat_flush)).to eq("pgbus_rails")
      expect(classify(frame("#{as}/callbacks.rb"), frame("#{ar}/base.rb"), stat_flush)).to eq("pgbus_rails")
    end

    it "names any other rails leaf, or a generic leaf with no owner, aj_other" do
      expect(classify(frame("#{aj}/core.rb"), executor_frame)).to eq("aj_other")
      expect(classify(frame("#{as}/callbacks.rb"), frame("<internal:kernel>"))).to eq("aj_other")
    end
  end

  describe ".rails_sub_pair" do
    it "yields a sub-bucket for running rails samples only" do
      rails_stack = [frame("/gems/activejob-8.1.4/lib/active_job/log_subscriber.rb")]
      pgbus_stack = [frame("/src/lib/pgbus/client.rb")]
      samples = [[rails_stack, :running, 3], [rails_stack, :idle, 5], [pgbus_stack, :running, 7]]

      pairs = samples.filter_map { |frames, category, weight| described_class.rails_sub_pair(frames, category, weight) }

      expect(pairs).to eq([["aj_log", 3]])
    end
  end

  describe ".rails_sub_table" do
    let(:cell) do
      { "role" => "worker", "location" => "local", "jit" => "yjit",
        "cpu_shares" => { "pgbus" => 0.35, "driver" => 0.25, "rails" => 0.4, "ruby" => 0.0 },
        "rails_sub_shares" => described_class::RAILS_SUB_BUCKETS.to_h { |b| [b, 0.0] }
                                                                .merge("aj_log" => 0.25, "aj_other" => 0.75) }
    end

    it "renders each sub-bucket as a share of rails and of on-CPU" do
      lines = described_class.rails_sub_table([cell])

      expect(lines.first).to eq("| cell | #{described_class::RAILS_SUB_BUCKETS.join(" | ")} |")
      expect(lines.last).to start_with("| worker / local / yjit | 25.0% / 10.0% |")
      expect(lines.last).to include("| 75.0% / 30.0% |")
    end

    it "skips a cell without sub-shares (a consumer, or a fleet row)" do
      expect(described_class.rails_sub_table([cell.merge("rails_sub_shares" => nil)]).size).to eq(2)
    end
  end

  describe ".aj_logs" do
    it "is default when unset, else the mode" do
      expect(described_class.aj_logs({})).to eq("default")
      %w[default warn quiet].each do |mode|
        expect(described_class.aj_logs("WP_BENCH_AJ_LOGS" => mode)).to eq(mode)
      end
    end

    it "rejects anything else" do
      expect { described_class.aj_logs("WP_BENCH_AJ_LOGS" => "off") }
        .to raise_error(ArgumentError, /WP_BENCH_AJ_LOGS must be default, warn or quiet \(got off\)/)
    end
  end

  describe ".rails_app?" do
    it "is on for 1 and off when unset or 0" do
      expect(described_class.rails_app?("WP_BENCH_RAILS_APP" => "1")).to be(true)
      expect(described_class.rails_app?("WP_BENCH_RAILS_APP" => "0")).to be(false)
      expect(described_class.rails_app?({})).to be(false)
    end

    it "rejects anything else" do
      expect { described_class.rails_app?("WP_BENCH_RAILS_APP" => "yes") }
        .to raise_error(ArgumentError, /WP_BENCH_RAILS_APP must be 0 or 1/)
    end
  end

  describe ".jit_label" do
    it "reports yjit when YJIT is enabled" do
      stub_const("RubyVM::YJIT", Module.new { def self.enabled? = true })
      expect(described_class.jit_label).to eq("yjit")
    end

    it "reports none when YJIT is present but disabled and ZJIT is absent" do
      stub_const("RubyVM::YJIT", Module.new { def self.enabled? = false })
      hide_const("RubyVM::ZJIT")
      expect(described_class.jit_label).to eq("none")
    end
  end
end
