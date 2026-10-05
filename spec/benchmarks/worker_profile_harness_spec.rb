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
