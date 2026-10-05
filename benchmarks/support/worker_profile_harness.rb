# frozen_string_literal: true

require "json"

# Worker profiling harness (issue #484).
#
# Answers "where does a job's wall time go?" for a REAL Pgbus::Process::Worker
# draining a real PGMQ queue, not an executor called in a loop. It runs the
# drain twice per cell: once unprofiled for an honest jobs/s, once under
# vernier for the split. Vernier is used (not stackprof) because the job work
# happens on the Concurrent::FixedThreadPool threads while the worker's main
# loop sits in a wait; vernier samples every thread and tags each sample as
# running, idle (GVL released: blocked on a socket or a condition variable) or
# stalled (runnable but waiting for the GVL).
#
# Two thread groups are split separately so neither swamps the other: the
# job-pool threads ("worker-N") and the worker's main loop thread (the one that
# reads batches and hands them to the pool). Each sample lands in one bucket:
#
#   db_wait    idle inside libpq (via pgmq-ruby or ActiveRecord) — waiting on Postgres
#   pool_wait  idle waiting to check out a connection (connection_pool or AR pool)
#   idle       idle anywhere else — a pool thread waiting for work, the loop's wake wait
#   gvl_wait   runnable but waiting for the GVL (often right after libpq returns)
#   pgbus      running; the deepest non-stdlib frame is pgbus code
#   driver     running inside pg / pgmq-ruby / connection_pool code
#   rails      running inside ActiveJob / ActiveSupport / ActiveRecord / GlobalID
#   ruby       running in stdlib, concurrent-ruby or the bench's own job
#
# The pure pieces (classification, share math, JIT label) carry no DB or
# profiler dependency and are pinned by spec/benchmarks/worker_profile_harness_spec.rb.
module WorkerProfileHarness
  BUCKETS = %i[db_wait pool_wait idle gvl_wait pgbus driver rails ruby].freeze
  # The on-CPU buckets: "of the time some thread held the GVL, whose code ran?"
  # Under the GVL a worker process runs at most one core of Ruby, so when
  # CPU/wall nears 100% this composition — not the per-thread wall split — says
  # what a CPU saving would buy.
  CPU_BUCKETS = %i[pgbus driver rails ruby].freeze
  ANY_THREAD = //

  DRIVER_PATTERNS = %w[/pg- /pgmq-ruby- /connection_pool-].freeze
  RAILS_PATTERNS = %w[/activejob- /activesupport- /activerecord- /activemodel- /railties- /globalid-].freeze
  PGBUS_PATTERN = "/lib/pgbus/"
  # libpq calls are C-function frames with no source path; their label is the tell.
  DRIVER_LABEL_PREFIX = "PG::"
  POOL_WAIT_LABELS = ["ConnectionPool::TimedStack#pop", "ConnectionPool::Queue#wait_poll"].freeze

  # vernier's per-sample category ids (Vernier::Output::Firefox::Thread::SAMPLE_CATEGORY_NAMES).
  SAMPLE_CATEGORIES = { 1 => :idle, 2 => :stalled }.freeze

  POOL_THREAD_NAME = /\Aworker-\d+\z/
  LOOP_THREAD_NAME = "pgbus-bench-worker-loop"

  Frame = Struct.new(:filename, :label, keyword_init: true)

  Cell = Struct.new(:role, :location, :jit, :jobs, :wall_s, :cpu_s, :gc_s, :jobs_per_s, :pool_shares, :loop_shares,
                    :cpu_shares,
                    :profile_path, keyword_init: true) do
    def to_h = super.transform_keys(&:to_s)
  end

  module_function

  def classify_frame(frame)
    filename = frame.filename
    return :driver if frame.label.to_s.start_with?(DRIVER_LABEL_PREFIX)
    return :ruby if filename.nil?
    return :pgbus if filename.include?(PGBUS_PATTERN)
    return :driver if DRIVER_PATTERNS.any? { |p| filename.include?(p) }
    return :rails if RAILS_PATTERNS.any? { |p| filename.include?(p) }

    :ruby
  end

  # frames: leaf first, root last; each responds to #filename and #label.
  def classify_sample(frames, category:)
    case category
    when :idle
      return :pool_wait if frames.any? { |f| pool_wait_frame?(f) }

      frames.any? { |f| classify_frame(f) == :driver } ? :db_wait : :idle
    when :stalled
      :gvl_wait
    else
      frames.each do |f|
        bucket = classify_frame(f)
        return bucket unless bucket == :ruby
      end
      :ruby
    end
  end

  def pool_wait_frame?(frame)
    label = frame.label.to_s
    POOL_WAIT_LABELS.any? { |l| label.end_with?(l) }
  end

  def sample_category(id)
    SAMPLE_CATEGORIES.fetch(id, :running)
  end

  # pairs: enumerable of [bucket, weight]. Returns every bucket in `buckets`
  # order; pairs outside `buckets` are dropped before normalising.
  def shares(pairs, buckets: BUCKETS)
    totals = buckets.to_h { |b| [b, 0.0] }
    pairs.each { |bucket, weight| totals[bucket] += weight if totals.key?(bucket) }
    sum = totals.values.sum
    return totals if sum.zero?

    totals.transform_values { |v| v / sum }
  end

  def jit_label
    return "yjit" if defined?(RubyVM::YJIT) && RubyVM::YJIT.enabled?
    return "zjit" if defined?(RubyVM::ZJIT) && RubyVM::ZJIT.enabled?

    "none"
  end

  # Walks a Vernier::Result and yields [bucket, weight] for every sample taken
  # on a thread whose name matches `thread_name` (a Regexp or an exact String).
  def bucket_pairs(result, thread_name)
    return enum_for(__method__, result, thread_name) unless block_given?

    result.threads.each_value do |thread|
      next unless thread_name === thread[:name].to_s # rubocop:disable Style/CaseEquality

      categories = thread[:sample_categories] || []
      thread[:samples].each_with_index do |stack_idx, i|
        frames = result.stack(stack_idx).frames
        yield classify_sample(frames, category: sample_category(categories[i])), thread[:weights][i]
      end
    end
  end

  def pool_shares(result) = shares(bucket_pairs(result, POOL_THREAD_NAME))
  def loop_shares(result) = shares(bucket_pairs(result, LOOP_THREAD_NAME))
  def cpu_shares(result) = shares(bucket_pairs(result, ANY_THREAD), buckets: CPU_BUCKETS)
end
