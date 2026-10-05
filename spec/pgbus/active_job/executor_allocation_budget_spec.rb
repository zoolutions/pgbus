# frozen_string_literal: true

require "spec_helper"
require "memory_profiler"
require "active_job"

# Issue #484: a hard allocation gate for Executor#execute, like the Client
# budgets in spec/pgbus/allocation_budget_spec.rb. Only objects allocated from
# pgbus's own files count: JSON, ActiveJob and ActiveSupport allocate the bulk
# of a job's objects and vary across the Rails/Ruby CI matrix, so a total
# budget would gate on someone else's code.
RSpec.describe Pgbus::ActiveJob::Executor do
  let(:job_class) do
    Class.new(ActiveJob::Base) do
      self.queue_adapter = :inline
      def self.name = "ExecutorAllocationBudgetJob"
      def perform(*); end
    end
  end

  let(:client) { build_mock_client }
  # Own config + an info-level logger: other specs leave the global logger at
  # debug, which would build every debug line and measure the wrong thing.
  let(:config) { Pgbus::Configuration.new.tap { |c| c.stats_enabled = true } }
  let(:executor) { described_class.new(client: client, config: config, stat_buffer: []) }
  # msg_id / read_ct are Strings, as pgmq-ruby returns them from PG.
  let(:message) do
    Struct.new(:msg_id, :message, :read_ct, :headers, :enqueued_at)
          .new("1", JSON.generate(job_class.new(42).serialize), "1", nil, nil)
  end

  around do |example|
    previous_logger = Pgbus.configuration.logger
    Pgbus.configuration.logger = Logger.new(IO::NULL, level: :info)
    example.run
  ensure
    Pgbus.configuration.logger = previous_logger
    Pgbus::VisibilityHeartbeat.stop
  end

  before do
    ActiveJob::Base.logger = Logger.new(IO::NULL)
    stub_const("ExecutorAllocationBudgetJob", job_class)
    allow(client).to receive(:archive_message).and_return(true)
  end

  def pgbus_allocations(&)
    pgbus_lib = File.expand_path("../../../lib/pgbus", __dir__)
    report = MemoryProfiler.report(&)
    report.allocated_objects_by_file.select { |row| row[:data].start_with?(pgbus_lib) }.sum { |row| row[:count] }
  end

  # Rails hidden: with the dummy app loaded the executor wraps perform in
  # Rails.application.executor, whose allocations depend on the app, not pgbus.
  it "allocates fewer than 10 pgbus-owned objects per successful job" do
    hide_const("Rails")
    # Measure the success path: a regression into the failure path must fail
    # here, not be measured silently.
    expect(executor.execute(message, "default")).to eq(:success)
    4.times { executor.execute(message, "default") }

    per_job = pgbus_allocations { 10.times { executor.execute(message, "default") } } / 10.0

    expect(per_job).to be < 10
  end

  # The trim itself, independent of Rails and the CI matrix: at info level the
  # debug tag ("msg_id=… queue=… read_ct=…") is never built.
  it "does not build the debug log tag at info level" do
    5.times { executor.execute(message, "default") }

    report = MemoryProfiler.report { executor.execute(message, "default") }
    tags = report.strings_allocated.map(&:first).grep(/\Amsg_id=/)

    expect(tags).to be_empty
  end
end
