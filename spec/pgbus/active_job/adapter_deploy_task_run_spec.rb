# frozen_string_literal: true

require "spec_helper"
require "active_job"

# Issue #660: the adapter tells a marked deploy task job whether its enqueue
# left a live message, so a dropped enqueue or retry is not taken for one.
RSpec.describe Pgbus::ActiveJob::Adapter, "#enqueue of a deploy task's job" do
  subject(:adapter) { described_class.new }

  let(:mock_client) { build_mock_client }
  let(:on_conflict) { :block }
  let(:job_class) do
    conflict = on_conflict
    Class.new(ActiveJob::Base) do
      include Pgbus::Concurrency
      include Pgbus::ActiveJob::DeployTaskRunId

      def self.name = "AdapterDeployTaskRunSpec::Job"
      def perform(*) = nil

      limits_concurrency to: 1, key: ->(*) { "deploy-run-k" }, on_conflict: conflict
    end
  end
  let(:plain_class) do
    Class.new(ActiveJob::Base) do
      include Pgbus::ActiveJob::DeployTaskRunId

      def self.name = "AdapterDeployTaskRunSpec::Plain"
      def perform(*) = nil
    end
  end

  def marked(klass = job_class)
    klass.new.tap { |job| job.deploy_task_run_id = 7 }
  end

  before do
    stub_const("AdapterDeployTaskRunSpec::Job", job_class)
    stub_const("AdapterDeployTaskRunSpec::Plain", plain_class)
    stub_bus_record_pool
    allow(Pgbus).to receive(:client).and_return(mock_client)
    allow(mock_client).to receive(:send_message).and_return(42)
    allow(Pgbus::Semaphore).to receive(:transaction).and_yield
    allow(Pgbus::Concurrency::Semaphore).to receive(:acquire).and_return(:acquired)
    allow(Pgbus::Concurrency::BlockedExecution).to receive(:insert)
    allow(Pgbus.logger).to receive(:info)
  end

  it "marks a sent job as enqueued, on enqueue and on enqueue_at" do
    job = marked(plain_class)
    adapter.enqueue(job)
    expect(job.deploy_task_run_enqueued).to be(true)

    later = marked(plain_class)
    adapter.enqueue_at(later, Time.current.to_f + 60)
    expect(later.deploy_task_run_enqueued).to be(true)
  end

  it "marks a job parked behind its concurrency limit as enqueued: it runs later" do
    allow(Pgbus::Concurrency::Semaphore).to receive(:acquire).and_return(:blocked)
    job = marked

    adapter.enqueue(job)

    expect(job.deploy_task_run_enqueued).to be(true)
    expect(Pgbus::Concurrency::BlockedExecution).to have_received(:insert)
  end

  context "with on_conflict: :discard" do
    let(:on_conflict) { :discard }

    it "marks a job its concurrency limit dropped as not enqueued" do
      allow(Pgbus::Concurrency::Semaphore).to receive(:acquire).and_return(:blocked)
      job = marked

      adapter.enqueue(job)

      expect(job.deploy_task_run_enqueued).to be(false)
      expect(mock_client).not_to have_received(:send_message)
    end
  end

  it "marks a job a uniqueness on_conflict dropped as not enqueued" do
    allow(adapter).to receive(:uniqueness_rejected?).and_return(true)
    job = marked(plain_class)

    adapter.enqueue(job)

    expect(job.deploy_task_run_enqueued).to be(false)
  end

  it "leaves an unmarked job alone" do
    job = plain_class.new

    adapter.enqueue(job)

    expect(job.deploy_task_run_enqueued).to be_nil
  end
end
