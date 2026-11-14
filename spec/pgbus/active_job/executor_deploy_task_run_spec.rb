# frozen_string_literal: true

require "spec_helper"
require "json"
require "active_job"

# Issue #660: a deploy task's job carries its run's id in the payload; the
# executor stamps that run's outcome where it signals a batch.
RSpec.describe Pgbus::ActiveJob::Executor, "#execute with a deploy task run id" do
  subject(:executor) { described_class.new(client: mock_client, config: config) }

  let(:mock_client) { build_mock_client }
  let(:config) { Pgbus.configuration }
  let(:queue_name) { "default" }
  let(:job_id) { SecureRandom.uuid }
  let(:payload) do
    { "job_class" => "TestJob", "job_id" => job_id, "queue_name" => queue_name, "arguments" => [],
      "pgbus_deploy_task_run_id" => 31 }
  end
  let(:job_double) { build_job_double(job_class: "TestJob", queue_name: queue_name, job_id: job_id) }

  def message(read_ct: 1, body: payload)
    build_message_double(msg_id: 5, message: JSON.generate(body), read_ct: read_ct)
  end

  before do
    allow(ActiveJob::Base).to receive(:deserialize).and_return(job_double)
    allow(job_double).to receive(:successfully_enqueued?).and_return(nil)
    allow(Pgbus::Concurrency).to receive(:extract_key).and_return(nil)
    stub_const("Pgbus::JobStat", Class.new)
    allow(Pgbus::JobStat).to receive_messages(record!: nil, table_exists?: true)
    allow(Pgbus::FailedEventRecorder).to receive_messages(record!: nil, clear!: nil, last_error: nil)
    allow(Pgbus::Deploy::Schema).to receive(:outcomes?).and_return(true)
    allow(Pgbus::DeployTaskRun).to receive_messages(mark_completed!: true, mark_failed!: true)
    allow(Pgbus::ErrorReporter).to receive(:report)
  end

  it "stamps completed_at on success" do
    expect(executor.execute(message, queue_name)).to eq(:success)

    expect(Pgbus::DeployTaskRun).to have_received(:mark_completed!).with(31)
    expect(Pgbus::DeployTaskRun).not_to have_received(:mark_failed!)
  end

  it "stamps nothing when the attempt re-enqueued itself (retry_on), then stamps on the final success" do
    allow(job_double).to receive(:successfully_enqueued?).and_return(true)
    expect(executor.execute(message, queue_name)).to eq(:success)
    expect(Pgbus::DeployTaskRun).not_to have_received(:mark_completed!)

    allow(job_double).to receive(:successfully_enqueued?).and_return(nil)
    executor.execute(message, queue_name)

    expect(Pgbus::DeployTaskRun).to have_received(:mark_completed!).with(31).once
  end

  it "stamps nothing when perform raises" do
    allow(job_double).to receive(:perform_now).and_raise(StandardError, "boom")
    allow(Pgbus::ErrorReporter).to receive(:report)

    expect(executor.execute(message, queue_name)).to eq(:failed)
    expect(Pgbus::DeployTaskRun).not_to have_received(:mark_completed!)
    expect(Pgbus::DeployTaskRun).not_to have_received(:mark_failed!)
  end

  it "stamps failed_at on dead-letter" do
    allow(mock_client).to receive(:move_to_dead_letter)

    expect(executor.execute(message(read_ct: config.max_retries + 1), queue_name)).to eq(:dead_lettered)
    expect(Pgbus::DeployTaskRun).to have_received(:mark_failed!).with(31)
  end

  it "stamps failed_at when a :while_executing duplicate is discarded" do
    body = payload.merge("pgbus_uniqueness_key" => "TestJob:1", "pgbus_uniqueness_strategy" => "while_executing",
                         "pgbus_uniqueness_on_conflict" => "discard")
    allow(Pgbus::Uniqueness).to receive(:acquire_execution_lock).and_return(false)

    expect(executor.execute(message(body: body), queue_name)).to eq(:discarded)
    expect(Pgbus::DeployTaskRun).to have_received(:mark_failed!).with(31)
  end

  it "reports a failing outcome write and still completes the job" do
    allow(Pgbus::DeployTaskRun).to receive(:mark_completed!).and_raise(StandardError, "db blip")

    expect(executor.execute(message, queue_name)).to eq(:success)
    expect(mock_client).to have_received(:archive_message)
    expect(Pgbus::ErrorReporter).to have_received(:report)
      .with(kind_of(StandardError), hash_including(action: "deploy_task_run_outcome", deploy_task_run_id: 31))
  end

  it "writes nothing before the outcome columns are migrated" do
    allow(Pgbus::Deploy::Schema).to receive(:outcomes?).and_return(false)

    executor.execute(message, queue_name)

    expect(Pgbus::DeployTaskRun).not_to have_received(:mark_completed!)
  end

  it "neither probes the schema nor asks the job about a retry for an unmarked job" do
    body = payload.except("pgbus_deploy_task_run_id")

    expect(executor.execute(message(body: body), queue_name)).to eq(:success)
    expect(Pgbus::Deploy::Schema).not_to have_received(:outcomes?)
    expect(job_double).not_to have_received(:successfully_enqueued?)
    expect(Pgbus::DeployTaskRun).not_to have_received(:mark_completed!)
  end
end
