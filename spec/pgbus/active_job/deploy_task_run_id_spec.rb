# frozen_string_literal: true

require "spec_helper"
require "active_job"

RSpec.describe Pgbus::ActiveJob::DeployTaskRunId do
  let(:job_class) do
    Class.new(ActiveJob::Base) do
      include Pgbus::ActiveJob::DeployTaskRunId

      def self.name = "DeployTaskRunIdSpec::Job"
      def perform(*); end
    end
  end
  let(:plain_class) do
    Class.new(ActiveJob::Base) do
      def self.name = "DeployTaskRunIdSpec::Job"
      def perform(*); end
    end
  end

  before { stub_const("DeployTaskRunIdSpec::Job", job_class) }

  it "leaves an unmarked job's payload byte-for-byte unchanged" do
    job = job_class.new(1, "a")
    plain = plain_class.new(1, "a")
    plain.job_id = job.job_id
    allow(Time).to receive(:now).and_return(Time.utc(2026, 11, 1)) # serialize stamps enqueued_at

    expect(JSON.generate(job.serialize)).to eq(JSON.generate(plain.serialize))
    expect(job.serialize).not_to have_key(described_class::METADATA_KEY)
  end

  it "carries the run id under pgbus_deploy_task_run_id and round-trips it" do
    job = job_class.new
    job.deploy_task_run_id = 42

    data = job.serialize
    restored = ActiveJob::Base.deserialize(data)

    expect(data["pgbus_deploy_task_run_id"]).to eq(42)
    expect(restored.deploy_task_run_id).to eq(42)
  end

  it "marks a single instance of a class that does not include it" do
    job = plain_class.new.extend(described_class)
    job.deploy_task_run_id = 7

    expect(job.serialize).to include("pgbus_deploy_task_run_id" => 7)
  end

  it "keeps the id on a retry_on re-enqueue" do
    job = job_class.new
    job.deploy_task_run_id = 9
    retried = ActiveJob::Base.deserialize(job.serialize)

    expect(retried.serialize).to include("pgbus_deploy_task_run_id" => 9)
  end
end
