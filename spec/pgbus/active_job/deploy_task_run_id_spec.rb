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

  it "never serializes the transient enqueued flag" do
    job = job_class.new
    job.deploy_task_run_id = 5
    job.deploy_task_run_enqueued = true

    data = job.serialize

    expect(data.keys.grep(/deploy_task_run/)).to eq(["pgbus_deploy_task_run_id"])
    expect(ActiveJob::Base.deserialize(data).deploy_task_run_enqueued).to be_nil
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

  # Issue #662: every ActiveJob discard (discard_on, a retry_on that gives
  # up) goes through run_after_discard_procs; the mixin marks the instance
  # there, so the executor can stamp failed_at for a job that returned
  # normally without doing its work.
  describe "#pgbus_discarded?" do
    let(:calls) { [] }

    def discarding_class(&body)
      log = calls
      Class.new(ActiveJob::Base) do
        include Pgbus::ActiveJob::DeployTaskRunId

        self.queue_adapter = :test
        def self.name = "DeployTaskRunIdSpec::Job"
        define_method(:calls) { log }
        class_eval(&body)
      end
    end

    around do |example|
      saved = ActiveJob::Base.logger
      ActiveJob::Base.logger = Logger.new(IO::NULL)
      example.run
    ensure
      ActiveJob::Base.logger = saved
    end

    it "is false for a job that performed" do
      job = job_class.new
      job.perform_now

      expect(job.pgbus_discarded?).to be(false)
    end

    it "is true after discard_on without a block" do
      job = discarding_class do
        discard_on ArgumentError
        def perform = raise(ArgumentError, "bad row")
      end.new

      job.perform_now

      expect(job.pgbus_discarded?).to be(true)
    end

    it "is true after discard_on with a block, which still runs" do
      job = discarding_class do
        discard_on(ArgumentError) { |j, error| j.calls << [:block, error.message] }
        def perform = raise(ArgumentError, "bad row")
      end.new

      job.perform_now

      expect(job.pgbus_discarded?).to be(true)
      expect(calls).to eq([[:block, "bad row"]])
    end

    it "is true once retry_on with a block gives up, and false on an attempt whose retry was queued" do
      klass = discarding_class do
        retry_on(RuntimeError, wait: 0, attempts: 2) { |j, _error| j.calls << :gave_up }
        def perform = raise("not yet")
      end

      first = klass.new
      first.perform_now
      expect(first.pgbus_discarded?).to be(false)
      expect(calls).to be_empty

      last = klass.new.tap { |job| job.deserialize(first.serialize) }
      last.perform_now
      expect(last.pgbus_discarded?).to be(true)
      expect(calls).to eq([:gave_up])
    end

    it "re-raises when retry_on without a block gives up, as before" do
      klass = discarding_class do
        retry_on RuntimeError, wait: 0, attempts: 1
        def perform = raise("not yet")
      end
      job = klass.new

      expect { job.perform_now }.to raise_error(RuntimeError, "not yet")
    end

    it "runs the job's own after_discard procs once each, in order, a subclass's included" do
      parent = discarding_class do
        discard_on ArgumentError
        after_discard { |j, _error| j.calls << :parent }
        def perform = raise(ArgumentError, "bad row")
      end
      child = Class.new(parent) do
        def self.name = "DeployTaskRunIdSpec::Job"
        after_discard { |j, _error| j.calls << :child }
      end
      job = child.new

      job.perform_now

      expect(job.pgbus_discarded?).to be(true)
      expect(calls).to eq(%i[parent child])
    end

    it "is false for a job that rescues inside perform and returns" do
      job = discarding_class do
        discard_on ArgumentError
        define_method(:perform) do
          raise ArgumentError, "bad row"
        rescue ArgumentError
          calls << :rescued
        end
      end.new

      job.perform_now

      expect(job.pgbus_discarded?).to be(false)
      expect(calls).to eq([:rescued])
    end

    it "marks a single extended instance of a class that does not include it" do
      klass = Class.new(ActiveJob::Base) do
        self.queue_adapter = :test
        def self.name = "DeployTaskRunIdSpec::Job"
        discard_on ArgumentError
        def perform = raise(ArgumentError, "bad row")
      end
      job = klass.new.extend(described_class)

      job.perform_now

      expect(job.pgbus_discarded?).to be(true)
    end

    it "overrides run_after_discard_procs privately, as ActiveJob declares it" do
      expect(described_class.private_instance_methods(false)).to include(:run_after_discard_procs)
      expect(job_class.new).not_to respond_to(:run_after_discard_procs)
    end
  end
end
