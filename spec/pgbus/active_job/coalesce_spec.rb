# frozen_string_literal: true

require "spec_helper"
require "active_job"
require "active_support/testing/time_helpers"

# Issue #682: Pgbus.coalesce holds a unit of work's bulk-eligible enqueues at
# the adapter and sends them together when the block ends: one send_batch per
# (queue, priority) group, each job not sent by it retried on its own, and
# whatever still fails handed to on_failure.
RSpec.describe Pgbus::ActiveJob::Coalesce do
  include ActiveSupport::Testing::TimeHelpers

  let(:adapter) { Pgbus::ActiveJob::Adapter.new }
  let(:mock_client) { build_mock_client }
  let(:next_id) { [100] }

  let(:plain_class) { job_class("CoalesceSpec::PlainJob") }

  def job_class(name, &body)
    the_adapter = adapter
    Class.new(ActiveJob::Base) do
      include Pgbus::ActiveJob::BatchId

      self.queue_adapter = the_adapter
      define_singleton_method(:name) { name }
      def perform(*) = nil
      class_eval(&body) if body
    end
  end

  def take_id
    next_id[0] += 1
  end

  before do
    ActiveJob::Base.logger = Logger.new(IO::NULL)
    stub_const("CoalesceSpec", Module.new)
    stub_const("CoalesceSpec::PlainJob", plain_class)
    stub_bus_record_pool
    allow(Pgbus).to receive(:client).and_return(mock_client)
    allow(mock_client).to receive(:send_message) { take_id }
    allow(mock_client).to receive(:send_batch) { |_queue, payloads, **| payloads.map { take_id } }
  end

  after do
    Thread.current[:pgbus_coalesce] = nil
    Thread.current[:pgbus_batch_id] = nil
  end

  describe "buffering" do
    it "sends three perform_later calls as one send_batch when the block ends" do
      jobs = nil
      Pgbus.coalesce do
        jobs = Array.new(3) { |i| plain_class.perform_later("e#{i}") }

        expect(mock_client).not_to have_received(:send_batch)
      end

      expect(mock_client).to have_received(:send_batch).once
      expect(mock_client).not_to have_received(:send_message)
      expect(jobs.map(&:provider_job_id)).to eq([101, 102, 103])
      expect(jobs).to all(be_successfully_enqueued)
    end

    it "returns the job from perform_later, optimistic, with no provider_job_id until the flush" do
      Pgbus.coalesce do
        job = plain_class.perform_later("a")

        expect(job).to be_a(plain_class)
        expect(job.successfully_enqueued?).to be(true)
        expect(job.provider_job_id).to be_nil
      end
    end

    it "returns the block's value" do
      expect(Pgbus.coalesce { :done }).to eq(:done)
    end

    it "sends one send_batch per queue and priority" do
      Pgbus.coalesce do
        plain_class.set(queue: "a").perform_later(1)
        plain_class.set(queue: "b").perform_later(2)
        plain_class.set(queue: "a", priority: 1).perform_later(3)
        plain_class.set(queue: "a").perform_later(4)
      end

      expect(mock_client).to have_received(:send_batch).exactly(3).times
      expect(mock_client).to have_received(:send_batch).with("a", satisfy { |p| p.size == 2 }, priority: nil)
    end

    it "sends right away outside a scope" do
      plain_class.perform_later("a")

      expect(mock_client).to have_received(:send_message).once
      expect(mock_client).not_to have_received(:send_batch)
    end

    it "clears the scope after the block, so a later enqueue goes out at once" do
      Pgbus.coalesce { plain_class.perform_later(1) }
      plain_class.perform_later(2)

      expect(described_class.current).to be_nil
      expect(mock_client).to have_received(:send_message).once
    end

    it "sends nothing and emits no batch for an empty block" do
      Pgbus.coalesce { nil }

      expect(mock_client).not_to have_received(:send_batch)
    end
  end

  describe "what bypasses the buffer" do
    before do
      allow(Pgbus::Semaphore).to receive(:transaction).and_yield
      allow(Pgbus::Concurrency::Semaphore).to receive(:acquire).and_return(:acquired)
      allow(Pgbus::UniquenessKey).to receive_messages(acquire!: true, bind!: nil)
    end

    it "sends a job with uniqueness at once" do
      unique = job_class("CoalesceSpec::UniqueJob") do
        include Pgbus::Uniqueness

        ensures_uniqueness strategy: :until_executing, key: ->(id) { "c-#{id}" }
      end
      stub_const("CoalesceSpec::UniqueJob", unique)

      Pgbus.coalesce do
        unique.perform_later(1)

        expect(mock_client).to have_received(:send_message).once
      end
      expect(mock_client).not_to have_received(:send_batch)
    end

    it "sends a job with a concurrency limit at once" do
      limited = job_class("CoalesceSpec::LimitedJob") do
        include Pgbus::Concurrency

        limits_concurrency to: 1, key: ->(*) { "c-limit" }
      end
      stub_const("CoalesceSpec::LimitedJob", limited)

      Pgbus.coalesce do
        limited.perform_later

        expect(mock_client).to have_received(:send_message).once
      end
    end

    it "holds a future set(wait:) job and sends it at the flush with the delay left then (issue #686)" do
      Pgbus.coalesce do
        plain_class.set(wait: 5.minutes).perform_later(1)

        expect(mock_client).not_to have_received(:send_batch)
        travel 2.minutes
      end

      expect(mock_client).to have_received(:send_batch).with("default", anything, delay: a_value_within(1).of(180), priority: nil)
      expect(mock_client).not_to have_received(:send_message)
    ensure
      travel_back
    end

    it "sends a retry re-enqueue (executions > 0) at once" do
      Pgbus.coalesce do
        job = plain_class.new(1)
        job.executions = 1
        job.enqueue

        expect(mock_client).to have_received(:send_message).once
      end
    end

    it "sends a deploy task run's job at once, so the runner reads its enqueue outcome" do
      deploy = job_class("CoalesceSpec::DeployJob") { include Pgbus::ActiveJob::DeployTaskRunId }
      stub_const("CoalesceSpec::DeployJob", deploy)

      Pgbus.coalesce do
        job = deploy.new
        job.deploy_task_run_id = 7
        job.enqueue

        expect(mock_client).to have_received(:send_message).once
        expect(job.deploy_task_run_enqueued).to be(true)
      end
    end

    it "sends a batch member at once, counted into its batch" do
      allow(Pgbus::Batch).to receive(:track_enqueue)
      Thread.current[:pgbus_batch_id] = "open-batch"

      Pgbus.coalesce do
        plain_class.perform_later(1)

        expect(Pgbus::Batch).to have_received(:track_enqueue).once
        expect(mock_client).to have_received(:send_message)
          .with("default", hash_including(Pgbus::Batch::METADATA_KEY => "open-batch"), delay: 0, priority: nil)
      end
    end

    it "sends a job added to a batch (enqueue_batch_id) at once" do
      allow(Pgbus::Batch).to receive(:track_enqueue)

      Pgbus.coalesce do
        job = plain_class.new(1)
        Pgbus::ActiveJob::BatchId.capture([job], "added-batch")
        adapter.enqueue(job)

        expect(mock_client).to have_received(:send_message).once
      end
    end

    it "sends a workflow step job at once, keeping its own enqueue-failure contract" do
      Pgbus.coalesce do
        adapter.enqueue(Pgbus::Workflow::StepJob.new(SecureRandom.uuid))

        expect(mock_client).to have_received(:send_message).once
      end
    end

    it "sends a job enqueued on another thread at once" do
      Pgbus.coalesce do
        Thread.new { plain_class.perform_later(1) }.join # rubocop:disable ThreadSafety/NewThread -- joined here

        expect(mock_client).to have_received(:send_message).once
      end
      expect(mock_client).not_to have_received(:send_batch)
    end

    it "sends at once a job pushed after the flush has started" do
      pushed = []
      scope = nil
      allow(mock_client).to receive(:send_batch) do |_queue, payloads, **|
        pushed << scope.push(adapter, plain_class.new(:direct), nil)
        plain_class.perform_later(:late)
        payloads.map { take_id }
      end

      Pgbus.coalesce do
        scope = described_class.current
        plain_class.perform_later(1)
      end

      expect(pushed).to eq([false])
      expect(mock_client).to have_received(:send_batch).once
      expect(mock_client).to have_received(:send_message).once
    end
  end

  describe "a send that leaves jobs unsent" do
    before do
      allow(mock_client).to receive(:send_batch).and_return([])
    end

    it "retries each job the batch did not send on its own" do
      jobs = nil
      Pgbus.coalesce { jobs = Array.new(2) { |i| plain_class.perform_later(i) } }

      expect(mock_client).to have_received(:send_message).twice
      expect(jobs).to all(be_successfully_enqueued)
      expect(jobs.map(&:provider_job_id)).to eq([101, 102])
    end

    it "retries the jobs on their own when send_batch raises outright" do
      allow(mock_client).to receive(:send_batch).and_raise(IOError, "gone")
      jobs = nil

      Pgbus.coalesce { jobs = Array.new(2) { |i| plain_class.perform_later(i) } }

      expect(mock_client).to have_received(:send_message).twice
      expect(jobs).to all(be_successfully_enqueued)
    end

    context "when the retry fails too" do
      before { allow(mock_client).to receive(:send_message).and_raise(IOError, "still gone") }

      it "raises Pgbus::EnqueueError naming the job by default" do
        job = nil
        expect do
          Pgbus.coalesce { job = plain_class.perform_later(1) }
        end.to raise_error(Pgbus::EnqueueError, /CoalesceSpec::PlainJob/)

        expect(job.successfully_enqueued?).to be(false)
      end

      it "names the job class and id in the error, with the send's error as its cause" do
        job = nil
        error = nil
        begin
          Pgbus.coalesce { job = plain_class.perform_later(1) }
        rescue Pgbus::EnqueueError => e
          error = e
        end

        expect(error.message).to include("CoalesceSpec::PlainJob (#{job.job_id})")
        expect(error.cause).to be_a(IOError)
      end

      it "reports it with the serialized payloads under :report and does not raise" do
        stub_const("Rails", Module.new)
        reporter = double("Rails.error")
        allow(Rails).to receive(:error).and_return(reporter)
        allow(reporter).to receive(:report)
        job = nil

        expect { Pgbus.coalesce(on_failure: :report) { job = plain_class.perform_later(1) } }.not_to raise_error

        expect(reporter).to have_received(:report).with(
          an_instance_of(Pgbus::EnqueueError),
          hash_including(context: { pgbus_coalesce: [hash_including("job_id" => job.job_id)] })
        )
      end

      it "reports through Pgbus::ErrorReporter under :report without Rails.error" do
        hide_const("Rails")
        allow(Pgbus::ErrorReporter).to receive(:report)

        expect { Pgbus.coalesce(on_failure: :report) { plain_class.perform_later(1) } }.not_to raise_error

        expect(Pgbus::ErrorReporter).to have_received(:report)
          .with(an_instance_of(Pgbus::EnqueueError), hash_including(:pgbus_coalesce))
      end

      it "hands the jobs and the error to a callable" do
        received = []
        job = nil

        Pgbus.coalesce(on_failure: ->(jobs, error) { received << [jobs, error] }) { job = plain_class.perform_later(1) }

        expect(received.size).to eq(1)
        expect(received.first[0]).to eq([job])
        expect(received.first[1]).to be_a(Pgbus::EnqueueError)
      end
    end
  end

  describe "a retry that returns without enqueueing the job" do
    it "fails it" do
      allow(mock_client).to receive_messages(send_batch: [], send_message: nil)
      failed = []

      Pgbus.coalesce(on_failure: ->(jobs, _error) { failed.concat(jobs) }) { plain_class.perform_later(1) }

      expect(failed.size).to eq(1)
    end
  end

  describe "#finish! with a failure and no error" do
    it "names the jobs without a cause" do
      job = plain_class.new(1)
      stub = instance_double(Pgbus::ActiveJob::Adapter)
      allow(stub).to receive(:deliver_coalesced).and_return(
        Pgbus::ActiveJob::Coalesce::Result.new(buffered: 1, sent: 0, retried: 0, failed: [job], error: nil)
      )
      scope = described_class.new
      scope.push(stub, job, job.serialize)

      expect { scope.finish! }.to raise_error(Pgbus::EnqueueError) { |e| expect(e.cause).to be_nil }
    end
  end

  describe "the block raising" do
    it "still flushes, then re-raises the block's error" do
      expect do
        Pgbus.coalesce do
          plain_class.perform_later(1)
          raise ArgumentError, "boom"
        end
      end.to raise_error(ArgumentError, "boom")

      expect(mock_client).to have_received(:send_batch).once
    end

    it "keeps the block's error and reports a failed flush instead of raising it" do
      allow(mock_client).to receive(:send_batch).and_raise(IOError, "gone")
      allow(mock_client).to receive(:send_message).and_raise(IOError, "gone")
      hide_const("Rails")
      allow(Pgbus::ErrorReporter).to receive(:report)

      expect do
        Pgbus.coalesce do
          plain_class.perform_later(1)
          raise ArgumentError, "boom"
        end
      end.to raise_error(ArgumentError, "boom")

      expect(Pgbus::ErrorReporter).to have_received(:report).with(an_instance_of(Pgbus::EnqueueError), anything)
    end
  end

  describe "the block raising, with a callable on_failure" do
    before do
      allow(mock_client).to receive(:send_batch).and_raise(IOError, "gone")
      allow(mock_client).to receive(:send_message).and_raise(IOError, "gone")
    end

    it "still hands the failed jobs to the callable, then re-raises the block's error" do
      received = []

      expect do
        Pgbus.coalesce(on_failure: ->(jobs, _error) { received.concat(jobs) }) do
          plain_class.perform_later(1)
          raise ArgumentError, "boom"
        end
      end.to raise_error(ArgumentError, "boom")

      expect(received.size).to eq(1)
    end

    it "reports a callable that raises, and keeps the block's error" do
      hide_const("Rails")
      allow(Pgbus::ErrorReporter).to receive(:report)

      expect do
        Pgbus.coalesce(on_failure: ->(*) { raise "callable broke" }) do
          plain_class.perform_later(1)
          raise ArgumentError, "boom"
        end
      end.to raise_error(ArgumentError, "boom")

      expect(Pgbus::ErrorReporter).to have_received(:report).with(an_instance_of(RuntimeError), anything)
    end
  end

  describe "serialization at push" do
    let(:sent) { [] }

    before do
      allow(mock_client).to receive(:send_batch) do |queue, payloads, **opts|
        sent << [queue, opts[:priority], payloads]
        payloads.map { take_id }
      end
    end

    it "raises an unserializable argument at perform_later, as outside a scope" do
      expect do
        Pgbus.coalesce { plain_class.perform_later(Object.new) }
      end.to raise_error(ActiveJob::SerializationError)

      expect(mock_client).not_to have_received(:send_batch)
    end

    it "keeps the block's error when an argument cannot be serialized" do
      expect do
        Pgbus.coalesce do
          plain_class.perform_later(1)
          begin
            plain_class.perform_later(Object.new)
          rescue ActiveJob::SerializationError
            nil
          end
          raise ArgumentError, "boom"
        end
      end.to raise_error(ArgumentError, "boom")

      expect(sent.size).to eq(1)
    end

    it "sends the arguments as they were at perform_later" do
      args = { "a" => 1 }

      Pgbus.coalesce do
        plain_class.perform_later(args)
        args["a"] = 2
      end

      expect(sent.first[2].first["arguments"].first).to include("a" => 1)
    end

    it "sends to the queue and priority the job had at perform_later" do
      Pgbus.coalesce do
        job = plain_class.set(queue: "first", priority: 2).perform_later(1)
        job.queue_name = "changed"
        job.priority = 9
      end

      expect(sent.map { |queue, priority, _| [queue, priority] }).to eq([["first", 2]])
    end

    it "reports the payloads captured at push, never serializing again" do
      allow(mock_client).to receive(:send_batch).and_raise(IOError, "gone")
      allow(mock_client).to receive(:send_message).and_raise(IOError, "gone")
      hide_const("Rails")
      allow(Pgbus::ErrorReporter).to receive(:report)
      job = nil

      Pgbus.coalesce(on_failure: :report) do
        job = plain_class.perform_later(1)
        allow(job).to receive(:serialize).and_raise("serialized again")
      end

      expect(Pgbus::ErrorReporter).to have_received(:report)
        .with(an_instance_of(Pgbus::EnqueueError), { pgbus_coalesce: [hash_including("job_id" => job.job_id)] })
    end
  end

  describe "a raising pgbus.coalesce subscriber" do
    it "keeps the block's error and reports the subscriber's" do
      hide_const("Rails")
      allow(Pgbus::ErrorReporter).to receive(:report)
      callback = ->(*) { raise "subscriber broke" }

      ActiveSupport::Notifications.subscribed(callback, "pgbus.coalesce") do
        expect do
          Pgbus.coalesce do
            plain_class.perform_later(1)
            raise ArgumentError, "boom"
          end
        end.to raise_error(ArgumentError, "boom")
      end

      expect(Pgbus::ErrorReporter).to have_received(:report).with(an_instance_of(RuntimeError), anything)
    end

    it "still hands the failed jobs to on_failure" do
      hide_const("Rails")
      allow(Pgbus::ErrorReporter).to receive(:report)
      allow(mock_client).to receive(:send_batch).and_raise(IOError, "gone")
      allow(mock_client).to receive(:send_message).and_raise(IOError, "gone")
      callback = ->(*) { raise "subscriber broke" }
      received = []

      ActiveSupport::Notifications.subscribed(callback, "pgbus.coalesce") do
        Pgbus.coalesce(on_failure: ->(jobs, _error) { received.concat(jobs) }) { plain_class.perform_later(1) }
      end

      expect(received.size).to eq(1)
    end

    it "still hands the failed jobs to on_failure when the block raised" do
      hide_const("Rails")
      allow(Pgbus::ErrorReporter).to receive(:report)
      allow(mock_client).to receive(:send_batch).and_raise(IOError, "gone")
      allow(mock_client).to receive(:send_message).and_raise(IOError, "gone")
      callback = ->(*) { raise "subscriber broke" }
      received = []

      ActiveSupport::Notifications.subscribed(callback, "pgbus.coalesce") do
        expect do
          Pgbus.coalesce(on_failure: ->(jobs, _error) { received.concat(jobs) }) do
            plain_class.perform_later(1)
            raise ArgumentError, "boom"
          end
        end.to raise_error(ArgumentError, "boom")
      end

      expect(received.size).to eq(1)
    end
  end

  describe "nesting" do
    it "joins the outer scope and flushes only at its end" do
      Pgbus.coalesce do
        Pgbus.coalesce { plain_class.perform_later(1) }

        expect(mock_client).not_to have_received(:send_batch)
        plain_class.perform_later(2)
      end

      expect(mock_client).to have_received(:send_batch).once.with("default", satisfy { |p| p.size == 2 }, priority: nil)
    end
  end

  describe "on_failure" do
    it "rejects an unknown mode before the block runs" do
      ran = false

      expect { Pgbus.coalesce(on_failure: :ignore) { ran = true } }.to raise_error(ArgumentError, /on_failure/)
      expect(ran).to be(false)
    end

    it "requires a block" do
      expect { Pgbus.coalesce }.to raise_error(ArgumentError, /block/)
    end
  end

  describe "instrumentation" do
    it "emits pgbus.coalesce with buffered, sent, retried, failed and duration" do
      events = []
      callback = ->(*, payload) { events << payload }
      allow(mock_client).to receive(:send_batch).and_return([1])

      ActiveSupport::Notifications.subscribed(callback, "pgbus.coalesce") do
        Pgbus.coalesce { Array.new(2) { |i| plain_class.perform_later(i) } }
      end

      expect(events.size).to eq(1)
      expect(events.first).to include(buffered: 2, sent: 0, retried: 2, failed: 0)
      expect(events.first[:duration]).to be_a(Float)
    end
  end

  describe "current attributes" do
    let(:current) { Class.new(ActiveSupport::CurrentAttributes) { attribute :tenant } }
    let(:current_job) { job_class("CoalesceSpec::CurrentJob") { include Pgbus::ActiveJob::CurrentAttributes } }
    let(:payloads) { [] }

    around do |example|
      original = Pgbus.configuration.current_attributes
      Pgbus.configuration.current_attributes = ["CoalesceSpec::Current"]
      example.run
    ensure
      Pgbus.configuration.current_attributes = original
    end

    before do
      stub_const("CoalesceSpec::Current", current)
      stub_const("CoalesceSpec::CurrentJob", current_job)
      allow(mock_client).to receive(:send_batch) do |_queue, batch, **|
        payloads.concat(batch)
        batch.map { take_id }
      end
    end

    after { current.reset }

    it "keeps a snapshot the job already carries" do
      job = current_job.new(1)
      job.pgbus_current_attributes = { "kept" => true }

      Pgbus.coalesce do
        current.tenant = "at-push"
        job.enqueue
      end

      expect(payloads.first[Pgbus::CurrentAttributes::METADATA_KEY]).to eq({ "kept" => true })
    end

    it "snapshots Current at push, so the flush sends the context the job was enqueued with" do
      Pgbus.coalesce do
        current.tenant = "at-push"
        current_job.perform_later(1)
        current.tenant = "at-flush"
      end

      expect(payloads.first[Pgbus::CurrentAttributes::METADATA_KEY].to_s).to include("at-push")
    end
  end
end
