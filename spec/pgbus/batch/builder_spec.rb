# frozen_string_literal: true

require "spec_helper"
require "active_job"

RSpec.describe Pgbus::Batch::Builder do
  let(:relation) { double("relation", update_all: 1) }
  let(:record) { double("BatchEntry", status: "pending", properties: "{}") }
  let(:logger) { instance_double(Logger, warn: nil, error: nil, info: nil, debug: nil) }

  let(:work_job) do
    Class.new(ActiveJob::Base) do
      def self.name = "BuilderSpec::WorkJob"
      def perform(*); end
    end
  end

  let(:callback_job) do
    Class.new(ActiveJob::Base) do
      def self.name = "BuilderSpec::CallbackJob"
      def perform(*); end
    end
  end

  before do
    stub_const("BuilderSpec::WorkJob", work_job)
    stub_const("BuilderSpec::CallbackJob", callback_job)
    allow(Pgbus::BatchEntry).to receive_messages(create!: record, find_by: record, where: relation,
                                                 check_finished!: { just_finished: false, record: nil })
    allow(Pgbus::Batch).to receive_messages(callback_jobs_migrated?: true, executions_migrated?: true)
    allow(ActiveJob).to receive(:perform_all_later)
    allow(Pgbus).to receive(:logger).and_return(logger)
    Pgbus::Batch.reset_executions_migrated_cache!
  end

  describe "Pgbus.batch" do
    it "yields a builder and returns the started batch" do
      yielded = nil

      batch = Pgbus.batch(description: "import") { |b| yielded = b }

      expect(yielded).to be_a(described_class)
      expect(batch).to be_a(Pgbus::Batch)
      expect(Pgbus::BatchEntry).to have_received(:create!)
        .with(hash_including(batch_id: batch.batch_id, description: "import", status: "pending"))
    end

    it "is Pgbus::Batch.start" do
      expect(Pgbus::Batch.start(properties: { "a" => 1 }) { |_b| nil }).to be_a(Pgbus::Batch)
    end

    it "raises without a block" do
      expect { Pgbus.batch(description: "import") }.to raise_error(ArgumentError, /block/)
    end

    it "creates the row before the block and starts processing only after it returns" do
      events = []
      allow(Pgbus::BatchEntry).to receive(:create!) do
        events << :create
        record
      end
      allow(relation).to receive(:update_all) do |attrs|
        events << attrs
        1
      end

      Pgbus.batch { |_b| events << :block }

      expect(events).to eq([:create, :block, { status: "processing" }])
    end

    it "holds the batch tag for the whole block" do
      tags = []

      batch = Pgbus.batch do |_b|
        tags << Thread.current[:pgbus_batch_id]
        tags << Thread.current[:pgbus_batch_id]
      end

      expect(tags).to eq([batch.batch_id, batch.batch_id])
      expect(Thread.current[:pgbus_batch_id]).to be_nil
    end

    it "passes callback keywords through to Pgbus::Batch.new" do
      batch = Pgbus.batch(on_complete: callback_job) { |_b| nil }

      expect(batch.on_complete).to eq(callback_job)
    end

    it "skips the block when a held uniqueness key discards the batch" do
      allow(Pgbus::UniquenessKey).to receive_messages(acquire!: false, clear_bind_stamp!: nil)
      ran = false

      batch = Pgbus.batch(uniqueness_key: "nightly", on_conflict: :discard) { |_b| ran = true }

      expect(ran).to be(false)
      expect(batch).to be_discarded
      expect(Pgbus::BatchEntry).not_to have_received(:create!)
    end
  end

  describe "callbacks" do
    it "writes on_complete into the on_finish columns of the open batch" do
      Pgbus.batch { |b| b.on_complete(callback_job) }

      expect(Pgbus::BatchEntry).to have_received(:where)
        .with(batch_id: anything, status: %w[pending processing])
      expect(relation).to have_received(:update_all)
        .with(on_finish_class: "BuilderSpec::CallbackJob", on_finish_job: nil)
    end

    it "serializes a configured instance into the jsonb column" do
      Pgbus.batch { |b| b.on_success(callback_job.new.set(queue: :critical)) }

      expect(relation).to have_received(:update_all).with(
        on_success_class: nil,
        on_success_job: hash_including("job_class" => "BuilderSpec::CallbackJob", "queue_name" => "critical")
      )
    end

    it "writes on_failure into the on_failure columns" do
      Pgbus.batch { |b| b.on_failure(callback_job) }

      expect(relation).to have_received(:update_all)
        .with(on_failure_class: "BuilderSpec::CallbackJob", on_failure_job: nil)
    end

    it "updates the readers of the returned batch" do
      batch = Pgbus.batch do |b|
        b.on_complete(callback_job)
        b.on_success(callback_job)
        b.on_failure(callback_job)
      end

      expect([batch.on_complete, batch.on_success, batch.on_failure]).to all(eq(callback_job))
    end

    it "accepts a callback set after jobs were added" do
      Pgbus.batch do |b|
        b.add(work_job.new)
        b.on_complete(callback_job)
      end

      expect(relation).to have_received(:update_all).with(hash_including(on_finish_class: "BuilderSpec::CallbackJob"))
    end

    it "rejects a block, since a callback is stored on the batch row" do
      expect { Pgbus.batch { |b| b.on_complete { nil } } }.to raise_error(ArgumentError, /ActiveJob/)
    end

    it "rejects anything that is not an ActiveJob class or instance" do
      expect { Pgbus.batch { |b| b.on_complete("BuilderSpec::CallbackJob") } }
        .to raise_error(ArgumentError, /ActiveJob/)
      expect { Pgbus.batch { |b| b.on_complete(Class.new) } }.to raise_error(ArgumentError, /ActiveJob/)
    end

    it "raises AlreadyFinished when the batch is no longer open" do
      allow(relation).to receive(:update_all).and_return(0)

      expect { Pgbus.batch { |b| b.on_complete(callback_job) } }.to raise_error(Pgbus::Batch::AlreadyFinished)
    end

    it "maps the deprecated on_finish onto on_complete and warns once" do
      batch = Pgbus.batch do |b|
        b.on_finish(callback_job)
        b.on_finish(callback_job)
      end

      expect(batch.on_complete).to eq(callback_job)
      expect(logger).to have_received(:warn).once
    end
  end

  describe "#add" do
    it "enqueues job instances in one bulk call inside the batch tag" do
      jobs = [work_job.new(1), work_job.new(2)]
      tag = nil
      allow(ActiveJob).to receive(:perform_all_later) { tag = Thread.current[:pgbus_batch_id] }

      batch = Pgbus.batch { |b| b.add(*jobs) }

      expect(ActiveJob).to have_received(:perform_all_later).with(jobs).once
      expect(tag).to eq(batch.batch_id)
    end

    it "flattens arrays and returns the jobs" do
      jobs = [work_job.new(1), work_job.new(2), work_job.new(3)]
      returned = nil

      Pgbus.batch { |b| returned = b.add(jobs.first(2), jobs.last) }

      expect(ActiveJob).to have_received(:perform_all_later).with(jobs)
      expect(returned).to eq(jobs)
    end

    it "yields a block inside the batch tag" do
      tag = nil

      batch = Pgbus.batch { |b| b.add { tag = Thread.current[:pgbus_batch_id] } }

      expect(tag).to eq(batch.batch_id)
      expect(ActiveJob).not_to have_received(:perform_all_later)
    end

    it "rejects anything that is not a job instance" do
      [work_job, work_job.set(wait: 60), "BuilderSpec::WorkJob", nil].each do |value|
        expect { Pgbus.batch { |b| b.add(value) } }.to raise_error(ArgumentError, /job instance/)
      end
      expect(ActiveJob).not_to have_received(:perform_all_later)
    end

    it "refuses an add from a thread that does not carry the batch tag" do
      adder = lambda do |b|
        Thread.new do # rubocop:disable ThreadSafety/NewThread -- joined at once; models a fan-out thread inside the block
          Thread.current.report_on_exception = false
          b.add(work_job.new)
        end.join
      end

      expect { Pgbus.batch(&adder) }.to raise_error(ArgumentError, /thread or fiber/)
      expect(ActiveJob).not_to have_received(:perform_all_later)
    end

    it "refuses an add while another batch's block holds the tag" do
      expect do
        Pgbus.batch { |outer| Pgbus.batch { |_inner| outer.add(work_job.new) } }
      end.to raise_error(ArgumentError, /thread or fiber/)
    end

    it "is a no-op without jobs or a block" do
      Pgbus.batch(&:add)

      expect(ActiveJob).not_to have_received(:perform_all_later)
    end
  end

  describe "after the block returns" do
    it "refuses to add jobs or set callbacks, so nothing is enqueued outside the batch tag" do
      leaked = nil
      Pgbus.batch { |b| leaked = b }

      expect { leaked.add(work_job.new) }.to raise_error(ArgumentError, /batch\.add/)
      expect { leaked.on_complete(callback_job) }.to raise_error(ArgumentError, /batch\.add/)
      expect(ActiveJob).not_to have_received(:perform_all_later)
    end
  end
end
