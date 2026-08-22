# frozen_string_literal: true

require "spec_helper"

RSpec.describe "Selective railtie loading" do # rubocop:disable RSpec/DescribeClass
  # The dummy app uses selective railtie requires (require "rails" +
  # individual railties) instead of require "rails/all". These specs
  # verify that Pgbus models and integrations work in that scenario.

  describe "BusRecord availability" do
    it "is defined and usable before any engine initializer runs" do
      # BusRecord lives in lib/pgbus/ and is autoloaded by the gem loader.
      # It doesn't depend on the engine registering app/models.
      expect(defined?(Pgbus::BusRecord)).to eq("constant")
      expect(Pgbus::BusRecord.superclass).to eq(ActiveRecord::Base)
    end
  end

  describe "model inheritance chain" do
    Pgbus::BusRecord.descendants # eager-load known subclasses

    {
      "BatchEntry" => "pgbus_batches",
      "BatchExecution" => "pgbus_batch_executions",
      "BlockedExecution" => "pgbus_blocked_executions",
      "ProcessEntry" => "pgbus_processes",
      "ProcessedEvent" => "pgbus_processed_events",
      "RecurringExecution" => "pgbus_recurring_executions",
      "RecurringTask" => "pgbus_recurring_tasks",
      "Semaphore" => "pgbus_semaphores",
      "OutboxEntry" => "pgbus_outbox_entries",
      "QueueState" => "pgbus_queue_states",
      "UniquenessKey" => "pgbus_uniqueness_keys",
      "JobStat" => "pgbus_job_stats"
    }.each do |class_name, table_name|
      it "#{class_name} inherits from BusRecord and uses #{table_name}" do
        klass = Pgbus.const_get(class_name)
        expect(klass.ancestors).to include(Pgbus::BusRecord)
        expect(klass.table_name).to eq(table_name)
      end
    end
  end

  describe "ActiveJob integration" do
    before do
      require "active_job"
      require "active_job/queue_adapters/pgbus_adapter"
      ActiveJob::Base.include(Pgbus::Concurrency) unless ActiveJob::Base < Pgbus::Concurrency
      ActiveJob::Base.include(Pgbus::Uniqueness) unless ActiveJob::Base < Pgbus::Uniqueness
    end

    it "Concurrency module is includable into ActiveJob::Base" do
      expect(ActiveJob::Base.ancestors).to include(Pgbus::Concurrency)
    end

    it "Uniqueness module is includable into ActiveJob::Base" do
      expect(ActiveJob::Base.ancestors).to include(Pgbus::Uniqueness)
    end

    it "limits_concurrency class method is available" do
      job_class = Class.new(ActiveJob::Base) do
        limits_concurrency to: 1, key: ->(*) { "test" }
      end
      expect(job_class.pgbus_concurrency[:limit]).to eq(1)
    end

    it "ensures_uniqueness class method is available" do
      job_class = Class.new(ActiveJob::Base) do
        ensures_uniqueness key: ->(*) { "test" }, strategy: :until_executed
      end
      expect(job_class.pgbus_uniqueness[:strategy]).to eq(:until_executed)
    end
  end

  describe "engine connects_to" do
    it "uses BusRecord for separate database configuration" do
      # The engine initializer should reference BusRecord, not ApplicationRecord
      engine_file = File.read(File.expand_path("../../lib/pgbus/engine.rb", __dir__))
      expect(engine_file).to include("Pgbus::BusRecord.connects_to")
      expect(engine_file).not_to include("Pgbus::ApplicationRecord.connects_to")
    end
  end
end
