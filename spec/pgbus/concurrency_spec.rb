# frozen_string_literal: true

require "spec_helper"
require "active_job"

RSpec.describe Pgbus::Concurrency do
  let(:test_job_class) do
    Class.new(ActiveJob::Base) do
      include Pgbus::Concurrency

      self.queue_adapter = :test

      limits_concurrency to: 2,
                         key: ->(user_id) { "TestJob-#{user_id}" },
                         duration: 900,
                         on_conflict: :discard

      def perform(user_id); end
    end
  end

  let(:no_concurrency_job_class) do
    Class.new(ActiveJob::Base) do
      self.queue_adapter = :test
      def perform; end
    end
  end

  describe ".limits_concurrency" do
    it "stores concurrency config on the class" do
      config = test_job_class.pgbus_concurrency
      expect(config[:limit]).to eq(2)
      expect(config[:duration]).to eq(900)
      expect(config[:on_conflict]).to eq(:discard)
      expect(config[:key]).to be_a(Proc)
    end

    it "validates to: is a positive integer" do
      expect do
        Class.new(ActiveJob::Base) do
          include Pgbus::Concurrency

          limits_concurrency to: 0, key: -> { "x" }
        end
      end.to raise_error(ArgumentError, /positive integer/)
    end

    it "validates on_conflict is a known strategy" do
      expect do
        Class.new(ActiveJob::Base) do
          include Pgbus::Concurrency

          limits_concurrency to: 1, on_conflict: :unknown
        end
      end.to raise_error(ArgumentError, /on_conflict/)
    end

    it "validates duration is a positive number" do
      expect do
        Class.new(ActiveJob::Base) do
          include Pgbus::Concurrency

          limits_concurrency to: 1, duration: -5
        end
      end.to raise_error(ArgumentError, /duration/)
    end

    it "validates key is callable" do
      expect do
        Class.new(ActiveJob::Base) do
          include Pgbus::Concurrency

          limits_concurrency to: 1, key: "not_callable"
        end
      end.to raise_error(ArgumentError, /callable/)
    end

    it "rejects key: false at definition time (would otherwise silently act as the default key)" do
      expect do
        Class.new(ActiveJob::Base) do
          include Pgbus::Concurrency

          limits_concurrency to: 1, key: false
        end
      end.to raise_error(ArgumentError, /callable/)
    end

    it "defaults key to the enqueued job's class name (no default proc stored)" do
      job_class = Class.new(ActiveJob::Base) do
        include Pgbus::Concurrency

        limits_concurrency to: 1

        def perform; end
      end
      stub_const("DefaultKeyJob", job_class)

      # No proc is stored for the default — the key is resolved from the job
      # instance's class at resolve time, so base-class declarations can't
      # collapse subclasses into the declaring class's key (issue #357).
      expect(job_class.pgbus_concurrency[:key]).to be_nil
      expect(described_class.resolve_key(DefaultKeyJob.new)).to eq("DefaultKeyJob")
    end
  end

  describe "inheritance of base-class declarations (issue #357)" do
    before do
      stub_const("ThrottledBaseJob", Class.new(ActiveJob::Base) do
        include Pgbus::Concurrency

        limits_concurrency to: 2, on_conflict: :discard
      end)
      stub_const("ThrottledChildJob", Class.new(ThrottledBaseJob))
    end

    it "makes the base-class config visible to subclasses" do
      config = ThrottledChildJob.pgbus_concurrency
      expect(config).to be_present
      expect(config[:limit]).to eq(2)
      expect(config[:on_conflict]).to eq(:discard)
    end

    it "resolves the SUBCLASS name for the class-name default, not the declaring class" do
      expect(described_class.resolve_key(ThrottledChildJob.new)).to eq("ThrottledChildJob")
    end

    it "lets a subclass's own declaration override the inherited one" do
      stub_const("OverridingChildJob", Class.new(ThrottledBaseJob) do
        limits_concurrency to: 5, key: ->(*) { "custom" }
      end)

      expect(OverridingChildJob.pgbus_concurrency[:limit]).to eq(5)
      expect(described_class.resolve_key(OverridingChildJob.new)).to eq("custom")
      expect(ThrottledChildJob.pgbus_concurrency[:limit]).to eq(2)
    end
  end

  describe ".resolve_key" do
    it "resolves the concurrency key from job arguments" do
      job = test_job_class.new(42)
      expect(described_class.resolve_key(job)).to eq("TestJob-42")
    end

    it "forwards keyword arguments to the key lambda" do
      kw_job_class = Class.new(ActiveJob::Base) do
        include Pgbus::Concurrency

        self.queue_adapter = :test

        limits_concurrency to: 1,
                           key: ->(user_id:) { "KWJob-#{user_id}" }

        def perform(user_id:); end
      end

      job = kw_job_class.new(user_id: 99)
      expect(described_class.resolve_key(job)).to eq("KWJob-99")
    end

    it "returns nil for jobs without concurrency" do
      job = no_concurrency_job_class.new
      expect(described_class.resolve_key(job)).to be_nil
    end
  end

  describe ".inject_metadata" do
    it "adds concurrency key to payload hash" do
      job = test_job_class.new(42)
      payload = { "job_class" => "TestJob", "arguments" => [42] }
      result = described_class.inject_metadata(job, payload)
      expect(result["pgbus_concurrency_key"]).to eq("TestJob-42")
    end

    it "returns original payload for jobs without concurrency" do
      job = no_concurrency_job_class.new
      payload = { "job_class" => "NoConcurrency" }
      result = described_class.inject_metadata(job, payload)
      expect(result).not_to have_key("pgbus_concurrency_key")
    end
  end

  describe ".extract_key" do
    it "extracts the concurrency key from a payload" do
      payload = { "pgbus_concurrency_key" => "TestJob-42" }
      expect(described_class.extract_key(payload)).to eq("TestJob-42")
    end

    it "returns nil when no concurrency key present" do
      expect(described_class.extract_key({})).to be_nil
    end
  end

  describe ".config_for" do
    it "returns the class's declared limit and duration" do
      klass = Class.new do
        include Pgbus::Concurrency

        limits_concurrency to: 3, key: ->(*) { "k" }, duration: 60
      end

      expect(described_class.config_for(klass)).to eq(limit: 3, duration: 60)
    end

    # A class that no longer resolves must not be forced to limit 1: with a
    # `to: 3` semaphore still holding 2 slots, limit 1 would refuse every
    # promotion and the parked jobs could never reach the executor (which is
    # what dead-letters a missing class). A nil limit means "keep the limit
    # the semaphore row already records".
    it "leaves the limit to the semaphore row when the class does not resolve" do
      expect(described_class.config_for(nil)).to eq(limit: nil, duration: Pgbus::Concurrency::DEFAULT_DURATION)
    end
  end

  describe ".effective_duration" do
    # The semaphore is only kept alive by the visibility heartbeat, which
    # first beats one interval in. A duration shorter than that expires
    # before the first touch and the sweep promotes beside a running job.
    it "floors a duration shorter than two heartbeat intervals" do
      config = Pgbus::Configuration.new
      config.visibility_timeout = 30

      expect(described_class.effective_duration(5, config: config)).to eq(20)
    end

    it "leaves a duration longer than the floor alone" do
      config = Pgbus::Configuration.new
      config.visibility_timeout = 30

      expect(described_class.effective_duration(900, config: config)).to eq(900)
    end
  end
end
