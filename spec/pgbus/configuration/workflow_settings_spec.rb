# frozen_string_literal: true

require "spec_helper"
require "active_support/core_ext/numeric/time"

RSpec.describe Pgbus::Configuration::WorkflowSettings do
  subject(:config) { Pgbus::Configuration.new }

  describe "defaults" do
    it "matches the epic's configuration decision" do
      expect(config).to have_attributes(
        workflow_queue: "default",
        workflow_retention: 30 * 24 * 3600,
        workflow_sweep_interval: 300,
        workflow_stall_threshold: 3600,
        workflow_stall_action: :pause,
        workflow_max_payload_size: 131_072,
        workflow_error_backtrace_lines: 50
      )
    end

    it "passes validate!" do
      expect { config.validate! }.not_to raise_error
    end
  end

  describe "duration settings" do
    it "coerce an ActiveSupport::Duration to integer seconds" do
      config.workflow_retention = 7.days
      config.workflow_sweep_interval = 1.minute
      config.workflow_stall_threshold = 10.minutes

      expect(config.workflow_retention).to eq(7 * 24 * 3600)
      expect(config.workflow_sweep_interval).to eq(60)
      expect(config.workflow_stall_threshold).to eq(600)
    end

    it "reject zero and non-numeric values on assignment" do
      %i[workflow_retention workflow_sweep_interval workflow_stall_threshold].each do |setting|
        expect { config.public_send("#{setting}=", 0) }.to raise_error(Pgbus::ConfigurationError, /#{setting}/)
        expect { config.public_send("#{setting}=", "soon") }.to raise_error(Pgbus::ConfigurationError, /#{setting}/)
      end
    end

    it "accepts a nil workflow_retention (cleanup off)" do
      config.workflow_retention = nil

      expect(config.workflow_retention).to be_nil
      expect { config.validate! }.not_to raise_error
    end

    it "rejects an infinite sweep interval, stall threshold or retention" do
      %i[workflow_sweep_interval workflow_stall_threshold workflow_retention].each do |setting|
        fresh = Pgbus::Configuration.new
        fresh.public_send("#{setting}=", Float::INFINITY)

        expect { fresh.validate! }.to raise_error(Pgbus::ConfigurationError, /#{setting} must be a finite/)
      end
    end

    it "requires a sweep interval and a stall threshold" do
      config.instance_variable_set(:@workflow_sweep_interval, nil)
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /workflow_sweep_interval/)

      config.workflow_sweep_interval = 300
      config.instance_variable_set(:@workflow_stall_threshold, nil)
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /workflow_stall_threshold/)
    end
  end

  describe "#workflow_stall_action=" do
    it "accepts :pause, :reattempt and :cancel, as Symbols or Strings" do
      %i[pause reattempt cancel].each do |action|
        config.workflow_stall_action = action.to_s

        expect(config.workflow_stall_action).to eq(action)
      end
    end

    it "rejects anything else" do
      expect { config.workflow_stall_action = :retry }
        .to raise_error(Pgbus::ConfigurationError, /workflow_stall_action.*pause.*reattempt.*cancel/)
    end
  end

  describe "#workflow_queue=" do
    it "stores the queue name as a String" do
      config.workflow_queue = :workflows

      expect(config.workflow_queue).to eq("workflows")
    end

    it "rejects a queue whose prefixed PGMQ name is too long, at validate!" do
      config.workflow_queue = "w" * 60

      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /workflow_queue.*too long/)
    end

    it "rejects a blank queue" do
      expect { config.workflow_queue = "" }.to raise_error(Pgbus::ConfigurationError, /workflow_queue/)
      expect { config.workflow_queue = nil }.to raise_error(Pgbus::ConfigurationError, /workflow_queue/)
    end
  end

  describe "integer limits" do
    it "rejects a non-positive payload size or backtrace length" do
      config.workflow_max_payload_size = 0
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /workflow_max_payload_size/)

      config.workflow_max_payload_size = 1024
      config.workflow_error_backtrace_lines = -1
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /workflow_error_backtrace_lines/)
    end
  end

  describe "#finalize!" do
    it "freezes the workflow settings into shareable values" do
      config.workflow_queue = "workflows"
      config.finalize!

      expect(config.workflow_queue).to be_frozen
      expect(Ractor.shareable?(config.workflow_queue)).to be(true)
    end
  end
end
