# frozen_string_literal: true

require "spec_helper"
require "active_support/core_ext/numeric/time"

RSpec.describe Pgbus::Configuration::DeploySettings do
  subject(:config) { Pgbus::Configuration.new }

  describe "defaults" do
    it "has no app revision" do
      expect(config.app_revision).to be_nil
    end

    it "has no deploy tasks and no deploy task files" do
      expect(config.deploy_tasks).to be_nil
      expect(config.deploy_tasks_files).to be_nil
    end

    it "runs deploy tasks" do
      expect(config.deploy_tasks_enabled).to be(true)
    end

    it "keeps deploy task runs for 90 days" do
      expect(config.deploy_task_run_retention).to eq(90 * 24 * 3600)
    end
  end

  describe "#deploy_task_run_retention=" do
    it "coerces a Duration to seconds" do
      config.deploy_task_run_retention = 30.days

      expect(config.deploy_task_run_retention).to eq(30 * 24 * 3600)
    end

    it "accepts nil to skip the cleanup" do
      config.deploy_task_run_retention = nil

      expect(config.deploy_task_run_retention).to be_nil
    end

    it "rejects a non-positive value" do
      expect { config.deploy_task_run_retention = 0 }
        .to raise_error(Pgbus::ConfigurationError, /deploy_task_run_retention.*positive/)
    end
  end

  describe "#validate!" do
    it "accepts a String, a callable or nil app_revision" do
      ["abc123", -> { "abc123" }, nil].each do |value|
        config.app_revision = value
        expect { config.validate! }.not_to raise_error
      end
    end

    it "rejects any other app_revision" do
      config.app_revision = 42

      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /app_revision/)
    end

    it "rejects a deploy_tasks that is not a Hash" do
      config.deploy_tasks = ["backfill"]

      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /deploy_tasks must be a Hash/)
    end

    it "rejects a deploy_tasks_files that is not an Array" do
      config.deploy_tasks_files = "config/deploy_tasks.yml"

      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /deploy_tasks_files must be an Array/)
    end

    it "rejects a non-boolean deploy_tasks_enabled" do
      config.deploy_tasks_enabled = "yes"

      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /deploy_tasks_enabled/)
    end
  end

  describe "#finalize!" do
    it "leaves a callable app_revision callable" do
      config.app_revision = -> { "abc123" }
      config.finalize!

      expect(config.app_revision.call).to eq("abc123")
    end
  end
end
