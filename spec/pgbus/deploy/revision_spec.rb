# frozen_string_literal: true

require "spec_helper"
require "fileutils"
require "pathname"
require "tmpdir"

RSpec.describe Pgbus::Deploy::Revision do
  let(:config) { Pgbus::Configuration.new }
  let(:env_keys) { %w[PGBUS_REVISION DASH_VERSION KAMAL_VERSION] }

  around do |example|
    saved = env_keys.to_h { |key| [key, ENV.fetch(key, nil)] }
    env_keys.each { |key| ENV.delete(key) }
    example.run
  ensure
    saved.each { |key, value| value.nil? ? ENV.delete(key) : ENV[key] = value }
  end

  describe ".current" do
    it "is nil when nothing is configured" do
      expect(described_class.current(config)).to be_nil
    end

    it "prefers config.app_revision over every other source" do
      config.app_revision = "from-config"
      ENV["PGBUS_REVISION"] = "from-env"

      expect(described_class.current(config)).to eq("from-config")
    end

    it "calls a callable app_revision" do
      config.app_revision = -> { "abc123" }

      expect(described_class.current(config)).to eq("abc123")
    end

    it "stringifies and strips the revision" do
      config.app_revision = -> { :" v42 " }

      expect(described_class.current(config)).to eq("v42")
    end

    it "falls through a callable that raises, with a warning" do
      logger = instance_double(Logger, warn: nil)
      allow(Pgbus).to receive(:logger).and_return(logger)
      config.app_revision = -> { raise "git missing" }
      ENV["PGBUS_REVISION"] = "env-rev"

      expect(described_class.current(config)).to eq("env-rev")
      expect(logger).to have_received(:warn)
    end

    it "treats a blank app_revision as absent" do
      config.app_revision = "  "
      ENV["DASH_VERSION"] = "dash-rev"

      expect(described_class.current(config)).to eq("dash-rev")
    end

    it "reads PGBUS_REVISION before DASH_VERSION before KAMAL_VERSION" do
      ENV["KAMAL_VERSION"] = "kamal"
      expect(described_class.current(config)).to eq("kamal")

      ENV["DASH_VERSION"] = "dash"
      expect(described_class.current(config)).to eq("dash")

      ENV["PGBUS_REVISION"] = "pgbus"
      expect(described_class.current(config)).to eq("pgbus")
    end

    it "ignores blank environment variables" do
      ENV["PGBUS_REVISION"] = ""
      ENV["KAMAL_VERSION"] = "kamal"

      expect(described_class.current(config)).to eq("kamal")
    end

    context "with a REVISION file at Rails.root (Capistrano)" do
      let(:root) { Pathname.new(Dir.mktmpdir) }

      before do
        app_root = root
        stub_const("Rails", Module.new.tap { |m| m.define_singleton_method(:root) { app_root } })
      end

      after { FileUtils.remove_entry(root) }

      it "reads and strips the file" do
        root.join("REVISION").write("deadbeef\n")

        expect(described_class.current(config)).to eq("deadbeef")
      end

      it "loses to the environment" do
        root.join("REVISION").write("deadbeef\n")
        ENV["KAMAL_VERSION"] = "kamal"

        expect(described_class.current(config)).to eq("kamal")
      end

      it "is nil, with a warning, when the file cannot be read" do
        root.join("REVISION").write("deadbeef\n")
        allow(File).to receive(:read).and_raise(Errno::EACCES, "REVISION")
        allow(Pgbus.logger).to receive(:warn)

        expect(described_class.current(config)).to be_nil
        expect(Pgbus.logger).to have_received(:warn)
      end

      it "is nil when the file is missing or blank" do
        expect(described_class.current(config)).to be_nil

        root.join("REVISION").write("\n")
        expect(described_class.current(config)).to be_nil
      end
    end

    it "works on a finalized (frozen) configuration" do
      config.app_revision = "frozen-rev"
      config.finalize!

      expect(described_class.current(config)).to eq("frozen-rev")
    end

    it "defaults to Pgbus.configuration" do
      allow(Pgbus.configuration).to receive(:app_revision).and_return("global")

      expect(described_class.current).to eq("global")
    end
  end
end
