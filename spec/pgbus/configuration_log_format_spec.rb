# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Configuration do
  subject(:config) { described_class.new }

  # A fresh Configuration defaults its logger to the shared Rails.logger, so
  # setting #log_format= would mutate that global logger's formatter and leak
  # into request specs (Rails::Rack::Logger#push_tags then fails on the bare
  # Pgbus formatter). Give the config-under-test an isolated logger instead.
  before { config.logger = Logger.new(IO::NULL) }

  describe "#log_format" do
    it "defaults to :text" do
      expect(config.log_format).to eq(:text)
    end

    it "can be set to :json" do
      config.log_format = :json

      expect(config.log_format).to eq(:json)
      expect(config.logger.formatter).to be_a(Pgbus::LogFormatter::JSON)
    end

    it "can be set to :text" do
      config.log_format = :json
      config.log_format = :text

      expect(config.log_format).to eq(:text)
      expect(config.logger.formatter).to be_a(Pgbus::LogFormatter::Text)
    end

    it "raises for invalid formats" do
      expect { config.log_format = :xml }.to raise_error(Pgbus::ConfigurationError, /Invalid log_format/)
    end

    it "accepts string values" do
      config.log_format = "json"

      expect(config.log_format).to eq(:json)
    end
  end

  # The configuration is built when pgbus loads, before Rails.logger exists:
  # a log_format set that early (config/application.rb) can only format the
  # $stdout fallback, so the engine applies it again once Rails.logger exists.
  describe "#apply_log_format!" do
    let(:rails_logger) { Logger.new(IO::NULL) }

    before do
      config.logger = nil
      allow(config).to receive(:rails_logger).and_return(nil)
    end

    it "installs a log_format set before Rails.logger existed on Rails.logger" do
      config.log_format = :json
      allow(config).to receive(:rails_logger).and_return(rails_logger)

      config.apply_log_format!

      expect(rails_logger.formatter).to be_a(Pgbus::LogFormatter::JSON)
    end

    it "leaves the formatter alone when no log_format was set" do
      allow(config).to receive(:rails_logger).and_return(rails_logger)

      config.apply_log_format!

      expect(rails_logger.formatter).to be_nil
    end
  end
end
