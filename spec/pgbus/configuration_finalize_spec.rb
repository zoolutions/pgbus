# frozen_string_literal: true

require "spec_helper"

# Pgbus.finalize! (the engine's after_initialize) deep-freezes every plain-data
# setting, so each configuration reader hands back a Ractor-shareable value
# (issue #527). Writers keep working: a setter assigns a new value, and a later
# Pgbus.configure finalizes again.
RSpec.describe Pgbus::Configuration do
  # Settings whose values are callables or live objects by contract: they stay
  # on the main Ractor and are not frozen.
  def self.main_only
    {
      logger: "the host's Logger (it holds a Monitor)",
      error_reporters: "a list of callables, which apps append to with <<",
      web_auth: "a callable",
      web_data_source: "a data source object",
      fair_share: "a callable",
      event_fair_share: "a callable",
      metrics_backend: "a Pgbus::Metrics::Backend instance (or a Symbol naming one)",
      streams_presence_member: "a callable",
      streams_presence_on_change: "a callable",
      streams_authorize: "a callable"
    }
  end

  # Every public, argument-free reader of a setting, less the main-only ones.
  def self.data_settings
    probe = described_class.new
    settings = described_class.public_instance_methods.select do |name|
      name.match?(/\A[a-z_0-9]+\z/) && described_class.instance_method(name).arity.zero? &&
        (probe.instance_variable_defined?(:"@#{name}") || %i[streams_presence_ttl streams_authorize].include?(name))
    end
    (settings - main_only.keys).sort
  end

  def self.readers
    data_settings.to_h { |name| ["Pgbus.configuration.#{name}", -> { Pgbus.configuration.public_send(name) }] }
  end

  # A value for every collection-valued (and a few other) data settings.
  def self.configure_data(config)
    config.database_url = +"postgres://localhost/pgbus_test"
    config.connection_params = { host: +"db", variables: { statement_timeout: +"5s" } }
    config.connects_to = { database: { writing: :pgbus } }
    config.workers = [{ queues: [+"default", +"mailers"], threads: 3 }]
    config.roles = [:workers]
    config.priority_levels = 3
    config.allowed_global_id_models = [+"User"]
    config.web_filter_parameters = [+"token", /secret/]
    config.streams_durable_patterns = [+"orders", /\Achat:/]
    config.streams_presence_patterns = [/\Aroom:/]
    config.streams_retention = { +"chat" => 60 }
    config.streams_presence_ttl = { /\Aroom:/ => 30, default: 60 }
    config.recurring_tasks = { +"cleanup" => { class: +"CleanupJob", schedule: +"every hour" } }
    config.recurring_tasks_files = [+"config/recurring.yml", +"config/recurring_extra.yml"]
    config.event_consumers = [{ topics: [+"orders.#"], threads: 2, read_ahead: 4 }]
    config.current_attributes = :auto
  end

  # Each example works on a fresh global configuration; the suite's is put back.
  around do |example|
    original = Pgbus.configuration
    Pgbus.instance_variable_set(:@configuration, described_class.new)
    example.run
  ensure
    Pgbus.instance_variable_set(:@configuration, original)
  end

  describe "Pgbus.finalize! with the defaults" do
    it_behaves_like "a boot-finalized configuration", readers: readers, finalize: -> { Pgbus.finalize! }
  end

  describe "Pgbus.finalize! with every collection setting configured" do
    it_behaves_like "a boot-finalized configuration",
                    readers: readers,
                    finalize: -> { configure_data(Pgbus.configuration) && Pgbus.finalize! }
  end

  describe "#finalize!" do
    subject(:config) { described_class.new }

    it "copies the app's own collections instead of freezing them" do
      patterns = [+"orders"]
      config.streams_durable_patterns = patterns

      config.finalize!

      expect(config.streams_durable_patterns).to be_frozen
      expect(patterns).not_to be_frozen
    end

    it "leaves callables and the logger alone" do
      reporter = ->(_error, _context) {}
      config.error_reporters << reporter

      config.finalize!

      expect(config.error_reporters).not_to be_frozen
      expect { config.error_reporters << reporter }.not_to raise_error
      expect(config.logger).not_to be_frozen
    end

    it "still adds a capsule after finalize" do
      config.finalize!

      config.capsule :late, queues: %w[late], threads: 1

      expect(config.capsule_named(:late)).to include(queues: %w[late], threads: 1)
    end

    it "keeps setters working" do
      config.finalize!

      config.queue_prefix = "after_boot"

      expect(config.queue_prefix).to eq("after_boot")
    end

    it "reports whether it ran" do
      expect { config.finalize! }.to change(config, :finalized?).from(false).to(true)
    end

    it "keeps answering stream_durable? from the frozen patterns" do
      config.streams_durable_patterns = ["orders", /\Achat:/]
      config.finalize!

      expect(config.stream_durable?("chat:1")).to be(true)
      expect(config.stream_durable?("other")).to be(false)
    end
  end

  describe "Pgbus.finalize!" do
    it "validates and finalizes the global configuration" do
      Pgbus.configuration.drain_timeout = 0

      expect { Pgbus.finalize! }.to raise_error(Pgbus::ConfigurationError, /drain_timeout/)
    end

    it "finalizes the global configuration" do
      Pgbus.finalize!

      expect(Pgbus.configuration).to be_finalized
      expect(Ractor.shareable?(Pgbus.configuration.workers)).to be(true)
    end

    it "finalizes again at the end of a later Pgbus.configure" do
      Pgbus.finalize!

      Pgbus.configure { |c| c.streams_durable_patterns = [+"late"] }

      expect(Ractor.shareable?(Pgbus.configuration.streams_durable_patterns)).to be(true)
    end

    it "finalizes around the unshareable values an app configures, keeping them as they are" do
      seen = []
      auth = ->(request) { seen << request }
      share = ->(queues) { queues.to_h { |queue| [queue, seen.size] } }
      redact = ->(key, value) { value.replace("[FILTERED]") if key == "card" }
      logger = Logger.new(StringIO.new)
      Pgbus.configuration.web_auth = auth
      Pgbus.configuration.fair_share = share
      Pgbus.configuration.logger = logger
      Pgbus.configuration.web_filter_parameters = [:token, redact]
      Pgbus.configuration.streams_durable_patterns = [+"orders"]

      expect { Pgbus.finalize! }.not_to raise_error

      config = Pgbus.configuration
      expect([config.web_auth, config.fair_share, config.logger]).to eq([auth, share, logger])
      expect(config.web_auth.call(:request)).to equal(seen).and eq([:request])
      expect(config.web_filter_parameters).to eq([:token, redact])
      expect(config.web_filter_parameters).not_to be_frozen
      expect(Ractor.shareable?(config.streams_durable_patterns)).to be(true)
    end
  end

  describe "Pgbus.configure" do
    # Without Rails, or once the app has booted. During a Rails boot the
    # engine finalizes instead (spec/pgbus/engine_spec.rb).
    it "finalizes at its end outside a Rails boot" do
      Pgbus.configure { |c| c.streams_durable_patterns = [+"orders"] }

      expect(Pgbus.configuration).to be_finalized
      expect(Ractor.shareable?(Pgbus.configuration.streams_durable_patterns)).to be(true)
    end
  end
end
