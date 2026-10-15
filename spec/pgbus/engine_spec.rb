# frozen_string_literal: true

require "rails_helper"

require "tmpdir"

# Two-layer coverage for lib/pgbus/engine.rb:
#
#   1. Post-boot assertions against the already-booted spec/dummy app
#      (mounted by rails_helper). These verify the initializers ran with
#      their real effects: middleware installed, i18n paths appended, web
#      constants required, logger defaulted.
#
#   2. Isolated initializer-block tests: each `initializer "name" do |app|`
#      block is fetched from Pgbus::Engine.initializers and invoked directly
#      with a stub app rooted at a tmpdir. This exercises the branchy blocks
#      (pgbus.recurring fallbacks, pgbus.db, AppSignal guard)
#      without rebooting Rails or touching the dummy app's real config dir.
RSpec.describe Pgbus::Engine do
  # Fetch a named initializer's block so it can be invoked in isolation.
  def initializer_block(name)
    described_class.initializers.find { |i| i.name == name.to_sym || i.name == name }.instance_variable_get(:@block)
  end

  # Restore the suite baseline config after any example that mutates or
  # resets it, so random-order runs don't leak state into later specs.
  def restore_baseline_config
    Pgbus.reset!
    Pgbus.configure do |c|
      c.logger = Logger.new(IO::NULL)
      c.queue_prefix = "pgbus_test"
      c.default_queue = "default"
    end
  end

  describe "post-boot state" do
    it "installs the watermark cache middleware when streams are enabled" do
      expect(Pgbus.configuration.streams_enabled).to be(true)
      names = Rails.application.middleware.map(&:name)
      expect(names).to include("Pgbus::Streams::WatermarkCacheMiddleware")
    end

    it "appends the engine's locale files to the i18n load path" do
      pgbus_locales = I18n.load_path.select do |path|
        path.include?("pgbus") && path.include?("locales") && path.end_with?(".yml")
      end
      expect(pgbus_locales).not_to be_empty
    end

    it "requires the web authentication and data source constants" do
      expect(defined?(Pgbus::Web::Authentication)).to eq("constant")
      expect(defined?(Pgbus::Web::DataSource)).to eq("constant")
    end

    it "defaults the configuration logger" do
      expect(Pgbus.configuration.logger).not_to be_nil
    end
  end

  describe "the mounted stream endpoint (issue #522)" do
    let(:stream_app) do
      route = described_class.routes.routes.find { |r| r.name == "streams" }
      route.app.app
    end

    around do |example|
      saved_secret = Pgbus.configuration.streams_signed_name_secret
      saved_authorize = Pgbus.configuration.streams_authorize
      Pgbus.configuration.streams_signed_name_secret = "a" * 64
      example.run
    ensure
      Pgbus.configuration.streams_signed_name_secret = saved_secret
      Pgbus.configuration.streams_authorize = saved_authorize
    end

    it "mounts a StreamApp with no authorize: of its own" do
      expect(stream_app).to be_a(Pgbus::Web::StreamApp)
      expect(stream_app.instance_variable_get(:@authorize)).to be_nil
    end

    it "answers 403 through config.streams_authorize" do
      Pgbus.configuration.streams_authorize = ->(_env, _stream_name) { false }
      env = Rack::MockRequest.env_for("/#{Pgbus::Streams::SignedName.sign("chat")}")

      status, = stream_app.call(env)

      expect(status).to eq(403)
    end
  end

  describe ".warn_if_legacy_yaml_config" do
    let(:tmpdir) { Dir.mktmpdir }
    let(:logger) { instance_spy(Logger) }

    before { FileUtils.mkdir_p(File.join(tmpdir, "config")) }
    after { FileUtils.remove_entry(tmpdir) }

    it "warns, pointing at the docs, when config/pgbus.yml is present" do
      File.write(File.join(tmpdir, "config", "pgbus.yml"), "default_queue: legacy\n")

      described_class.warn_if_legacy_yaml_config(Pathname.new(tmpdir), logger)

      expect(logger).to have_received(:warn) do |&block|
        message = block.call
        expect(message).to include("config/pgbus.yml")
        expect(message).to include("no longer loaded")
        expect(message).to include("config/initializers/pgbus.rb")
        expect(message).to match(%r{https?://\S+})
      end
    end

    it "is silent when config/pgbus.yml is absent" do
      described_class.warn_if_legacy_yaml_config(Pathname.new(tmpdir), logger)

      expect(logger).not_to have_received(:warn)
    end

    it "does not raise when the logger is nil" do
      File.write(File.join(tmpdir, "config", "pgbus.yml"), "default_queue: legacy\n")

      expect { described_class.warn_if_legacy_yaml_config(Pathname.new(tmpdir), nil) }.not_to raise_error
    end
  end

  describe "pgbus.active_job initializer" do
    # The dummy app does not require active_job/railtie, so the on_load(:active_job)
    # hook has not fired against ActiveJob::Base at boot. Load ActiveJob here and
    # run the hooks to prove the initializer includes both concerns.
    it "includes Concurrency, Uniqueness and BatchId on ActiveJob::Base" do
      require "active_job"

      # Ensure the engine's on_load block is registered, then flush the hooks.
      initializer_block("pgbus.active_job").call
      ActiveSupport.run_load_hooks(:active_job, ActiveJob::Base)

      expect(ActiveJob::Base.include?(Pgbus::Concurrency)).to be(true)
      expect(ActiveJob::Base.include?(Pgbus::Uniqueness)).to be(true)
      expect(ActiveJob::Base.include?(Pgbus::ActiveJob::BatchId)).to be(true)
    end
  end

  describe "pgbus.recurring initializer" do
    subject(:block) { initializer_block("pgbus.recurring") }

    let(:tmpdir) { Dir.mktmpdir }
    let(:app) { instance_double(Rails::Application, root: Pathname.new(tmpdir)) }
    let(:default_path) { File.join(tmpdir, "config", "recurring.yml") }
    let(:task_yaml) do
      <<~YAML
        cleanup:
          class: CleanupJob
          schedule: "every hour"
      YAML
    end

    before do
      FileUtils.mkdir_p(File.join(tmpdir, "config"))
      restore_baseline_config
    end

    after do
      FileUtils.remove_entry(tmpdir)
      restore_baseline_config
    end

    it "skips when recurring_tasks is already set" do
      Pgbus.configuration.recurring_tasks = { "existing" => { "class" => "X" } }

      block.call(app)

      expect(Pgbus.configuration.recurring_tasks).to eq("existing" => { "class" => "X" })
    end

    it "loads explicit recurring_tasks_files when they contain tasks" do
      explicit = File.join(tmpdir, "config", "explicit.yml")
      File.write(explicit, task_yaml)
      Pgbus.configuration.recurring_tasks_files = [explicit]

      block.call(app)

      expect(Pgbus.configuration.recurring_tasks).to include("cleanup")
    end

    it "falls back to default recurring.yml when explicit files are empty" do
      empty = File.join(tmpdir, "config", "empty.yml")
      File.write(empty, "{}\n")
      File.write(default_path, task_yaml)
      Pgbus.configuration.recurring_tasks_files = [empty]

      block.call(app)

      expect(Pgbus.configuration.recurring_tasks).to include("cleanup")
      expect(Pgbus.configuration.recurring_tasks_file).to eq(default_path)
    end

    it "loads the default recurring.yml when no explicit files are configured" do
      File.write(default_path, task_yaml)

      block.call(app)

      expect(Pgbus.configuration.recurring_tasks).to include("cleanup")
      expect(Pgbus.configuration.recurring_tasks_file).to eq(default_path)
    end

    it "is a no-op when neither explicit files nor a default exist" do
      block.call(app)

      expect(Pgbus.configuration.recurring_tasks).to be_nil
    end
  end

  describe "pgbus.db initializer" do
    after { restore_baseline_config }

    let(:block) { initializer_block("pgbus.db") }

    it "wires BusRecord.connects_to when configuration.connects_to is set" do
      connects = { database: { writing: :pgbus } }
      Pgbus.configuration.connects_to = connects
      allow(Pgbus::BusRecord).to receive(:connects_to)

      block.call
      ActiveSupport.run_load_hooks(:active_record, ActiveRecord::Base)

      # on_load(:active_record) fires the block once per already-loaded model
      # plus the explicit run above, so assert the contract (called with the
      # configured args) rather than a brittle exact call count.
      expect(Pgbus::BusRecord).to have_received(:connects_to).with(**connects).at_least(:once)
    end

    it "does not call connects_to when configuration.connects_to is nil" do
      Pgbus.configuration.connects_to = nil
      allow(Pgbus::BusRecord).to receive(:connects_to)

      block.call
      ActiveSupport.run_load_hooks(:active_record, ActiveRecord::Base)

      expect(Pgbus::BusRecord).not_to have_received(:connects_to)
    end

    it "prepends the purge/drop guard onto ActiveRecord::Tasks::DatabaseTasks" do
      Pgbus.configuration.connects_to = nil

      block.call
      ActiveSupport.run_load_hooks(:active_record, ActiveRecord::Base)

      expect(ActiveRecord::Tasks::DatabaseTasks.singleton_class.ancestors)
        .to include(Pgbus::DatabaseTasksGuard)
    end
  end

  describe "pgbus.streams.assets initializer" do
    let(:assets) { Struct.new(:precompile).new([]) }
    let(:app) { double("app", config: double("config", assets: assets)) }

    it "precompiles the minified element, its sourcemap and the readable source" do
      initializer_block("pgbus.streams.assets").call(app)

      expect(assets.precompile).to contain_exactly(
        "pgbus/stream_source_element.min.js",
        "pgbus/stream_source_element.min.js.map",
        "pgbus/stream_source_element.js"
      )
    end
  end

  describe "pgbus.streams.importmap initializer" do
    let(:importmap) { spy("importmap") }

    before do
      stub_const("Importmap::Map", Class.new)
      allow(Rails).to receive(:application).and_return(double("app", importmap: importmap))
    end

    # The app has booted, so on_load(:after_initialize) runs the hook at once —
    # once per earlier run_load_hooks(:after_initialize) call, and the AppSignal
    # examples below make more of those, so the count depends on example order.
    it "pins pgbus/stream_source_element to the minified file" do
      initializer_block("pgbus.streams.importmap").call

      expect(importmap).to have_received(:pin)
        .with("pgbus/stream_source_element", to: "pgbus/stream_source_element.min.js").at_least(:once)
      expect(importmap).not_to have_received(:pin).with("pgbus/stream_source_element", to: "pgbus/stream_source_element.js")
    end
  end

  describe "pgbus.logger initializer" do
    # Rails.logger exists when engine initializers run; a log_format set
    # before it did (config/application.rb) is applied to it here.
    it "applies the configured log_format to the logger pgbus now resolves" do
      allow(Pgbus.configuration).to receive(:apply_log_format!)

      initializer_block("pgbus.logger").call

      expect(Pgbus.configuration).to have_received(:apply_log_format!)
    end
  end

  describe "pgbus.finalize initializer" do
    after { restore_baseline_config }

    # The app has booted, so on_load(:after_initialize) runs the hook at once.
    it "finalizes the configuration after initialize" do
      Pgbus.reset!

      initializer_block("pgbus.finalize").call

      expect(Pgbus.configuration).to be_finalized
      expect(Ractor.shareable?(Pgbus.configuration.workers)).to be(true)
    end

    it "runs after the app's own initializer files" do
      expect(described_class.initializers.find { |i| i.name == "pgbus.finalize" }.after).to eq(:load_config_initializers)
    end

    # An initializer's Pgbus.configure leaves the configuration open: later
    # initializers and after_initialize blocks may still change it in place.
    it "leaves finalizing to after_initialize while the app boots" do
      Pgbus.reset!
      allow(Rails.application).to receive(:initialized?).and_return(false)

      Pgbus.configure { |c| c.streams_durable_patterns = [+"early"] }

      expect(Pgbus.configuration).not_to be_finalized
      expect(Pgbus.configuration.streams_durable_patterns).not_to be_frozen
    end
  end

  describe "pgbus.integrations.appsignal initializer" do
    subject(:block) { initializer_block("pgbus.integrations.appsignal") }

    after { restore_baseline_config }

    it "is a no-op when Appsignal is undefined" do
      skip "Appsignal is loaded in this environment" if defined?(Appsignal)

      block.call
      expect { ActiveSupport.run_load_hooks(:after_initialize, Rails.application) }.not_to raise_error
    end

    it "is a no-op when appsignal_enabled is false" do
      Pgbus.configuration.appsignal_enabled = false
      allow(Pgbus::Integrations::Appsignal).to receive(:install!) if defined?(Pgbus::Integrations::Appsignal)

      block.call
      ActiveSupport.run_load_hooks(:after_initialize, Rails.application)

      expect(Pgbus::Integrations::Appsignal).not_to have_received(:install!) if defined?(Pgbus::Integrations::Appsignal)
    end
  end
end
