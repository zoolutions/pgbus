# frozen_string_literal: true

require "spec_helper"
require "tmpdir"
require "fileutils"
require "stringio"
require "rails/generators"
require "generators/pgbus/install_generator"

RSpec.describe Pgbus::Generators::InstallGenerator do
  describe "migration template" do
    let(:template_path) do
      File.expand_path("../../../lib/generators/pgbus/templates/migration.rb.erb", __dir__)
    end

    it "exists" do
      expect(File.exist?(template_path)).to be true
    end

    it "uses enable_extension only conditionally, not as the sole install path" do
      content = File.read(template_path)
      expect(content).to include("Pgbus::PgmqSchema.install_sql")
      expect(content).to include("extension_available?")
    end

    it "references PgmqSchema for installation" do
      content = File.read(template_path)
      expect(content).to include("Pgbus::PgmqSchema")
    end

    it "includes pgmq_schema_mode detection" do
      content = File.read(template_path)
      expect(content).to include("pgmq_schema_mode")
    end

    it "creates pgbus_processed_events with the two-phase completed_at column" do
      content = File.read(template_path)
      expect(content).to include("t.datetime :completed_at")
    end

    # Same tables and indexes as pgbus:add_workflows (issue #540), and the
    # rollback drops them children first.
    it "creates the three workflow tables and drops them on rollback" do
      content = File.read(template_path)
      %w[pgbus_workflows pgbus_workflow_steps pgbus_workflow_events].each do |table|
        expect(content).to include("create_table :#{table} do |t|")
        expect(content).to include("drop_table :#{table}")
      end
      expect(content).to include('name: "idx_pgbus_workflows_one_ongoing"', 'name: "idx_pgbus_workflows_idempotency"')
      expect(content).to include(%(where: "status::text IN ('ready','performing','waiting','paused')"))
      expect(content.index("drop_table :pgbus_workflow_events")).to be < content.index("drop_table :pgbus_workflows\n")
      expect(content.index("drop_table :pgbus_workflow_steps")).to be < content.index("drop_table :pgbus_workflows\n")
    end

    it "carries the waiting-step columns and the signals table (issue #541)" do
      content = File.read(template_path)
      expect(content).to include("t.datetime :timeout_at", "t.string :batch_id", "t.bigint :signal_id")
      expect(content).to include("create_table :pgbus_workflow_signals do |t|")
      expect(content).to include('name: "idx_pgbus_workflow_signals_idempotency"')
      expect(content.index("drop_table :pgbus_workflow_signals")).to be < content.index("drop_table :pgbus_workflows\n")
    end
  end

  describe "initializer template" do
    let(:template_path) do
      File.expand_path("../../../lib/generators/pgbus/templates/initializer.rb.erb", __dir__)
    end

    it "exists" do
      expect(File.exist?(template_path)).to be true
    end

    it "wraps configuration in a Pgbus.configure block" do
      content = File.read(template_path)
      expect(content).to include("Pgbus.configure do |c|")
    end

    it "bakes in the #311 realtime broadcast-queue default" do
      content = File.read(template_path)
      expect(content).to include('c.streams_broadcast_queue = "realtime"')
    end

    it "backs the realtime queue with a dedicated capsule" do
      content = File.read(template_path)
      expect(content).to include("realtime")
      expect(content).to match(/c\.capsule\s+:realtime/)
    end
  end

  describe "end-to-end install" do
    let(:tmpdir) { Dir.mktmpdir }
    let(:initializer_path) { File.join(tmpdir, "config", "initializers", "pgbus.rb") }
    let(:legacy_yml_path) { File.join(tmpdir, "config", "pgbus.yml") }

    before do
      FileUtils.mkdir_p(File.join(tmpdir, "config"))
      generator = described_class.new([], {}, destination_root: tmpdir)
      generator.destination_root = tmpdir
      capture_io { generator.create_config_file }
    end

    after { FileUtils.rm_rf(tmpdir) }

    it "creates config/initializers/pgbus.rb" do
      expect(File.exist?(initializer_path)).to be true
    end

    it "does not create the legacy config/pgbus.yml" do
      expect(File.exist?(legacy_yml_path)).to be false
    end

    it "generates Ruby that loads via Pgbus.configure and passes validate!" do
      expect { eval_initializer(initializer_path) }.not_to raise_error
    end

    it "produces a realtime capsule that drains the realtime broadcast queue" do
      config = eval_initializer(initializer_path)
      broadcast_queue = config.streams_broadcast_queue
      drains = Array(config.workers).any? do |w|
        queues = w[:queues] || w["queues"] || []
        queues.include?("*") || queues.include?(broadcast_queue)
      end
      expect(broadcast_queue).to eq("realtime")
      expect(drains).to be true
    end

    it "leaves no queue drained by more than one worker" do
      # Guards against the built-in default capsule (seeded by
      # Configuration#initialize) surviving alongside the generated named
      # capsules, which would drain the "default" queue twice.
      config = eval_initializer(initializer_path)
      all_queues = Array(config.workers).flat_map { |w| Array(w[:queues] || w["queues"]) }
      duplicated = all_queues.tally.select { |_queue, count| count > 1 }
      expect(duplicated).to be_empty, "queues drained by multiple workers: #{duplicated.keys.inspect}"
    end

    it "keeps every worker a named capsule (the built-in anonymous default is cleared)" do
      config = eval_initializer(initializer_path)
      anonymous = Array(config.workers).reject { |w| w[:name] || w["name"] }
      expect(anonymous).to be_empty, "unexpected anonymous worker entries: #{anonymous.inspect}"
    end
  end

  describe "post-install messaging" do
    let(:tmpdir) { Dir.mktmpdir }

    after { FileUtils.rm_rf(tmpdir) }

    it "points users at the initializer, not the legacy yml" do
      generator = described_class.new([], {}, destination_root: tmpdir)
      generator.destination_root = tmpdir
      out, = capture_io { generator.display_post_install }
      expect(out).to include("config/initializers/pgbus.rb")
      expect(out).not_to include("config/pgbus.yml")
    end
  end

  # Load a generated initializer through the real Pgbus.configure path — which
  # runs validate! — then return the resulting configuration and reset the
  # global so specs stay isolated. This exercises the exact boot behavior a
  # Rails app gets loading config/initializers/pgbus.rb.
  def eval_initializer(path)
    Pgbus.reset!
    eval(File.read(path), TOPLEVEL_BINDING, path) # rubocop:disable Security/Eval
    Pgbus.configuration
  ensure
    Pgbus.reset!
  end

  def capture_io
    original_stdout = $stdout
    original_stderr = $stderr
    $stdout = StringIO.new
    $stderr = StringIO.new
    yield
    [$stdout.string, $stderr.string]
  ensure
    $stdout = original_stdout
    $stderr = original_stderr
  end
end
