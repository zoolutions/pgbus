# frozen_string_literal: true

require_relative "../integration_helper"
require "erb"

# Issue #583: pgbus:add_failed_events_source_queue moves pgbus_failed_events
# from one row per (queue_name, msg_id) to one per (queue_name, msg_id,
# source_queue). Run against a real table in a schema of its own, rolled back.
RSpec.describe "pgbus_failed_events source_queue migration (integration)", :integration do
  let(:conn) { ActiveRecord::Base.connection }
  let(:scratch_schema) { "pgbus_failed_source_probe_#{Process.pid}" }
  let(:migration) { migration_from("add_failed_events_source_queue.rb.erb", :AddPgbusFailedEventsSourceQueue) }

  def migration_from(template, class_name, **locals)
    path = File.expand_path("../../lib/generators/pgbus/templates/#{template}", __dir__)
    version = "[#{ActiveRecord::VERSION::MAJOR}.#{ActiveRecord::VERSION::MINOR}]"
    source = ERB.new(File.read(path)).result_with_hash(migration_version: version, **locals)
    namespace = Module.new
    namespace.module_eval(source, path)
    namespace.const_get(class_name).new
  end

  def run(migration, direction)
    migration.suppress_messages { migration.exec_migration(conn, direction) }
  end

  def in_scratch_schema
    conn.transaction do
      conn.execute("CREATE SCHEMA #{scratch_schema}")
      conn.execute("SET LOCAL search_path TO #{scratch_schema}")
      yield
      raise ActiveRecord::Rollback
    end
  end

  def unique_indexes
    conn.indexes(:pgbus_failed_events).select(&:unique).to_h { |index| [index.name, index.columns] }
  end

  def insert(queue, msg_id, source = nil, error = "boom")
    columns = source ? "queue_name, msg_id, source_queue, error_message" : "queue_name, msg_id, error_message"
    values = source ? [queue, msg_id, source, error] : [queue, msg_id, error]
    conn.exec_insert(
      "INSERT INTO pgbus_failed_events (#{columns}) VALUES (#{values.each_index.map { |i| "$#{i + 1}" }.join(", ")})",
      "spec", values
    )
  end

  # The table as pgbus:install created it before this migration.
  def create_old_table
    conn.execute(<<~SQL)
      CREATE TABLE pgbus_failed_events (
        id BIGSERIAL PRIMARY KEY, queue_name VARCHAR NOT NULL, msg_id BIGINT, payload JSONB, headers JSONB,
        error_class VARCHAR, error_message TEXT, backtrace TEXT, retry_count INTEGER DEFAULT 0,
        failed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
      );
      CREATE UNIQUE INDEX idx_pgbus_failed_events_queue_msg ON pgbus_failed_events (queue_name, msg_id);
    SQL
  end

  it "keeps the old rows, with no source_queue, and lets two sub-queues' failures share a msg_id" do
    in_scratch_schema do
      create_old_table
      insert("default", 1)
      insert("mailers", 1)

      run(migration, :up)

      expect(unique_indexes).to eq("idx_pgbus_failed_events_queue_msg_source" => %w[queue_name msg_id source_queue])
      expect(conn.select_rows("SELECT queue_name, source_queue FROM pgbus_failed_events ORDER BY id"))
        .to eq([["default", nil], ["mailers", nil]])

      insert("default", 7, "pgbus_default_p0")
      insert("default", 7, "pgbus_default_p1")
      expect { insert("default", 7, "pgbus_default_p1") }.to raise_error(ActiveRecord::RecordNotUnique)
    end
  end

  it "is a no-op when run again" do
    in_scratch_schema do
      create_old_table
      run(migration, :up)

      expect { run(migration, :up) }.not_to raise_error
      expect(unique_indexes.keys).to eq(%w[idx_pgbus_failed_events_queue_msg_source])
    end
  end

  it "rolls back to one row per queue and msg_id, keeping the latest" do
    in_scratch_schema do
      create_old_table
      run(migration, :up)
      insert("default", 7, "pgbus_default_p0", "first")
      insert("default", 7, "pgbus_default_p1", "second")

      run(migration, :down)

      expect(unique_indexes).to eq("idx_pgbus_failed_events_queue_msg" => %w[queue_name msg_id])
      expect(conn.column_exists?(:pgbus_failed_events, :source_queue)).to be(false)
      expect(conn.select_rows("SELECT msg_id, error_message FROM pgbus_failed_events")).to eq([[7, "second"]])
    end
  end

  it "is not undone by the old unique-index generator run afterwards" do
    old_index = migration_from("add_failed_events_unique_index.rb.erb", :AddPgbusFailedEventsUniqueIndex)

    in_scratch_schema do
      create_old_table
      run(migration, :up)
      run(old_index, :up)

      expect(unique_indexes.keys).to eq(%w[idx_pgbus_failed_events_queue_msg_source])
    end
  end

  # A fresh install lands on the same shape as an upgraded one. The install
  # migration's PGMQ statements are skipped: the database has PGMQ already.
  it "matches the table a fresh pgbus:install creates" do
    install = migration_from("migration.rb.erb", :CreatePgbusTables, pgmq_schema_mode: "embedded")
    def install.execute(sql, ...)
      super unless sql.include?("pgmq.")
    end

    in_scratch_schema do
      run(install, :up)

      expect(unique_indexes).to eq("idx_pgbus_failed_events_queue_msg_source" => %w[queue_name msg_id source_queue])
    end
  end
end
