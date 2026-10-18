# frozen_string_literal: true

require_relative "../../integration_helper"
require "erb"

# Issue #573: schema.rb records a partial index's predicate as Postgres prints
# it back (`connection.indexes(...).where`), and db:schema:load replays that
# string. The one-ongoing predicate in the generator templates must be a fixed
# point of that round trip, or a migrated database and a schema-loaded one
# dump different schema.rb lines for the same index.
RSpec.describe "Workflow one-ongoing index predicate (integration)", :integration do
  let(:conn) { ActiveRecord::Base.connection }
  let(:index_name) { "idx_pgbus_workflows_one_ongoing" }
  let(:scratch_schema) { "pgbus_one_ongoing_probe_#{Process.pid}" }

  def migration_from(template, class_name, **locals)
    path = File.expand_path("../../../lib/generators/pgbus/templates/#{template}", __dir__)
    version = "[#{ActiveRecord::VERSION::MAJOR}.#{ActiveRecord::VERSION::MINOR}]"
    source = ERB.new(File.read(path)).result_with_hash(migration_version: version, **locals)
    namespace = Module.new
    namespace.module_eval(source, path)
    namespace.const_get(class_name).new
  end

  def dumped_predicate
    conn.indexes(:pgbus_workflows).find { |index| index.name == index_name }.where
  end

  # Runs the migration in a schema of its own, so it creates the real tables
  # instead of finding the bootstrap ones, then rebuilds the index from its
  # dumped predicate the way db:schema:load does. Rolled back afterwards.
  def expect_round_trip_after(migration)
    conn.transaction do
      conn.execute("CREATE SCHEMA #{scratch_schema}")
      conn.execute("SET LOCAL search_path TO #{scratch_schema}")
      migration.suppress_messages { migration.exec_migration(conn, :up) }
      migrated = dumped_predicate

      conn.remove_index :pgbus_workflows, name: index_name
      conn.add_index :pgbus_workflows, %i[workflow_class subject_type subject_id],
                     unique: true, where: migrated, name: index_name

      expect(dumped_predicate).to eq(migrated)
      raise ActiveRecord::Rollback
    end
  end

  it "dumps the same predicate after a schema load as after the add_workflows migration" do
    expect_round_trip_after(migration_from("add_workflows.rb.erb", :AddPgbusWorkflows))
  end

  # The install migration also installs PGMQ, creates the default queues and
  # tunes their tables. The integration database has PGMQ already, so the
  # statements that touch it are skipped; every pgbus table is still created.
  it "dumps the same predicate after a schema load as after the install migration" do
    migration = migration_from("migration.rb.erb", :CreatePgbusTables, pgmq_schema_mode: "embedded")
    def migration.execute(sql, ...)
      super unless sql.include?("pgmq")
    end

    expect_round_trip_after(migration)
  end
end
