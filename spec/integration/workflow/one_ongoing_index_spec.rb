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
  let(:columns) { %i[workflow_class subject_type subject_id] }

  def template_source(template)
    File.read(File.expand_path("../../../lib/generators/pgbus/templates/#{template}", __dir__))
  end

  def dumped_predicate(name)
    conn.indexes(:pgbus_workflows).find { |index| index.name == name }.where
  end

  # db:schema:load builds the index from the predicate schema.rb recorded.
  def reload_from_dump(name, predicate)
    conn.remove_index :pgbus_workflows, name: name
    conn.add_index :pgbus_workflows, columns, unique: true, where: predicate, name: name
  end

  # Runs the generated pgbus:add_workflows migration in a schema of its own,
  # so it creates the real tables instead of finding the bootstrap ones.
  it "dumps the same predicate after a schema load as after the add_workflows migration" do
    version = "[#{ActiveRecord::VERSION::MAJOR}.#{ActiveRecord::VERSION::MINOR}]"
    source = ERB.new(template_source("add_workflows.rb.erb")).result_with_hash(migration_version: version)
    namespace = Module.new
    namespace.module_eval(source, "add_workflows.rb")
    migration = namespace.const_get(:AddPgbusWorkflows).new

    conn.transaction do
      conn.execute("CREATE SCHEMA pgbus_one_ongoing_probe")
      conn.execute("SET LOCAL search_path TO pgbus_one_ongoing_probe")
      migration.suppress_messages { migration.exec_migration(conn, :up) }
      migrated = dumped_predicate(index_name)

      reload_from_dump(index_name, migrated)

      expect(dumped_predicate(index_name)).to eq(migrated)
      raise ActiveRecord::Rollback
    end
  end

  # The fresh-install migration also installs PGMQ and creates queues, so its
  # predicate is taken from the template and round-tripped on its own index.
  it "dumps the same predicate after a schema load as after the install migration" do
    predicate = template_source("migration.rb.erb")[/where: "([^"]+)",\s+name: "#{index_name}"/, 1]
    expect(predicate).not_to be_nil

    conn.transaction do
      probe = "idx_pgbus_workflows_install_probe"
      conn.add_index :pgbus_workflows, columns, unique: true, where: predicate, name: probe
      migrated = dumped_predicate(probe)

      reload_from_dump(probe, migrated)

      expect(dumped_predicate(probe)).to eq(migrated)
      raise ActiveRecord::Rollback
    end
  end
end
