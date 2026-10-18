# frozen_string_literal: true

require_relative "../../integration_helper"

# Issue #573: schema.rb records a partial index's predicate as Postgres prints
# it back (`connection.indexes(...).where`), and db:schema:load replays that
# string. The one-ongoing predicate in the generator templates must be a fixed
# point of that round trip, or a migrated database and a schema-loaded one
# dump different schema.rb lines for the same index.
RSpec.describe "Workflow one-ongoing index predicate (integration)", :integration do
  let(:conn) { ActiveRecord::Base.connection }
  let(:columns) { %i[workflow_class subject_type subject_id] }

  def template_predicate(template)
    path = File.expand_path("../../../lib/generators/pgbus/templates/#{template}", __dir__)
    File.read(path)[/where: "([^"]+)",\s+name: "idx_pgbus_workflows_one_ongoing"/, 1] ||
      raise("no idx_pgbus_workflows_one_ongoing predicate in #{template}")
  end

  def dumped_predicate(index_name)
    conn.indexes(:pgbus_workflows).find { |index| index.name == index_name }.where
  end

  %w[add_workflows.rb.erb migration.rb.erb].each do |template|
    it "dumps the same predicate after a schema load as after the migration (#{template})" do
      conn.transaction do
        conn.add_index :pgbus_workflows, columns,
                       unique: true, where: template_predicate(template), name: "idx_pgbus_workflows_migrated_probe"
        migrated = dumped_predicate("idx_pgbus_workflows_migrated_probe")

        conn.add_index :pgbus_workflows, columns,
                       unique: true, where: migrated, name: "idx_pgbus_workflows_loaded_probe"

        expect(dumped_predicate("idx_pgbus_workflows_loaded_probe")).to eq(migrated)
        raise ActiveRecord::Rollback
      end
    end
  end
end
