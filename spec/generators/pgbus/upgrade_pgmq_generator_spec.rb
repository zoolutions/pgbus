# frozen_string_literal: true

require "spec_helper"

RSpec.describe "Pgbus upgrade PGMQ generator migration template" do # rubocop:disable RSpec/DescribeClass
  describe "upgrade migration template" do
    let(:template_path) do
      File.expand_path("../../../lib/generators/pgbus/templates/upgrade_pgmq.rb.erb", __dir__)
    end

    it "exists" do
      expect(File.exist?(template_path)).to be true
    end

    it "drops existing pgmq functions before re-creating" do
      content = File.read(template_path)
      expect(content).to include("drop_pgmq_functions_sql")
    end

    it "uses PgmqSchema for the new SQL" do
      content = File.read(template_path)
      expect(content).to include("Pgbus::PgmqSchema")
    end

    it "re-installs the NOTIFY insert triggers the function drop cascaded away (issue #360)" do
      content = File.read(template_path)
      expect(content).to include("reinstall_notify_triggers_sql")
      # The repair must run AFTER install_sql re-creates the trigger function.
      expect(content.index("install_sql")).to be < content.index("reinstall_notify_triggers_sql")
    end

    it "applies the table fixups the function drop-and-reapply cannot carry" do
      content = File.read(template_path)
      expect(content).to include("fixups_sql")
    end

    # Order matters twice over: install_sql must run first because the fixups
    # call pgmq.format_table_name, which the drop step removed; and the
    # NOTIFY repair is last so it sees the final schema.
    it "runs the fixups after the schema is re-installed and before the NOTIFY repair" do
      content = File.read(template_path)
      expect(content.index("install_sql")).to be < content.index("fixups_sql")
      expect(content.index("fixups_sql")).to be < content.index("reinstall_notify_triggers_sql")
    end

    it "reads the installed version to decide which fixups apply, tolerating no tracking table" do
      content = File.read(template_path)
      expect(content).to include("table_exists?")
      expect(content).to include("ORDER BY installed_at DESC")
    end

    it "tracks the version in pgbus_pgmq_schema_versions" do
      content = File.read(template_path)
      expect(content).to include("pgbus_pgmq_schema_versions")
    end

    it "records the install method as upgrade" do
      content = File.read(template_path)
      expect(content).to include("'upgrade'")
    end

    it "raises on down migration" do
      content = File.read(template_path)
      expect(content).to include("IrreversibleMigration")
    end
  end
end
