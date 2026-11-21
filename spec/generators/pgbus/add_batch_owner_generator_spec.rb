# frozen_string_literal: true

require "spec_helper"
require "rails/generators"
require "generators/pgbus/add_batch_owner_generator"

RSpec.describe Pgbus::Generators::AddBatchOwnerGenerator do
  it_behaves_like "a pgbus generator", /owner/i

  describe "generated migration" do
    let(:basename) { "_add_pgbus_batch_owner.rb" }

    it "writes the migration into db/migrate by default" do
      generate_migration(described_class, basename: basename) do |path, _content|
        expect(path).not_to be_nil
      end
    end

    it "routes into db/pgbus_migrate when --database is set" do
      generate_migration(
        described_class,
        options: { database: "pgbus" },
        migrate_dir: "db/pgbus_migrate",
        basename: basename
      ) do |path, _content|
        expect(path).not_to be_nil
      end
    end

    it "adds the owner_process_id column idempotently" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to match(/class AddPgbusBatchOwner < ActiveRecord::Migration\[\d+\.\d+\]/)
        expect(content).to include("add_column :pgbus_batches, :owner_process_id, :bigint unless column_exists?")
        expect(content).to include("remove_column :pgbus_batches, :owner_process_id if column_exists?")
      end
    end
  end
end
