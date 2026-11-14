# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Deploy::Task do
  describe ".from_configuration" do
    it "reads string keys" do
      task = described_class.from_configuration(
        "backfill", "class" => "BackfillJob", "version" => 2, "queue" => "maintenance",
                    "args" => [{ "batch_size" => 500 }], "priority" => 3, "description" => "Recompute totals"
      )

      expect(task).to have_attributes(
        key: "backfill", class_name: "BackfillJob", command: nil, version: "2", every_deploy: false,
        queue_name: "maintenance", arguments: [{ "batch_size" => 500 }], priority: 3, description: "Recompute totals"
      )
    end

    it "reads symbol keys" do
      task = described_class.from_configuration("warm", command: "Cache.warm!", every_deploy: true)

      expect(task).to have_attributes(command: "Cache.warm!", every_deploy: true)
    end

    it "defaults version to \"1\", priority to 0 and arguments to []" do
      task = described_class.from_configuration("backfill", class: "BackfillJob")

      expect(task).to have_attributes(version: "1", priority: 0, arguments: [])
    end

    it "keeps a bare Hash as one argument" do
      task = described_class.from_configuration("backfill", class: "BackfillJob", args: { "batch_size" => 500 })

      expect(task.arguments).to eq([{ "batch_size" => 500 }])
    end

    it "wraps a single argument in an Array" do
      task = described_class.from_configuration("backfill", class: "BackfillJob", args: 42)

      expect(task.arguments).to eq([42])
    end

    it "reads after: as one key or a list of keys, defaulting to none" do
      expect(described_class.from_configuration("b", class: "BJob", after: "a").dependencies).to eq(%w[a])
      expect(described_class.from_configuration("c", class: "CJob", after: %w[a b a]).dependencies).to eq(%w[a b])
      expect(described_class.from_configuration("d", class: "DJob", after: :a).dependencies).to eq(%w[a])
      expect(described_class.from_configuration("e", class: "EJob").dependencies).to eq([])
    end

    it "freezes the dependencies" do
      expect(described_class.from_configuration("b", class: "BJob", after: "a").dependencies).to be_frozen
    end
  end

  describe "#valid?" do
    it "accepts a class task" do
      expect(described_class.from_configuration("a", class: "BackfillJob")).to be_valid
    end

    it "accepts a command task" do
      expect(described_class.from_configuration("a", command: "Cache.warm!")).to be_valid
    end

    it "requires a class or a command" do
      task = described_class.from_configuration("a", version: 2)

      expect(task).not_to be_valid
      expect(task.errors).to include("Either class or command is required")
    end

    it "rejects a blank version" do
      task = described_class.from_configuration("a", class: "BackfillJob", version: " ")

      expect(task).not_to be_valid
      expect(task.errors).to include("Version must not be blank")
    end

    it "rejects a task that declares both class and command" do
      task = described_class.from_configuration("a", class: "BackfillJob", command: "Cache.warm!")

      expect(task).not_to be_valid
      expect(task.errors).to include("Declare either class or command, not both")
    end

    it "rejects a blank key" do
      task = described_class.from_configuration(" ", class: "BackfillJob")

      expect(task).not_to be_valid
      expect(task.errors).to include("Key must not be blank")
    end

    it "rejects a class or command that is not a String" do
      expect(described_class.from_configuration("a", class: { "name" => "X" })).not_to be_valid
      expect(described_class.from_configuration("a", command: ["X.y"]).errors)
        .to include("command must be a String")
    end

    it "rejects a non-boolean every_deploy" do
      task = described_class.from_configuration("a", class: "BackfillJob", every_deploy: "yes")

      expect(task).not_to be_valid
      expect(task.errors).to include("every_deploy must be true or false")
    end

    it "rejects an after: entry that is not a task key" do
      task = described_class.from_configuration("b", class: "BJob", after: ["a", { "x" => 1 }, " "])

      expect(task).not_to be_valid
      expect(task.errors).to include("after: must name task keys")
    end

    it "rejects a task that names itself in after:" do
      task = described_class.from_configuration("a", class: "AJob", after: %w[b a])

      expect(task).not_to be_valid
      expect(task.errors).to include("after: cycle a \u2192 a")
    end
  end

  describe "#warnings" do
    it "warns that version is ignored when every_deploy is set" do
      task = described_class.from_configuration("a", class: "BackfillJob", every_deploy: true, version: 3)

      expect(task).to be_valid
      expect(task.warnings).to include(/version is ignored/)
    end

    it "has none for a plain versioned task, after validation" do
      task = described_class.from_configuration("a", class: "BackfillJob", version: 3)

      expect(task).to be_valid
      expect(task.warnings).to be_empty
    end

    it "validates on first read" do
      task = described_class.from_configuration("a", class: "BackfillJob", every_deploy: true, version: 3)

      expect(task.warnings).to include(/version is ignored/)
    end
  end

  describe "#armed_for" do
    it "is the version for a versioned task" do
      task = described_class.from_configuration("a", class: "BackfillJob", version: 4)

      expect(task.armed_for("abc123")).to eq("4")
    end

    it "is the revision for an every_deploy task" do
      task = described_class.from_configuration("a", class: "BackfillJob", every_deploy: true)

      expect(task.armed_for("abc123")).to eq("abc123")
      expect(task.armed_for(nil)).to be_nil
    end
  end

  describe "#job_class" do
    it "resolves the class name" do
      expect(described_class.from_configuration("a", class: "String").job_class).to eq(String)
    end

    it "is nil for an unknown class or a command task" do
      expect(described_class.from_configuration("a", class: "NoSuchJob").job_class).to be_nil
      expect(described_class.from_configuration("a", command: "X.y").job_class).to be_nil
    end
  end

  describe "#to_h" do
    it "returns the declared attributes" do
      task = described_class.from_configuration("a", command: "Cache.warm!", queue: "low", every_deploy: true)

      expect(task.to_h).to eq(
        key: "a", class_name: nil, command: "Cache.warm!", version: "1", every_deploy: true,
        queue_name: "low", arguments: [], priority: 0, description: nil, dependencies: []
      )
    end
  end
end
