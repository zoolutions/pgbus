# frozen_string_literal: true

require_relative "../../integration_helper"

RSpec.describe Pgbus::DeployTask, :integration do
  let(:now) { Time.utc(2026, 10, 16, 12, 0, 0) }

  def task(key = "backfill", **options)
    Pgbus::Deploy::Task.from_configuration(key, class: "BackfillJob", **options)
  end

  def sync(*tasks, revision: "rev-b")
    described_class.sync_from_config!(tasks, revision: revision, now: now)
  end

  def rollover_run(key, version:, revision: "rev-a")
    Pgbus::DeployTaskRun.create!(task_key: key, version: version, revision: revision,
                                 triggered_by: "rollover", run_at: now - 3600)
  end

  def row(key = "backfill")
    described_class.find_by!(key: key)
  end

  describe ".sync_from_config!" do
    it "upserts the declared attributes as a static row" do
      sync(task(version: 2, queue: "maintenance", args: [{ "batch_size" => 500 }], priority: 3,
                description: "Recompute totals"))

      expect(row).to have_attributes(
        class_name: "BackfillJob", version: "2", every_deploy: false, queue_name: "maintenance",
        priority: 3, description: "Recompute totals", static: true, enabled: true
      )
      expect(row.parsed_arguments).to eq([{ "batch_size" => 500 }])
    end

    it "arms a new task for its version" do
      sync(task)

      expect(row).to have_attributes(armed_at: now, armed_for: "1")
    end

    it "arms a declared version that has no rollover run" do
      rollover_run("backfill", version: "1")

      sync(task(version: 2))

      expect(row).to have_attributes(armed_at: now, armed_for: "2")
    end

    it "does not arm a declared version that already has a rollover run, even when the stored version differs" do
      sync(task(version: 2))
      described_class.where(key: "backfill").update_all(armed_at: nil, armed_for: nil, version: "9")
      rollover_run("backfill", version: "2")

      sync(task(version: 2))

      expect(row.armed_at).to be_nil
    end

    it "still arms a task whose only run was manual" do
      Pgbus::DeployTaskRun.create!(task_key: "backfill", version: "1", revision: "rev-b",
                                   triggered_by: "manual", run_at: now - 60)

      sync(task)

      expect(row.armed_for).to eq("1")
    end

    it "does not re-arm v1 when an old-revision scheduler re-syncs after v1 ran" do
      rollover_run("backfill", version: "1")
      sync(task(version: 2))

      sync(task(version: 1), revision: "rev-a")

      expect(row).to have_attributes(armed_at: now, armed_for: "2")
    end

    it "supersedes a pending arm with a later bump, keeping armed_at" do
      sync(task(version: 2))
      later = now + 600

      described_class.sync_from_config!([task(version: 3)], revision: "rev-c", now: later)

      expect(row).to have_attributes(armed_at: now, armed_for: "3")
    end

    it "arms an every_deploy task with no run for the revision" do
      sync(task(every_deploy: true))

      expect(row).to have_attributes(armed_at: now, armed_for: "rev-b")
    end

    it "does not arm an every_deploy task whose last rollover run is this revision" do
      rollover_run("backfill", version: "1", revision: "rev-b")

      sync(task(every_deploy: true))

      expect(row.armed_at).to be_nil
    end

    it "arms an every_deploy task whose last rollover run is another revision (a rollback re-fires it)" do
      rollover_run("backfill", version: "1", revision: "rev-b")
      rollover_run("backfill", version: "1", revision: "rev-a")

      sync(task(every_deploy: true))

      expect(row.armed_for).to eq("rev-b")
    end

    it "does not arm an every_deploy task without a revision" do
      sync(task(every_deploy: true), revision: nil)

      expect(row.armed_at).to be_nil
    end

    it "deletes static rows no longer declared" do
      sync(task("old"), task("kept"))

      sync(task("kept"))

      expect(described_class.pluck(:key)).to eq(["kept"])
    end

    it "preserves enabled: false and an existing arm" do
      sync(task)
      described_class.where(key: "backfill").update_all(enabled: false)

      described_class.sync_from_config!([task], revision: "rev-b", now: now + 600)

      expect(row).to have_attributes(enabled: false, armed_at: now, armed_for: "1")
    end
  end

  describe "#claim!" do
    before { sync(task) }

    it "returns true once, then false" do
      expect(row.claim!(armed_for: "1")).to be(true)
      expect(row.claim!(armed_for: "1")).to be(false)
      expect(row.armed_at).to be_nil
    end

    it "returns false for a different armed_for and leaves the arm" do
      expect(row.claim!(armed_for: "2")).to be(false)
      expect(row.armed_for).to eq("1")
      expect(row.armed_at).to eq(now)
    end
  end

  describe "#take_over!" do
    it "re-stamps an arm still held for the value it was read with" do
      sync(task(version: 2))
      stale = row

      expect(stale.take_over!(armed_for: "1", now: now + 60)).to be(true)
      expect(row.armed_for).to eq("1")
      expect(stale.take_over!(armed_for: "3", now: now + 60)).to be(false)
    end
  end

  describe ".pending?" do
    it "is true only while an enabled task is armed" do
      sync(task)
      expect(described_class.pending?).to be(true)

      described_class.update_all(enabled: false)
      expect(described_class.pending?).to be(false)
    end
  end

  describe "#rearm!" do
    it "restores the arm" do
      sync(task)
      row.claim!(armed_for: "1")

      row.rearm!(armed_for: "1", now: now + 60)

      expect(row).to have_attributes(armed_at: now + 60, armed_for: "1")
    end
  end

  describe "#rearm! after another scheduler re-armed" do
    it "leaves the other arm alone" do
      sync(task)
      stale = row
      stale.claim!(armed_for: "1")
      described_class.where(id: stale.id).update_all(armed_at: now + 30, armed_for: "2")

      stale.rearm!(armed_for: "1", now: now + 60)

      expect(row).to have_attributes(armed_at: now + 30, armed_for: "2")
    end
  end

  describe "#to_task with corrupt arguments" do
    it "raises instead of dropping the arguments, while the display falls back to none" do
      sync(task)
      described_class.where(key: "backfill").update_all(arguments: "{not json")

      expect(row.parsed_arguments).to eq([])
      expect { row.to_task }.to raise_error(JSON::ParserError)
    end
  end

  describe "scopes" do
    it "selects armed and enabled rows" do
      sync(task("a"), task("b"), task("c"))
      described_class.where(key: "b").update_all(armed_at: nil)
      described_class.where(key: "c").update_all(enabled: false)

      expect(described_class.armed.order(:key).pluck(:key)).to eq(%w[a c])
      expect(described_class.enabled.armed.pluck(:key)).to eq(%w[a])
    end
  end

  describe "#to_task" do
    it "rebuilds the declared task from the row" do
      sync(task("warm", class: nil, command: "Cache.warm!", every_deploy: true, queue: "low"))

      expect(row("warm").to_task.to_h).to include(command: "Cache.warm!", every_deploy: true, queue_name: "low")
    end
  end
end
