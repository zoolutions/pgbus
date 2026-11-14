# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Deploy::Dependencies do
  def task(key, **options)
    Pgbus::Deploy::Task.from_configuration(key, class: "#{key.capitalize}Job", **options)
  end

  def outcome(key, version: "1", revision: "rev-a", completed: false, latest_failed: false, latest_id: 1)
    Pgbus::DeployTaskRun::Outcome.new(task_key: key, version: version, revision: revision, completed: completed,
                                      latest_failed: latest_failed, latest_id: latest_id)
  end

  describe ".prune" do
    def prune(*tasks, declared: tasks.map(&:key))
      valid, skipped = described_class.prune(tasks, declared)
      [valid.map(&:key).sort, skipped]
    end

    it "keeps tasks whose dependencies are all declared and valid" do
      expect(prune(task("a"), task("b", after: "a"), task("c", after: %w[a b]))).to eq([%w[a b c], {}])
    end

    it "skips a task naming an unknown key" do
      expect(prune(task("a"), task("b", after: "x"))).to eq([%w[a], { "b" => "after: unknown task 'x'" }])
    end

    it "skips a dependent of a task that was itself skipped, and that task's dependents in turn" do
      valid, skipped = prune(task("b", after: "a"), task("c", after: "b"), declared: %w[a b c])

      expect(valid).to eq([])
      expect(skipped).to eq("b" => "after: 'a' is skipped", "c" => "after: 'b' is skipped")
    end

    it "skips every task in a cycle of three, naming it, and the tasks that wait on the cycle" do
      valid, skipped = prune(task("a", after: "c"), task("b", after: "a"), task("c", after: "b"),
                             task("d", after: "a"), task("e"))

      expect(valid).to eq(%w[e])
      expect(skipped.slice("a", "b", "c").values).to all(eq("after: cycle a → c → b → a"))
      expect(skipped["d"]).to eq("after: 'a' is skipped")
    end

    it "skips a two-task cycle" do
      _, skipped = prune(task("a", after: "b"), task("b", after: "a"))

      expect(skipped).to eq("a" => "after: cycle a → b → a", "b" => "after: cycle a → b → a")
    end
  end

  describe "#state" do
    let(:revision) { "rev-b" }

    def state(dependency, *outcomes)
      described_class.new(outcomes, revision: revision).state(dependency)
    end

    it "is nil (satisfied) for a completed run at the dependency's version, at any revision" do
      expect(state(task("a", version: 2), outcome("a", version: "2", revision: "old", completed: true))).to be_nil
    end

    it "is not_run without a run at the current version" do
      expect(state(task("a", version: 2), outcome("a", version: "1", completed: true))).to eq("not_run")
      expect(state(task("a"))).to eq("not_run")
    end

    it "is running while the newest run at the version has no outcome" do
      expect(state(task("a"), outcome("a", latest_failed: true, latest_id: 1),
                   outcome("a", revision: "rev-b", latest_id: 2))).to eq("running")
    end

    it "is failed when the newest run at the version failed" do
      expect(state(task("a"), outcome("a", latest_failed: true, latest_id: 3),
                   outcome("a", revision: "rev-b", latest_id: 2))).to eq("failed")
    end

    it "needs this revision for an every_deploy dependency" do
      warm = task("warm", every_deploy: true)

      expect(state(warm, outcome("warm", revision: "rev-a", completed: true))).to eq("not_run")
      expect(state(warm, outcome("warm", revision: "rev-b", completed: true))).to be_nil
    end

    it "never satisfies an every_deploy dependency without a revision" do
      expect(described_class.new([outcome("warm", revision: nil, completed: true)], revision: nil)
        .state(task("warm", every_deploy: true))).to eq("not_run")
    end
  end

  describe "#waiting_on" do
    it "lists each unsatisfied dependency with its state, a missing one as not run" do
      deps = described_class.new([outcome("a", completed: true), outcome("b", latest_failed: true)], revision: "r")
      by_key = { "a" => task("a"), "b" => task("b") }

      expect(deps.waiting_on(%w[a b gone], by_key))
        .to eq([{ key: "b", state: "failed" }, { key: "gone", state: "not_run" }])
    end
  end

  describe ".load" do
    it "reads the outcomes of the given dependencies in one call" do
      allow(Pgbus::DeployTaskRun).to receive(:outcomes_for).and_return([outcome("a", completed: true)])

      deps = described_class.load([task("a"), task("b"), task("a")], revision: "r")

      expect(Pgbus::DeployTaskRun).to have_received(:outcomes_for).with(%w[a b])
      expect(deps.state(task("a"))).to be_nil
    end
  end
end
