# frozen_string_literal: true

require "spec_helper"
require "active_support/core_ext/numeric/time"
# String#to_time, as in a Rails app: a String must still not pass as a wait_until.
require "active_support/core_ext/string/conversions"

RSpec.describe "Pgbus::Workflow step declarations" do # rubocop:disable RSpec/DescribeClass
  def workflow_class(name = "SpecWorkflow", parent: Pgbus::Workflow, &body)
    Class.new(parent) do
      define_singleton_method(:name) { name }
      class_eval(&body) if body
    end
  end

  describe "step forms" do
    it "takes a named block, an anonymous block, a method name and `step def`" do
      klass = workflow_class do
        step(:welcome) { :ran }
        step { :anonymous }
        step :remind
        step def complete = :done

        def remind = :reminded
      end

      expect(klass.workflow_steps.names).to eq(%w[welcome step_2 remind complete])
      expect(klass.workflow_steps.fetch("remind").body).to eq(:remind)
      expect(klass.workflow_steps.fetch(:complete).body).to eq(:complete)
    end

    it "names an anonymous step past a name already taken" do
      klass = workflow_class do
        step("step_2") { nil }
        step { nil }
      end

      expect(klass.workflow_steps.names).to eq(%w[step_2 step_3])
    end

    it "keeps each step's wait, skip_if, timeout and job options on a frozen definition" do
      klass = workflow_class do
        step :reminder, wait: 2.days, skip_if: -> { true }, timeout: 30.seconds, job_options: { queue: :mail, priority: 2 } do
          nil
        end
      end

      definition = klass.workflow_steps.fetch(:reminder)
      expect(definition).to be_frozen
      expect(definition).to have_attributes(name: "reminder", wait: 2.days.to_i, timeout: 30, job_options: { queue: "mail", priority: 2 })
      expect(definition.skip_if).to be_a(Proc)
    end

    it "defaults to no wait and no timeout" do
      definition = workflow_class { step(:only) { nil } }.workflow_steps.fetch(:only)

      expect(definition.wait).to eq(0)
      expect(definition.timeout).to be_nil
    end
  end

  describe "ordering" do
    it "inserts with before_step: and after_step:" do
      klass = workflow_class do
        step(:a) { nil }
        step(:c) { nil }
        step(:b, after_step: :a) { nil }
        step(:start, before_step: :a) { nil }
      end

      expect(klass.workflow_steps.names).to eq(%w[start a b c])
    end

    it "rejects a before_step: or after_step: naming an unknown step" do
      expect { workflow_class { step(:a, after_step: :missing) { nil } } }
        .to raise_error(ArgumentError, /after_step: :missing.*no step named missing/)
    end

    it "rejects before_step: and after_step: together" do
      expect do
        workflow_class do
          step(:a) { nil }
          step(:b, before_step: :a, after_step: :a) { nil }
        end
      end.to raise_error(ArgumentError, /before_step: or after_step:, not both/)
    end

    it "answers next_after for every step, and nil after the last" do
      steps = workflow_class do
        step(:a) { nil }
        step(:b) { nil }
      end.workflow_steps

      expect(steps.first.name).to eq("a")
      expect(steps.next_after("a").name).to eq("b")
      expect(steps.next_after("b")).to be_nil
      expect(steps.next_after("unknown")).to be_nil
    end
  end

  describe "removed_step" do
    it "leaves a gravestone where the step was, so next_after skips past it" do
      klass = workflow_class do
        step(:welcome) { nil }
        removed_step :reminder
        step(:complete) { nil }
      end

      expect(klass.workflow_steps.names).to eq(%w[welcome complete])
      expect(klass.workflow_steps.removed?(:reminder)).to be(true)
      expect(klass.workflow_steps.fetch(:reminder)).to be_nil
      expect(klass.workflow_steps.next_after("reminder").name).to eq("complete")
    end

    it "removes a step a parent declared, in the subclass only" do
      parent = workflow_class("ParentWorkflow") do
        step(:a) { nil }
        step(:b) { nil }
      end
      child = workflow_class("ChildWorkflow", parent: parent) { removed_step :b }

      expect(child.workflow_steps.names).to eq(%w[a])
      expect(parent.workflow_steps.names).to eq(%w[a b])
    end

    it "rejects a step declared with a removed name" do
      expect do
        workflow_class do
          removed_step :old
          step(:old) { nil }
        end
      end.to raise_error(ArgumentError, /old is a removed step/)
    end
  end

  describe "inheritance" do
    it "lets a subclass add steps without touching the parent's registry" do
      parent = workflow_class("ParentWorkflow") { step(:a) { nil } }
      child = workflow_class("ChildWorkflow", parent: parent) { step(:b) { nil } }

      expect(child.workflow_steps.names).to eq(%w[a b])
      expect(parent.workflow_steps.names).to eq(%w[a])
      expect(parent.workflow_steps).to be_frozen
    end
  end

  describe "validation at class load" do
    it "rejects a duplicate step name" do
      expect do
        workflow_class do
          step(:a) { nil }
          step(:a) { :again }
        end
      end.to raise_error(ArgumentError, /duplicate step a/)
    end

    it "rejects a job_options: wait_until that is not a time, a String included" do
      ["not a time", 42].each do |bad|
        expect { workflow_class { step(:a, job_options: { wait_until: bad }) { nil } } }
          .to raise_error(ArgumentError, /wait_until must be a Time/), "for #{bad.inspect}"
      end

      at = Time.utc(2030, 1, 1)
      expect(workflow_class { step(:a, job_options: { wait_until: at }) { nil } }.workflow_steps.fetch(:a).job_options)
        .to eq(wait_until: at)
    end

    it "rejects a blank queue and a non-Integer priority in job_options:" do
      [nil, "", "  "].each do |bad|
        expect { workflow_class { step(:a, job_options: { queue: bad }) { nil } } }
          .to raise_error(ArgumentError, /queue must be a queue name/), "for #{bad.inspect}"
      end
      expect { workflow_class { step(:a, job_options: { priority: "high" }) { nil } } }
        .to raise_error(ArgumentError, /priority must be an Integer/)
    end

    it "rejects an infinite or NaN wait:/timeout: with ArgumentError" do
      [Float::INFINITY, Float::NAN].each do |bad|
        expect { workflow_class { step(:a, timeout: bad) { nil } } }.to raise_error(ArgumentError, /timeout:/)
        expect { workflow_class { step(:a, wait: bad) { nil } } }.to raise_error(ArgumentError, /wait:/)
      end
    end

    it "raises ArgumentError for a before_step: that is not a name" do
      expect { workflow_class { step(:a, before_step: 1) { nil } } }.to raise_error(ArgumentError, /before_step: :"1"/)
    end

    it "rejects an unknown job_options: key" do
      expect { workflow_class { step(:a, job_options: { retry: 3 }) { nil } } }
        .to raise_error(ArgumentError, /job_options: :retry.*queue, :priority, :wait, :wait_until/)
    end

    it "rejects an unknown step option" do
      expect { workflow_class { step(:a, retries: 3) { nil } } }
        .to raise_error(ArgumentError, /unknown step option :retries/)
    end

    it "accepts a positive timeout: and rejects anything else" do
      expect(workflow_class { step(:a, timeout: 0.5) { nil } }.workflow_steps.fetch(:a).timeout).to eq(0.5)

      [0, -1, "30s", 0.seconds].each do |bad|
        expect { workflow_class { step(:a, timeout: bad) { nil } } }
          .to raise_error(ArgumentError, /timeout: must be a positive number of seconds/), "for #{bad.inspect}"
      end
    end

    it "rejects a negative wait:" do
      expect { workflow_class { step(:a, wait: -1) { nil } } }.to raise_error(ArgumentError, /wait:/)
    end

    it "rejects a skip_if: that is neither callable nor a method name" do
      expect { workflow_class { step(:a, skip_if: true) { nil } } }.to raise_error(ArgumentError, /skip_if:/)
    end

    it "rejects a step with neither a name nor a body" do
      expect { workflow_class { step } }.to raise_error(ArgumentError, /needs a name or a block/)
    end

    it "validates set_step_job_options the same way" do
      expect { workflow_class { set_step_job_options(concurrency: 1) } }
        .to raise_error(ArgumentError, /job_options: :concurrency/)
      expect(workflow_class { set_step_job_options(queue: :slow) }.step_job_options).to eq(queue: "slow")
    end
  end

  describe ".definition_digest" do
    let(:base) do
      workflow_class do
        step(:a) { nil }
        step(:b) { nil }
      end
    end

    it "is stable when a step's options or body change" do
      same = workflow_class do
        step(:a, wait: 5) { :different_body }
        step(:b) { nil }
      end

      expect(same.definition_digest).to eq(base.definition_digest)
      expect(base.definition_digest).to match(/\A\h{64}\z/)
    end

    it "changes when a step is added, removed or reordered" do
      added = workflow_class(parent: base) { step(:c) { nil } }
      removed = workflow_class(parent: base) { removed_step :b }
      reordered = workflow_class do
        step(:b) { nil }
        step(:a) { nil }
      end

      digests = [base, added, removed, reordered].map(&:definition_digest)
      expect(digests.uniq.size).to eq(4)
    end
  end

  describe ".registered_classes" do
    after { Pgbus::Workflow::Registry.reset! }

    it "lists every named subclass once" do
      Pgbus::Workflow::Registry.reset!
      first = workflow_class("FirstWorkflow")
      second = workflow_class("SecondWorkflow")
      Pgbus::Workflow::Registry.register(first)

      expect(Pgbus::Workflow.registered_classes).to contain_exactly(first, second)
    end

    it "keeps the latest class when a name is defined again (code reloading), and drops the old one" do
      Pgbus::Workflow::Registry.reset!
      old = workflow_class("ReloadedWorkflow")
      Pgbus::Workflow::Registry.register(old)
      reloaded = workflow_class("ReloadedWorkflow")
      Pgbus::Workflow::Registry.register(reloaded)

      expect(Pgbus::Workflow.registered_classes).to eq([reloaded])
      expect(Pgbus::Workflow::Registry.instance_variable_get(:@classes).count { |k| k.name == "ReloadedWorkflow" }).to eq(1)
    end

    it "loses no class when two threads define classes at once", :first_call_race do
      Pgbus::Workflow::Registry.reset!
      hold_first_call(Pgbus::Workflow::Registry, :merged)

      classes = race { workflow_class("RacingWorkflow#{Thread.current.object_id}") }

      expect(Pgbus::Workflow.registered_classes).to match_array(classes)
    end
  end
end
