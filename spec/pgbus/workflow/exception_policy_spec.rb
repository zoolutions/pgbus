# frozen_string_literal: true

require "spec_helper"
require "active_support/core_ext/numeric/time"

RSpec.describe Pgbus::Workflow::ExceptionPolicy do
  let(:timeout_error_class) { Class.new(StandardError) { def self.name = "SpecTimeout" } }
  let(:declined_error_class) { Class.new(StandardError) { def self.name = "SpecDeclined" } }
  let(:workflow) { Object.new.extend(Pgbus::Workflow::FlowControl).tap { |w| def w.step_context = :in_step } }

  def policy(action = nil, **, &) = described_class.new(action, **, &)

  describe ".resolve" do
    let(:error) { timeout_error_class.new("read timeout") }
    let(:step_level) { policy(:skip!, matching: timeout_error_class) }
    let(:class_specific) { policy(:reattempt!, matching: timeout_error_class) }
    let(:class_blanket) { policy(:cancel!) }

    it "prefers the step's own policy" do
      expect(described_class.resolve(error, step_policies: [step_level], class_policies: [class_blanket, class_specific]))
        .to be(step_level)
    end

    it "then a class-level policy that names the error, even when a blanket one was declared first" do
      expect(described_class.resolve(error, step_policies: [], class_policies: [class_blanket, class_specific]))
        .to be(class_specific)
    end

    it "then a class-level blanket policy" do
      expect(described_class.resolve(error, class_policies: [class_blanket])).to be(class_blanket)
    end

    it "falls back to pause!" do
      resolved = described_class.resolve(error)

      expect(resolved).to be(described_class::DEFAULT)
      expect(resolved.action).to eq(:pause!)
    end

    it "skips policies that do not match" do
      declined_only = policy(:cancel!, matching: declined_error_class)

      expect(described_class.resolve(error, class_policies: [declined_only])).to be(described_class::DEFAULT)
    end
  end

  describe "#matches?" do
    it "takes a class, a class name, or a list of either, matching subclasses" do
      error = Class.new(timeout_error_class).new

      expect(policy(:pause!, matching: timeout_error_class)).to be_matches(error)
      expect(policy(:pause!, matching: "SpecTimeout")).to be_matches(error)
      expect(policy(:pause!, matching: [declined_error_class, "SpecTimeout"])).to be_matches(error)
      expect(policy(:pause!, matching: declined_error_class)).not_to be_matches(error)
    end

    it "matches every StandardError when no matching: is given" do
      expect(policy(:pause!)).to be_matches(RuntimeError.new)
      expect(policy(:pause!)).not_to be_specific
    end
  end

  describe "#decide" do
    let(:error) { timeout_error_class.new }

    it "reattempts with the policy's wait until max_reattempts consecutive reattempts are spent" do
      reattempting = policy(:reattempt!, wait: 15.seconds, max_reattempts: 2)

      fresh = reattempting.decide(error, workflow: workflow, consecutive_reattempts: -> { 1 })
      expect(fresh.outcome).to have_attributes(action: :reattempt, wait: 15)
      expect(fresh.terminal).to be(false)

      spent = reattempting.decide(error, workflow: workflow, consecutive_reattempts: -> { 2 })
      expect(spent.outcome.action).to eq(:pause)
      expect(spent.terminal).to be(true)
    end

    it "applies terminal_action once the reattempts are spent" do
      cancelling = policy(:reattempt!, max_reattempts: 1, terminal_action: :cancel!)

      expect(cancelling.decide(error, workflow: workflow, consecutive_reattempts: -> { 1 }).outcome.action).to eq(:cancel)
    end

    it "does not count reattempts without a max_reattempts" do
      counter = -> { raise "must not be called" }

      expect(policy(:reattempt!).decide(error, workflow: workflow, consecutive_reattempts: counter).outcome.action)
        .to eq(:reattempt)
    end

    it "reports by default, never with report: :never, and only on the terminal action with :terminal_only" do
      count = ->(n) { -> { n } }
      terminal_only = policy(:reattempt!, max_reattempts: 3, report: :terminal_only)

      expect(policy(:pause!).decide(error, workflow: workflow).report).to be(true)
      expect(policy(:pause!, report: :never).decide(error, workflow: workflow).report).to be(false)
      expect(terminal_only.decide(error, workflow: workflow, consecutive_reattempts: count[0]).report).to be(false)
      expect(terminal_only.decide(error, workflow: workflow, consecutive_reattempts: count[3]).report).to be(true)
      expect(policy(:skip!, report: :terminal_only).decide(error, workflow: workflow).report).to be(true)
    end

    describe "an imperative policy" do
      it "runs the block on the workflow and takes the verb it calls" do
        seen = nil
        imperative = policy { |e| (seen = e) && reattempt!(wait: 5) }

        decision = imperative.decide(error, workflow: workflow)

        expect(seen).to be(error)
        expect(decision.outcome).to have_attributes(action: :reattempt, wait: 5)
      end

      it "pauses when the block calls no verb" do
        expect(policy { |_e| :nothing }.decide(error, workflow: workflow).outcome.action).to eq(:pause)
      end

      it "pauses and hands back the block's own error when it raises" do
        decision = policy { |_e| raise ArgumentError, "handler bug" }.decide(error, workflow: workflow)

        expect(decision.outcome.action).to eq(:pause)
        expect(decision.handler_error).to be_a(ArgumentError)
        expect(decision.report).to be(true)
      end
    end
  end

  describe "validation" do
    it "needs an action or a block, not both" do
      expect { policy }.to raise_error(ArgumentError, /needs action:/)
      expect { policy(:pause!) { nil } }.to raise_error(ArgumentError, /not both/)
    end

    it "accepts the verbs with or without the bang" do
      expect(policy(:reattempt).action).to eq(:reattempt!)
    end

    it "rejects an unknown action, terminal_action or report" do
      expect { policy(:retry!) }.to raise_error(ArgumentError, /action: must be one of/)
      expect { policy(:reattempt!, max_reattempts: 1, terminal_action: :reattempt!) }.to raise_error(ArgumentError, /terminal_action/)
      expect { policy(:pause!, report: :sometimes) }.to raise_error(ArgumentError, /report:/)
    end

    it "rejects wait: and terminal_action: on anything but reattempt!, the block form included" do
      expect { policy(:pause!, wait: 5) }.to raise_error(ArgumentError, /wait: only applies to action: :reattempt!/)
      expect { policy(:cancel!, terminal_action: :skip!) }.to raise_error(ArgumentError, /terminal_action: only applies/)
      expect { policy(wait: 5) { |_e| reattempt! } }.to raise_error(ArgumentError, /wait: only applies/)
      expect(policy(:reattempt!, wait: 5, terminal_action: :cancel!).terminal_action).to eq(:cancel!)
      expect { policy(:reattempt!, terminal_action: false) }.to raise_error(ArgumentError, /terminal_action: must be one of/)
    end

    it "rejects max_reattempts on anything but reattempt!, and a non-positive count" do
      expect { policy(:pause!, max_reattempts: 3) }.to raise_error(ArgumentError, /only applies to action: :reattempt!/)
      expect { policy(:reattempt!, max_reattempts: 0) }.to raise_error(ArgumentError, /positive Integer/)
    end
  end

  describe ".coerce_all (the step-level on_exception: option)" do
    it "takes a policy, an action, a Hash, or an Array of those" do
      ready = policy(:skip!)
      coerced = described_class.coerce_all([ready, :cancel!, { matching: "SpecTimeout", action: :reattempt!, wait: 1 }])

      expect(coerced.first).to be(ready)
      expect(coerced.map(&:action)).to eq(%i[skip! cancel! reattempt!])
      expect(coerced.last.matching).to eq(["SpecTimeout"])
      expect(described_class.coerce_all(nil)).to eq([])
      expect(described_class.coerce_all({ action: :pause! }).size).to eq(1)
    end

    it "rejects anything else" do
      expect { described_class.coerce_all(42) }.to raise_error(ArgumentError, /on_exception:/)
    end
  end

  describe "the class-level DSL" do
    it "declares specific, blanket and imperative policies in order" do
      error_class = timeout_error_class
      klass = Class.new(Pgbus::Workflow) do
        on_exception error_class, action: :reattempt!, wait: 15, max_reattempts: 5
        on_exception action: :cancel!
        on_exception(ArgumentError) { |_e| skip! }
      end

      expect(klass.exception_policies.map(&:action)).to eq([:reattempt!, :cancel!, nil])
      expect(klass.exception_policies.first.matching).to eq([error_class])
      expect(klass.exception_policies.last.handler).to be_a(Proc)
    end
  end
end
