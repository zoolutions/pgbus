# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Workflow::FlowControl do
  let(:klass) do
    Class.new(Pgbus::Workflow) do
      def self.name = "FlowSpecWorkflow"
    end
  end
  let(:record) { double("WorkflowEntry", workflow_id: "wf-1", status: "performing") }
  let(:workflow) { klass.new(record) }

  def in_step(&)
    workflow.send(:enter_step, Pgbus::Workflow::StepContext::Attempt.new(step_id: 1, step_name: "charge", attempt: 2))
    catch(described_class::THROW_TAG) do
      workflow.instance_exec(&)
      :fell_through
    end
  end

  describe "inside a step body" do
    {
      pause!: :pause,
      cancel!: :cancel,
      skip!: :skip,
      finished!: :finish,
      reattempt!: :reattempt
    }.each do |verb, action|
      it "#{verb} ends the body with a #{action} outcome" do
        outcome = in_step { public_send(verb) }

        expect(outcome).to be_a(described_class::Outcome)
        expect(outcome.action).to eq(action)
      end
    end

    it "pause! and cancel! carry their actor: and reason: for the audit trail" do
      expect(in_step do
        pause!(actor: "reviewer", reason: "fraud check")
      end).to have_attributes(action: :pause, actor: "reviewer", reason: "fraud check")
      expect(in_step { cancel!(reason: "duplicate order") }).to have_attributes(action: :cancel, actor: nil, reason: "duplicate order")
    end

    it "reattempt! carries its wait: in seconds" do
      expect(in_step { reattempt!(wait: 30) }.wait).to eq(30)
      expect(in_step { reattempt! }.wait).to be_nil
    end

    it "stops the body at the verb" do
      ran = []
      in_step do
        ran << :before
        skip!
        ran << :after
      end

      expect(ran).to eq([:before])
    end

    it "is not caught by a rescue StandardError in the body" do
      outcome = in_step do
        pause!
      rescue StandardError
        :rescued
      end

      expect(outcome.action).to eq(:pause)
    end
  end

  describe "outside a step body" do
    it "raises NotInStep for the step-only verbs" do
      %i[reattempt! skip! finished!].each do |verb|
        expect { workflow.public_send(verb) }.to raise_error(Pgbus::Workflow::NotInStep, /#{Regexp.escape(verb.to_s)}/)
      end
    end

    it "routes pause! and cancel! to the Runner with the actor and reason" do
      updated = double("WorkflowEntry", workflow_id: "wf-1", status: "paused")
      allow(Pgbus::Workflow::Runner).to receive(:apply).and_return(updated)

      workflow.pause!(actor: "alice", reason: "SMTP down")
      workflow.cancel!

      expect(Pgbus::Workflow::Runner).to have_received(:apply).with(workflow, :pause, actor: "alice", reason: "SMTP down")
      expect(Pgbus::Workflow::Runner).to have_received(:apply).with(workflow, :cancel, actor: "app", reason: nil)
      expect(workflow.record).to be(updated)
    end
  end

  describe described_class::Outcome do
    it "maps an exception policy action to its outcome" do
      expect(described_class.for(:pause!).action).to eq(:pause)
      expect(described_class.for(:reattempt!, wait: 5)).to have_attributes(action: :reattempt, wait: 5)
      expect(described_class.for(:cancel).action).to eq(:cancel)
      expect { described_class.for(:explode!) }.to raise_error(ArgumentError, /explode!/)
    end
  end
end
