# frozen_string_literal: true

require "spec_helper"
require "active_job"
require "active_support/core_ext/numeric/time"

# Issue #540, step 5, with the models mocked: start!, the outside verbs and
# their guards, the audit trail. spec/integration/workflow/verbs_spec.rb runs
# the same against a real database.
RSpec.describe Pgbus::Workflow do
  after { Pgbus::Workflow::Registry.reset! }

  let(:klass) do
    Class.new(described_class) do
      def self.name = "SpecOrderWorkflow"

      step(:charge) { nil }
      step(:receipt, wait: 60) { nil }
    end
  end
  let(:events) { [] }
  let(:record) do
    double("WorkflowEntry", workflow_id: "wf-1", workflow_class: "SpecOrderWorkflow", status: "ready", current_step: "charge",
                            subject_gid: nil, arguments: {}, assign_attributes: nil, workflow_klass: klass)
  end

  before do
    allow(Pgbus::WorkflowEntry).to receive(:transaction).and_yield
    allow(Pgbus::WorkflowEntry).to receive(:connection).and_return(double("connection", transaction_open?: false))
    allow(Pgbus::WorkflowEvent).to receive(:record!) { |**attrs| events << attrs }
    allow(Pgbus::WorkflowStep).to receive(:insert_next!) { |**attrs| double("WorkflowStep", id: 5, **attrs, attempt: 1) }
  end

  describe ".start!" do
    before do
      allow(Pgbus::WorkflowEntry).to receive_messages(insert_run!: true, find_by!: record)
    end

    it "inserts the run and its first step row, and returns an instance of the class" do
      wf = klass.start!(nil, plan: "pro")

      expect(wf).to be_a(klass)
      expect(wf).not_to be_duplicate_start
      expect(Pgbus::WorkflowEntry).to have_received(:insert_run!).with(
        hash_including(workflow_class: "SpecOrderWorkflow", status: "ready", current_step: "charge", subject_type: nil,
                       idempotency_key: nil, definition_digest: klass.definition_digest)
      )
      expect(Pgbus::WorkflowStep).to have_received(:insert_next!).with(hash_including(step_name: "charge"))
      expect(events.map { |e| [e[:event], e[:actor]] }).to eq([%w[started app], %w[step_scheduled app]])
    end

    it "serializes the arguments through ActiveJob::Arguments" do
      klass.start!(nil, plan: "pro", count: 2)

      expect(Pgbus::WorkflowEntry).to have_received(:insert_run!)
        .with(hash_including(arguments: hash_including("plan" => "pro", "count" => 2)))
    end

    it "raises PayloadTooLarge over workflow_max_payload_size" do
      allow(Pgbus.configuration).to receive(:workflow_max_payload_size).and_return(32)

      expect { klass.start!(nil, blob: "x" * 64) }.to raise_error(described_class::PayloadTooLarge, /arguments of SpecOrderWorkflow/)
      expect(Pgbus::WorkflowEntry).not_to have_received(:insert_run!)
    end

    it "raises AlreadyRunning, naming the run that holds the slot, on the one-ongoing index" do
      user = Struct.new(:id) do
        def self.name = "SpecUser"
        def to_global_id = "gid://pgbus-test/SpecUser/#{id}"
      end.new(7)
      allow(Pgbus::WorkflowEntry).to receive(:insert_run!).and_raise(ActiveRecord::RecordNotUnique, "idx_pgbus_workflows_one_ongoing")
      ongoing = double("ongoing scope")
      allow(ongoing).to receive(:find_by).with(workflow_class: "SpecOrderWorkflow", subject_type: "SpecUser", subject_id: "7")
                                         .and_return(record)
      allow(Pgbus::WorkflowEntry).to receive(:ongoing).and_return(ongoing)

      expect { klass.start!(user) }.to raise_error(described_class::AlreadyRunning) { |error|
        expect(error.workflow.workflow_id).to eq("wf-1")
      }
    end

    it "returns the existing run with duplicate_start? when the idempotency key is taken" do
      allow(Pgbus::WorkflowEntry).to receive(:insert_run!).and_return(false)
      allow(Pgbus::WorkflowEntry).to receive(:where).with(workflow_class: "SpecOrderWorkflow", idempotency_key: "k")
                                                    .and_return(double("scope", take!: record))

      wf = klass.start!(nil, idempotency_key: "k")

      expect(wf).to be_duplicate_start
      expect(Pgbus::WorkflowStep).not_to have_received(:insert_next!)
    end

    it "rejects a class with no steps or no name" do
      expect { Class.new(described_class) { def self.name = "EmptyWorkflow" }.start! }.to raise_error(ArgumentError, /declares no steps/)
      expect { Class.new(described_class) { step(:a) { nil } }.start! }.to raise_error(ArgumentError, /anonymous/)
    end

    it "folds a class or step job_options wait into the first step's scheduled_at" do
      klass.set_step_job_options(wait: 600)

      klass.start!

      expect(Pgbus::WorkflowStep).to have_received(:insert_next!)
        .with(hash_including(step_name: "charge", scheduled_at: be_within(5).of(600.seconds.from_now)))
    end

    it "rejects a subject that has no GlobalID" do
      expect { klass.start!(Object.new) }.to raise_error(ArgumentError, /GlobalID record/)
    end
  end

  describe "outside verbs" do
    let(:workflow) { klass.new(record) }

    before do
      allow(Pgbus::WorkflowEntry).to receive(:lock_for).with("wf-1").and_return(record)
      allow(Pgbus::WorkflowEntry).to receive(:transition!).and_return(1)
      allow(Pgbus::WorkflowStep).to receive(:cancel_open!)
    end

    it "derives each guarded UPDATE's from-states from the States table" do
      workflow.pause!(actor: "alice", reason: "hold")
      allow(record).to receive(:status).and_return("paused")
      allow(Pgbus::WorkflowStep).to receive(:latest_for).and_return(double("WorkflowStep", status: "failed", step_name: "charge"))
      workflow.resume!

      expect(Pgbus::WorkflowEntry).to have_received(:transition!)
        .with("wf-1", hash_including(from: described_class::States.from(:pause), to: "paused"))
      expect(Pgbus::WorkflowEntry).to have_received(:transition!)
        .with("wf-1", hash_including(from: described_class::States.from(:resume), to: "ready"))
    end

    it "records each verb's event with the actor and reason given, \"app\" by default" do
      workflow.pause!(actor: "dashboard:alice", reason: "SMTP down")
      allow(record).to receive(:status).and_return("paused")
      workflow.cancel!

      expect(events.map { |e| e.values_at(:event, :actor, :reason) })
        .to eq([["paused", "dashboard:alice", "SMTP down"], ["cancelled", "app", nil]])
      expect(Pgbus::WorkflowStep).to have_received(:cancel_open!).with("wf-1", now: a_kind_of(Time))
    end

    it "raises the States table's error without changing anything" do
      allow(record).to receive(:status).and_return("finished")

      expect { workflow.resume! }.to raise_error(described_class::NotPaused)
      expect { workflow.cancel! }.to raise_error(described_class::InvalidTransition, /cannot cancel a finished workflow/)
      expect(Pgbus::WorkflowEntry).not_to have_received(:transition!)
      expect(events).to be_empty
    end

    it "raises RecordNotFound when the run is gone" do
      allow(Pgbus::WorkflowEntry).to receive(:lock_for).and_return(nil)

      expect { workflow.pause! }.to raise_error(ActiveRecord::RecordNotFound)
    end

    describe "resume!" do
      before { allow(record).to receive(:status).and_return("paused") }

      it "schedules a new attempt of a failed step" do
        allow(Pgbus::WorkflowStep).to receive(:latest_for).and_return(double("WorkflowStep", status: "failed", step_name: "charge"))

        workflow.resume!

        expect(Pgbus::WorkflowStep).to have_received(:insert_next!).with(hash_including(step_name: "charge"))
      end

      it "schedules the next step, with its wait, after a completed one" do
        allow(Pgbus::WorkflowStep).to receive(:latest_for).and_return(double("WorkflowStep", status: "completed", step_name: "charge"))

        workflow.resume!

        expect(Pgbus::WorkflowStep).to have_received(:insert_next!)
          .with(hash_including(step_name: "receipt", scheduled_at: be_within(5).of(60.seconds.from_now)))
      end

      it "finishes the run when the completed step was the last" do
        allow(Pgbus::WorkflowStep).to receive(:latest_for).and_return(double("WorkflowStep", status: "completed", step_name: "receipt"))

        workflow.resume!

        expect(Pgbus::WorkflowEntry).to have_received(:transition!).with("wf-1", hash_including(to: "finished"))
        expect(events.map { |e| e[:event] }).to eq(%w[resumed finished])
      end

      it "sends a scheduled step again by bumping enqueued_at" do
        scheduled = double("WorkflowStep", status: "scheduled", step_name: "charge")
        allow(scheduled).to receive(:update!)
        allow(Pgbus::WorkflowStep).to receive(:latest_for).and_return(scheduled)

        workflow.resume!

        expect(scheduled).to have_received(:update!).with(enqueued_at: a_kind_of(Time))
      end

      it "goes back to performing when the paused step is still running" do
        allow(Pgbus::WorkflowStep).to receive(:latest_for).and_return(double("WorkflowStep", status: "in_progress", step_name: "charge"))

        workflow.resume!

        expect(Pgbus::WorkflowEntry).to have_received(:transition!).with("wf-1", hash_including(to: "performing"))
      end
    end

    it "resume_at! cancels what was open and schedules the named step" do
      allow(record).to receive(:status).and_return("paused")

      workflow.resume_at!(:receipt, actor: "ops")

      expect(Pgbus::WorkflowStep).to have_received(:cancel_open!)
      expect(Pgbus::WorkflowStep).to have_received(:insert_next!).with(hash_including(step_name: "receipt"))
      expect(events.first).to include(event: "resumed", actor: "ops", details: { resume_at: "receipt" })
    end

    it "run_now! and reschedule! move the scheduled step and record from/to" do
      scheduled = double("WorkflowStep", id: 3, step_name: "charge", attempt: 1, scheduled_at: 1.hour.from_now)
      allow(scheduled).to receive(:update!) { |attrs| allow(scheduled).to receive(:scheduled_at).and_return(attrs[:scheduled_at]) }
      allow(Pgbus::WorkflowStep).to receive(:where).with(workflow_id: "wf-1", status: "scheduled")
                                                   .and_return(double("scope", order: double("ordered", first: scheduled)))

      workflow.run_now!
      later = 2.hours.from_now
      workflow.reschedule!(at: later, reason: "after the deploy")

      expect(events.map { |e| e[:event] }).to eq(%w[run_now rescheduled])
      expect { workflow.reschedule!(at: "tomorrow-ish") }.to raise_error(ArgumentError, /takes a Time/)
      expect(scheduled).to have_received(:update!).with(scheduled_at: later, enqueued_at: a_kind_of(Time))
    end
  end

  describe "idempotency keys" do
    let(:workflow) { klass.new(record) }

    it "keeps step_idempotency_key per step and varies attempt_idempotency_key per attempt" do
      keys = [1, 2].map do |attempt|
        workflow.send(:enter_step, described_class::StepContext::Attempt.new(step_id: attempt, step_name: "charge", attempt: attempt))
        [workflow.step_idempotency_key, workflow.attempt_idempotency_key]
      end

      expect(keys.map(&:first).uniq).to eq(["pgbus-wf:wf-1:charge"])
      expect(keys.map(&:last)).to eq(["pgbus-wf:wf-1:charge:1", "pgbus-wf:wf-1:charge:2"])
    end

    it "raises NotInStep outside a step" do
      expect { workflow.step_idempotency_key }.to raise_error(described_class::NotInStep)
    end
  end

  describe "readers" do
    it "loads arguments with indifferent access and a nil subject for a subject-less run" do
      serialized = ActiveJob::Arguments.serialize([{ plan: "pro" }]).first
      allow(record).to receive(:arguments).and_return(serialized)
      workflow = klass.new(record)

      expect(workflow.arguments[:plan]).to eq("pro")
      expect(workflow.arguments["plan"]).to eq("pro")
      expect(workflow.subject).to be_nil
      expect(workflow.available_actions).to include(:pause, :cancel)
    end

    it "returns nil for a subject whose record is gone" do
      allow(record).to receive(:subject_gid).and_return("gid://pgbus-test/SpecUser/1")
      allow(Pgbus::Serializer).to receive(:locate_global_id).and_raise(ActiveRecord::RecordNotFound)

      expect(klass.new(record).subject).to be_nil
    end

    it ".find returns an instance of the class that started the run" do
      allow(Pgbus::WorkflowEntry).to receive(:find_by).with(workflow_id: "wf-1").and_return(record)
      allow(Pgbus::WorkflowEntry).to receive(:find_by).with(workflow_id: "missing").and_return(nil)

      expect(described_class.find("wf-1")).to be_a(klass)
      expect(klass.find("wf-1")).to be_a(klass)
      expect(described_class.find("missing")).to be_nil
    end

    it ".find on another workflow class does not return the run" do
      other = Class.new(described_class) { def self.name = "SpecOtherWorkflow" }
      allow(Pgbus::WorkflowEntry).to receive(:find_by).with(workflow_id: "wf-1").and_return(record)

      expect(other.find("wf-1")).to be_nil
    end
  end
end
