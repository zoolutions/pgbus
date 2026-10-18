# frozen_string_literal: true

require "spec_helper"
require "active_support/core_ext/numeric/time"
# String#to_time, as in a Rails app: a String must still not pass as a deadline.
require "active_support/core_ext/string/conversions"

# Issue #574: the parked step's timeout_at, from a static wait_timeout: or a
# per-run one (a lambda or a method name evaluated on the workflow instance).
# spec/integration/workflow/wait_timeout_spec.rb parks real runs with it.
RSpec.describe Pgbus::Workflow::StepDefinition, "#timeout_at_for" do
  let(:now) { Time.utc(2026, 10, 18, 12, 0, 0) }
  let(:returned) { { value: nil } }
  let(:seen) { [] }
  let(:workflow) do
    holder = returned
    calls = seen
    Class.new(Pgbus::Workflow) do
      define_singleton_method(:name) { "SpecDeadlineWorkflow" }
      define_method(:reply_deadline) do
        calls << :reply_deadline
        holder[:value]
      end
      define_method(:expires_at) { holder[:value] }
    end.new(nil)
  end

  def definition(wait_timeout)
    described_class.new(name: :await_reply, body: :await_reply, wait_for: :replied, wait_timeout: wait_timeout)
  end

  def timeout_at(value, wait_timeout: -> { expires_at })
    returned[:value] = value
    definition(wait_timeout).timeout_at_for(workflow, now)
  end

  it "counts a static wait_timeout: from park, and has no deadline without one" do
    expect(definition(3600).timeout_at_for(workflow, now)).to eq(now + 3600)
    expect(definition(nil).timeout_at_for(workflow, now)).to be_nil
    expect(seen).to be_empty
  end

  it "takes a Time a lambda returns as the deadline, however far from park" do
    expect(timeout_at(now + 90)).to eq(now + 90)
    expect(timeout_at(now + 5.days)).to eq(now + 5.days)
  end

  it "takes anything with #to_time that is not a String" do
    expect(timeout_at((now + 600).to_datetime)).to eq(now + 600)
    expect(timeout_at(Time.use_zone("Europe/Stockholm") { Time.zone.at(now + 60) })).to eq(now + 60)
  end

  it "turns a deadline already past into now, so the next sweep applies on_timeout" do
    expect(timeout_at(now - 3600)).to eq(now)
    expect(timeout_at(now)).to eq(now)
  end

  it "counts a Duration or a number of seconds from park" do
    expect(timeout_at(2.hours)).to eq(now + 7200)
    expect(timeout_at(90)).to eq(now + 90)
    expect(timeout_at(1.5)).to eq(now + 1.5)
  end

  it "has no deadline for this attempt when the lambda returns nil" do
    expect(timeout_at(nil)).to be_nil
  end

  it "calls the instance method a Symbol names, once" do
    expect(timeout_at(now + 60, wait_timeout: :reply_deadline)).to eq(now + 60)
    expect(seen).to eq([:reply_deadline])
  end

  it "raises ArgumentError, naming the step, for a value that is neither a deadline nor a duration" do
    ["tomorrow", Object.new, :soon, true].each do |bad|
      expect { timeout_at(bad) }
        .to raise_error(ArgumentError, /wait_timeout: of step await_reply must return a Time, a Duration, a number of seconds or nil, got/),
            "for #{bad.inspect}"
    end
  end

  it "raises ArgumentError for a duration that is not positive" do
    [0, -5, -1.hour, Float::INFINITY].each do |bad|
      expect { timeout_at(bad) }
        .to raise_error(ArgumentError, /wait_timeout \(step await_reply\): must be a positive number of seconds/), "for #{bad.inspect}"
    end
  end

  it "lets an error the lambda raises through, for the step's exception policies" do
    failing = definition(-> { raise KeyError, "no deadline on the offer" })

    expect { failing.timeout_at_for(workflow, now) }.to raise_error(KeyError, "no deadline on the offer")
  end

  it "evaluates the lambda as a guard: a flow-control verb inside it raises NotInStep" do
    workflow.send(:enter_step, Pgbus::Workflow::StepContext::Attempt.new(step_id: 1, step_name: "await_reply", attempt: 1))
    skipping = definition(-> { skip! })

    expect { skipping.timeout_at_for(workflow, now) }.to raise_error(Pgbus::Workflow::NotInStep)
  end

  describe "#resolve_wait_timeout" do
    it "returns the lambda's deadline as a Time, its duration as seconds, or nil" do
      returned[:value] = now + 60
      expect(definition(-> { expires_at }).resolve_wait_timeout(workflow)).to eq(now + 60)
      returned[:value] = 2.minutes
      expect(definition(-> { expires_at }).resolve_wait_timeout(workflow)).to eq(120)
      returned[:value] = nil
      expect(definition(-> { expires_at }).resolve_wait_timeout(workflow)).to be_nil
    end
  end
end
