# frozen_string_literal: true

require "spec_helper"
require "active_support/core_ext/numeric/time"

# Issue #541, step 1: fan_out and wait_for:/wait_timeout:/on_timeout:
# declarations, all checked when the class body loads.
RSpec.describe "Pgbus::Workflow waiting step declarations" do # rubocop:disable RSpec/DescribeClass
  def workflow_class(name = "SpecWaitingWorkflow", &body)
    Class.new(Pgbus::Workflow) do
      define_singleton_method(:name) { name }
      class_eval(&body) if body
    end
  end

  let(:matcher_object) do
    Class.new do
      def self.name = "ShippedToEurope"
      def matches?(signal) = signal.payload["region"] == "eu"
    end.new
  end

  describe "fan_out" do
    it "registers a fan_out definition that defaults on_batch_failure to pause!" do
      klass = workflow_class do
        fan_out(:notify) { |b| b }
        fan_out :ship, on_batch_failure: :cancel!, wait: 5, timeout: 60 do |b|
          b
        end
      end

      notify = klass.workflow_steps.fetch(:notify)
      expect(notify).to have_attributes(kind: "fan_out", on_batch_failure: :pause!, wait_for: nil)
      expect(notify).to be_frozen
      expect(klass.workflow_steps.fetch(:ship)).to have_attributes(on_batch_failure: :cancel!, wait: 5, timeout: 60)
      expect(klass.workflow_steps.digest_pairs).to eq([%w[notify fan_out], %w[ship fan_out]])
    end

    it "takes on_batch_failure: :pause!, :cancel! or :skip!, with or without the bang" do
      klass = workflow_class { fan_out(:a, on_batch_failure: :skip) { |b| b } }

      expect(klass.workflow_steps.fetch(:a).on_batch_failure).to eq(:skip!)
      [:reattempt!, :finished!, "explode", nil].each do |bad|
        expect { workflow_class { fan_out(:a, on_batch_failure: bad) { |b| b } } }
          .to raise_error(ArgumentError, /on_batch_failure: must be one of :pause!, :cancel!, :skip!/), "for #{bad.inspect}"
      end
    end

    it "runs a method named like the step when there is no block, and anchors like step" do
      klass = workflow_class do
        step(:first) { nil }
        fan_out :notify
        fan_out(:before_notify, before_step: :notify) { |b| b }
      end

      expect(klass.workflow_steps.names).to eq(%w[first before_notify notify])
      expect(klass.workflow_steps.fetch(:notify).body).to eq(:notify)
    end

    it "cannot wait for a signal, and on_batch_failure: belongs to fan_out only" do
      expect { workflow_class { fan_out(:a, wait_for: :shipped) { |b| b } } }
        .to raise_error(ArgumentError, /fan_out :a cannot take wait_for:/)
      expect { workflow_class { fan_out(:a, wait_timeout: 60) { |b| b } } }
        .to raise_error(ArgumentError, /fan_out :a cannot take wait_timeout:/)
      expect { workflow_class { step(:a, on_batch_failure: :pause!) { nil } } }
        .to raise_error(ArgumentError, /unknown step option :on_batch_failure/)
      expect { workflow_class { fan_out(:a, retries: 2) { |b| b } } }
        .to raise_error(ArgumentError, /unknown fan_out option :retries \(accepted: wait, skip_if, job_options, timeout, on_exception\)/)
    end
  end

  describe "wait_for:" do
    it "takes a signal name, a SignalMatcher or any object answering matches?" do
      matcher = Pgbus::Workflow::SignalMatcher.new(:shipped) { |payload| payload[:carrier] == "dhl" }
      custom = matcher_object
      klass = workflow_class do
        step(:by_name, wait_for: :payment_confirmed) { nil }
        step(:by_string, wait_for: "refunded") { nil }
        step(:by_matcher, wait_for: matcher) { nil }
        step(:by_object, wait_for: custom) { nil }
      end

      steps = klass.workflow_steps
      expect(steps.fetch(:by_name).wait_for).to be_a(Pgbus::Workflow::SignalMatcher)
      expect(steps.fetch(:by_name)).to have_attributes(wait_for_name: "payment_confirmed", waits_for_signal?: true)
      expect(steps.fetch(:by_string).wait_for_name).to eq("refunded")
      expect(steps.fetch(:by_matcher).wait_for).to be(matcher)
      expect(steps.fetch(:by_matcher).wait_for_name).to eq("shipped")
      expect(steps.fetch(:by_object).wait_for).to be(custom)
      expect(steps.fetch(:by_object).wait_for_name).to eq("ShippedToEurope")
    end

    it "rejects anything that is not a name or a matcher" do
      [42, Object.new, ""].each do |bad|
        expect { workflow_class { step(:a, wait_for: bad) { nil } } }
          .to raise_error(ArgumentError, /wait_for: takes a signal name/), "for #{bad.inspect}"
      end
    end

    it "leaves a plain step without a wait" do
      definition = workflow_class { step(:a) { nil } }.workflow_steps.fetch(:a)

      expect(definition).to have_attributes(wait_for: nil, wait_for_name: nil, wait_timeout: nil, on_timeout: nil,
                                            waits_for_signal?: false, kind: "step")
    end
  end

  describe "wait_timeout: and on_timeout:" do
    it "defaults on_timeout to pause! and takes each of pause!, cancel!, skip! and run" do
      klass = workflow_class do
        step(:a, wait_for: :x, wait_timeout: 2.days) { nil }
        step(:b, wait_for: :x, wait_timeout: 60, on_timeout: :cancel) { nil }
        step(:c, wait_for: :x, wait_timeout: 60, on_timeout: :skip!) { nil }
        step(:d, wait_for: :x, wait_timeout: 60, on_timeout: :run) { nil }
      end

      steps = klass.workflow_steps
      expect(steps.fetch(:a)).to have_attributes(wait_timeout: 2.days.to_i, on_timeout: :pause!)
      expect(%i[b c d].map { |name| steps.fetch(name).on_timeout }).to eq(%i[cancel! skip! run])
    end

    it "rejects an unknown on_timeout: and a non-positive wait_timeout:" do
      expect { workflow_class { step(:a, wait_for: :x, wait_timeout: 60, on_timeout: :explode) { nil } } }
        .to raise_error(ArgumentError, /on_timeout: must be one of :pause!, :cancel!, :skip!, :run/)
      [0, -5, "1d"].each do |bad|
        expect { workflow_class { step(:a, wait_for: :x, wait_timeout: bad) { nil } } }
          .to raise_error(ArgumentError, /wait_timeout: must be a positive number of seconds/), "for #{bad.inspect}"
      end
    end

    it "requires wait_for:, and on_timeout: requires wait_timeout:" do
      expect { workflow_class { step(:a, wait_timeout: 60) { nil } } }
        .to raise_error(ArgumentError, /wait_timeout: needs wait_for:/)
      expect { workflow_class { step(:a, on_timeout: :cancel!) { nil } } }
        .to raise_error(ArgumentError, /on_timeout: needs wait_for: and wait_timeout:/)
      expect { workflow_class { step(:a, wait_for: :x, on_timeout: :cancel!) { nil } } }
        .to raise_error(ArgumentError, /on_timeout: needs wait_for: and wait_timeout:/)
      expect { workflow_class { step(:a, wait_for: :x, on_timeout: false) { nil } } }
        .to raise_error(ArgumentError, /on_timeout: needs wait_for: and wait_timeout:/)
    end

    it "keeps timeout: (the run) apart from wait_timeout: (the wait) on one step" do
      definition = workflow_class { step(:a, wait_for: :x, wait_timeout: 3600, timeout: 30) { nil } }.workflow_steps.fetch(:a)

      expect(definition).to have_attributes(wait_timeout: 3600, timeout: 30)
    end
  end
end
