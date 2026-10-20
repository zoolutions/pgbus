# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Process::ReadinessSnapshot do
  describe "#ready?" do
    it "is not ready before boot completes" do
      snapshot = described_class.new(booted: false, shutting_down: false, expected: 0, live: 0)

      expect(snapshot.ready?).to be false
    end

    it "is ready once booted with all expected children live" do
      snapshot = described_class.new(booted: true, shutting_down: false, expected: 3, live: 3)

      expect(snapshot.ready?).to be true
    end

    it "is not ready when a child is missing" do
      snapshot = described_class.new(booted: true, shutting_down: false, expected: 3, live: 2)

      expect(snapshot.ready?).to be false
    end

    it "is not ready while shutting down, even with all children live" do
      snapshot = described_class.new(booted: true, shutting_down: true, expected: 3, live: 3)

      expect(snapshot.ready?).to be false
    end

    it "is ready for a zero-child deployment once booted" do
      snapshot = described_class.new(booted: true, shutting_down: false, expected: 0, live: 0)

      expect(snapshot.ready?).to be true
    end
  end

  # Issue #587: forked counts the fork table, live only the children that
  # checked in. A wedged boot keeps live < expected.
  describe "#forked" do
    it "defaults to live when not given" do
      snapshot = described_class.new(booted: true, shutting_down: false, expected: 3, live: 2)

      expect(snapshot.forked).to eq(2)
    end

    it "is not ready while a forked child has not checked in" do
      snapshot = described_class.new(booted: true, shutting_down: false, expected: 6, live: 5, forked: 6)

      expect(snapshot.ready?).to be false
      expect(snapshot.status).to eq("DEGRADED")
    end

    it "is ready once every forked child has checked in" do
      snapshot = described_class.new(booted: true, shutting_down: false, expected: 6, live: 6, forked: 6)

      expect(snapshot.ready?).to be true
    end
  end

  describe "#status" do
    it "reports BOOTING before boot completes" do
      snapshot = described_class.new(booted: false, shutting_down: false, expected: 0, live: 0)

      expect(snapshot.status).to eq("BOOTING")
    end

    it "reports OK when ready" do
      snapshot = described_class.new(booted: true, shutting_down: false, expected: 2, live: 2)

      expect(snapshot.status).to eq("OK")
    end

    it "reports DEGRADED when a child is missing" do
      snapshot = described_class.new(booted: true, shutting_down: false, expected: 2, live: 1)

      expect(snapshot.status).to eq("DEGRADED")
    end

    it "reports DRAINING while shutting down, taking precedence over BOOTING" do
      snapshot = described_class.new(booted: false, shutting_down: true, expected: 0, live: 0)

      expect(snapshot.status).to eq("DRAINING")
    end
  end
end
