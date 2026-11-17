# frozen_string_literal: true

require "rails_helper"
require "pgbus/testing"

# Inline signal delivery runs on the caller's thread: outside an active
# executor, Rails.application.executor.wrap would reset every
# CurrentAttributes on completion (issue #674 review).
RSpec.describe "Pgbus::Testing inline signals and CurrentAttributes" do # rubocop:disable RSpec/DescribeClass
  let(:current) do
    stub_const("SignalProbeCurrent", Class.new(ActiveSupport::CurrentAttributes) { attribute :user })
  end
  let(:seen) { [] }

  before do
    Pgbus::Signals.reset!
    allow(Pgbus::Web::Streamer).to receive(:listen_signal)
    Pgbus.on_signal(:probe) { |payload| seen << [payload, current.user] }
    Pgbus::Testing.inline!
  end

  after do
    Pgbus::Testing.disabled!
    Pgbus::Testing.clear_signals!
    Pgbus::Signals.reset!
    current.reset
  end

  it "keeps the caller's Current attributes and shows them to the subscriber" do
    current.user = "alice"

    Pgbus.signal(:probe, "x")

    expect(seen).to eq([%w[x alice]])
    expect(current.user).to eq("alice")
  end
end
