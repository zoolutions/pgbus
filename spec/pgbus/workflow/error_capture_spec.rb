# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Workflow::ErrorCapture do
  def raised(error)
    raise error
  rescue StandardError => e
    e
  end

  it "records the class, the message and the backtrace" do
    captured = described_class.capture(raised(ArgumentError.new("bad input")), backtrace_lines: 50)

    expect(captured).to include(error_class: "ArgumentError", error_message: "bad input")
    expect(captured[:error_backtrace]).to include("error_capture_spec.rb")
  end

  it "caps the message at 10_000 characters" do
    captured = described_class.capture(raised(RuntimeError.new("x" * 20_000)), backtrace_lines: 5)

    expect(captured[:error_message].length).to eq(described_class::MESSAGE_LIMIT)
  end

  it "cuts the backtrace to the configured number of lines" do
    error = RuntimeError.new("deep")
    error.set_backtrace(Array.new(80) { |i| "app/models/thing.rb:#{i}:in 'call'" })

    expect(described_class.capture(error, backtrace_lines: 7)[:error_backtrace].lines.size).to eq(7)
  end

  it "defaults to config.workflow_error_backtrace_lines" do
    error = RuntimeError.new("deep")
    error.set_backtrace(Array.new(80) { |i| "app/models/thing.rb:#{i}" })
    allow(Pgbus.configuration).to receive(:workflow_error_backtrace_lines).and_return(3)

    expect(described_class.capture(error)[:error_backtrace].lines.size).to eq(3)
  end

  it "cleans the backtrace through Rails.backtrace_cleaner, keeping the raw one when cleaning leaves nothing" do
    cleaner = double("BacktraceCleaner")
    allow(cleaner).to receive(:clean) { |lines| lines.reject { |line| line.include?("/gems/") } }
    stub_const("Rails", double("Rails", backtrace_cleaner: cleaner))
    error = RuntimeError.new("mixed")
    error.set_backtrace(["/gems/rack/lib/rack.rb:1", "app/jobs/a.rb:2"])
    gems_only = RuntimeError.new("gems")
    gems_only.set_backtrace(["/gems/rack/lib/rack.rb:1"])

    expect(described_class.capture(error, backtrace_lines: 10)[:error_backtrace]).to eq("app/jobs/a.rb:2")
    expect(described_class.capture(gems_only, backtrace_lines: 10)[:error_backtrace]).to eq("/gems/rack/lib/rack.rb:1")
  end

  # Each level raised while rescuing the one below, so Ruby links #cause.
  def nested(depth, level = 1)
    return raised(RuntimeError.new("level #{level}")) if level == depth

    begin
      raise nested(depth, level + 1)
    rescue StandardError
      raise "level #{level}"
    end
  rescue StandardError => e
    e
  end

  it "records the cause chain, at most five deep" do
    error = nested(7)

    causes = described_class.capture(error, backtrace_lines: 1)[:error_causes]

    expect(causes.size).to eq(5)
    expect(error.message).to eq("level 1")
    expect(causes.first).to eq("class" => "RuntimeError", "message" => "level 2")
  end

  it "caps each cause's message at 1_000 characters" do
    error = begin
      begin
        raise ArgumentError, "y" * 5_000
      rescue ArgumentError
        raise "outer"
      end
    rescue RuntimeError => e
      e
    end

    cause = described_class.capture(error, backtrace_lines: 1)[:error_causes].first
    expect(cause["message"].length).to eq(described_class::CAUSE_MESSAGE_LIMIT)
    expect(cause["message"]).to end_with("…")
  end

  it "stops a cyclic cause chain where it repeats" do
    outer = RuntimeError.new("outer")
    inner = RuntimeError.new("inner")
    outer.define_singleton_method(:cause) { inner }
    inner.define_singleton_method(:cause) { outer }

    expect(described_class.capture(outer, backtrace_lines: 1)[:error_causes]).to eq([{ "class" => "RuntimeError", "message" => "inner" }])
  end

  it "handles an error with no backtrace" do
    expect(described_class.capture(RuntimeError.new("never raised"), backtrace_lines: 5)[:error_backtrace]).to eq("")
  end
end
