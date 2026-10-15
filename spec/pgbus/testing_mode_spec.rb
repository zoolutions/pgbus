# frozen_string_literal: true

require "spec_helper"
require "async"
require "pgbus/testing"

# The process-wide test mode must be visible from every thread and fiber (the
# streamer's listener thread, an Async task), while a block-form override
# stays scoped to the fiber that set it (issue #527). Thread.main[] only reads
# the main thread's CURRENT fiber, so a mode set inside a fiber used to vanish.
RSpec.describe Pgbus::Testing, ".mode" do
  after do
    described_class.disabled!
    described_class.store.clear!
  end

  def mode_in_new_thread
    Thread.new { described_class.mode }.value
  end

  it "sees a mode set on the main thread from a new thread" do
    described_class.fake!

    expect(mode_in_new_thread).to eq(:fake)
  end

  it "sees a mode set inside an Async task from a new thread once the task is done" do
    Sync { described_class.fake! }

    expect(mode_in_new_thread).to eq(:fake)
  end

  it "sees a mode set inside one Async task from a sibling task" do
    seen = Sync do |task|
      task.async { described_class.inline! }.wait
      task.async { described_class.mode }.wait
    end

    expect(seen).to eq(:inline)
  end

  it "sees a mode set on another thread from the main thread" do
    Thread.new { described_class.fake! }.join

    expect(described_class.mode).to eq(:fake)
  end

  it "keeps a block-form override inside the Async task that set it" do
    # Thread::Queue blocks only the waiting fiber under Async's scheduler.
    entered = Queue.new
    release = Queue.new

    seen = Sync do |task|
      owner = task.async do
        described_class.fake! do
          entered << true
          release.pop
          described_class.mode
        end
      end
      sibling = task.async do
        entered.pop
        mode = described_class.mode
        release << true
        mode
      end
      [owner.wait, sibling.wait]
    end

    expect(seen).to eq(%i[fake disabled])
  end

  it "keeps a block-form override on the thread that set it" do
    entered = Queue.new
    release = Queue.new
    owner = Thread.new do
      described_class.fake! do
        entered << true
        release.pop
        described_class.mode
      end
    end

    entered.pop
    seen_elsewhere = described_class.mode
    release << true

    expect([owner.value, seen_elsewhere]).to eq(%i[fake disabled])
  end
end
