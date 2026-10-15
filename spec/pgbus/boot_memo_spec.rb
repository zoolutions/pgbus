# frozen_string_literal: true

require "spec_helper"
require "concurrent"

# The process-wide memos on Pgbus must hand every thread the same object, also
# when the first callers arrive at once (issue #527): an unlocked `@x ||= build`
# lets two threads each build one, and a second Client orphans its pool.
RSpec.describe Pgbus do
  let(:threads) { 20 }

  after { described_class.reset! }

  # Holds the first racer inside `klass.new` until a second racer has asked
  # for one too, so an unlocked `||=` builds two every run. Only the racing
  # threads are held; anything built before the race is left alone.
  def hold_first_build(klass, built, &build)
    count_lock = Mutex.new
    count = 0
    second = Queue.new
    allow(klass).to receive(:new).and_wrap_original do |original, *args, **kwargs|
      next original.call(*args, **kwargs) unless Thread.current[:pgbus_boot_memo_racer]

      mine = count_lock.synchronize { count += 1 }
      second.pop(timeout: 1) if mine == 1
      second << true if mine == 2
      (build ? build.call : original.call(*args, **kwargs)).tap { |object| built << object }
    end
  end

  def race
    start = Queue.new
    racers = Array.new(threads) do
      Thread.new do
        Thread.current[:pgbus_boot_memo_racer] = true
        start.pop
        yield
      end
    end
    threads.times { start << true }
    racers.map(&:value)
  end

  describe ".client" do
    it "builds one Client for 20 threads making the first call at once" do
      described_class.reset!
      built = Concurrent::Array.new
      hold_first_build(Pgbus::Client, built) { instance_double(Pgbus::Client, close: nil) }

      clients = race { described_class.client }

      expect(clients.uniq.size).to eq(1)
      expect(built.size).to eq(1)
    end
  end

  describe ".configuration" do
    it "is one Configuration for 20 threads reading it right after reset!" do
      described_class.reset!
      built = Concurrent::Array.new
      hold_first_build(Pgbus::Configuration, built)

      configurations = race { described_class.configuration }

      expect(configurations.uniq.size).to eq(1)
      expect(built).to be_empty
    end
  end

  describe ".stream" do
    it "is one Stream for 20 threads asking for the same name right after reset!" do
      described_class.reset!
      allow(described_class).to receive(:client).and_return(instance_double(Pgbus::Client))
      built = Concurrent::Array.new
      hold_first_build(Concurrent::Map, built)

      streams = race { described_class.stream("boot-memo") }

      expect(streams.uniq.size).to eq(1)
      expect(built).to be_empty
    end
  end
end
