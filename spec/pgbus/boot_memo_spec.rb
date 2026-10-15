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
  # for one too (an unlocked `||=`: both build, every run) or every other
  # racer has started and is blocked (the lock: they wait for the first).
  # Only the racing threads are held; anything built before the race is left
  # alone. No timeout decides the outcome.
  def hold_first_build(klass, built, &build)
    count = Concurrent::AtomicFixnum.new(0)
    second = Concurrent::AtomicBoolean.new(false)
    allow(klass).to receive(:new).and_wrap_original do |original, *args, **kwargs|
      next original.call(*args, **kwargs) unless Thread.current[:pgbus_boot_memo_racer]

      case count.increment
      when 1 then Thread.pass until second.true? || others_blocked?
      when 2 then second.make_true
      end
      (build ? build.call : original.call(*args, **kwargs)).tap { |object| built << object }
    end
  end

  def racers = (@racers ||= Concurrent::Array.new)
  def started = (@started ||= Concurrent::Set.new)

  def others_blocked?
    (racers - [Thread.current]).all? { |racer| started.include?(racer) && racer.stop? }
  end

  def race(&)
    go = Queue.new
    threads.times do
      racers << Thread.new do
        Thread.current[:pgbus_boot_memo_racer] = true
        go.pop
        started << Thread.current
        yield
      end
    end
    threads.times { go << true }
    racers.each do |racer|
      racer.join
    rescue StandardError
      nil # raised again by value below, once every racer is done
    end
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
