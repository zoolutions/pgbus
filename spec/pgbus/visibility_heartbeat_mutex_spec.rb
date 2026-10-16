# frozen_string_literal: true

require "spec_helper"
require "concurrent"

# The heartbeat's lock must exist before the first caller arrives: a lazily
# built `@mutex ||= Mutex.new` is itself a race, and two threads that both see
# nil each lock a Mutex of their own (issue #527).
RSpec.describe Pgbus::VisibilityHeartbeat do
  let(:threads) { 50 }

  # A just-loaded copy of the module, as the first job of a process sees it.
  # Evaluated inside an anonymous module so the real one is left alone.
  def fresh_heartbeat
    path = File.expand_path("../../lib/pgbus/visibility_heartbeat.rb", __dir__)
    sandbox = Module.new
    sandbox.module_eval(File.read(path), path)
    sandbox.const_get(:Pgbus)::VisibilityHeartbeat
  end

  # Holds the first lazy Mutex.new until a second racer has asked for one too,
  # so the old `||=` loses every run. Only the racing threads are held, and a
  # lock built at load never gets here.
  def hold_first_lazy_lock(created)
    count_lock = Mutex.new
    count = 0
    second = Queue.new
    allow(Thread::Mutex).to receive(:new).and_wrap_original do |original|
      next original.call unless Thread.current[:pgbus_heartbeat_racer]

      mine = count_lock.synchronize { count += 1 }
      second.pop(timeout: 1) if mine == 1
      second << true if mine == 2
      original.call.tap { |lock| created << lock }
    end
  end

  it "is one Mutex for 50 threads making their first call at once" do
    heartbeat = fresh_heartbeat
    created = Concurrent::Array.new
    hold_first_lazy_lock(created)
    start = Queue.new

    racers = Array.new(threads) do
      Thread.new do
        Thread.current[:pgbus_heartbeat_racer] = true
        start.pop
        heartbeat.send(:synchronize) do
          [heartbeat.instance_variable_get(:@mutex), *created].find(&:owned?)
        end
      end
    end
    threads.times { start << true }
    held = racers.map(&:value)

    expect(created).to be_empty
    expect(held.uniq.size).to eq(1)
  end

  it "builds its lock when the module loads" do
    expect(fresh_heartbeat.instance_variable_get(:@mutex)).to be_a(Thread::Mutex)
  end
end
