# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::DedupCache do
  subject(:cache) { described_class.new(max_size: 5, ttl: 10) }

  describe "#seen?" do
    it "returns false for unseen keys" do
      expect(cache.seen?("event-1:Handler")).to be false
    end

    it "returns true for recently marked keys" do
      cache.mark!("event-1:Handler")
      expect(cache.seen?("event-1:Handler")).to be true
    end

    it "returns false for expired keys" do
      cache.mark!("event-1:Handler")
      # Simulate TTL expiry by backdating the entry
      cache.instance_variable_get(:@cache)["event-1:Handler"] =
        Process.clock_gettime(Process::CLOCK_MONOTONIC) - 20
      expect(cache.seen?("event-1:Handler")).to be false
    end
  end

  describe "#mark!" do
    it "adds the key to the cache" do
      cache.mark!("event-1:Handler")
      expect(cache.size).to eq(1)
    end

    it "evicts the oldest entry when max_size is reached" do
      5.times { |i| cache.mark!("event-#{i}:Handler") }
      expect(cache.size).to eq(5)

      cache.mark!("event-new:Handler")
      expect(cache.size).to eq(5)
      # oldest entry should be gone
      expect(cache.seen?("event-0:Handler")).to be false
      # newest should still be there
      expect(cache.seen?("event-new:Handler")).to be true
    end
  end

  describe "insertion-order bookkeeping" do
    def insertion_order(dedup)
      dedup.instance_variable_get(:@insertion_order)
    end

    def expire!(dedup, key)
      dedup.instance_variable_get(:@cache)[key] =
        Process.clock_gettime(Process::CLOCK_MONOTONIC) - 20
    end

    it "removes an expired-and-evicted key from @insertion_order" do
      cache.mark!("event-1:Handler")
      expire!(cache, "event-1:Handler")

      expect(cache.seen?("event-1:Handler")).to be false
      expect(insertion_order(cache)).not_to include("event-1:Handler")
    end

    it "keeps a re-marked key alive when older keys are evicted" do
      # victim goes in first, so its stale order entry sits at the front.
      cache.mark!("event-victim:Handler")
      cache.mark!("event-old-0:Handler")
      cache.mark!("event-old-1:Handler")

      # Expire victim, then re-mark it so it is now the NEWEST live entry —
      # but with the leak bug a stale duplicate lingers at the front of
      # @insertion_order and would be shifted first by evict_oldest.
      expire!(cache, "event-victim:Handler")
      cache.seen?("event-victim:Handler") # expiry eviction
      cache.mark!("event-victim:Handler") # re-mark (now newest)

      # Fill to max_size and push past it to trigger evict_oldest.
      cache.mark!("event-old-2:Handler")
      cache.mark!("event-old-3:Handler")
      cache.mark!("event-trigger:Handler")

      # old-0 (genuinely oldest) should be gone; the re-marked victim must survive.
      expect(cache.seen?("event-old-0:Handler")).to be false
      expect(cache.seen?("event-victim:Handler")).to be true
    end

    it "keeps @insertion_order bounded by @cache size across expire/re-mark cycles" do
      20.times do
        cache.mark!("event-recycle:Handler")
        expire!(cache, "event-recycle:Handler")
        cache.seen?("event-recycle:Handler")
      end

      expect(insertion_order(cache).size).to be <= cache.size + 1
    end
  end

  describe "#clear!" do
    it "removes all entries" do
      3.times { |i| cache.mark!("event-#{i}:Handler") }
      cache.clear!
      expect(cache.size).to eq(0)
    end
  end

  describe "thread safety" do
    it "handles concurrent access without errors" do
      big_cache = described_class.new(max_size: 1000, ttl: 60)
      threads = 10.times.map do |t|
        Thread.new do
          100.times do |i|
            key = "event-#{t}-#{i}:Handler"
            big_cache.mark!(key)
            big_cache.seen?(key)
          end
        end
      end
      threads.each(&:join)
      expect(big_cache.size).to be <= 1000
    end

    # mark! used to check `key?` and the size, then insert: two threads could
    # both pass the checks before either inserted (issue #527). The first
    # racer is held right after its +check+ until the second has made its own
    # (the old check-then-act: both get through every run) or is blocked on
    # the lock (mark! now). A racer counts as blocked only once it has
    # started, so one the scheduler has not run yet is waited for, not
    # mistaken for blocked.
    def race_mark(target, keys, check:)
      racers = []
      started = Concurrent::Set.new
      second_checked = Concurrent::AtomicBoolean.new(false)
      hold_first_check(target, check, racers, started, second_checked)

      go = Queue.new
      keys.each do |key|
        racers << Thread.new do
          go.pop
          started << Thread.current
          target.mark!(key)
        end
      end
      keys.size.times { go << true }
      racers.each(&:join)
    end

    def hold_first_check(target, check, racers, started, second_checked)
      count = Concurrent::AtomicFixnum.new(0)
      allow(target.instance_variable_get(:@cache)).to receive(check).and_wrap_original do |original, *args|
        present = original.call(*args)
        case count.increment
        when 1
          contender = (racers - [Thread.current]).first
          Thread.pass until second_checked.true? || (started.include?(contender) && contender.stop?)
        when 2 then second_checked.make_true
        end
        present
      end
    end

    it "records a key marked by two threads at once once" do
      race_mark(cache, %w[same:Handler same:Handler], check: :key?)

      expect(cache.instance_variable_get(:@insertion_order)).to eq(%w[same:Handler])
    end

    it "stays within max_size when two threads add the last keys at once" do
      small = described_class.new(max_size: 2, ttl: 10)
      small.mark!("first:Handler")

      race_mark(small, %w[second:Handler third:Handler], check: :size)

      expect(small.size).to eq(2)
    end
  end
end
