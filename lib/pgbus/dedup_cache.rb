# frozen_string_literal: true

require "concurrent"

module Pgbus
  # Thread-safe in-memory dedup cache with TTL.
  # Sits in front of the pgbus_processed_events table to avoid
  # hitting the database for recently-seen (event_id, handler_class) pairs.
  #
  # Inspired by LavinMQ's built-in message deduplication with LRU + TTL.
  class DedupCache
    DEFAULT_MAX_SIZE = 10_000
    DEFAULT_TTL = 300 # 5 minutes

    def initialize(max_size: DEFAULT_MAX_SIZE, ttl: DEFAULT_TTL)
      @max_size = max_size
      @ttl = ttl
      @cache = Concurrent::Map.new
      @insertion_order = Concurrent::Array.new
      # Serializes the check-then-act writers (mark!, expiry eviction, clear!)
      # so the size bound and @insertion_order stay consistent (issue #527).
      # seen? reads @cache without it.
      @lock = Mutex.new
    end

    # Returns true if this key was already seen (duplicate).
    # Returns false if it's new (first time seen — caller should proceed).
    def seen?(key)
      entry = @cache[key]
      return false unless entry

      if expired?(entry)
        @lock.synchronize { evict(key) if @cache[key].equal?(entry) }
        return false
      end

      true
    end

    # Mark a key as seen. Call this after successfully claiming idempotency
    # in the database so future lookups skip the DB.
    def mark!(key)
      @lock.synchronize do
        already_present = @cache.key?(key)
        evict_oldest if !already_present && @cache.size >= @max_size

        @cache[key] = monotonic_now
        @insertion_order << key unless already_present
      end
    end

    def size
      @cache.size
    end

    def clear!
      @lock.synchronize do
        @cache.clear
        @insertion_order.clear
      end
    end

    private

    def expired?(timestamp)
      (monotonic_now - timestamp) > @ttl
    end

    def evict(key)
      @cache.delete(key)
      # Concurrent::Array#delete is O(n), but this only runs on expiry and n is
      # bounded by @max_size, so it stays cheap. Without it, @insertion_order
      # leaks and stale duplicates can evict live keys in evict_oldest.
      @insertion_order.delete(key)
    end

    def evict_oldest
      while @cache.size >= @max_size && !@insertion_order.empty?
        oldest_key = @insertion_order.shift
        # Skip residual order entries for keys already gone from @cache so a
        # stale duplicate never evicts a freshly re-marked (live) key.
        next unless @cache.key?(oldest_key)

        @cache.delete(oldest_key)
      end
    end

    def monotonic_now
      ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
    end
  end
end
