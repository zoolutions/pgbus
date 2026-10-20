# frozen_string_literal: true

module Pgbus
  module Process
    # Immutable container-local readiness state, published by the supervisor
    # (one atomic swap per monitor pass) and read by the standalone health
    # server's accept thread — the immutability is what makes the cross-thread
    # handoff safe without a lock (issue #386).
    #
    # `expected` is the child count forked by boot_processes; `forked` is the
    # current fork-table size; `live` counts the forks that have checked in
    # (a process row on this host, or a liveness byte). A child sitting in
    # crash-restart backoff, or one that forked and hangs before checking in
    # (issue #587), keeps `live < expected`, which is exactly the signal a
    # rolling deploy's health gate needs to fail on: the replacement
    # container never goes ready, and the orchestrator keeps the old
    # container running. `forked` defaults to `live` for callers that do not
    # track check-ins.
    ReadinessSnapshot = Data.define(:booted, :shutting_down, :expected, :live, :forked) do
      def initialize(booted:, shutting_down:, expected:, live:, forked: live)
        super
      end

      def ready?
        booted && !shutting_down && live >= expected
      end

      # DRAINING wins over BOOTING: a supervisor told to stop mid-boot is
      # leaving, not arriving, and must never look like it will become ready.
      def status
        return "DRAINING" if shutting_down
        return "BOOTING" unless booted

        ready? ? "OK" : "DEGRADED"
      end
    end
  end
end
