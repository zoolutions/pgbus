# frozen_string_literal: true

require "json"

module Pgbus
  module Deploy
    # Has the fleet rolled over to +revision+? (issue #536). A pure read of
    # pgbus_processes: live rows (heartbeat within Heartbeat::ALIVE_THRESHOLD)
    # whose metadata["revision"] differs from +revision+ are lagging (a missing
    # revision is a pre-upgrade process, so it lags too), and at least one live
    # worker on +revision+ must exist. The scheduler and the dashboard both
    # consume the Status, so the page prints the reason the scheduler acts on.
    module Rollover
      # reason – :ready, :no_revision, :lagging or :no_workers.
      # lagging – [{ kind:, hostname:, pid:, revision: }].
      # live_by_revision – { revision => live process count }, nil for rows without one.
      Status = Data.define(:revision, :lagging, :workers_live, :reason, :live_by_revision) do
        def rolled_over?
          reason == :ready
        end
      end

      module_function

      def check(revision:, now: Time.current)
        cutoff = now - Process::Heartbeat::ALIVE_THRESHOLD
        rows = ProcessEntry.where(last_heartbeat_at: cutoff..).pluck(:kind, :hostname, :pid, :metadata)
        evaluate(rows.map { |kind, hostname, pid, metadata| { kind:, hostname:, pid:, metadata: } }, revision: revision)
      end

      # [live, revision] of the newest live scheduler row: the revision the
      # dashboard judges the rollover against (the one the scheduler acts on).
      def scheduler_revision(now: Time.current)
        row = ProcessEntry.where(kind: "scheduler", last_heartbeat_at: (now - Process::Heartbeat::ALIVE_THRESHOLD)..)
                          .order(created_at: :desc, id: :desc).first
        row ? [true, revision_of(row.metadata)] : [false, nil]
      end

      # rows – live process rows as { kind:, hostname:, pid:, metadata: }.
      def evaluate(rows, revision:)
        live = rows.map { |r| { kind: r[:kind], hostname: r[:hostname], pid: r[:pid], revision: revision_of(r[:metadata]) } }
        lagging = live.reject { |r| revision && r[:revision] == revision }
        workers_live = live.count { |r| r[:kind] == "worker" && revision && r[:revision] == revision }

        Status.new(revision: revision, lagging: revision ? lagging : [], workers_live: workers_live,
                   reason: reason_for(revision, lagging, workers_live), live_by_revision: live.map { |r| r[:revision] }.tally)
      end

      def reason_for(revision, lagging, workers_live)
        return :no_revision if revision.nil?
        return :lagging if lagging.any?
        return :no_workers if workers_live.zero?

        :ready
      end

      def revision_of(metadata)
        metadata = JSON.parse(metadata) if metadata.is_a?(String)
        metadata.is_a?(Hash) ? metadata["revision"] : nil
      rescue JSON::ParserError
        nil
      end
    end
  end
end
