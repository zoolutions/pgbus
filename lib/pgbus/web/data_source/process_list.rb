# frozen_string_literal: true

module Pgbus
  module Web
    class DataSource
      # The Processes page's rows and the context ProcessState needs to put
      # them in words (issue #493). Every status in one result set is derived
      # against one ProcessState::Thresholds, so one now.
      module ProcessList
        # thresholds: pass the ones the caller puts the rows in words with, so
        # badge and sentence share one now.
        def processes(thresholds: process_thresholds)
          rows = connection.select_all("SELECT * FROM pgbus_processes ORDER BY kind, created_at")
          rows.to_a.map { |r| format_process(r, thresholds) }
        rescue StandardError => e
          Pgbus.logger.debug { "[Pgbus::Web] Error fetching processes: #{e.message}" }
          []
        end

        # The dashboard host's config: the stall limits the page states are
        # this host's stall_threshold, as the badge always was.
        def process_thresholds(now: Time.now)
          config = Pgbus.configuration
          ProcessState::Thresholds.new(
            now: now, alive_threshold: Process::Heartbeat::ALIVE_THRESHOLD, stall_threshold: config.stall_threshold,
            dispatch_interval: config.dispatch_interval, recurring_schedule_interval: config.recurring_schedule_interval,
            polling_interval: config.polling_interval, notify_fallback_poll: Process::Consumer::NOTIFY_FALLBACK_POLL_SECONDS,
            notify_wakeup: config.worker_notify_wakeup?, reap_interval: Process::Dispatcher::REAP_INTERVAL
          )
        end

        def process_context(rows:, thresholds: process_thresholds)
          ProcessState.context_for(rows, thresholds)
        end

        private

        def format_process(row, thresholds)
          heartbeat = JobState.time(row["last_heartbeat_at"])
          metadata = row["metadata"].is_a?(String) ? JSON.parse(row["metadata"]) : row["metadata"]
          status = ProcessState.derive(kind: row["kind"], metadata: metadata, heartbeat_at: heartbeat,
                                       thresholds: thresholds)
          started_at = metadata.is_a?(Hash) && metadata["started_at"]

          {
            id: row["id"].to_i,
            kind: row["kind"],
            hostname: row["hostname"],
            pid: row["pid"].to_i,
            metadata: metadata,
            last_heartbeat_at: heartbeat,
            healthy: status == :healthy,
            status: status,
            created_at: row["created_at"],
            supervisor_pid: metadata.is_a?(Hash) ? metadata["supervisor_pid"] : nil,
            started_at: started_at ? Time.at(started_at.to_f) : nil
          }
        end
      end
    end
  end
end
