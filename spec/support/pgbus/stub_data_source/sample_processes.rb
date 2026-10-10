# frozen_string_literal: true

module Pgbus
  module Test
    class StubDataSource
      # The Processes page's sample data (issue #493): every kind and every
      # status, a supervisor with its children, a standalone stalled consumer,
      # the two forks of a processes: 2 capsule (#503) and a worker from before
      # the new heartbeat keys. Rows carry status: as DataSource derives it;
      # the stub never derives. Part of fill_sample_data!.
      module SampleProcesses
        SUPERVISOR_PID = 100

        def sample_processes(now)
          [supervisor_row(now), *supervised_rows(now), standalone_consumer(now), *capsule_forks(now),
           pre_upgrade_worker(now)]
        end

        private

        def process_row(id, kind, pid, now, hostname: "web-01.prod", status: :healthy, heartbeat_ago: 12,
                        metadata: {})
          { id: id, kind: kind, hostname: hostname, pid: pid, metadata: { "pid" => pid }.merge(metadata),
            last_heartbeat_at: now - heartbeat_ago, healthy: status == :healthy, status: status,
            created_at: now - 3600, supervisor_pid: metadata["supervisor_pid"],
            started_at: metadata["started_at"] && Time.at(metadata["started_at"]) }
        end

        def child(now, started_ago, tick_ago, extra = {})
          { "supervisor_pid" => SUPERVISOR_PID, "started_at" => now.to_f - started_ago,
            "loop_tick_at" => tick_ago && (now.to_f - tick_ago) }.compact.merge(extra)
        end

        def supervisor_row(now)
          process_row(1, "supervisor", SUPERVISOR_PID, now,
                      metadata: { "hostname" => "web-01.prod", "started_at" => now.to_f - 10_800 })
        end

        def supervised_rows(now)
          [
            process_row(2, "worker", 101, now, heartbeat_ago: 5, metadata: child(now, 2520, 5, healthy_worker_metadata)),
            process_row(3, "worker", 102, now, status: :stalled, heartbeat_ago: 20,
                                               metadata: child(now, 5400, 120, stalled_worker_metadata)),
            process_row(4, "dispatcher", 103, now, metadata: child(now, 10_790, 3)),
            process_row(5, "scheduler", 104, now, metadata: child(now, 10_790, 2, "tasks" => 14)),
            process_row(6, "outbox_poller", 105, now, status: :stale, heartbeat_ago: 420,
                                                      metadata: child(now, 10_790, nil))
          ]
        end

        def healthy_worker_metadata
          { "queues" => %w[default mailers], "threads" => 5, "execution_mode" => "threads", "consumer_priority" => 0,
            "capacity" => 5, "busy" => 3, "buffered" => 2, "in_flight" => 5, "jobs_processed" => 1240,
            "jobs_failed" => 3, "rates" => { "processed" => 12.4, "failed" => 0.2, "dequeued" => 12.6 },
            "memory_mb" => 310, "max_jobs" => 10_000, "max_memory_mb" => 512, "max_lifetime" => 3600 }
        end

        def stalled_worker_metadata
          { "queues" => %w[imports], "threads" => 2, "execution_mode" => "threads", "consumer_priority" => 0,
            "capacity" => 2, "busy" => 2, "buffered" => 0, "jobs_processed" => 88, "memory_mb" => 455,
            "max_jobs" => 10_000, "max_memory_mb" => 512, "max_lifetime" => 3600 }
        end

        def standalone_consumer(now)
          process_row(7, "consumer", 200, now, hostname: "worker-02.prod", status: :stalled, heartbeat_ago: 30,
                                               metadata: {
                                                 "supervisor_pid" => 1, "started_at" => now.to_f - 86_400,
                                                 "loop_tick_at" => now.to_f - 150, "topics" => ["orders.*", "users.*"],
                                                 "threads" => 3, "capacity" => 3, "busy" => 1, "buffered" => 0,
                                                 "jobs_processed" => 5230, "memory_mb" => 220
                                               })
        end

        # A processes: 2 capsule (issue #503): one row per fork, standalone.
        def capsule_forks(now)
          [1, 2].map do |n|
            process_row(7 + n, "worker", 70_000 + n, now, hostname: "worker-02.prod", heartbeat_ago: 5 * n,
                                                          metadata: {
                                                            "capsule" => "render", "process" => "#{n}/2",
                                                            "queues" => ["render"], "threads" => 1,
                                                            "capacity" => 1, "busy" => n - 1,
                                                            "started_at" => now.to_f - 3600, "loop_tick_at" => now.to_f - n
                                                          })
          end
        end

        # A worker from before the issue #493 keys: no started_at, no
        # supervisor_pid, no pool or limits; queues as a comma String.
        def pre_upgrade_worker(now)
          process_row(10, "worker", 300, now, hostname: "worker-03.prod", heartbeat_ago: 8,
                                              metadata: {
                                                "queues" => "default,mailers", "threads" => 5,
                                                "rates" => { "processed" => 0.0 }, "loop_tick_at" => now.to_f - 4
                                              })
        end
      end
    end
  end
end
