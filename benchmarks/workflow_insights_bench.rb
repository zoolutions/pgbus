# frozen_string_literal: true

# The workflow insights' query budget (issue #549), against a real database:
# about 1 M pgbus_workflow_steps rows over 20 workflow classes, spread across
# 30 days, then the two reads the insights pages make, timed:
#
#   workflow_definition(class, window: 24h)  budget 250 ms
#   workflow_definitions(window: 24h)        budget 500 ms
#
# A miss means an hourly rollup table (written by the dispatcher), as its own
# issue, not a slower page.
#
#   PGBUS_DATABASE_URL=postgres://localhost/pgbus_bench bundle exec ruby benchmarks/workflow_insights_bench.rb
#
# The tables and indexes mirror lib/generators/pgbus/templates/add_workflows.rb.erb
# (created when missing). Only rows whose workflow_id starts with "bench-" are
# written, and they are deleted before seeding and after the run.

require "active_record"
require "active_job"
require "uri"
require "pgbus"

DATABASE_URL = ENV.fetch("PGBUS_DATABASE_URL") do
  abort "PGBUS_DATABASE_URL not set. Example: postgres://localhost/pgbus_bench"
end

CLASSES = 20
RUNS = Integer(ENV.fetch("INSIGHTS_BENCH_RUNS", "263000"))
SAMPLES = 7
BUDGETS_MS = { definition: 250, definitions: 500 }.freeze

ActiveRecord::Base.establish_connection(DATABASE_URL)
ActiveRecord::Base.logger = nil
Pgbus.configure do |c|
  c.database_url = DATABASE_URL
  c.logger = Logger.new(IO::NULL)
  c.pgmq_schema_mode = :embedded
  c.listen_notify = false
  c.stats_enabled = false
end

CONN = ActiveRecord::Base.connection

def create_tables!
  CONN.execute(<<~SQL)
    CREATE TABLE IF NOT EXISTS pgbus_workflows (
      id BIGSERIAL PRIMARY KEY, workflow_id VARCHAR NOT NULL, workflow_class VARCHAR NOT NULL,
      subject_type VARCHAR, subject_id VARCHAR, subject_gid VARCHAR, arguments JSONB NOT NULL DEFAULT '{}',
      status VARCHAR NOT NULL DEFAULT 'ready', current_step VARCHAR, step_job_options JSONB NOT NULL DEFAULT '{}',
      error_class VARCHAR, error_message TEXT, error_backtrace TEXT, paused_at TIMESTAMP, finished_at TIMESTAMP,
      cancelled_at TIMESTAMP, idempotency_key VARCHAR, definition_digest VARCHAR NOT NULL,
      rerun_of_workflow_id VARCHAR, created_at TIMESTAMP NOT NULL, updated_at TIMESTAMP NOT NULL
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_pgbus_workflows_workflow_id ON pgbus_workflows (workflow_id);
    CREATE INDEX IF NOT EXISTS idx_pgbus_workflows_status ON pgbus_workflows (status, updated_at);
    CREATE INDEX IF NOT EXISTS idx_pgbus_workflows_class_status ON pgbus_workflows (workflow_class, status, created_at);
    CREATE TABLE IF NOT EXISTS pgbus_workflow_steps (
      id BIGSERIAL PRIMARY KEY, workflow_id VARCHAR NOT NULL, workflow_class VARCHAR NOT NULL, step_name VARCHAR NOT NULL,
      attempt INTEGER NOT NULL DEFAULT 1, status VARCHAR NOT NULL DEFAULT 'scheduled', job_id VARCHAR, msg_id BIGINT,
      scheduled_at TIMESTAMP NOT NULL, enqueued_at TIMESTAMP, started_at TIMESTAMP, finished_at TIMESTAMP,
      stall_at TIMESTAMP, outcome VARCHAR, output JSONB, error_class VARCHAR, error_message TEXT, error_backtrace TEXT,
      error_causes JSONB, worker VARCHAR, app_revision VARCHAR, batch_id VARCHAR, wait_for VARCHAR, signal_id BIGINT,
      waiting_since TIMESTAMP, timeout_at TIMESTAMP, created_at TIMESTAMP NOT NULL
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_pgbus_workflow_steps_attempt ON pgbus_workflow_steps (workflow_id, step_name, attempt);
    CREATE INDEX IF NOT EXISTS idx_pgbus_workflow_steps_scheduled ON pgbus_workflow_steps (status, scheduled_at);
    CREATE INDEX IF NOT EXISTS idx_pgbus_workflow_steps_stall ON pgbus_workflow_steps (status, stall_at);
    CREATE INDEX IF NOT EXISTS idx_pgbus_workflow_steps_insights ON pgbus_workflow_steps (workflow_class, step_name, finished_at);
    CREATE TABLE IF NOT EXISTS pgbus_workflow_events (
      id BIGSERIAL PRIMARY KEY, workflow_id VARCHAR NOT NULL, step_id BIGINT, step_name VARCHAR, attempt INTEGER,
      event VARCHAR NOT NULL, actor VARCHAR NOT NULL, reason TEXT, details JSONB NOT NULL DEFAULT '{}',
      created_at TIMESTAMP NOT NULL
    );
  SQL
end

def clean!
  CONN.execute("DELETE FROM pgbus_workflow_steps WHERE workflow_id LIKE 'bench-%'")
  CONN.execute("DELETE FROM pgbus_workflows WHERE workflow_id LIKE 'bench-%'")
end

# Each run reaches step s1..s4 by its status: finished runs completed all
# four, a cancelled run stopped at s2, a ready run waits on s1, a performing
# one runs s2, a waiting one waits on s3, a paused one failed s2.
def seed!
  CONN.execute("SELECT setseed(0.549)")
  CONN.execute(<<~SQL)
    INSERT INTO pgbus_workflows (workflow_id, workflow_class, status, current_step, definition_digest, created_at,
                                 updated_at, finished_at, cancelled_at, paused_at)
    SELECT 'bench-' || g, 'BenchWorkflow' || (g % #{CLASSES}), r.status, r.step, 'bench', r.created_at,
           COALESCE(r.finished_at, r.cancelled_at, r.paused_at, r.created_at), r.finished_at, r.cancelled_at, r.paused_at
    FROM generate_series(1, #{RUNS}) g
    CROSS JOIN LATERAL (SELECT now()::timestamp - (random() * interval '30 days') AS created_at, g % 100 AS bucket) t
    CROSS JOIN LATERAL (
      SELECT CASE WHEN t.bucket < 90 THEN 'finished' WHEN t.bucket < 93 THEN 'cancelled' WHEN t.bucket < 95 THEN 'ready'
                  WHEN t.bucket < 96 THEN 'performing' WHEN t.bucket < 98 THEN 'waiting' ELSE 'paused' END AS status,
             CASE WHEN t.bucket < 90 THEN 's4' WHEN t.bucket < 93 THEN 's2' WHEN t.bucket < 95 THEN 's1'
                  WHEN t.bucket < 96 THEN 's2' WHEN t.bucket < 98 THEN 's3' ELSE 's2' END AS step,
             t.created_at,
             CASE WHEN t.bucket < 90 THEN t.created_at + (random() * interval '1 hour') END AS finished_at,
             CASE WHEN t.bucket BETWEEN 90 AND 92 THEN t.created_at + interval '10 minutes' END AS cancelled_at,
             CASE WHEN t.bucket >= 98 THEN t.created_at + interval '5 minutes' END AS paused_at
    ) r
  SQL
  CONN.execute(<<~SQL)
    INSERT INTO pgbus_workflow_steps (workflow_id, workflow_class, step_name, attempt, status, outcome, scheduled_at,
                                      started_at, finished_at, error_class, app_revision, created_at)
    SELECT w.workflow_id, w.workflow_class, 's' || n, 1, s.status, s.outcome, s.scheduled_at,
           CASE WHEN s.status <> 'scheduled' AND s.status <> 'cancelled' THEN s.scheduled_at + (random() * interval '2 seconds') END,
           CASE WHEN s.status IN ('completed', 'failed', 'cancelled') THEN s.scheduled_at + (random() * interval '30 seconds') END,
           CASE WHEN s.status = 'failed' THEN 'Timeout::Error' END,
           'rev' || (EXTRACT(EPOCH FROM w.created_at)::bigint / 345600), s.scheduled_at
    FROM pgbus_workflows w
    CROSS JOIN generate_series(1, 4) n
    CROSS JOIN LATERAL (
      SELECT w.created_at + ((n - 1) * interval '40 seconds') AS scheduled_at,
             CASE WHEN n < substr(w.current_step, 2)::int OR w.status = 'finished' THEN 'completed'
                  WHEN w.status = 'cancelled' THEN 'cancelled' WHEN w.status = 'ready' THEN 'scheduled'
                  WHEN w.status = 'performing' THEN 'in_progress' WHEN w.status = 'waiting' THEN 'waiting'
                  ELSE 'failed' END AS status,
             CASE WHEN n < substr(w.current_step, 2)::int OR w.status = 'finished' THEN 'completed'
                  WHEN w.status = 'cancelled' THEN 'cancelled' WHEN w.status = 'paused' THEN 'paused' END AS outcome
    ) s
    WHERE w.workflow_id LIKE 'bench-%' AND n <= substr(w.current_step, 2)::int
  SQL
  CONN.execute("ANALYZE pgbus_workflows")
  CONN.execute("ANALYZE pgbus_workflow_steps")
end

def median_and_max_ms
  timings = Array.new(SAMPLES) do
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    yield
    (Process.clock_gettime(Process::CLOCK_MONOTONIC) - started) * 1000
  end
  sorted = timings.sort
  [sorted[sorted.size / 2], sorted.last]
end

CLASSES.times do |i|
  Object.const_set("BenchWorkflow#{i}", Class.new(Pgbus::Workflow) { %i[s1 s2 s3 s4].each { |name| step(name) { nil } } })
end

create_tables!
clean!
seed_started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
seed!
seed_seconds = Process.clock_gettime(Process::CLOCK_MONOTONIC) - seed_started
counts = CONN.select_rows(<<~SQL).first.map(&:to_i)
  SELECT (SELECT COUNT(*) FROM pgbus_workflows WHERE workflow_id LIKE 'bench-%'),
         (SELECT COUNT(*) FROM pgbus_workflow_steps WHERE workflow_id LIKE 'bench-%')
SQL

puts "=" * 70
puts "Pgbus workflow insights query budget (issue #549)"
puts "  DB: #{DATABASE_URL.sub(%r{//[^@]*@}, "//***@")}  PostgreSQL #{CONN.select_value("SHOW server_version")}"
puts "  #{counts[0]} runs, #{counts[1]} step rows over #{CLASSES} classes, 30 days (seeded in #{seed_seconds.round(1)}s)"
puts "=" * 70

data_source = Pgbus::Web::DataSource.new(client: nil)
window = Pgbus::Web::WorkflowWindow.parse("24h", retention: nil)
results = {}
begin
  data_source.workflow_definition("BenchWorkflow7", window: window) # warm the plan cache
  results[:definition] = median_and_max_ms { data_source.workflow_definition("BenchWorkflow7", window: window) }
  data_source.workflow_definitions(window: window)
  results[:definitions] = median_and_max_ms { data_source.workflow_definitions(window: window) }

  detail = data_source.workflow_definition("BenchWorkflow7", window: window)
  puts "  sanity: BenchWorkflow7 started #{detail[:started]}, finished #{detail[:finished]}, " \
       "#{detail[:steps].sum { |s| s[:sample_size] }} attempts in the window"
  results.each do |name, (median, max)|
    verdict = median <= BUDGETS_MS.fetch(name) ? "within" : "OVER"
    puts format("  %-34s median %7.1f ms  max %7.1f ms  (budget %d ms: %s)",
                name == :definition ? "workflow_definition(24h)" : "workflow_definitions(24h)", median, max,
                BUDGETS_MS.fetch(name), verdict)
  end
ensure
  clean!
end

exit(results.all? { |name, (median, _)| median <= BUDGETS_MS.fetch(name) } ? 0 : 1)
