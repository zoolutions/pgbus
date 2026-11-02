# frozen_string_literal: true

require_relative "../integration_helper"
require "erb"

# Issue #633: PGMQ lower-cases its table names, so before #633 a queue named
# "pgbus_Orders" lived in pgmq.q_pgbus_orders while pgmq.meta recorded the
# mixed-case spelling. pgbus now spells every queue in lower case; the
# pgbus:fold_queue_name_case migration brings an existing install's rows to
# that spelling without touching a table.
#
# The legacy state is seeded with raw pgmq.* calls on purpose (the
# never-do "no direct PGMQ calls" exception for fixtures): Pgbus::Client can
# no longer produce a mixed-case queue. Every example runs in a transaction
# that is rolled back.
RSpec.describe "pgbus:fold_queue_name_case migration (integration)", :integration do
  let(:conn) { ActiveRecord::Base.connection }
  let(:migration) { migration_from("fold_queue_name_case.rb.erb", :FoldPgbusQueueNameCase) }

  def migration_from(template, class_name)
    path = File.expand_path("../../lib/generators/pgbus/templates/#{template}", __dir__)
    version = "[#{ActiveRecord::VERSION::MAJOR}.#{ActiveRecord::VERSION::MINOR}]"
    source = ERB.new(File.read(path)).result_with_hash(migration_version: version)
    namespace = Module.new
    namespace.module_eval(source, path)
    namespace.const_get(class_name).new
  end

  def migrate
    migration.suppress_messages { migration.exec_migration(conn, :up) }
  end

  def rolled_back
    conn.transaction do
      yield
      raise ActiveRecord::Rollback
    end
  end

  def meta_names(like)
    conn.select_values("SELECT queue_name FROM pgmq.meta WHERE lower(queue_name) LIKE #{conn.quote(like)} ORDER BY 1")
  end

  def trigger_args(table)
    conn.select_values(<<~SQL)
      SELECT encode(t.tgargs, 'escape')
      FROM pg_trigger t
      JOIN pg_class c ON c.oid = t.tgrelid
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = 'pgmq' AND c.relname = #{conn.quote(table)} AND NOT t.tgisinternal
    SQL
  end

  def table_count(name)
    conn.select_value("SELECT count(*) FROM pgmq.q_#{name}").to_i
  end

  it "renames a mixed-case queue's pgmq.meta row, keeps its messages and re-arms NOTIFY under the new spelling" do
    rolled_back do
      conn.execute("SELECT pgmq.create('pgbus_int_fold_Orders')")
      conn.execute("SELECT pgmq.enable_notify_insert('pgbus_int_fold_Orders', 0)")
      conn.execute(%(SELECT pgmq.send('pgbus_int_fold_Orders', '{"n":1}'::jsonb)))

      migrate

      expect(meta_names("pgbus_int_fold_orders")).to eq(["pgbus_int_fold_orders"])
      expect(table_count("pgbus_int_fold_orders")).to eq(1)
      expect(trigger_args("q_pgbus_int_fold_orders").sole).to eq('pgbus_int_fold_orders\\000')
      expect(conn.select_rows(
               "SELECT queue_name, throttle_interval_ms FROM pgmq.notify_insert_throttle " \
               "WHERE lower(queue_name) = 'pgbus_int_fold_orders'"
             )).to eq([["pgbus_int_fold_orders", 0]])
    end
  end

  it "drops only the extra pgmq.meta row when both spellings exist, and keeps the lower-case row's NOTIFY throttle" do
    rolled_back do
      conn.execute("SELECT pgmq.create('pgbus_int_fold_twin')")
      conn.execute("SELECT pgmq.enable_notify_insert('pgbus_int_fold_twin', 250)")
      # Armed last, so the shared table's trigger carries the mixed-case name.
      conn.execute("SELECT pgmq.create('pgbus_int_fold_Twin')")
      conn.execute("SELECT pgmq.enable_notify_insert('pgbus_int_fold_Twin', 0)")
      conn.execute(%(SELECT pgmq.send('pgbus_int_fold_Twin', '{"n":1}'::jsonb)))
      conn.execute(%(SELECT pgmq.send('pgbus_int_fold_twin', '{"n":2}'::jsonb)))

      migrate

      expect(meta_names("pgbus_int_fold_twin")).to eq(["pgbus_int_fold_twin"])
      expect(table_count("pgbus_int_fold_twin")).to eq(2)
      expect(trigger_args("q_pgbus_int_fold_twin").sole).to eq('pgbus_int_fold_twin\\000')
      expect(conn.select_value(
        "SELECT throttle_interval_ms FROM pgmq.notify_insert_throttle WHERE queue_name = 'pgbus_int_fold_twin'"
      ).to_i).to eq(250)
    end
  end

  it "moves a mixed-case queue's topic bindings to the lower-case spelling" do
    rolled_back do
      conn.execute("SELECT pgmq.create('pgbus_int_fold_Topic')")
      conn.execute("SELECT pgmq.bind_topic('orders.*', 'pgbus_int_fold_Topic')")

      migrate

      expect(conn.select_rows(
               "SELECT pattern, queue_name FROM pgmq.topic_bindings WHERE lower(queue_name) = 'pgbus_int_fold_topic'"
             )).to eq([["orders.*", "pgbus_int_fold_topic"]])
    end
  end

  it "lower-cases pgbus's own queue-name columns that are matched against the new spelling" do
    rolled_back do
      conn.execute(<<~SQL)
        INSERT INTO pgbus_stream_queues (queue_name) VALUES ('pgbus_int_fold_Z2lk'), ('pgbus_int_fold_Dup'), ('pgbus_int_fold_dup');
        INSERT INTO pgbus_queue_states (queue_name, paused, created_at, updated_at)
          VALUES ('Critical', true, now(), now()), ('Mixed', true, now(), now()), ('mixed', false, now(), now());
        INSERT INTO pgbus_uniqueness_keys (lock_key, queue_name, msg_id) VALUES ('fold-lock', 'Critical', 7);
        INSERT INTO pgbus_failed_events (queue_name, msg_id, source_queue, error_message)
          VALUES ('Critical', 3, 'pgbus_Critical_p1', 'only'),
                 ('Critical', 4, 'pgbus_Critical_p1', 'before the deploy'),
                 ('Critical', 4, 'pgbus_critical_p1', 'after the deploy'),
                 ('Critical', 5, 'pgbus_Critical_p1', 'older spelling'),
                 ('Critical', 5, 'pgbus_CRITICAL_p1', 'newer spelling');
      SQL

      migrate

      expect(conn.select_values("SELECT queue_name FROM pgbus_stream_queues WHERE lower(queue_name) LIKE 'pgbus_int_fold_%' ORDER BY 1"))
        .to eq(%w[pgbus_int_fold_dup pgbus_int_fold_z2lk])
      expect(conn.select_rows("SELECT queue_name, paused FROM pgbus_queue_states ORDER BY 1"))
        .to eq([["critical", true], ["mixed", true]])
      expect(conn.select_value("SELECT queue_name FROM pgbus_uniqueness_keys WHERE lock_key = 'fold-lock'")).to eq("critical")
      # queue_name is the executor's logical name, kept as configured; the
      # source queue is the physical one, now spelled in lower case.
      expect(conn.select_rows("SELECT msg_id, source_queue, error_message FROM pgbus_failed_events ORDER BY msg_id"))
        .to eq([[3, "pgbus_critical_p1", "only"], [4, "pgbus_critical_p1", "after the deploy"],
                [5, "pgbus_critical_p1", "newer spelling"]])
      expect(conn.select_values("SELECT DISTINCT queue_name FROM pgbus_failed_events")).to eq(["Critical"])
    end
  end

  it "leaves a mixed-case queue that is not pgbus's (another PGMQ user of the schema) alone" do
    rolled_back do
      conn.execute("SELECT pgmq.create('OtherApp_Jobs')")

      migrate

      expect(meta_names("otherapp_jobs")).to eq(["OtherApp_Jobs"])
    end
  end

  it "folds a mixed-case row recreated after it ran, when run again" do
    rolled_back do
      conn.execute("SELECT pgmq.create('pgbus_int_fold_Again')")
      migrate
      conn.execute("SELECT pgmq.create('pgbus_int_fold_Again')") # an older process restarts

      migrate

      expect(meta_names("pgbus_int_fold_again")).to eq(["pgbus_int_fold_again"])
      expect { migrate }.not_to(change { meta_names("pgbus_int_fold_%") })
    end
  end

  it "is a no-op on an install with no mixed-case queue" do
    rolled_back do
      conn.execute("SELECT pgmq.create('pgbus_int_fold_plain')")

      expect { migrate }.not_to(change { meta_names("pgbus_int_fold_%") })
    end
  end
end
