# frozen_string_literal: true

require_relative "../integration_helper"

# Issue #633: PGMQ lower-cases its table names, so pgbus spells every queue
# the way its table is. Seeding the pre-#633 state takes raw pgmq.* calls
# (the never-do "no direct PGMQ calls" exception for fixtures): the client
# can no longer produce a mixed-case queue.
RSpec.describe "Queue names in lower case (integration)", :integration do
  let(:client) { Pgbus.client }
  let(:conn) { ActiveRecord::Base.connection }

  def meta_rows(name)
    conn.select_values("SELECT queue_name FROM pgmq.meta WHERE lower(queue_name) = lower(#{conn.quote(name)}) ORDER BY 1")
  end

  def trigger_arg(table)
    conn.select_value(<<~SQL)
      SELECT encode(t.tgargs, 'escape')
      FROM pg_trigger t
      JOIN pg_class c ON c.oid = t.tgrelid
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = 'pgmq' AND c.relname = #{conn.quote(table)} AND NOT t.tgisinternal
    SQL
  end

  def drop_both(lower, mixed)
    conn.execute("DELETE FROM pgmq.meta WHERE queue_name = #{conn.quote(mixed)}")
    conn.execute("SELECT pgmq.drop_queue(#{conn.quote(lower)})") if meta_rows(lower).any?
  end

  it "registers one pgmq.meta row for two spellings of a queue, and both reach the same messages" do
    client.ensure_queue("CaseOrders")
    client.ensure_queue("caseorders")
    client.send_message("CaseOrders", { "n" => 1 })

    expect(meta_rows("pgbus_int_caseorders")).to eq(["pgbus_int_caseorders"])
    expect(client.read_batch("caseorders", qty: 5, vt: 0).size).to eq(1)
  ensure
    client.drop_queue("caseorders")
  end

  context "with LISTEN/NOTIFY on" do
    around do |example|
      saved = Pgbus.configuration.listen_notify
      Pgbus.configuration.listen_notify = true
      Pgbus.reset_client!
      example.run
    ensure
      Pgbus.configuration.listen_notify = saved
      Pgbus.reset_client!
    end

    # A process from before #633 re-arms the shared table's trigger with its
    # mixed-case name, so it NOTIFYs on a channel no current listener uses.
    it "re-arms a trigger armed under another spelling of the queue" do
      client.ensure_queue("CaseNotify")
      conn.execute("SELECT pgmq.create('pgbus_int_CaseNotify')")
      conn.execute("SELECT pgmq.enable_notify_insert('pgbus_int_CaseNotify', 250)")
      expect(trigger_arg("q_pgbus_int_casenotify")).to eq('pgbus_int_CaseNotify\\000')

      Pgbus.reset_client! # the next boot: a cold queue memo
      Pgbus.client.ensure_queue("CaseNotify")

      expect(trigger_arg("q_pgbus_int_casenotify")).to eq('pgbus_int_casenotify\\000')
      expect(Pgbus.client.notify_enabled?("CaseNotify")).to be(true)
    ensure
      drop_both("pgbus_int_casenotify", "pgbus_int_CaseNotify")
    end
  end

  # Until the fold migration runs, an install keeps its mixed-case pgmq.meta
  # rows; the tuning migrations build table names from them.
  it "tunes a mixed-case queue's lower-case tables" do
    conn.transaction do
      conn.execute("SELECT pgmq.create('pgbus_int_CaseTune')")

      conn.execute(Pgbus::TableMaintenance.fillfactor_sql_for_all_queues)
      conn.execute(Pgbus::AutovacuumTuning.sql_for_all_queues)

      options = conn.select_value("SELECT reloptions::text FROM pg_class WHERE relname = 'q_pgbus_int_casetune'")
      expect(options).to include("fillfactor=70").and(include("autovacuum_vacuum_scale_factor"))
      raise ActiveRecord::Rollback
    end
  end

  it "lists the pgmq.meta rows spelled in mixed case" do
    conn.execute("SELECT pgmq.create('pgbus_int_CaseList')")

    expect(client.mixed_case_queue_names).to include("pgbus_int_CaseList")
  ensure
    conn.execute("SELECT pgmq.drop_queue('pgbus_int_CaseList')")
  end

  it "folds mixed-case queues with rake pgbus:queues:fold_case" do
    require "rake"
    rake = Rake::Application.new
    original = Rake.application
    Rake.application = rake
    Rake::Task.define_task(:environment)
    load File.expand_path("../../lib/tasks/pgbus_queues.rake", __dir__)
    conn.execute("SELECT pgmq.create('pgbus_int_CaseRake')")

    expect { Rake::Task["pgbus:queues:fold_case"].invoke }.to output(/Folded: pgbus_int_CaseRake/).to_stdout
    expect(meta_rows("pgbus_int_caserake")).to eq(["pgbus_int_caserake"])
  ensure
    Rake.application = original
    conn.execute("SELECT pgmq.drop_queue('pgbus_int_caserake')") if meta_rows("pgbus_int_caserake").any?
  end

  describe Pgbus::QueueState do
    # The dashboard names a queue from its pgmq.meta row (now lower case); a
    # worker from its configuration ("Critical").
    it "finds a pause under any spelling of the queue" do
      described_class.pause!("critical", reason: "dashboard")

      expect(described_class.paused?("Critical")).to be(true)
      expect(described_class.find_by(queue_name: "CRITICAL")).to be_present
      expect(described_class.pluck(:queue_name)).to eq(["critical"])
    end
  end

  describe "dropping a queue" do
    # Until the fold migration runs, the dashboard lists both spellings and the
    # orphan stream sweep sees both; they name one table.
    it "removes only the pgmq.meta row of a mixed-case duplicate, keeping the shared table" do
      client.ensure_queue("casedup")
      client.send_message("casedup", { "n" => 1 })
      conn.execute("SELECT pgmq.create('pgbus_int_CaseDup')")

      client.drop_queue("pgbus_int_CaseDup", prefixed: false)

      expect(meta_rows("pgbus_int_casedup")).to eq(["pgbus_int_casedup"])
      expect(client.read_batch("casedup", qty: 5, vt: 0).size).to eq(1)
    ensure
      client.drop_queue("casedup")
    end

    it "re-arms the shared trigger under the lower-case name when the duplicate armed it last" do
      conn.execute("SELECT pgmq.create('pgbus_int_casearm')")
      conn.execute("SELECT pgmq.enable_notify_insert('pgbus_int_casearm', 250)")
      conn.execute("SELECT pgmq.create('pgbus_int_CaseArm')")
      conn.execute("SELECT pgmq.enable_notify_insert('pgbus_int_CaseArm', 0)")

      client.drop_queue("pgbus_int_CaseArm", prefixed: false)

      expect(trigger_arg("q_pgbus_int_casearm")).to eq('pgbus_int_casearm\\000')
      expect(conn.select_value("SELECT throttle_interval_ms FROM pgmq.notify_insert_throttle " \
                               "WHERE queue_name = 'pgbus_int_casearm'").to_i).to eq(250)
    ensure
      drop_both("pgbus_int_casearm", "pgbus_int_CaseArm")
    end

    it "drops a queue named in mixed case whose own row is gone (only the lower-case one is registered)" do
      client.ensure_queue("caseonly")

      client.drop_queue("pgbus_int_CaseOnly", prefixed: false)

      expect(meta_rows("pgbus_int_caseonly")).to be_empty
    end

    it "removes every spelling's pgmq.meta row with the table" do
      client.ensure_queue("casegone")
      conn.execute("SELECT pgmq.create('pgbus_int_CaseGone')")

      client.drop_queue("casegone")

      expect(meta_rows("pgbus_int_casegone")).to be_empty
    end

    it "clears failed events recorded under a mixed-case spelling of it" do
      client.ensure_queue("casedrop")
      conn.execute(<<~SQL)
        INSERT INTO pgbus_failed_events (queue_name, msg_id, source_queue)
        VALUES ('CaseDrop', 1, 'pgbus_int_casedrop'), ('pgbus_int_CaseDrop', 2, 'pgbus_int_casedrop')
      SQL

      client.drop_queue("casedrop")

      expect(conn.select_value("SELECT count(*) FROM pgbus_failed_events WHERE lower(queue_name) LIKE '%casedrop'").to_i)
        .to eq(0)
    end
  end
end
