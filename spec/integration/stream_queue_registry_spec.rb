# frozen_string_literal: true

require_relative "../integration_helper"

# Issue #401: record! used upsert(unique_by:), which resolves the unique
# index through the pool's schema cache. SchemaCache#data_source_exists?
# caches negative answers permanently, and SchemaCache#indexes returns []
# whenever that cached probe says false — so one wrong first probe (while
# the table genuinely exists) poisoned every record! in the process with
# "No unique index found for queue_name" until restart.
RSpec.describe "StreamQueue registry (integration)", :integration do
  before { Pgbus::StreamQueue.reset_cache! }

  def registry_count(queue_name)
    ActiveRecord::Base.connection.select_value(
      "SELECT COUNT(*) FROM pgbus_stream_queues WHERE queue_name = #{ActiveRecord::Base.connection.quote(queue_name)}"
    )
  end

  describe ".record! with a poisoned pool schema cache (issue #401 regression)" do
    it "still registers the queue after data_source_exists? latched a false negative" do
      # Warm the table_exists? memo first. The guard is a live query
      # (data_source_sql, not the pool schema cache) so it stays true under
      # the poison below — asserting that here keeps the example targeting
      # the index-resolution path explicitly rather than by side effect.
      expect(Pgbus::StreamQueue.table_exists?).to be(true)

      pool = Pgbus::StreamQueue.connection_pool
      cache = pool.schema_reflection.send(:cache, pool)
      cache.instance_variable_get(:@data_sources)["pgbus_stream_queues"] = false

      expect(Pgbus::StreamQueue.record!("pgbus_int_poisoned_probe")).to be(true)
      expect(registry_count("pgbus_int_poisoned_probe")).to eq(1)
    ensure
      cache&.instance_variable_get(:@data_sources)&.delete("pgbus_stream_queues")
    end
  end

  describe ".record! schema-cache traffic" do
    it "issues no schema-cache (SCHEMA) queries once the table_exists? memo is warm" do
      expect(Pgbus::StreamQueue.table_exists?).to be(true) # warm the live-probe memo
      # table_exists? is memoized across examples, so it may not touch the
      # connection here. A connection another example left disconnected (or a
      # dead backend) would connect inside record!, and connecting runs AR's
      # own SCHEMA queries (configure_connection, the type map). Start from a
      # disconnected pool and connect before subscribing, so the only SCHEMA
      # queries that can show up are record!'s.
      Pgbus::StreamQueue.connection_pool.disconnect!
      Pgbus::StreamQueue.connection.verify!

      events = []
      callback = ->(_name, _start, _finish, _id, payload) { events << payload }
      ActiveSupport::Notifications.subscribed(callback, "sql.active_record") do
        expect(Pgbus::StreamQueue.record!("pgbus_int_no_schema_probe")).to be(true)
      end

      schema_queries = events.select { |payload| payload[:name] == "SCHEMA" }
      expect(schema_queries).to be_empty
      expect(events.map { |payload| payload[:sql] })
        .to include(a_string_matching(/INSERT INTO "pgbus_stream_queues"/))
    end
  end

  describe ".record! idempotency" do
    it "is idempotent and keeps the first registration row (ON CONFLICT DO NOTHING)" do
      expect(Pgbus::StreamQueue.record!("pgbus_int_idempotent")).to be(true)
      expect(Pgbus::StreamQueue.record!("pgbus_int_idempotent")).to be(true)

      expect(registry_count("pgbus_int_idempotent")).to eq(1)
    end
  end
end
