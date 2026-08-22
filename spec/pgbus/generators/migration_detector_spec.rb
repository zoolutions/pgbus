# frozen_string_literal: true

require "spec_helper"
require "pgbus/generators/migration_detector"

RSpec.describe Pgbus::Generators::MigrationDetector do
  subject(:detector) { described_class.new(connection) }

  let(:connection) { double("ActiveRecord::Connection") }

  # Default: every core table exists so fresh_install? is false. Tests
  # override specific tables via stub_table / stub_missing helpers.
  before do
    stub_tables(described_class::CORE_INSTALL_TABLES + %w[pgbus_uniqueness_keys pgbus_failed_events pgbus_batch_executions])
    allow(connection).to receive_messages(columns: [], indexes: [])
    stub_columns("pgbus_batches", %w[id batch_id on_failure_class failed_jobs completed_jobs total_jobs status])

    # Default: autovacuum already tuned so it doesn't pollute other tests.
    # Tests for autovacuum detection override these stubs explicitly.
    allow(connection).to receive(:select_value)
      .with("SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pgmq'")
      .and_return(1)
    allow(connection).to receive(:select_value)
      .with("SELECT queue_name FROM pgmq.meta ORDER BY queue_name LIMIT 1")
      .and_return("pgbus_default")
    allow(connection).to receive(:select_value)
      .with(/reloptions.*autovacuum_vacuum_scale_factor/)
      .and_return("t")
    # Default: fillfactor already applied so it doesn't pollute other tests.
    allow(connection).to receive(:select_value)
      .with(/reloptions.*fillfactor/)
      .and_return("t")
  end

  # Test helpers -----------------------------------------------------------
  #
  # Tests in this file stub connection.table_exists? by building up a
  # set of "present" tables and routing the predicate through it. That
  # lets us write tests as "given the DB has tables X and Y, expect the
  # detector to return..." without tons of boilerplate.

  def stub_tables(names)
    present = Set.new(names.map(&:to_s))
    allow(connection).to receive(:table_exists?) { |name| present.include?(name.to_s) }
    @present_tables = present
  end

  # Both add_table and remove_table mutate the set captured by
  # stub_tables. The instance-variable pattern is deliberate: helper
  # methods need a shared reference across before/example boundaries,
  # and let() blocks would need a workaround for the mutation.
  def add_table(name)
    @present_tables << name.to_s # rubocop:disable RSpec/InstanceVariable
  end

  def remove_table(name)
    @present_tables.delete(name.to_s) # rubocop:disable RSpec/InstanceVariable
  end

  def stub_columns(table, names)
    allow(connection).to receive(:columns).with(table).and_return(
      names.map { |n| double("Column", name: n) }
    )
  end

  def stub_indexes(table, names)
    allow(connection).to receive(:indexes).with(table).and_return(
      names.map { |n| double("Index", name: n) }
    )
  end

  describe "#missing_migrations" do
    context "when the database has no pgbus tables at all" do
      it "returns the FRESH_INSTALL sentinel so the caller redirects to pgbus:install" do
        stub_tables([]) # truly empty

        expect(detector.missing_migrations).to eq([described_class::FRESH_INSTALL])
      end
    end

    context "when pgbus_uniqueness_keys is present and up to date" do
      it "does not queue the uniqueness_keys migration" do
        expect(detector.missing_migrations).not_to include(:uniqueness_keys)
      end
    end

    context "when pgbus_uniqueness_keys is missing" do
      before { remove_table("pgbus_uniqueness_keys") }

      it "queues the uniqueness_keys migration" do
        expect(detector.missing_migrations).to include(:uniqueness_keys)
      end

      it "queues it exactly once even if legacy pgbus_job_locks also exists" do
        add_table("pgbus_job_locks")

        # add_uniqueness_keys only creates the modern table — it does not
        # touch a legacy pgbus_job_locks table. Its presence alongside a
        # missing pgbus_uniqueness_keys should still queue exactly one
        # entry; retiring the legacy table is a separate, manual
        # `pgbus:migrate_job_locks` step.
        expect(detector.missing_migrations.count(:uniqueness_keys)).to eq(1)
      end
    end

    context "with job stats detection" do
      context "when pgbus_job_stats is missing entirely" do
        before { remove_table("pgbus_job_stats") }

        it "queues add_job_stats + latency + queue_index so the table lands on the current schema" do
          missing = detector.missing_migrations
          expect(missing).to include(:add_job_stats, :add_job_stats_latency, :add_job_stats_queue_index)
        end
      end

      context "when pgbus_job_stats exists without latency columns" do
        before do
          add_table("pgbus_job_stats")
          stub_columns("pgbus_job_stats", %w[id job_class queue_name status duration_ms created_at])
          stub_indexes("pgbus_job_stats", %w[idx_pgbus_job_stats_time idx_pgbus_job_stats_queue_time])
        end

        it "queues add_job_stats_latency but not add_job_stats" do
          missing = detector.missing_migrations
          expect(missing).to include(:add_job_stats_latency)
          expect(missing).not_to include(:add_job_stats)
        end

        it "does NOT queue add_job_stats_queue_index when that index already exists" do
          expect(detector.missing_migrations).not_to include(:add_job_stats_queue_index)
        end
      end

      context "when pgbus_job_stats exists with latency columns but missing queue_index" do
        before do
          add_table("pgbus_job_stats")
          stub_columns("pgbus_job_stats", %w[id job_class queue_name status duration_ms enqueue_latency_ms retry_count created_at])
          stub_indexes("pgbus_job_stats", %w[idx_pgbus_job_stats_time idx_pgbus_job_stats_class_time])
        end

        it "queues only add_job_stats_queue_index" do
          missing = detector.missing_migrations
          expect(missing).to include(:add_job_stats_queue_index)
          expect(missing).not_to include(:add_job_stats, :add_job_stats_latency)
        end
      end
    end

    context "with stream stats detection" do
      it "queues add_stream_stats when the table is missing" do
        expect(detector.missing_migrations).to include(:add_stream_stats)
      end

      it "does not queue add_stream_stats when the table already exists" do
        add_table("pgbus_stream_stats")

        expect(detector.missing_migrations).not_to include(:add_stream_stats)
      end
    end

    context "with stream queue registry detection" do
      it "queues add_stream_queues when pgbus_stream_queues is missing" do
        expect(detector.missing_migrations).to include(:add_stream_queues)
      end

      it "does not queue add_stream_queues when the table already exists" do
        add_table("pgbus_stream_queues")

        expect(detector.missing_migrations).not_to include(:add_stream_queues)
      end
    end

    context "with presence detection" do
      it "queues add_presence when pgbus_presence_members is missing" do
        expect(detector.missing_migrations).to include(:add_presence)
      end

      it "does not queue add_presence when the table already exists" do
        add_table("pgbus_presence_members")

        expect(detector.missing_migrations).not_to include(:add_presence)
      end
    end

    context "with queue states detection" do
      it "queues add_queue_states when pgbus_queue_states is missing" do
        expect(detector.missing_migrations).to include(:add_queue_states)
      end
    end

    context "with processed event completion detection" do
      it "queues add_processed_event_completion when the completed_at column is missing" do
        stub_columns("pgbus_processed_events", %w[id event_id handler_class processed_at])

        expect(detector.missing_migrations).to include(:add_processed_event_completion)
      end

      it "does not queue it when completed_at is already present" do
        stub_columns("pgbus_processed_events", %w[id event_id handler_class processed_at completed_at])

        expect(detector.missing_migrations).not_to include(:add_processed_event_completion)
      end

      it "does not queue it when the table itself is missing" do
        remove_table("pgbus_processed_events")

        expect(detector.missing_migrations).not_to include(:add_processed_event_completion)
      end
    end

    context "with batch executions detection" do
      it "queues add_batch_executions when the executions table is missing" do
        remove_table("pgbus_batch_executions")

        expect(detector.missing_migrations).to include(:add_batch_executions)
      end

      it "does not queue it when the table and renamed columns are present" do
        stub_columns("pgbus_batches", %w[id batch_id on_failure_class failed_jobs completed_jobs total_jobs status])

        expect(detector.missing_migrations).not_to include(:add_batch_executions)
      end

      it "queues add_batch_executions when the table exists but pre-rename columns remain" do
        stub_columns("pgbus_batches", %w[id batch_id on_discard_class discarded_jobs failed_jobs status])

        expect(detector.missing_migrations).to include(:add_batch_executions)
      end
    end

    context "with outbox detection" do
      it "queues add_outbox when pgbus_outbox_entries is missing" do
        expect(detector.missing_migrations).to include(:add_outbox)
      end
    end

    context "with recurring detection" do
      it "queues add_recurring when both recurring tables are missing" do
        expect(detector.missing_migrations).to include(:add_recurring)
      end

      it "queues add_recurring when only one of the two recurring tables is missing" do
        add_table("pgbus_recurring_tasks")
        # pgbus_recurring_executions still missing

        expect(detector.missing_migrations).to include(:add_recurring)
      end

      it "does not queue add_recurring when both recurring tables exist" do
        add_table("pgbus_recurring_tasks")
        add_table("pgbus_recurring_executions")

        expect(detector.missing_migrations).not_to include(:add_recurring)
      end
    end

    context "with failed events index detection" do
      it "queues add_failed_events_index when the table exists but the unique index does not" do
        stub_indexes("pgbus_failed_events", %w[idx_pgbus_failed_events_queue idx_pgbus_failed_events_time])

        expect(detector.missing_migrations).to include(:add_failed_events_index)
      end

      it "does not queue it when the unique index already exists" do
        stub_indexes("pgbus_failed_events", %w[idx_pgbus_failed_events_queue_msg])

        expect(detector.missing_migrations).not_to include(:add_failed_events_index)
      end

      it "does not queue it when the table itself does not exist (fresh install handles it)" do
        remove_table("pgbus_failed_events")

        expect(detector.missing_migrations).not_to include(:add_failed_events_index)
      end
    end

    context "with autovacuum detection" do
      before do
        allow(connection).to receive(:select_value)
          .with("SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pgmq'")
          .and_return(1)
      end

      it "queues tune_autovacuum when pgmq exists but autovacuum is not tuned" do
        allow(connection).to receive(:select_value)
          .with("SELECT queue_name FROM pgmq.meta ORDER BY queue_name LIMIT 1")
          .and_return("pgbus_default")

        allow(connection).to receive(:select_value)
          .with(/reloptions.*autovacuum_vacuum_scale_factor/)
          .and_return(false)

        expect(detector.missing_migrations).to include(:tune_autovacuum)
      end

      it "does not queue tune_autovacuum when already tuned" do
        allow(connection).to receive(:select_value)
          .with("SELECT queue_name FROM pgmq.meta ORDER BY queue_name LIMIT 1")
          .and_return("pgbus_default")

        allow(connection).to receive(:select_value)
          .with(/reloptions.*autovacuum_vacuum_scale_factor/)
          .and_return("t")

        expect(detector.missing_migrations).not_to include(:tune_autovacuum)
      end

      it "does not queue tune_autovacuum when pgmq schema is absent" do
        allow(connection).to receive(:select_value)
          .with("SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pgmq'")
          .and_return(nil)

        expect(detector.missing_migrations).not_to include(:tune_autovacuum)
      end

      it "does not queue tune_autovacuum when no queues exist" do
        allow(connection).to receive(:select_value)
          .with("SELECT queue_name FROM pgmq.meta ORDER BY queue_name LIMIT 1")
          .and_return(nil)

        expect(detector.missing_migrations).not_to include(:tune_autovacuum)
      end
    end

    context "with fillfactor detection" do
      before do
        allow(connection).to receive(:select_value)
          .with("SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pgmq'")
          .and_return(1)
      end

      it "queues tune_fillfactor when pgmq exists but fillfactor is not tuned" do
        allow(connection).to receive(:select_value)
          .with("SELECT queue_name FROM pgmq.meta ORDER BY queue_name LIMIT 1")
          .and_return("pgbus_default")

        allow(connection).to receive(:select_value)
          .with(/reloptions.*fillfactor/)
          .and_return(false)

        expect(detector.missing_migrations).to include(:tune_fillfactor)
      end

      it "does not queue tune_fillfactor when already tuned" do
        allow(connection).to receive(:select_value)
          .with("SELECT queue_name FROM pgmq.meta ORDER BY queue_name LIMIT 1")
          .and_return("pgbus_default")

        allow(connection).to receive(:select_value)
          .with(/reloptions.*fillfactor/)
          .and_return("t")

        expect(detector.missing_migrations).not_to include(:tune_fillfactor)
      end

      it "does not queue tune_fillfactor when pgmq schema is absent" do
        allow(connection).to receive(:select_value)
          .with("SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pgmq'")
          .and_return(nil)

        expect(detector.missing_migrations).not_to include(:tune_fillfactor)
      end

      it "does not queue tune_fillfactor when no queues exist" do
        allow(connection).to receive(:select_value)
          .with("SELECT queue_name FROM pgmq.meta ORDER BY queue_name LIMIT 1")
          .and_return(nil)

        expect(detector.missing_migrations).not_to include(:tune_fillfactor)
      end
    end

    context "with a realistic partially-upgraded database snapshot" do
      # Given a realistic "partially-upgraded" database (core install
      # migration ran, job stats base migration ran, but neither latency
      # nor queue_index were applied), we expect a specific ordered list
      # of upgrade migrations so operators see a predictable plan.
      before do
        stub_tables(%w[
                      pgbus_processed_events
                      pgbus_processes
                      pgbus_failed_events
                      pgbus_semaphores
                      pgbus_blocked_executions
                      pgbus_batches
                      pgbus_uniqueness_keys
                      pgbus_job_stats
                      pgbus_recurring_tasks
                      pgbus_recurring_executions
                    ])
        stub_columns("pgbus_job_stats", %w[id job_class queue_name status duration_ms created_at])
        stub_indexes("pgbus_job_stats", %w[idx_pgbus_job_stats_time])
        stub_indexes("pgbus_failed_events", %w[idx_pgbus_failed_events_queue_msg])

        # Autovacuum: pgmq exists but not yet tuned
        allow(connection).to receive(:select_value)
          .with("SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pgmq'")
          .and_return(1)
        allow(connection).to receive(:select_value)
          .with("SELECT queue_name FROM pgmq.meta ORDER BY queue_name LIMIT 1")
          .and_return("pgbus_default")
        allow(connection).to receive(:select_value)
          .with(/reloptions.*autovacuum_vacuum_scale_factor/)
          .and_return(false)
        # Fillfactor: not yet tuned either
        allow(connection).to receive(:select_value)
          .with(/reloptions.*fillfactor/)
          .and_return(false)
      end

      it "queues only the specific add-on migrations that are missing" do
        expect(detector.missing_migrations).to contain_exactly(
          :add_job_stats_latency,
          :add_job_stats_queue_index,
          :add_stream_stats,
          :add_stream_queues,
          :add_presence,
          :add_queue_states,
          :add_outbox,
          :add_processed_event_completion,
          :add_batch_executions,
          :tune_autovacuum,
          :tune_fillfactor
        )
      end
    end

    context "when connection.table_exists? raises" do
      # Defensive: the detector should not crash the generator if the
      # AR connection is in a weird state. A raising probe is treated
      # as "table does not exist".
      before do
        allow(connection).to receive(:table_exists?).and_raise(StandardError, "db connection reset")
      end

      it "treats all probes as missing and returns FRESH_INSTALL" do
        expect(detector.missing_migrations).to eq([described_class::FRESH_INSTALL])
      end
    end
  end

  describe "GENERATOR_MAP" do
    it "has an entry for every symbol returned by missing_migrations (excluding FRESH_INSTALL)" do
      # Sanity check: if a new migration key is added to the detector
      # without a GENERATOR_MAP entry, the update generator would
      # silently skip it. Verify the mapping is complete by scanning
      # every code path through the detector.
      keys = %i[
        uniqueness_keys add_job_stats add_job_stats_latency
        add_job_stats_queue_index add_stream_stats add_presence
        add_queue_states add_outbox add_recurring add_failed_events_index
        add_processed_event_completion add_batch_executions add_stream_queues
        tune_autovacuum tune_fillfactor
      ]

      keys.each do |key|
        expect(described_class::GENERATOR_MAP).to have_key(key),
                                                  "missing GENERATOR_MAP entry for #{key}"
        expect(described_class::DESCRIPTIONS).to have_key(key),
                                                 "missing DESCRIPTIONS entry for #{key}"
      end
    end
  end
end
