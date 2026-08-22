# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Client do
  # Stub pgmq-ruby so it never loads the real gem
  subject(:client) do
    allow(PGMQ::Client).to receive(:new).and_return(mock_pgmq)
    # Pre-mark PGMQ schema as ensured for most tests via the constructor seam.
    # Schema installation tests build their own client with schema_ensured: false.
    c = described_class.new(config, schema_ensured: true)
    # Stub autovacuum tuning — runs raw SQL which needs a real PG connection.
    allow(c).to receive(:tune_autovacuum)
    # Stub notify trigger check — runs raw SQL which needs a real PG connection.
    # Tests that exercise this method explicitly override this stub.
    allow(c).to receive(:notify_trigger_current?).and_return(false)
    c
  end

  before do
    # Client#initialize loads pgmq via the class method load_pgmq_gem!; stub it
    # so the fake PGMQ::Client below is not overwritten by the real gem. A
    # per-example class-method stub, torn down cleanly — unlike stubbing the
    # global Kernel#require, which would permanently block the real gem load.
    allow(described_class).to receive(:load_pgmq_gem!)
    stub_const("PGMQ::Client", Class.new do
      def initialize(*args, **kwargs); end
    end)
  end

  let(:config) do
    Pgbus::Configuration.new.tap do |c|
      c.database_url = "postgres://localhost/pgbus_test"
      c.queue_prefix = "pgbus_test"
    end
  end
  let(:mock_pgmq) { build_mock_pgmq }

  describe "#ensure_pgmq_schema (via ensure_queue)" do
    # Override the shared subject to leave the schema UN-ensured so ensure_queue
    # actually runs the install path (the default subject pre-marks it ensured).
    subject(:client) do
      allow(PGMQ::Client).to receive(:new).and_return(mock_pgmq)
      c = described_class.new(config, schema_ensured: false)
      allow(c).to receive(:tune_autovacuum)
      allow(c).to receive(:notify_trigger_current?).and_return(false)
      c
    end

    let(:raw_conn) { double("raw_conn") }

    before do
      allow(client).to receive(:with_raw_connection).and_yield(raw_conn)
      # Transaction + advisory-lock framing around check+install (issue #397).
      # Allowed here so every example in this describe tolerates the framing;
      # the framing-specific examples assert on these explicitly.
      allow(raw_conn).to receive(:exec).with("BEGIN")
      allow(raw_conn).to receive(:exec).with(/pg_advisory_xact_lock/)
      allow(raw_conn).to receive(:exec).with("COMMIT")
      allow(raw_conn).to receive(:exec).with("ROLLBACK")
    end

    it "installs via embedded SQL when pgmq.meta missing and no extension (auto mode)" do
      allow(raw_conn).to receive(:exec).with(/pg_tables.*pgmq.*meta/).and_return(double(ntuples: 0))
      allow(raw_conn).to receive(:exec).with(/pg_available_extensions/).and_return(double(ntuples: 0))
      allow(raw_conn).to receive(:exec).with(Pgbus::PgmqSchema.install_sql).and_return(nil)

      client.ensure_queue("jobs")

      expect(raw_conn).to have_received(:exec).with(Pgbus::PgmqSchema.install_sql)
    end

    it "installs via extension when available in auto mode" do
      allow(raw_conn).to receive(:exec).with(/pg_tables.*pgmq.*meta/).and_return(double(ntuples: 0))
      allow(raw_conn).to receive(:exec).with(/pg_available_extensions/).and_return(double(ntuples: 1))
      allow(raw_conn).to receive(:exec).with("CREATE EXTENSION IF NOT EXISTS pgmq").and_return(nil)

      client.ensure_queue("jobs")

      expect(raw_conn).to have_received(:exec).with("CREATE EXTENSION IF NOT EXISTS pgmq")
    end

    it "respects :extension schema mode" do
      config.pgmq_schema_mode = :extension
      allow(raw_conn).to receive(:exec).with(/pg_tables.*pgmq.*meta/).and_return(double(ntuples: 0))
      allow(raw_conn).to receive(:exec).with("CREATE EXTENSION IF NOT EXISTS pgmq").and_return(nil)

      client.ensure_queue("jobs")

      expect(raw_conn).to have_received(:exec).with("CREATE EXTENSION IF NOT EXISTS pgmq")
      expect(raw_conn).not_to have_received(:exec).with(Pgbus::PgmqSchema.install_sql)
      config.pgmq_schema_mode = :auto
    end

    it "skips installation when pgmq.meta already exists" do
      allow(raw_conn).to receive(:exec).with(/pg_tables.*pgmq.*meta/).and_return(double(ntuples: 1))

      client.ensure_queue("jobs")

      expect(raw_conn).not_to have_received(:exec).with(Pgbus::PgmqSchema.install_sql)
    end

    it "wraps install failures as SchemaNotReady" do
      allow(raw_conn).to receive(:exec).with(/pg_tables.*pgmq.*meta/).and_return(double(ntuples: 0))
      allow(raw_conn).to receive(:exec).with(/pg_available_extensions/).and_return(double(ntuples: 0))
      allow(raw_conn).to receive(:exec).with(Pgbus::PgmqSchema.install_sql)
                     .and_raise(StandardError, "permission denied for schema pgmq")

      expect { client.ensure_queue("jobs") }.to raise_error(
        Pgbus::SchemaNotReady, /PGMQ schema installation failed/
      )
    end

    it "only checks once per client instance" do
      allow(raw_conn).to receive(:exec).with(/pg_tables.*pgmq.*meta/).and_return(double(ntuples: 1))

      client.ensure_queue("jobs")
      client.ensure_queue("events")

      expect(raw_conn).to have_received(:exec).with(/pg_tables.*pgmq.*meta/).once
    end

    it "wraps check+install in a transaction holding the install advisory lock (#397)" do
      allow(raw_conn).to receive(:exec).with(/pg_tables.*pgmq.*meta/).and_return(double(ntuples: 0))
      allow(raw_conn).to receive(:exec).with(/pg_available_extensions/).and_return(double(ntuples: 0))
      allow(raw_conn).to receive(:exec).with(Pgbus::PgmqSchema.install_sql).and_return(nil)

      client.ensure_queue("jobs")

      expect(raw_conn).to have_received(:exec).with("BEGIN").ordered
      expect(raw_conn).to have_received(:exec)
        .with("SELECT pg_advisory_xact_lock(#{Pgbus::Client::PGMQ_INSTALL_LOCK_KEY})").ordered
      expect(raw_conn).to have_received(:exec).with(/pg_tables.*pgmq.*meta/).ordered
      expect(raw_conn).to have_received(:exec).with(Pgbus::PgmqSchema.install_sql).ordered
      expect(raw_conn).to have_received(:exec).with("COMMIT").ordered
    end

    context "when the connection is already inside a caller's transaction (#398 review P1)" do
      before do
        require "pg"
        allow(raw_conn).to receive(:transaction_status).and_return(PG::PQTRANS_INTRANS)
        allow(raw_conn).to receive(:exec).with(/SAVEPOINT/)
      end

      it "frames check+install in a savepoint and never issues BEGIN/COMMIT" do
        allow(raw_conn).to receive(:exec).with(/pg_tables.*pgmq.*meta/).and_return(double(ntuples: 0))
        allow(raw_conn).to receive(:exec).with(/pg_available_extensions/).and_return(double(ntuples: 0))
        allow(raw_conn).to receive(:exec).with(Pgbus::PgmqSchema.install_sql).and_return(nil)

        client.ensure_queue("jobs")

        expect(raw_conn).to have_received(:exec).with("SAVEPOINT pgbus_pgmq_install").ordered
        expect(raw_conn).to have_received(:exec)
          .with("SELECT pg_advisory_xact_lock(#{Pgbus::Client::PGMQ_INSTALL_LOCK_KEY})").ordered
        expect(raw_conn).to have_received(:exec).with(Pgbus::PgmqSchema.install_sql).ordered
        expect(raw_conn).to have_received(:exec).with("RELEASE SAVEPOINT pgbus_pgmq_install").ordered
        expect(raw_conn).not_to have_received(:exec).with("BEGIN")
        expect(raw_conn).not_to have_received(:exec).with("COMMIT")
      end

      it "does not cache schema_ensured — the install is only durable once the caller commits" do
        allow(raw_conn).to receive(:exec).with(/pg_tables.*pgmq.*meta/).and_return(double(ntuples: 0))
        allow(raw_conn).to receive(:exec).with(/pg_available_extensions/).and_return(double(ntuples: 0))
        allow(raw_conn).to receive(:exec).with(Pgbus::PgmqSchema.install_sql).and_return(nil)

        client.ensure_queue("jobs")
        client.ensure_queue("events")

        expect(raw_conn).to have_received(:exec).with(/pg_tables.*pgmq.*meta/).twice
      end

      it "rolls back to the savepoint — never the whole transaction — on a duplicate install" do
        check = double("check_result")
        allow(check).to receive(:ntuples).and_return(0, 1)
        allow(raw_conn).to receive(:exec).with(/pg_tables.*pgmq.*meta/).and_return(check)
        allow(raw_conn).to receive(:exec).with(/pg_available_extensions/).and_return(double(ntuples: 0))
        allow(raw_conn).to receive(:exec).with(Pgbus::PgmqSchema.install_sql).and_raise(
          PG::UniqueViolation.new("ERROR: duplicate key value")
        )

        expect { client.ensure_queue("jobs") }.not_to raise_error

        expect(raw_conn).to have_received(:exec).with("ROLLBACK TO SAVEPOINT pgbus_pgmq_install")
        expect(raw_conn).not_to have_received(:exec).with("ROLLBACK")
        expect(raw_conn).not_to have_received(:exec).with("COMMIT")
      end
    end

    it "holds the connection mutex while installing on a shared Proc connection" do
      require "pg"
      allow(PGMQ::Client).to receive(:new).and_return(mock_pgmq)
      allow(config).to receive(:connection_options).and_return(-> { raw_conn })
      shared_client = described_class.new(config, schema_ensured: false)
      allow(shared_client).to receive(:tune_autovacuum)
      allow(shared_client).to receive(:notify_trigger_current?).and_return(false)
      allow(shared_client).to receive(:with_raw_connection).and_yield(raw_conn)
      owned_during_check = nil
      allow(raw_conn).to receive(:exec).with(/pg_tables.*pgmq.*meta/) do
        owned_during_check = shared_client.instance_variable_get(:@pgmq_mutex).owned?
        double(ntuples: 1)
      end

      shared_client.ensure_queue("jobs")

      expect(owned_during_check).to be(true)
    end

    it "owns the transaction when the connection reports an idle status" do
      require "pg"
      allow(raw_conn).to receive(:transaction_status).and_return(PG::PQTRANS_IDLE)
      allow(raw_conn).to receive(:exec).with(/pg_tables.*pgmq.*meta/).and_return(double(ntuples: 1))

      client.ensure_queue("jobs")

      expect(raw_conn).to have_received(:exec).with("BEGIN")
      expect(raw_conn).to have_received(:exec).with("COMMIT")
    end

    context "when another process wins the install race (#397)" do
      before { require "pg" }

      it "treats a duplicate-object install failure as installed by the winner" do
        check = double("check_result")
        allow(check).to receive(:ntuples).and_return(0, 1)
        allow(raw_conn).to receive(:exec).with(/pg_tables.*pgmq.*meta/).and_return(check)
        allow(raw_conn).to receive(:exec).with(/pg_available_extensions/).and_return(double(ntuples: 0))
        allow(raw_conn).to receive(:exec).with(Pgbus::PgmqSchema.install_sql).and_raise(
          PG::UniqueViolation.new('ERROR: duplicate key value violates unique constraint "pg_namespace_nspname_index"')
        )

        expect { client.ensure_queue("jobs") }.not_to raise_error

        expect(raw_conn).to have_received(:exec).with("ROLLBACK")
        expect(raw_conn).to have_received(:exec).with(/pg_tables.*pgmq.*meta/).twice
      end

      it "still wraps as SchemaNotReady when the re-check finds no schema" do
        check = double("check_result")
        allow(check).to receive(:ntuples).and_return(0, 0)
        allow(raw_conn).to receive(:exec).with(/pg_tables.*pgmq.*meta/).and_return(check)
        allow(raw_conn).to receive(:exec).with(/pg_available_extensions/).and_return(double(ntuples: 0))
        allow(raw_conn).to receive(:exec).with(Pgbus::PgmqSchema.install_sql).and_raise(
          PG::UniqueViolation.new("ERROR: duplicate key value")
        )

        expect { client.ensure_queue("jobs") }.to raise_error(
          Pgbus::SchemaNotReady, /PGMQ schema installation failed/
        )
      end
    end

    # Helpers for the process-wide serialization example: connection doubles
    # pre-stubbed with the transaction/advisory-lock framing.
    def framed_conn(name)
      conn = double(name)
      allow(conn).to receive(:exec).with("BEGIN")
      allow(conn).to receive(:exec).with(/pg_advisory_xact_lock/)
      allow(conn).to receive(:exec).with("COMMIT")
      conn
    end

    # Blocks inside the install until `release` is signalled, reporting entry
    # on `started` — lets the example hold client A mid-install deterministically.
    def install_blocking_conn(started, release)
      conn = framed_conn("conn_a")
      allow(conn).to receive(:exec).with(/pg_tables.*pgmq.*meta/).and_return(double(ntuples: 0))
      allow(conn).to receive(:exec).with(/pg_available_extensions/).and_return(double(ntuples: 0))
      allow(conn).to receive(:exec).with(Pgbus::PgmqSchema.install_sql) do
        started << true
        release.pop
      end
      conn
    end

    def unensured_client(conn)
      c = described_class.new(config, schema_ensured: false)
      allow(c).to receive(:tune_autovacuum)
      allow(c).to receive(:notify_trigger_current?).and_return(false)
      allow(c).to receive(:with_raw_connection).and_yield(conn)
      c
    end

    # Bounded deterministic wait: true once the thread is blocked or
    # terminated, false if it never settles (#398/#399 review — a fixed sleep
    # can false-pass, and silent fall-through re-creates a fixed sleep).
    def settled?(thread)
      5_000.times do
        return true if [false, nil, "sleep"].include?(thread.status)

        sleep 0.001
      end
      false
    end

    it "serializes installs process-wide across client instances (#397)" do
      install_started = Queue.new
      release_install = Queue.new
      conn_a = install_blocking_conn(install_started, release_install)
      conn_b = framed_conn("conn_b")
      allow(conn_b).to receive(:exec).with(/pg_tables.*pgmq.*meta/).and_return(double(ntuples: 1))
      client_a = client
      allow(client_a).to receive(:with_raw_connection).and_yield(conn_a)
      client_b = unensured_client(conn_b)

      thread_a = Thread.new { client_a.ensure_queue("jobs") }
      install_started.pop
      thread_b = Thread.new { client_b.ensure_queue("jobs") }
      # Deterministic (no fixed sleep, #398 review P3): with A parked inside its
      # install, B is the only runnable thread — wait until it either blocks
      # (serialized: parked on the install mutex) or terminates (unserialized:
      # it ran its whole path). At that settled point, zero execs on B's
      # connection is exactly the serialization property; an unserialized B has
      # terminated WITH execs recorded and fails the assertion every time.
      expect(settled?(thread_b)).to be(true), "thread B never settled (blocked or terminated) within the wait budget"
      expect(conn_b).not_to have_received(:exec)

      release_install << true
      [thread_a, thread_b].each { |t| t.join(5) }
      expect(conn_b).to have_received(:exec).with(/pg_tables.*pgmq.*meta/).once
      expect(conn_b).not_to have_received(:exec).with(Pgbus::PgmqSchema.install_sql)
    end
  end

  describe "#ensure_queue when queue DDL rides a caller's transaction (#399 review)" do
    subject(:client) do
      allow(config).to receive(:connection_options).and_return(-> { raw_conn })
      allow(PGMQ::Client).to receive(:new).and_return(mock_pgmq)
      c = described_class.new(config, schema_ensured: true)
      allow(c).to receive(:tune_autovacuum)
      allow(c).to receive(:notify_trigger_current?).and_return(false)
      c
    end

    let(:raw_conn) { double("raw_conn") }

    before { require "pg" }

    it "probes the shared connection only while holding the connection mutex" do
      owned_during_probe = nil
      allow(raw_conn).to receive(:transaction_status) do
        owned_during_probe = client.instance_variable_get(:@pgmq_mutex).owned?
        PG::PQTRANS_IDLE
      end

      client.ensure_queue("jobs")

      expect(owned_during_probe).to be(true)
    end

    it "creates the queue but does not cache it — the DDL is only durable once the caller commits" do
      allow(raw_conn).to receive(:transaction_status).and_return(PG::PQTRANS_INTRANS)

      client.ensure_queue("jobs")
      client.ensure_queue("jobs")

      expect(mock_pgmq).to have_received(:create).with("pgbus_test_jobs").twice
    end

    it "resumes caching once the shared connection is idle again" do
      allow(raw_conn).to receive(:transaction_status).and_return(
        PG::PQTRANS_INTRANS, PG::PQTRANS_IDLE, PG::PQTRANS_IDLE
      )

      client.ensure_queue("jobs") # inside caller txn — created, not cached
      client.ensure_queue("jobs") # idle — created and cached
      client.ensure_queue("jobs") # cache hit

      expect(mock_pgmq).to have_received(:create).with("pgbus_test_jobs").twice
    end
  end

  describe "#ensure_queue" do
    it "tunes autovacuum when creating a queue" do
      client.ensure_queue("jobs")

      expect(client).to have_received(:tune_autovacuum).with("pgbus_test_jobs")
    end

    it "creates the queue with the prefixed name" do
      client.ensure_queue("jobs")

      expect(mock_pgmq).to have_received(:create).with("pgbus_test_jobs")
    end

    it "enables LISTEN/NOTIFY when listen_notify is true" do
      config.listen_notify = true
      allow(client).to receive(:notify_trigger_current?).and_return(false)
      client.ensure_queue("jobs")

      expect(mock_pgmq).to have_received(:enable_notify_insert).with("pgbus_test_jobs", throttle_interval_ms: Pgbus::Client::NOTIFY_THROTTLE_MS)
    end

    it "skips LISTEN/NOTIFY when listen_notify is false" do
      config.listen_notify = false
      client.ensure_queue("jobs")

      expect(mock_pgmq).not_to have_received(:enable_notify_insert)
    end

    it "skips enable_notify_insert when trigger already exists with correct throttle" do
      config.listen_notify = true
      allow(client).to receive(:notify_trigger_current?).with("pgbus_test_jobs", Pgbus::Client::NOTIFY_THROTTLE_MS).and_return(true)
      client.ensure_queue("jobs")

      expect(mock_pgmq).not_to have_received(:enable_notify_insert)
    end

    it "calls enable_notify_insert when trigger exists but throttle differs" do
      config.listen_notify = true
      allow(client).to receive(:notify_trigger_current?).with("pgbus_test_jobs", Pgbus::Client::NOTIFY_THROTTLE_MS).and_return(false)
      client.ensure_queue("jobs")

      expect(mock_pgmq).to have_received(:enable_notify_insert).with("pgbus_test_jobs", throttle_interval_ms: Pgbus::Client::NOTIFY_THROTTLE_MS)
    end

    describe "duplicate NOTIFY trigger race (issue #403)" do
      # Two processes with cold memos both pass the notify_trigger_current?
      # check-then-act window; the loser's CREATE CONSTRAINT TRIGGER fails
      # with PG::DuplicateObject, wrapped by pgmq-ruby as ConnectionError.
      let(:duplicate_error) do
        real_pgmq_connection_error
        PGMQ::Errors::ConnectionError.new(
          'Database connection error: ERROR:  trigger "trigger_notify_queue_insert_listeners" ' \
          'for relation "q_pgbus_test_jobs" already exists'
        )
      end

      before { config.listen_notify = true }

      it "treats the loser's duplicate-trigger error as success when the winner installed the same throttle" do
        allow(client).to receive(:notify_trigger_current?).and_return(false, true)
        allow(mock_pgmq).to receive(:enable_notify_insert).and_raise(duplicate_error)

        expect { client.ensure_queue("jobs") }.not_to raise_error
        expect(mock_pgmq).to have_received(:enable_notify_insert).once
      end

      it "retries enable_notify_insert once when the winner installed a different throttle" do
        allow(client).to receive(:notify_trigger_current?).and_return(false, false)
        attempts = 0
        allow(mock_pgmq).to receive(:enable_notify_insert) do
          attempts += 1
          raise duplicate_error if attempts == 1
        end

        expect { client.ensure_queue("jobs") }.not_to raise_error
        expect(mock_pgmq).to have_received(:enable_notify_insert).twice
      end

      it "propagates a duplicate error when the retry also loses the race" do
        allow(client).to receive(:notify_trigger_current?).and_return(false, false)
        allow(mock_pgmq).to receive(:enable_notify_insert).and_raise(duplicate_error)

        expect { client.ensure_queue("jobs") }.to raise_error(PGMQ::Errors::ConnectionError)
        expect(mock_pgmq).to have_received(:enable_notify_insert).twice
      end

      it "recognizes the duplicate via the PG::DuplicateObject cause when the message is localized" do
        real_pgmq_connection_error
        stub_const("PG::DuplicateObject", Class.new(StandardError))
        localized = begin
          raise PG::DuplicateObject, "localized message"
        rescue PG::DuplicateObject
          begin
            raise PGMQ::Errors::ConnectionError,
                  'Database connection error: FEHLER: Trigger "trigger_notify_queue_insert_listeners" existiert bereits'
          rescue PGMQ::Errors::ConnectionError => e
            e
          end
        end

        allow(client).to receive(:notify_trigger_current?).and_return(false, true)
        allow(mock_pgmq).to receive(:enable_notify_insert).and_raise(localized)

        expect { client.ensure_queue("jobs") }.not_to raise_error
      end

      it "propagates a localized duplicate message when the PG::DuplicateObject cause was dropped" do
        # Residual gap, pinned as intentional: with the cause gone AND the
        # message localized, nothing proves this is a duplicate — an
        # unrecognizable error must propagate, never be swallowed. The
        # English-text fallback is defense-in-depth, not a promise; the real
        # pgmq-ruby path always sets the cause (raise inside rescue PG::Error).
        real_pgmq_connection_error
        localized = PGMQ::Errors::ConnectionError.new(
          'Database connection error: FEHLER: Trigger "trigger_notify_queue_insert_listeners" existiert bereits'
        )
        allow(client).to receive(:notify_trigger_current?).and_return(false)
        allow(mock_pgmq).to receive(:enable_notify_insert).and_raise(localized)

        expect { client.ensure_queue("jobs") }.to raise_error(PGMQ::Errors::ConnectionError, /existiert bereits/)
      end

      it "propagates duplicate errors about other objects" do
        real_pgmq_connection_error
        other = PGMQ::Errors::ConnectionError.new(
          'Database connection error: ERROR:  constraint "something_else" already exists'
        )
        allow(client).to receive(:notify_trigger_current?).and_return(false)
        allow(mock_pgmq).to receive(:enable_notify_insert).and_raise(other)

        expect { client.ensure_queue("jobs") }.to raise_error(PGMQ::Errors::ConnectionError, /something_else/)
      end

      it "propagates genuine connection errors unchanged" do
        real_pgmq_connection_error
        refused = PGMQ::Errors::ConnectionError.new("Database connection error: connection refused")
        allow(client).to receive(:notify_trigger_current?).and_return(false)
        allow(mock_pgmq).to receive(:enable_notify_insert).and_raise(refused)

        expect { client.ensure_queue("jobs") }.to raise_error(PGMQ::Errors::ConnectionError, /connection refused/)
      end
    end

    describe "duplicate relation race on queue creation (issue #404)" do
      # CREATE TABLE IF NOT EXISTS is not race-safe: two backends creating a
      # not-yet-existing queue both pass the existence check, and the loser
      # raises unique_violation on pg_class_relname_nsp_index — wrapped by
      # pgmq-ruby as ConnectionError with the PG error as cause.
      let(:duplicate_cause_class) { stub_const("PG::UniqueViolation", Class.new(StandardError)) }
      let(:duplicate_error) do
        real_pgmq_connection_error
        begin
          raise duplicate_cause_class, "catalog race"
        rescue duplicate_cause_class
          begin
            raise PGMQ::Errors::ConnectionError,
                  'Database connection error: ERROR:  duplicate key value violates unique constraint "pg_class_relname_nsp_index"'
          rescue PGMQ::Errors::ConnectionError => e
            e
          end
        end
      end

      before { allow(client).to receive(:queue_registered?).and_return(true) }

      it "treats the loser's duplicate as success when the winner's queue is registered" do
        allow(mock_pgmq).to receive(:create).and_raise(duplicate_error)

        expect { client.ensure_queue("jobs") }.not_to raise_error
        expect(mock_pgmq).to have_received(:create).once
      end

      it "skips autovacuum tuning on the recheck-success path — the winner already tuned" do
        allow(mock_pgmq).to receive(:create).and_raise(duplicate_error)

        client.ensure_queue("jobs")

        expect(client).not_to have_received(:tune_autovacuum)
      end

      it "retries pgmq.create once when the recheck cannot confirm the queue" do
        allow(client).to receive(:queue_registered?).and_return(false)
        attempts = 0
        allow(mock_pgmq).to receive(:create) do
          attempts += 1
          raise duplicate_error if attempts == 1
        end

        expect { client.ensure_queue("jobs") }.not_to raise_error
        expect(mock_pgmq).to have_received(:create).twice
        expect(client).to have_received(:tune_autovacuum).with("pgbus_test_jobs")
      end

      it "propagates when the retry also fails, carrying the original duplicate as cause" do
        allow(client).to receive(:queue_registered?).and_return(false)
        allow(mock_pgmq).to receive(:create).and_raise(duplicate_error)

        expect { client.ensure_queue("jobs") }.to raise_error(PGMQ::Errors::ConnectionError)
        expect(mock_pgmq).to have_received(:create).twice
      end

      it "recognizes an unwrapped duplicate error class directly" do
        stub_const("PG::DuplicateTable", Class.new(StandardError))
        allow(mock_pgmq).to receive(:create).and_raise(PG::DuplicateTable, "relation already exists")

        expect { client.ensure_queue("jobs") }.not_to raise_error
      end

      it "propagates non-duplicate errors without rechecking" do
        real_pgmq_connection_error
        refused = PGMQ::Errors::ConnectionError.new("Database connection error: connection refused")
        allow(mock_pgmq).to receive(:create).and_raise(refused)

        expect { client.ensure_queue("jobs") }.to raise_error(PGMQ::Errors::ConnectionError, /connection refused/)
        expect(client).not_to have_received(:queue_registered?)
      end

      context "with group_mode FIFO index creation" do
        before { config.group_mode = :fifo }

        it "treats the loser's duplicate index as success" do
          allow(mock_pgmq).to receive(:create_fifo_index).and_raise(duplicate_error)

          expect { client.ensure_queue("jobs") }.not_to raise_error
        end

        it "propagates non-duplicate FIFO index errors" do
          real_pgmq_connection_error
          refused = PGMQ::Errors::ConnectionError.new("Database connection error: connection refused")
          allow(mock_pgmq).to receive(:create_fifo_index).and_raise(refused)

          expect { client.ensure_queue("jobs") }.to raise_error(PGMQ::Errors::ConnectionError, /connection refused/)
        end
      end
    end

    it "is idempotent — only creates the queue once" do
      client.ensure_queue("jobs")
      client.ensure_queue("jobs")

      expect(mock_pgmq).to have_received(:create).with("pgbus_test_jobs").once
    end

    it "creates different queues independently" do
      client.ensure_queue("jobs")
      client.ensure_queue("events")

      expect(mock_pgmq).to have_received(:create).with("pgbus_test_jobs").once
      expect(mock_pgmq).to have_received(:create).with("pgbus_test_events").once
    end

    it "propagates PGMQ connection errors when queue creation fails" do
      real_pgmq_connection_error

      allow(mock_pgmq).to receive(:create).and_raise(
        PGMQ::Errors::ConnectionError.new("Database connection error: connection refused")
      )

      expect { client.ensure_queue("jobs") }.to raise_error(PGMQ::Errors::ConnectionError)
    end
  end

  describe "#ensure_dead_letter_queue" do
    it "tunes autovacuum when creating a DLQ" do
      client.ensure_dead_letter_queue("jobs")

      expect(client).to have_received(:tune_autovacuum).with("pgbus_test_jobs_dlq")
    end

    it "creates the DLQ with correct suffix" do
      client.ensure_dead_letter_queue("jobs")

      expect(mock_pgmq).to have_received(:create).with("pgbus_test_jobs_dlq")
    end

    it "is idempotent — only creates the DLQ once" do
      client.ensure_dead_letter_queue("jobs")
      client.ensure_dead_letter_queue("jobs")

      expect(mock_pgmq).to have_received(:create).with("pgbus_test_jobs_dlq").once
    end
  end

  describe "#send_message" do
    it "ensures the queue exists before sending" do
      client.send_message("default", { "type" => "test" })

      expect(mock_pgmq).to have_received(:create).with("pgbus_test_default")
    end

    it "produces a JSON-serialized message to the prefixed queue" do
      client.send_message("default", { "key" => "value" })

      expect(mock_pgmq).to have_received(:produce).with(
        "pgbus_test_default",
        '{"key":"value"}',
        headers: nil,
        delay: 0
      )
    end

    it "passes headers and delay" do
      client.send_message("default", "payload", headers: { "x" => 1 }, delay: 5)

      expect(mock_pgmq).to have_received(:produce).with(
        "pgbus_test_default",
        "payload",
        headers: '{"x":1}',
        delay: 5
      )
    end

    it "does not double-serialize a string payload" do
      client.send_message("default", '{"already":"json"}')

      expect(mock_pgmq).to have_received(:produce).with(
        "pgbus_test_default",
        '{"already":"json"}',
        headers: nil,
        delay: 0
      )
    end
  end

  describe "#send_batch" do
    it "produces a batch of serialized messages" do
      payloads = [{ "a" => 1 }, { "b" => 2 }]
      client.send_batch("default", payloads)

      expect(mock_pgmq).to have_received(:produce_batch).with(
        "pgbus_test_default",
        ['{"a":1}', '{"b":2}'],
        headers: nil,
        delay: 0
      )
    end

    it "serializes headers when provided" do
      client.send_batch("default", ["p1"], headers: [{ "h" => 1 }])

      expect(mock_pgmq).to have_received(:produce_batch).with(
        "pgbus_test_default",
        ["p1"],
        headers: ['{"h":1}'],
        delay: 0
      )
    end

    it "preserves nil elements in mixed headers arrays" do
      client.send_batch("default", %w[p1 p2], headers: [nil, { "h" => 1 }])

      expect(mock_pgmq).to have_received(:produce_batch).with(
        "pgbus_test_default",
        %w[p1 p2],
        headers: [nil, '{"h":1}'],
        delay: 0
      )
    end
  end

  describe "#read_message" do
    it "reads from the prefixed queue with default visibility timeout" do
      client.read_message("default")

      expect(mock_pgmq).to have_received(:read).with("pgbus_test_default", vt: config.visibility_timeout)
    end

    it "allows overriding the visibility timeout" do
      client.read_message("default", vt: 60)

      expect(mock_pgmq).to have_received(:read).with("pgbus_test_default", vt: 60)
    end
  end

  describe "#read_batch" do
    it "reads a batch from the prefixed queue" do
      client.read_batch("default", qty: 10)

      expect(mock_pgmq).to have_received(:read_batch).with("pgbus_test_default", vt: config.visibility_timeout, qty: 10)
    end
  end

  describe "#read_multi" do
    it "reads from multiple prefixed queues in a single call" do
      client.read_multi(%w[default urgent], qty: 10)

      expect(mock_pgmq).to have_received(:read_multi).with(
        %w[pgbus_test_default pgbus_test_urgent],
        vt: config.visibility_timeout,
        qty: 10,
        limit: nil
      )
    end

    it "allows overriding the visibility timeout" do
      client.read_multi(%w[default], qty: 5, vt: 60)

      expect(mock_pgmq).to have_received(:read_multi).with(
        %w[pgbus_test_default],
        vt: 60,
        qty: 5,
        limit: nil
      )
    end

    it "forwards the limit: argument so callers can cap the total across queues" do
      client.read_multi(%w[default urgent statistics], qty: 5, limit: 5)

      expect(mock_pgmq).to have_received(:read_multi).with(
        %w[pgbus_test_default pgbus_test_urgent pgbus_test_statistics],
        vt: config.visibility_timeout,
        qty: 5,
        limit: 5
      )
    end
  end

  describe "#read_with_poll" do
    it "delegates to pgmq.read_with_poll with correct args" do
      client.read_with_poll("default", qty: 5, max_poll_seconds: 2, poll_interval_ms: 50)

      expect(mock_pgmq).to have_received(:read_with_poll).with(
        "pgbus_test_default",
        vt: config.visibility_timeout,
        qty: 5,
        max_poll_seconds: 2,
        poll_interval_ms: 50
      )
    end
  end

  describe "#delete_message" do
    it "deletes from the prefixed queue by default" do
      client.delete_message("default", 42)

      expect(mock_pgmq).to have_received(:delete).with("pgbus_test_default", 42)
    end

    it "skips prefix when prefixed: false" do
      client.delete_message("raw_queue", 99, prefixed: false)

      expect(mock_pgmq).to have_received(:delete).with("raw_queue", 99)
    end
  end

  describe "#archive_message" do
    it "archives from the prefixed queue by default" do
      client.archive_message("default", 7)

      expect(mock_pgmq).to have_received(:archive).with("pgbus_test_default", 7)
    end

    it "skips prefix when prefixed: false" do
      client.archive_message("pgbus_test_default_p0", 42, prefixed: false)

      expect(mock_pgmq).to have_received(:archive).with("pgbus_test_default_p0", 42)
    end
  end

  describe "#archive_batch" do
    it "archives multiple messages from the prefixed queue" do
      client.archive_batch("default", [1, 2, 3])

      expect(mock_pgmq).to have_received(:archive_batch).with("pgbus_test_default", [1, 2, 3])
    end

    it "skips prefix when prefixed: false" do
      client.archive_batch("raw_queue", [4, 5], prefixed: false)

      expect(mock_pgmq).to have_received(:archive_batch).with("raw_queue", [4, 5])
    end
  end

  describe "#delete_batch" do
    it "deletes multiple messages from the prefixed queue" do
      client.delete_batch("default", [1, 2])

      expect(mock_pgmq).to have_received(:delete_batch).with("pgbus_test_default", [1, 2])
    end

    it "skips prefix when prefixed: false" do
      client.delete_batch("pgbus_test_default_dlq", [4, 5], prefixed: false)

      expect(mock_pgmq).to have_received(:delete_batch).with("pgbus_test_default_dlq", [4, 5])
    end
  end

  describe "#set_visibility_timeout" do
    it "adds prefix by default" do
      client.set_visibility_timeout("default", 5, vt: 120)

      expect(mock_pgmq).to have_received(:set_vt).with("pgbus_test_default", 5, vt: 120)
    end

    it "skips prefix when prefixed: false" do
      client.set_visibility_timeout("raw_queue", 3, vt: 60, prefixed: false)

      expect(mock_pgmq).to have_received(:set_vt).with("raw_queue", 3, vt: 60)
    end
  end

  describe "#transaction" do
    it "delegates to pgmq.transaction" do
      yielded = false
      client.transaction { |_txn| yielded = true }

      expect(yielded).to be true
    end
  end

  describe "#move_to_dead_letter" do
    it "ensures the DLQ exists" do
      message = build_message_double(msg_id: 42, message: '{"data":"test"}', headers: nil)
      client.move_to_dead_letter("default", message)

      expect(mock_pgmq).to have_received(:create).with("pgbus_test_default_dlq")
    end

    it "produces to DLQ and deletes from the original queue within a transaction" do
      message = build_message_double(msg_id: 42, message: '{"data":"test"}', headers: nil)
      client.move_to_dead_letter("default", message)

      expect(mock_pgmq).to have_received(:transaction)
      expect(mock_pgmq).to have_received(:produce).with("pgbus_test_default_dlq", '{"data":"test"}', headers: nil)
      expect(mock_pgmq).to have_received(:delete).with("pgbus_test_default", 42)
    end
  end

  describe "#metrics" do
    context "with a queue_name" do
      it "returns metrics for the prefixed queue" do
        client.metrics("default")

        expect(mock_pgmq).to have_received(:metrics).with("pgbus_test_default")
      end
    end

    context "without a queue_name" do
      it "returns all metrics" do
        client.metrics

        expect(mock_pgmq).to have_received(:metrics_all)
      end
    end
  end

  describe "#oldest_claimable_ages" do
    let(:raw_conn) { double("PG::Connection") }

    before { allow(mock_pgmq).to receive(:with_connection).and_yield(raw_conn) }

    context "with a queue_name" do
      it "returns the vt-aware age of the oldest claimable message in the prefixed queue" do
        allow(raw_conn).to receive(:exec)
          .with(/min\(vt\).*FROM pgmq\.q_pgbus_test_default.*WHERE vt <= NOW\(\)/m)
          .and_return([{ "age_sec" => "42" }])

        expect(client.oldest_claimable_ages("default")).to eq(42)
      end

      it "returns nil when only vt-parked (scheduled/retrying) messages remain" do
        allow(raw_conn).to receive(:exec).and_return([{ "age_sec" => nil }])

        expect(client.oldest_claimable_ages("default")).to be_nil
      end
    end

    context "without a queue_name" do
      it "maps every queue in pgmq.meta to its claimable age" do
        allow(raw_conn).to receive(:exec)
          .with(/FROM pgmq\.meta/)
          .and_return([{ "queue_name" => "pgbus_test_default" }, { "queue_name" => "pgbus_test_mailers" }])
        allow(raw_conn).to receive(:exec)
          .with(/FROM pgmq\.q_pgbus_test_default/m)
          .and_return([{ "age_sec" => "10" }])
        allow(raw_conn).to receive(:exec)
          .with(/FROM pgmq\.q_pgbus_test_mailers/m)
          .and_return([{ "age_sec" => nil }])

        expect(client.oldest_claimable_ages).to eq(
          "pgbus_test_default" => 10,
          "pgbus_test_mailers" => nil
        )
      end
    end
  end

  describe "#pool_stats" do
    it "returns pgmq pool stats merged with the configured pool_timeout" do
      allow(mock_pgmq).to receive(:stats).and_return({ size: 5, available: 3 })

      expect(client.pool_stats).to eq(size: 5, available: 3, pool_timeout: config.pool_timeout)
    end

    it "reflects the pgmq pool availability as it changes" do
      allow(mock_pgmq).to receive(:stats).and_return({ size: 8, available: 0 })

      stats = client.pool_stats

      expect(stats[:size]).to eq(8)
      expect(stats[:available]).to eq(0)
    end

    it "returns an empty hash instead of raising when pgmq.stats fails" do
      allow(mock_pgmq).to receive(:stats).and_raise(StandardError, "boom")

      expect(client.pool_stats).to eq({})
    end
  end

  describe "#verify_connection!" do
    let(:raw_conn) { double("PG::Connection") }

    before do
      real_pgmq_connection_error
      stub_const("PG::Error", Class.new(StandardError)) unless defined?(PG::Error)
    end

    context "when the connection is healthy" do
      before do
        allow(mock_pgmq).to receive(:with_connection).and_yield(raw_conn)
        allow(raw_conn).to receive(:exec).with("SELECT 1").and_return(double("PG::Result"))
      end

      it "runs SELECT 1 through a pooled connection" do
        client.verify_connection!

        expect(raw_conn).to have_received(:exec).with("SELECT 1")
      end

      it "returns a truthy value" do
        expect(client.verify_connection!).to be_truthy
      end
    end

    context "when require_primary is set and the connection is in recovery (a replica, issue #332)" do
      before do
        config.require_primary = true
        allow(mock_pgmq).to receive(:with_connection).and_yield(raw_conn)
        allow(raw_conn).to receive(:exec).with("SELECT 1").and_return(double("PG::Result"))
        # PrimaryValidator asks pg_is_in_recovery(); a replica returns "t".
        allow(raw_conn).to receive(:exec).with(Pgbus::Process::PrimaryValidator::RECOVERY_QUERY)
                                         .and_return(double("PG::Result", getvalue: "t"))
      end

      it "raises rather than reporting a healthy connection" do
        expect { client.verify_connection! }
          .to raise_error(Pgbus::ConfigurationError, /replica|recovery/i)
      end
    end

    context "when require_primary is set and the connection is on the primary (issue #332)" do
      before do
        config.require_primary = true
        allow(mock_pgmq).to receive(:with_connection).and_yield(raw_conn)
        allow(raw_conn).to receive(:exec).with("SELECT 1").and_return(double("PG::Result"))
        allow(raw_conn).to receive(:exec).with(Pgbus::Process::PrimaryValidator::RECOVERY_QUERY)
                                         .and_return(double("PG::Result", getvalue: "f"))
      end

      it "verifies successfully" do
        expect(client.verify_connection!).to be_truthy
      end
    end

    context "when pgmq raises a ConnectionError" do
      before do
        allow(mock_pgmq).to receive(:with_connection)
          .and_raise(PGMQ::Errors::ConnectionError, "could not connect to server")
      end

      it "raises Pgbus::ConfigurationError" do
        expect { client.verify_connection! }.to raise_error(Pgbus::ConfigurationError)
      end

      it "includes the underlying error message" do
        expect { client.verify_connection! }
          .to raise_error(Pgbus::ConfigurationError, /could not connect to server/)
      end

      it "names the database_url config source" do
        expect { client.verify_connection! }
          .to raise_error(Pgbus::ConfigurationError, /database_url/)
      end
    end

    context "when a raw PG::Error surfaces" do
      before do
        allow(mock_pgmq).to receive(:with_connection).and_yield(raw_conn)
        allow(raw_conn).to receive(:exec).with("SELECT 1")
                                         .and_raise(PG::Error, "server closed the connection unexpectedly")
      end

      it "raises Pgbus::ConfigurationError carrying the PG message" do
        expect { client.verify_connection! }
          .to raise_error(Pgbus::ConfigurationError, /server closed the connection unexpectedly/)
      end
    end

    context "when configured with connection_params" do
      let(:config) do
        Pgbus::Configuration.new.tap do |c|
          c.connection_params = { host: "localhost", dbname: "pgbus_test" }
          c.queue_prefix = "pgbus_test"
        end
      end

      before do
        allow(mock_pgmq).to receive(:with_connection)
          .and_raise(PGMQ::Errors::ConnectionError, "boom")
      end

      it "names the connection_params config source" do
        expect { client.verify_connection! }
          .to raise_error(Pgbus::ConfigurationError, /connection_params/)
      end
    end
  end

  describe "#ping" do
    let(:raw_conn) { double("PG::Connection") }

    it "runs SELECT 1 through with_raw_connection and returns true" do
      allow(client).to receive(:with_raw_connection).and_yield(raw_conn)
      allow(raw_conn).to receive(:exec).with("SELECT 1").and_return(double("PG::Result"))

      expect(client.ping).to be(true)
      expect(raw_conn).to have_received(:exec).with("SELECT 1")
    end

    it "propagates a connection error to the caller" do
      stub_const("PG::Error", Class.new(StandardError)) unless defined?(PG::Error)
      allow(client).to receive(:with_raw_connection)
        .and_raise(PG::Error, "could not connect to server")

      expect { client.ping }.to raise_error(PG::Error, /could not connect to server/)
    end
  end

  describe "#configured_queues" do
    it "returns the logical queues derived from the configuration" do
      config.workers = [{ queues: %w[critical mailers], threads: 2 }]

      expect(client.configured_queues).to contain_exactly("default", "critical", "mailers")
    end
  end

  describe "#physical_queue_names" do
    it "returns the single prefixed name when priority is disabled" do
      expect(client.physical_queue_names("jobs")).to eq(["pgbus_test_jobs"])
    end

    context "when priority levels are configured" do
      let(:config) do
        Pgbus::Configuration.new.tap do |c|
          c.database_url = "postgres://localhost/pgbus_test"
          c.queue_prefix = "pgbus_test"
          c.priority_levels = 3
        end
      end

      it "expands to the _pN sub-queue names bootstrap actually creates" do
        expect(client.physical_queue_names("jobs"))
          .to eq(%w[pgbus_test_jobs_p0 pgbus_test_jobs_p1 pgbus_test_jobs_p2])
      end
    end
  end

  describe "#pgmq_installed?" do
    let(:raw_conn) { double("PG::Connection") }

    before { allow(client).to receive(:with_raw_connection).and_yield(raw_conn) }

    it "returns true when the pgmq.meta table exists" do
      allow(raw_conn).to receive(:exec).with(/pgmq.*meta/).and_return(double(ntuples: 1))

      expect(client.pgmq_installed?).to be(true)
    end

    it "returns false when the pgmq schema is absent" do
      allow(raw_conn).to receive(:exec).with(/pgmq.*meta/).and_return(double(ntuples: 0))

      expect(client.pgmq_installed?).to be(false)
    end
  end

  describe "#pgmq_schema_version" do
    let(:raw_conn) { double("PG::Connection") }

    before { allow(client).to receive(:with_raw_connection).and_yield(raw_conn) }

    it "returns the latest installed version from the tracking table" do
      allow(raw_conn).to receive(:exec)
        .with(/pgbus_pgmq_schema_versions/)
        .and_return([{ "version" => "1.5.0" }])

      expect(client.pgmq_schema_version).to eq("1.5.0")
    end

    it "returns nil when the tracking table is empty" do
      allow(raw_conn).to receive(:exec)
        .with(/pgbus_pgmq_schema_versions/)
        .and_return([])

      expect(client.pgmq_schema_version).to be_nil
    end

    it "returns nil when the tracking table does not exist" do
      stub_const("PG::UndefinedTable", Class.new(StandardError)) unless defined?(PG::UndefinedTable)
      allow(raw_conn).to receive(:exec)
        .with(/pgbus_pgmq_schema_versions/)
        .and_raise(PG::UndefinedTable, "relation \"pgbus_pgmq_schema_versions\" does not exist")

      expect(client.pgmq_schema_version).to be_nil
    end
  end

  describe "#notify_enabled?" do
    let(:conn) { double("PG::Connection") }

    before do
      # The class-level subject stubs notify_trigger_current? — call the real
      # method here so we exercise the prefix + pooled-check wiring.
      allow(client).to receive(:notify_trigger_current?).and_call_original
    end

    it "returns true when the prefixed queue has a current NOTIFY trigger" do
      allow(mock_pgmq).to receive(:with_connection).and_yield(conn)
      allow(conn).to receive(:exec_params).and_return(double("PG::Result", ntuples: 1))

      expect(client.notify_enabled?("jobs")).to be(true)
      expect(conn).to have_received(:exec_params).with(
        a_string_matching(/pg_trigger/),
        ["pgbus_test_jobs", Pgbus::Client::NOTIFY_THROTTLE_MS]
      )
    end

    it "returns false when no current trigger exists" do
      allow(mock_pgmq).to receive(:with_connection).and_yield(conn)
      allow(conn).to receive(:exec_params).and_return(double("PG::Result", ntuples: 0))

      expect(client.notify_enabled?("jobs")).to be(false)
    end
  end

  describe "#notify_trigger_current? (via ensure_queue)" do
    # The class-level subject stubs notify_trigger_current? so most specs skip
    # its raw SQL. Here we exercise the real method to prove it routes through
    # the pooled PGMQ connection instead of opening a fresh PG.connect.
    let(:conn) { double("PG::Connection") }
    let(:result) { double("PG::Result", ntuples: 0) }

    before do
      config.listen_notify = true
      allow(client).to receive(:notify_trigger_current?).and_call_original
      allow(conn).to receive(:exec_params).and_return(result)
    end

    it "checks the trigger through the pooled @pgmq.with_connection, not a fresh PG.connect" do
      allow(mock_pgmq).to receive(:with_connection).and_yield(conn)

      client.ensure_queue("jobs")

      expect(mock_pgmq).to have_received(:with_connection)
    end

    it "never opens a raw PG.connect connection during an N-queue bootstrap" do
      stub_const("PG", Module.new) unless defined?(PG)
      allow(PG).to receive(:connect)
      allow(mock_pgmq).to receive(:with_connection).and_yield(conn)
      allow(client).to receive(:with_raw_connection).and_call_original

      client.ensure_all_queues

      expect(PG).not_to have_received(:connect)
      expect(client).not_to have_received(:with_raw_connection)
    end

    it "passes the physical queue name and throttle interval to exec_params" do
      allow(mock_pgmq).to receive(:with_connection).and_yield(conn)

      client.ensure_queue("jobs")

      expect(conn).to have_received(:exec_params).with(
        a_string_matching(/pg_trigger/),
        ["pgbus_test_jobs", Pgbus::Client::NOTIFY_THROTTLE_MS]
      )
    end

    it "skips enable_notify_insert when the pooled check finds a current trigger" do
      allow(mock_pgmq).to receive(:with_connection).and_yield(conn)
      allow(conn).to receive(:exec_params).and_return(double("PG::Result", ntuples: 1))

      client.ensure_queue("jobs")

      expect(mock_pgmq).not_to have_received(:enable_notify_insert)
    end

    it "falls back to false (and enables notify) when the pooled connection raises" do
      real_pgmq_connection_error
      allow(mock_pgmq).to receive(:with_connection)
        .and_raise(PGMQ::Errors::ConnectionError, "schema not ready")

      expect { client.ensure_queue("jobs") }.not_to raise_error
      expect(mock_pgmq).to have_received(:enable_notify_insert)
    end

    context "with the shared-connection Proc path (pool_size=1)" do
      # Force the Proc branch of Client#initialize (@shared_connection = true,
      # @pgmq_mutex = Mutex.new, pgmq pool_size=1) so we prove the trigger check
      # does not deadlock on a nested checkout against a single-slot pool.
      subject(:shared_client) do
        allow(config).to receive(:connection_options).and_return(-> { conn })
        allow(PGMQ::Client).to receive(:new).and_return(mock_pgmq)
        c = described_class.new(config, schema_ensured: true)
        allow(c).to receive(:tune_autovacuum)
        c
      end

      before { allow(mock_pgmq).to receive(:with_connection).and_yield(conn) }

      it "uses a Mutex to serialize the single-slot pool" do
        expect(shared_client.shared_connection?).to be(true)
      end

      it "completes the trigger check without a nested pool checkout / deadlock" do
        # create() and the trigger check are sequential checkouts, not nested:
        # @pgmq.create returns before notify_trigger_current? checks out again.
        expect { shared_client.ensure_queue("jobs") }.not_to raise_error
        expect(mock_pgmq).to have_received(:with_connection)
      end
    end
  end

  describe "#list_queues" do
    it "delegates to pgmq.list_queues" do
      client.list_queues

      expect(mock_pgmq).to have_received(:list_queues)
    end
  end

  describe "#purge_queue" do
    it "purges the prefixed queue" do
      client.purge_queue("default")

      expect(mock_pgmq).to have_received(:purge_queue).with("pgbus_test_default")
    end

    it "skips prefixing when prefixed: false" do
      client.purge_queue("pgbus_test_default", prefixed: false)

      expect(mock_pgmq).to have_received(:purge_queue).with("pgbus_test_default")
    end
  end

  describe "#drop_queue" do
    it "drops the prefixed queue" do
      client.drop_queue("default")

      expect(mock_pgmq).to have_received(:drop_queue).with("pgbus_test_default")
    end

    it "skips prefixing when prefixed: false" do
      client.drop_queue("pgbus_test_default", prefixed: false)

      expect(mock_pgmq).to have_received(:drop_queue).with("pgbus_test_default")
    end

    it "removes the queue from the created cache" do
      client.ensure_queue("default")
      client.drop_queue("default")
      # Observable proof the cache entry was evicted: the next ensure_queue must
      # re-create the queue rather than short-circuiting on a stale cache hit.
      client.ensure_queue("default")

      expect(mock_pgmq).to have_received(:create).with("pgbus_test_default").twice
    end
  end

  describe "#purge_archive" do
    let(:conn) { double("conn") }
    let(:result) { double("result", cmd_tuples: 50) }

    before do
      allow(client).to receive(:with_raw_connection).and_yield(conn)
      allow(conn).to receive(:exec_params).and_return(result)
    end

    it "deletes archive entries older than the given time" do
      cutoff = Time.now - 3600
      client.purge_archive("default", older_than: cutoff)

      expect(conn).to have_received(:exec_params).with(
        a_string_matching(/DELETE FROM pgmq\.a_pgbus_test_default/),
        [cutoff, 1000]
      )
    end

    it "loops until batch is not full" do
      small_result = double("result", cmd_tuples: 10)
      allow(conn).to receive(:exec_params).and_return(result, small_result)

      total = client.purge_archive("default", older_than: Time.now, batch_size: 50)
      expect(total).to eq(60) # 50 + 10
    end
  end

  describe "#message_exists?" do
    let(:conn) { double("conn") }

    before { allow(client).to receive(:with_raw_connection).and_yield(conn) }

    it "raises ArgumentError when neither msg_id nor uniqueness_key is given" do
      expect { client.message_exists?("default") }.to raise_error(ArgumentError, /msg_id, uniqueness_key, or both/)
    end

    it "filters msg_id lookup by uniqueness_key when both are given" do
      allow(conn).to receive(:exec_params).with(
        a_string_matching(/msg_id = \$1.*pgbus_uniqueness_key/m),
        [1, "MyJob"]
      ).and_return(double("result", ntuples: 1))

      expect(client.message_exists?("default", msg_id: 1, uniqueness_key: "MyJob")).to be(true)
    end

    it "accepts a symbol queue name" do
      allow(conn).to receive(:exec_params).with(
        a_string_matching(/pgmq\.q_pgbus_test_default/),
        [42]
      ).and_return(double("result", ntuples: 1))

      expect(client.message_exists?(:default, msg_id: 42)).to be(true)
    end

    context "when looking up by msg_id" do
      it "returns true when the row exists in the prefixed queue table" do
        allow(conn).to receive(:exec_params).with(
          a_string_matching(/SELECT 1 FROM pgmq\.q_pgbus_test_default WHERE msg_id = \$1/),
          [42]
        ).and_return(double("result", ntuples: 1))

        expect(client.message_exists?("default", msg_id: 42)).to be(true)
      end

      context "when priority sub-queues are enabled" do
        before do
          config.priority_levels = 3
          # Parent before already constructed the client with StandardStrategy.
          client.instance_variable_set(:@queue_strategy, Pgbus::QueueFactory.for(config))
        end

        after { config.priority_levels = nil }

        it "returns true when the msg_id lives in a priority sub-queue" do
          allow(conn).to receive(:exec_params) do |sql, _binds|
            ntuples = sql.include?("q_pgbus_test_default_p1") ? 1 : 0
            double("result", ntuples: ntuples)
          end

          expect(client.message_exists?("default", msg_id: 42)).to be(true)
        end

        it "returns false when no physical sub-queue holds the msg_id" do
          allow(conn).to receive(:exec_params).and_return(double("result", ntuples: 0))

          expect(client.message_exists?("default", msg_id: 42)).to be(false)
        end
      end

      it "returns false when the row does not exist" do
        allow(conn).to receive(:exec_params).and_return(double("result", ntuples: 0))

        expect(client.message_exists?("default", msg_id: 42)).to be(false)
      end

      it "accepts an already-prefixed physical queue name" do
        allow(conn).to receive(:exec_params).with(
          a_string_matching(/pgmq\.q_pgbus_test_default/),
          [42]
        ).and_return(double("result", ntuples: 1))

        expect(client.message_exists?("pgbus_test_default", msg_id: 42)).to be(true)
      end
    end

    context "when looking up by uniqueness_key" do
      it "queries the JSONB pgbus_uniqueness_key field" do
        allow(conn).to receive(:exec_params).with(
          a_string_matching(/message::jsonb ->> 'pgbus_uniqueness_key' = \$1/),
          ["MyJob"]
        ).and_return(double("result", ntuples: 1))

        expect(client.message_exists?("default", uniqueness_key: "MyJob")).to be(true)
      end
    end

    context "when the queue table is missing" do
      before { stub_const("PG::UndefinedTable", Class.new(StandardError)) }

      it "returns nil when the cause is PG::UndefinedTable (locale-independent)" do
        cause = PG::UndefinedTable.new("relation does not exist")
        wrapped = ActiveRecord::StatementInvalid.new("ERROR: relation \"pgmq.q_x\" does not exist")
        allow(wrapped).to receive(:cause).and_return(cause)
        allow(conn).to receive(:exec_params).and_raise(wrapped)

        expect(client.message_exists?("nonexistent", msg_id: 1)).to be_nil
      end

      it "re-raises a StatementInvalid when it is not an UndefinedTable" do
        wrapped = ActiveRecord::StatementInvalid.new("syntax error")
        allow(wrapped).to receive(:cause).and_return(StandardError.new("syntax error"))
        allow(conn).to receive(:exec_params).and_raise(wrapped)

        expect { client.message_exists?("default", msg_id: 1) }.to raise_error(ActiveRecord::StatementInvalid)
      end
    end
  end

  describe "#uniqueness_keys_present" do
    let(:conn) { double("conn") }

    before { allow(client).to receive(:with_raw_connection).and_yield(conn) }

    it "returns an empty Set without querying when given no keys" do
      allow(conn).to receive(:exec)
      allow(conn).to receive(:exec_params)

      expect(client.uniqueness_keys_present([])).to eq(Set.new)
      expect(client.uniqueness_keys_present(nil)).to eq(Set.new)
      expect(conn).not_to have_received(:exec)
      expect(conn).not_to have_received(:exec_params)
    end

    it "unions keys found across live pgmq.meta queue tables" do
      allow(conn).to receive(:exec)
        .with("SELECT queue_name FROM pgmq.meta ORDER BY queue_name")
        .and_return(
          [
            { "queue_name" => "pgbus_test_default" },
            { "queue_name" => "pgbus_test_critical" }
          ]
        )
      allow(conn).to receive(:exec_params).with(
        a_string_matching(/pgmq\.q_pgbus_test_default.*IN \(\$1, \$2\)/m),
        ["ERP::Manager", "Gone"]
      ).and_return([{ "k" => "ERP::Manager" }])
      allow(conn).to receive(:exec_params).with(
        a_string_matching(/pgmq\.q_pgbus_test_critical.*IN \(\$1, \$2\)/m),
        ["ERP::Manager", "Gone"]
      ).and_return([])

      expect(client.uniqueness_keys_present(["ERP::Manager", "Gone"])).to eq(Set["ERP::Manager"])
    end

    it "skips a missing queue table and keeps keys found in other queues" do
      stub_const("PG::UndefinedTable", Class.new(StandardError))
      allow(conn).to receive(:exec)
        .with("SELECT queue_name FROM pgmq.meta ORDER BY queue_name")
        .and_return(
          [
            { "queue_name" => "pgbus_test_stale" },
            { "queue_name" => "pgbus_test_critical" }
          ]
        )
      allow(conn).to receive(:exec_params).with(
        a_string_matching(/pgmq\.q_pgbus_test_stale/),
        anything
      ).and_raise(PG::UndefinedTable.new("relation does not exist"))
      allow(conn).to receive(:exec_params).with(
        a_string_matching(/pgmq\.q_pgbus_test_critical/),
        anything
      ).and_return([{ "k" => "ERP::Manager" }])

      expect(client.uniqueness_keys_present(["ERP::Manager"])).to eq(Set["ERP::Manager"])
    end

    it "raises when pgmq.meta cannot be read" do
      allow(conn).to receive(:exec).and_raise(StandardError, "connection refused")

      expect { client.uniqueness_keys_present(["ERP::Manager"]) }.to raise_error(StandardError, "connection refused")
    end
  end

  describe "#read_batch_prioritized" do
    context "when priority is not enabled" do
      it "falls back to regular read_batch" do
        client.read_batch_prioritized("default", qty: 5)

        expect(mock_pgmq).to have_received(:read_batch).with("pgbus_test_default", vt: config.visibility_timeout, qty: 5)
      end
    end

    context "when priority is enabled" do
      before { config.priority_levels = 3 }
      after { config.priority_levels = nil }

      it "reads from p0 first, then p1, then p2" do
        # Ensure all sub-queues are created first
        client.ensure_queue("default")

        msg0 = build_message_double(msg_id: 1, message: '{"p":0}')
        msg1 = build_message_double(msg_id: 2, message: '{"p":1}')

        allow(mock_pgmq).to receive(:read_batch)
          .with("pgbus_test_default_p0", anything).and_return([msg0])
        allow(mock_pgmq).to receive(:read_batch)
          .with("pgbus_test_default_p1", anything).and_return([msg1])
        allow(mock_pgmq).to receive(:read_batch)
          .with("pgbus_test_default_p2", anything).and_return([])

        results = client.read_batch_prioritized("default", qty: 5)

        expect(results.size).to eq(2)
        expect(results[0][0]).to eq("pgbus_test_default_p0")
        expect(results[1][0]).to eq("pgbus_test_default_p1")
      end

      it "stops when qty is filled" do
        client.ensure_queue("default")

        msgs = 3.times.map { |i| build_message_double(msg_id: i, message: "{}") }
        allow(mock_pgmq).to receive(:read_batch)
          .with("pgbus_test_default_p0", anything).and_return(msgs)

        results = client.read_batch_prioritized("default", qty: 3)

        expect(results.size).to eq(3)
        # Should not read from p1 or p2
        expect(mock_pgmq).not_to have_received(:read_batch).with("pgbus_test_default_p1", anything)
      end
    end
  end

  describe "#send_message with priority" do
    context "when priority is enabled" do
      before { config.priority_levels = 3 }
      after { config.priority_levels = nil }

      it "routes to the correct sub-queue" do
        client.send_message("default", { "data" => "test" }, priority: 0)

        expect(mock_pgmq).to have_received(:produce).with(
          "pgbus_test_default_p0",
          '{"data":"test"}',
          headers: nil,
          delay: 0
        )
      end

      it "uses default_priority when none specified" do
        config.default_priority = 1
        client.send_message("default", { "data" => "test" })

        expect(mock_pgmq).to have_received(:produce).with(
          "pgbus_test_default_p1",
          '{"data":"test"}',
          headers: nil,
          delay: 0
        )
      end

      it "clamps priority to valid range" do
        client.send_message("default", { "data" => "test" }, priority: 99)

        expect(mock_pgmq).to have_received(:produce).with(
          "pgbus_test_default_p2",
          '{"data":"test"}',
          headers: nil,
          delay: 0
        )
      end
    end
  end

  describe "#send_stream_message (durable stream broadcasts)" do
    # Durable broadcasts publish through the dedicated streams pool
    # (@streams_pgmq), so this block wires two distinct PGMQ::Client doubles
    # and asserts on the streams one (issue #315).
    subject(:client) do
      allow(PGMQ::Client).to receive(:new).and_return(job_pgmq, streams_pgmq)
      c = described_class.new(config, schema_ensured: true)
      allow(c).to receive(:tune_autovacuum)
      allow(c).to receive(:notify_trigger_current?).and_return(false)
      c
    end

    let(:job_pgmq) { build_mock_pgmq }
    let(:streams_pgmq) { build_mock_pgmq }

    before do
      allow(client).to receive_messages(ensure_stream_queue: nil)
    end

    it "targets the bare queue when priority is disabled" do
      client.send_stream_message("chat_42", { "html" => "<p>hi</p>" })

      expect(streams_pgmq).to have_received(:produce).with(
        "pgbus_test_chat_42",
        '{"html":"<p>hi</p>"}',
        headers: nil,
        delay: 0
      )
    end

    context "when priority_levels > 1 is configured (issue #310)" do
      before { config.priority_levels = 3 }
      after { config.priority_levels = nil }

      it "still targets the BARE queue, not a _pN sub-queue" do
        # The streamer LISTENs and replays on the bare queue only. Routing a
        # durable broadcast to _p1 (as send_message would) means the browser
        # never receives it. Streams are peek-based; priority is meaningless.
        client.send_stream_message("chat_42", { "html" => "<p>hi</p>" })

        expect(streams_pgmq).to have_received(:produce).with(
          "pgbus_test_chat_42",
          anything,
          headers: nil,
          delay: 0
        )
        expect(streams_pgmq).not_to have_received(:produce).with(
          a_string_matching(/_p\d+$/), any_args
        )
      end
    end

    it "ensures the stream queue (bare) before producing" do
      client.send_stream_message("chat_42", { "html" => "x" })

      expect(client).to have_received(:ensure_stream_queue).with("chat_42")
    end

    it "produces via the dedicated streams pool, not the job pool (issue #315)" do
      # The publish INSERT must draw from @streams_pgmq so a saturated job
      # pool can't block a broadcast on job-pool checkout.
      client.send_stream_message("chat_42", { "html" => "x" })

      expect(streams_pgmq).to have_received(:produce)
      expect(job_pgmq).not_to have_received(:produce)
    end
  end

  describe "dedicated streams DB pool (issue #315)" do
    # Two distinct PGMQ::Client doubles: the job pool and the streams pool.
    # Client#initialize calls PGMQ::Client.new twice on the dedicated
    # (String/Hash) connection path — once for @pgmq, once for @streams_pgmq.
    subject(:client) do
      allow(PGMQ::Client).to receive(:new).and_return(job_pgmq, streams_pgmq)
      c = described_class.new(config, schema_ensured: true)
      allow(c).to receive(:tune_autovacuum)
      allow(c).to receive(:notify_trigger_current?).and_return(false)
      allow(c).to receive(:ensure_stream_queue)
      c
    end

    let(:job_pgmq) { build_mock_pgmq }
    let(:streams_pgmq) { build_mock_pgmq }

    it "builds a second PGMQ::Client sized by streams_pool_size / streams_pool_timeout" do
      config.streams_pool_size = 8
      config.streams_pool_timeout = 3
      client # force construction

      expect(PGMQ::Client).to have_received(:new).with(anything, pool_size: 8, pool_timeout: 3)
    end

    context "with connection_guc_mode :session and database.yml :variables (issue #332)" do
      let(:config) do
        Pgbus::Configuration.new.tap do |c|
          c.connection_params = { host: "localhost", dbname: "pgbus_test",
                                  variables: { "client_min_messages" => "warning" } }
          c.queue_prefix = "pgbus_test"
          c.connection_guc_mode = :session
        end
      end

      it "passes a callable connection factory to PGMQ::Client (post-connect SET, pooler-safe)" do
        received = []
        allow(PGMQ::Client).to receive(:new) do |opts, **_kw|
          received << opts
          mock_pgmq
        end

        c = described_class.new(config, schema_ensured: true)
        allow(c).to receive(:tune_autovacuum)

        expect(received).to include(an_instance_of(Proc))
      end

      it "the factory applies each :variables entry via SET on a fresh connection" do
        raw_conn = double("PG::Connection", exec: nil)
        # Client#initialize probes PG.library_version (libpq_read_bounds_effective?)
        # during construction, so the PG stub must carry it too — not just
        # .connect, which the factory calls later. A bare Module.new lacks it and
        # blows up at construction (seed-dependent, only in CI).
        pg_module = Module.new do
          def self.library_version = 160_000
        end
        allow(pg_module).to receive(:connect).and_return(raw_conn)
        stub_const("PG", pg_module)

        factory = nil
        allow(PGMQ::Client).to receive(:new) do |opts, **_kw|
          factory ||= opts if opts.respond_to?(:call)
          mock_pgmq
        end

        # Build directly (not the memoized subject, whose block re-stubs .new).
        c = described_class.new(config, schema_ensured: true)
        allow(c).to receive(:tune_autovacuum)
        factory.call

        expect(raw_conn).to have_received(:exec).with(/SET\s+client_min_messages\s*=\s*['"]?warning['"]?/i)
      end
    end

    it "tags the streams pool connection with a per-process application_name (P1, issue #323)" do
      # A String URL streams pool gets the application_name appended so the
      # autoscaler can count peer processes from pg_stat_activity.
      client # force construction

      expect(PGMQ::Client).to have_received(:new)
        .with(a_string_matching(/application_name=pgbus_streams_#{Process.pid}\b/),
              pool_size: anything, pool_timeout: anything)
    end

    it "builds the streams pool from streams_connection_options, not the job DB (P2, issue #323)" do
      # When a separate streams DB is configured, the streams pool must connect
      # there — not to the job database_url.
      config.streams_database_url = "postgres://streamshost/streamsdb"
      client # force construction

      expect(PGMQ::Client).to have_received(:new)
        .with(a_string_starting_with("postgres://streamshost/streamsdb"),
              pool_size: config.streams_pool_size, pool_timeout: config.streams_pool_timeout)
    end

    it "routes the streams pool through streams_pool_* independently of the LISTEN overrides (issue #358)" do
      # Pooler-bypass installs pin only the LISTEN connection to the direct
      # port (streams_port); the pool's INSERT/SELECT traffic is pooler-safe
      # and must not eat direct-port max_connections slots.
      config.streams_port = 5432
      config.streams_pool_port = 6432
      client # force construction

      expect(PGMQ::Client).to have_received(:new)
        .with(a_string_matching(/\bport=6432\b/),
              pool_size: config.streams_pool_size, pool_timeout: config.streams_pool_timeout)
      expect(PGMQ::Client).not_to have_received(:new)
        .with(a_string_matching(/\bport=5432\b/), any_args)
    end

    describe "#read_after over the streams pool" do
      let(:conn) { double("PG::Connection") }

      before do
        allow(streams_pgmq).to receive(:with_connection).and_yield(conn)
      end

      it "reads through the pooled streams connection, never a per-call PG.connect" do
        allow(conn).to receive(:exec_params).and_return([])
        allow(client).to receive(:with_raw_connection).and_call_original

        client.read_after("chat_42", after_id: 0)

        expect(streams_pgmq).to have_received(:with_connection)
        expect(client).not_to have_received(:with_raw_connection)
      end

      it "routes stream_current_msg_id through the pooled streams connection too" do
        allow(conn).to receive(:exec).and_return([{ "max" => "7" }])

        expect(client.stream_current_msg_id("chat_42")).to eq(7)
        expect(streams_pgmq).to have_received(:with_connection)
      end

      it "still swallows a missing-queue UndefinedTable to an empty result" do
        skip "PG not loaded" unless defined?(PG::UndefinedTable)

        allow(conn).to receive(:exec_params)
          .and_raise(PG::UndefinedTable.new('relation "pgmq.q_pgbus_test_chat_42" does not exist'))

        expect(client.read_after("chat_42", after_id: 0)).to eq([])
      end
    end

    describe "#streams_pool_stats" do
      it "returns the streams pgmq pool stats merged with streams_pool_timeout" do
        allow(streams_pgmq).to receive(:stats).and_return({ size: 5, available: 2 })

        expect(client.streams_pool_stats).to eq(size: 5, available: 2, pool_timeout: config.streams_pool_timeout)
      end
    end

    context "with the shared-ActiveRecord (Proc) connection path" do
      subject(:shared_client) do
        allow(config).to receive(:connection_options).and_return(-> { proc_conn })
        allow(PGMQ::Client).to receive(:new).and_return(job_pgmq)
        c = described_class.new(config, schema_ensured: true)
        allow(c).to receive(:tune_autovacuum)
        allow(c).to receive(:notify_trigger_current?).and_return(false)
        allow(c).to receive(:ensure_stream_queue)
        c
      end

      let(:proc_conn) { double("PG::Connection") }

      it "does NOT build a second pool (libpq is not thread-safe)" do
        shared_client # force construction

        expect(PGMQ::Client).to have_received(:new).once
      end

      it "routes send_stream_message through the single serialized job pool" do
        shared_client.send_stream_message("chat_42", { "html" => "x" })

        expect(job_pgmq).to have_received(:produce)
      end

      it "reads via with_raw_connection (the shared serialized connection)" do
        allow(shared_client).to receive(:with_raw_connection).and_yield(proc_conn)
        allow(proc_conn).to receive(:exec_params).and_return([])

        shared_client.read_after("chat_42", after_id: 0)

        expect(shared_client).to have_received(:with_raw_connection)
      end
    end
  end

  describe "#bind_topic" do
    it "ensures the queue and binds the pattern" do
      client.bind_topic("orders.#", "events")

      expect(mock_pgmq).to have_received(:create).with("pgbus_test_events")
      expect(mock_pgmq).to have_received(:bind_topic).with("orders.#", "pgbus_test_events")
    end
  end

  describe "#publish_to_topic" do
    it "publishes a serialized payload with the routing key" do
      client.publish_to_topic("orders.created", { "id" => 1 })

      expect(mock_pgmq).to have_received(:produce_topic).with(
        "orders.created",
        '{"id":1}',
        headers: nil,
        delay: 0
      )
    end

    it "serializes headers when provided" do
      client.publish_to_topic("orders.created", "body", headers: { "trace" => "abc" }, delay: 3)

      expect(mock_pgmq).to have_received(:produce_topic).with(
        "orders.created",
        "body",
        headers: '{"trace":"abc"}',
        delay: 3
      )
    end
  end

  describe "stale pgmq connection recovery" do
    before do
      # Load the genuine PGMQ::Errors::ConnectionError so these prove
      # with_stale_connection_retry rescues the real pgmq-ruby class, not a fake.
      real_pgmq_connection_error
      # Stale retries now back off between attempts. Stub the delay so the
      # suite doesn't actually sleep; individual tests that assert the backoff
      # sequence override this with a spy.
      allow(client).to receive(:sleep)
    end

    describe "#send_message" do
      it "retries once when the pooled pgmq connection was killed (PQsocket)" do
        call_count = 0
        allow(mock_pgmq).to receive(:produce) do
          call_count += 1
          raise(PGMQ::Errors::ConnectionError, "Database connection error: PQsocket() can't get socket descriptor") if call_count == 1

          1
        end

        expect(client.send_message("default", { "k" => "v" })).to eq(1)
        expect(call_count).to eq(2)
      end

      it "does not retry on mid-flight server-close errors (potential duplicate risk)" do
        allow(mock_pgmq).to receive(:produce)
          .and_raise(PGMQ::Errors::ConnectionError, "Database connection error: server closed the connection unexpectedly")

        expect { client.send_message("default", { "k" => "v" }) }.to raise_error(PGMQ::Errors::ConnectionError, /server closed/)
      end

      it "does not retry on non-stale ConnectionError (e.g. pool timeout)" do
        call_count = 0
        allow(mock_pgmq).to receive(:produce) do
          call_count += 1
          raise(PGMQ::Errors::ConnectionError, "Connection pool timeout: waited 5.00s")
        end

        expect { client.send_message("default", { "k" => "v" }) }.to raise_error(PGMQ::Errors::ConnectionError, /pool timeout/)
        expect(call_count).to eq(1)
      end

      context "when a pool-timeout ConnectionError is raised" do
        before do
          allow(mock_pgmq).to receive(:stats).and_return({ size: 5, available: 0 })
          allow(mock_pgmq).to receive(:produce)
            .and_raise(PGMQ::Errors::ConnectionError, "Connection pool timeout: waited 5.00s")
        end

        it "re-raises the same PGMQ::Errors::ConnectionError class" do
          expect { client.send_message("default", { "k" => "v" }) }
            .to raise_error(PGMQ::Errors::ConnectionError)
        end

        it "appends the pool state to the message" do
          # Hash#inspect renders differently across Ruby versions ({size: 5}
          # on 3.4+, {:size=>5} on 3.3), so assert on the keys and values
          # without depending on the punctuation between them.
          expect { client.send_message("default", { "k" => "v" }) }
            .to raise_error(PGMQ::Errors::ConnectionError, /size.*5.*available.*0/m)
        end

        it "appends an actionable hint to the message" do
          expect { client.send_message("default", { "k" => "v" }) }
            .to raise_error(PGMQ::Errors::ConnectionError, /pool_size|worker threads/)
        end

        it "preserves the original pool-timeout text" do
          expect { client.send_message("default", { "k" => "v" }) }
            .to raise_error(PGMQ::Errors::ConnectionError, /Connection pool timeout: waited 5.00s/)
        end

        it "does not enrich non-pool-timeout connection errors" do
          allow(mock_pgmq).to receive(:produce)
            .and_raise(PGMQ::Errors::ConnectionError, "Database connection error: server closed the connection unexpectedly")

          expect { client.send_message("default", { "k" => "v" }) }
            .to raise_error(PGMQ::Errors::ConnectionError) { |e| expect(e.message).not_to include("worker threads") }
        end

        it "still raises even if pool_stats itself fails" do
          allow(mock_pgmq).to receive(:stats).and_raise(StandardError, "stats boom")

          expect { client.send_message("default", { "k" => "v" }) }
            .to raise_error(PGMQ::Errors::ConnectionError, /Connection pool timeout/)
        end
      end

      it "gives up after the maximum retries and re-raises" do
        call_count = 0
        allow(mock_pgmq).to receive(:produce) do
          call_count += 1
          raise(PGMQ::Errors::ConnectionError, "Database connection error: PQsocket() can't get socket descriptor")
        end

        expect { client.send_message("default", { "k" => "v" }) }.to raise_error(PGMQ::Errors::ConnectionError)
        # Initial attempt + STALE_RETRY_ATTEMPTS (2) retries = 3 calls.
        expect(call_count).to eq(3)
      end

      it "retries twice with increasing backoff then succeeds" do
        call_count = 0
        allow(mock_pgmq).to receive(:produce) do
          call_count += 1
          raise(PGMQ::Errors::ConnectionError, "Database connection error: PQsocket() can't get socket descriptor") if call_count <= 2

          1
        end

        expect(client.send_message("default", { "k" => "v" })).to eq(1)
        expect(call_count).to eq(3)
        expect(client).to have_received(:sleep).with(0.1).ordered
        expect(client).to have_received(:sleep).with(0.5).ordered
      end

      it "logs one warning per retry with the attempt count" do
        call_count = 0
        allow(mock_pgmq).to receive(:produce) do
          call_count += 1
          raise(PGMQ::Errors::ConnectionError, "Database connection error: PQsocket() can't get socket descriptor") if call_count <= 2

          1
        end
        # Capture the lazily-evaluated warn block bodies so we can assert the
        # attempt-count text this change interpolates.
        warnings = []
        allow(Pgbus.logger).to receive(:warn) { |&blk| warnings << blk.call }

        client.send_message("default", { "k" => "v" })

        expect(warnings).to contain_exactly(
          a_string_matching(%r{attempt 1/2}),
          a_string_matching(%r{attempt 2/2})
        )
      end

      it "does not sleep on the success path" do
        allow(mock_pgmq).to receive(:produce).and_return(1)

        client.send_message("default", { "k" => "v" })

        expect(client).not_to have_received(:sleep)
      end

      it "does not sleep when a non-stale ConnectionError raises" do
        allow(mock_pgmq).to receive(:produce)
          .and_raise(PGMQ::Errors::ConnectionError, "Connection pool timeout: waited 5.00s")

        expect { client.send_message("default", { "k" => "v" }) }.to raise_error(PGMQ::Errors::ConnectionError)
        expect(client).not_to have_received(:sleep)
      end
    end

    # The backoff sleep MUST NOT run while @pgmq_mutex is held, or a stalled
    # retry would block every other operation on the shared-connection path for
    # the full delay. The retry wrapper sits *outside* `synchronized`, so the
    # exception unwinds out of the mutex before the rescue sleeps — this guards
    # against a regression that relocates the sleep inside `synchronized`.
    describe "backoff does not hold the connection mutex (shared-connection path)" do
      subject(:shared_client) do
        allow(PGMQ::Client).to receive(:new).and_return(mock_pgmq)
        c = described_class.new(shared_config, schema_ensured: true)
        allow(c).to receive(:notify_trigger_current?).and_return(false)
        c
      end

      let(:shared_config) do
        Pgbus::Configuration.new.tap do |c|
          c.database_url = nil
          c.connection_params = nil
          c.pool_size = 5
          c.queue_prefix = "pgbus_test"
        end
      end

      before do
        raw_conn = double("PG::Connection")
        ar_connection = double("AR::ConnectionAdapter", raw_connection: raw_conn)
        ar_base = double("AR::Base", connection: ar_connection)
        allow(ar_base).to receive(:connection_db_config).and_raise(StandardError, "no config")
        stub_const("ActiveRecord::Base", ar_base)
      end

      it "runs the shared-connection path (mutex is a real Mutex)" do
        expect(shared_client.shared_connection?).to be(true)
      end

      it "does not hold the connection mutex while sleeping between retries" do
        locked_during_sleep = []
        allow(shared_client).to receive(:sleep) { locked_during_sleep << shared_client.synchronizing? }

        call_count = 0
        allow(mock_pgmq).to receive(:produce) do
          call_count += 1
          raise(PGMQ::Errors::ConnectionError, "Database connection error: PQsocket() can't get socket descriptor") if call_count <= 2

          1
        end

        expect(shared_client.send_message("default", { "k" => "v" })).to eq(1)
        expect(locked_during_sleep).to eq([false, false])
      end
    end

    describe "#send_batch" do
      it "retries once when the pooled pgmq connection was killed" do
        call_count = 0
        allow(mock_pgmq).to receive(:produce_batch) do
          call_count += 1
          raise(PGMQ::Errors::ConnectionError, "Database connection error: PQsocket() can't get socket descriptor") if call_count == 1

          [1]
        end

        expect(client.send_batch("default", [{ "k" => "v" }])).to eq([1])
        expect(call_count).to eq(2)
      end

      it "does not retry on non-stale ConnectionError" do
        call_count = 0
        allow(mock_pgmq).to receive(:produce_batch) do
          call_count += 1
          raise(PGMQ::Errors::ConnectionError, "Connection pool timeout: waited 5.00s")
        end

        expect { client.send_batch("default", [{ "k" => "v" }]) }.to raise_error(PGMQ::Errors::ConnectionError, /pool timeout/)
        expect(call_count).to eq(1)
      end
    end

    describe "#publish_to_topic" do
      it "retries once when the pooled pgmq connection was killed" do
        call_count = 0
        allow(mock_pgmq).to receive(:produce_topic) do
          call_count += 1
          raise(PGMQ::Errors::ConnectionError, "Database connection error: PQsocket() can't get socket descriptor") if call_count == 1

          nil
        end

        client.publish_to_topic("orders.created", { "id" => 1 })

        expect(call_count).to eq(2)
      end

      it "does not retry on non-stale ConnectionError" do
        call_count = 0
        allow(mock_pgmq).to receive(:produce_topic) do
          call_count += 1
          raise(PGMQ::Errors::ConnectionError, "Connection pool timeout: waited 5.00s")
        end

        expect { client.publish_to_topic("orders.created", { "id" => 1 }) }.to raise_error(PGMQ::Errors::ConnectionError, /pool timeout/)
        expect(call_count).to eq(1)
      end
    end

    describe "SSL connection patterns" do
      let(:ssl_eof_msg) { "Database connection error: PQconsumeInput() SSL error: unexpected eof while reading" }
      let(:ssl_syscall_msg) { "Database connection error: SSL SYSCALL error: Connection reset by peer" }

      it "retries on SSL EOF error" do
        call_count = 0
        allow(mock_pgmq).to receive(:produce) do
          call_count += 1
          raise(PGMQ::Errors::ConnectionError, ssl_eof_msg) if call_count == 1

          1
        end

        expect(client.send_message("default", { "k" => "v" })).to eq(1)
        expect(call_count).to eq(2)
      end

      it "retries on SSL SYSCALL error" do
        call_count = 0
        allow(mock_pgmq).to receive(:produce) do
          call_count += 1
          raise(PGMQ::Errors::ConnectionError, ssl_syscall_msg) if call_count == 1

          1
        end

        expect(client.send_message("default", { "k" => "v" })).to eq(1)
        expect(call_count).to eq(2)
      end
    end

    describe "ensure_queue inside retry scope" do
      it "retries send_message when ensure_queue raises stale connection error" do
        create_count = 0
        allow(mock_pgmq).to receive(:create) do
          create_count += 1
          raise(PGMQ::Errors::ConnectionError, "Database connection error: PQsocket() can't get socket descriptor") if create_count == 1

          nil
        end
        # A freshly-built client has an empty queue cache, so ensure_queue really
        # calls @pgmq.create (proven by create_count == 2 below).

        expect(client.send_message("default", { "k" => "v" })).to eq(1)
        expect(create_count).to eq(2)
      end

      it "retries send_batch when ensure_queue raises stale connection error" do
        create_count = 0
        allow(mock_pgmq).to receive(:create) do
          create_count += 1
          raise(PGMQ::Errors::ConnectionError, "Database connection error: connection is closed") if create_count == 1

          nil
        end
        expect(client.send_batch("default", [{ "k" => "v" }])).to eq([1, 2])
        expect(create_count).to eq(2)
      end

      it "retries send_message when ensure_queue raises SSL EOF error" do
        ssl_eof = "Database connection error: PQconsumeInput() SSL error: unexpected eof while reading"
        create_count = 0
        allow(mock_pgmq).to receive(:create) do
          create_count += 1
          raise(PGMQ::Errors::ConnectionError, ssl_eof) if create_count == 1

          nil
        end
        expect(client.send_message("default", { "k" => "v" })).to eq(1)
        expect(create_count).to eq(2)
      end
    end

    describe "ensure_dead_letter_queue inside retry scope" do
      it "retries move_to_dead_letter when ensure_dead_letter_queue raises stale connection error" do
        create_count = 0
        allow(mock_pgmq).to receive(:create) do |name|
          if name.end_with?("_dlq")
            create_count += 1
            raise(PGMQ::Errors::ConnectionError, "Database connection error: connection is closed") if create_count == 1
          end
          nil
        end

        message = build_message_double(msg_id: 42, message: '{"job":"test"}', read_ct: 5)
        client.move_to_dead_letter("default", message)

        expect(create_count).to eq(2)
      end
    end

    describe "bind_topic inside retry scope" do
      it "retries bind_topic when ensure_queue raises stale connection error" do
        create_count = 0
        allow(mock_pgmq).to receive(:create) do
          create_count += 1
          raise(PGMQ::Errors::ConnectionError, "Database connection error: connection not open") if create_count == 1

          nil
        end

        client.bind_topic("orders.*", "default")

        expect(create_count).to eq(2)
        expect(mock_pgmq).to have_received(:bind_topic)
      end
    end

    describe "read operations" do
      # Every read call site goes through with_stale_connection_retry.
      # Using a stale-socket message ensures pgmq-ruby's pre-flight error
      # is recoverable via one retry.
      let(:stale_msg) { "Database connection error: PQsocket() can't get socket descriptor" }

      it "retries read_message" do
        call_count = 0
        allow(mock_pgmq).to receive(:read) do
          call_count += 1
          raise(PGMQ::Errors::ConnectionError, stale_msg) if call_count == 1

          nil
        end

        client.read_message("default")
        expect(call_count).to eq(2)
      end

      it "retries read_batch" do
        call_count = 0
        allow(mock_pgmq).to receive(:read_batch) do
          call_count += 1
          raise(PGMQ::Errors::ConnectionError, stale_msg) if call_count == 1

          []
        end

        client.read_batch("default", qty: 5)
        expect(call_count).to eq(2)
      end

      it "retries read_with_poll" do
        call_count = 0
        allow(mock_pgmq).to receive(:read_with_poll) do
          call_count += 1
          raise(PGMQ::Errors::ConnectionError, stale_msg) if call_count == 1

          []
        end

        client.read_with_poll("default", qty: 5)
        expect(call_count).to eq(2)
      end

      it "retries read_multi" do
        call_count = 0
        allow(mock_pgmq).to receive(:read_multi) do
          call_count += 1
          raise(PGMQ::Errors::ConnectionError, stale_msg) if call_count == 1

          []
        end

        client.read_multi(["default"], qty: 5)
        expect(call_count).to eq(2)
      end
    end

    describe "modification operations" do
      let(:stale_msg) { "Database connection error: PQsocket() can't get socket descriptor" }

      it "retries delete_message" do
        call_count = 0
        allow(mock_pgmq).to receive(:delete) do
          call_count += 1
          raise(PGMQ::Errors::ConnectionError, stale_msg) if call_count == 1

          true
        end

        client.delete_message("default", 1)
        expect(call_count).to eq(2)
      end

      it "retries archive_message" do
        call_count = 0
        allow(mock_pgmq).to receive(:archive) do
          call_count += 1
          raise(PGMQ::Errors::ConnectionError, stale_msg) if call_count == 1

          true
        end

        client.archive_message("default", 1)
        expect(call_count).to eq(2)
      end

      it "retries delete_batch" do
        call_count = 0
        allow(mock_pgmq).to receive(:delete_batch) do |_q, ids|
          call_count += 1
          raise(PGMQ::Errors::ConnectionError, stale_msg) if call_count == 1

          ids.map(&:to_s)
        end

        client.delete_batch("default", [1, 2])
        expect(call_count).to eq(2)
      end

      it "retries archive_batch" do
        call_count = 0
        allow(mock_pgmq).to receive(:archive_batch) do |_q, ids|
          call_count += 1
          raise(PGMQ::Errors::ConnectionError, stale_msg) if call_count == 1

          ids.map(&:to_s)
        end

        client.archive_batch("default", [1, 2])
        expect(call_count).to eq(2)
      end

      it "retries set_visibility_timeout" do
        call_count = 0
        allow(mock_pgmq).to receive(:set_vt) do
          call_count += 1
          raise(PGMQ::Errors::ConnectionError, stale_msg) if call_count == 1

          nil
        end

        client.set_visibility_timeout("default", 1, vt: 30)
        expect(call_count).to eq(2)
      end

      it "retries transaction (caller block re-runs on pre-flight error)" do
        call_count = 0
        block_count = 0
        # @pgmq.transaction raises before yielding on first attempt,
        # then yields on second — matches pre-flight semantics.
        allow(mock_pgmq).to receive(:transaction) do |&blk|
          call_count += 1
          raise(PGMQ::Errors::ConnectionError, stale_msg) if call_count == 1

          blk.call(mock_pgmq)
        end

        client.transaction { |_txn| block_count += 1 }

        expect(call_count).to eq(2)
        expect(block_count).to eq(1)
      end
    end

    describe "admin operations" do
      let(:stale_msg) { "Database connection error: PQsocket() can't get socket descriptor" }

      it "retries metrics for single queue" do
        call_count = 0
        allow(mock_pgmq).to receive(:metrics) do
          call_count += 1
          raise(PGMQ::Errors::ConnectionError, stale_msg) if call_count == 1

          nil
        end

        client.metrics("default")
        expect(call_count).to eq(2)
      end

      it "retries metrics_all when no queue given" do
        call_count = 0
        allow(mock_pgmq).to receive(:metrics_all) do
          call_count += 1
          raise(PGMQ::Errors::ConnectionError, stale_msg) if call_count == 1

          []
        end

        client.metrics
        expect(call_count).to eq(2)
      end

      it "retries list_queues" do
        call_count = 0
        allow(mock_pgmq).to receive(:list_queues) do
          call_count += 1
          raise(PGMQ::Errors::ConnectionError, stale_msg) if call_count == 1

          []
        end

        client.list_queues
        expect(call_count).to eq(2)
      end

      it "retries purge_queue" do
        call_count = 0
        allow(mock_pgmq).to receive(:purge_queue) do
          call_count += 1
          raise(PGMQ::Errors::ConnectionError, stale_msg) if call_count == 1

          nil
        end

        client.purge_queue("default")
        expect(call_count).to eq(2)
      end

      it "retries drop_queue and clears the memo after success" do
        call_count = 0
        allow(mock_pgmq).to receive(:drop_queue) do
          call_count += 1
          raise(PGMQ::Errors::ConnectionError, stale_msg) if call_count == 1

          true
        end
        # Populate the created-cache the public way.
        client.ensure_queue("default")

        expect(client.drop_queue("default")).to be(true)
        expect(call_count).to eq(2)
        # Memo cleared: the next ensure_queue re-creates rather than cache-hitting.
        client.ensure_queue("default")
        expect(mock_pgmq).to have_received(:create).with("pgbus_test_default").twice
      end
    end
  end

  describe "#ensure_all_queues" do
    it "creates the default queue" do
      client.ensure_all_queues

      expect(mock_pgmq).to have_received(:create).with("pgbus_test_default")
    end

    it "creates all queues from worker configs" do
      config.workers = [
        { queues: %w[default urgent], threads: 5 },
        { queues: %w[emails], threads: 2 }
      ]
      client.ensure_all_queues

      expect(mock_pgmq).to have_received(:create).with("pgbus_test_default")
      expect(mock_pgmq).to have_received(:create).with("pgbus_test_urgent")
      expect(mock_pgmq).to have_received(:create).with("pgbus_test_emails")
    end

    it "creates queues for recurring tasks" do
      config.recurring_tasks = {
        "daily_report" => { class: "ReportJob", schedule: "0 2 * * *", queue: "reports" }
      }
      client.ensure_all_queues

      expect(mock_pgmq).to have_received(:create).with("pgbus_test_reports")
    end

    it "deduplicates queues" do
      config.workers = [
        { queues: %w[default], threads: 5 },
        { queues: %w[default], threads: 3 }
      ]
      client.ensure_all_queues

      expect(mock_pgmq).to have_received(:create).with("pgbus_test_default").once
    end

    it "skips wildcard queue entries" do
      config.workers = [{ queues: %w[*], threads: 5 }]
      client.ensure_all_queues

      # Should still create default queue but not try to create "*"
      expect(mock_pgmq).to have_received(:create).with("pgbus_test_default")
      expect(mock_pgmq).not_to have_received(:create).with("pgbus_test_*")
    end

    it "creates priority sub-queues when priority is enabled" do
      config.priority_levels = 3
      client.ensure_all_queues

      expect(mock_pgmq).to have_received(:create).with("pgbus_test_default_p0")
      expect(mock_pgmq).to have_received(:create).with("pgbus_test_default_p1")
      expect(mock_pgmq).to have_received(:create).with("pgbus_test_default_p2")
      config.priority_levels = nil
    end

    it "logs the bootstrapped queues" do
      allow(Pgbus.logger).to receive(:info)
      client.ensure_all_queues

      expect(Pgbus.logger).to have_received(:info).at_least(:once)
    end
  end

  describe "#close" do
    it "closes the pgmq connection" do
      client.close

      expect(mock_pgmq).to have_received(:close)
    end

    it "closes the dedicated streams pool too so its connections don't leak (issue #315)" do
      job_pgmq = build_mock_pgmq
      streams_pgmq = build_mock_pgmq
      allow(PGMQ::Client).to receive(:new).and_return(job_pgmq, streams_pgmq)
      dedicated = described_class.new(config, schema_ensured: true)

      dedicated.close

      expect(job_pgmq).to have_received(:close)
      expect(streams_pgmq).to have_received(:close)
    end

    it "does not double-close on the shared-AR path where the streams pool aliases the job pool" do
      allow(config).to receive(:connection_options).and_return(-> { double("PG::Connection") })
      allow(PGMQ::Client).to receive(:new).and_return(mock_pgmq)
      shared = described_class.new(config, schema_ensured: true)

      shared.close

      expect(mock_pgmq).to have_received(:close).once
    end
  end

  describe "connection pooling strategy" do
    it "uses configured pool_size when database_url is set (dedicated connections)" do
      url_config = Pgbus::Configuration.new.tap do |c|
        c.database_url = "postgres://localhost/pgbus_test"
        c.connection_params = nil
        c.pool_size = 5
        c.queue_prefix = "pgbus_test"
      end

      allow(PGMQ::Client).to receive(:new).and_return(mock_pgmq)
      described_class.new(url_config)

      # Connection bounds (statement_timeout + keepalives) are asserted in detail
      # in the "libpq connection bounds" describe below; here we only pin pool sizing.
      # at_least(:once): the dedicated path builds two pools (job + streams,
      # issue #315). Here streams_pool_size defaults to 5, colliding with the
      # job pool_size, so both .new calls match this matcher.
      expect(PGMQ::Client).to have_received(:new).with(
        a_string_starting_with("postgres://localhost/pgbus_test?"),
        pool_size: 5,
        pool_timeout: url_config.pool_timeout
      ).at_least(:once)
    end

    it "uses configured pool_size when connection_params is set (dedicated connections)" do
      url_config = Pgbus::Configuration.new.tap do |c|
        c.database_url = nil
        c.connection_params = { host: "localhost", dbname: "pgbus_test" }
        c.pool_size = 3
        c.queue_prefix = "pgbus_test"
      end

      allow(PGMQ::Client).to receive(:new).and_return(mock_pgmq)
      described_class.new(url_config)

      expect(PGMQ::Client).to have_received(:new).with(
        hash_including(host: "localhost", dbname: "pgbus_test"),
        pool_size: 3,
        pool_timeout: url_config.pool_timeout
      )
    end

    it "forces pool_size=1 when connection_options falls back to Proc (shared connection)" do
      lambda_config = Pgbus::Configuration.new.tap do |c|
        c.database_url = nil
        c.connection_params = nil
        c.pool_size = 5
        c.queue_prefix = "pgbus_test"
      end

      # Simulate the Rails path where AR config extraction fails, falling back to Proc
      raw_conn = double("PG::Connection")
      ar_connection = double("AR::ConnectionAdapter", raw_connection: raw_conn)
      ar_base = double("AR::Base", connection: ar_connection)
      allow(ar_base).to receive(:connection_db_config).and_raise(StandardError, "no config")
      stub_const("ActiveRecord::Base", ar_base)

      allow(PGMQ::Client).to receive(:new).and_return(mock_pgmq)
      described_class.new(lambda_config)

      expect(PGMQ::Client).to have_received(:new).with(
        an_instance_of(Proc),
        pool_size: 1,
        pool_timeout: lambda_config.pool_timeout
      )
    end

    it "does not use mutex synchronization for dedicated connections" do
      config.database_url = "postgres://localhost/pgbus_test"
      config.pool_size = 5

      # With dedicated connections, operations should not go through the mutex.
      expect(client.shared_connection?).to be(false)
    end

    it "uses mutex synchronization when falling back to shared (Proc) connections" do
      lambda_config = Pgbus::Configuration.new.tap do |c|
        c.database_url = nil
        c.connection_params = nil
        c.pool_size = 5
        c.queue_prefix = "pgbus_test"
      end

      raw_conn = double("PG::Connection")
      ar_connection = double("AR::ConnectionAdapter", raw_connection: raw_conn)
      ar_base = double("AR::Base", connection: ar_connection)
      allow(ar_base).to receive(:connection_db_config).and_raise(StandardError, "no config")
      stub_const("ActiveRecord::Base", ar_base)

      allow(PGMQ::Client).to receive(:new).and_return(mock_pgmq)
      shared_client = described_class.new(lambda_config, schema_ensured: true)

      expect(shared_client.shared_connection?).to be(true)
    end
  end

  describe "connection-health circuit breaker (issue #197)" do
    before do
      real_pgmq_connection_error
    end

    # Force the client's in-memory breaker into the open state so reads fail
    # fast. The latch itself is unit-tested in connection_health_spec; here we
    # only assert the Client wiring (which paths are gated, which aren't).
    def open_breaker!
      health = client.connection_health
      Pgbus::Client::ConnectionHealth::OPEN_THRESHOLD.times do
        health.run_guarded { raise PGMQ::Errors::ConnectionError, "no connection to the server" }
      rescue PGMQ::Errors::ConnectionError
        nil
      end
      expect(health).to be_open
    end

    context "when the breaker is open" do
      before { open_breaker! }

      %i[read_message read_batch read_multi read_grouped read_grouped_rr read_grouped_head].each do |method|
        it "fails #{method} fast without touching the pool" do
          call =
            case method
            when :read_message then -> { client.read_message("default") }
            when :read_multi then -> { client.read_multi(%w[default], qty: 5) }
            else -> { client.public_send(method, "default", qty: 5) }
            end

          expect { call.call }.to raise_error(Pgbus::ConnectionCircuitOpenError)
          expect(mock_pgmq).not_to have_received(method == :read_message ? :read : method)
        end
      end

      it "fails read_batch_prioritized fast (non-priority delegates to read_batch)" do
        expect { client.read_batch_prioritized("default", qty: 5) }
          .to raise_error(Pgbus::ConnectionCircuitOpenError)
      end

      it "does NOT short-circuit send_message (enqueues must surface failures)" do
        expect { client.send_message("default", { "k" => "v" }) }.not_to raise_error
        expect(mock_pgmq).to have_received(:produce)
      end

      it "does NOT short-circuit send_batch" do
        expect { client.send_batch("default", [{ "k" => "v" }]) }.not_to raise_error
        expect(mock_pgmq).to have_received(:produce_batch)
      end
    end

    context "when the breaker is closed (healthy)" do
      it "passes reads through to pgmq" do
        allow(mock_pgmq).to receive(:read_batch).and_return([])

        expect(client.read_batch("default", qty: 5)).to eq([])
        expect(mock_pgmq).to have_received(:read_batch)
      end
    end

    context "when a read raises a ConnectionError" do
      it "records the failure and re-raises (does not swallow)" do
        allow(mock_pgmq).to receive(:read_batch)
          .and_raise(PGMQ::Errors::ConnectionError, "no connection to the server")

        Pgbus::Client::ConnectionHealth::OPEN_THRESHOLD.times do
          expect { client.read_batch("default", qty: 5) }.to raise_error(PGMQ::Errors::ConnectionError)
        end

        # After OPEN_THRESHOLD connection errors the breaker is open: the next
        # read fails fast with the circuit error instead of re-hitting pgmq.
        expect { client.read_batch("default", qty: 5) }.to raise_error(Pgbus::ConnectionCircuitOpenError)
      end
    end
  end

  describe "libpq connection bounds (issue #198)" do
    before do
      real_pgmq_connection_error
      allow(PGMQ::Client).to receive(:new).and_return(mock_pgmq)
      # PG (the pg gem) is loaded via pgmq-ruby in production; these unit specs
      # mock PGMQ, so stub the one PG method the read-bounds gate consults.
      stub_pg_library_version
    end

    def build_client(config)
      c = described_class.new(config, schema_ensured: true)
      allow(c).to receive(:notify_trigger_current?).and_return(false)
      c
    end

    context "when connection_options is a Hash" do
      it "merges statement_timeout, keepalives, and tcp_user_timeout sized from read_timeout" do
        hash_config = Pgbus::Configuration.new.tap do |c|
          c.database_url = nil
          c.connection_params = { host: "localhost", dbname: "pgbus_test" }
          c.read_timeout = 10
          c.queue_prefix = "pgbus_test"
        end

        build_client(hash_config)

        # statement_timeout = read_timeout; tcp_user_timeout = read_timeout + slack (5s).
        # at_least(:once): job + streams pools both get the same bounded opts.
        expect(PGMQ::Client).to have_received(:new).with(
          hash_including(
            host: "localhost",
            dbname: "pgbus_test",
            options: "-c statement_timeout=10000",
            keepalives: 1,
            tcp_user_timeout: 15_000
          ),
          anything
        ).at_least(:once)
      end

      it "preserves a caller-supplied :options instead of clobbering it" do
        hash_config = Pgbus::Configuration.new.tap do |c|
          c.database_url = nil
          c.connection_params = { host: "localhost", options: "-c search_path=myapp" }
          c.read_timeout = 10
          c.queue_prefix = "pgbus_test"
        end

        build_client(hash_config)

        expect(PGMQ::Client).to have_received(:new).with(
          hash_including(options: "-c search_path=myapp -c statement_timeout=10000"),
          anything
        ).at_least(:once)
      end
    end

    context "when libpq is older than 12 (rejects the tcp_user_timeout keyword)" do
      before { allow(PG).to receive(:library_version).and_return(110_000) }

      it "keeps statement_timeout but omits keepalives / tcp_user_timeout (Hash)" do
        hash_config = Pgbus::Configuration.new.tap do |c|
          c.database_url = nil
          c.connection_params = { host: "localhost", dbname: "pgbus_test" }
          c.read_timeout = 5
          c.queue_prefix = "pgbus_test"
        end

        build_client(hash_config)

        expect(PGMQ::Client).to have_received(:new).with(
          hash_including(options: "-c statement_timeout=5000"),
          anything
        ).at_least(:once)
        expect(PGMQ::Client).not_to have_received(:new).with(
          hash_including(:tcp_user_timeout),
          anything
        )
      end

      it "keeps statement_timeout but omits the socket keywords (URI)" do
        url_config = Pgbus::Configuration.new.tap do |c|
          c.database_url = "postgres://localhost/pgbus_test"
          c.read_timeout = 5
          c.queue_prefix = "pgbus_test"
        end

        build_client(url_config)

        expect(PGMQ::Client).to have_received(:new).with(
          "postgres://localhost/pgbus_test?options=-c%20statement_timeout%3D5000",
          anything
        ).at_least(:once)
      end
    end

    context "when connection_options is a URI string without a query" do
      it "appends keepalives + tcp_user_timeout and the URL-encoded statement_timeout with ?" do
        url_config = Pgbus::Configuration.new.tap do |c|
          c.database_url = "postgres://localhost/pgbus_test"
          c.read_timeout = 5
          c.queue_prefix = "pgbus_test"
        end

        build_client(url_config)

        expect(PGMQ::Client).to have_received(:new).with(
          "postgres://localhost/pgbus_test?keepalives=1&keepalives_idle=30" \
          "&keepalives_interval=10&keepalives_count=3&tcp_user_timeout=10000" \
          "&options=-c%20statement_timeout%3D5000",
          anything
        ).at_least(:once)
      end
    end

    context "when connection_options is a URI string with an existing query" do
      it "appends with & so the existing query is preserved" do
        url_config = Pgbus::Configuration.new.tap do |c|
          c.database_url = "postgresql://localhost/pgbus_test?sslmode=require"
          c.read_timeout = 5
          c.queue_prefix = "pgbus_test"
        end

        build_client(url_config)

        expect(PGMQ::Client).to have_received(:new).with(
          a_string_starting_with("postgresql://localhost/pgbus_test?sslmode=require&keepalives=1")
            .and(ending_with("&options=-c%20statement_timeout%3D5000")),
          anything
        ).at_least(:once)
      end
    end

    context "when connection_options is a key=value conninfo string" do
      it "appends space-separated keepalives + tcp_user_timeout and a quoted options clause" do
        conninfo_config = Pgbus::Configuration.new.tap do |c|
          c.database_url = "host=localhost dbname=pgbus_test"
          c.read_timeout = 5
          c.queue_prefix = "pgbus_test"
        end

        build_client(conninfo_config)

        expect(PGMQ::Client).to have_received(:new).with(
          "host=localhost dbname=pgbus_test keepalives=1 keepalives_idle=30 " \
          "keepalives_interval=10 keepalives_count=3 tcp_user_timeout=10000 " \
          "options='-c statement_timeout=5000'",
          anything
        ).at_least(:once)
      end
    end

    context "when read_timeout is nil" do
      it "passes connection options through unchanged (bounding disabled)" do
        no_timeout_config = Pgbus::Configuration.new.tap do |c|
          c.database_url = "postgres://localhost/pgbus_test"
          c.read_timeout = nil
          c.queue_prefix = "pgbus_test"
        end

        build_client(no_timeout_config)

        expect(PGMQ::Client).to have_received(:new).with(
          "postgres://localhost/pgbus_test",
          anything
        ).at_least(:once)
      end
    end

    context "when connection_options is a Proc (shared AR connection)" do
      it "passes the Proc through unchanged — pgbus does not own that connection" do
        lambda_config = Pgbus::Configuration.new.tap do |c|
          c.database_url = nil
          c.connection_params = nil
          c.read_timeout = 5
          c.queue_prefix = "pgbus_test"
        end

        ar_connection = double("AR::ConnectionAdapter", raw_connection: double("PG::Connection"))
        ar_base = double("AR::Base", connection: ar_connection)
        allow(ar_base).to receive(:connection_db_config).and_raise(StandardError, "no config")
        stub_const("ActiveRecord::Base", ar_base)

        build_client(lambda_config)

        expect(PGMQ::Client).to have_received(:new).with(an_instance_of(Proc), anything)
      end

      it "logs a one-time hint to configure libpq timeouts in database.yml" do
        lambda_config = Pgbus::Configuration.new.tap do |c|
          c.database_url = nil
          c.connection_params = nil
          c.read_timeout = 5
          c.queue_prefix = "pgbus_test"
        end

        ar_connection = double("AR::ConnectionAdapter", raw_connection: double("PG::Connection"))
        ar_base = double("AR::Base", connection: ar_connection)
        allow(ar_base).to receive(:connection_db_config).and_raise(StandardError, "no config")
        stub_const("ActiveRecord::Base", ar_base)

        # The Proc-fallback path also logs its own warn, so capture all warns and
        # assert that at least one carries the database.yml hint.
        warnings = []
        logger = double("Pgbus.logger")
        allow(logger).to receive(:warn) { |&block| warnings << block.call if block }
        allow(Pgbus).to receive(:logger).and_return(logger)

        build_client(lambda_config)

        expect(warnings).to include(a_string_matching(/database\.yml/))
      end
    end

    describe "#libpq_read_bounds_effective? (whether Ruby Timeout is needed)" do
      def bounds_effective_for(config)
        build_client(config).send(:libpq_read_bounds_effective?)
      end

      let(:dedicated_config) do
        Pgbus::Configuration.new.tap do |c|
          c.database_url = "postgres://localhost/pgbus_test"
          c.read_timeout = 5
          c.queue_prefix = "pgbus_test"
        end
      end

      it "is true on a dedicated connection when the platform + libpq support tcp_user_timeout" do
        skip "host libpq/socket lacks TCP_USER_TIMEOUT" unless Socket.const_defined?(:TCP_USER_TIMEOUT) &&
                                                               PG.library_version >= 120_000

        expect(bounds_effective_for(dedicated_config)).to be(true)
      end

      it "is false when TCP_USER_TIMEOUT is unavailable (non-Linux hosts)" do
        hide_const("Socket::TCP_USER_TIMEOUT") if Socket.const_defined?(:TCP_USER_TIMEOUT)

        expect(bounds_effective_for(dedicated_config)).to be(false)
      end

      it "is false when read_timeout is nil (no libpq bound is installed)" do
        dedicated_config.read_timeout = nil

        expect(bounds_effective_for(dedicated_config)).to be(false)
      end

      it "is false when libpq is older than 12 (rejects tcp_user_timeout keyword)" do
        allow(PG).to receive(:library_version).and_return(110_000)

        expect(bounds_effective_for(dedicated_config)).to be(false)
      end
    end

    describe "server-side cancellation mapping" do
      let(:timeout_msg) { "PG::QueryCanceled: ERROR: canceling statement due to statement timeout" }

      it "maps a statement-timeout ConnectionError on read_batch to ReadTimeoutError" do
        allow(mock_pgmq).to receive(:read_batch)
          .and_raise(PGMQ::Errors::ConnectionError, timeout_msg)

        expect { client.read_batch("default", qty: 5) }
          .to raise_error(Pgbus::ReadTimeoutError, /statement timeout/)
      end

      it "maps a statement-timeout ConnectionError on read_message to ReadTimeoutError" do
        allow(mock_pgmq).to receive(:read)
          .and_raise(PGMQ::Errors::ConnectionError, timeout_msg)

        expect { client.read_message("default") }
          .to raise_error(Pgbus::ReadTimeoutError)
      end

      it "does not map a non-timeout ConnectionError" do
        allow(mock_pgmq).to receive(:read_batch)
          .and_raise(PGMQ::Errors::ConnectionError, "no connection to the server")

        expect { client.read_batch("default", qty: 5) }
          .to raise_error(PGMQ::Errors::ConnectionError)
      end
    end
  end
end
