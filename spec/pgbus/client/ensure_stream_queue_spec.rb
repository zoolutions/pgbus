# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Client::EnsureStreamQueue do
  subject(:client) do
    allow(PGMQ::Client).to receive(:new).and_return(mock_pgmq)
    c = Pgbus::Client.new(config)
    c.instance_variable_set(:@schema_ensured, true)
    c
  end

  before do
    # Client#initialize probes PG.library_version when read_timeout is set
    # (default 30s) and TCP_USER_TIMEOUT exists. Unit specs don't load libpq.
    stub_pg_library_version
    # Stub the class method that loads pgmq so the faked PGMQ::Client stands;
    # a clean per-example stub, unlike stubbing global Kernel#require.
    allow(Pgbus::Client).to receive(:load_pgmq_gem!)
    stub_const("PGMQ::Client", Class.new do
      def initialize(*args, **kwargs); end
    end)
    allow(client).to receive(:with_raw_connection).and_yield(raw_conn)
    allow(client).to receive(:ensure_single_queue)
    # Stub notify trigger check — runs raw SQL which needs a real PG connection.
    allow(client).to receive(:notify_trigger_current?).and_return(false)
    allow(Pgbus::StreamQueue).to receive(:record!) { |_name, &on_durable| on_durable&.call || true }
  end

  let(:config) do
    Pgbus::Configuration.new.tap do |c|
      c.database_url = "postgres://localhost/pgbus_test"
      c.queue_prefix = "pgbus_test"
    end
  end
  let(:mock_pgmq) { build_mock_pgmq }
  let(:raw_conn)  { double("raw_conn", exec: nil) }

  describe "#ensure_stream_queue" do
    it "creates the BARE queue directly, bypassing the priority strategy" do
      # Must use ensure_single_queue on the bare physical name, NOT
      # ensure_queue (which fans out to the priority strategy's _p0.._pN
      # sub-queues under priority_levels>1, leaving the bare queue — the one
      # the streamer reads and NOTIFYs on — uncreated. Issue #310.
      client.ensure_stream_queue("chat")
      expect(client).to have_received(:ensure_single_queue).with("pgbus_test_chat")
    end

    it "creates the bare queue even when priority_levels > 1" do
      # Build a client whose strategy is PriorityStrategy (memoized at
      # construction), so this genuinely exercises the #310 scenario rather
      # than relying on the bare-name path the default client already takes.
      priority_config = Pgbus::Configuration.new.tap do |c|
        c.database_url = "postgres://localhost/pgbus_test"
        c.queue_prefix = "pgbus_test"
        c.priority_levels = 3
      end
      priority_client = Pgbus::Client.new(priority_config)
      priority_client.instance_variable_set(:@schema_ensured, true)
      allow(priority_client).to receive_messages(
        with_raw_connection: nil, ensure_single_queue: nil, notify_trigger_current?: false
      )
      allow(priority_client).to receive(:with_raw_connection).and_yield(raw_conn)

      priority_client.ensure_stream_queue("chat")

      expect(priority_client).to have_received(:ensure_single_queue).with("pgbus_test_chat")
      expect(priority_client).not_to have_received(:ensure_single_queue).with(a_string_matching(/_p\d+$/))
    end

    it "records the physical queue name in the StreamQueue registry" do
      client.ensure_stream_queue("chat")

      expect(Pgbus::StreamQueue).to have_received(:record!).with("pgbus_test_chat")
    end

    # A registration inside the caller's open transaction can still roll back,
    # so it is not memoized: the next ensure records it again.
    # Issue #695: the registry row is memoized by the block record! runs once
    # it is written, at once or after the caller's commit (never on a
    # rollback); the archive index has its own memo and is not redone.
    it "records again until the registry row is written, without redoing the index" do
      hooks = []
      allow(Pgbus::StreamQueue).to receive(:record!) do |_name, &on_durable|
        hooks << on_durable
        :pending
      end
      allow(raw_conn).to receive(:exec)

      expect(client.ensure_stream_queue("chat")).to be(false)
      expect(client.ensure_stream_queue("chat")).to be(false)
      hooks.first.call
      expect(client.ensure_stream_queue("chat")).to be(true)

      expect(Pgbus::StreamQueue).to have_received(:record!).with("pgbus_test_chat").twice
      expect(raw_conn).to have_received(:exec).with(a_string_matching(/CREATE INDEX/)).once
    end

    # An unmigrated install (no pgbus_stream_queues) or a failing insert is
    # memoized too, as before: not retried on every broadcast.
    it "memoizes when record! returns false" do
      allow(Pgbus::StreamQueue).to receive(:record!).and_return(false)
      allow(raw_conn).to receive(:exec)

      client.ensure_stream_queue("chat")
      expect(client.ensure_stream_queue("chat")).to be(true)

      expect(Pgbus::StreamQueue).to have_received(:record!).once
      expect(raw_conn).to have_received(:exec).with(a_string_matching(/CREATE INDEX/)).once
    end

    # On the shared ActiveRecord connection the index DDL rides the caller's
    # transaction too (the @queues_created rule in Client).
    it "redoes the index while its DDL rides the caller's transaction" do
      allow(client).to receive(:queue_ddl_rides_caller_transaction?).and_return(true, false)
      allow(raw_conn).to receive(:exec)

      expect(client.ensure_stream_queue("chat")).to be(false)
      expect(client.ensure_stream_queue("chat")).to be(true)
      client.ensure_stream_queue("chat")

      expect(raw_conn).to have_received(:exec).with(a_string_matching(/CREATE INDEX/)).twice
      expect(Pgbus::StreamQueue).to have_received(:record!).once
    end

    it "records the registry entry only once per stream per process" do
      allow(raw_conn).to receive(:exec)

      client.ensure_stream_queue("chat")
      client.ensure_stream_queue("chat")

      expect(Pgbus::StreamQueue).to have_received(:record!).with("pgbus_test_chat").once
    end

    it "creates the msg_id index on the archive table" do
      client.ensure_stream_queue("chat")

      expect(raw_conn).to have_received(:exec)
        .with(a_string_matching(/CREATE INDEX IF NOT EXISTS\s+a_pgbus_test_chat_msg_id_idx\s+ON pgmq\.a_pgbus_test_chat\s*\(msg_id\)/m))
    end

    it "is idempotent — calling twice skips the second CREATE INDEX via per-process memoization" do
      allow(raw_conn).to receive(:exec)

      client.ensure_stream_queue("chat")
      client.ensure_stream_queue("chat")

      # ensure_single_queue still runs twice — its own memoization is on a
      # different layer (@queues_created) and this spec mocks it out.
      # The CREATE INDEX SQL only runs on the first call because we
      # now memoize on @stream_indexes_created.
      expect(raw_conn).to have_received(:exec).once
      expect(client).to have_received(:ensure_single_queue).twice
    end

    it "memoizes index creation per stream, not globally" do
      allow(raw_conn).to receive(:exec)

      client.ensure_stream_queue("chat")
      client.ensure_stream_queue("orders")

      expect(raw_conn).to have_received(:exec).twice
    end

    it "normalizes stream names with invalid characters via QueueNameValidator" do
      received_sql = nil
      allow(raw_conn).to receive(:exec) do |sql|
        received_sql = sql
      end

      expect { client.ensure_stream_queue("nope; DROP TABLE") }.not_to raise_error
      expect(received_sql).to match(/pgmq\.a_pgbus_test_nopeDROPTABLE/i)
      expect(received_sql).not_to include(";")
    end

    # Same herd, third DDL step (issue #404): the archive msg_id index is a
    # raw CREATE INDEX IF NOT EXISTS, so two first-broadcasts race the
    # catalog insert and the loser gets a raw PG::UniqueViolation.
    context "when a concurrent ensure won the archive-index race" do
      it "treats the duplicate as success and still registers the stream" do
        stub_const("PG::UniqueViolation", Class.new(StandardError))
        allow(raw_conn).to receive(:exec).and_raise(
          PG::UniqueViolation,
          'ERROR:  duplicate key value violates unique constraint "pg_class_relname_nsp_index"'
        )

        expect { client.ensure_stream_queue("chat") }.not_to raise_error
        expect(Pgbus::StreamQueue).to have_received(:record!).with("pgbus_test_chat")
        expect(client.instance_variable_get(:@stream_indexes_created)["chat"]).to be(true)
      end

      it "propagates non-duplicate index errors" do
        stub_const("PG::InsufficientPrivilege", Class.new(StandardError))
        allow(raw_conn).to receive(:exec).and_raise(
          PG::InsufficientPrivilege, "permission denied for schema pgmq"
        )

        expect { client.ensure_stream_queue("chat") }.to raise_error(PG::InsufficientPrivilege)
        expect(client.instance_variable_get(:@stream_indexes_created)["chat"]).to be_nil
      end
    end

    # Deploy-time thundering herd (issue #403): two processes with cold memos
    # ensure the same lazy stream queue concurrently; the loser's CREATE
    # CONSTRAINT TRIGGER inside pgmq.enable_notify_insert fails with
    # PG::DuplicateObject once the winner commits. The trigger provably
    # exists, so the broadcast must proceed instead of dropping.
    context "when a concurrent ensure won the NOTIFY trigger race" do
      it "treats the duplicate-trigger error as success" do
        real_pgmq_connection_error
        allow(client).to receive(:notify_trigger_current?).and_return(false, true)
        allow(mock_pgmq).to receive(:enable_notify_insert).and_raise(
          PGMQ::Errors::ConnectionError,
          'Database connection error: ERROR:  trigger "trigger_notify_queue_insert_listeners" ' \
          'for relation "q_pgbus_test_chat" already exists'
        )

        expect { client.ensure_stream_queue("chat") }.not_to raise_error
        expect(mock_pgmq).to have_received(:enable_notify_insert)
          .with("pgbus_test_chat", throttle_interval_ms: 0).once
      end
    end

    # Stale process-local memo after another process drops the physical queue
    # (orphan stream sweep, dashboard drop, manual drop_queue). Without recovery
    # enable_notify_insert raises "Queue does not exist" even though ensure
    # already ran — the memo skipped pgmq.create.
    context "when the process-local queue memo is stale" do
      let(:full_name) { "pgbus_test_chat" }
      let(:missing_queue_message) do
        'Queue "pgbus_test_chat" does not exist. Create it first using pgmq.create()'
      end

      before do
        real_pgmq_connection_error
        client.instance_variable_get(:@queues_created)[full_name] = true
        client.instance_variable_get(:@stream_indexes_created)["chat"] = true
      end

      it "clears the memo and recreates the queue when enable_notify reports it missing" do
        calls = 0
        allow(mock_pgmq).to receive(:enable_notify_insert) do
          calls += 1
          raise PGMQ::Errors::ConnectionError, missing_queue_message if calls == 1

          nil
        end

        expect { client.ensure_stream_queue("chat") }.not_to raise_error

        expect(client).to have_received(:ensure_single_queue).with(full_name).twice
        expect(client.instance_variable_get(:@queues_created)[full_name]).to be_nil
        expect(client.instance_variable_get(:@stream_indexes_created)["chat"]).to be(true)
        expect(calls).to eq(2)
      end

      it "re-raises unrelated ConnectionErrors without clearing the memo or retrying" do
        # Use a non-stale ConnectionError so with_stale_connection_retry does not
        # wrap this path (PQsocket / server-closed match the stale patterns and
        # would re-enter ensure_single_queue on their own).
        allow(mock_pgmq).to receive(:enable_notify_insert)
          .and_raise(PGMQ::Errors::ConnectionError, "permission denied for schema pgmq")

        expect { client.ensure_stream_queue("chat") }
          .to raise_error(PGMQ::Errors::ConnectionError, /permission denied/)

        expect(client).to have_received(:ensure_single_queue).with(full_name).once
        expect(client.instance_variable_get(:@queues_created)[full_name]).to be(true)
      end
    end

    # Residual of #403: two processes both see "notify trigger not current"
    # and both run DROP TRIGGER under AccessExclusiveLock. 0.16.1 skips DROP
    # when the trigger is current and treats a concurrent CREATE as success,
    # but the TOCTOU window still deadlocks (or hits lock_timeout /
    # statement_timeout-while-locking). getzazu/app#3817 / #3827.
    context "when enable_notify_insert deadlocks on the notify-insert race" do
      def deadlock_error
        PGMQ::Errors::ConnectionError.new(
          "Database connection error: ERROR:  deadlock detected\n" \
          "DETAIL:  Process 1 waits for AccessExclusiveLock on relation 3487593"
        )
      end

      def lock_timeout_error
        PGMQ::Errors::ConnectionError.new(
          "Database connection error: ERROR:  canceling statement due to lock timeout"
        )
      end

      def lock_not_available_error
        PGMQ::Errors::ConnectionError.new(
          "Database connection error: ERROR:  lock not available"
        )
      end

      def statement_timeout_lock_error
        PGMQ::Errors::ConnectionError.new(
          "Database connection error: ERROR:  canceling statement due to statement timeout\n" \
          "CONTEXT:  while locking tuple"
        )
      end

      before do
        real_pgmq_connection_error
        allow(client).to receive(:sleep)
      end

      it "retries the deadlock and succeeds on the next attempt" do
        calls = 0
        allow(mock_pgmq).to receive(:enable_notify_insert) do
          calls += 1
          raise deadlock_error if calls == 1

          nil
        end

        expect { client.ensure_stream_queue("chat") }.not_to raise_error
        expect(calls).to eq(2)
        expect(client).to have_received(:sleep).with(Pgbus::Client::NotifyLockRetry::DELAYS.first).once
        expect(Pgbus::StreamQueue).to have_received(:record!).with("pgbus_test_chat")
      end

      it "retries lock-timeout, lock-not-available, and statement-timeout lock waits" do
        [lock_timeout_error, lock_not_available_error, statement_timeout_lock_error].each do |error|
          calls = 0
          allow(mock_pgmq).to receive(:enable_notify_insert) do
            calls += 1
            raise error if calls == 1

            nil
          end

          expect { client.ensure_stream_queue("chat") }.not_to raise_error
          expect(calls).to eq(2)
        end
      end

      it "does not retry a bare statement timeout" do
        calls = 0
        allow(mock_pgmq).to receive(:enable_notify_insert) do
          calls += 1
          raise PGMQ::Errors::ConnectionError,
                "Database connection error: ERROR:  canceling statement due to statement timeout"
        end

        expect { client.ensure_stream_queue("chat") }
          .to raise_error(PGMQ::Errors::ConnectionError, /statement timeout/)
        expect(calls).to eq(1)
        expect(client).not_to have_received(:sleep)
      end

      it "does not add lock-retries on a missing-queue ConnectionError" do
        # ensure_stream_queue_tables already recovers missing-queue once.
        # Lock retry must not multiply that into ATTEMPTS extra create cycles.
        missing = PGMQ::Errors::ConnectionError.new(
          'Queue "pgbus_test_chat" does not exist. Create it first using pgmq.create()'
        )
        allow(mock_pgmq).to receive(:enable_notify_insert).and_raise(missing)

        expect { client.ensure_stream_queue("chat") }
          .to raise_error(PGMQ::Errors::ConnectionError, /does not exist/)
        expect(mock_pgmq).to have_received(:enable_notify_insert).twice
        expect(client).not_to have_received(:sleep)
      end

      it "lock-retries a deadlock after missing-queue recovery without re-running recreate" do
        missing = PGMQ::Errors::ConnectionError.new(
          'Queue "pgbus_test_chat" does not exist. Create it first using pgmq.create()'
        )
        calls = 0
        allow(mock_pgmq).to receive(:enable_notify_insert) do
          calls += 1
          raise missing if calls == 1
          raise deadlock_error if calls == 2

          nil
        end
        allow(client).to receive(:forget_stream_queue_memo!).and_call_original

        expect { client.ensure_stream_queue("chat") }.not_to raise_error
        expect(calls).to eq(3)
        expect(client).to have_received(:sleep).once
        # Recovery (forget + recreate) stays outside the lock-retry budget.
        # The third ensure_single_queue is the lock-retry of recovery setup,
        # not a second forget+recreate.
        expect(client).to have_received(:forget_stream_queue_memo!).once
        expect(client).to have_received(:ensure_single_queue).exactly(3).times
      end

      it "re-raises after the retry budget is exhausted" do
        allow(mock_pgmq).to receive(:enable_notify_insert).and_raise(deadlock_error)

        expect { client.ensure_stream_queue("chat") }
          .to raise_error(PGMQ::Errors::ConnectionError, /deadlock detected/)
        expect(mock_pgmq).to have_received(:enable_notify_insert)
          .exactly(Pgbus::Client::NotifyLockRetry::ATTEMPTS).times
        expect(client).to have_received(:sleep)
          .exactly(Pgbus::Client::NotifyLockRetry::ATTEMPTS - 1).times
      end

      it "retries a deadlock on the create-path 250ms notify enable" do
        # ensure_single_queue → create_queue_physically enables notify at
        # NOTIFY_THROTTLE_MS before the stream override to 0ms. That first
        # DROP TRIGGER can deadlock the same way.
        allow(client).to receive(:ensure_single_queue).and_call_original
        allow(mock_pgmq).to receive(:create)
        calls = 0
        allow(mock_pgmq).to receive(:enable_notify_insert) do
          calls += 1
          raise deadlock_error if calls == 1

          nil
        end

        expect { client.ensure_stream_queue("chat") }.not_to raise_error
        expect(calls).to be >= 2
        expect(client).to have_received(:sleep).with(Pgbus::Client::NotifyLockRetry::DELAYS.first).once
      end
    end

    context "when a shared-connection client retries a notify lock failure" do
      subject(:shared_client) do
        allow(PGMQ::Client).to receive(:new).and_return(mock_pgmq)
        c = Pgbus::Client.new(shared_config)
        c.instance_variable_set(:@schema_ensured, true)
        allow(c).to receive(:ensure_single_queue)
        allow(c).to receive(:notify_trigger_current?).and_return(false)
        allow(c).to receive(:with_raw_connection).and_yield(raw_conn)
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
        real_pgmq_connection_error
        raw = double("PG::Connection")
        ar_connection = double("AR::ConnectionAdapter", raw_connection: raw)
        ar_base = double("AR::Base", connection: ar_connection)
        allow(ar_base).to receive(:connection_db_config).and_raise(StandardError, "no config")
        stub_const("ActiveRecord::Base", ar_base)
      end

      it "does not hold the connection mutex while sleeping between lock retries" do
        expect(shared_client.shared_connection?).to be(true)

        locked_during_sleep = []
        allow(shared_client).to receive(:sleep) { locked_during_sleep << shared_client.synchronizing? }

        calls = 0
        allow(mock_pgmq).to receive(:enable_notify_insert) do
          calls += 1
          raise PGMQ::Errors::ConnectionError, "Database connection error: ERROR:  deadlock detected" if calls == 1

          nil
        end

        expect { shared_client.ensure_stream_queue("chat") }.not_to raise_error
        expect(locked_during_sleep).to eq([false])
      end

      it "re-raises the original lock error when the caller transaction is aborted" do
        # A deadlock inside an open AR transaction leaves the shared
        # connection in PQTRANS_INERROR. Retrying on that socket cannot
        # recover and would mask the deadlock with "current transaction
        # is aborted".
        stub_const("PG::PQTRANS_IDLE", 0)
        allow(raw_conn).to receive(:respond_to?) { |name, *| name.to_sym == :transaction_status }
        allow(raw_conn).to receive(:transaction_status).and_return(3)
        allow(shared_client).to receive(:sleep)
        allow(mock_pgmq).to receive(:enable_notify_insert)
          .and_raise(PGMQ::Errors::ConnectionError, "Database connection error: ERROR:  deadlock detected")

        expect { shared_client.ensure_stream_queue("chat") }
          .to raise_error(PGMQ::Errors::ConnectionError, /deadlock detected/)
        expect(mock_pgmq).to have_received(:enable_notify_insert).once
        expect(shared_client).not_to have_received(:sleep)
      end
    end
  end
end
