# frozen_string_literal: true

require "spec_helper"
require "json"
require "active_job"

RSpec.describe Pgbus::ActiveJob::Executor do
  subject(:executor) { described_class.new(client: mock_client, config: config) }

  let(:mock_client) { build_mock_client }
  let(:config) { Pgbus.configuration }
  let(:queue_name) { "default" }
  let(:job_id) { SecureRandom.uuid }
  let(:job_payload) do
    { "job_class" => "TestJob", "job_id" => job_id, "queue_name" => queue_name, "arguments" => [] }
  end
  let(:message_json) { JSON.generate(job_payload) }
  let(:job_double) { build_job_double(job_class: "TestJob", queue_name: queue_name, job_id: job_id) }

  before do
    allow(ActiveSupport::Notifications).to receive(:instrument).and_call_original
    allow(ActiveJob::Base).to receive(:deserialize).with(job_payload).and_return(job_double)
    # By default, no concurrency key in payloads
    allow(Pgbus::Concurrency).to receive(:extract_key).and_return(nil)
    # Stub stat recording
    stub_const("Pgbus::JobStat", Class.new)
    allow(Pgbus::JobStat).to receive_messages(record!: nil, table_exists?: true)
    # Stub failure tracking
    allow(Pgbus::FailedEventRecorder).to receive_messages(record!: nil, clear!: nil)
  end

  describe "#execute" do
    context "when job succeeds" do
      let(:message) { build_message_double(msg_id: 5, message: message_json, read_ct: 1) }

      it "deserializes, performs, archives, instruments, and returns :success" do
        result = executor.execute(message, queue_name)

        expect(ActiveJob::Base).to have_received(:deserialize).with(job_payload)
        expect(job_double).to have_received(:perform_now)
        expect(mock_client).to have_received(:archive_message).with(queue_name, 5)
        expect(ActiveSupport::Notifications).to have_received(:instrument).with("pgbus.job_completed", queue: queue_name,
                                                                                                       job_class: "TestJob")
        expect(result).to eq(:success)
      end

      it "clears any prior failed event record" do
        executor.execute(message, queue_name)

        expect(Pgbus::FailedEventRecorder).to have_received(:clear!).with(
          queue_name: queue_name, msg_id: 5
        )
      end
    end

    context "when read_ct exceeds max_retries (DLQ routing)" do
      let(:message) { build_message_double(msg_id: 7, message: message_json, read_ct: config.max_retries + 1) }

      it "moves message to dead letter queue and returns :dead_lettered" do
        result = executor.execute(message, queue_name)

        expect(mock_client).to have_received(:move_to_dead_letter).with(queue_name, message)
        expect(ActiveJob::Base).not_to have_received(:deserialize)
        expect(result).to eq(:dead_lettered)
      end

      it "clears any prior failed event record" do
        executor.execute(message, queue_name)

        expect(Pgbus::FailedEventRecorder).to have_received(:clear!).with(
          queue_name: queue_name, msg_id: 7
        )
      end
    end

    context "when a GlobalID job argument is rejected by the allowlist (issue #368)" do
      let(:order_class) { Class.new }
      let(:job_payload) do
        {
          "job_class" => "TestJob",
          "job_id" => job_id,
          "queue_name" => queue_name,
          "arguments" => [{ "_aj_globalid" => "gid://pgbus-test/Secret/1" }]
        }
      end
      let(:message) { build_message_double(msg_id: 9, message: message_json, read_ct: 1) }

      before do
        stub_const("Order", order_class)
        stub_const("Secret", Class.new)
        config.allowed_global_id_models = [order_class]
        # Serializer must run the real allowlist gate; do not stub deserialize
        # for the happy path of this context — rejection happens before it.
        allow(ActiveJob::Base).to receive(:deserialize).and_call_original
      end

      after { config.allowed_global_id_models = nil }

      it "treats SerializationError as a job failure (not process-fatal)" do
        result = executor.execute(message, queue_name)

        expect(result).to eq(:failed)
        expect(ActiveJob::Base).not_to have_received(:deserialize)
        expect(Pgbus::FailedEventRecorder).to have_received(:record!).with(
          hash_including(queue_name: queue_name, msg_id: 9)
        )
      end

      it "uses the executor's injected config, not only Pgbus.configuration" do
        # Global is allow-all; this executor's config still rejects Secret.
        Pgbus.configuration.allowed_global_id_models = nil
        config.allowed_global_id_models = [order_class]

        expect(executor.execute(message, queue_name)).to eq(:failed)
        expect(ActiveJob::Base).not_to have_received(:deserialize)
      end
    end

    context "when job.perform_now raises" do
      let(:message) { build_message_double(msg_id: 3, message: message_json, read_ct: 1) }
      let(:error) { StandardError.new("boom") }

      before do
        allow(job_double).to receive(:perform_now).and_raise(error)
      end

      it "logs the error, instruments failure, and returns :failed" do
        result = executor.execute(message, queue_name)

        expect(ActiveSupport::Notifications).to have_received(:instrument).with(
          "pgbus.job_failed",
          hash_including(queue: queue_name, job_class: "TestJob", error: "StandardError")
        )
        expect(result).to eq(:failed)
      end

      it "records the failure in pgbus_failed_events" do
        executor.execute(message, queue_name)

        expect(Pgbus::FailedEventRecorder).to have_received(:record!).with(
          queue_name: queue_name,
          msg_id: 3,
          payload: job_payload,
          headers: nil,
          error: error,
          retry_count: 0
        )
      end
    end

    # A job that succeeded but failed to archive redelivers after VT expiry
    # and runs twice. Archiving is idempotent (archiving an already-archived
    # message is a no-op), so a connection error on archive is safe to retry
    # once — unlike sends, where a retry could duplicate the message.
    context "when archive fails with a connection error after the job succeeded" do
      let(:message) { build_message_double(msg_id: 9, message: message_json, read_ct: 1) }

      # Load the genuine PGMQ::Errors::ConnectionError so this proves the
      # executor's archive-retry rescue (archive_from → connection_error?) matches
      # the real pgmq-ruby hierarchy — not a fake StandardError subclass.
      before { real_pgmq_connection_error }

      it "retries the archive once and returns :success" do
        calls = 0
        allow(mock_client).to receive(:archive_message) do
          calls += 1
          raise PGMQ::Errors::ConnectionError, "server closed the connection unexpectedly" if calls == 1

          true
        end

        result = executor.execute(message, queue_name)

        expect(mock_client).to have_received(:archive_message).with(queue_name, 9).twice
        expect(result).to eq(:success)
      end

      it "falls through to the failure path when the retry also fails" do
        allow(mock_client).to receive(:archive_message)
          .and_raise(PGMQ::Errors::ConnectionError, "server closed the connection unexpectedly")

        result = executor.execute(message, queue_name)

        expect(mock_client).to have_received(:archive_message).twice
        expect(result).to eq(:failed)
      end

      it "does not retry archive errors that are not connection errors" do
        allow(mock_client).to receive(:archive_message).and_raise(StandardError, "boom")

        result = executor.execute(message, queue_name)

        expect(mock_client).to have_received(:archive_message).once
        expect(result).to eq(:failed)
      end

      it "retries archive on the source_queue path too" do
        calls = 0
        allow(mock_client).to receive(:archive_message) do
          calls += 1
          raise PGMQ::Errors::ConnectionError, "connection reset by peer" if calls == 1

          true
        end

        result = executor.execute(message, queue_name, source_queue: "pgbus_default_p0")

        expect(mock_client).to have_received(:archive_message).with("pgbus_default_p0", 9, prefixed: false).twice
        expect(result).to eq(:success)
      end
    end

    # Regression: issue #126. Under `execution_mode: :async` the reactor or a
    # nested Async/Sync block can raise Async::Stop / Async::Cancel, both of
    # which inherit from Exception (NOT StandardError). A `rescue StandardError`
    # misses them entirely — control flow is lost between perform_now and
    # archive_from, the pgbus_failed_events row is never written, and the
    # uniqueness lock stays held until VT expiry finally allows a clean retry.
    # The executor must treat any Exception as a visible failure: record it,
    # instrument it, and return :failed so telemetry exists.
    context "when job.perform_now raises a non-StandardError (fiber interrupt)" do
      let(:message) { build_message_double(msg_id: 126, message: message_json, read_ct: 1) }
      let(:fiber_stop_class) { Class.new(Exception) } # rubocop:disable Lint/InheritException
      let(:fiber_stop) { fiber_stop_class.new("task stopped") }

      before do
        stub_const("FakeAsyncStop", fiber_stop_class)
        allow(job_double).to receive(:perform_now).and_raise(fiber_stop)
      end

      it "records the failure in pgbus_failed_events instead of swallowing silently" do
        executor.execute(message, queue_name)

        expect(Pgbus::FailedEventRecorder).to have_received(:record!).with(
          queue_name: queue_name,
          msg_id: 126,
          payload: job_payload,
          headers: nil,
          error: fiber_stop,
          retry_count: 0
        )
      end

      it "instruments pgbus.job_failed and returns :failed" do
        result = executor.execute(message, queue_name)

        expect(ActiveSupport::Notifications).to have_received(:instrument).with(
          "pgbus.job_failed",
          hash_including(queue: queue_name, job_class: "TestJob", error: "FakeAsyncStop")
        )
        expect(result).to eq(:failed)
      end

      it "re-raises truly fatal signals (SystemExit, Interrupt) instead of swallowing them" do
        allow(job_double).to receive(:perform_now).and_raise(SystemExit)

        expect { executor.execute(message, queue_name) }.to raise_error(SystemExit)
      end
    end

    context "when message payload is nil (JSON parse fails)" do
      let(:message) { build_message_double(msg_id: 9, message: nil, read_ct: 1) }

      it "returns :failed and safely handles nil payload via &.dig" do
        result = executor.execute(message, queue_name)

        expect(ActiveJob::Base).not_to have_received(:deserialize)
        expect(ActiveSupport::Notifications).to have_received(:instrument).with(
          "pgbus.job_failed",
          hash_including(queue: queue_name, job_class: nil)
        )
        expect(result).to eq(:failed)
      end
    end

    context "when instrumentation subscriber raises" do
      let(:message) { build_message_double(msg_id: 11, message: message_json, read_ct: 1) }

      before do
        allow(ActiveSupport::Notifications).to receive(:instrument)
          .with("pgbus.job_completed", anything)
          .and_raise(StandardError, "subscriber blew up")
      end

      it "rescues the subscriber error and still returns :success" do
        result = executor.execute(message, queue_name)

        expect(mock_client).to have_received(:archive_message).with(queue_name, 11)
        expect(result).to eq(:success)
      end
    end

    context "with batch_id in payload" do
      let(:batch_payload) { job_payload.merge("pgbus_batch_id" => "batch-abc") }
      let(:message_json) { JSON.generate(batch_payload) }
      let(:message) { build_message_double(msg_id: 30, message: message_json, read_ct: 1) }

      before do
        allow(ActiveJob::Base).to receive(:deserialize).with(batch_payload).and_return(job_double)
        allow(Pgbus::Batch).to receive(:job_completed)
        allow(Pgbus::Batch).to receive(:job_discarded)
      end

      it "signals batch completed on success" do
        executor.execute(message, queue_name)
        expect(Pgbus::Batch).to have_received(:job_completed).with("batch-abc")
      end

      it "signals batch discarded on dead letter" do
        dlq_message = build_message_double(msg_id: 31, message: message_json, read_ct: config.max_retries + 1)
        executor.execute(dlq_message, queue_name)
        expect(Pgbus::Batch).to have_received(:job_discarded).with("batch-abc")
      end

      it "does not signal batch on transient failure" do
        allow(job_double).to receive(:perform_now).and_raise(StandardError, "transient")
        executor.execute(message, queue_name)
        expect(Pgbus::Batch).not_to have_received(:job_completed)
        expect(Pgbus::Batch).not_to have_received(:job_discarded)
      end
    end

    context "with concurrency key in payload" do
      let(:concurrency_payload) { job_payload.merge("pgbus_concurrency_key" => "TestJob-42") }
      let(:message_json) { JSON.generate(concurrency_payload) }
      let(:message) { build_message_double(msg_id: 20, message: message_json, read_ct: 1) }

      before do
        allow(Pgbus::Concurrency).to receive(:extract_key).and_call_original
        allow(ActiveJob::Base).to receive(:deserialize).with(concurrency_payload).and_return(job_double)
        allow(Pgbus::Concurrency::Semaphore).to receive(:release)
        allow(Pgbus::Concurrency::BlockedExecution).to receive(:promote_next).and_return(false)
      end

      it "releases semaphore when no blocked jobs to promote" do
        executor.execute(message, queue_name)

        expect(Pgbus::Concurrency::BlockedExecution).to have_received(:promote_next).with("TestJob-42", client: mock_client)
        expect(Pgbus::Concurrency::Semaphore).to have_received(:release).with("TestJob-42")
      end

      it "skips semaphore release when promote_next succeeds (atomic handoff)" do
        allow(Pgbus::Concurrency::BlockedExecution).to receive(:promote_next).and_return(true)

        executor.execute(message, queue_name)

        expect(Pgbus::Concurrency::BlockedExecution).to have_received(:promote_next).with("TestJob-42", client: mock_client)
        expect(Pgbus::Concurrency::Semaphore).not_to have_received(:release)
      end

      it "releases semaphore on dead letter when no blocked jobs" do
        dlq_message = build_message_double(msg_id: 21, message: message_json, read_ct: config.max_retries + 1)

        executor.execute(dlq_message, queue_name)

        expect(Pgbus::Concurrency::Semaphore).to have_received(:release).with("TestJob-42")
      end

      it "does not signal concurrency on transient failure" do
        allow(job_double).to receive(:perform_now).and_raise(StandardError, "transient")

        executor.execute(message, queue_name)

        expect(Pgbus::Concurrency::Semaphore).not_to have_received(:release)
        expect(Pgbus::Concurrency::BlockedExecution).not_to have_received(:promote_next)
      end

      it "does not signal concurrency if archive_message fails (message will be retried)" do
        allow(mock_client).to receive(:archive_message).and_raise(StandardError, "DB gone")

        executor.execute(message, queue_name)

        expect(Pgbus::Concurrency::Semaphore).not_to have_received(:release)
        expect(Pgbus::Concurrency::BlockedExecution).not_to have_received(:promote_next)
      end
    end

    context "with job stat recording" do
      let(:enqueued_at) { (Time.now.utc - 0.5).iso8601(6) } # 500ms ago
      let(:message) { build_message_double(msg_id: 1, message: message_json, read_ct: 1, enqueued_at: enqueued_at) }

      it "records a success stat with enqueue latency and retry count" do
        executor.execute(message, queue_name)

        expect(Pgbus::JobStat).to have_received(:record!).with(
          job_class: "TestJob",
          queue_name: queue_name,
          status: "success",
          duration_ms: an_instance_of(Integer),
          enqueue_latency_ms: a_value >= 400,
          retry_count: 0
        )
      end

      it "records a failed stat with enqueue latency" do
        allow(job_double).to receive(:perform_now).and_raise(StandardError, "boom")

        executor.execute(message, queue_name)

        expect(Pgbus::JobStat).to have_received(:record!).with(
          job_class: "TestJob",
          queue_name: queue_name,
          status: "failed",
          duration_ms: an_instance_of(Integer),
          enqueue_latency_ms: a_value >= 400,
          retry_count: 0
        )
      end

      it "records a dead_lettered stat with retry count from read_ct" do
        dlq_message = build_message_double(
          msg_id: 99, message: message_json,
          read_ct: config.max_retries + 1, enqueued_at: enqueued_at
        )

        executor.execute(dlq_message, queue_name)

        expect(Pgbus::JobStat).to have_received(:record!).with(
          job_class: "TestJob",
          queue_name: queue_name,
          status: "dead_lettered",
          duration_ms: an_instance_of(Integer),
          enqueue_latency_ms: a_value >= 400,
          retry_count: config.max_retries
        )
      end

      it "sets retry_count to read_ct minus 1 (first read is not a retry)" do
        retry_message = build_message_double(msg_id: 50, message: message_json, read_ct: 3, enqueued_at: enqueued_at)

        executor.execute(retry_message, queue_name)

        expect(Pgbus::JobStat).to have_received(:record!).with(
          hash_including(retry_count: 2)
        )
      end

      it "does not record stats when stats_enabled is false" do
        config.stats_enabled = false

        executor.execute(message, queue_name)

        expect(Pgbus::JobStat).not_to have_received(:record!)
      ensure
        config.stats_enabled = true
      end

      it "handles nil enqueued_at gracefully" do
        nil_enqueued = build_message_double(msg_id: 60, message: message_json, read_ct: 1, enqueued_at: nil)

        executor.execute(nil_enqueued, queue_name)

        expect(Pgbus::JobStat).to have_received(:record!).with(
          hash_including(enqueue_latency_ms: nil)
        )
      end

      it "handles malformed enqueued_at gracefully" do
        bad_enqueued = build_message_double(msg_id: 61, message: message_json, read_ct: 1, enqueued_at: "not-a-date")

        executor.execute(bad_enqueued, queue_name)

        expect(Pgbus::JobStat).to have_received(:record!).with(
          hash_including(enqueue_latency_ms: nil)
        )
      end

      it "computes latency from numeric epoch enqueued_at" do
        epoch_enqueued = build_message_double(msg_id: 62, message: message_json, read_ct: 1,
                                              enqueued_at: Time.now.to_f - 0.5)

        executor.execute(epoch_enqueued, queue_name)

        expect(Pgbus::JobStat).to have_received(:record!).with(
          hash_including(enqueue_latency_ms: a_value >= 400)
        )
      end

      it "computes latency from Time object enqueued_at" do
        time_enqueued = build_message_double(msg_id: 63, message: message_json, read_ct: 1,
                                             enqueued_at: Time.now.utc - 0.5)

        executor.execute(time_enqueued, queue_name)

        expect(Pgbus::JobStat).to have_received(:record!).with(
          hash_including(enqueue_latency_ms: a_value >= 400)
        )
      end
    end

    # Uniqueness locking has three code paths in Executor#execute:
    #   1. No uniqueness key → no locking calls, no lock release
    #   2. Strategy = :until_executed → release on success OR dead-letter
    #   3. Strategy = :while_executing → acquire before perform, release on success
    # All three have been shipped without direct unit coverage; the audit
    # agents flagged this as one of the top test gaps.
    context "with uniqueness key in payload" do
      let(:uniqueness_payload) do
        job_payload.merge(
          "pgbus_uniqueness_key" => "TestJob:user-42",
          "pgbus_uniqueness_strategy" => "until_executed"
        )
      end
      let(:message_json) { JSON.generate(uniqueness_payload) }
      let(:message) { build_message_double(msg_id: 70, message: message_json, read_ct: 1) }

      before do
        allow(ActiveJob::Base).to receive(:deserialize).with(uniqueness_payload).and_return(job_double)
        allow(Pgbus::Uniqueness).to receive_messages(extract_key: "TestJob:user-42", extract_strategy: :until_executed)
        allow(Pgbus::Uniqueness).to receive(:release_lock)
      end

      context "with the until_executed strategy" do
        it "releases the lock after a successful run" do
          executor.execute(message, queue_name)

          expect(Pgbus::Uniqueness).to have_received(:release_lock).with("TestJob:user-42").once
        end

        it "does not release the lock if archive_from fails (retry path)" do
          # If archive fails, job_succeeded stays false, the ensure block
          # skips release — so the next retry still sees the lock and
          # enforces at-most-once semantics.
          allow(mock_client).to receive(:archive_message).and_raise(StandardError, "DB gone")

          executor.execute(message, queue_name)

          expect(Pgbus::Uniqueness).not_to have_received(:release_lock)
        end

        it "does not release the lock when perform_now raises (retry path)" do
          allow(job_double).to receive(:perform_now).and_raise(StandardError, "transient")

          executor.execute(message, queue_name)

          expect(Pgbus::Uniqueness).not_to have_received(:release_lock)
        end

        it "releases the lock on dead-lettering (terminal state)" do
          dlq_message = build_message_double(msg_id: 71, message: message_json, read_ct: config.max_retries + 1)

          executor.execute(dlq_message, queue_name)

          expect(Pgbus::Uniqueness).to have_received(:release_lock).with("TestJob:user-42")
        end

        it "releases the lock after archive and before job_completed (issue #418)" do
          order = []
          allow(mock_client).to receive(:archive_message) do
            order << :archive
            true
          end
          allow(Pgbus::Uniqueness).to receive(:release_lock) { order << :release }
          allow(ActiveSupport::Notifications).to receive(:instrument).and_wrap_original do |method, *args, &block|
            order << :completed if args.first == "pgbus.job_completed"
            method.call(*args, &block)
          end

          executor.execute(message, queue_name)

          expect(order.index(:archive)).to be < order.index(:release)
          expect(order.index(:release)).to be < order.index(:completed)
        end

        it "returns :success when release_lock raises after archive" do
          allow(Pgbus::Uniqueness).to receive(:release_lock).and_raise(StandardError, "pooler timeout")
          allow(Pgbus.logger).to receive(:warn)

          result = executor.execute(message, queue_name)

          expect(result).to eq(:success)
          expect(mock_client).to have_received(:archive_message)
          expect(Pgbus::Uniqueness).to have_received(:release_lock).with("TestJob:user-42").once
        end
      end

      context "with the while_executing strategy" do
        before do
          allow(Pgbus::Uniqueness).to receive_messages(extract_strategy: :while_executing, acquire_execution_lock: true)
        end

        it "acquires the execution lock before perform_now and releases on success" do
          executor.execute(message, queue_name)

          expect(Pgbus::Uniqueness).to have_received(:acquire_execution_lock).with(
            "TestJob:user-42", uniqueness_payload
          )
          expect(job_double).to have_received(:perform_now)
          expect(Pgbus::Uniqueness).to have_received(:release_lock).with("TestJob:user-42").once
        end

        it "returns :skipped without performing if another worker already holds the lock" do
          allow(Pgbus::Uniqueness).to receive(:acquire_execution_lock).and_return(false)

          result = executor.execute(message, queue_name)

          expect(job_double).not_to have_received(:perform_now)
          expect(mock_client).not_to have_received(:archive_message)
          expect(result).to eq(:skipped)
        end
      end

      it "does not crash when uniqueness_key is nil (payload has strategy but no key)" do
        allow(Pgbus::Uniqueness).to receive(:extract_key).and_return(nil)

        expect do
          executor.execute(message, queue_name)
        end.not_to raise_error
        # ensure block guards with `if uniqueness_key` — no release call
        expect(Pgbus::Uniqueness).not_to have_received(:release_lock)
      end
    end

    describe "lifecycle debug logging" do
      let(:message) { build_message_double(msg_id: 80, message: message_json, read_ct: 1) }
      let(:log_output) { StringIO.new }

      let(:previous_logger) { config.logger }

      before do
        previous_logger # memoize before overwriting
        config.logger = Logger.new(log_output, level: Logger::DEBUG)
      end

      after do
        config.logger = previous_logger
      end

      it "emits debug logs at each phase on success" do
        executor.execute(message, queue_name)

        output = log_output.string
        expect(output).to include("[Pgbus::Executor] start msg_id=80")
        expect(output).to include("[Pgbus::Executor] deserialized msg_id=80")
        expect(output).to include("[Pgbus::Executor] running msg_id=80")
        expect(output).to include("[Pgbus::Executor] perform_returned msg_id=80")
        expect(output).to include("[Pgbus::Executor] archived msg_id=80")
        expect(output).to include("[Pgbus::Executor] done msg_id=80")
      end

      it "includes queue and read_ct in the tag" do
        executor.execute(message, queue_name)

        output = log_output.string
        expect(output).to include("queue=default")
        expect(output).to include("read_ct=1")
      end

      it "includes job_class after deserialization" do
        executor.execute(message, queue_name)

        output = log_output.string
        expect(output).to include("job_class=TestJob")
      end

      it "emits debug logs for dead letter path" do
        dlq_message = build_message_double(msg_id: 81, message: message_json, read_ct: config.max_retries + 1)

        executor.execute(dlq_message, queue_name)

        output = log_output.string
        expect(output).to include("[Pgbus::Executor] start msg_id=81")
        expect(output).to include("[Pgbus::Executor] dead_lettered msg_id=81")
        expect(output).to include("job_class=TestJob")
      end

      it "emits debug log on failure" do
        allow(job_double).to receive(:perform_now).and_raise(StandardError, "boom")

        executor.execute(message, queue_name)

        output = log_output.string
        expect(output).to include("[Pgbus::Executor] start msg_id=80")
        expect(output).to include("[Pgbus::Executor] failed msg_id=80")
      end

      it "does not evaluate debug blocks when logger level is above debug" do
        config.logger = Logger.new(log_output, level: Logger::INFO)

        executor.execute(message, queue_name)

        output = log_output.string
        expect(output).not_to include("[Pgbus::Executor]")
      end
    end

    describe "retry backoff (VT-based retry path)" do
      let(:message) { build_message_double(msg_id: 50, message: message_json, read_ct: 2) }

      before do
        allow(job_double).to receive(:perform_now).and_raise(StandardError, "transient")
      end

      it "extends the visibility timeout with exponential backoff on failure" do
        executor.execute(message, queue_name)

        # read_ct=2 → attempt=1 (first retry), base=5 → delay=5
        expect(mock_client).to have_received(:set_visibility_timeout).with(
          queue_name, 50, vt: a_value_between(4, 6)
        )
      end

      it "increases backoff with higher read_ct" do
        msg = build_message_double(msg_id: 51, message: message_json, read_ct: 4)

        executor.execute(msg, queue_name)

        # read_ct=4 → attempt=3, base=5 → 5*2^2=20
        expect(mock_client).to have_received(:set_visibility_timeout).with(
          queue_name, 51, vt: a_value_between(17, 23)
        )
      end

      it "does not set VT on first read (read_ct=1)" do
        msg = build_message_double(msg_id: 52, message: message_json, read_ct: 1)

        executor.execute(msg, queue_name)

        expect(mock_client).not_to have_received(:set_visibility_timeout)
      end

      it "does not blow up if set_visibility_timeout raises" do
        allow(mock_client).to receive(:set_visibility_timeout).and_raise(StandardError, "pg gone")

        expect { executor.execute(message, queue_name) }.not_to raise_error
      end
    end

    describe "#execute_job (reloader vs executor wrapping)" do
      let(:message) { build_message_double(msg_id: 90, message: message_json, read_ct: 1) }
      let(:mock_executor_wrapper) { double("executor") }
      let(:mock_reloader_wrapper) { double("reloader") }
      let(:mock_app_config) { double("config") }
      let(:mock_app) do
        double("Rails.application",
               executor: mock_executor_wrapper,
               reloader: mock_reloader_wrapper,
               config: mock_app_config)
      end

      before do
        allow(mock_executor_wrapper).to receive(:wrap).and_yield
        allow(mock_reloader_wrapper).to receive(:wrap).and_yield
      end

      def stub_rails!
        rails = double("Rails", application: mock_app)
        allow(rails).to receive(:respond_to?) { |name, *| name == :application }
        stub_const("Rails", rails)
      end

      context "when Rails.application.config.enable_reloading is true (development)" do
        before do
          allow(mock_app_config).to receive(:respond_to?).with(:enable_reloading).and_return(true)
          allow(mock_app_config).to receive(:enable_reloading).and_return(true)
          stub_rails!
        end

        it "wraps job execution in reloader.wrap" do
          executor.execute(message, queue_name)

          expect(mock_reloader_wrapper).to have_received(:wrap)
          expect(mock_executor_wrapper).not_to have_received(:wrap)
          expect(job_double).to have_received(:perform_now)
        end
      end

      context "when Rails.application.config.enable_reloading is false (production)" do
        before do
          allow(mock_app_config).to receive(:respond_to?).with(:enable_reloading).and_return(true)
          allow(mock_app_config).to receive(:enable_reloading).and_return(false)
          stub_rails!
        end

        it "wraps job execution in executor.wrap" do
          executor.execute(message, queue_name)

          expect(mock_executor_wrapper).to have_received(:wrap)
          expect(mock_reloader_wrapper).not_to have_received(:wrap)
          expect(job_double).to have_received(:perform_now)
        end
      end

      context "when Rails.application.config does not respond to enable_reloading (Rails < 7.1)" do
        before do
          allow(mock_app_config).to receive(:respond_to?).with(:enable_reloading).and_return(false)
          allow(mock_app_config).to receive(:cache_classes).and_return(false)
          stub_rails!
        end

        it "falls back to checking cache_classes and uses reloader.wrap when cache_classes is false" do
          executor.execute(message, queue_name)

          expect(mock_reloader_wrapper).to have_received(:wrap)
          expect(mock_executor_wrapper).not_to have_received(:wrap)
        end
      end

      context "when Rails.application.config.cache_classes is true (Rails < 7.1 production)" do
        before do
          allow(mock_app_config).to receive(:respond_to?).with(:enable_reloading).and_return(false)
          allow(mock_app_config).to receive(:cache_classes).and_return(true)
          stub_rails!
        end

        it "uses executor.wrap" do
          executor.execute(message, queue_name)

          expect(mock_executor_wrapper).to have_received(:wrap)
          expect(mock_reloader_wrapper).not_to have_received(:wrap)
        end
      end

      context "when Rails is not defined" do
        it "calls perform_now directly without any wrapper" do
          hide_const("Rails") if defined?(Rails)

          executor.execute(message, queue_name)

          expect(job_double).to have_received(:perform_now)
        end
      end
    end
  end
end
