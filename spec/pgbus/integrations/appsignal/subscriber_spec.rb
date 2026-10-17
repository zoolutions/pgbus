# frozen_string_literal: true

require "spec_helper"

RSpec.describe "Pgbus::Integrations::Appsignal::Subscriber" do
  # We stub the entire ::Appsignal module so the spec can run without the
  # real appsignal gem on the load path. The double (spec/support/
  # appsignal_double.rb) keeps a current transaction and the real gem's
  # set_error guard, so a "reports the error" example cannot pass through a
  # call the real gem drops (issue #552).
  let(:double_pair) { AppsignalDouble.build }
  let(:appsignal_class) { double_pair.appsignal }
  let(:transaction_class) { double_pair.transaction_class }
  let(:transactions) { transaction_class.all }

  before do
    stub_const("Appsignal", appsignal_class)
    stub_const("Appsignal::Transaction", transaction_class)
    stub_const("Appsignal::Transaction::BACKGROUND_JOB", "background_job")
    require "pgbus/integrations/appsignal/subscriber"
    Pgbus::Integrations::Appsignal::Subscriber.reset!
    Pgbus::Integrations::Appsignal::Subscriber.install!
  end

  after do
    Pgbus::Integrations::Appsignal::Subscriber.reset!
    AppsignalDouble.reset!
  end

  def instrument_execute(payload = {}, &)
    ActiveSupport::Notifications.instrument("pgbus.executor.execute", { queue: "default", job_class: "TestJob" }.merge(payload), &)
  end

  def instrument_event_processed(payload = {}, &)
    ActiveSupport::Notifications.instrument(
      "pgbus.event_processed", { handler: "MyHandler", routing_key: "orders.created" }.merge(payload), &
    )
  end

  describe ".install!" do
    it "subscribes once when two threads install at once", :first_call_race do
      Pgbus::Integrations::Appsignal::Subscriber.reset!
      hold_first_call(ActiveSupport::Notifications, :subscribe)

      race { Pgbus::Integrations::Appsignal::Subscriber.install! }

      expect(ActiveSupport::Notifications).to have_received(:subscribe).with("pgbus.job_completed").once
    end
  end

  describe "pgbus.executor.execute" do
    it "creates a background-job transaction with action, queue start, tags and arguments" do
      instrument_execute(
        job_id: "abc-123", provider_job_id: "42", arguments: [1, "hi"],
        enqueued_at: "2026-05-05T10:00:00Z", read_ct: 1, msg_id: 9
      ) { :ok }

      transaction = transactions.sole
      expect(transaction).to have_attributes(
        namespace: "background_job", action: "TestJob#perform", queue_start: (Time.utc(2026, 5, 5, 10).to_f * 1000).to_i,
        params: { arguments: [1, "hi"] }, errors: [], completed?: true
      )
      expect(transaction.tags).to include("queue" => "default", "job_class" => "TestJob", "active_job_id" => "abc-123")
      expect(appsignal_class.distributions.map(&:first)).to eq(["pgbus_job_duration_ms"])
    end

    it "makes the transaction current while the block runs, and only then" do
      seen = []

      instrument_execute { seen << [transaction_class.current?, transaction_class.current.action] }

      expect(seen).to eq([[true, "TestJob#perform"]])
      expect(transaction_class.current?).to be(false)
    end

    it "records an error raised inside the block on the job's transaction, and still completes it once" do
      error = RuntimeError.new("boom")

      expect { instrument_execute { raise error } }.to raise_error(error)

      expect(transactions.sole).to have_attributes(action: "TestJob#perform", errors: [error], completed?: true)
      expect(transaction_class.current?).to be(false)
      expect(appsignal_class.distributions.map(&:first)).to eq(["pgbus_job_duration_ms"])
    end

    it "keeps one error sample when the Rails error reporter already reported it on the current transaction" do
      error = RuntimeError.new("boom")

      reported_then_raised = lambda do
        instrument_execute do
          appsignal_class.report_error(error)
          raise error
        end
      end

      expect(&reported_then_raised).to raise_error(error)
      expect(transactions.sole.errors).to eq([error])
    end

    it "times the block with a monotonic clock" do
      instrument_execute { sleep 0.02 }

      _name, duration, tags = appsignal_class.distributions.sole
      expect(duration).to be >= 20
      expect(tags).to eq(queue: "default", job_class: "TestJob")
    end

    it "reuses a transaction that is already current and leaves it to its owner to complete" do
      host = transaction_class.create("http_request")

      instrument_execute { :ok }

      expect(transactions).to eq([host])
      expect(host.completed?).to be(false)
      expect(transaction_class.current).to be(host)
    end

    it "still completes its own transaction when a span of the same kind runs inside it" do
      instrument_execute(job_class: "OuterJob") { instrument_execute(job_class: "InnerJob") { :ok } }

      expect(transactions.sole).to have_attributes(action: "OuterJob#perform", completed?: true)
      expect(transaction_class.current?).to be(false)
      expect(appsignal_class.distributions.map { |_name, _value, tags| tags[:job_class] }).to eq(%w[InnerJob OuterJob])
    end

    it "keeps each fiber's transaction to itself when two jobs interleave (execution_mode: :async)" do
      seen = {}
      first = Fiber.new do
        instrument_execute(job_class: "FirstJob") do
          Fiber.yield
          seen[:first] = transaction_class.current
        end
      end
      first.resume
      instrument_execute(job_class: "SecondJob") { seen[:second] = transaction_class.current }
      first.resume

      expect(seen.transform_values(&:action)).to eq(first: "FirstJob#perform", second: "SecondJob#perform")
      expect(transactions).to all(be_completed)
      expect(appsignal_class.distributions.map { |_name, _value, tags| tags[:job_class] }).to contain_exactly("FirstJob", "SecondJob")
    end

    it "never lets an AppSignal failure reach the job, and never leaves a transaction current" do
      allow(appsignal_class).to receive(:add_distribution_value).and_raise(StandardError, "agent down")
      allow(Pgbus.logger).to receive(:warn).and_call_original

      expect(instrument_execute { :ran }).to eq(:ran)

      expect(transactions.sole.completed?).to be(true)
      expect(transaction_class.current?).to be(false)
      expect(Pgbus.logger).to have_received(:warn)
    end

    it "still runs the job when the transaction cannot be created" do
      allow(transaction_class).to receive(:create).and_raise(StandardError, "agent down")

      expect(instrument_execute { :ran }).to eq(:ran)
      expect(transaction_class.current?).to be(false)
    end
  end

  describe "pgbus.job_completed" do
    it "increments the processed counter" do
      ActiveSupport::Notifications.instrument("pgbus.job_completed", queue: "default", job_class: "TestJob")

      expect(appsignal_class.counters).to include(
        ["pgbus_queue_job_count", 1, { queue: "default", job_class: "TestJob", status: "processed" }]
      )
    end
  end

  describe "pgbus.job_failed" do
    let(:err) { StandardError.new("nope") }

    def instrument_job_failed
      ActiveSupport::Notifications.instrument(
        "pgbus.job_failed",
        queue: "default", job_class: "TestJob", job_id: "abc-123", read_ct: 2, msg_id: 9,
        error: "StandardError", exception_object: err
      )
    end

    it "increments the failed counter" do
      instrument_job_failed

      expect(appsignal_class.counters).to include(["pgbus_queue_job_count", 1, hash_including(status: "failed")])
    end

    it "still reports the error when the failed counter raises" do
      allow(appsignal_class).to receive(:increment_counter).and_raise(RuntimeError, "agent down")

      instrument_job_failed

      expect(transactions.sole).to have_attributes(action: "TestJob#perform", errors: [err])
    end

    it "reports an error raised outside the execute span on a background-job transaction of its own" do
      instrument_job_failed

      expect(transactions.sole).to have_attributes(
        namespace: "background_job", action: "TestJob#perform", errors: [err], completed?: true
      )
      expect(transactions.sole.tags).to include("queue" => "default", "job_class" => "TestJob", "attempts" => 2)
    end

    it "adds an error raised outside the span to a host transaction without renaming it" do
      host = transaction_class.create("http_request")

      instrument_job_failed

      expect(transactions).to eq([host])
      expect(host).to have_attributes(namespace: "http_request", action: nil, tags: {}, errors: [err], completed?: false)
    end

    it "reports nothing more for the error the execute span already recorded" do
      expect { instrument_execute { raise err } }.to raise_error(err)

      instrument_job_failed

      expect(transactions.sole).to have_attributes(action: "TestJob#perform", errors: [err])
    end

    it "reports a later error outside the span even after a span recorded another one" do
      earlier = RuntimeError.new("earlier")
      expect { instrument_execute { raise earlier } }.to raise_error(earlier)

      instrument_job_failed

      expect(transactions.map(&:errors)).to eq([[earlier], [err]])
    end
  end

  describe "pgbus.job_dead_lettered" do
    it "increments the dead_lettered counter" do
      ActiveSupport::Notifications.instrument("pgbus.job_dead_lettered", queue: "default", job_class: "TestJob")

      expect(appsignal_class.counters).to include(
        ["pgbus_queue_job_count", 1, hash_including(status: "dead_lettered")]
      )
    end
  end

  describe "pgbus.event_processed" do
    it "creates a transaction and increments the processed counter" do
      instrument_event_processed(event_id: "ev-1", published_at: Time.utc(2026, 5, 5, 10, 0, 0), read_ct: 1, msg_id: 7) { :ok }

      expect(transactions.sole).to have_attributes(
        namespace: "background_job", action: "MyHandler#handle", queue_start: (Time.utc(2026, 5, 5, 10).to_f * 1000).to_i, completed?: true
      )
      expect(transactions.sole.tags).to eq("handler" => "MyHandler", "routing_key" => "orders.created", "attempts" => 1)
      expect(appsignal_class.counters).to include(
        ["pgbus_event_count", 1, hash_including(handler: "MyHandler", status: "processed")]
      )
      expect(appsignal_class.distributions.map(&:first)).to eq(["pgbus_event_duration_ms"])
    end

    it "makes the transaction current while the handler runs" do
      seen = nil

      instrument_event_processed { seen = transaction_class.current.action }

      expect(seen).to eq("MyHandler#handle")
      expect(transaction_class.current?).to be(false)
    end

    it "records an error raised inside the handler on the handler's transaction" do
      error = RuntimeError.new("handler bug")

      expect { instrument_event_processed { raise error } }.to raise_error(error)

      expect(transactions.sole).to have_attributes(action: "MyHandler#handle", errors: [error], completed?: true)
      # Unchanged, and the same in Metrics::Subscriber: the processed count is
      # taken at finish, so a raised handler also counts as processed.
      expect(appsignal_class.counters.map { |_name, _value, tags| tags[:status] }).to eq(["processed"])
    end
  end

  describe "pgbus.event_failed" do
    let(:err) { StandardError.new("handler bug") }

    def instrument_event_failed
      ActiveSupport::Notifications.instrument(
        "pgbus.event_failed",
        event_id: nil, handler: "MyHandler", routing_key: "orders.created", error: "StandardError", exception_object: err
      )
    end

    it "increments the failed counter" do
      instrument_event_failed

      expect(appsignal_class.counters).to include(["pgbus_event_count", 1, hash_including(status: "failed")])
    end

    it "still reports the error when the failed counter raises" do
      allow(appsignal_class).to receive(:increment_counter).and_raise(RuntimeError, "agent down")

      instrument_event_failed

      expect(transactions.sole).to have_attributes(action: "MyHandler#handle", errors: [err])
    end

    it "reports an error raised before the handler ran (building the event) on a <Handler>#handle transaction" do
      instrument_event_failed

      expect(transactions.sole).to have_attributes(
        namespace: "background_job", action: "MyHandler#handle", errors: [err], completed?: true
      )
      expect(transactions.sole.tags).to eq("handler" => "MyHandler", "routing_key" => "orders.created")
    end

    it "reports nothing more for the error the handler span already recorded" do
      expect { instrument_event_processed { raise err } }.to raise_error(err)

      instrument_event_failed

      expect(transactions.sole.errors).to eq([err])
    end
  end

  describe "pgbus.event_dead_lettered" do
    it "increments the event dead_lettered counter, tagged with the handler and routing key (issue #543)" do
      ActiveSupport::Notifications.instrument(
        "pgbus.event_dead_lettered",
        queue_name: "q_orders", routing_key: "orders.placed", handler: "NotifyHandler",
        event_id: "e-1", read_ct: 6, msg_id: 7, error: "RuntimeError"
      )

      expect(appsignal_class.counters).to eq(
        [["pgbus_event_count", 1, { handler: "NotifyHandler", routing_key: "orders.placed", status: "dead_lettered" }]]
      )
    end

    it "drops the handler and routing key tags when a body that is not JSON left them nil" do
      ActiveSupport::Notifications.instrument(
        "pgbus.event_dead_lettered",
        queue_name: "q_orders", routing_key: nil, handler: nil, event_id: nil, read_ct: 6, msg_id: 7, error: nil
      )

      expect(appsignal_class.counters).to eq([["pgbus_event_count", 1, { status: "dead_lettered" }]])
    end

    it "counts only: no transaction and no error report (nothing was raised at move time)" do
      ActiveSupport::Notifications.instrument(
        "pgbus.event_dead_lettered", handler: "NotifyHandler", routing_key: "orders.placed", error: "RuntimeError"
      )

      expect(transactions).to be_empty
    end
  end

  describe "pgbus.client.send_message / send_batch / read_batch" do
    it "tracks counters and distributions" do
      ActiveSupport::Notifications.instrument("pgbus.client.send_message", queue: "default") { nil }
      ActiveSupport::Notifications.instrument("pgbus.client.send_batch", queue: "default", count: 5) { nil }
      ActiveSupport::Notifications.instrument("pgbus.client.read_batch", queue: "default", count: 3) { nil }

      counter_names = appsignal_class.counters.map(&:first)
      expect(counter_names).to include("pgbus_messages_sent", "pgbus_messages_read")

      send_distributions = appsignal_class.distributions.map(&:first)
      expect(send_distributions).to include("pgbus_send_duration_ms", "pgbus_send_batch_duration_ms")
    end
  end

  describe "pgbus.stream.broadcast" do
    it "tracks count and bytes" do
      ActiveSupport::Notifications.instrument(
        "pgbus.stream.broadcast",
        stream: "chat:42",
        deferred: false,
        bytes: 128
      ) { nil }

      expect(appsignal_class.counters).to include(
        ["pgbus_stream_broadcast_count", 1, hash_including(stream: "chat:42", deferred: "false")]
      )
      expect(appsignal_class.distributions).to include(
        ["pgbus_stream_broadcast_bytes", 128, { stream: "chat:42" }]
      )
    end
  end

  describe "pgbus.outbox.publish" do
    it "tracks count and duration" do
      ActiveSupport::Notifications.instrument("pgbus.outbox.publish", queue: "default", kind: :job) { nil }

      expect(appsignal_class.counters).to include(["pgbus_outbox_published", 1, { kind: :job }])
      expect(appsignal_class.distributions.map(&:first)).to include("pgbus_outbox_publish_duration_ms")
    end
  end

  describe "pgbus.recurring.enqueue" do
    it "increments the enqueued counter" do
      ActiveSupport::Notifications.instrument(
        "pgbus.recurring.enqueue",
        task: "send_digest",
        class_name: "DigestJob",
        queue: "default",
        run_at: Time.now
      ) { nil }

      expect(appsignal_class.counters).to include(
        ["pgbus_recurring_enqueued", 1, hash_including(task: "send_digest")]
      )
    end
  end

  describe "pgbus.worker.recycle" do
    it "increments the recycled counter with the reason and kind: worker" do
      ActiveSupport::Notifications.instrument(
        "pgbus.worker.recycle",
        reason: :max_jobs,
        jobs_processed: 10_000
      )

      expect(appsignal_class.counters).to include(
        ["pgbus_worker_recycled", 1, hash_including(reason: :max_jobs, kind: "worker")]
      )
    end
  end

  describe "pgbus.consumer.recycle" do
    it "increments the recycled counter with the reason and kind: consumer" do
      ActiveSupport::Notifications.instrument(
        "pgbus.consumer.recycle",
        reason: :max_memory,
        jobs_processed: 500
      )

      expect(appsignal_class.counters).to include(
        ["pgbus_worker_recycled", 1, hash_including(reason: :max_memory, kind: "consumer")]
      )
    end
  end

  describe "pgbus.client.pool" do
    it "is not subscribed — the minutely probe already covers AppSignal pool gauges" do
      appsignal_class.gauges.clear

      ActiveSupport::Notifications.instrument("pgbus.client.pool", size: 10, available: 7, pool_timeout: 5)

      expect(appsignal_class.gauges).to be_empty
      expect(appsignal_class.counters).to be_empty
      expect(appsignal_class.distributions).to be_empty
    end
  end

  describe "subscriber resilience" do
    it "swallows errors and logs at warn level" do
      allow(appsignal_class).to receive(:increment_counter).and_raise(StandardError, "agent down")
      allow(Pgbus.logger).to receive(:warn).and_call_original

      expect do
        ActiveSupport::Notifications.instrument("pgbus.job_completed", queue: "default", job_class: "TestJob")
      end.not_to raise_error

      expect(Pgbus.logger).to have_received(:warn)
    end
  end

  describe ".reset!" do
    it "detaches the span subscribers too" do
      Pgbus::Integrations::Appsignal::Subscriber.reset!

      instrument_execute { :ok }
      instrument_event_processed { :ok }

      expect(transactions).to be_empty
    end
  end

  describe "workflow events (issue #540)" do
    let(:base) { { workflow_class: "SignupWorkflow", workflow_id: "wf-1" } }

    it "mirrors the Prometheus step histogram and counter" do
      ActiveSupport::Notifications.instrument("pgbus.workflow.step", **base, step_name: "welcome", attempt: 1,
                                                                             outcome: "completed", duration: 0.25)

      tags = { workflow_class: "SignupWorkflow", step: "welcome", outcome: "completed" }
      expect(appsignal_class.distributions).to include(["pgbus_workflow_step_duration_seconds", 0.25, tags])
      expect(appsignal_class.counters).to include(["pgbus_workflow_steps_total", 1, tags])
    end

    it "counts finished, paused, cancelled and stalled runs" do
      ActiveSupport::Notifications.instrument("pgbus.workflow_finished", **base)
      ActiveSupport::Notifications.instrument("pgbus.workflow_paused", **base, error_class: "Timeout::Error")
      ActiveSupport::Notifications.instrument("pgbus.workflow_cancelled", **base)
      ActiveSupport::Notifications.instrument("pgbus.workflow.stall_detected", **base, step_name: "charge", detection: "sweep")

      expect(appsignal_class.counters).to include(
        ["pgbus_workflows_finished_total", 1, { workflow_class: "SignupWorkflow" }],
        ["pgbus_workflows_paused_total", 1, { workflow_class: "SignupWorkflow", error_class: "Timeout::Error" }],
        ["pgbus_workflows_cancelled_total", 1, { workflow_class: "SignupWorkflow" }],
        ["pgbus_workflow_stalls_total", 1, { workflow_class: "SignupWorkflow", step: "charge", detection: "sweep" }]
      )
    end

    it "sets the status and stuck-reason gauges" do
      ActiveSupport::Notifications.instrument(
        "pgbus.workflow.gauge",
        statuses: [{ workflow_class: "SignupWorkflow", status: "ready", count: 4 }],
        stuck: [{ workflow_class: "SignupWorkflow", reason: "overdue", count: 1 }]
      )

      expect(appsignal_class.gauges).to include(
        ["pgbus_workflows", 4, { workflow_class: "SignupWorkflow", status: "ready" }],
        ["pgbus_workflows_stuck", 1, { workflow_class: "SignupWorkflow", reason: "overdue" }]
      )
    end
  end
end
