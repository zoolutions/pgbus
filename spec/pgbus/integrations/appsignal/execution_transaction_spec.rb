# frozen_string_literal: true

require "spec_helper"
require "json"
require "active_job"

# Issue #552: the job's (or handler's) background_job transaction is current
# while the work runs and carries the error of that execution, wherever it was
# raised. Drives the real Executor and Handler against the stateful AppSignal
# double (spec/support/appsignal_double.rb), which keeps the real
# Appsignal.set_error guard.
RSpec.describe "AppSignal transactions around a pgbus execution" do # rubocop:disable RSpec/DescribeClass
  let(:double_pair) { AppsignalDouble.build }
  let(:transaction_class) { double_pair.transaction_class }
  let(:transactions) { transaction_class.all }
  let(:log) { [] }

  before do
    stub_const("Appsignal", double_pair.appsignal)
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

  describe "a job run by Pgbus::ActiveJob::Executor" do
    subject(:executor) { Pgbus::ActiveJob::Executor.new(client: build_mock_client, config: Pgbus.configuration) }

    before do
      stub_const("Pgbus::JobStat", Class.new)
      allow(Pgbus::JobStat).to receive_messages(record!: nil, table_exists?: true)
      allow(Pgbus::FailedEventRecorder).to receive_messages(record!: nil, clear!: nil, last_error: nil)
    end

    def message_for(job_class, read_ct: 1)
      payload = { "job_class" => job_class, "job_id" => SecureRandom.uuid, "queue_name" => "default", "arguments" => [],
                  "enqueued_at" => "2026-05-05T10:00:00Z" }
      build_message_double(msg_id: 1, message: JSON.generate(payload), read_ct: read_ct)
    end

    def define_job(name, &perform)
      stub_const(name, Class.new(ActiveJob::Base) { define_method(:perform, &perform) })
    end

    it "reports a failure raised before perform (an unknown job class) on the job's transaction, on every attempt" do
      results = [1, 2].map { |attempt| executor.execute(message_for("NoSuchJob", read_ct: attempt), "default") }

      expect(results).to eq(%i[failed failed])
      expect(transactions.size).to eq(2)
      expect(transactions).to all(have_attributes(namespace: "background_job", action: "NoSuchJob#perform", completed?: true))
      expect(transactions.map { |t| t.errors.map(&:class) }).to all(eq([ActiveJob::UnknownJobClassError]))
      expect(transactions.map { |t| t.tags.slice("queue", "job_class", "attempts") }).to eq(
        [{ "queue" => "default", "job_class" => "NoSuchJob", "attempts" => 1 },
         { "queue" => "default", "job_class" => "NoSuchJob", "attempts" => 2 }]
      )
    end

    it "has a current transaction while perform runs, and records an error raised inside it there" do
      events = log
      define_job("ExplodingJob") do
        events << Appsignal::Transaction.current.action
        raise "boom"
      end

      expect(executor.execute(message_for("ExplodingJob"), "default")).to eq(:failed)

      expect(log).to eq(["ExplodingJob#perform"])
      expect(transactions.sole).to have_attributes(action: "ExplodingJob#perform", completed?: true)
      expect(transactions.sole.errors.map(&:message)).to eq(["boom"])
      expect(transactions.sole.queue_start).to eq((Time.utc(2026, 5, 5, 10).to_f * 1000).to_i)
    end

    it "lets job code attach data to the job's transaction" do
      define_job("TaggingJob") { Appsignal::Transaction.current.add_tags("tenant" => "acme") }

      expect(executor.execute(message_for("TaggingJob"), "default")).to eq(:success)

      expect(transactions.sole.tags).to include("tenant" => "acme", "job_class" => "TaggingJob")
      expect(transactions.sole.errors).to be_empty
    end

    it "reports a message whose body is not JSON (raised before the span) on an UnknownJob#perform transaction" do
      message = build_message_double(msg_id: 1, message: "not json", read_ct: 1)

      expect(executor.execute(message, "default")).to eq(:failed)

      expect(transactions.sole).to have_attributes(action: "UnknownJob#perform", completed?: true)
      expect(transactions.sole.errors.map(&:class)).to eq([JSON::ParserError])
    end

    it "pairs start and finish inside a fiber (execution_mode: :async)" do
      define_job("FiberJob") { raise "in a fiber" }

      Fiber.new { executor.execute(message_for("FiberJob"), "default") }.resume

      expect(transactions.sole).to have_attributes(action: "FiberJob#perform", completed?: true)
      expect(transactions.sole.errors.map(&:message)).to eq(["in a fiber"])
      expect(transaction_class.current?).to be(false)
    end
  end

  describe "an event run by a Pgbus::EventBus::Handler" do
    let(:handler_class) do
      events = log
      Class.new(Pgbus::EventBus::Handler) do
        define_method(:handle) do |event|
          events << Appsignal::Transaction.current.action
          raise "handler bug" if event.payload["explode"]
        end
      end
    end

    before { stub_const("OrderHandler", handler_class) }

    def message_with(published_at: Time.now.utc.iso8601(6), payload: {})
      body = { "event_id" => SecureRandom.uuid, "payload" => payload, "published_at" => published_at,
               "headers" => { "routing_key" => "orders.created" } }
      build_message_double(msg_id: 3, message: JSON.generate(body), read_ct: 1)
    end

    it "has a current transaction inside handle" do
      expect(OrderHandler.new.process(message_with)).to eq(:handled)

      expect(log).to eq(["OrderHandler#handle"])
      expect(transactions.sole).to have_attributes(action: "OrderHandler#handle", errors: [], completed?: true)
    end

    it "records an error raised inside handle on the handler's transaction, once" do
      expect { OrderHandler.new.process(message_with(payload: { "explode" => true })) }.to raise_error("handler bug")

      expect(transactions.sole).to have_attributes(action: "OrderHandler#handle", completed?: true)
      expect(transactions.sole.errors.map(&:message)).to eq(["handler bug"])
    end

    it "reports an error raised while building the event on an OrderHandler#handle transaction" do
      expect { OrderHandler.new.process(message_with(published_at: "not a time")) }.to raise_error(ArgumentError)

      expect(log).to be_empty
      expect(transactions.sole).to have_attributes(namespace: "background_job", action: "OrderHandler#handle", completed?: true)
      expect(transactions.sole.errors.map(&:class)).to eq([ArgumentError])
      expect(transactions.sole.tags).to include("handler" => "OrderHandler")
    end
  end
end
