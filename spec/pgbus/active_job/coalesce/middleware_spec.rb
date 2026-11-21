# frozen_string_literal: true

require "spec_helper"
require "active_job"
require "rack/body_proxy"

# Issue #682: the Rack middleware wraps the request in Pgbus.coalesce and
# flushes before it hands the response back, never after the response.
RSpec.describe Pgbus::ActiveJob::Coalesce::Middleware do
  let(:adapter) { Pgbus::ActiveJob::Adapter.new }
  let(:mock_client) { build_mock_client }
  let(:sent) { [] }

  let(:job_class) do
    the_adapter = adapter
    Class.new(ActiveJob::Base) do
      self.queue_adapter = the_adapter
      def self.name = "CoalesceMiddlewareSpec::Job"
      def perform(*) = nil
    end
  end

  before do
    ActiveJob::Base.logger = Logger.new(IO::NULL)
    stub_const("CoalesceMiddlewareSpec::Job", job_class)
    allow(Pgbus).to receive(:client).and_return(mock_client)
    allow(mock_client).to receive(:send_message) { sent << :message and 1 }
    allow(mock_client).to receive(:send_batch) do |_queue, payloads, **|
      sent << :batch
      payloads.each_index.map { |i| i + 1 }
    end
  end

  def app_enqueuing(count, response: [200, {}, ["ok"]])
    klass = job_class
    lambda do |_env|
      count.times { |i| klass.perform_later(i) }
      response
    end
  end

  it "sends the request's jobs in one send_batch before call returns" do
    middleware = described_class.new(app_enqueuing(3))

    response = middleware.call({})

    expect(response).to eq([200, {}, ["ok"]])
    expect(sent).to eq([:batch])
  end

  it "propagates the flush failure out of call under on_failure: :raise (the default)" do
    allow(mock_client).to receive(:send_batch).and_raise(IOError, "gone")
    allow(mock_client).to receive(:send_message).and_raise(IOError, "gone")
    middleware = described_class.new(app_enqueuing(1))

    expect { middleware.call({}) }.to raise_error(Pgbus::EnqueueError)
  end

  it "closes the response body before the flush failure propagates" do
    allow(mock_client).to receive(:send_batch).and_raise(IOError, "gone")
    allow(mock_client).to receive(:send_message).and_raise(IOError, "gone")
    body = instance_double(Rack::BodyProxy, close: nil)
    middleware = described_class.new(app_enqueuing(1, response: [200, {}, body]))

    expect { middleware.call({}) }.to raise_error(Pgbus::EnqueueError)
    expect(body).to have_received(:close)
  end

  it "closes the response body when a callable on_failure raises" do
    allow(mock_client).to receive(:send_batch).and_raise(IOError, "gone")
    allow(mock_client).to receive(:send_message).and_raise(IOError, "gone")
    body = instance_double(Rack::BodyProxy, close: nil)
    failing = ->(*) { raise ArgumentError, "callable broke" }
    middleware = described_class.new(app_enqueuing(1, response: [200, {}, body]), on_failure: failing)

    expect { middleware.call({}) }.to raise_error(ArgumentError, "callable broke")
    expect(body).to have_received(:close)
  end

  it "keeps the flush error when closing the body raises too" do
    allow(mock_client).to receive(:send_batch).and_raise(IOError, "gone")
    allow(mock_client).to receive(:send_message).and_raise(IOError, "gone")
    body = instance_double(Rack::BodyProxy)
    allow(body).to receive(:close).and_raise(IOError, "close broke")
    allow(Pgbus.logger).to receive(:warn)
    middleware = described_class.new(app_enqueuing(1, response: [200, {}, body]))

    expect { middleware.call({}) }.to raise_error(Pgbus::EnqueueError)
    expect(Pgbus.logger).to have_received(:warn)
  end

  it "leaves the body alone when the app itself raises" do
    middleware = described_class.new(->(_env) { raise ArgumentError, "app broke" })

    expect { middleware.call({}) }.to raise_error(ArgumentError, "app broke")
  end

  it "rejects an unknown on_failure when it is built" do
    expect { described_class.new(->(_env) {}, on_failure: :ignore) }.to raise_error(ArgumentError, /on_failure/)
  end

  it "passes its on_failure through" do
    allow(mock_client).to receive(:send_batch).and_raise(IOError, "gone")
    allow(mock_client).to receive(:send_message).and_raise(IOError, "gone")
    failures = []
    middleware = described_class.new(app_enqueuing(1), on_failure: ->(jobs, _error) { failures.concat(jobs) })

    expect(middleware.call({})).to eq([200, {}, ["ok"]])
    expect(failures.size).to eq(1)
  end

  it "sends a job enqueued while the body is iterated at once" do
    klass = job_class
    body = Enumerator.new do |y|
      klass.perform_later(:streamed)
      y << "chunk"
    end
    middleware = described_class.new(->(_env) { [200, {}, body] })

    _, _, returned = middleware.call({})
    returned.each { |_| nil }

    expect(sent).to eq([:message])
  end
end
