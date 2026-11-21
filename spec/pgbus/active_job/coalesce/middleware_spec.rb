# frozen_string_literal: true

require "spec_helper"
require "active_job"

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
