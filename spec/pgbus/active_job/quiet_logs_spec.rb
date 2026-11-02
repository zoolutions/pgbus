# frozen_string_literal: true

require "spec_helper"
require "active_job"
require "active_job/log_subscriber"

# Issue #618: config.quiet_active_job_logs drops ActiveJob's per-job
# "Performing …" / "Performed …" lines in a worker process by unsubscribing the
# LogSubscriber from perform_start.active_job and perform.active_job only.
# Each example builds its own Fanout and subscribes a LogSubscriber instance
# per event (the shape attach_to produces) without touching the class-level
# attach_to state the global subscriber lives in.
RSpec.describe Pgbus::ActiveJob::QuietLogs do
  let(:fanout) { ActiveSupport::Notifications::Fanout.new }
  let(:log_subscriber) { ActiveJob::LogSubscriber.new }
  let(:events) { %w[perform_start perform enqueue retry_stopped discard].map { |e| "#{e}.active_job" } }

  before { events.each { |name| fanout.subscribe(name, log_subscriber) } }

  def log_listener?(name)
    fanout.all_listeners_for(name).any? { |l| l.respond_to?(:delegate) && l.delegate.is_a?(ActiveJob::LogSubscriber) }
  end

  it "unsubscribes ActiveJob's log subscriber from perform_start and perform" do
    described_class.apply!(notifier: fanout)

    expect(log_listener?("perform_start.active_job")).to be(false)
    expect(log_listener?("perform.active_job")).to be(false)
  end

  it "leaves every other ActiveJob event's log line subscribed" do
    described_class.apply!(notifier: fanout)

    %w[enqueue retry_stopped discard].each do |event|
      expect(log_listener?("#{event}.active_job")).to be(true), "#{event}.active_job lost its log subscriber"
    end
  end

  it "leaves another listener on perform.active_job subscribed" do
    seen = []
    fanout.subscribe("perform.active_job", ->(*args) { seen << args.first })

    described_class.apply!(notifier: fanout)
    fanout.publish("perform.active_job", Time.now, Time.now, "id", {})

    expect(seen).to eq(["perform.active_job"])
  end

  # listeners_for skips a silenced listener (its logger above info); the
  # LogSubscriber must still go, or lowering the level later brings it back.
  it "unsubscribes a silenced log subscriber too" do
    allow(log_subscriber).to receive(:silenced?).and_return(true)

    described_class.apply!(notifier: fanout)
    allow(log_subscriber).to receive(:silenced?).and_return(false)

    expect(log_listener?("perform.active_job")).to be(false)
  end

  it "is a no-op the second time for the same notifier, true the first" do
    expect(described_class.apply!(notifier: fanout)).to be(true)
    fanout.subscribe("perform.active_job", log_subscriber)

    expect(described_class.apply!(notifier: fanout)).to be(false)
    expect(log_listener?("perform.active_job")).to be(true)
  end

  it "applies once per notifier, not once per process" do
    other = ActiveSupport::Notifications::Fanout.new
    other.subscribe("perform.active_job", log_subscriber)
    described_class.apply!(notifier: fanout)

    expect(described_class.apply!(notifier: other)).to be(true)
    expect(other.all_listeners_for("perform.active_job")).to be_empty
  end
end
