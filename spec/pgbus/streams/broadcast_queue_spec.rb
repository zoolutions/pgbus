# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Streams::TurboBroadcastable do
  # install_broadcast_queue! is defined on the Pgbus::Streams parent module in
  # this same file; describing the autoloaded constant makes Zeitwerk load it
  # (Zeitwerk won't load the file for a bare Pgbus::Streams reference).

  # Minimal stand-ins for turbo-rails' broadcast job classes. Real ActiveJob
  # exposes `queue_as` (a class method that sets the queue) and `queue_name`
  # (reads it). We fake just enough surface to assert the assignment. We do
  # not load turbo-rails in unit tests.
  let(:fake_job_class) do
    # A spec double: built per example and driven from the example's thread.
    # rubocop:disable-next ThreadSafety/ClassInstanceVariable
    Class.new do
      class << self
        attr_reader :assigned_queue

        def queue_as(name)
          @assigned_queue = name
        end
      end
    end
  end

  def stub_turbo_jobs(action:, broadcast:, stream:)
    stub_const("Turbo::Streams::ActionBroadcastJob", action)
    stub_const("Turbo::Streams::BroadcastJob", broadcast)
    stub_const("Turbo::Streams::BroadcastStreamJob", stream)
  end

  it "assigns the queue to all three Turbo broadcast job classes" do
    action = fake_job_class
    broadcast = fake_job_class
    stream = fake_job_class
    stub_turbo_jobs(action: action, broadcast: broadcast, stream: stream)

    Pgbus::Streams.install_broadcast_queue!("realtime")

    expect(action.assigned_queue).to eq("realtime")
    expect(broadcast.assigned_queue).to eq("realtime")
    expect(stream.assigned_queue).to eq("realtime")
  end

  it "is a no-op when the queue name is nil" do
    action = fake_job_class
    stub_turbo_jobs(action: action, broadcast: fake_job_class, stream: fake_job_class)

    Pgbus::Streams.install_broadcast_queue!(nil)

    expect(action.assigned_queue).to be_nil
  end

  it "does not raise when a Turbo job class is not defined (turbo-rails absent)" do
    hide_const("Turbo::Streams::ActionBroadcastJob") if defined?(Turbo::Streams::ActionBroadcastJob)

    expect { Pgbus::Streams.install_broadcast_queue!("realtime") }.not_to raise_error
  end
end
