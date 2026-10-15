# frozen_string_literal: true

require "spec_helper"

# In a real `pgbus start` the app boots first, so the engine's Pgbus.finalize!
# has frozen the configuration's collections before the supervisor plans its
# forks and each fork builds its Worker or Consumer (issue #527). Everything
# the process model takes from the configuration must work frozen.
RSpec.describe "the process model on a finalized configuration" do # rubocop:disable RSpec/DescribeClass
  let(:config) do
    Pgbus::Configuration.new.tap do |c|
      c.logger = Logger.new(IO::NULL)
      c.workers = [{ name: "mail", queues: %w[default mailers], threads: 2, processes: 2 }]
      c.event_consumers = [{ topics: %w[orders.#], threads: 2 }]
      c.recurring_tasks = { "cleanup" => { "class" => "CleanupJob", "schedule" => "every hour" } }
      c.finalize!
    end
  end

  before do
    allow(Pgbus).to receive(:client).and_return(build_mock_client)
    allow(Pgbus::Process::Heartbeat).to receive(:new)
      .and_return(instance_double(Pgbus::Process::Heartbeat, start: true, stop: true))
  end

  it "has frozen the worker settings the supervisor hands to a fork" do
    expect(config.workers.first[:queues]).to be_frozen
  end

  it "plans every worker and consumer fork" do
    supervisor = Pgbus::Process::Supervisor.new(config: config)
    allow(supervisor).to receive(:fork).and_return(4242, 4243, 4244)

    supervisor.send(:boot_workers)
    supervisor.send(:boot_consumers)

    expect(supervisor.instance_variable_get(:@intended_children)).to eq(3)
  end

  it "builds a Worker from a finalized capsule that can still evict a deleted queue" do
    capsule = config.workers.first
    worker = Pgbus::Process::Worker.new(queues: capsule[:queues], threads: capsule[:threads], config: config,
                                        capsule: capsule[:name])
    missing = StandardError.new('relation "pgmq.q_pgbus_mailers" does not exist')

    worker.send(:evict_missing_queues, missing)

    expect(worker.queues).to eq(%w[default])
  end

  it "builds a Consumer from a finalized consumer entry" do
    allow(Pgbus::EventBus::Registry).to receive(:instance)
      .and_return(instance_double(Pgbus::EventBus::Registry, subscribers: [], queue_names_for_topics: []))

    consumer = Pgbus::Process::Consumer.new(topics: config.event_consumers.first[:topics], config: config)

    expect(consumer.topics).to eq(%w[orders.#])
  end

  it "loads the recurring schedule from finalized task settings" do
    schedule = Pgbus::Recurring::Schedule.new(config: config)

    expect(schedule.tasks.map(&:key)).to eq(%w[cleanup])
  end
end
