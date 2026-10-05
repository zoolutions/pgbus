# frozen_string_literal: true

require_relative "worker_profile_runner"

# Drives one consumer cell of the worker profiling bench (issue #486): a real
# Pgbus::Process::Consumer draining a real event-bus topic queue. It reuses
# WorkerProfileRunner's global setup, measurement and vernier wiring, so the
# worker and consumer numbers come from the same harness.
#
# The handler is a plain Pgbus::EventBus::Handler, NOT `idempotent!`: the cell
# measures the consumer's claim loop and transport, not the dedup table's
# claim-row cost (that path is its own bench).
module ConsumerProfileRunner
  QUEUE = "wprof_events"
  PATTERN = "wprof.#"
  ROUTING_KEY = "wprof.bench"

  class WprofHandler < Pgbus::EventBus::Handler
    def self.name = "ConsumerProfileRunner::WprofHandler"

    def handle(_event); end
  end

  module_function

  def setup!(database_url, threads:)
    WorkerProfileRunner.setup!(database_url, threads: threads)
    registry = Pgbus::EventBus::Registry.instance
    registry.clear!
    registry.subscribe(PATTERN, WprofHandler, queue_name: QUEUE)
    registry.setup_all!
    reset!
  end

  def reset!
    Pgbus.client.purge_queue(QUEUE)
    WorkerProfileRunner.reset!
  end

  # Publishes `jobs` events (not timed), then drains them with a real
  # Consumer. Same return shape as WorkerProfileRunner.drain. A nil read_ahead
  # is not passed, so the bench runs against a Consumer without the keyword.
  def drain(jobs:, threads:, read_ahead: nil, profile_path: nil)
    reset!
    jobs.times { |i| Pgbus::EventBus::Publisher.publish(ROUTING_KEY, { "n" => i }) }
    Pgbus.stopping = false
    options = { topics: [PATTERN], threads: threads }
    options[:read_ahead] = read_ahead unless read_ahead.nil?
    consumer = Pgbus::Process::Consumer.new(**options)

    measurement = WorkerProfileRunner.drive(consumer, profile_path) { consumer.jobs_processed >= jobs }

    # Consumer#handle_message rescues a failure, counts it as processed and
    # leaves the message in the queue for redelivery. An empty queue is the
    # proof that every event was handled and archived.
    left = Pgbus.client.metrics(QUEUE).queue_length.to_i
    raise "#{left} events left in #{QUEUE} after the drain — the bench measured errors, not work" if left.positive?

    measurement.merge(jobs: jobs)
  end
end
