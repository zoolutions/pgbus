# frozen_string_literal: true

require_relative "../../integration_helper"
require "active_job"
require "active_job/queue_adapters/pgbus_adapter"
require "globalid"

# Shared setup for the workflow integration specs (issue #540): a GlobalID
# subject model on its own table, StepJob on the real pgbus adapter, and a
# drain that runs StepJob messages through the real executor.
module WorkflowIntegration
  QUEUE = "default"

  def self.included(base)
    base.class_eval do
      before do
        GlobalID.app ||= "pgbus-integration"
        ActiveJob::Base.logger = Logger.new(IO::NULL)
        # Only the workflow specs use StepJob and BatchFinishedJob, so they
        # stay on :pgbus (ActiveJob::Base defaults to the in-process :async).
        Pgbus::Workflow::StepJob.queue_adapter = :pgbus
        Pgbus::Workflow::BatchFinishedJob.queue_adapter = :pgbus
        Pgbus.client.ensure_queue(QUEUE)
        WorkflowIntegration.ensure_subject_table
        stub_const("WorkflowSpecUser", WorkflowIntegration.subject_class)
        allow(Pgbus::ErrorReporter).to receive(:report)
      end

      let(:workflow_threads) { [] }

      # A thread an example started but never joined (an assertion failed
      # first) must not run into the next example's purge and cleanup.
      after do
        workflow_threads.each { |thread| thread.join(5) || thread.kill }
        Pgbus::VisibilityHeartbeat.stop
      end
    end
  end

  def self.ensure_subject_table
    conn = ActiveRecord::Base.connection
    conn.execute(<<~SQL)
      CREATE TABLE IF NOT EXISTS pgbus_spec_users (
        id BIGSERIAL PRIMARY KEY,
        name VARCHAR,
        onboarded BOOLEAN NOT NULL DEFAULT FALSE,
        closed BOOLEAN NOT NULL DEFAULT FALSE
      )
    SQL
    conn.execute("DELETE FROM pgbus_spec_users")
  end

  def self.subject_class
    Class.new(ActiveRecord::Base) do
      self.table_name = "pgbus_spec_users"
      include GlobalID::Identification
    end
  end

  # What a run looked like when run_until gave up: its step rows, its audit
  # trail and any job the executor recorded as failed, so a CI failure
  # carries its own diagnosis.
  def self.diagnose(workflow)
    steps = Pgbus::WorkflowStep.where(workflow_id: workflow.workflow_id).order(:id).map do |step|
      "  step #{step.step_name}##{step.attempt} #{step.status}/#{step.outcome.inspect} msg=#{step.msg_id} " \
        "#{step.error_class} #{step.error_message.to_s[0, 200]}"
    end
    events = workflow.events.map { |event| "  event #{event.event} #{event.actor} #{event.details.to_json}" }
    rows = ActiveRecord::Base.connection.select_all(
      "SELECT queue_name, error_class, error_message, backtrace FROM pgbus_failed_events ORDER BY id DESC LIMIT 5"
    )
    failed = rows.map do |row|
      "  failed_event #{row["queue_name"]} #{row["error_class"]}: #{row["error_message"].to_s[0, 300]}\n    " \
        "#{row["backtrace"].to_s.lines.first(8).map(&:strip).join("\n    ")}"
    end
    (steps + events + failed).join("\n")
  rescue StandardError => e
    "  (diagnosis failed: #{e.class}: #{e.message})"
  end

  def create_user(**attrs) = WorkflowSpecUser.create!(name: "user", **attrs)

  def executor = Pgbus::ActiveJob::Executor.new

  def read_step_message(vt: 30) = Pgbus.client.read_message(QUEUE, vt: vt)

  def queue_depth
    physical = Pgbus.configuration.queue_name(QUEUE)
    ActiveRecord::Base.connection.select_value("SELECT count(*) FROM pgmq.q_#{physical}").to_i
  end

  # Runs StepJob messages through the executor until the run reaches one of
  # +statuses+ or +timeout+ passes (wait: steps are PGMQ-delayed, so an empty
  # read only means "not yet").
  def run_until(workflow, statuses: %w[finished cancelled paused], timeout: 10)
    deadline = monotonic + timeout
    loop do
      return workflow if statuses.include?(workflow.reload.status)
      if monotonic > deadline
        raise "#{workflow.inspect} did not reach #{statuses.join("/")} within #{timeout}s\n#{WorkflowIntegration.diagnose(workflow)}"
      end

      message = read_step_message
      message ? executor.execute(message, QUEUE) : sleep(0.1)
    end
  end

  # Executes every message already visible, without waiting for delayed ones.
  def drain_visible
    while (message = read_step_message)
      executor.execute(message, QUEUE)
    end
  end

  def steps_of(workflow) = Pgbus::WorkflowStep.where(workflow_id: workflow.workflow_id).order(:id).to_a

  def events_of(workflow) = workflow.events.map(&:event)

  def monotonic = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)

  # A thread with its own connection checkout, returned to the pool after,
  # and joined (or killed) by the after hook if the example did not.
  def in_thread(&block)
    Thread.new { ActiveRecord::Base.connection_pool.with_connection(&block) }.tap { |thread| workflow_threads << thread }
  end

  def wait_for(timeout: 5)
    deadline = monotonic + timeout
    until yield
      raise "condition not met within #{timeout}s" if monotonic > deadline

      sleep 0.05
    end
  end
end
