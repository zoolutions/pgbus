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
        # Only the workflow specs use StepJob, so it stays on :pgbus.
        Pgbus::Workflow::StepJob.queue_adapter = :pgbus
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
      raise "#{workflow.inspect} did not reach #{statuses.join("/")} within #{timeout}s" if monotonic > deadline

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
