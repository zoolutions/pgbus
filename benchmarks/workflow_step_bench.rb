# frozen_string_literal: true

require_relative "bench_helper"
require "active_job"

# The gem's overhead per workflow step (issue #540): one StepExecution — claim
# under the run's lock, the guards, the body, the outcome, the next step row
# and the audit events — with the three models replaced by in-memory fakes. No
# database: this is pgbus's own CPU and allocation cost per step, not the four
# round trips a real step makes (claim, finish, next-row insert, events), which
# dominate it in production.

puts "=" * 60
puts "Pgbus Workflow StepJob Benchmarks (mocked models — measures gem overhead)"
puts "=" * 60

ActiveJob::Base.logger = Logger.new(IO::NULL)

# The benchmarked workflow, and in-memory stand-ins for the rows the StepJob
# reads and writes.
module WorkflowBenchFakes
  class BenchStepWorkflow < Pgbus::Workflow
    step(:charge) { nil }
    step(:receipt) { nil }
  end

  Row = Struct.new(:id, :workflow_id, :workflow_class, :step_name, :attempt, :status, :scheduled_at, :started_at,
                   :stall_at, :msg_id, :worker, :app_revision, :current_step, keyword_init: true) do
    def assign_attributes(attrs) = attrs.each { |key, value| self[key] = value if members.include?(key) }

    def workflow_klass = BenchStepWorkflow
  end

  # Runner.locked asks the connection whether an app transaction is open
  # (Notes#flush_after_commit); the fakes run outside any.
  NO_TRANSACTION = Object.new.tap { |connection| connection.define_singleton_method(:transaction_open?) { false } }

  module_function

  def install!
    run = Row.new(workflow_id: "wf-bench", workflow_class: BenchStepWorkflow.name, status: "ready", current_step: "charge")
    step = Row.new(id: 1, workflow_id: "wf-bench", workflow_class: BenchStepWorkflow.name, step_name: "charge", attempt: 1,
                   status: "scheduled", scheduled_at: Time.now - 1)
    define(Pgbus::WorkflowEntry, :transaction) { |*, &block| block.call }
    define(Pgbus::WorkflowEntry, :connection) { NO_TRANSACTION }
    define(Pgbus::WorkflowEntry, :lock_for) { |_id| run }
    define(Pgbus::WorkflowEntry, :transition!) do |_id, to:, **|
      run.status = to
      1
    end
    define(Pgbus::WorkflowStep, :find_by) do |id:|
      step.status = "scheduled"
      run.status = "ready"
      step if id == 1
    end
    define(Pgbus::WorkflowStep, :claim!) { |*, **| 1 }
    define(Pgbus::WorkflowStep, :finish!) { |*, **| 1 }
    define(Pgbus::WorkflowStep, :insert_next!) { |**attrs| Row.new(id: 2, attempt: 1, **attrs.slice(:step_name, :scheduled_at)) }
    define(Pgbus::WorkflowEvent, :record!) { |**| nil }
  end

  def define(model, name, &) = model.define_singleton_method(name, &)
end

WorkflowBenchFakes.install!
delivery = { msg_id: 42, read_ct: 1, queue_name: "default" }.freeze
run_step = -> { Pgbus::Workflow::StepExecution.new(1, delivery: delivery).call }

5.times { run_step.call }

BenchSupport.header("StepExecution#call (claim → body → outcome → next row → events)")
BenchSupport.ips(time: 5, warmup: 2) do |x|
  x.report("one workflow step") { run_step.call }
end

BenchSupport.header("Allocations per step (1000 steps)")
report = BenchSupport.allocations("1000 steps") { 1000.times { run_step.call } }
puts format("  per step: ~%<objects>d objects, ~%<bytes>d bytes, retained %<retained>d objects",
            objects: report.total_allocated / 1000, bytes: report.total_allocated_memsize / 1000,
            retained: report.total_retained / 1000)

BenchSupport.header("Top allocation sites (pgbus files)")
pgbus_lib = File.expand_path("../lib/pgbus", __dir__)
report.allocated_objects_by_location
      .select { |row| row[:data].start_with?(pgbus_lib) }
      .first(10)
      .each { |row| puts format("  %6d  %s", row[:count] / 1000, row[:data].delete_prefix("#{pgbus_lib}/")) }
