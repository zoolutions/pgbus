# frozen_string_literal: true

require "json"

module Pgbus
  # One signal delivered to a workflow run (pgbus_workflow_signals, issue
  # #541): pending (buffered, no step has taken it), claimed (a wait_for:
  # step took it and runs with it as received_signal) and consumed (that
  # step ended). Written only by Pgbus::Workflow::Signals under the run's row
  # lock.
  #
  # +payload+ reads back the signal! payload with indifferent access
  # (through ActiveJob::Arguments, as workflow arguments do).
  class WorkflowSignal < BusRecord
    self.table_name = "pgbus_workflow_signals"

    STATUSES = %w[pending claimed consumed].freeze

    # Inserts a signal. A second signal with the same (workflow_id,
    # idempotency_key) inserts nothing and returns nil. Raw SQL rather than
    # insert_all(unique_by:), which resolves the index through the schema
    # cache (issue #401), as WorkflowEntry.insert_run! does.
    def self.insert_signal!(workflow_id:, name:, payload:, idempotency_key:, now: Time.current)
      sql = <<~SQL
        INSERT INTO #{table_name} (workflow_id, name, payload, idempotency_key, status, created_at)
        VALUES ($1, $2, $3, $4, 'pending', $5)
        ON CONFLICT (workflow_id, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
        RETURNING id
      SQL
      result = connection.exec_query(sql, "WorkflowSignal Insert",
                                     [workflow_id, name, JSON.generate(payload), idempotency_key, now])
      id = result.rows.first&.first
      id && find(id)
    end

    # The pending signals of a run, oldest first; only +name+'s when given.
    def self.pending_for(workflow_id, name: nil)
      scope = where(workflow_id: workflow_id, status: "pending").order(:id)
      name ? scope.where(name: name.to_s) : scope
    end

    # The signal an earlier attempt of +step_name+ claimed and did not
    # consume (a reattempt, or a resume! after a failure, runs with it).
    def self.held_by_step(workflow_id, step_name)
      attempts = WorkflowStep.where(workflow_id: workflow_id, step_name: step_name.to_s).select(:id)
      where(workflow_id: workflow_id, status: "claimed", claimed_by_step_id: attempts).order(:id).first
    end

    # pending → claimed, by +step_id+. Returns the number of rows moved.
    def self.claim!(id, step_id:, now: Time.current)
      where(id: id, status: "pending").update_all(status: "claimed", claimed_by_step_id: step_id, claimed_at: now)
    end

    # claimed → consumed. Returns the number of rows moved.
    def self.consume!(id, now: Time.current)
      where(id: id, status: "claimed").update_all(status: "consumed", consumed_at: now)
    end

    attr_writer :duplicate_delivery

    # True on what signal! returned for a redelivery: a signal with the same
    # idempotency key was already stored, and nothing else happened.
    def duplicate_delivery? = @duplicate_delivery == true

    def payload
      return @payload if defined?(@payload)

      @payload = Workflow::Payload.load(super) || {}.with_indifferent_access
    end

    def reload(...)
      remove_instance_variable(:@payload) if defined?(@payload)
      super
    end
  end
end
