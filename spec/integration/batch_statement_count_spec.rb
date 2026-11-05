# frozen_string_literal: true

require_relative "../integration_helper"

# Issue #650: a batch member's completion signal on the migrated path used to
# run nine or ten statements in two transactions (delete the execution row,
# lock and fetch the batch, increment, then finish and refetch). It now runs
# two statements: one CTE that deletes the row and counts it into the batch
# (RETURNING the row), and the single-winner finish UPDATE (RETURNING the
# row), which keeps its transaction for the READ COMMITTED re-check the
# winner runs.
RSpec.describe "Batch completion statement count (integration)", :integration do
  let(:batch_id) { SecureRandom.uuid }
  let(:job_ids) { Array.new(3) { SecureRandom.uuid } }

  before do
    Pgbus::Batch.reset_executions_migrated_cache!
    Pgbus::BatchEntry.create!(batch_id: batch_id, status: "processing", total_jobs: 3, properties: "{}")
    job_ids.each { |job_id| Pgbus::BatchExecution.insert_for!(batch_id: batch_id, job_id: job_id, queue_name: "default") }
    # The first member warms the schema probe and the column cache, so the
    # members measured below see only their own statements.
    Pgbus::Batch.job_completed(batch_id, job_id: job_ids[0])
  end

  # Every sql.active_record event the block runs, SCHEMA lookups left out,
  # as [name, sql].
  def statements_during(&)
    events = []
    callback = lambda do |*, payload|
      events << [payload[:name], payload[:sql]] unless payload[:name] == "SCHEMA"
    end
    result = ActiveSupport::Notifications.subscribed(callback, "sql.active_record", &)
    [events, result]
  end

  it "completes a middle member in two statements and one finish transaction" do
    events, result = statements_during { Pgbus::Batch.job_completed(batch_id, job_id: job_ids[1]) }

    expect(events.map(&:first)).to eq(["Pgbus Batch Count", "TRANSACTION", "Pgbus Batch Finish", "TRANSACTION"])
    expect(events.map(&:last).values_at(1, 3)).to eq(%w[BEGIN COMMIT])
    expect(result[:just_finished]).to be(false)
    expect(result[:record].completed_jobs).to eq(2)
  end

  # The finish adds the re-check. What finish_if_needed runs after the
  # COMMIT (callbacks, the run-lock release) is not this change's and is not
  # pinned here.
  it "adds one re-check for the last member, which wins the finish" do
    Pgbus::Batch.job_completed(batch_id, job_id: job_ids[1])

    events, result = statements_during { Pgbus::Batch.job_completed(batch_id, job_id: job_ids[2]) }

    expect(events.map(&:first).first(5)).to eq(
      ["Pgbus Batch Count", "TRANSACTION", "Pgbus Batch Finish", "Pgbus::BatchExecution Exists?", "TRANSACTION"]
    )
    expect(events.map(&:last).values_at(1, 4)).to eq(%w[BEGIN COMMIT])
    expect(events[3].last).to include("pgbus_batch_executions")
    expect(result[:just_finished]).to be(true)
  end

  it "hands finish_if_needed a record built from RETURNING" do
    Pgbus::Batch.job_completed(batch_id, job_id: job_ids[1])
    Pgbus::Batch.job_discarded(batch_id, job_id: job_ids[2]).then do |result|
      record = result[:record]

      expect(record).to be_a(Pgbus::BatchEntry)
      expect(record).to be_persisted
      expect(record.batch_id).to eq(batch_id)
      expect([record.total_jobs, record.completed_jobs, record.failed_jobs]).to eq([3, 2, 1])
      expect(record.status).to eq("finished")
      expect(record.finished_at).to be_present
      expect(record.properties).not_to be_nil
    end
  end
end
