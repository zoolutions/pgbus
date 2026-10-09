# frozen_string_literal: true

require_relative "../integration_helper"

# The Jobs page's state CASE is SQL; only a real PGMQ proves it (issue #489).
# One message per state in one queue, plus an orphaned failed row and a
# blocked execution, then job_rows(state:) and job_state_counts must agree.
RSpec.describe "Dashboard job list (integration)", :integration do
  let(:client) { Pgbus.client }
  let(:data_source) { Pgbus::Web::DataSource.new(client: client) }
  let(:queue) { "joblist" }
  let(:physical) { Pgbus.configuration.queue_name(queue) }

  # Messages are read in msg_id order, so each read claims the one just sent.
  let!(:ids) do
    client.ensure_queue(queue)
    client.purge_queue(queue)
    conn = Pgbus::BusRecord.connection
    conn.exec_delete("DELETE FROM pgbus_failed_events WHERE queue_name IN ($1, $2)", "test", [queue, physical])
    conn.exec_delete("DELETE FROM pgbus_blocked_executions WHERE queue_name IN ($1, $2)", "test", [queue, physical])

    running = client.send_message(queue, payload("RunningJob")).to_i
    read_one(vt: 60)

    retrying = client.send_message(queue, payload("RetryingJob")).to_i
    read_one(vt: 60)
    fail_attempt(retrying, error: Timeout::Error.new("slow"))

    retry_in_progress = client.send_message(queue, payload("SecondAttemptJob")).to_i
    read_one(vt: 60)
    fail_attempt(retry_in_progress)
    client.set_visibility_timeout(queue, retry_in_progress, vt: 0)
    sleep 0.01
    client.read_batch(queue, qty: 5, vt: 60) # re-claims only the expired one

    lease_expired = client.send_message(queue, payload("LeaseJob")).to_i
    read_one(vt: 0)

    ready = client.send_message(queue, payload("ReadyJob")).to_i
    scheduled = client.send_message(queue, payload("LaterJob"), delay: 3600).to_i

    fail_attempt(987_654) # failed row whose message is gone
    Pgbus::BlockedExecution.create!(concurrency_key: "Import:1", queue_name: queue, payload: payload("ParkedJob"),
                                    expires_at: 1.hour.from_now)

    { running: running, retrying: retrying, retry_in_progress: retry_in_progress,
      lease_expired: lease_expired, ready: ready, scheduled: scheduled }
  end

  def payload(name) = { "job_class" => name, "arguments" => [] }

  def fail_attempt(msg_id, error: StandardError.new("boom"))
    Pgbus::FailedEventRecorder.record!(queue_name: queue, msg_id: msg_id, payload: payload("FailJob"),
                                       headers: nil, error: error, retry_count: 0)
  end

  def read_one(vt:)
    client.read_batch(queue, qty: 1, vt: vt).first.msg_id.to_i
  end

  def states_by_msg_id(rows)
    rows.select { |r| r[:source] == "queue" }.to_h { |r| [r[:msg_id], r[:state]] }
  end

  it "derives every state in SQL" do
    rows = data_source.job_rows(queue_name: physical)

    expect(states_by_msg_id(rows)).to eq(
      ids[:running] => "running",
      ids[:retrying] => "retrying",
      ids[:retry_in_progress] => "running",
      ids[:lease_expired] => "ready",
      ids[:ready] => "ready",
      ids[:scheduled] => "scheduled"
    )
    orphan = rows.find { |r| r[:source] == "failed" }
    expect(orphan).to include(state: "retrying", msg_id: 987_654, logical_queue: queue)
    blocked = rows.find { |r| r[:source] == "blocked" }
    expect(blocked).to include(state: "blocked", concurrency_key: "Import:1", job_class: "ParkedJob")
  end

  it "joins the failure onto its queue row" do
    row = data_source.job_rows(queue_name: physical, state: "retrying").find { |r| r[:source] == "queue" }

    expect(row).to include(msg_id: ids[:retrying], error_class: "Timeout::Error", job_class: "RetryingJob")
    expect(row[:failed_event_id]).to be_a(Integer)
  end

  it "filters each tab by state" do
    expect(data_source.job_rows(queue_name: physical, state: "scheduled").map { |r| r[:msg_id] })
      .to eq([ids[:scheduled]])
    expect(data_source.job_rows(queue_name: physical, state: "blocked").map { |r| r[:source] }).to eq(["blocked"])
    expect(data_source.job_rows(queue_name: physical, state: "retrying").map { |r| r[:source] })
      .to contain_exactly("queue", "failed")
  end

  it "pages across sources by time" do
    page_one = data_source.job_rows(queue_name: physical, page: 1, per_page: 5)
    page_two = data_source.job_rows(queue_name: physical, page: 2, per_page: 5)

    expect(page_one.size).to eq(5)
    expect(page_two.size).to eq(3)
    expect((page_one + page_two).map { |r| [r[:source], r[:id]] }.uniq.size).to eq(8)
  end

  it "counts every state in one query" do
    counts = data_source.job_state_counts(queue_name: physical)

    expect(%w[all ready scheduled running retrying blocked].to_h { |s| [s, counts[s]] })
      .to eq("all" => 8, "ready" => 2, "scheduled" => 1, "running" => 2, "retrying" => 2, "blocked" => 1)
    expect(counts.capped?("all")).to be(false)
  end

  it "counts the ready jobs ahead of each ready row" do
    rows = data_source.job_rows(queue_name: physical, state: "ready")

    ahead = data_source.jobs_ahead(rows)

    expect(ahead).to eq([physical, ids[:lease_expired]] => 0, [physical, ids[:ready]] => 1)
  end
end
