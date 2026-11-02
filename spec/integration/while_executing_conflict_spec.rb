# frozen_string_literal: true

require_relative "../integration_helper"
require "active_job"
require "active_job/queue_adapters/pgbus_adapter"

# Issue #629: a :while_executing duplicate that meets another message holding
# its key follows the job's on_conflict policy. Before, every policy skipped
# the copy without touching its visibility timeout, so each read spent one of
# max_retries and a holder that outlived max_retries reads dead-lettered every
# queued copy with no error.
#
# Six enqueues for one key; the first copy's lock is taken bound to its own
# message (as the executor does when it starts that copy), so the other five
# meet a holder that is another message.
RSpec.describe ":while_executing duplicates (integration)", :integration do
  let(:client) { Pgbus.client }
  let(:config) { Pgbus.configuration }
  let(:executor) { Pgbus::ActiveJob::Executor.new(client: client) }
  let(:runs) { Concurrent::Array.new }

  before do
    Pgbus::UniquenessKey.delete_all
    ActiveJob::Base.queue_adapter = :pgbus
  end

  after { ActiveJob::Base.queue_adapter = :test }

  def build_rollup_job(policy, runs)
    Class.new(ActiveJob::Base) do
      include Pgbus::Uniqueness

      ensures_uniqueness strategy: :while_executing, key: ->(parent_id) { "rollup-#{parent_id}" }, on_conflict: policy

      define_method(:perform) { |parent_id| runs << parent_id }
    end
  end

  def enqueue_six_and_hold_the_first
    6.times { RollupJob.perform_later(42) }
    messages = client.read_batch("default", qty: 10, vt: 30)
    expect(messages.size).to eq(6)
    holder, *duplicates = messages.sort_by { |m| m.msg_id.to_i }
    Pgbus::UniquenessKey.acquire!("rollup-42", queue_name: "default", msg_id: holder.msg_id.to_i)
    [holder, duplicates]
  end

  def queue_rows(msg_ids)
    ActiveRecord::Base.connection.select_rows(
      "SELECT msg_id, EXTRACT(EPOCH FROM (vt - clock_timestamp())) FROM pgmq.q_#{config.queue_name("default")} " \
      "WHERE msg_id IN (#{msg_ids.map(&:to_i).join(",")}) ORDER BY msg_id"
    )
  end

  def dlq_size
    client.ensure_dead_letter_queue("default")
    ActiveRecord::Base.connection.select_value("SELECT count(*) FROM pgmq.q_#{config.dead_letter_queue_name("default")}").to_i
  end

  context "with on_conflict: :discard" do
    before { stub_const("RollupJob", build_rollup_job(:discard, runs)) }

    it "runs one copy and completes the five duplicates without running them" do
      holder, duplicates = enqueue_six_and_hold_the_first

      expect(duplicates.map { |m| executor.execute(m, "default") }).to all(eq(:discarded))
      expect(executor.execute(holder, "default")).to eq(:success)

      expect(runs).to eq([42])
      expect(queue_rows(duplicates.map(&:msg_id) + [holder.msg_id])).to be_empty
      ids = (duplicates.map(&:msg_id) + [holder.msg_id]).map(&:to_i)
      archived = ActiveRecord::Base.connection.select_value(
        "SELECT count(*) FROM pgmq.a_#{config.queue_name("default")} WHERE msg_id IN (#{ids.join(",")})"
      ).to_i
      expect(archived).to eq(6)
      expect(dlq_size).to eq(0)
      expect(Pgbus::UniquenessKey.locked?("rollup-42")).to be(false)
    end
  end

  context "with on_conflict: :reject" do
    before { stub_const("RollupJob", build_rollup_job(:reject, runs)) }

    it "backs every skipped duplicate off for at least the visibility timeout and runs them after the holder" do
      holder, duplicates = enqueue_six_and_hold_the_first

      expect(duplicates.map { |m| executor.execute(m, "default") }).to all(eq(:skipped))

      # Still queued, none visible before the read's own timeout would have
      # let it back: the first skip is visibility_timeout * 2^0, jittered up only.
      remaining = queue_rows(duplicates.map(&:msg_id)).map { |(_, seconds)| seconds.to_f }
      expect(remaining.size).to eq(5)
      expect(remaining).to all(be > config.visibility_timeout - 2)

      # The second skip of each waits twice as long (jitter is at most 15%):
      # repeated skips stretch over the holder's run instead of spinning on
      # the flat timeout.
      duplicates.each { |m| client.set_visibility_timeout("default", m.msg_id.to_i, vt: 0) }
      second = Array.new(5) { client.read_message("default", vt: 30) }
      expect(second.map { |m| m.read_ct.to_i }).to all(eq(2))
      expect(second.map { |m| executor.execute(m, "default") }).to all(eq(:skipped))
      stretched = queue_rows(duplicates.map(&:msg_id)).map { |(_, seconds)| seconds.to_f }
      expect(stretched).to all(be > (config.visibility_timeout * 2 * 0.85) - 2)

      expect(executor.execute(holder, "default")).to eq(:success)
      expect(Pgbus::UniquenessKey.locked?("rollup-42")).to be(false)

      # The holder is done: each duplicate now runs, one after another.
      duplicates.each { |m| client.set_visibility_timeout("default", m.msg_id.to_i, vt: 0) }
      results = duplicates.map do
        message = client.read_message("default", vt: 30)
        executor.execute(message, "default")
      end

      expect(results).to all(eq(:success))
      expect(runs).to eq([42] * 6)
      expect(dlq_size).to eq(0)
    end

    # read_ct cannot be decremented, so a copy skipped on every read still
    # dead-letters once it passes max_retries — but says why.
    it "dead-letters a copy skipped past max_retries as a uniqueness conflict" do
      _holder, duplicates = enqueue_six_and_hold_the_first
      copy = duplicates.first
      config.max_retries.times do |i|
        message = i.zero? ? copy : client.read_message("default", vt: 30)
        expect(message.msg_id).to eq(copy.msg_id)
        expect(executor.execute(message, "default")).to eq(:skipped)
        duplicates.drop(1).each { |m| client.set_visibility_timeout("default", m.msg_id.to_i, vt: 3600) }
        client.set_visibility_timeout("default", copy.msg_id.to_i, vt: 0)
      end

      last = client.read_message("default", vt: 30)
      expect(last.read_ct.to_i).to eq(config.max_retries + 1)
      expect(executor.execute(last, "default")).to eq(:dead_lettered)

      dead = client.read_message(config.dead_letter_queue_name("default").delete_prefix("#{config.queue_prefix}_"),
                                 vt: 30)
      expect(Pgbus::DeadLetterHeader.parse(dead.headers)).to include(
        "reason" => "uniqueness_conflict", "attempts" => config.max_retries + 1
      )
      expect(Pgbus::UniquenessKey.locked?("rollup-42")).to be(true)
    end
  end
end
