# frozen_string_literal: true

require "spec_helper"

require_relative "../../../lib/pgbus/web/data_source"

# Issues #631, #636: a re-sent job payload takes its uniqueness key and its
# concurrency slot the way an enqueue does. The happy paths, the held key and
# the full limit run against a real database in
# spec/integration/dashboard_resend_uniqueness_spec.rb and
# dashboard_resend_concurrency_spec.rb; these cover the failures around the
# send.
RSpec.describe Pgbus::Web::DataSource::Resend do
  subject(:data_source) { Pgbus::Web::DataSource.new(client: mock_client) }

  let(:mock_client) { double("Pgbus::Client", pgmq: double("pgmq")) }
  let(:mock_connection) { double("ActiveRecord::Connection") }
  let(:payload) do
    { "job_class" => "OrderJob", "job_id" => "j-1", "executions" => 0,
      "pgbus_uniqueness_key" => "order-1", "pgbus_uniqueness_strategy" => "until_executed" }
  end
  let(:holder) { ["default", 9, "2026-11-02 10:00:00+00"] }
  # What acquire! stamps the row with (and keeps for this thread's bind!).
  let(:acquired_at) { "2026-11-02 10:06:00.654321" }

  before do
    allow(Pgbus::BusRecord).to receive(:connection).and_return(mock_connection)
    allow(Pgbus::UniquenessKey).to receive(:unbind!).and_return(nil)
    allow(Pgbus::UniquenessKey).to receive(:acquire!) do |key, **|
      (Thread.current[:pgbus_uniqueness_created_at] ||= {})[key] = acquired_at
      true
    end
    allow(Pgbus::UniquenessKey).to receive(:release_if_unbound!)
    allow(Pgbus::UniquenessKey).to receive(:bind!)
    allow(Pgbus::UniquenessKey).to receive(:release!)
    allow(Pgbus::UniquenessKey).to receive(:clear_bind_stamp!)
    allow(Pgbus.logger).to receive(:warn)
  end

  after { Thread.current[:pgbus_uniqueness_created_at] = nil }

  def resend(payload, drop_source: -> {}, &)
    data_source.send(:resend_job, payload, lock_queue: "default", holder: holder, drop_source: drop_source, &)
  end

  it "acquires a free key before the send and binds it to the new message by its stamp" do
    expect(resend(payload) { "17" }).to eq(:sent)

    expect(Pgbus::UniquenessKey).to have_received(:acquire!).with("order-1", queue_name: "default", msg_id: 0)
    expect(Pgbus::UniquenessKey).to have_received(:bind!)
      .with("order-1", queue_name: "default", msg_id: 17, acquired_at: acquired_at)
    expect(Pgbus::UniquenessKey).to have_received(:clear_bind_stamp!).with("order-1")
  end

  it "gives the key back by its stamp and re-raises when the send fails" do
    expect { resend(payload) { raise StandardError, "produce failed" } }.to raise_error(StandardError, "produce failed")

    expect(Pgbus::UniquenessKey).to have_received(:release_if_unbound!).with("order-1", acquired_before: acquired_at)
    expect(Pgbus::UniquenessKey).not_to have_received(:release!)
    expect(Pgbus::UniquenessKey).not_to have_received(:bind!)
    expect(Pgbus::UniquenessKey).to have_received(:clear_bind_stamp!).with("order-1")
  end

  it "keeps the key when the send's outcome is unknown (the copy may be live)" do
    stub_const("PG::Error", Class.new(StandardError)) unless defined?(PG::Error)

    expect { resend(payload) { raise PG::Error, "server closed the connection" } }.to raise_error(PG::Error)

    expect(Pgbus::UniquenessKey).not_to have_received(:release_if_unbound!)
    expect(Pgbus.logger).to have_received(:warn) do |&block|
      expect(block.call).to include("Re-send of order-1 may have been sent (PG::Error); its key stays held")
    end
  end

  it "logs a failed give-back and raises the send's error" do
    allow(Pgbus::UniquenessKey).to receive(:release_if_unbound!).and_raise(StandardError, "release failed")

    expect { resend(payload) { raise ArgumentError, "produce failed" } }.to raise_error(ArgumentError, "produce failed")

    expect(Pgbus.logger).to have_received(:warn) do |&block|
      expect(block.call).to include("Giving back the uniqueness key order-1 failed: release failed")
    end
  end

  it "logs a failed bind and still reports the copy as sent" do
    allow(Pgbus::UniquenessKey).to receive(:bind!).and_raise(StandardError, "connection lost")

    expect(resend(payload) { "17" }).to eq(:sent)

    expect(Pgbus.logger).to have_received(:warn) do |&block|
      expect(block.call).to include("Uniqueness bind of re-sent order-1 failed: connection lost")
    end
  end

  # PGMQ sends on its own connection: a failed delete of the failed row
  # cannot undo the send, so the live copy keeps its key.
  it "binds a re-sent failed job's key before deleting its failed row, and keeps it when the delete fails" do
    event = { "id" => 1, "queue_name" => "default", "msg_id" => 42, "payload" => JSON.generate(payload), "headers" => nil }
    allow(mock_client).to receive(:send_message).and_return("17")
    allow(mock_connection).to receive(:exec_delete).and_raise(StandardError, "delete failed")

    expect { data_source.send(:resend_failed_event, event) }.to raise_error(StandardError, "delete failed")

    expect(Pgbus::UniquenessKey).to have_received(:bind!)
      .with("order-1", queue_name: "default", msg_id: 17, acquired_at: acquired_at)
    expect(Pgbus::UniquenessKey).not_to have_received(:release_if_unbound!)
  end

  describe "the row the source message still holds" do
    let(:stamp) { "2026-11-02 10:05:00.123456" }

    before { allow(Pgbus::UniquenessKey).to receive(:unbind!).and_return(stamp) }

    it "is taken over for the copy and bound to it by its stamp, without acquiring" do
      expect(resend(payload) { "17" }).to eq(:sent)

      expect(Pgbus::UniquenessKey).to have_received(:unbind!).with(
        "order-1", from_queue_name: "default", from_msg_id: 9, from_enqueued_at: "2026-11-02 10:00:00+00",
                   queue_name: "default"
      )
      expect(Pgbus::UniquenessKey).not_to have_received(:acquire!)
      expect(Pgbus::UniquenessKey).to have_received(:bind!)
        .with("order-1", queue_name: "default", msg_id: 17, acquired_at: stamp)
    end

    it "goes by its stamp when the send fails" do
      expect { resend(payload) { raise StandardError, "produce failed" } }.to raise_error(StandardError)

      expect(Pgbus::UniquenessKey).to have_received(:release_if_unbound!).with("order-1", acquired_before: stamp)
    end
  end

  it "sends nothing, and says why, while another copy holds the key" do
    allow(Pgbus::UniquenessKey).to receive(:acquire!).and_return(false)
    sent = false

    expect(resend(payload) { sent = true }).to be(false)

    expect(sent).to be(false)
    expect(Pgbus.logger).to have_received(:warn) do |&block|
      expect(block.call).to include("Not re-sending OrderJob: uniqueness key order-1 is held by another copy")
    end
  end

  it "takes no key for a :while_executing payload, one without a key, or one that is not an object" do
    [payload.merge("pgbus_uniqueness_strategy" => "while_executing"), payload.except("pgbus_uniqueness_key"),
     "a bare string", [1, 2]].each do |other|
      expect(resend(other) { "1" }).to eq(:sent)
    end

    expect(Pgbus::UniquenessKey).not_to have_received(:acquire!)
  end

  it "takes no key for an :until_executing retry_on re-enqueue (executions > 0), but does for a first attempt" do
    until_executing = payload.merge("pgbus_uniqueness_strategy" => "until_executing")

    resend(until_executing.merge("executions" => 2)) { "1" }
    expect(Pgbus::UniquenessKey).not_to have_received(:acquire!)

    resend(until_executing) { "2" }
    expect(Pgbus::UniquenessKey).to have_received(:acquire!).once
  end

  it "takes the key of an :until_executed retry (executions > 0): its copy releases it on success" do
    resend(payload.merge("executions" => 3)) { "1" }

    expect(Pgbus::UniquenessKey).to have_received(:acquire!).with("order-1", queue_name: "default", msg_id: 0)
  end

  it "takes a numeric key as text" do
    resend(payload.merge("pgbus_uniqueness_key" => 42)) { "1" }

    expect(Pgbus::UniquenessKey).to have_received(:acquire!).with("42", queue_name: "default", msg_id: 0)
  end

  # Issue #636: the limits_concurrency half.
  describe "a payload with a concurrency key" do
    let(:slotted) { payload.merge("pgbus_concurrency_key" => "order-slot-1") }
    let(:limits) { { limit: 1, duration: 600, on_conflict: :block } }
    let(:park_result) { instance_double(ActiveRecord::Result, rows: [[77]]) }

    before do
      stub_const("OrderJob", Class.new { def self.pgbus_concurrency = nil })
      allow(OrderJob).to receive(:pgbus_concurrency) { limits }
      allow(Pgbus::Semaphore).to receive(:transaction) { |**, &blk| blk.call }
      allow(Pgbus::Concurrency::Semaphore).to receive(:acquire).and_return(:acquired)
      allow(Pgbus::Concurrency::Semaphore).to receive(:release)
      allow(Pgbus::Concurrency::BlockedExecution).to receive(:insert).and_return(park_result)
      stub_const("PG::Error", Class.new(StandardError)) unless defined?(PG::Error)
    end

    it "takes a slot leased for the class's duration before the send" do
      expect(resend(slotted) { "17" }).to eq(:sent)

      expect(Pgbus::Concurrency::Semaphore).to have_received(:acquire)
        .with("order-slot-1", 1, Pgbus::Concurrency.effective_duration(600))
    end

    it "gives the slot and the key back when the send fails before producing" do
      expect { resend(slotted) { raise ArgumentError, "produce failed" } }.to raise_error(ArgumentError)

      expect(Pgbus::Concurrency::Semaphore).to have_received(:release).with("order-slot-1")
      expect(Pgbus::UniquenessKey).to have_received(:release_if_unbound!).with("order-1", acquired_before: acquired_at)
    end

    it "keeps the slot and the key when the send's outcome is unknown" do
      expect { resend(slotted) { raise PG::Error, "server closed the connection" } }.to raise_error(PG::Error)

      expect(Pgbus::Concurrency::Semaphore).not_to have_received(:release)
      expect(Pgbus::UniquenessKey).not_to have_received(:release_if_unbound!)
    end

    it "logs a failed slot give-back and raises the send's error" do
      allow(Pgbus::Concurrency::Semaphore).to receive(:release).and_raise(StandardError, "release failed")

      expect { resend(slotted) { raise ArgumentError, "produce failed" } }.to raise_error(ArgumentError, "produce failed")

      expect(Pgbus.logger).to have_received(:warn) do |&block|
        expect(block.call).to include("Giving back the concurrency slot of order-slot-1 failed: release failed")
      end
    end

    # A database error before the produce proves nothing was sent.
    it "gives the key back when the slot upsert raises a database error" do
      allow(Pgbus::Concurrency::Semaphore).to receive(:acquire).and_raise(PG::Error, "deadlock detected")
      sent = false

      expect { resend(slotted) { sent = true } }.to raise_error(PG::Error)

      expect(sent).to be(false)
      expect(Pgbus::UniquenessKey).to have_received(:release_if_unbound!).with("order-1", acquired_before: acquired_at)
    end

    describe "at a full limit" do
      before { allow(Pgbus::Concurrency::Semaphore).to receive(:acquire).and_return(:blocked) }

      it "parks the copy on the logical queue at the default priority, and removes the source" do
        dropped = false
        sent = false

        expect(resend(slotted, drop_source: -> { dropped = true }) { sent = true }).to eq(:parked)

        expect(sent).to be(false)
        expect(dropped).to be(true)
        expect(Pgbus::Concurrency::BlockedExecution).to have_received(:insert).with(
          concurrency_key: "order-slot-1", queue_name: "default", payload: slotted,
          priority: Pgbus.configuration.default_priority, duration: 600
        )
        expect(Pgbus::UniquenessKey).not_to have_received(:bind!)
        expect(Pgbus::UniquenessKey).not_to have_received(:release_if_unbound!)
      end

      it "parks the copy at the job's own priority when the payload carries one" do
        resend(slotted.merge("priority" => 3)) { "17" }

        expect(Pgbus::Concurrency::BlockedExecution).to have_received(:insert).with(hash_including(priority: 3))
      end

      it "withdraws the park and gives the key back when removing the source fails" do
        allow(Pgbus::BlockedExecution).to receive(:where).with(id: 77).and_return(double(delete_all: 1))

        expect { resend(slotted, drop_source: -> { raise ArgumentError, "delete failed" }) { "17" } }
          .to raise_error(ArgumentError, "delete failed")

        expect(Pgbus::UniquenessKey).to have_received(:release_if_unbound!).with("order-1", acquired_before: acquired_at)
      end

      # The DELETE may have committed with its reply lost: withdrawing the
      # park could leave neither copy.
      it "keeps the park when removing the source fails with an unknown outcome" do
        allow(Pgbus::BlockedExecution).to receive(:where)
        wrapped = ActiveRecord::StatementInvalid.new("connection lost")
        allow(wrapped).to receive(:cause).and_return(PG::Error.new("server closed the connection"))

        expect(resend(slotted, drop_source: -> { raise wrapped }) { "17" }).to eq(:parked)

        expect(Pgbus::BlockedExecution).not_to have_received(:where)
        expect(Pgbus::UniquenessKey).not_to have_received(:release_if_unbound!)
      end

      # The COMMIT may have landed with its reply lost: the park may be live.
      it "keeps the key when the park's commit raises" do
        allow(Pgbus::Semaphore).to receive(:transaction) do |**, &blk|
          blk.call
          raise ActiveRecord::StatementInvalid, "commit failed"
        end

        expect { resend(slotted) { "17" } }.to raise_error(ActiveRecord::StatementInvalid)

        expect(Pgbus::UniquenessKey).not_to have_received(:release_if_unbound!)
      end

      it "carries the headers the copy would have been sent with in the parked payload" do
        data_source.send(:resend_job, slotted, lock_queue: "default", holder: holder, drop_source: -> {},
                                               headers: { "pgbus_dlq_retries" => 1 }) { "17" }

        parked = slotted.merge("pgbus_park_headers" => { "pgbus_dlq_retries" => 1 })
        expect(Pgbus::Concurrency::BlockedExecution).to have_received(:insert).with(hash_including(payload: parked))
      end

      it "keeps a park already promoted when removing the source fails, and says the source stays" do
        allow(Pgbus::BlockedExecution).to receive(:where).with(id: 77).and_return(double(delete_all: 0))

        expect(resend(slotted, drop_source: -> { raise StandardError, "delete failed" }) { "17" }).to eq(:parked)

        expect(Pgbus::UniquenessKey).not_to have_received(:release_if_unbound!)
        expect(Pgbus.logger).to have_received(:warn) do |&block|
          expect(block.call).to include("removing its source failed (StandardError: delete failed); the parked copy stands")
        end
      end

      %i[discard raise].each do |on_conflict|
        it "refuses under on_conflict: :#{on_conflict}, gives the key back and says why" do
          limits[:on_conflict] = on_conflict
          sent = false

          expect(resend(slotted) { sent = true }).to be(false)

          expect(sent).to be(false)
          expect(Pgbus::Concurrency::BlockedExecution).not_to have_received(:insert)
          expect(Pgbus::UniquenessKey).to have_received(:release_if_unbound!).with("order-1", acquired_before: acquired_at)
          expect(Pgbus.logger).to have_received(:warn) do |&block|
            expect(block.call).to include("concurrency limit for order-slot-1 is full (on_conflict: :#{on_conflict})")
          end
        end
      end

      it "parks the copy of a class that no longer resolves, judged against the recorded limit" do
        sent = false

        expect(resend(slotted.merge("job_class" => "GoneJob")) { sent = true }).to eq(:parked)

        expect(sent).to be(false)
        expect(Pgbus::Concurrency::Semaphore).to have_received(:acquire)
          .with("order-slot-1", nil, Pgbus::Concurrency.effective_duration(Pgbus::Concurrency::DEFAULT_DURATION))
      end
    end
  end
end
