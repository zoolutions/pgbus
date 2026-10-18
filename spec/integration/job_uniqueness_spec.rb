# frozen_string_literal: true

require_relative "../integration_helper"

RSpec.describe "Job uniqueness (integration)", :integration do
  before do
    Pgbus::UniquenessKey.delete_all
  end

  describe "UniquenessKey acquire/release" do
    it "prevents duplicate acquisition when lock is held" do
      acquired = Pgbus::UniquenessKey.acquire!("unique-order-42", queue_name: "default", msg_id: 1)
      expect(acquired).to be true

      duplicate = Pgbus::UniquenessKey.acquire!("unique-order-42", queue_name: "default", msg_id: 2)
      expect(duplicate).to be false
    end

    it "allows acquisition after lock is released" do
      Pgbus::UniquenessKey.acquire!("unique-order-42", queue_name: "default", msg_id: 1)
      Pgbus::UniquenessKey.release!("unique-order-42")

      acquired = Pgbus::UniquenessKey.acquire!("unique-order-42", queue_name: "default", msg_id: 3)
      expect(acquired).to be true
    end

    it "checks lock status correctly" do
      expect(Pgbus::UniquenessKey.locked?("unique-order-42")).to be false

      Pgbus::UniquenessKey.acquire!("unique-order-42", queue_name: "default", msg_id: 1)
      expect(Pgbus::UniquenessKey.locked?("unique-order-42")).to be true

      Pgbus::UniquenessKey.release!("unique-order-42")
      expect(Pgbus::UniquenessKey.locked?("unique-order-42")).to be false
    end
  end

  describe "UniquenessKey bind!" do
    it "updates queue_name and msg_id without changing lock_key or created_at" do
      Pgbus::UniquenessKey.acquire!("unique-order-42", queue_name: "pending", msg_id: 0)
      row = Pgbus::UniquenessKey.find("unique-order-42")
      original_created_at = row.created_at

      Pgbus::UniquenessKey.bind!("unique-order-42", queue_name: "critical", msg_id: 99)

      row.reload
      expect(row.lock_key).to eq("unique-order-42")
      expect(row.queue_name).to eq("critical")
      expect(row.msg_id).to eq(99)
      expect(row.created_at).to eq(original_created_at)
    end

    it "does not retarget a successor row acquired after this enqueue released" do
      # Simulates the enqueue-thread bind racing a successor acquire: this
      # thread's acquire! stamp is restored to the *first* row's created_at
      # after a second acquire! overwrote it. bind! must then no-op (msg_id
      # stays 0). Relies on acquire! storing created_at in
      # Thread.current[:pgbus_uniqueness_created_at]; without a stamp, bind!
      # only requires msg_id=0 and would retarget the successor.
      Pgbus::UniquenessKey.acquire!("unique-order-42", queue_name: "pending", msg_id: 0)
      first_created = Thread.current[:pgbus_uniqueness_created_at]["unique-order-42"]
      Pgbus::UniquenessKey.release!("unique-order-42")

      Pgbus::UniquenessKey.acquire!("unique-order-42", queue_name: "default", msg_id: 0)
      Thread.current[:pgbus_uniqueness_created_at]["unique-order-42"] = first_created
      Pgbus::UniquenessKey.bind!("unique-order-42", queue_name: "critical", msg_id: 99)

      row = Pgbus::UniquenessKey.find("unique-order-42")
      expect(row.queue_name).to eq("default")
      expect(row.msg_id).to eq(0)
    end

    it "clears the bind stamp without releasing the lock" do
      Pgbus::UniquenessKey.acquire!("unique-order-42", queue_name: "default", msg_id: 0)
      expect(Thread.current[:pgbus_uniqueness_created_at]).to have_key("unique-order-42")

      Pgbus::UniquenessKey.clear_bind_stamp!("unique-order-42")

      expect(Thread.current[:pgbus_uniqueness_created_at]).not_to have_key("unique-order-42")
      expect(Pgbus::UniquenessKey.locked?("unique-order-42")).to be true
    end
  end

  # Issue #564: a retry takes the running attempt's row before it is sent
  # (unbind!), binds it once sent (bind! with the stamp), and a finishing
  # message releases only the row it holds (release_held_by!).
  describe "UniquenessKey unbind!" do
    def unbind(from_msg_id: 7, from_queue_name: "default", from_enqueued_at: Time.current)
      Pgbus::UniquenessKey.unbind!("unique-order-42", from_queue_name: from_queue_name, from_msg_id: from_msg_id,
                                                      from_enqueued_at: from_enqueued_at, queue_name: "critical")
    end

    def row
      Pgbus::UniquenessKey.find("unique-order-42")
    end

    it "unbinds the running attempt's row onto the retry's queue, re-stamps it and returns the stamp" do
      Pgbus::UniquenessKey.acquire!("unique-order-42", queue_name: "default", msg_id: 7)
      Pgbus::UniquenessKey.where(lock_key: "unique-order-42").update_all(created_at: 1.hour.ago)

      stamp = unbind

      expect([row.queue_name, row.msg_id]).to eq(["critical", 0])
      expect(row.created_at).to be > 1.minute.ago
      expect(stamp).not_to be_nil
    end

    it "leaves a row bound to another message alone and returns nil" do
      Pgbus::UniquenessKey.acquire!("unique-order-42", queue_name: "default", msg_id: 9)

      expect(unbind).to be_nil
      expect(row.msg_id).to eq(9)
    end

    it "matches the running attempt's queue too, not only its msg_id" do
      Pgbus::UniquenessKey.acquire!("unique-order-42", queue_name: "critical", msg_id: 7)

      expect(unbind).to be_nil
    end

    # A job parked behind its concurrency slot is promoted without a bind,
    # so it runs holding an unbound row.
    it "takes an unbound row acquired before the running attempt was sent" do
      Pgbus::UniquenessKey.acquire!("unique-order-42", queue_name: "default", msg_id: 0)

      expect(unbind(from_enqueued_at: 1.second.from_now)).not_to be_nil
      expect(row.queue_name).to eq("critical")
    end

    it "leaves an unbound row acquired after the running attempt was sent (a successor's)" do
      Pgbus::UniquenessKey.acquire!("unique-order-42", queue_name: "default", msg_id: 0)

      expect(unbind(from_enqueued_at: 1.minute.ago)).to be_nil
    end

    it "takes only a bound row when the attempt's enqueued_at is unknown" do
      Pgbus::UniquenessKey.acquire!("unique-order-42", queue_name: "default", msg_id: 0)

      expect(unbind(from_enqueued_at: nil)).to be_nil
    end

    it "is no longer the attempt's to release, and is the retry's (sent after) to release" do
      Pgbus::UniquenessKey.acquire!("unique-order-42", queue_name: "default", msg_id: 7)
      attempt_sent_at = 1.minute.ago
      unbind(from_enqueued_at: attempt_sent_at)

      expect(Pgbus::UniquenessKey.release_held_by!("unique-order-42", queue_name: "default", msg_id: 7,
                                                                      enqueued_at: attempt_sent_at)).to eq(0)
      expect(Pgbus::UniquenessKey.release_held_by!("unique-order-42", queue_name: "critical", msg_id: 8,
                                                                      enqueued_at: 1.second.from_now)).to eq(1)
    end
  end

  describe "UniquenessKey bind! with the stamp unbind! returned" do
    it "binds the handed-over row to the retry" do
      Pgbus::UniquenessKey.acquire!("unique-order-42", queue_name: "default", msg_id: 7)
      stamp = Pgbus::UniquenessKey.unbind!("unique-order-42", from_queue_name: "default", from_msg_id: 7,
                                                              from_enqueued_at: Time.current, queue_name: "critical")

      bound = Pgbus::UniquenessKey.bind!("unique-order-42", queue_name: "critical", msg_id: 8, acquired_at: stamp)

      row = Pgbus::UniquenessKey.find("unique-order-42")
      expect(bound).to eq(1)
      expect([row.queue_name, row.msg_id]).to eq(["critical", 8])
    end

    it "binds nothing once the row went and a successor took the key" do
      Pgbus::UniquenessKey.acquire!("unique-order-42", queue_name: "default", msg_id: 7)
      stamp = Pgbus::UniquenessKey.unbind!("unique-order-42", from_queue_name: "default", from_msg_id: 7,
                                                              from_enqueued_at: Time.current, queue_name: "critical")
      Pgbus::UniquenessKey.release!("unique-order-42")
      Pgbus::UniquenessKey.acquire!("unique-order-42", queue_name: "default", msg_id: 0)

      bound = Pgbus::UniquenessKey.bind!("unique-order-42", queue_name: "critical", msg_id: 8, acquired_at: stamp)

      expect(bound).to eq(0)
      expect(Pgbus::UniquenessKey.find("unique-order-42").msg_id).to eq(0)
    end
  end

  describe "UniquenessKey release_held_by!" do
    let(:sent_at) { Time.current }

    def release(msg_id: 7, queue_name: "default", enqueued_at: sent_at)
      Pgbus::UniquenessKey.release_held_by!("unique-order-42", queue_name: queue_name, msg_id: msg_id,
                                                               enqueued_at: enqueued_at)
    end

    it "releases the row bound to this message" do
      Pgbus::UniquenessKey.acquire!("unique-order-42", queue_name: "default", msg_id: 7)

      expect(release).to eq(1)
      expect(Pgbus::UniquenessKey.locked?("unique-order-42")).to be(false)
    end

    it "keeps a row bound to another message (a retry, or a successor)" do
      Pgbus::UniquenessKey.acquire!("unique-order-42", queue_name: "default", msg_id: 8)

      expect(release).to eq(0)
      expect(Pgbus::UniquenessKey.locked?("unique-order-42")).to be(true)
    end

    it "keeps a row bound to the same msg_id on another queue" do
      Pgbus::UniquenessKey.acquire!("unique-order-42", queue_name: "critical", msg_id: 7)

      expect(release).to eq(0)
    end

    it "releases an unbound row acquired before this message was sent" do
      Pgbus::UniquenessKey.acquire!("unique-order-42", queue_name: "default", msg_id: 0)

      expect(release(enqueued_at: 1.second.from_now)).to eq(1)
    end

    it "keeps an unbound row acquired after this message was sent (a successor's)" do
      Pgbus::UniquenessKey.acquire!("unique-order-42", queue_name: "default", msg_id: 0)

      expect(release(enqueued_at: 1.minute.ago)).to eq(0)
      expect(Pgbus::UniquenessKey.locked?("unique-order-42")).to be(true)
    end

    # Zones picked so that reading the wall clock and dropping the offset
    # would put each moment on the wrong side of created_at.
    it "reads PGMQ's enqueued_at string with its offset" do
      Pgbus::UniquenessKey.acquire!("unique-order-42", queue_name: "default", msg_id: 0)
      created_at = Pgbus::UniquenessKey.find("unique-order-42").created_at
      before = (created_at - 1).in_time_zone("Asia/Tokyo").strftime("%Y-%m-%d %H:%M:%S.%6N%:z")
      after = (created_at + 1).in_time_zone("America/New_York").strftime("%Y-%m-%d %H:%M:%S.%6N%:z")

      expect(release(enqueued_at: before)).to eq(0)
      expect(release(enqueued_at: after)).to eq(1)
    end

    # created_at is a timestamp without time zone written in the session's
    # zone (ActiveRecord.default_timezone = :local runs the session in the
    # server's zone); the cast reads it back in that same zone.
    it "compares correctly when the session's time zone is not UTC" do
      conn = Pgbus::UniquenessKey.connection
      conn.execute("SET TIME ZONE 'Asia/Tokyo'")
      Pgbus::UniquenessKey.acquire!("unique-order-42", queue_name: "default", msg_id: 0)
      instant = Time.at(conn.select_value(
        "SELECT extract(epoch FROM created_at::timestamptz) FROM pgbus_uniqueness_keys WHERE lock_key = 'unique-order-42'"
      ).to_f)
      before = (instant - 1).in_time_zone("Asia/Tokyo").strftime("%Y-%m-%d %H:%M:%S.%6N%:z")
      after = (instant + 1).in_time_zone("America/New_York").strftime("%Y-%m-%d %H:%M:%S.%6N%:z")

      expect(release(enqueued_at: before)).to eq(0)
      expect(release(enqueued_at: after)).to eq(1)
    ensure
      conn&.execute("SET TIME ZONE 'UTC'")
    end

    # The dashboard reads enqueued_at through ActiveRecord, which hands over
    # a Time, not PGMQ's string: it has to reach the cast with its offset.
    %w[Asia/Tokyo America/New_York].each do |zone|
      context "with a Time and a #{zone} session" do
        let(:conn) { Pgbus::UniquenessKey.connection }

        before do
          conn.execute("SET TIME ZONE #{conn.quote(zone)}")
          Pgbus::UniquenessKey.acquire!("unique-order-42", queue_name: "default", msg_id: 0)
        end

        after { conn.execute("SET TIME ZONE 'UTC'") }

        def instant
          Time.at(conn.select_value(
            "SELECT extract(epoch FROM created_at::timestamptz) FROM pgbus_uniqueness_keys " \
            "WHERE lock_key = 'unique-order-42'"
          ).to_f)
        end

        it "releases one at a time on the right side of the lock's acquire" do
          sent = instant

          expect(release(enqueued_at: sent - 1)).to eq(0)
          expect(release(enqueued_at: sent + 1)).to eq(1)
        end

        it "releases in bulk on the right side of the lock's acquire" do
          sent = instant

          expect(Pgbus::UniquenessKey.release_all_held_by!([["unique-order-42", "default", 7, sent - 1]])).to eq(0)
          expect(Pgbus::UniquenessKey.release_all_held_by!([["unique-order-42", "default", 7, sent + 1]])).to eq(1)
        end

        it "hands over only on the right side of the lock's acquire" do
          sent = instant
          unbind = lambda do |sent_at|
            Pgbus::UniquenessKey.unbind!("unique-order-42", from_queue_name: "default", from_msg_id: 7,
                                                            from_enqueued_at: sent_at, queue_name: "default")
          end

          expect(unbind.call(sent - 1)).to be_nil
          expect(unbind.call(sent + 1)).not_to be_nil
        end
      end
    end

    it "releases the synthetic pending row like an unbound one" do
      Pgbus::UniquenessKey.acquire!("unique-order-42", queue_name: "pending", msg_id: 0)

      expect(release(enqueued_at: 1.second.from_now)).to eq(1)
    end

    it "releases an unbound row when the message carries no enqueued_at" do
      Pgbus::UniquenessKey.acquire!("unique-order-42", queue_name: "default", msg_id: 0)

      expect(release(enqueued_at: nil)).to eq(1)
    end
  end

  # The dashboard's bulk discards and purges: release_held_by! per message,
  # in one statement.
  describe "UniquenessKey release_all_held_by!" do
    it "releases each row its message holds and keeps the rest" do
      Pgbus::UniquenessKey.acquire!("bound-mine", queue_name: "default", msg_id: 1)
      Pgbus::UniquenessKey.acquire!("bound-elsewhere", queue_name: "default", msg_id: 99)
      Pgbus::UniquenessKey.acquire!("unbound-mine", queue_name: "default", msg_id: 0)
      Pgbus::UniquenessKey.acquire!("unbound-successor", queue_name: "default", msg_id: 0)
      Pgbus::UniquenessKey.acquire!("no-sent-at", queue_name: "default", msg_id: 0)

      released = Pgbus::UniquenessKey.release_all_held_by!(
        [
          ["bound-mine", "default", 1, 1.minute.ago],
          ["bound-elsewhere", "default", 2, 1.minute.ago],
          ["unbound-mine", "default", 3, "#{1.second.from_now.utc.strftime("%Y-%m-%d %H:%M:%S.%6N")}+00"],
          ["unbound-successor", "default", 4, 1.minute.ago],
          ["no-sent-at", "default", 5, nil]
        ]
      )

      expect(released).to eq(3)
      expect(Pgbus::UniquenessKey.pluck(:lock_key)).to contain_exactly("bound-elsewhere", "unbound-successor")
    end

    it "quotes the keys and queues it is given" do
      Pgbus::UniquenessKey.acquire!("it's-a-key", queue_name: "o'queue", msg_id: 1)

      expect(Pgbus::UniquenessKey.release_all_held_by!([["it's-a-key", "o'queue", 1, nil]])).to eq(1)
    end

    it "does nothing for an empty list" do
      expect(Pgbus::UniquenessKey.release_all_held_by!([])).to eq(0)
    end
  end

  describe "concurrent lock acquisition" do
    it "only one thread wins the lock" do
      results = Concurrent::Array.new
      barrier = Concurrent::CyclicBarrier.new(5)

      threads = 5.times.map do |i|
        Thread.new do
          barrier.wait
          acquired = Pgbus::UniquenessKey.acquire!(
            "race-key",
            queue_name: "default",
            msg_id: i
          )
          results << acquired
        end
      end

      threads.each(&:join)

      winners = results.count(true)
      losers = results.count(false)

      expect(winners).to eq(1)
      expect(losers).to eq(4)
    end
  end
end
