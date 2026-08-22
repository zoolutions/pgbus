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
