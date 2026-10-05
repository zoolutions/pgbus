# frozen_string_literal: true

module Pgbus
  module Process
    # The read-ahead buffer a Worker or Consumer owns (issue #486).
    #
    # Under database latency the claim loop used to issue one read round trip
    # per job: once the pool is full, slots free one at a time and each loop
    # step read `qty = free slots` = 1. With `read_ahead = N` the loop claims
    # up to N messages beyond its free slots and parks them here, so one read
    # returns a batch and the pool never waits on the reader.
    #
    # Surplus claims cannot live inside the execution pool: AsyncPool#post
    # raises at zero capacity and a queued block could not be handed back on
    # drain. So the buffer is drained into the pool only while it has a free
    # slot, and returned to the queue (vt: 0) on drain, recycle or shutdown.
    #
    # Every buffered message is kept invisible by a VisibilityHeartbeat hold
    # from claim until the run starts, under the executor's key
    # (`source_queue || queue_name`, `prefixed: source_queue.nil?`), so the
    # run's own tracking takes over the same entry with no gap.
    #
    # Only ever touched from the owning process's loop thread, so no mutex.
    class ClaimBuffer
      Claim = Struct.new(:queue_name, :message, :source_queue, :hold)

      HOLD_LABEL = "(read-ahead)"

      def initialize(config: Pgbus.configuration)
        @config = config
        @claims = []
      end

      def size
        @claims.size
      end

      def empty?
        @claims.empty?
      end

      def push(queue_name, message, source_queue = nil, client: Pgbus.client)
        hold = VisibilityHeartbeat.hold(
          client: client, queue_name: source_queue || queue_name, prefixed: source_queue.nil?,
          msg_id: message.msg_id, job_class: HOLD_LABEL, config: @config
        )
        @claims << Claim.new(queue_name, message, source_queue, hold)
        self
      end

      def shift
        @claims.shift
      end

      # Yields the oldest claims, one per free pool slot, and returns how many.
      # Capacity is re-read before every claim: the block's post takes a slot.
      def drain_into(pool)
        drained = 0
        while !@claims.empty? && pool.available_capacity.positive?
          yield @claims.shift
          drained += 1
        end
        drained
      end

      # How many messages the next read should claim: enough to fill the free
      # slots and top the buffer back up to `read_ahead`, never more than
      # `prefetch_room` (prefetch_limit minus everything already claimed).
      # With read_ahead 0 and an empty buffer this is `free_slots`: the
      # pre-#486 qty.
      def deficit(free_slots:, read_ahead:, prefetch_room: nil)
        want = free_slots + read_ahead - @claims.size
        want = [want, prefetch_room].min if prefetch_room
        want.clamp(0..)
      end

      # Hand every buffered message back to the queue right away instead of
      # leaving it invisible until its timeout. Their read_ct has already been
      # bumped, the same cost a crash or a stale claim pays. A failed return
      # is logged and skipped: the hold is dropped either way, so the message
      # reappears when its current timeout runs out. Returns the count.
      def return_all!(client: Pgbus.client)
        claims = @claims
        @claims = []
        claims.each { |claim| return_claim(claim, client) }
        claims.size
      end

      private

      def return_claim(claim, client)
        queue = claim.source_queue || claim.queue_name
        client.set_visibility_timeout(queue, claim.message.msg_id.to_i, vt: 0, prefixed: claim.source_queue.nil?)
      rescue StandardError => e
        Pgbus.logger.warn do
          "[Pgbus] Could not return read-ahead message msg_id=#{claim.message.msg_id} queue=#{queue}: " \
            "#{e.class}: #{e.message}"
        end
      ensure
        VisibilityHeartbeat.release(claim.hold)
      end
    end
  end
end
