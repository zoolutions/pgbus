# frozen_string_literal: true

require "concurrent"
require "socket"

module Pgbus
  module Process
    class Heartbeat
      INTERVAL = 60 # seconds
      ALIVE_THRESHOLD = 300 # 5 minutes

      attr_reader :process_entry

      def initialize(kind:, metadata: {}, on_beat: nil, loop_tick_supplier: nil, metadata_supplier: nil)
        @kind = kind
        # Every kind's row carries the app revision from its first
        # registration on, nil included: the deploy-task rollover gate reads
        # it (issue #536), and beat_metadata dups this hash so it survives.
        @metadata = metadata.merge(revision: Deploy::Revision.current)
        @on_beat = on_beat
        @loop_tick_supplier = loop_tick_supplier
        @metadata_supplier = metadata_supplier
        @timer = nil
        @stopped = false
        # Guards @process_id between the timer thread (beat) and the main
        # thread (stop): TimerTask#shutdown does not wait for an in-flight
        # beat, and a beat that re-registers after deregister_process ran
        # would leave a zombie row (issue #438).
        @mutex = Mutex.new
      end

      def start
        register_process
        @timer = Concurrent::TimerTask.new(execution_interval: INTERVAL) { beat }
        @timer.execute
      end

      def stop
        @stopped = true
        @timer&.shutdown
        @mutex.synchronize { deregister_process }
      end

      def beat
        return unless @process_id && !@stopped

        @on_beat&.call
        updates = { last_heartbeat_at: Time.current }
        metadata = beat_metadata
        updates[:metadata] = metadata unless metadata.nil?
        @mutex.synchronize { write_beat(updates) }
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Heartbeat failed: #{e.message}" }
      end

      private

      # Build the metadata hash to persist on this beat. Returns nil when no
      # supplier is configured so heartbeats without dynamic metadata
      # (supervisor/dispatcher/scheduler) leave the column untouched, exactly
      # as before. The loop_tick and metadata suppliers are called here — after
      # @on_beat has run — so any snapshot!/refresh in on_beat is reflected.
      def beat_metadata
        return nil unless @loop_tick_supplier || @metadata_supplier

        metadata = @metadata.dup
        metadata["loop_tick_at"] = @loop_tick_supplier.call&.to_f if @loop_tick_supplier
        metadata.merge!(@metadata_supplier.call) if @metadata_supplier
        metadata
      end

      # update_all by id matches 0 rows when the row was deleted underneath a
      # live process (stale-process reaper after a heartbeat gap, manual
      # cleanup, another host's clock skew). Nothing raises, so treat the
      # count as the signal: re-register and log once so the cause of the
      # deletion is discoverable, then land this beat's updates on the new
      # row (issue #438). Skipped after stop so a beat racing deregistration
      # cannot resurrect the row.
      def write_beat(updates)
        return if ProcessEntry.where(id: @process_id).update_all(updates).positive? || @stopped

        old_id = @process_id
        Pgbus.logger.warn do
          "[Pgbus] Process row id=#{old_id} kind=#{@kind} pid=#{::Process.pid} is gone " \
            "(stale-process reaper, manual cleanup, or clock skew?) — re-registering"
        end
        register_process
        ProcessEntry.where(id: @process_id).update_all(updates) if @process_id != old_id
      end

      def register_process
        record = ProcessEntry.create!(
          kind: @kind,
          hostname: Socket.gethostname,
          pid: ::Process.pid,
          metadata: @metadata,
          last_heartbeat_at: Time.current
        )
        @process_id = record.id
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Process registration failed: #{e.message}" }
      end

      def deregister_process
        return unless @process_id

        ProcessEntry.where(id: @process_id).delete_all
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Process deregistration failed: #{e.message}" }
      end
    end
  end
end
