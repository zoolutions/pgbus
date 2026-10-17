# frozen_string_literal: true

module Pgbus
  class Batch
    # What Pgbus.batch (and +batch { |b| ... }+ inside a job) yields: callback
    # setters and +add+ on a batch that is open for the length of the block.
    #
    #   Pgbus.batch(description: "Import #{source.id}") do |b|
    #     b.on_complete ImportDoneJob.new.set(queue: :critical)
    #     b.on_failure  ImportFailedJob
    #     b.add(*source.rows.map { |row| ImportRowJob.new(row.id) })
    #   end
    #
    # The batch row exists (and a uniqueness key is held) before the block
    # runs, and it stays pending until the block returns, so the order of the
    # calls does not matter and a job that completes during the block cannot
    # finish the batch early. Once the block has returned the builder is
    # closed: add more jobs through +batch.add+ on the returned batch.
    class Builder
      CLOSED_MESSAGE = "the batch builder only works inside its Pgbus.batch block; " \
                       "add more jobs later with batch.add on the batch Pgbus.batch returned"
      UNTAGGED_MESSAGE = "b.add ran on a thread or fiber outside its Pgbus.batch block (or inside another " \
                         "batch's block), where its jobs would not join the batch; call b.add from the block itself"

      # Flatten +jobs+ and check that each one is an ActiveJob instance.
      def self.job_instances(jobs)
        jobs = jobs.flatten
        invalid = jobs.grep_v(::ActiveJob::Base)
        return jobs if invalid.empty?

        raise ArgumentError,
              "a batch takes job instances (MyJob.new(args), optionally .set(wait:, queue:)), got #{invalid.first.inspect}"
      end

      def initialize(batch)
        @batch = batch
        @closed = false
      end

      def on_complete(job = nil, &) = set_callback(:on_complete, job, &)

      def on_success(job = nil, &) = set_callback(:on_success, job, &)

      def on_failure(job = nil, &) = set_callback(:on_failure, job, &)

      # Deprecated alias of #on_complete until 1.0.
      def on_finish(job = nil, &)
        Batch.warn_on_finish_deprecated
        set_callback(:on_complete, job, &)
      end

      # Enqueue job instances into the batch with one bulk send (and anything
      # the block enqueues). Returns the jobs. Membership is the batch tag the
      # block holds (Thread.current, so fiber-local), so an add from another
      # thread or fiber, or from inside a nested batch's block, raises rather
      # than send jobs that would not be counted.
      def add(*jobs)
        raise ArgumentError, CLOSED_MESSAGE if @closed
        raise ArgumentError, UNTAGGED_MESSAGE unless Thread.current[:pgbus_batch_id] == @batch.batch_id

        jobs = self.class.job_instances(jobs)
        ::ActiveJob.perform_all_later(jobs) if jobs.any?
        yield if block_given?
        jobs
      end

      # Called by Batch.start when the block returns or raises.
      def close
        @closed = true
      end

      private

      def set_callback(slot, job, &block)
        raise ArgumentError, CLOSED_MESSAGE if @closed

        unless block.nil? && callback_job?(job)
          raise ArgumentError,
                "#{slot} takes an ActiveJob class or a configured instance (MyJob.new.set(queue: :critical)), " \
                "not #{block ? "a block" : job.inspect}: callbacks are stored on the batch row"
        end

        @batch.send(:write_callback, slot, job)
        self
      end

      def callback_job?(job)
        (job.is_a?(Class) && job < ::ActiveJob::Base) || job.is_a?(::ActiveJob::Base)
      end
    end
  end
end
