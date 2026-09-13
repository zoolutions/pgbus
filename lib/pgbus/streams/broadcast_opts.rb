# frozen_string_literal: true

module Pgbus
  module Streams
    # The thread-local channel that carries pgbus-specific broadcast options
    # from a `Turbo::Broadcastable` / `Turbo::StreamsChannel` call site down to
    # `TurboBroadcastable#broadcast_stream_to`, which is the only place with a
    # `Pgbus::Streams::Stream` to hand them to.
    #
    # turbo-rails' broadcast helpers funnel everything through
    # `broadcast_stream_to(*streamables, content:)` — a signature with nowhere
    # to put `durable:`, `exclude:`, `visible_to:`, `event:` or `coalesce:`.
    # Rather than fork turbo's whole helper surface, the patches pull those
    # kwargs out before calling `super` and stash them here for the duration of
    # the broadcast.
    #
    # Both patches (`BroadcastableOverride` on the model concern and
    # `TurboBroadcastable` on the channel) share this module so there is one
    # definition of which keys exist and how they are saved/restored.
    module BroadcastOpts
      # Options a caller may pass to any Turbo broadcast helper. They are
      # deleted from the kwargs so they never reach turbo-rails' renderer.
      KEYS = %i[durable exclude visible_to event coalesce].freeze

      # `coalesce_target` is not a caller-facing kwarg — it is derived from the
      # broadcast's own `target:`/`targets:` by the channel patch, because
      # coalescing keys on `(stream, target)` and `broadcast_stream_to` never
      # sees the target otherwise.
      THREAD_LOCALS = {
        durable: :pgbus_broadcast_durable,
        exclude: :pgbus_broadcast_exclude,
        visible_to: :pgbus_broadcast_visible_to,
        event: :pgbus_broadcast_event,
        coalesce: :pgbus_broadcast_coalesce,
        coalesce_target: :pgbus_broadcast_coalesce_target
      }.freeze

      # Removes the pgbus options from `kwargs` (mutating it) and returns them.
      # Keys the caller did not pass are absent from the result, so an outer
      # `BroadcastOpts.with` block's values survive an inner broadcast that
      # doesn't set them.
      def self.extract!(kwargs)
        KEYS.each_with_object({}) do |key, opts|
          opts[key] = kwargs.delete(key) if kwargs.key?(key)
        end
      end

      # Sets the thread-locals for the given options for the duration of the
      # block, restoring the previous values afterwards (nesting-safe), even on
      # error. Only the keys passed are touched.
      def self.with(**opts)
        previous = {}

        opts.each do |key, value|
          tl_key = THREAD_LOCALS.fetch(key)
          previous[tl_key] = Thread.current[tl_key]
          Thread.current[tl_key] = value
        end

        yield
      ensure
        previous.each { |tl_key, value| Thread.current[tl_key] = value }
      end

      def self.[](key)
        Thread.current[THREAD_LOCALS.fetch(key)]
      end
    end
  end
end
