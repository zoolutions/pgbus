# frozen_string_literal: true

module Pgbus
  module Streams
    # Runtime patch for `Turbo::Broadcastable` that adds `durable:` kwarg
    # support to synchronous broadcast helpers. Applied at Rails engine boot
    # time when `defined?(::Turbo::Broadcastable)` — see `Pgbus::Engine`.
    #
    # Instance methods extract `durable:` from kwargs and set a thread-local
    # (`Thread.current[:pgbus_broadcast_durable]`) that
    # `TurboBroadcastable#broadcast_stream_to` reads. The thread-local is
    # always cleaned up after the broadcast, even on error.
    #
    # The `_later_to` variants are NOT overridden because turbo-rails
    # enqueues them as background jobs — the thread-local cannot survive
    # into the job execution context. For async broadcasts, use
    # `streams_durable_patterns` or `streams_default_broadcast_mode` config.
    #
    # Class methods (`broadcasts_to`, `broadcasts_refreshes_to`) accept
    # `durable:` and store it so the generated callbacks set the thread-local
    # before each broadcast.
    module BroadcastableOverride
      BROADCAST_METHODS = %i[
        broadcast_after_to
        broadcast_before_to
        broadcast_replace_to
        broadcast_append_to
        broadcast_prepend_to
        broadcast_update_to
        broadcast_remove_to
        broadcast_refresh_to
        broadcast_render_to
      ].freeze

      BROADCAST_ACTION_METHODS = %i[
        broadcast_action_to
      ].freeze

      # pgbus-specific broadcast options that Turbo's own broadcast helpers
      # don't understand. We pull them out of kwargs (so they never reach
      # turbo-rails' renderer) and thread them to broadcast_stream_to via
      # thread-locals, mirroring the original durable: shim.
      PGBUS_BROADCAST_OPTS = BroadcastOpts::KEYS

      BROADCAST_METHODS.each do |method_name|
        define_method(method_name) do |*streamables, **kwargs|
          opts = extract_pgbus_broadcast_opts(kwargs)
          with_pgbus_broadcast_opts(**opts) { super(*streamables, **kwargs) }
        end
      end

      BROADCAST_ACTION_METHODS.each do |method_name|
        define_method(method_name) do |*streamables, action:, **kwargs|
          opts = extract_pgbus_broadcast_opts(kwargs)
          with_pgbus_broadcast_opts(**opts) { super(*streamables, action: action, **kwargs) }
        end
      end

      module ClassMethods
        def broadcasts_to(stream, durable: nil, inserts_by: :append, target: broadcast_target_default, **rendering)
          if durable.nil?
            after_create_commit lambda {
              broadcast_action_later_to(
                stream.try(:call, self) || send(stream),
                action: inserts_by,
                target: target.try(:call, self) || target,
                **rendering
              )
            }
            after_update_commit -> { broadcast_replace_later_to(stream.try(:call, self) || send(stream), **rendering) }
            after_destroy_commit -> { broadcast_remove_to(stream.try(:call, self) || send(stream)) }
          else
            @pgbus_durable_streams ||= {}
            @pgbus_durable_streams[stream] = durable

            after_create_commit lambda {
              broadcast_action_to(
                stream.try(:call, self) || send(stream),
                action: inserts_by,
                target: target.try(:call, self) || target,
                durable: durable,
                **rendering
              )
            }
            after_update_commit lambda {
              broadcast_replace_to(stream.try(:call, self) || send(stream), durable: durable, **rendering)
            }
            after_destroy_commit lambda {
              broadcast_remove_to(stream.try(:call, self) || send(stream), durable: durable)
            }
          end
        end

        def broadcasts_refreshes_to(stream, durable: nil)
          if durable.nil?
            after_commit -> { broadcast_refresh_later_to(stream.try(:call, self) || send(stream)) }
          else
            @pgbus_durable_streams ||= {}
            @pgbus_durable_streams[stream] = durable

            after_commit lambda {
              broadcast_refresh_to(stream.try(:call, self) || send(stream), durable: durable)
            }
          end
        end

        def pgbus_durable_streams
          @pgbus_durable_streams || {}
        end
      end

      def self.install!(mod)
        return if mod.ancestors.include?(self)

        mod.prepend(self)

        # Turbo::Broadcastable uses ActiveSupport::Concern, which extends
        # each including class with Turbo::Broadcastable::ClassMethods.
        # Prepending our ClassMethods onto that nested module ensures any
        # class that includes Turbo::Broadcastable picks up our overrides
        # (broadcasts_to, broadcasts_refreshes_to) automatically.
        if defined?(::Turbo::Broadcastable::ClassMethods) &&
           !::Turbo::Broadcastable::ClassMethods.ancestors.include?(ClassMethods)
          ::Turbo::Broadcastable::ClassMethods.prepend(ClassMethods)
        end
      end

      private

      def extract_pgbus_broadcast_opts(kwargs)
        BroadcastOpts.extract!(kwargs)
      end

      # Set the pgbus broadcast thread-locals for the duration of the block,
      # restoring previous values afterwards (nested/concurrent-safe). Only
      # keys actually passed are touched, so unrelated outer broadcasts keep
      # their values.
      def with_pgbus_broadcast_opts(durable: :__unset__, exclude: :__unset__, visible_to: :__unset__,
                                    event: :__unset__, coalesce: :__unset__, &)
        opts = { durable: durable, exclude: exclude, visible_to: visible_to, event: event, coalesce: coalesce }
        opts.reject! { |_key, value| value == :__unset__ }

        BroadcastOpts.with(**opts, &)
      end
    end
  end
end
