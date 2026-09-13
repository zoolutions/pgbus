# frozen_string_literal: true

module Pgbus
  module Streams
    # Runtime patch that redirects `Turbo::StreamsChannel.broadcast_stream_to`
    # through pgbus instead of `ActionCable.server.broadcast`. Applied at
    # Rails engine boot time when `defined?(::Turbo::StreamsChannel)` —
    # see `Pgbus::Engine`'s initializer. When turbo-rails isn't loaded,
    # this patch is a no-op and pgbus streams continue to work via the
    # explicit `Pgbus.stream(...).broadcast(...)` API.
    #
    # After the patch:
    #
    #   class Order < ApplicationRecord
    #     broadcasts_to :account   # existing turbo-rails API, unchanged
    #   end
    #
    #   # In a controller:
    #   @order.update!(status: "shipped")
    #   # → Turbo::Broadcastable runs its after_update_commit callback
    #   # → calls Turbo::StreamsChannel.broadcast_replace_to
    #   # → which calls Turbo::StreamsChannel.broadcast_stream_to
    #   # → which is patched to call Pgbus.stream(name).broadcast(content)
    #   # → which inserts into PGMQ and fires NOTIFY
    #
    # Zero changes to user code. The entire Turbo::Broadcastable API
    # (broadcasts_to, broadcasts_refreshes, broadcast_replace_to,
    # broadcast_append_later_to, broadcasts_refreshes_to, etc) reuses
    # this code path because they all funnel through broadcast_stream_to.
    #
    # Signed stream name reuse: we don't touch `Turbo.signed_stream_verifier`,
    # so any existing `broadcasts_to :room` call continues to generate
    # tokens that our `Pgbus::Streams::SignedName.verify!` accepts (as
    # long as `Turbo.signed_stream_verifier_key` is set, which the Rails
    # app is already responsible for).
    module TurboBroadcastable
      # Every targeted broadcast helper (`broadcast_replace_to`,
      # `broadcast_append_to`, `broadcast_remove_to`, …) funnels through
      # `broadcast_action_to`, so this is the one place that sees both the
      # caller's kwargs and the `target:`/`targets:` the frame will carry.
      #
      # It does two things the model-level `BroadcastableOverride` can't:
      #
      # 1. Extracts the pgbus options from a *direct* channel call —
      #    `Turbo::StreamsChannel.broadcast_replace_to(..., coalesce: true)` —
      #    which never passes through `Turbo::Broadcastable` at all (this is
      #    the path phlex-reactive's `Streamable.broadcast_to` takes).
      # 2. Records the coalescing key. `coalesce:` dedupes on
      #    `(stream, target)`, but `broadcast_stream_to` only receives the
      #    rendered `content:` — the target is gone by then. We resolve it the
      #    same way turbo will (`convert_to_turbo_stream_dom_id`), so a record
      #    target keys on its stable dom_id rather than its object id.
      #
      # Resolving the key is skipped entirely unless coalescing was actually
      # requested, so the uncoalesced path stays byte-identical and free.
      def broadcast_action_to(*streamables, action:, target: nil, targets: nil, **rendering)
        opts = BroadcastOpts.extract!(rendering)

        coalesce = opts.key?(:coalesce) ? opts[:coalesce] : BroadcastOpts[:coalesce]
        opts[:coalesce_target] = pgbus_coalesce_target(target, targets) if coalesce

        BroadcastOpts.with(**opts) do
          super(*streamables, action: action, target: target, targets: targets, **rendering)
        end
      end

      # The two helpers that don't pass through `broadcast_action_to`. They
      # carry no target, so they can't coalesce (`Stream#broadcast` raises an
      # actionable error, same as `Pgbus.stream(x).broadcast(coalesce:)`
      # without one) — but they still need their pgbus options pulled out of
      # the kwargs, or a direct channel call leaks `durable: true` into
      # turbo's renderer and it ends up as an HTML attribute.
      def broadcast_refresh_to(*streamables, **attributes)
        BroadcastOpts.with(**BroadcastOpts.extract!(attributes)) do
          super(*streamables, **attributes)
        end
      end

      def broadcast_render_to(*streamables, **rendering)
        BroadcastOpts.with(**BroadcastOpts.extract!(rendering)) do
          super(*streamables, **rendering)
        end
      end

      def broadcast_stream_to(*streamables, content:)
        name = stream_name_from(streamables)
        override = BroadcastOpts[:durable]
        # When no explicit thread-local override is present, let the config
        # resolver decide: it checks `streams_durable_patterns` first (exact
        # string or regex match), then falls back to
        # `streams_default_broadcast_mode`. Passing this through keeps
        # pattern-based routing alive for the whole Turbo::Broadcastable and
        # phlex-reactive broadcast path (see issue #267).
        durable = override.nil? ? Pgbus.configuration.stream_durable?(name) : override
        Pgbus.stream(name, durable: durable).broadcast(
          content,
          exclude: BroadcastOpts[:exclude],
          visible_to: BroadcastOpts[:visible_to],
          event: BroadcastOpts[:event],
          coalesce: BroadcastOpts[:coalesce],
          target: BroadcastOpts[:coalesce_target]
        )
      end

      private

      # The coalescing key, resolved exactly as turbo resolves the rendered
      # `target=`/`targets=` attribute. `targets:` (a CSS selector) is the
      # fallback turbo itself uses when `target:` is absent; we drop the `#`
      # selector prefix because the key is never rendered — it only has to be
      # stable and distinct.
      def pgbus_coalesce_target(target, targets)
        convert_to_turbo_stream_dom_id(target) || convert_to_turbo_stream_dom_id(targets)
      end
    end

    # Apply the patch to Turbo::StreamsChannel's singleton class. Idempotent:
    # prepending the same module twice is a no-op. Called from
    # Pgbus::Engine's initializer when Turbo is detected.
    def self.install_turbo_broadcastable_patch!
      return unless defined?(::Turbo::StreamsChannel)
      return if ::Turbo::StreamsChannel.singleton_class.include?(TurboBroadcastable)

      ::Turbo::StreamsChannel.singleton_class.prepend(TurboBroadcastable)
    end

    # turbo-rails' async broadcast helpers (`broadcast_*_later_to`, the
    # default for `broadcasts_to`/`broadcasts_refreshes`) enqueue these three
    # ActiveJobs, which ship with no `queue_as` and so land on the default
    # queue — where a render+broadcast can wait behind long-running jobs
    # before the browser sees the update (#311). When the operator sets
    # `config.streams_broadcast_queue`, route them to that dedicated queue so
    # a `realtime:`-style worker capsule can isolate broadcast latency from job
    # throughput. Called from the engine's turbo_broadcastable initializer.
    # No-op when the queue is nil or turbo-rails is not loaded.
    def self.install_broadcast_queue!(queue_name)
      return if queue_name.nil?

      %w[
        Turbo::Streams::ActionBroadcastJob
        Turbo::Streams::BroadcastJob
        Turbo::Streams::BroadcastStreamJob
      ].each do |const_name|
        next unless Object.const_defined?(const_name)

        Object.const_get(const_name).queue_as(queue_name)
      end
    end
  end
end
