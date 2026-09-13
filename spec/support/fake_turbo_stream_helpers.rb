# frozen_string_literal: true

require "action_view"
require "active_support/core_ext/array/wrap"

# Shared behaviour for the fake `Turbo::StreamsChannel` doubles in the streams
# specs. turbo-rails is not loaded in unit tests, but two pieces of its
# behaviour are load-bearing for what those specs assert, so they are mirrored
# here rather than approximated.
module FakeTurboStreamHelpers
  # Verbatim from Turbo::Streams::ActionHelper#convert_to_turbo_stream_dom_id.
  #
  # `TurboBroadcastable#pgbus_coalesce_target` resolves the coalescing key
  # through this method, so an approximation here would let a spec assert a key
  # production never produces — `Admin::Order` keys on `admin_order_7`, not
  # `admin::order_7`, and a Class target keys on `new_order`, not `Order`.
  def convert_to_turbo_stream_dom_id(target, include_selector: false)
    target_array = Array.wrap(target)
    if target_array.any? { |value| value.respond_to?(:to_key) || value.is_a?(Class) }
      "#{"#" if include_selector}#{ActionView::RecordIdentifier.dom_id(*target_array)}"
    else
      target
    end
  end

  # Whatever is left of the kwargs after turbo's own broadcast helpers take
  # their share is handed to turbo's renderer (or, for `broadcast_refresh_to`,
  # straight onto the tag as HTML attributes). pgbus's options must never get
  # that far, so the fakes fail loudly the moment one does — otherwise a
  # permissive `**` swallows the leak and the "does not leak into turbo's
  # rendering kwargs" specs are vacuous.
  #
  # This is a DENYLIST of pgbus's own option names, not an allowlist of turbo's,
  # because that is exactly the invariant under test: "no pgbus option reaches
  # turbo". An allowlist would be both under-inclusive (turbo's refresh takes
  # arbitrary keys as HTML attributes, plus `request_id:`, so a legitimate call
  # would false-fail) and coupled to turbo's evolving kwarg surface.
  def reject_leaked_kwargs!(rendering)
    leaked = rendering.keys & Pgbus::Streams::BroadcastOpts::KEYS
    return if leaked.empty?

    raise ArgumentError, "leaked into turbo's rendering kwargs: #{leaked.inspect}"
  end
end
