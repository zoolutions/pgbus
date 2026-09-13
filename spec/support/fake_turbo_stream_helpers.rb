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
  # their share is handed to turbo's renderer. pgbus's options must never get
  # that far, so the fakes reject every keyword they don't themselves render.
  #
  # This is deliberately STRICTER than turbo: the contract under test is "the
  # pgbus option was extracted before `super`", not "turbo happened to tolerate
  # it". A permissive `**` would swallow a leak and make the
  # "does not leak into turbo's rendering kwargs" specs vacuous.
  RENDER_KEYS = %i[content html render partial template locals layout formats attributes].freeze

  def reject_leaked_kwargs!(rendering)
    leaked = rendering.keys - RENDER_KEYS
    return if leaked.empty?

    raise ArgumentError, "leaked into turbo's rendering kwargs: #{leaked.inspect}"
  end
end
