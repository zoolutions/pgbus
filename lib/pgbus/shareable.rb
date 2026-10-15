# frozen_string_literal: true

module Pgbus
  # Deep-frozen, Ractor-shareable copies of plain data, for the settings
  # Configuration#finalize! freezes at boot (issue #527).
  module Shareable
    # Leaves that are plain data. Modules and classes count too: they are
    # shareable by nature.
    LEAVES = [String, Symbol, Numeric, TrueClass, FalseClass, NilClass, Regexp, Module].freeze

    module_function

    # True when +value+ is plain data all the way down: the LEAVES, and Arrays,
    # Sets, Ranges and default-less Hashes of them. A Proc, a Logger or any
    # other object anywhere inside makes it false.
    def data?(value)
      case value
      when Array, Set then value.all? { |item| data?(item) }
      when Hash then value.default_proc.nil? && data?(value.default) && value.all? { |k, v| data?(k) && data?(v) }
      when Range then data?(value.begin) && data?(value.end)
      else LEAVES.any? { |leaf| value.is_a?(leaf) }
      end
    end

    # A deep-frozen copy of plain data (see data?), Ractor-shareable where the
    # engine has Ractors. The caller's own objects are copied, never frozen in
    # place.
    def freeze_copy(value)
      return ::Ractor.make_shareable(value, copy: true) if defined?(::Ractor) && ::Ractor.respond_to?(:make_shareable)

      deep_freeze(value)
    end

    def deep_freeze(value)
      case value
      when Array then value.map { |item| deep_freeze(item) }.freeze
      when Set then value.to_set { |item| deep_freeze(item) }.freeze
      when Hash then value.to_h { |k, v| [deep_freeze(k), deep_freeze(v)] }.freeze
      when String then value.frozen? ? value : value.dup.freeze
      else value
      end
    end
  end
end
