# frozen_string_literal: true

module Pgbus
  # Persists ActiveSupport::CurrentAttributes across enqueue → perform
  # (issue #430).
  #
  # `capture` snapshots the assigned attributes of every persisted class
  # (config.current_attributes: :auto, an explicit list, or per-class
  # only:/except: filters) serialized through ActiveJob::Arguments — so
  # GlobalID models, Symbols, Times round-trip like job arguments and fall
  # under the allowed_global_id_models allowlist on the way back. An
  # attribute holding an unpersisted record is skipped with a debug log
  # (no id → no GlobalID → it could never be restored; see
  # #reject_unpersisted). `restore` nests `klass.set(attrs)` for the
  # duration of a block.
  #
  # The ActiveJob side (Pgbus::ActiveJob::CurrentAttributes) calls capture
  # from `serialize` and restore around `perform_now`, so the hop works under
  # the pgbus worker and under Rails' :test / :inline adapters alike.
  module CurrentAttributes
    METADATA_KEY = "pgbus_current"

    # The names already warned about, so each missing class is logged once.
    # Built at load; Concurrent::Map#compute_if_absent is atomic.
    @missing_warned = Concurrent::Map.new

    class << self
      def enabled?(config = Pgbus.configuration)
        !config.current_attributes.nil?
      end

      # { "Current" => serialized_attrs, ... } or nil when there is nothing
      # to persist. `override` is a per-job-class spec (false disables; an
      # Array/Hash replaces the config's list for this job).
      def capture(config = Pgbus.configuration, override: nil)
        return nil if override == false
        return nil unless override || enabled?(config)

        captured = {}
        persisted_specs(config, override: override).each do |spec|
          klass = resolve_class(spec[:name]) or next
          attrs = reject_unpersisted(klass, filter_attrs(klass.attributes, spec))
          next if attrs.empty?

          captured[klass.name] = serialize_attrs(klass, attrs)
        end
        captured.empty? ? nil : captured
      end

      # Set every stored class's attributes for the block; previous values
      # come back afterwards (ActiveSupport::CurrentAttributes#set semantics).
      def restore(stored, &block)
        return yield if stored.nil? || stored.empty?

        stored.reduce(block) do |inner, (name, attrs)|
          klass = resolve_class(name)
          next inner unless klass

          values = deserialize_attrs(klass, attrs)
          -> { klass.set(values) { inner.call } }
        end.call
      end

      # Config/override value → nil | :auto | [{ name:, only:, except: }].
      # Raises Pgbus::ConfigurationError for anything else.
      def normalize(value)
        case value
        when nil then nil
        when :auto then :auto
        when Array then value.map { |entry| class_spec(entry, {}) }
        when Hash then value.map { |entry, filters| class_spec(entry, filters) }
        else
          raise Pgbus::ConfigurationError,
                "current_attributes must be nil, :auto, an Array of CurrentAttributes classes/names, " \
                "or a Hash of class/name => { only: [...] } | { except: [...] } (got #{value.inspect})"
        end
      end

      # Normalized [{ name:, only:, except: }] for the config (or override).
      def persisted_specs(config = Pgbus.configuration, override: nil)
        source = override.nil? ? config.current_attributes : normalize(override)
        return [] if source.nil? || source == false
        return source unless source == :auto

        ActiveSupport::CurrentAttributes.descendants.filter_map do |klass|
          { name: klass.name, only: nil, except: nil } if klass.name
        end
      end

      private

      def class_spec(entry, filters)
        name = case entry
               when Class, String then entry.to_s
               else
                 raise Pgbus::ConfigurationError,
                       "current_attributes entries must be classes or class names (got #{entry.inspect})"
               end
        unless filters.is_a?(Hash) && (filters.keys - %i[only except]).empty? && filters.size <= 1
          raise Pgbus::ConfigurationError,
                "current_attributes filters for #{name} must be { only: [...] } or { except: [...] } (got #{filters.inspect})"
        end

        { name: name, only: filter_list(name, filters[:only], :only), except: filter_list(name, filters[:except], :except) }
      end

      def filter_list(name, list, kind)
        return nil if list.nil?
        unless list.is_a?(Array) && list.all? { |a| a.is_a?(Symbol) || a.is_a?(String) }
          raise Pgbus::ConfigurationError,
                "current_attributes #{kind}: for #{name} must be an Array of attribute names (got #{list.inspect})"
        end

        list.map(&:to_sym)
      end

      def resolve_class(name)
        name.constantize
      rescue NameError
        @missing_warned.compute_if_absent(name) do # rubocop:disable ThreadSafety/ClassInstanceVariable -- see @missing_warned
          Pgbus.logger.warn { "[Pgbus] current_attributes: #{name} is not defined — skipping" }
          true
        end
        nil
      end

      def filter_attrs(attrs, spec)
        attrs = attrs.compact
        attrs = attrs.slice(*spec[:only]) if spec[:only]
        attrs = attrs.except(*spec[:except]) if spec[:except]
        attrs
      end

      # An unpersisted record cannot round-trip by definition — no id, no
      # GlobalID — and capture is ambient: the enqueuer never opted into
      # persisting this attribute per-call, so its momentary state (a dev-mode
      # fallback record, a form-built model assigned to Current before save,
      # a destroyed record whose locate is guaranteed to fail) must not abort
      # the enqueue. Skip it like an unassigned attribute. persisted? (not
      # new_record?) so destroyed-but-id-bearing records are skipped too;
      # objects without the Active Record duck-type pass through untouched
      # and still hit serialize_attrs' loud CurrentAttributesError if bad.
      def reject_unpersisted(klass, attrs)
        attrs.reject do |name, value|
          next false unless value.respond_to?(:persisted?) && !value.persisted?

          Pgbus.logger.debug do
            "[Pgbus] current_attributes: #{klass.name}##{name} holds an unpersisted #{value.class} — " \
              "skipped (no id to serialize; it could never be restored)"
          end
          true
        end
      end

      def serialize_attrs(klass, attrs)
        ::ActiveJob::Arguments.serialize([attrs]).first
      rescue ::ActiveJob::SerializationError, URI::Error
        culprit, value = attrs.find do |_name, v|
          ::ActiveJob::Arguments.serialize([v])
          false
        rescue ::ActiveJob::SerializationError, URI::Error
          true
        end
        raise Pgbus::CurrentAttributesError,
              "#{klass.name}##{culprit} (#{value.class}) cannot be serialized for job persistence — " \
              "make it GlobalID/JSON-serializable or exclude it: " \
              "config.current_attributes = { #{klass.name} => { except: [:#{culprit}] } }"
      end

      def deserialize_attrs(klass, attrs)
        instance = klass.instance
        known, unknown = attrs.partition { |name, _| name.start_with?("_aj_") || instance.respond_to?("#{name}=") }
        unless unknown.empty?
          Pgbus.logger.debug do
            "[Pgbus] current_attributes: #{klass.name} no longer defines #{unknown.map(&:first).join(", ")} — dropped"
          end
        end
        ::ActiveJob::Arguments.deserialize([known.to_h]).first
      end
    end
  end
end
