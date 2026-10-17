# frozen_string_literal: true

module Pgbus
  module MCP
    # How MCP tools write a data-source row as JSON: times as ISO 8601 UTC,
    # symbols as strings, keys as strings, recursively.
    module Serialization
      module_function

      def serialize(value)
        case value
        when Hash then value.to_h { |key, nested| [key.to_s, serialize(nested)] }
        when Array then value.map { |nested| serialize(nested) }
        when Symbol then value.to_s
        when Time, DateTime, ActiveSupport::TimeWithZone then value.utc.iso8601
        else value
        end
      end
    end
  end
end
