# frozen_string_literal: true

require "date"

module Pgbus
  module Web
    # The Workflows list's query string (issue #542), checked: a status from
    # DataSource::WorkflowList::STATUSES, the text filters stripped (blank
    # ones dropped, each at most MAX_LENGTH characters) and the created-at
    # range parsed in Time.zone (a bare date as the start or the end of that
    # day). Anything else raises Invalid, which the controller answers with
    # 400. #to_param is the same query back, for links that keep it.
    class WorkflowQuery
      class Invalid < StandardError; end

      TEXT_FILTERS = %i[workflow_class step error_class subject_type subject_id workflow_id].freeze
      DATE_FILTERS = %i[created_from created_to].freeze
      MAX_LENGTH = 255
      DATE_ONLY = /\A\d{4}-\d{2}-\d{2}\z/
      # A date (what <input type="date"> sends) or an ISO 8601 date and time.
      DATE_OR_TIME = /\A\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:?\d{2})?)?\z/

      attr_reader :status, :filters, :raw

      def initialize(params)
        @status = (params[:status].presence || "ongoing").to_s
        raise Invalid, "unknown status #{@status.inspect}" unless DataSource::WorkflowList::STATUSES.include?(@status)

        @raw = {}
        @filters = {}
        TEXT_FILTERS.each { |key| text(key, params[key]) }
        DATE_FILTERS.each { |key| date(key, params[key]) }
      end

      def filtered? = @raw.any?

      # The query as URL params (status and the filters as given), merged with +extra+.
      def to_param(**extra) = { status: status, **@raw }.merge(extra).compact

      private

      def text(key, value)
        value = scalar(key, value)
        return if value.empty?
        raise Invalid, "#{key} is longer than #{MAX_LENGTH} characters" if value.length > MAX_LENGTH

        @raw[key] = value
        @filters[key] = value
      end

      def date(key, value)
        value = scalar(key, value)
        return if value.empty?

        time = parse_time(value) || raise(Invalid, "#{key} #{value.inspect} is not a date")
        time = time.end_of_day if key == :created_to && value.match?(DATE_ONLY)
        @raw[key] = value
        @filters[key] = time
      end

      def scalar(key, value)
        return "" if value.nil?
        raise Invalid, "#{key} must be a single value" unless value.is_a?(String)

        value.strip
      end

      # Date.valid_date? because Time.zone.parse rolls 2026-02-30 over to March.
      def parse_time(value)
        return nil unless value.match?(DATE_OR_TIME) && Date.valid_date?(*value[0, 10].split("-").map(&:to_i))

        Time.zone ? Time.zone.parse(value) : Time.parse(value)
      rescue ArgumentError
        nil
      end
    end
  end
end
