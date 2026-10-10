# frozen_string_literal: true

require "time"
require "active_support/core_ext/time"
require "active_support/core_ext/date_time"

module Pgbus
  module Web
    # Every timestamp, age and duration the dashboard prints (issue #497).
    #
    # Accepts the shapes the data sources hand the views: Time (pg decoders),
    # ActiveSupport::TimeWithZone (AR attributes), DateTime, ISO 8601 or
    # Time#to_s Strings (PGMQ JSON, payload["scheduled_at"]), Numeric epoch
    # seconds (ActiveJob before 7.1), and nil. Every time is shown in
    # Time.zone. Every word comes from pgbus.helpers.time in the locale.
    # Pure: no I/O, no view context; the view helpers wrap it in <time>.
    module TimeFormat
      SCOPE = "pgbus.helpers.time"
      NONE = "—"
      MINUTE = 60
      HOUR = 3600
      DAY = 86_400

      module_function

      def coerce(value)
        case value
        when nil then nil
        when Time, DateTime, ActiveSupport::TimeWithZone then value.in_time_zone
        when Numeric then Time.at(value).in_time_zone
        when String then parse(value)
        end
      end

      def parse(string)
        string = string.strip
        return nil if string.empty?

        Time.zone ? Time.zone.parse(string) : Time.parse(string)
      rescue ArgumentError
        nil
      end

      # "5m ago", "in 2h" or "now", measured from now:.
      def relative(value, now: Time.current)
        time = coerce(value)
        return nil unless time

        delta = time - now
        return I18n.t("#{SCOPE}.now") if delta.abs < 1

        distance = largest_unit(delta.abs.to_i)
        I18n.t("#{SCOPE}.#{delta.negative? ? "past" : "future"}", distance: distance)
      end

      def absolute(value)
        coerce(value)&.strftime(I18n.t("#{SCOPE}.absolute_format"))
      end

      def clock(value)
        coerce(value)&.strftime(I18n.t("#{SCOPE}.clock_format"))
      end

      def iso(value)
        coerce(value)&.utc&.iso8601
      end

      # Two units at most: "45s", "2m 5s", "1h 2m", "1d 1h".
      def duration(seconds)
        return NONE unless seconds

        seconds = seconds.to_i
        if seconds < MINUTE then unit(:seconds, seconds)
        elsif seconds < HOUR then pair(:minutes, seconds / MINUTE, :seconds, seconds % MINUTE)
        elsif seconds < DAY then pair(:hours, seconds / HOUR, :minutes, (seconds % HOUR) / MINUTE)
        else pair(:days, seconds / DAY, :hours, (seconds % DAY) / HOUR)
        end
      end

      def ms_duration(millis)
        return NONE unless millis

        millis = millis.to_i
        if millis < 1000 then unit(:milliseconds, millis)
        elsif millis < 60_000 then unit(:seconds, (millis / 1000.0).round(1))
        else unit(:minutes, (millis / 60_000.0).round(1))
        end
      end

      # "15 minutes", "6 hours", "7 days" for an Insights range in minutes.
      def range_label(minutes)
        minutes = [minutes.to_i, 1].max

        if minutes > 1440 && (minutes % 1440).zero?
          I18n.t("#{SCOPE}.range.days", count: minutes / 1440)
        elsif minutes >= 60 && (minutes % 60).zero?
          I18n.t("#{SCOPE}.range.hours", count: minutes / 60)
        else
          I18n.t("#{SCOPE}.range.minutes", count: minutes)
        end
      end

      def largest_unit(seconds)
        if seconds < MINUTE then unit(:seconds, seconds)
        elsif seconds < HOUR then unit(:minutes, seconds / MINUTE)
        elsif seconds < DAY then unit(:hours, seconds / HOUR)
        else unit(:days, seconds / DAY)
        end
      end

      def pair(big, big_count, small, small_count)
        "#{unit(big, big_count)} #{unit(small, small_count)}"
      end

      def unit(name, count)
        I18n.t("#{SCOPE}.units.#{name}", count: count)
      end
    end
  end
end
