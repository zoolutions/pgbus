# frozen_string_literal: true

module RuboCop
  module Cop
    module Pgbus
      # Flags hand-rolled time formatting in the dashboard's Ruby code (issue
      # #497). Every timestamp, age and duration the dashboard prints goes
      # through one presenter, Pgbus::Web::TimeFormat, and its view helpers:
      #
      # - pgbus_time(value, clock: false): <time datetime title>5m ago</time>
      # - pgbus_timestamp(value): the absolute, then "(5m ago)"
      # - pgbus_absolute_time(value): the absolute alone
      # - pgbus_duration(seconds) / pgbus_ms_duration(millis): "2m 5s" / "1.5s"
      #
      # Those are in Time.zone, translated in every locale, sign-aware, and
      # accept every shape the data sources return. strftime and Rails' distance
      # helpers are none of these. Scoped by .rubocop.yml to app/ and
      # lib/pgbus/web/, excluding the presenter itself. ERB views are guarded by
      # spec/pgbus/web/time_conventions_spec.rb, since RuboCop cannot parse them.
      #
      # No autocorrect: which helper fits (list cell, expanded row, duration)
      # is a call the author makes.
      #
      # @example
      #   # bad
      #   task[:next_run_at]&.strftime("%Y-%m-%d %H:%M")
      #   time_ago_in_words(row[:created_at])
      #
      #   # good
      #   pgbus_time(task[:next_run_at], clock: true)
      #   pgbus_time(row[:created_at])
      class DashboardTimeFormatting < Base
        MSG = "Format dashboard times with pgbus_time / pgbus_timestamp / pgbus_absolute_time / " \
              "pgbus_duration (Pgbus::Web::TimeFormat), never `%<method>s`."

        RESTRICT_ON_SEND = %i[
          strftime time_ago_in_words distance_of_time_in_words distance_of_time_in_words_to_now
          to_fs to_formatted_s l localize
        ].freeze

        def on_send(node)
          add_offense(node.loc.selector, message: format(MSG, method: node.method_name))
        end
        alias on_csend on_send
      end
    end
  end
end
