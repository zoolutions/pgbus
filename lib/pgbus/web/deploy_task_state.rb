# frozen_string_literal: true

module Pgbus
  module Web
    # The Deploy tasks page's words for one task (issue #536), derived from
    # its row and the rollover the scheduler acts on (DataSource#deploy_rollover),
    # so the page prints the reason the scheduler waits for. Pure.
    #
    # States: disabled, waiting (armed, gate closed, with the gate's reason),
    # due (armed, gate open), ran (not armed, has a rollover run), never. A
    # manual run is appended to any state through manual_args.
    module DeployTaskState
      BADGE_TONES = { "disabled" => :gray, "waiting" => :yellow, "due" => :indigo, "ran" => :green,
                      "never" => :gray }.freeze

      Result = Data.define(:state, :reason_key, :reason_args, :badge_tone, :manual_args)

      module_function

      # row – a DataSource#deploy_tasks row. rollover – DataSource#deploy_rollover, or nil when unreadable.
      def present(row, rollover)
        state, key, args = derive(row, rollover)
        manual = row[:last_manual_run]
        Result.new(state: state, reason_key: key, reason_args: args, badge_tone: BADGE_TONES.fetch(state),
                   manual_args: manual ? { ago: manual[:run_at] } : nil)
      end

      def derive(row, rollover)
        return ["disabled", row[:armed_at] ? "disabled_armed" : "disabled", {}] unless row[:enabled]
        return armed(rollover) if row[:armed_at]

        run = row[:last_rollover_run]
        return ["never", "never", {}] unless run

        ["ran", "ran", { ago: run[:run_at], revision: run[:revision] }]
      end

      def armed(rollover)
        return ["waiting", "waiting_unknown", {}] unless rollover
        return ["waiting", "waiting_no_scheduler", {}] unless rollover[:scheduler]

        case rollover[:reason]
        when :ready then ["due", "due", {}]
        when :lagging then ["waiting", "waiting_lagging", { count: rollover[:lagging].size, revision: rollover[:revision] }]
        when :no_workers then ["waiting", "waiting_no_workers", { revision: rollover[:revision] }]
        else ["waiting", "waiting_no_revision", {}]
        end
      end
    end
  end
end
