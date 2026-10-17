# frozen_string_literal: true

module Pgbus
  class Workflow
    # Turns an exception into the step row's error columns: what an operator
    # needs to act on a failed step without opening the error service.
    #
    #   error_class      the class name
    #   error_message    capped at MESSAGE_LIMIT characters
    #   error_backtrace  cleaned through Rails.backtrace_cleaner when Rails is
    #                    loaded, cut to config.workflow_error_backtrace_lines
    #   error_causes     [{ "class", "message" }] along Exception#cause, at most
    #                    MAX_CAUSES deep (a cyclic chain stops where it repeats)
    module ErrorCapture
      MESSAGE_LIMIT = 10_000
      CAUSE_MESSAGE_LIMIT = 1_000
      MAX_CAUSES = 5

      module_function

      def capture(error, backtrace_lines: Pgbus.configuration.workflow_error_backtrace_lines)
        {
          error_class: error.class.name,
          error_message: truncate(error.message.to_s, MESSAGE_LIMIT),
          error_backtrace: backtrace(error, backtrace_lines).join("\n"),
          error_causes: causes(error)
        }
      end

      # The columns cleared when a later attempt succeeds or a run resumes.
      def cleared = { error_class: nil, error_message: nil, error_backtrace: nil }

      def backtrace(error, lines)
        raw = error.backtrace || []
        cleaned = clean(raw)
        (cleaned.empty? ? raw : cleaned).first(lines)
      end

      def causes(error)
        seen = {}.compare_by_identity
        seen[error] = true
        chain = []
        cause = error.cause
        while cause && chain.size < MAX_CAUSES && !seen[cause]
          seen[cause] = true
          chain << { "class" => cause.class.name, "message" => truncate(cause.message.to_s, CAUSE_MESSAGE_LIMIT) }
          cause = cause.cause
        end
        chain
      end

      def truncate(string, limit)
        string.length > limit ? "#{string[0, limit - 1]}…" : string
      end

      def clean(lines)
        return lines unless defined?(::Rails) && ::Rails.respond_to?(:backtrace_cleaner) && ::Rails.backtrace_cleaner

        ::Rails.backtrace_cleaner.clean(lines)
      end
    end
  end
end
