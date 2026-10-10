# frozen_string_literal: true

module Pgbus
  module Web
    # Turns one dead-letter row into the words an operator reads: what killed
    # the message and how hard pgbus tried (issue #495). Reads the
    # DeadLetterHeader block the mover wrote; a row without one predates it.
    # No I/O, like JobState: the view helper formats the args.
    module DeadLetterReason
      CELL_MESSAGE_LIMIT = 160

      Result = Data.define(:reason_key, :reason_args, :legacy, :source, :source_queue, :attempts, :max_retries,
                           :error_class, :error_message, :backtrace, :error_attempt, :retried_before, :died_at) do
        def legacy? = legacy

        # The attempts after the recorded error that recorded nothing (a
        # worker crashed mid-perform), or nil when the error is the last one.
        def later_attempts
          return unless error_attempt && attempts

          last_run = attempts - 1
          (error_attempt + 1)..last_run if error_attempt < last_run
        end
      end

      module_function

      def present(row)
        block = DeadLetterHeader.parse(row[:headers])
        return legacy(row) unless block

        attempts = block["attempts"]&.to_i
        max = block["max_retries"]&.to_i
        key, args = reason_for(block, attempts, max)
        Result.new(
          reason_key: key, reason_args: args, legacy: false, source: block["source"],
          source_queue: block["source_queue"], attempts: attempts, max_retries: max,
          error_class: block["error_class"], error_message: block["error_message"], backtrace: block["backtrace"],
          error_attempt: block["error_attempt"]&.to_i, retried_before: block["retries_from_dlq"]&.to_i,
          died_at: row[:enqueued_at]
        )
      end

      def reason_for(block, attempts, max)
        error_class = block["error_class"]
        unless error_class
          key = block["source"] == "consumer" ? "event_no_error" : "no_error_recorded"
          return [key, { attempts: attempts, max: max }]
        end

        args = { error_class: error_class, error_message: cell_message(block["error_message"]),
                 attempts: attempts, max: max }
        error_attempt = block["error_attempt"]&.to_i
        return ["error", args] unless error_attempt && attempts && error_attempt < attempts - 1

        ["error_earlier_attempt", args.merge(error_attempt: error_attempt)]
      end

      def legacy(row)
        Result.new(
          reason_key: "not_recorded", reason_args: { version: DeadLetterHeader::SINCE }, legacy: true,
          source: nil, source_queue: row[:queue_name].to_s.delete_suffix(Pgbus::DEAD_LETTER_SUFFIX),
          attempts: nil, max_retries: nil, error_class: nil, error_message: nil, backtrace: nil,
          error_attempt: nil, retried_before: nil, died_at: row[:enqueued_at]
        )
      end

      def cell_message(message)
        text = message.to_s
        text.length > CELL_MESSAGE_LIMIT ? "#{text[0, CELL_MESSAGE_LIMIT - 1]}…" : text
      end
    end
  end
end
