# frozen_string_literal: true

require "json"
require "time"

module Pgbus
  # The `pgbus_dead_letter` block a mover writes into a message's PGMQ
  # headers when it moves the message to a dead-letter queue (issue #495):
  # why it was moved, from where, after how many attempts, and the last
  # recorded error. Pure — no I/O — so the executor, the consumer, the
  # dashboard, MCP and the CLI all read and write the same shape.
  #
  # PGMQ headers travel as raw JSON strings, so every entry point accepts a
  # String (or a Hash, or nil) and build/strip_for_retry return a String.
  module DeadLetterHeader
    KEY = "pgbus_dead_letter"
    # Top-level counter on a live message retried out of a DLQ. Folded into
    # the block as `retries_from_dlq` when the message dies again.
    RETRIES_KEY = "pgbus_dlq_retries"
    ORIGINAL_KEY = "pgbus_original_headers"
    VERSION = 1
    REASON_MAX_RETRIES = "max_retries_exceeded"
    # The release that started writing the block: rows without it predate it.
    SINCE = "0.18.0"
    MESSAGE_LIMIT = 1_000
    BACKTRACE_LINES = 10
    BACKTRACE_LIMIT = 2_000

    module_function

    # error: FailedEventRecorder.last_error's Hash, or nil when no failure
    # was recorded. Returns the merged headers as a JSON string.
    def build(existing:, reason:, source:, source_queue:, attempts:, max_retries:, error: nil, now: Time.now.utc)
      headers = decode(existing, warn: true)
      retries = headers.delete(RETRIES_KEY).to_i
      block = {
        "version" => VERSION,
        "reason" => reason,
        "source" => source,
        "source_queue" => source_queue,
        "attempts" => attempts,
        "max_retries" => max_retries,
        "dead_lettered_at" => iso(now)
      }
      block["retries_from_dlq"] = retries if retries.positive?
      block.merge!(error_fields(error)) if error
      JSON.generate(headers.merge(KEY => block))
    end

    # The block as a string-keyed Hash, or nil. Never raises.
    def parse(headers)
      block = decode(headers, warn: false)[KEY]
      block.is_a?(Hash) ? block : nil
    end

    # A live message must not claim to be dead: drop the block, and carry
    # how many times it has come back out of a DLQ. A DLQ message has no
    # top-level counter (build folded it into the block), so the block is
    # the only place to read the previous count from.
    def strip_for_retry(headers)
      retries = parse(headers)&.dig("retries_from_dlq").to_i + 1
      stripped = decode(headers, warn: false)
      stripped.delete(KEY)
      stripped[RETRIES_KEY] = retries
      JSON.generate(stripped)
    end

    def error_fields(error)
      {
        "error_class" => error[:error_class],
        "error_message" => error[:error_message]&.to_s&.slice(0, MESSAGE_LIMIT),
        "backtrace" => backtrace_head(error[:backtrace]),
        "error_attempt" => error[:retry_count].to_i + 1,
        "error_recorded_at" => error[:failed_at] && iso(error[:failed_at])
      }.compact
    end

    def backtrace_head(backtrace)
      return if backtrace.nil? || backtrace.empty?

      lines = backtrace.is_a?(Array) ? backtrace : backtrace.to_s.split("\n")
      lines.first(BACKTRACE_LINES).join("\n").slice(0, BACKTRACE_LIMIT)
    end

    def iso(value)
      case value
      when Time then value.getutc.iso8601(6)
      when String then value
      else value.respond_to?(:to_time) ? value.to_time.utc.iso8601(6) : value.to_s
      end
    end

    # Always a Hash. Headers that are not a JSON object are kept, not
    # dropped, under ORIGINAL_KEY.
    def decode(headers, warn:)
      case headers
      when nil, "" then {}
      when Hash then headers.transform_keys(&:to_s)
      else wrap(JSON.parse(headers.to_s), headers, warn)
      end
    rescue JSON::ParserError
      log_kept(headers) if warn
      { ORIGINAL_KEY => headers }
    end

    def wrap(parsed, raw, warn)
      return parsed if parsed.is_a?(Hash)

      log_kept(raw) if warn
      { ORIGINAL_KEY => parsed }
    end

    def log_kept(raw)
      Pgbus.logger.warn do
        "[Pgbus] Dead-letter headers were not a JSON object; kept under #{ORIGINAL_KEY}: #{raw.to_s[0, 200]}"
      end
    end
  end
end
