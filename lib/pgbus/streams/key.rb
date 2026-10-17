# frozen_string_literal: true

require "digest"

module Pgbus
  module Streams
    # Short, pgbus-safe stream identifiers.
    #
    # PGMQ queue names are bounded by two ceilings: PostgreSQL's
    # NAMEDATALEN (63 chars for `pgmq.q_<name>`) and pgmq-ruby's own
    # stricter runtime check (`length >= 48` in
    # `PGMQ::Client#validate_queue_name!`). The effective budget is the
    # lower of the two, exposed as `QueueNameValidator::MAX_QUEUE_NAME_LENGTH`
    # (currently 47). Any stream name composed from UUID primary keys
    # and turbo-rails-style dom ids blows past that budget almost
    # immediately:
    #
    #     "gid://app/Ai::Chat/9c14e8b2-94c3-4c6f-8ca1-f50d2f5e22ca:messages"
    #     # => 63 chars, already too long before the "pgbus_" prefix is added.
    #
    # `stream_key` produces a deterministic short form suitable as a
    # pgbus stream identifier. It normalizes each part, joins with ":",
    # and enforces the queue-name budget (derived from the configured
    # `queue_prefix`) at the call site — raising ArgumentError rather
    # than letting the failure surface as an opaque QueueNameValidator
    # error deep inside `Pgbus.stream(...).broadcast(...)`.
    #
    # Silent truncation is intentionally NOT supported: trimming a
    # too-long key to fit would reintroduce the collision risk that the
    # 64-bit digest is chosen to eliminate. Callers who overflow should
    # shorten their own identifiers or adopt `Pgbus::Streams::Streamable`
    # on their ActiveRecord models.
    #
    # Usage:
    #
    #     Pgbus.stream_key(chat, :messages)
    #       # => "ai_chat_3a4f9c21b7d20e18:messages"
    #
    #     Pgbus.stream_key([user, :notifications])
    #       # => "user_5fa83c91d44a2701:notifications"
    #
    #     Pgbus.stream(Pgbus.stream_key(chat, :messages)).broadcast("<turbo-stream/>")
    #
    # Collision horizon: the 64-bit SHA-256 prefix gives a birthday bound
    # of roughly 5 billion records per model class before a 50% chance of
    # collision. For multi-tenant apps where a collision would mean two
    # records share a stream (and receive each other's broadcasts), this
    # is wide enough in practice. Callers with higher sensitivity can
    # pass `digest_bits: 128`.
    module Key
      DEFAULT_DIGEST_BITS = 64

      module_function

      # Compose a short pgbus-safe stream name from any mix of records,
      # strings, symbols, and arrays. Returns the joined key when it fits
      # the pgbus queue-name budget; raises ArgumentError otherwise.
      #
      # Fragments must not contain `:` — it's the join separator, so
      # `stream_key("a:b", :c)` and `stream_key("a", "b:c")` would both
      # produce `"a:b:c"` and collapse two logically distinct streams
      # onto one queue. Colons inside fragments (typically from a
      # `to_stream_key`/`to_gid_param` implementation that forgot to
      # sanitize) raise an ArgumentError at the call site.
      def stream_key(*parts, digest_bits: DEFAULT_DIGEST_BITS)
        flattened = Array(parts).flatten

        # Idempotency for an already-built key: a single String argument
        # is treated as a pre-built pgbus stream key and returned
        # unchanged (after the budget check). This lets a consumer hold
        # one `stream_key` value and pass it to both `turbo_stream_from`
        # and the broadcaster without the colon separator guard raising
        # on the second call. The guard only protects against ambiguous
        # *joins* (`stream_key('a:b', :c)` vs `stream_key('a', 'b:c')`),
        # and there is no second fragment here to collapse against, so the
        # hazard cannot arise. Symbols and records are NOT keys — a colon
        # in those never came from `stream_key` and stays a mistake.
        return stream_key!(flattened.first) if flattened.length == 1 && flattened.first.is_a?(String)

        fragments = flattened.map { |part| normalize(part, digest_bits: digest_bits) }
        fragments.each { |fragment| reject_colons!(fragment) }
        validate_budget!(fragments.join(":"))
      end

      # Accepts an already-built stream key verbatim, skipping the
      # per-fragment colon guard (a pre-built key legitimately contains
      # ':' separators). Still enforces the queue-name budget so an
      # oversized key fails at the call site rather than deep inside
      # Client#ensure_stream_queue. Use this when you hold a key string
      # and want to be explicit that no re-keying should happen — e.g.
      # passing the same value to `turbo_stream_from` and a broadcaster.
      def stream_key!(key)
        raise ArgumentError, "stream_key! key must be a String, got #{key.class}" unless key.is_a?(String)

        validate_budget!(key)
      end

      # Returns the key when it fits the pgbus queue-name budget; raises
      # ArgumentError with an actionable message otherwise. Shared by
      # `stream_key` and `stream_key!` so both paths fail identically.
      def validate_budget!(key)
        budget = queue_name_budget
        return key if key.length <= budget

        raise ArgumentError,
              "stream_key #{key.inspect} is #{key.length} chars, " \
              "exceeds pgbus budget of #{budget} " \
              "(queue_prefix=#{queue_prefix.inspect}, " \
              "pgbus_max_queue_name_length=#{QueueNameValidator::MAX_QUEUE_NAME_LENGTH}). " \
              "Shorten the streamables or use Pgbus::Streams::Streamable on the model."
      end

      # 64-bit (default) SHA-256 prefix of the record's primary key. Stdlib
      # only, deterministic, and fixed-length. CRC32's 32-bit output is
      # intentionally not used here: its ~77k-row birthday bound is too
      # tight for a multi-tenant stream identifier where a collision would
      # route two records' broadcasts to the same queue.
      # Full output size of the backing digest, in bits. Capping
      # digest_bits here matters because `SHA256.hexdigest` only
      # produces 64 hex chars (256 bits) no matter what — slicing
      # `[0, 128]` just returns all 64 chars — so a caller asking for
      # `digest_bits: 512` would silently get the same output as
      # `digest_bits: 256` and walk away believing they'd widened the
      # collision horizon. Raise instead.
      MAX_DIGEST_BITS = ::Digest::SHA256.new.digest_length * 8 # => 256

      def short_id(record, digest_bits: DEFAULT_DIGEST_BITS)
        unless digest_bits.is_a?(Integer) && digest_bits.positive? &&
               (digest_bits % 4).zero? && digest_bits <= MAX_DIGEST_BITS
          raise ArgumentError,
                "digest_bits must be a positive multiple of 4 and <= #{MAX_DIGEST_BITS} " \
                "(SHA-256 produces #{MAX_DIGEST_BITS} bits; asking for more would silently truncate)"
        end

        # Unpersisted records all share id=nil, which hashes to a single
        # constant digest and would collapse every new instance of the
        # same class into one stream. Fail loud at the first unsaved
        # call site — the whole point of the 64-bit digest is to
        # eliminate collisions, so silently producing a shared key here
        # would reintroduce exactly what it was chosen to prevent.
        if record.id.nil?
          raise ArgumentError,
                "#{record.class.name} must be persisted before generating a stream key " \
                "(record.id is nil — all unsaved records would collide on one stream)"
        end

        hex_chars = digest_bits / 4
        ::Digest::SHA256.hexdigest(record.id.to_s)[0, hex_chars]
      end

      # Normalize a single streamable fragment to a pgbus-safe string.
      # Mirrors the shape accepted by Turbo::Streams::StreamName and
      # Pgbus::Streams::Stream.name_from so the two code paths agree
      # on the wire format.
      #
      # - Strings and symbols pass through verbatim.
      # - ActiveRecord models become "<param_key>_<short_id>".
      # - Anything else responding to `to_gid_param` / `to_param` falls
      #   back to that; a UUID primary key would still overflow, which
      #   is why AR models are hashed above.
      def normalize(part, digest_bits: DEFAULT_DIGEST_BITS)
        case part
        when String, Symbol
          part.to_s
        else
          if defined?(::ActiveRecord::Base) && part.is_a?(::ActiveRecord::Base)
            "#{part.class.model_name.param_key}_#{short_id(part, digest_bits: digest_bits)}"
          elsif part.respond_to?(:to_stream_key)
            part.to_stream_key
          elsif part.respond_to?(:to_gid_param)
            part.to_gid_param
          elsif part.respond_to?(:to_param)
            part.to_param
          else
            part.to_s
          end
        end
      end

      # Budget = effective pgbus queue-name limit - "<queue_prefix>_"
      # length. Computed at call time (not a constant) so apps that
      # override `config.queue_prefix` get the correct budget
      # automatically.
      def queue_name_budget
        QueueNameValidator::MAX_QUEUE_NAME_LENGTH - queue_prefix.length - 1
      end

      # The live configuration's queue_prefix on the main Ractor. A non-main
      # Ractor cannot read Pgbus.configuration (it holds the logger and the
      # app's callables), so there it is the published snapshot's: the
      # prefix as the last Pgbus.finalize! or Pgbus.configure left it.
      def queue_prefix
        config = Shareable.main_ractor? ? Pgbus.configuration : Pgbus.configuration_snapshot
        config.queue_prefix
      end
      private_class_method :queue_prefix

      # Raises if a normalized fragment contains the `:` separator.
      # Kept private-ish via module_function so the guard is shared
      # between stream_key and any future composer without becoming
      # part of the public surface.
      def reject_colons!(fragment)
        return unless fragment.include?(":")

        raise ArgumentError,
              "stream_key fragment #{fragment.inspect} contains ':' which is the " \
              "join separator — two calls with different colon placements would " \
              "collapse to the same key (e.g. stream_key('a:b', :c) vs " \
              "stream_key('a', 'b:c') both produce 'a:b:c'). Strip or replace " \
              "colons in the offending streamable before calling stream_key."
      end
      private_class_method :reject_colons!
    end
  end
end
