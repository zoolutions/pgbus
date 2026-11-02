# frozen_string_literal: true

module Pgbus
  # Validates and sanitizes PGMQ queue names for safe use in SQL identifiers.
  #
  # PGMQ queue names are interpolated into SQL as table/sequence names
  # (e.g., pgmq.q_<name>, pgmq.a_<name>). This module enforces strict
  # validation to prevent SQL injection via crafted queue names.
  module QueueNameValidator
    # PostgreSQL's NAMEDATALEN caps identifiers at 63 bytes, but the
    # effective limit is tighter: pgmq-ruby (our transport gem) rejects
    # any queue name with `length >= PGMQ::QueueName::MAX_LENGTH` (48) in
    # `PGMQ::QueueName.validate!`, whose error has said "maximum length of
    # 47 characters" since pgmq-ruby 0.8.2 (48 before).
    # That leaves an actual usable ceiling of 47 characters for the
    # fully-prefixed name (`<queue_prefix>_<logical_name>`), which is what
    # this constant expresses. pgmq-ruby picked 48 to leave headroom for
    # PGMQ's internal tables (`pgmq.q_`, `pgmq.a_`, sequences, indexes)
    # which all get suffixed beyond the base name.
    #
    # Historically this was 61 (the raw PostgreSQL ceiling minus PGMQ's
    # `q_`/`a_` prefix). That was wrong in practice: names in the 48-61
    # range passed pgbus's validator but blew up deep inside pgmq-ruby
    # with an InvalidQueueNameError — exactly the kind of opaque failure
    # the validator was meant to catch up front.
    MAX_QUEUE_NAME_LENGTH = 47

    # Only alphanumeric characters and underscores are allowed.
    VALID_QUEUE_NAME_PATTERN = /\A[a-zA-Z0-9_]+\z/

    module_function

    # Validates a queue name for safe SQL identifier use.
    # Returns the name if valid, raises ArgumentError if not.
    def validate!(name)
      name = name.to_s
      raise ArgumentError, "Queue name cannot be blank" if name.empty?
      if name.length > MAX_QUEUE_NAME_LENGTH
        raise ArgumentError,
              "Queue name too long (#{name.length} chars, max #{MAX_QUEUE_NAME_LENGTH}): #{name.inspect}"
      end

      unless VALID_QUEUE_NAME_PATTERN.match?(name)
        raise ArgumentError,
              "Invalid queue name: #{name.inspect}. Only alphanumeric characters and underscores are allowed."
      end

      name
    end

    # Normalizes a queue name by replacing common separators (hyphens,
    # dots, colons) with underscores, stripping remaining invalid
    # characters, and collapsing consecutive underscores. Use this for
    # names from external sources (e.g., Turbo stream names like
    # "hotwire-livereload" or "gid://app/Foo/1") where the intent is
    # to derive a valid PGMQ queue name that preserves as much of the
    # original identifier as possible.
    #
    # Colons in particular are the turbo-rails stream-name separator
    # (`Pgbus.stream([user, :notifications])` → `"user_gid:notifications"`),
    # so they must map to a safe character rather than be stripped —
    # otherwise `"a:b"` and `"ab"` would collide on the same queue.
    #
    # The result is lower case (issue #633): PGMQ lower-cases its table
    # names (`pgmq.format_table_name`), so "Orders" and "orders" share
    # `pgmq.q_orders` while `pgmq.meta` records each spelling as its own
    # queue. Spelling every name the way its table is gives one name per
    # table. `validate!` and `sanitize!` keep case: they also run on names
    # read back from `pgmq.meta`, which an install from before #633 may
    # still hold in mixed case (`rails generate pgbus:fold_queue_name_case`).
    def normalize(name)
      name = name.to_s
      return validate!(name).downcase if VALID_QUEUE_NAME_PATTERN.match?(name)

      normalized = name.gsub(/[-.:]/, "_")          # hyphens/dots/colons → underscores
                       .gsub(/[^a-zA-Z0-9_]/, "")   # strip remaining invalid chars
                       .gsub(/_+/, "_")              # collapse consecutive underscores
                       .gsub(/\A_|_\z/, "")          # strip leading/trailing underscores
                       .downcase
      validate!(normalized)
      normalized
    end

    # Sanitizes a queue name by removing invalid characters, then validates.
    # Use this for names from untrusted sources (e.g., URL params).
    def sanitize!(name)
      sanitized = name.to_s.gsub(/[^a-zA-Z0-9_]/, "")
      validate!(sanitized)
      sanitized
    end
  end
end
