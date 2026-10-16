# frozen_string_literal: true

require_relative "../../integration_helper"

# Integration tests for the stream-name length safety net and the
# `Pgbus.stream_key` helper. These exercise the full path:
#
#   Pgbus.stream_key(...)  -> short name
#   Pgbus.stream(name)     -> constructs Stream (which calls validate_name_length!)
#   stream.broadcast(html) -> ensure_stream_queue -> queue_name(name)
#                             -> QueueNameValidator.validate!
#                             -> real PGMQ insert
#   stream.read_after      -> real PGMQ read
#
# The point is to prove the two layers (Pgbus::Streams::Stream's
# validate_name_length! and the underlying QueueNameValidator) agree on
# the budget for the real, configured queue_prefix and that the short
# digest produced by `stream_key` is actually accepted by PGMQ.
RSpec.describe "Streams: name length safety net (integration)", :integration do
  let(:budget) { Pgbus::Streams::Key.queue_name_budget }

  # A Struct that looks enough like an AR model for Pgbus::Streams::Key
  # to hash its id into a short_id. Using a real ActiveRecord model would
  # force integration_helper to bootstrap an unrelated table; this stays
  # focused on the naming layer while still going through real PGMQ.
  let(:fake_model_class) do
    Class.new do
      attr_reader :id

      def initialize(id)
        @id = id
      end

      def to_stream_key
        "fake_chat_#{Digest::SHA256.hexdigest(id.to_s)[0, 16]}"
      end
    end
  end

  let(:fake_chat) { fake_model_class.new("9c14e8b2-94c3-4c6f-8ca1-f50d2f5e22ca") }

  # Snapshots the set of PGMQ queue names via Pgbus::Client (the library
  # boundary) rather than reaching into pgmq.meta directly. This keeps
  # the spec aligned with the project rule that all PGMQ interactions
  # go through Client — see .claude/rules/coding-style.md.
  def queue_names_snapshot
    Pgbus.client.list_queues.map { |q| q.respond_to?(:queue_name) ? q.queue_name : q.to_s }.sort
  end

  describe "Pgbus.stream_key produces a broadcast-able short name" do
    it "round-trips a broadcast + read_after through real PGMQ" do
      name = Pgbus.stream_key(fake_chat, :messages)
      expect(name).to match(/\Afake_chat_[0-9a-f]{16}:messages\z/)

      stream = Pgbus.stream(name)
      msg_id = stream.broadcast("<turbo-stream>hello</turbo-stream>")
      expect(msg_id.to_i).to be > 0

      replayed = stream.read_after(after_id: 0, limit: 10)
      payloads = replayed.map { |envelope| JSON.parse(envelope.payload) }
      expect(payloads.map { |p| p["html"] }).to include("<turbo-stream>hello</turbo-stream>")
    end

    it "is stable — two constructions of the same stream land in the same PGMQ queue" do
      name = Pgbus.stream_key(fake_chat, :messages)
      Pgbus.stream(name).broadcast("<a/>")
      Pgbus.stream(name).broadcast("<b/>")

      replayed = Pgbus.stream(name).read_after(after_id: 0, limit: 10)
      htmls = replayed.map { |envelope| JSON.parse(envelope.payload)["html"] }
      expect(htmls).to include("<a/>", "<b/>")
    end
  end

  describe "overflow path — forgotten call site fails fast" do
    it "raises StreamNameTooLong at Pgbus.stream(...) before touching PGMQ" do
      # Simulates `broadcast_replace_to [long_record, :some_long_suffix]`
      # where the composed name overflows. The error must originate
      # from Pgbus::Streams::Stream, not from deep inside PGMQ, and
      # must NOT have side effects on the queue catalog.
      long = "leak_check_#{"z" * budget}"
      queues_before = queue_names_snapshot

      expect { Pgbus.stream(long) }
        .to raise_error(Pgbus::Streams::StreamNameTooLong, /exceeds pgbus budget/)

      queues_after = queue_names_snapshot
      expect(queues_after).to eq(queues_before)
    end

    it "names the streamables and points at the helper in the error message" do
      long = "x" * (budget + 5)
      expect { Pgbus.stream(long) }
        .to raise_error(Pgbus::Streams::StreamNameTooLong) do |err|
          expect(err.message).to include(long.length.to_s)
          expect(err.message).to include("Pgbus.stream_key")
        end
    end

    it "remains an ArgumentError so pre-existing rescues still catch it" do
      long = "y" * (budget + 5)
      expect { Pgbus.stream(long) }.to raise_error(ArgumentError)
    end
  end

  describe "budget tracks config.queue_prefix" do
    it "allows a name that fits the current budget and reaches real PGMQ" do
      # Pick a name exactly at the budget so the assertion actually
      # exercises the boundary, not a comfortable interior value.
      short_name = "s" * budget
      expect(short_name.length).to eq(budget)

      stream = Pgbus.stream(short_name)
      expect { stream.broadcast("<x/>") }.not_to raise_error

      # Verify the raw queue_name(...) computation agrees with the budget
      # — the fully-prefixed name must stay within MAX_QUEUE_NAME_LENGTH,
      # which is both pgbus's and (indirectly) pgmq-ruby's ceiling.
      full_queue_name = Pgbus.configuration.queue_name(short_name)
      expect(full_queue_name.length).to be <= Pgbus::QueueNameValidator::MAX_QUEUE_NAME_LENGTH
    end

    it "rejects the same name under a longer prefix" do
      original_prefix = Pgbus.configuration.queue_prefix
      name = "z" * (budget - 2) # fits under the default prefix

      # Grow the prefix relative to whatever the suite booted with so
      # the test deterministically triggers overflow even if the
      # baseline prefix ever changes. Appending any non-empty suffix
      # shrinks the budget and pushes `name` past it.
      Pgbus.configuration.queue_prefix = "#{original_prefix}_overflow"
      Pgbus.instance_variable_set(:@stream_cache, Concurrent::Map.new)

      expect { Pgbus.stream(name) }
        .to raise_error(Pgbus::Streams::StreamNameTooLong)
    ensure
      Pgbus.configuration.queue_prefix = original_prefix
      Pgbus.instance_variable_set(:@stream_cache, Concurrent::Map.new)
    end
  end

  describe "stream_key helper boundary at exactly the budget" do
    it "accepts a key that is exactly queue_name_budget characters" do
      at_budget = "p" * budget
      expect { Pgbus.stream_key(at_budget) }.not_to raise_error
      expect(Pgbus.stream(at_budget)).to be_a(Pgbus::Streams::Stream)
    end

    it "rejects a key that is one character over the budget" do
      over = "p" * (budget + 1)
      expect { Pgbus.stream_key(over) }
        .to raise_error(ArgumentError, /exceeds pgbus budget/)
    end
  end
end
