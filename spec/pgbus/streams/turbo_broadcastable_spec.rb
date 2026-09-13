# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Streams::TurboBroadcastable do
  # Fake Turbo::StreamsChannel with the same broadcast_stream_to signature
  # as the real turbo-rails version. We do not load turbo-rails in unit
  # tests — the patch is exercised via a minimal stand-in, and the
  # integration test in Phase 5 uses the real gem.
  let(:fake_turbo_module) do
    Module.new do
      def self.name
        "Turbo::StreamsChannel"
      end

      class << self
        # rubocop:disable RSpec/InstanceVariable -- this is inside an anonymous
        # Module.new, not the example group's context
        def broadcast_stream_to(*streamables, content:)
          @broadcasts ||= []
          @broadcasts << { streamables: streamables, content: content }
          :action_cable_called
        end
        # rubocop:enable RSpec/InstanceVariable

        def broadcast_replace_to(*streamables, **opts)
          broadcast_action_to(*streamables, action: :replace, **opts)
        end

        def broadcast_refresh_to(*streamables, **attributes)
          broadcast_stream_to(*streamables, content: "<turbo-stream action='refresh' #{attributes.keys.join}/>")
        end

        def broadcast_render_to(*streamables, **rendering)
          broadcast_stream_to(*streamables, content: "<turbo-stream #{rendering.keys.join}/>")
        end

        def broadcast_action_to(*streamables, action:, target: nil, targets: nil, **)
          resolved = convert_to_turbo_stream_dom_id(target) ||
                     convert_to_turbo_stream_dom_id(targets, include_selector: true)
          broadcast_stream_to(
            *streamables,
            content: "<turbo-stream action='#{action}' target='#{resolved}'/>"
          )
        end

        # Mirrors Turbo::Streams::ActionHelper#convert_to_turbo_stream_dom_id
        def convert_to_turbo_stream_dom_id(target, include_selector: false)
          target_array = target.is_a?(Array) ? target : [target].compact
          return target unless target_array.any? { |v| v.respond_to?(:to_key) || v.is_a?(Class) }

          dom_id = target_array.map { |v| fake_dom_id(v) }.join("_")
          include_selector ? "##{dom_id}" : dom_id
        end

        def fake_dom_id(value)
          value.respond_to?(:to_key) ? "#{value.class.name.downcase}_#{value.to_key.first}" : value.to_s
        end

        def stream_name_from(streamables)
          # Mirror Turbo::Streams::StreamName#stream_name_from
          return streamables.map { |s| stream_name_from([s]) }.join(":") if streamables.length != 1

          s = streamables.first
          if s.is_a?(Array)
            s.map { |x| stream_name_from([x]) }.join(":")
          elsif s.respond_to?(:to_gid_param)
            s.to_gid_param
          else
            s.to_s
          end
        end

        attr_reader :broadcasts

        def reset_broadcasts!
          @broadcasts = []
        end
      end
    end
  end

  before do
    stub_const("Turbo", Module.new)
    stub_const("Turbo::StreamsChannel", fake_turbo_module)
    fake_turbo_module.reset_broadcasts!
  end

  describe ".install_turbo_broadcastable_patch!" do
    it "prepends the patch onto Turbo::StreamsChannel's singleton class" do
      Pgbus::Streams.install_turbo_broadcastable_patch!
      expect(Turbo::StreamsChannel.singleton_class.include?(described_class)).to be true
    end

    it "is idempotent — calling twice does not double-prepend" do
      Pgbus::Streams.install_turbo_broadcastable_patch!
      Pgbus::Streams.install_turbo_broadcastable_patch!
      expect(
        Turbo::StreamsChannel.singleton_class.ancestors.count(described_class)
      ).to eq(1)
    end

    it "is a no-op when Turbo::StreamsChannel is not defined" do
      hide_const("Turbo::StreamsChannel")
      expect { Pgbus::Streams.install_turbo_broadcastable_patch! }.not_to raise_error
    end
  end

  describe "patched broadcast_stream_to" do
    before do
      Pgbus::Streams.install_turbo_broadcastable_patch!
      allow(Pgbus).to receive(:stream).and_return(
        instance_double(Pgbus::Streams::Stream, broadcast: 1248)
      )
    end

    it "routes the broadcast through Pgbus.stream(...).broadcast instead of ActionCable" do
      Turbo::StreamsChannel.broadcast_stream_to("room:42", content: "<turbo-stream>X</turbo-stream>")

      expect(Pgbus).to have_received(:stream).with("room:42", durable: false)
      expect(fake_turbo_module.broadcasts).to be_empty
    end

    it "derives the stream name from a GlobalID-like streamable" do
      account = double("Account", to_gid_param: "gid://app/Account/42")
      Turbo::StreamsChannel.broadcast_stream_to(account, content: "<turbo-stream/>")

      expect(Pgbus).to have_received(:stream).with("gid://app/Account/42", durable: false)
    end

    it "joins multiple streamables with colons (turbo-rails parity)" do
      account = double("Account", to_gid_param: "gid://app/Account/42")
      Turbo::StreamsChannel.broadcast_stream_to(account, :messages, content: "<turbo-stream/>")

      expect(Pgbus).to have_received(:stream).with("gid://app/Account/42:messages", durable: false)
    end

    it "uses durable mode when configured" do
      allow(Pgbus.configuration).to receive(:streams_default_broadcast_mode).and_return(:durable)
      Turbo::StreamsChannel.broadcast_stream_to("room:42", content: "<turbo-stream/>")

      expect(Pgbus).to have_received(:stream).with("room:42", durable: true)
    end

    it "routes durable when the stream name matches streams_durable_patterns" do
      allow(Pgbus.configuration).to receive_messages(
        streams_default_broadcast_mode: :ephemeral,
        streams_durable_patterns: [/\Areps_workout:/]
      )
      Turbo::StreamsChannel.broadcast_stream_to("reps_workout:42", content: "<turbo-stream/>")

      expect(Pgbus).to have_received(:stream).with("reps_workout:42", durable: true)
    end

    it "stays ephemeral when the stream name does not match streams_durable_patterns" do
      allow(Pgbus.configuration).to receive_messages(
        streams_default_broadcast_mode: :ephemeral,
        streams_durable_patterns: [/\Areps_workout:/]
      )
      Turbo::StreamsChannel.broadcast_stream_to("room:42", content: "<turbo-stream/>")

      expect(Pgbus).to have_received(:stream).with("room:42", durable: false)
    end

    it "thread-local override wins over a matching durable pattern" do
      allow(Pgbus.configuration).to receive(:streams_durable_patterns).and_return([/\Areps_workout:/])
      Thread.current[:pgbus_broadcast_durable] = false
      Turbo::StreamsChannel.broadcast_stream_to("reps_workout:42", content: "<turbo-stream/>")

      expect(Pgbus).to have_received(:stream).with("reps_workout:42", durable: false)
    ensure
      Thread.current[:pgbus_broadcast_durable] = nil
    end

    it "uses thread-local durable override when set" do
      Thread.current[:pgbus_broadcast_durable] = true
      Turbo::StreamsChannel.broadcast_stream_to("room:42", content: "<turbo-stream/>")

      expect(Pgbus).to have_received(:stream).with("room:42", durable: true)
    ensure
      Thread.current[:pgbus_broadcast_durable] = nil
    end

    it "thread-local false overrides durable config" do
      allow(Pgbus.configuration).to receive(:streams_default_broadcast_mode).and_return(:durable)
      Thread.current[:pgbus_broadcast_durable] = false
      Turbo::StreamsChannel.broadcast_stream_to("room:42", content: "<turbo-stream/>")

      expect(Pgbus).to have_received(:stream).with("room:42", durable: false)
    ensure
      Thread.current[:pgbus_broadcast_durable] = nil
    end

    it "falls back to config when thread-local is nil" do
      Thread.current[:pgbus_broadcast_durable] = nil
      Turbo::StreamsChannel.broadcast_stream_to("room:42", content: "<turbo-stream/>")

      expect(Pgbus).to have_received(:stream).with("room:42", durable: false)
    end
  end

  describe "coalesce: on the Turbo::StreamsChannel path (issue #465)" do
    let(:stream) { instance_double(Pgbus::Streams::Stream, broadcast: nil) }

    before do
      Pgbus::Streams.install_turbo_broadcastable_patch!
      allow(Pgbus).to receive(:stream).and_return(stream)
    end

    after do
      Thread.current[:pgbus_broadcast_coalesce] = nil
      Thread.current[:pgbus_broadcast_coalesce_target] = nil
    end

    it "forwards coalesce: and the resolved target from a direct channel call" do
      Turbo::StreamsChannel.broadcast_replace_to("room:42", target: "count-badge", coalesce: 50)

      expect(stream).to have_received(:broadcast)
        .with(anything, hash_including(coalesce: 50, target: "count-badge"))
    end

    it "accepts coalesce: true (default window resolved downstream)" do
      Turbo::StreamsChannel.broadcast_replace_to("room:42", target: "count-badge", coalesce: true)

      expect(stream).to have_received(:broadcast)
        .with(anything, hash_including(coalesce: true, target: "count-badge"))
    end

    it "does not leak coalesce: into turbo's rendering kwargs" do
      expect do
        Turbo::StreamsChannel.broadcast_replace_to("room:42", target: "t", coalesce: 50)
      end.not_to raise_error
    end

    it "keys on the dom_id turbo will render, not the raw record" do
      record_class = Class.new do
        def to_key = [7]
      end
      stub_const("Order", record_class)

      Turbo::StreamsChannel.broadcast_replace_to("room:42", target: Order.new, coalesce: 50)

      expect(stream).to have_received(:broadcast)
        .with(anything, hash_including(coalesce: 50, target: "order_7"))
    end

    it "falls back to targets: when target: is absent" do
      Turbo::StreamsChannel.broadcast_action_to(
        "room:42", action: :replace, targets: ".row", coalesce: 50
      )

      expect(stream).to have_received(:broadcast)
        .with(anything, hash_including(coalesce: 50, target: ".row"))
    end

    it "honours a thread-local coalesce set by an outer wrapper" do
      Pgbus::Streams::BroadcastOpts.with(coalesce: 25) do
        Turbo::StreamsChannel.broadcast_replace_to("room:42", target: "count-badge")
      end

      expect(stream).to have_received(:broadcast)
        .with(anything, hash_including(coalesce: 25, target: "count-badge"))
    end

    it "passes coalesce: nil and target: nil when coalescing is not requested" do
      Turbo::StreamsChannel.broadcast_replace_to("room:42", target: "count-badge")

      expect(stream).to have_received(:broadcast)
        .with(anything, hash_including(coalesce: nil, target: nil))
    end

    it "restores the thread-locals after the broadcast" do
      Turbo::StreamsChannel.broadcast_replace_to("room:42", target: "t", coalesce: 50)

      expect(Thread.current[:pgbus_broadcast_coalesce]).to be_nil
      expect(Thread.current[:pgbus_broadcast_coalesce_target]).to be_nil
    end

    it "restores the thread-locals even when the broadcast raises" do
      allow(stream).to receive(:broadcast).and_raise(RuntimeError, "boom")

      expect do
        Turbo::StreamsChannel.broadcast_replace_to("room:42", target: "t", coalesce: 50)
      end.to raise_error(RuntimeError, "boom")

      expect(Thread.current[:pgbus_broadcast_coalesce]).to be_nil
      expect(Thread.current[:pgbus_broadcast_coalesce_target]).to be_nil
    end

    it "extracts pgbus opts from a direct broadcast_refresh_to (no target to coalesce on)" do
      Turbo::StreamsChannel.broadcast_refresh_to("room:42", durable: true)

      expect(Pgbus).to have_received(:stream).with("room:42", durable: true)
      expect(fake_turbo_module.broadcasts).to be_empty
    end

    it "extracts pgbus opts from a direct broadcast_render_to" do
      Turbo::StreamsChannel.broadcast_render_to("room:42", exclude: "conn-1")

      expect(stream).to have_received(:broadcast)
        .with(anything, hash_including(exclude: "conn-1", coalesce: nil, target: nil))
    end

    it "extracts the other pgbus opts from a direct channel call too" do
      Turbo::StreamsChannel.broadcast_replace_to(
        "room:42", target: "t", durable: true, exclude: "conn-1", visible_to: :admins, event: "reactive"
      )

      expect(Pgbus).to have_received(:stream).with("room:42", durable: true)
      expect(stream).to have_received(:broadcast)
        .with(anything, hash_including(exclude: "conn-1", visible_to: :admins, event: "reactive"))
    end
  end
end
