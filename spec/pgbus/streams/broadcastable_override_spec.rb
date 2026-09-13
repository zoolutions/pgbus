# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Streams::BroadcastableOverride do
  let(:fake_stream) { instance_double(Pgbus::Streams::Stream, broadcast: 1248) }

  let(:broadcastable_module) do
    Module.new do
      def self.name
        "Turbo::Broadcastable"
      end

      def broadcast_replace_to(*streamables, **rendering)
        Turbo::StreamsChannel.broadcast_replace_to(*streamables, **rendering)
      end

      def broadcast_append_to(*streamables, **rendering)
        Turbo::StreamsChannel.broadcast_append_to(*streamables, **rendering)
      end

      def broadcast_prepend_to(*streamables, **rendering)
        Turbo::StreamsChannel.broadcast_prepend_to(*streamables, **rendering)
      end

      def broadcast_update_to(*streamables, **rendering)
        Turbo::StreamsChannel.broadcast_update_to(*streamables, **rendering)
      end

      def broadcast_remove_to(*streamables, **rendering)
        Turbo::StreamsChannel.broadcast_remove_to(*streamables, **rendering)
      end

      def broadcast_action_to(*streamables, action:, **rendering)
        Turbo::StreamsChannel.broadcast_action_to(*streamables, action: action, **rendering)
      end

      def broadcast_refresh_to(*streamables, **attributes)
        Turbo::StreamsChannel.broadcast_refresh_to(*streamables, **attributes)
      end

      def broadcast_after_to(*streamables, **rendering)
        Turbo::StreamsChannel.broadcast_after_to(*streamables, **rendering)
      end

      def broadcast_before_to(*streamables, **rendering)
        Turbo::StreamsChannel.broadcast_before_to(*streamables, **rendering)
      end

      def broadcast_render_to(*streamables, **rendering)
        Turbo::StreamsChannel.broadcast_render_to(*streamables, **rendering)
      end

      def suppressed_turbo_broadcasts?
        false
      end
    end
  end

  let(:fake_turbo_channel) do
    Module.new do
      def self.name
        "Turbo::StreamsChannel"
      end

      class << self
        attr_reader :last_call

        def broadcast_replace_to(*streamables, **opts)
          broadcast_action_to(*streamables, action: :replace, **opts)
        end

        def broadcast_append_to(*streamables, **opts)
          broadcast_action_to(*streamables, action: :append, **opts)
        end

        def broadcast_prepend_to(*streamables, **opts)
          broadcast_action_to(*streamables, action: :prepend, **opts)
        end

        def broadcast_update_to(*streamables, **opts)
          broadcast_action_to(*streamables, action: :update, **opts)
        end

        def broadcast_remove_to(*streamables, **opts)
          broadcast_action_to(*streamables, action: :remove, render: false, **opts)
        end

        def broadcast_after_to(*streamables, **)
          broadcast_stream_to(*streamables, content: "<turbo-stream action='after'/>")
        end

        def broadcast_before_to(*streamables, **)
          broadcast_stream_to(*streamables, content: "<turbo-stream action='before'/>")
        end

        def broadcast_refresh_to(*streamables, **)
          broadcast_stream_to(*streamables, content: "<turbo-stream action='refresh'/>")
        end

        def broadcast_action_to(*streamables, action:, target: nil, targets: nil, **)
          broadcast_stream_to(
            *streamables,
            content: "<turbo-stream action='#{action}' " \
                     "target='#{convert_to_turbo_stream_dom_id(target) || convert_to_turbo_stream_dom_id(targets)}'/>"
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

        def broadcast_render_to(*streamables, **)
          broadcast_stream_to(*streamables, content: "<turbo-stream/>")
        end

        def broadcast_stream_to(*streamables, content:)
          @last_call = { streamables: streamables, content: content }
        end

        def stream_name_from(streamables)
          streamables.join(":")
        end

        def reset!
          @last_call = nil
        end
      end
    end
  end

  let(:model_class) do
    bm = broadcastable_module
    Class.new do
      include bm

      def self.name
        "TestModel"
      end

      def self.model_name
        Struct.new(:plural, :element).new("test_models", "test_model")
      end
    end
  end

  let(:model) { model_class.new }

  before do
    stub_const("Turbo", Module.new) unless defined?(Turbo)
    stub_const("Turbo::StreamsChannel", fake_turbo_channel)
    stub_const("Turbo::Broadcastable", broadcastable_module)
    fake_turbo_channel.reset!

    allow(Pgbus).to receive(:stream).and_return(fake_stream)

    _trigger = Pgbus::Streams::TurboBroadcastable

    Pgbus::Streams.install_turbo_broadcastable_patch!
    described_class.install!(broadcastable_module)
  end

  after do
    Thread.current[:pgbus_broadcast_durable] = nil
    Thread.current[:pgbus_broadcast_exclude] = nil
    Thread.current[:pgbus_broadcast_visible_to] = nil
    Thread.current[:pgbus_broadcast_event] = nil
    Thread.current[:pgbus_broadcast_coalesce] = nil
    Thread.current[:pgbus_broadcast_coalesce_target] = nil
  end

  describe "instance-level durable: kwarg" do
    it "forwards durable: true to Pgbus.stream for broadcast_replace_to" do
      model.broadcast_replace_to("room:42", durable: true, html: "<div/>")

      expect(Pgbus).to have_received(:stream).with("room:42", durable: true)
    end

    it "forwards durable: false to Pgbus.stream for broadcast_replace_to" do
      model.broadcast_replace_to("room:42", durable: false, html: "<div/>")

      expect(Pgbus).to have_received(:stream).with("room:42", durable: false)
    end

    it "falls back to config mode when durable: is omitted" do
      allow(Pgbus.configuration).to receive(:streams_default_broadcast_mode).and_return(:ephemeral)
      model.broadcast_replace_to("room:42", html: "<div/>")

      expect(Pgbus).to have_received(:stream).with("room:42", durable: false)
    end

    it "forwards durable: for broadcast_append_to" do
      model.broadcast_append_to("room:42", durable: true, html: "<div/>")

      expect(Pgbus).to have_received(:stream).with("room:42", durable: true)
    end

    it "forwards durable: for broadcast_prepend_to" do
      model.broadcast_prepend_to("room:42", durable: true, html: "<div/>")

      expect(Pgbus).to have_received(:stream).with("room:42", durable: true)
    end

    it "forwards durable: for broadcast_update_to" do
      model.broadcast_update_to("room:42", durable: true, html: "<div/>")

      expect(Pgbus).to have_received(:stream).with("room:42", durable: true)
    end

    it "forwards durable: for broadcast_remove_to" do
      model.broadcast_remove_to("room:42", durable: true)

      expect(Pgbus).to have_received(:stream).with("room:42", durable: true)
    end

    it "forwards durable: for broadcast_action_to" do
      model.broadcast_action_to("room:42", action: :replace, durable: true, html: "<div/>")

      expect(Pgbus).to have_received(:stream).with("room:42", durable: true)
    end

    it "forwards durable: for broadcast_refresh_to" do
      model.broadcast_refresh_to("room:42", durable: true)

      expect(Pgbus).to have_received(:stream).with("room:42", durable: true)
    end

    it "forwards durable: for broadcast_after_to" do
      model.broadcast_after_to("room:42", durable: true, target: "item_5")

      expect(Pgbus).to have_received(:stream).with("room:42", durable: true)
    end

    it "forwards durable: for broadcast_before_to" do
      model.broadcast_before_to("room:42", durable: true, target: "item_5")

      expect(Pgbus).to have_received(:stream).with("room:42", durable: true)
    end

    it "forwards durable: for broadcast_render_to" do
      model.broadcast_render_to("room:42", durable: true, html: "<div/>")

      expect(Pgbus).to have_received(:stream).with("room:42", durable: true)
    end

    it "cleans up the thread-local after the broadcast completes" do
      model.broadcast_replace_to("room:42", durable: true, html: "<div/>")

      expect(Thread.current[:pgbus_broadcast_durable]).to be_nil
    end

    it "cleans up the thread-local even if an error occurs" do
      allow(fake_stream).to receive(:broadcast).and_raise(RuntimeError, "boom")

      expect do
        model.broadcast_replace_to("room:42", durable: true, html: "<div/>")
      end.to raise_error(RuntimeError, "boom")

      expect(Thread.current[:pgbus_broadcast_durable]).to be_nil
    end
  end

  describe "instance-level exclude: / visible_to: / event: kwargs" do
    it "forwards exclude: to Stream#broadcast (actor-echo suppression)" do
      model.broadcast_replace_to("room:42", exclude: "conn-abc", html: "<div/>")

      expect(fake_stream).to have_received(:broadcast)
        .with(anything, hash_including(exclude: "conn-abc"))
    end

    it "forwards visible_to: to Stream#broadcast" do
      model.broadcast_replace_to("room:42", visible_to: :admins, html: "<div/>")

      expect(fake_stream).to have_received(:broadcast)
        .with(anything, hash_including(visible_to: :admins))
    end

    it "forwards event: to Stream#broadcast" do
      model.broadcast_replace_to("room:42", event: "presence", html: "<div/>")

      expect(fake_stream).to have_received(:broadcast)
        .with(anything, hash_including(event: "presence"))
    end

    it "composes exclude: with durable: (both reach their destinations)" do
      model.broadcast_append_to("room:42", durable: true, exclude: "conn-9", html: "<div/>")

      expect(Pgbus).to have_received(:stream).with("room:42", durable: true)
      expect(fake_stream).to have_received(:broadcast)
        .with(anything, hash_including(exclude: "conn-9"))
    end

    it "passes nil for the opts when none are given (unchanged default path)" do
      model.broadcast_replace_to("room:42", html: "<div/>")

      expect(fake_stream).to have_received(:broadcast)
        .with(anything, hash_including(exclude: nil, visible_to: nil, event: nil))
    end

    it "does NOT leak exclude: into turbo-rails rendering kwargs" do
      # The override must delete :exclude from kwargs before calling super,
      # so it never reaches Turbo's renderer (which would error on it).
      expect do
        model.broadcast_replace_to("room:42", exclude: "conn-abc", html: "<div/>")
      end.not_to raise_error
    end

    it "cleans up the exclude/visible_to/event thread-locals after the broadcast" do
      model.broadcast_replace_to("room:42", exclude: "conn-abc", visible_to: :admins, event: "x", html: "<div/>")

      expect(Thread.current[:pgbus_broadcast_exclude]).to be_nil
      expect(Thread.current[:pgbus_broadcast_visible_to]).to be_nil
      expect(Thread.current[:pgbus_broadcast_event]).to be_nil
    end
  end

  describe "instance-level coalesce: kwarg (issue #465)" do
    it "forwards coalesce: and the target to Stream#broadcast" do
      model.broadcast_replace_to("room:42", target: "count-badge", coalesce: 50, html: "<div/>")

      expect(fake_stream).to have_received(:broadcast)
        .with(anything, hash_including(coalesce: 50, target: "count-badge"))
    end

    it "forwards coalesce: true" do
      model.broadcast_replace_to("room:42", target: "count-badge", coalesce: true, html: "<div/>")

      expect(fake_stream).to have_received(:broadcast)
        .with(anything, hash_including(coalesce: true, target: "count-badge"))
    end

    it "forwards coalesce: for broadcast_append_to" do
      model.broadcast_append_to("room:42", target: "list", coalesce: 50, html: "<div/>")

      expect(fake_stream).to have_received(:broadcast)
        .with(anything, hash_including(coalesce: 50, target: "list"))
    end

    it "forwards coalesce: for broadcast_action_to" do
      model.broadcast_action_to("room:42", action: :update, target: "list", coalesce: 50, html: "<div/>")

      expect(fake_stream).to have_received(:broadcast)
        .with(anything, hash_including(coalesce: 50, target: "list"))
    end

    it "composes coalesce: with durable: and exclude:" do
      model.broadcast_replace_to(
        "room:42", target: "count-badge", coalesce: 50, durable: true, exclude: "conn-9", html: "<div/>"
      )

      expect(Pgbus).to have_received(:stream).with("room:42", durable: true)
      expect(fake_stream).to have_received(:broadcast)
        .with(anything, hash_including(coalesce: 50, target: "count-badge", exclude: "conn-9"))
    end

    it "does NOT leak coalesce: into turbo-rails rendering kwargs" do
      expect do
        model.broadcast_replace_to("room:42", target: "t", coalesce: 50, html: "<div/>")
      end.not_to raise_error
    end

    it "coalesces a broadcast wrapped in with_pgbus_broadcast_opts(coalesce:)" do
      model.send(:with_pgbus_broadcast_opts, coalesce: 50) do
        model.broadcast_replace_to("room:42", target: "count-badge", html: "<div/>")
      end

      expect(fake_stream).to have_received(:broadcast)
        .with(anything, hash_including(coalesce: 50, target: "count-badge"))
    end

    it "passes coalesce: nil and target: nil when coalesce: is absent" do
      model.broadcast_replace_to("room:42", target: "count-badge", html: "<div/>")

      expect(fake_stream).to have_received(:broadcast)
        .with(anything, hash_including(coalesce: nil, target: nil))
    end

    it "cleans up the coalesce thread-locals after the broadcast" do
      model.broadcast_replace_to("room:42", target: "t", coalesce: 50, html: "<div/>")

      expect(Thread.current[:pgbus_broadcast_coalesce]).to be_nil
      expect(Thread.current[:pgbus_broadcast_coalesce_target]).to be_nil
    end
  end

  describe ".install!" do
    it "is idempotent" do
      described_class.install!(broadcastable_module)
      described_class.install!(broadcastable_module)

      count = broadcastable_module.ancestors.count(described_class)
      expect(count).to eq(1)
    end
  end
end
