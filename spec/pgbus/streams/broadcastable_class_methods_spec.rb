# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Streams::BroadcastableOverride::ClassMethods do
  let(:fake_stream) { instance_double(Pgbus::Streams::Stream, broadcast: 1248) }

  let(:fake_turbo_channel) do
    Module.new do
      def self.name
        "Turbo::StreamsChannel"
      end

      class << self
        def broadcast_replace_to(*streamables, **opts)
          broadcast_action_to(*streamables, action: :replace, **opts)
        end

        def broadcast_append_to(*streamables, **opts)
          broadcast_action_to(*streamables, action: :append, **opts)
        end

        def broadcast_remove_to(*streamables, **opts)
          broadcast_action_to(*streamables, action: :remove, render: false, **opts)
        end

        def broadcast_replace_later_to(*streamables, **opts)
          broadcast_action_later_to(*streamables, action: :replace, **opts)
        end

        def broadcast_action_later_to(*streamables, action:, **rendering)
          broadcast_action_to(*streamables, action: action, **rendering)
        end

        def broadcast_action_to(*streamables, action:, **)
          broadcast_stream_to(*streamables, content: "<turbo-stream action='#{action}'/>")
        end

        def broadcast_refresh_to(*streamables, **)
          broadcast_stream_to(*streamables, content: "<turbo-stream action='refresh'/>")
        end

        def broadcast_refresh_later_to(*streamables, **)
          broadcast_stream_to(*streamables, content: "<turbo-stream action='refresh'/>")
        end

        def broadcast_stream_to(*streamables, content:)
          # patched by TurboBroadcastable
        end

        def stream_name_from(streamables)
          streamables.join(":")
        end

        def reset!; end
      end
    end
  end

  before do
    stub_const("Turbo", Module.new) unless defined?(Turbo)
    stub_const("Turbo::StreamsChannel", fake_turbo_channel)

    allow(Pgbus).to receive(:stream).and_return(fake_stream)

    _trigger = Pgbus::Streams::TurboBroadcastable
    Pgbus::Streams.install_turbo_broadcastable_patch!
  end

  after do
    Thread.current[:pgbus_broadcast_durable] = nil
  end

  describe "broadcasts_to with durable: true" do
    let(:model_class) do
      # A spec double: built per example and driven from the example's thread.
      # rubocop:disable-next ThreadSafety/ClassInstanceVariable
      klass = Class.new do
        # Simulate ActiveRecord callback infrastructure
        class << self
          def after_create_commit(callback = nil, &block)
            @after_create_callbacks ||= []
            @after_create_callbacks << (callback || block)
          end

          def after_update_commit(callback = nil, &block)
            @after_update_callbacks ||= []
            @after_update_callbacks << (callback || block)
          end

          def after_destroy_commit(callback = nil, &block)
            @after_destroy_callbacks ||= []
            @after_destroy_callbacks << (callback || block)
          end

          def after_commit(callback = nil, &block)
            @after_commit_callbacks ||= []
            @after_commit_callbacks << (callback || block)
          end

          attr_reader :after_create_callbacks, :after_update_callbacks,
                      :after_destroy_callbacks, :after_commit_callbacks

          def broadcast_target_default
            "test_models"
          end

          def model_name
            Struct.new(:plural, :element).new("test_models", "test_model")
          end

          def name
            "TestModel"
          end

          def suppressed_turbo_broadcasts
            false
          end

          def suppressed_turbo_broadcasts?
            false
          end
        end

        def suppressed_turbo_broadcasts?
          false
        end

        def broadcast_target_default
          "test_models"
        end

        def to_partial_path
          "test_models/test_model"
        end

        def broadcast_replace_to(*streamables, **rendering)
          Turbo::StreamsChannel.broadcast_replace_to(*streamables, **rendering)
        end

        def broadcast_replace_later_to(*streamables, **rendering)
          Turbo::StreamsChannel.broadcast_replace_later_to(*streamables, **rendering)
        end

        def broadcast_remove_to(*streamables, **rendering)
          Turbo::StreamsChannel.broadcast_remove_to(*streamables, **rendering)
        end

        def broadcast_action_to(*streamables, action:, **rendering)
          Turbo::StreamsChannel.broadcast_action_to(*streamables, action: action, **rendering)
        end

        def broadcast_action_later_to(*streamables, action:, **rendering)
          Turbo::StreamsChannel.broadcast_action_later_to(*streamables, action: action, **rendering)
        end

        def broadcast_refresh_to(*streamables, **attributes)
          Turbo::StreamsChannel.broadcast_refresh_to(*streamables, **attributes)
        end

        def broadcast_refresh_later_to(*streamables, **attributes)
          Turbo::StreamsChannel.broadcast_refresh_later_to(*streamables, **attributes)
        end

        def account
          "account:42"
        end
      end

      stub_const("Turbo::Broadcastable", Module.new do
        def self.included(_base); end
      end)

      # Include the override which adds class methods
      klass.extend(described_class)
      Pgbus::Streams::BroadcastableOverride.install!(klass)
      klass
    end

    it "stores the durable setting from broadcasts_to" do
      model_class.broadcasts_to(:account, durable: true)

      expect(model_class.pgbus_durable_streams).to include(:account)
    end

    it "applies durable: true when create callback fires" do
      model_class.broadcasts_to(:account, durable: true)
      instance = model_class.new

      # Simulate the create callback
      callback = model_class.after_create_callbacks.last
      instance.instance_exec(&callback)

      expect(Pgbus).to have_received(:stream).with("account:42", durable: true)
    end

    it "applies durable: true when update callback fires" do
      model_class.broadcasts_to(:account, durable: true)
      instance = model_class.new

      callback = model_class.after_update_callbacks.last
      instance.instance_exec(&callback)

      expect(Pgbus).to have_received(:stream).with("account:42", durable: true)
    end

    it "applies durable: true when destroy callback fires" do
      model_class.broadcasts_to(:account, durable: true)
      instance = model_class.new

      callback = model_class.after_destroy_callbacks.last
      instance.instance_exec(&callback)

      expect(Pgbus).to have_received(:stream).with("account:42", durable: true)
    end

    it "does not set durable when broadcasts_to omits durable:" do
      allow(Pgbus.configuration).to receive(:streams_default_broadcast_mode).and_return(:ephemeral)
      model_class.broadcasts_to(:account)
      instance = model_class.new

      callback = model_class.after_create_callbacks.last
      instance.instance_exec(&callback)

      expect(Pgbus).to have_received(:stream).with("account:42", durable: false)
    end
  end

  describe "broadcasts_refreshes_to with durable: true" do
    let(:model_class) do
      # A spec double: built per example and driven from the example's thread.
      # rubocop:disable-next ThreadSafety/ClassInstanceVariable
      klass = Class.new do
        class << self
          def after_commit(callback = nil, &block)
            @after_commit_callbacks ||= []
            @after_commit_callbacks << (callback || block)
          end

          attr_reader :after_commit_callbacks

          def name
            "TestModel"
          end

          def model_name
            Struct.new(:plural, :element).new("test_models", "test_model")
          end

          def suppressed_turbo_broadcasts
            false
          end

          def suppressed_turbo_broadcasts?
            false
          end
        end

        def suppressed_turbo_broadcasts?
          false
        end

        def broadcast_refresh_to(*streamables, **attributes)
          Turbo::StreamsChannel.broadcast_refresh_to(*streamables, **attributes)
        end

        def broadcast_refresh_later_to(*streamables, **attributes)
          Turbo::StreamsChannel.broadcast_refresh_later_to(*streamables, **attributes)
        end

        def board
          "board:99"
        end
      end

      stub_const("Turbo::Broadcastable", Module.new do
        def self.included(_base); end
      end)

      klass.extend(described_class)
      Pgbus::Streams::BroadcastableOverride.install!(klass)
      klass
    end

    it "applies durable: true for broadcasts_refreshes_to" do
      model_class.broadcasts_refreshes_to(:board, durable: true)
      instance = model_class.new

      callback = model_class.after_commit_callbacks.last
      instance.instance_exec(&callback)

      expect(Pgbus).to have_received(:stream).with("board:99", durable: true)
    end
  end
end
