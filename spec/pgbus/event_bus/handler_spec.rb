# frozen_string_literal: true

require "spec_helper"
require "active_job"
require "json"

RSpec.describe Pgbus::EventBus::Handler do
  include PgmqDoubles

  let(:event_id) { SecureRandom.uuid }
  let(:published_at) { Time.now.utc.iso8601(6) }
  let(:raw_payload) { { "key" => "value" } }
  let(:raw_message) do
    {
      "event_id" => event_id,
      "payload" => raw_payload,
      "published_at" => published_at
    }.to_json
  end
  let(:message) { build_message_double(msg_id: 1, message: raw_message) }

  describe "concrete subclass that implements #handle" do
    let(:handler_class) do
      Class.new(described_class) do
        attr_reader :received_event

        def handle(event)
          @received_event = event
        end
      end
    end
    let(:handler) { handler_class.new }

    describe "#process" do
      it "parses JSON message, builds event, calls handle, and returns :handled" do
        result = handler.process(message)

        expect(result).to eq(:handled)
        expect(handler.received_event).to be_a(Pgbus::Event)
        expect(handler.received_event.event_id).to eq(event_id)
        expect(handler.received_event.payload).to eq(raw_payload)
      end

      it "parses published_at into a Time object" do
        handler.process(message)

        expect(handler.received_event.published_at).to be_a(Time)
      end

      it "handles nil published_at gracefully" do
        raw = { "event_id" => event_id, "payload" => raw_payload, "published_at" => nil }.to_json
        msg = build_message_double(msg_id: 2, message: raw)

        handler.process(msg)

        expect(handler.received_event.published_at).to be_a(Time)
      end
    end

    describe "Current attributes restore (issue #431)" do
      let(:current_class) { Class.new(ActiveSupport::CurrentAttributes) { attribute :tenant, :request_id } }
      let(:seen) { [] }
      let(:handler_class) do
        sink = seen
        Class.new(described_class) do
          attr_reader :received_event

          define_method(:handle) do |event|
            @received_event = event
            sink << [HandlerSpecCurrent.tenant, HandlerSpecCurrent.request_id]
          end
        end
      end
      let(:context) { { "HandlerSpecCurrent" => { "tenant" => "acme", "request_id" => "req-1" } } }
      let(:raw_message) do
        { "event_id" => event_id, "payload" => raw_payload, "published_at" => published_at,
          "pgbus_current" => context }.to_json
      end

      before { stub_const("HandlerSpecCurrent", current_class) }

      after { ActiveSupport::CurrentAttributes.clear_all }

      # The guarantee is scoped: inside handle the persisted context is set;
      # after process nothing leaks. What the OUTER value reverts to is
      # version-dependent when a Rails app is loaded (the executor wrap's
      # reset_all hook clears Current around the unit of work on 7.1, while
      # set's block-revert restores it when no executor is present), so the
      # spec pins only the no-leak contract.
      it "sets the persisted Current for the duration of handle without leaking it afterwards" do
        handler.process(message)

        expect(seen).to eq([%w[acme req-1]])
        expect(HandlerSpecCurrent.tenant).to be_nil
        expect(HandlerSpecCurrent.request_id).to be_nil
      end

      it "exposes the raw context on the event" do
        handler.process(message)

        expect(handler.received_event.context).to eq(context)
      end

      it "leaves Current alone for an envelope without pgbus_current" do
        plain = build_message_double(msg_id: 3, message: { "event_id" => event_id, "payload" => raw_payload }.to_json)

        handler.process(plain)

        expect(seen).to eq([[nil, nil]])
        expect(handler.received_event.context).to be_nil
      end

      it "rejects a GlobalID in the context that is not in allowed_global_id_models before handle runs" do
        stub_const("Secret", Class.new)
        stub_const("Order", Class.new)
        Pgbus.configuration.allowed_global_id_models = [Order]
        secret_context = { "HandlerSpecCurrent" => { "tenant" => { "_aj_globalid" => "gid://pgbus-test/Secret/1" } } }
        tagged = build_message_double(
          msg_id: 4,
          message: { "event_id" => event_id, "payload" => raw_payload, "pgbus_current" => secret_context }.to_json
        )

        expect { handler.process(tagged) }.to raise_error(Pgbus::SerializationError, /not in allowed_global_id_models/)
        expect(seen).to be_empty
      ensure
        Pgbus.configuration.allowed_global_id_models = nil
      end
    end

    describe "GlobalID payload resolution" do
      let(:resolved_object) { double("User", id: 42) }
      let(:gid_uri) { "gid://pgbus-test/User/42" }
      let(:global_id_payload) { { "_global_id" => gid_uri } }
      let(:raw_message) do
        {
          "event_id" => event_id,
          "payload" => global_id_payload,
          "published_at" => published_at
        }.to_json
      end

      before do
        allow(GlobalID::Locator).to receive(:locate).and_return(resolved_object)
      end

      it "resolves GlobalID payloads via GlobalID::Locator" do
        handler.process(message)

        expect(handler.received_event.payload).to eq(resolved_object)
        expect(GlobalID::Locator).to have_received(:locate)
      end
    end

    describe "instrumentation via ActiveSupport::Notifications" do
      before do
        stub_const("ActiveSupport::Notifications", double("AS::Notifications"))
        allow(ActiveSupport::Notifications).to receive(:instrument) do |_name, _payload, &block|
          block&.call
        end
      end

      it "instruments pgbus.event_processed after handling" do
        stub_const("TestHandler", handler_class)
        test_handler = TestHandler.new

        test_handler.process(message)

        expect(ActiveSupport::Notifications).to have_received(:instrument).with(
          "pgbus.event_processed",
          hash_including(event_id: event_id, handler: "TestHandler")
        )
      end
    end
  end

  describe ".idempotent! / .idempotent?" do
    it "defaults to not idempotent" do
      klass = Class.new(described_class)
      expect(klass.idempotent?).to be false
    end

    it "becomes idempotent after calling .idempotent!" do
      klass = Class.new(described_class) do
        idempotent!
      end
      expect(klass.idempotent?).to be true
    end

    it "does not affect sibling subclasses" do
      idempotent_klass = Class.new(described_class) { idempotent! }
      normal_klass = Class.new(described_class)

      expect(idempotent_klass.idempotent?).to be true
      expect(normal_klass.idempotent?).to be false
    end
  end

  describe "idempotent handler" do
    let(:handler_class) do
      Class.new(described_class) do
        idempotent!

        attr_reader :handled_events

        def handle(event)
          (@handled_events ||= []) << event
        end
      end
    end
    let(:handler) { handler_class.new }
    let(:insert_result) { double("InsertAll::Result", rows: [[1]]) }
    let(:empty_result) { double("InsertAll::Result", rows: []) }
    let(:relation) { double("ActiveRecord::Relation", update_all: 1) }
    let(:cache_key) { "#{event_id}:#{handler_class.name}" }

    before do
      allow(Pgbus::ProcessedEvent).to receive(:completion_column?).and_return(true)
      allow(Pgbus::ProcessedEvent).to receive(:where)
        .with(event_id: event_id, handler_class: handler_class.name)
        .and_return(relation)
    end

    context "with a fresh claim (insert wins)" do
      before { allow(Pgbus::ProcessedEvent).to receive(:insert).and_return(insert_result) }

      it "runs handle and returns :handled" do
        result = handler.process(message)

        expect(result).to eq(:handled)
        expect(handler.handled_events.size).to eq(1)
      end

      it "inserts a pending claim (no completed_at) with the unique constraint" do
        handler.process(message)

        expect(Pgbus::ProcessedEvent).to have_received(:insert).with(
          { event_id: event_id, handler_class: handler_class.name, processed_at: kind_of(Time) },
          unique_by: %i[event_id handler_class]
        )
      end

      it "completes the claim after handle returns" do
        handler.process(message)

        expect(relation).to have_received(:update_all).with(completed_at: kind_of(Time))
      end

      # The drop that motivated StaleConnectionRetry: the socket dies between
      # handle() returning and the stamp landing, so the host app paged for a
      # message whose handler had already succeeded.
      context "when the claim stamp hits a stale connection" do
        before do
          allow(Pgbus::EventBus::StaleConnectionRetry).to receive(:reconnect_leased!)
          attempts = 0
          allow(relation).to receive(:update_all) do
            attempts += 1
            raise ActiveRecord::ConnectionFailed, "PQconsumeInput() SSL error: unexpected eof while reading" if attempts == 1

            1
          end
        end

        it "reconnects, stamps the claim and still reports :handled" do
          expect(handler.process(message)).to eq(:handled)

          expect(relation).to have_received(:update_all).twice
          expect(Pgbus::EventBus::StaleConnectionRetry).to have_received(:reconnect_leased!).once
        end

        # The recovered stamp must not re-run the handler: retrying `process`
        # rather than the stamp alone would double every side effect.
        it "runs handle exactly once" do
          handler.process(message)

          expect(handler.handled_events.size).to eq(1)
        end
      end

      it "marks the dedup cache once the claim is completed" do
        handler.process(message)

        expect(handler_class.dedup_cache.seen?(cache_key)).to be true
      end
    end

    context "with a completed claim (row exists, completed_at set)" do
      before do
        allow(Pgbus::ProcessedEvent).to receive(:insert).and_return(empty_result)
        allow(relation).to receive(:pick).with(:completed_at).and_return(Time.now.utc)
      end

      it "returns :skipped without running handle" do
        result = handler.process(message)

        expect(result).to eq(:skipped)
        expect(handler.handled_events).to be_nil
      end

      it "never writes a completion for a skipped event" do
        handler.process(message)

        expect(relation).not_to have_received(:update_all)
      end

      it "marks the dedup cache so the next delivery skips the database" do
        handler.process(message)
        handler_class.new.process(message)

        expect(Pgbus::ProcessedEvent).to have_received(:insert).once
      end
    end

    context "with a pending claim (prior attempt crashed between claim and handle)" do
      before do
        allow(Pgbus::ProcessedEvent).to receive(:insert).and_return(empty_result)
        allow(relation).to receive(:pick).with(:completed_at).and_return(nil)
      end

      it "re-runs handle instead of skipping" do
        result = handler.process(message)

        expect(result).to eq(:handled)
        expect(handler.handled_events.size).to eq(1)
      end

      it "completes the claim after the re-run" do
        handler.process(message)

        expect(relation).to have_received(:update_all).with(completed_at: kind_of(Time))
      end
    end

    context "when handle raises" do
      let(:handler_class) do
        Class.new(described_class) do
          idempotent!

          def handle(_event)
            raise "boom"
          end
        end
      end

      before { allow(Pgbus::ProcessedEvent).to receive(:insert).and_return(insert_result) }

      it "leaves the claim pending so redelivery re-runs the handler" do
        expect { handler.process(message) }.to raise_error("boom")

        expect(relation).not_to have_received(:update_all)
      end

      it "does not mark the dedup cache for an incomplete execution" do
        expect { handler.process(message) }.to raise_error("boom")

        expect(handler_class.dedup_cache.seen?(cache_key)).to be false
      end
    end

    context "when schema detection fails after the claim insert (transient DB error)" do
      # The pending row is already inserted when completion_column? raises
      # (detection errors are deliberately not memoized) — the crash-safety
      # contract of issue #385 requires that this delivery fails loudly and
      # the pending claim re-runs on the next one.
      let(:detection_error) { Class.new(StandardError) }

      before do
        allow(Pgbus::ProcessedEvent).to receive(:insert).and_return(insert_result)
        allow(Pgbus::ProcessedEvent).to receive(:completion_column?)
          .and_raise(detection_error.new("connection dropped"))
      end

      it "propagates the error without running handle, leaving the message for VT redelivery" do
        expect { handler.process(message) }.to raise_error(detection_error)

        expect(handler.handled_events).to be_nil
      end

      it "does not mark the dedup cache for the failed delivery" do
        expect { handler.process(message) }.to raise_error(detection_error)

        expect(handler_class.dedup_cache.seen?(cache_key)).to be false
      end

      it "re-runs the still-pending claim once detection recovers on redelivery" do
        expect { handler.process(message) }.to raise_error(detection_error)

        allow(Pgbus::ProcessedEvent).to receive_messages(completion_column?: true, insert: empty_result) # row exists now
        allow(relation).to receive(:pick).with(:completed_at).and_return(nil) # still pending

        expect(handler.process(message)).to eq(:handled)
        expect(handler.handled_events.size).to eq(1)
      end
    end

    context "when the completed_at column has not been migrated yet (legacy fallback)" do
      before do
        allow(Pgbus::ProcessedEvent).to receive(:completion_column?).and_return(false)
      end

      it "returns :handled on insert win without any completion write" do
        allow(Pgbus::ProcessedEvent).to receive(:insert).and_return(insert_result)

        result = handler.process(message)

        expect(result).to eq(:handled)
        expect(Pgbus::ProcessedEvent).not_to have_received(:where)
      end

      it "returns :skipped when the row already exists (single-phase claim)" do
        allow(Pgbus::ProcessedEvent).to receive(:insert).and_return(empty_result)

        result = handler.process(message)

        expect(result).to eq(:skipped)
      end

      it "marks the dedup cache at claim time (legacy behavior)" do
        allow(Pgbus::ProcessedEvent).to receive(:insert).and_return(insert_result)

        handler.process(message)

        expect(handler_class.dedup_cache.seen?(cache_key)).to be true
      end
    end
  end

  describe "#handle (base class)" do
    it "raises NotImplementedError when not overridden" do
      handler = described_class.new

      event = Pgbus::Event.new(event_id: event_id, payload: raw_payload)
      expect { handler.handle(event) }.to raise_error(NotImplementedError, /must implement #handle/)
    end
  end

  describe "Rails executor wrapping" do
    let(:handler_class) do
      Class.new(described_class) do
        attr_reader :received_event

        def handle(event)
          @received_event = event
        end
      end
    end
    let(:handler) { handler_class.new }

    context "when Rails is loaded" do
      let(:mock_executor_wrapper) { double("executor") }
      let(:mock_reloader_wrapper) { double("reloader") }
      let(:mock_app_config) { double("config") }
      let(:mock_app) do
        double("Rails.application",
               executor: mock_executor_wrapper,
               reloader: mock_reloader_wrapper,
               config: mock_app_config)
      end

      def stub_rails!
        rails = double("Rails", application: mock_app)
        allow(rails).to receive(:respond_to?) { |name, *| name == :application }
        stub_const("Rails", rails)
      end

      before do
        allow(mock_executor_wrapper).to receive(:wrap).and_yield
        allow(mock_reloader_wrapper).to receive(:wrap).and_yield
      end

      context "when enable_reloading is true (development)" do
        before do
          allow(mock_app_config).to receive(:respond_to?).with(:enable_reloading).and_return(true)
          allow(mock_app_config).to receive(:enable_reloading).and_return(true)
          stub_rails!
        end

        it "wraps process in reloader.wrap so handlers see code changes" do
          handler.process(message)

          expect(mock_reloader_wrapper).to have_received(:wrap)
          expect(mock_executor_wrapper).not_to have_received(:wrap)
          expect(handler.received_event.event_id).to eq(event_id)
        end
      end

      context "when enable_reloading is false (production)" do
        before do
          allow(mock_app_config).to receive(:respond_to?).with(:enable_reloading).and_return(true)
          allow(mock_app_config).to receive(:enable_reloading).and_return(false)
          stub_rails!
        end

        it "wraps process in executor.wrap" do
          handler.process(message)

          expect(mock_executor_wrapper).to have_received(:wrap)
          expect(mock_reloader_wrapper).not_to have_received(:wrap)
        end
      end

      context "when config does not respond to enable_reloading (Rails < 7.1)" do
        before do
          allow(mock_app_config).to receive(:respond_to?).with(:enable_reloading).and_return(false)
          stub_rails!
        end

        it "falls back to !cache_classes — reloader.wrap when cache_classes is false" do
          allow(mock_app_config).to receive(:cache_classes).and_return(false)

          handler.process(message)

          expect(mock_reloader_wrapper).to have_received(:wrap)
        end

        it "falls back to !cache_classes — executor.wrap when cache_classes is true" do
          allow(mock_app_config).to receive(:cache_classes).and_return(true)

          handler.process(message)

          expect(mock_executor_wrapper).to have_received(:wrap)
        end
      end
    end

    context "when Rails is not loaded" do
      it "runs the handler without a wrapper (no-op)" do
        hide_const("Rails") if defined?(Rails)

        result = handler.process(message)

        expect(result).to eq(:handled)
        expect(handler.received_event.event_id).to eq(event_id)
      end
    end
  end
end
