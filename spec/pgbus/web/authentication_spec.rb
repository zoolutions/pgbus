# frozen_string_literal: true

require "spec_helper"

require "active_support"
require_relative "../../../lib/pgbus/web/authentication"

RSpec.describe Pgbus::Web::Authentication do
  let(:controller_class) do
    # A spec double: built per example and driven from the example's thread.
    # rubocop:disable-next ThreadSafety/ClassInstanceVariable
    Class.new do
      # Simulate before_action
      def self.before_action(method_name)
        @_before_actions ||= []
        @_before_actions << method_name
      end

      def self.before_actions
        @_before_actions || []
      end

      include Pgbus::Web::Authentication

      attr_accessor :request, :head_status

      def head(status)
        @head_status = status
      end
    end
  end

  let(:controller) { controller_class.new }
  let(:request) { double("request") }

  before { controller.request = request }

  describe "#authenticate_pgbus!" do
    context "when web_auth is nil" do
      before do
        Pgbus.configuration.web_auth = nil
        described_class.auth_warned = false
      end

      it "allows access" do
        controller.send(:authenticate_pgbus!)
        expect(controller.head_status).to be_nil
      end

      it "logs a warning once" do
        allow(Pgbus.logger).to receive(:warn).and_yield
        controller.send(:authenticate_pgbus!)
        controller.send(:authenticate_pgbus!)
        expect(Pgbus.logger).to have_received(:warn).once
      end

      it "logs the warning once when the first requests arrive at once", :first_call_race do
        allow(Pgbus.logger).to receive(:warn).and_yield
        hold_first_call(controller, :default_base_controller?)

        race { controller.send(:authenticate_pgbus!) }

        expect(Pgbus.logger).to have_received(:warn).once
      end
    end

    context "when web_auth is nil but a gating base_controller_class is set (issue #334)" do
      before do
        Pgbus.configuration.web_auth = nil
        described_class.auth_warned = false
        # A non-default base_controller_class signals the app deliberately chose
        # a controller that already gates access (e.g. an AdminController with
        # its own before_action), so the dashboard is NOT actually open.
        Pgbus.configuration.base_controller_class = "AdminController"
      end

      after { Pgbus.configuration.base_controller_class = "::ActionController::Base" }

      it "does not warn about an unauthenticated dashboard" do
        allow(Pgbus.logger).to receive(:warn).and_yield
        controller.send(:authenticate_pgbus!)
        expect(Pgbus.logger).not_to have_received(:warn)
      end

      it "still allows access (the gating is the host controller's job)" do
        controller.send(:authenticate_pgbus!)
        expect(controller.head_status).to be_nil
      end
    end

    context "when base_controller_class is the default in a different form (issue #334 review)" do
      before do
        Pgbus.configuration.web_auth = nil
        described_class.auth_warned = false
      end

      after { Pgbus.configuration.base_controller_class = "::ActionController::Base" }

      # "ActionController::Base" (no leading ::) and the ::-prefixed form are the
      # SAME default — the dashboard is still open, so the warning must fire.
      it "warns for the unprefixed default string" do
        Pgbus.configuration.base_controller_class = "ActionController::Base"
        allow(Pgbus.logger).to receive(:warn).and_yield

        controller.send(:authenticate_pgbus!)

        expect(Pgbus.logger).to have_received(:warn)
      end
    end

    context "when web_auth returns true" do
      before { Pgbus.configuration.web_auth = ->(_req) { true } }
      after { Pgbus.configuration.web_auth = nil }

      it "allows access" do
        controller.send(:authenticate_pgbus!)
        expect(controller.head_status).to be_nil
      end
    end

    context "when web_auth returns false" do
      before { Pgbus.configuration.web_auth = ->(_req) { false } }
      after { Pgbus.configuration.web_auth = nil }

      it "returns unauthorized" do
        controller.send(:authenticate_pgbus!)
        expect(controller.head_status).to eq(:unauthorized)
      end
    end
  end

  it "registers before_action" do
    expect(controller_class.before_actions).to include(:authenticate_pgbus!)
  end
end
