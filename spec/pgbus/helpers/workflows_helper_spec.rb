# frozen_string_literal: true

require "spec_helper"
require "action_view"

require_relative "../../../app/helpers/pgbus/application_helper"
require_relative "../../../app/helpers/pgbus/button_helper"
require_relative "../../../app/helpers/pgbus/workflows_helper"

RSpec.describe Pgbus::WorkflowsHelper do
  let(:helper) do
    Class.new do
      include ActionView::Helpers::TagHelper
      include ActionView::Helpers::UrlHelper
      include ActionView::Helpers::OutputSafetyHelper
      include ActionView::Helpers::TranslationHelper
      include Pgbus::ApplicationHelper
      include Pgbus::ButtonHelper
      include Pgbus::WorkflowsHelper
    end.new
  end

  before(:all) do # rubocop:disable RSpec/BeforeAfterAll
    I18n.load_path |= Dir[File.expand_path("../../../config/locales/*.yml", __dir__)]
    I18n.backend.reload!
  end

  around { |example| I18n.with_locale(:en) { example.run } }

  after { Pgbus.configuration.workflow_subject_url = nil }

  describe "#pgbus_workflow_safe_url" do
    it "keeps http(s) URLs with a host and paths on this site" do
      expect(%w[https://shop.example/orders/1 HTTP://shop.example/o /admin/orders/1].map { |u| helper.pgbus_workflow_safe_url(u) })
        .to eq(%w[https://shop.example/orders/1 HTTP://shop.example/o /admin/orders/1])
    end

    it "drops everything that could leave the site or run script" do
      unsafe = ["javascript:alert(1)", " JavaScript:alert(1)", "data:text/html,x", "//evil.example/x", "/\\evil.example",
                "https:///nohost", "orders/1", "", nil, "http://exa mple.com"]

      expect(unsafe.map { |u| helper.pgbus_workflow_safe_url(u) }).to all(be_nil)
    end
  end

  describe "#pgbus_workflow_subject" do
    it "shows a subject-less run as a dash" do
      expect(helper.pgbus_workflow_subject({ subject_type: nil })).to eq("—")
    end

    it "is plain text without workflow_subject_url, and when the lambda raises" do
      expect(helper.pgbus_workflow_subject({ subject_type: "Order", subject_id: "7" })).to eq("Order 7")

      allow(Pgbus.logger).to receive(:error)
      Pgbus.configuration.workflow_subject_url = ->(*) { raise "no route" }
      expect(helper.pgbus_workflow_subject({ subject_type: "Order", subject_id: "7" })).to eq("Order 7")
      expect(Pgbus.logger).to have_received(:error)
    end
  end

  describe "#pgbus_workflow_resume_label" do
    it "says what Resume will do" do
      labels = { retry: "capture", continue: "receipt", finish: nil, resend: "capture", rewait: "ship", carry_on: "capture" }
               .map { |action, step| helper.pgbus_workflow_resume_label({ resume_action: action, resume_step: step }) }

      expect(labels).to eq(["Resume: retry capture", "Resume: continue with receipt", "Resume: finish the run",
                            "Resume: send capture again", "Resume: wait again on ship", "Resume: carry on with capture"])
      expect(helper.pgbus_workflow_resume_label({ resume_action: nil })).to eq("Resume")
    end
  end

  describe "#pgbus_workflow_event_sentence" do
    it "names the event, the actor, the step and the reason" do
      expect(helper.pgbus_workflow_event_sentence({ event: "resumed", actor: "dashboard:alice", reason: "SMTP fixed" }))
        .to eq("Resumed by dashboard:alice: SMTP fixed")
      expect(helper.pgbus_workflow_event_sentence({ event: "step_failed", actor: "system", step_name: "capture", attempt: 2 }))
        .to eq("Step failed by system (capture #2)")
      expect(helper.pgbus_workflow_event_sentence({ event: "brand_new", actor: "app" })).to eq("Brand new by app")
    end
  end

  describe "#pgbus_workflow_value" do
    it "prints scalars as they are and structures as JSON" do
      expect([helper.pgbus_workflow_value("pro"), helper.pgbus_workflow_value(3), helper.pgbus_workflow_value({ "a" => [1] })])
        .to eq(["pro", "3", '{"a":[1]}'])
    end
  end
end
