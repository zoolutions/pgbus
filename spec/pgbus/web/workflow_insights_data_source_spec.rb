# frozen_string_literal: true

require "spec_helper"

# The workflow insights reads of Web::DataSource with the queries mocked
# (issue #549): the guards, the class resolution and the error paths. The
# SQL runs against a real database in
# spec/integration/workflow/insights_data_source_spec.rb.
RSpec.describe Pgbus::Web::DataSource::WorkflowInsights do
  subject(:data_source) { Pgbus::Web::DataSource.new(client: double("Pgbus::Client")) }

  let(:queries) { Pgbus::Web::WorkflowInsightsQueries }
  let(:window) { Pgbus::Web::WorkflowWindow.parse("24h", retention: nil) }
  let(:klass) do
    Class.new(Pgbus::Workflow) do
      def self.name = "InsightsSpecWorkflow"
      step(:one) { nil }
      step(:two) { nil }
    end
  end

  before do
    allow(Pgbus::Workflow::Sweep).to receive_messages(tables_ready?: true, waiting_ready?: true)
    allow(Pgbus::Workflow).to receive(:registered_classes).and_return([klass])
    allow(Pgbus::WorkflowEntry).to receive(:stuck_reason_counts).and_return({})
    allow(Pgbus.logger).to receive(:error)
  end

  after { Pgbus::Workflow::Registry.reset! }

  def stub_queries(**answers)
    defaults = { class_names: [], class_known?: false, ongoing: [], ended: [], step_stats: {}, duration_histogram: {},
                 throughput: {}, funnel_reached: {}, funnel_stopped: {}, by_revision: [] }
    defaults.merge(answers).each { |name, value| allow(queries).to receive(name).and_return(value) }
  end

  describe "an install without the workflow tables" do
    before { allow(Pgbus::Workflow::Sweep).to receive(:tables_ready?).and_return(false) }

    it "reads as empty without logging an error" do
      expect(data_source.workflow_definitions(window: window)).to eq([])
      expect(data_source.workflow_definition("InsightsSpecWorkflow", window: window)).to be_nil
      expect(Pgbus.logger).not_to have_received(:error)
    end
  end

  describe "#workflow_definitions" do
    it "lists a registered class without rows, with zeros" do
      stub_queries

      expect(data_source.workflow_definitions(window: window).sole)
        .to include(workflow_class: "InsightsSpecWorkflow", loaded: true, ongoing: 0, paused: 0, attention: 0, finished: 0,
                    cancelled: 0, e2e_ms: { p50: nil, p95: nil, samples: 0 }, removed_in_flight: {})
    end

    it "sums the stuck reasons of each class" do
      stub_queries
      allow(Pgbus::WorkflowEntry).to receive(:stuck_reason_counts)
        .and_return({ %w[InsightsSpecWorkflow paused_error] => 2, %w[InsightsSpecWorkflow overdue] => 1 })

      expect(data_source.workflow_definitions(window: window).sole[:attention]).to eq(3)
    end

    it "logs a failing read and answers an empty list" do
      allow(queries).to receive(:ongoing).and_raise(ActiveRecord::StatementInvalid, "db down")

      expect(data_source.workflow_definitions(window: window)).to eq([])
      expect(Pgbus.logger).to have_received(:error)
    end
  end

  describe "#workflow_definition" do
    it "is nil for a name neither registered nor in the rows, without constantizing it" do
      stub_queries(class_known?: false)
      name = +"Kernel"
      allow(name).to receive(:safe_constantize)

      expect(data_source.workflow_definition(name, window: window)).to be_nil
      expect(name).not_to have_received(:safe_constantize)
    end

    it "uses the registered class without asking the rows" do
      stub_queries

      result = data_source.workflow_definition("InsightsSpecWorkflow", window: window)

      expect(result).to include(workflow_class: "InsightsSpecWorkflow", loaded: true, started: 0)
      expect(result[:steps].map { |s| s[:name] }).to eq(%w[one two])
      expect(queries).not_to have_received(:class_known?)
    end

    it "logs a failing read and answers nil" do
      stub_queries
      allow(queries).to receive(:step_stats).and_raise(ActiveRecord::StatementInvalid, "db down")

      expect(data_source.workflow_definition("InsightsSpecWorkflow", window: window)).to be_nil
      expect(Pgbus.logger).to have_received(:error)
    end
  end
end
