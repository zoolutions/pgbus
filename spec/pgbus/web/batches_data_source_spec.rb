# frozen_string_literal: true

require "spec_helper"

require_relative "../../../lib/pgbus/web/data_source"

RSpec.describe Pgbus::Web::DataSource do
  subject(:data_source) { described_class.new(client: mock_client) }

  let(:mock_client) { double("Pgbus::Client", pgmq: double("pgmq")) }

  describe "#batches" do
    let(:now) { Time.current }
    let(:batch_attrs) do
      { on_finish_class: nil, on_success_class: nil, on_discard_class: nil }
    end
    let(:records) do
      [
        double("BatchEntry", batch_attrs.merge(batch_id: "aaa", description: "Import users",
                                               status: "processing", total_jobs: 100, completed_jobs: 50,
                                               discarded_jobs: 2, failed_jobs: 0,
                                               created_at: now - 3600, finished_at: nil)),
        double("BatchEntry", batch_attrs.merge(batch_id: "bbb", description: "Send emails",
                                               status: "finished", total_jobs: 10, completed_jobs: 10,
                                               discarded_jobs: 0, failed_jobs: 0,
                                               created_at: now - 7200, finished_at: now - 3600))
      ]
    end

    before do
      scope = double("scope")
      allow(Pgbus::BatchEntry).to receive(:order).with(created_at: :desc).and_return(scope)
      allow(scope).to receive(:limit).with(25).and_return(scope)
      allow(scope).to receive(:offset).with(0).and_return(records)
    end

    it "returns all batches ordered by most recent first" do
      result = data_source.batches

      expect(result.size).to eq(2)
      expect(result.first[:batch_id]).to eq("aaa")
      expect(result.first[:status]).to eq("processing")
      expect(result.first[:progress_pct]).to eq(52)
      expect(result.second[:batch_id]).to eq("bbb")
      expect(result.second[:progress_pct]).to eq(100)
    end

    it "returns empty array on error" do
      allow(Pgbus::BatchEntry).to receive(:order).and_raise(StandardError, "boom")

      expect(data_source.batches).to eq([])
    end

    it "handles zero total_jobs without division error" do
      record = double("BatchEntry", batch_id: "ccc", description: nil, status: "finished",
                                    total_jobs: 0, completed_jobs: 0, discarded_jobs: 0, failed_jobs: 0,
                                    on_finish_class: nil, on_success_class: nil, on_discard_class: nil,
                                    created_at: Time.current, finished_at: Time.current)
      scope = double("scope")
      allow(Pgbus::BatchEntry).to receive(:order).with(created_at: :desc).and_return(scope)
      allow(scope).to receive(:limit).with(25).and_return(scope)
      allow(scope).to receive(:offset).with(0).and_return([record])

      result = data_source.batches
      expect(result.first[:progress_pct]).to eq(100)
    end
  end

  describe "#batch_detail" do
    it "returns a single batch by batch_id" do
      record = double("BatchEntry", batch_id: "aaa", description: "Import users", status: "processing",
                                    total_jobs: 100, completed_jobs: 50, discarded_jobs: 2, failed_jobs: 0,
                                    on_finish_class: "FinishJob", on_success_class: "SuccessJob",
                                    on_discard_class: "DiscardJob", properties: '{"user_id":1}',
                                    created_at: Time.current - 3600, finished_at: nil)
      allow(Pgbus::BatchEntry).to receive(:find_by).with(batch_id: "aaa").and_return(record)

      result = data_source.batch_detail("aaa")

      expect(result[:batch_id]).to eq("aaa")
      expect(result[:description]).to eq("Import users")
      expect(result[:on_finish_class]).to eq("FinishJob")
      expect(result[:properties]).to eq('{"user_id":1}')
      expect(result[:progress_pct]).to eq(52)
    end

    it "shows the job_class of a callback configured as an ActiveJob instance (issue #415)" do
      record = double("BatchEntry", batch_id: "bbb", description: nil, status: "finished",
                                    total_jobs: 1, completed_jobs: 1, failed_jobs: 0,
                                    on_finish_class: nil, on_success_class: nil, on_failure_class: nil,
                                    on_finish_job: { "job_class" => "ReportJob", "queue_name" => "critical" },
                                    on_success_job: nil, on_failure_job: nil,
                                    properties: "{}", created_at: Time.current, finished_at: Time.current)
      allow(Pgbus::BatchEntry).to receive(:find_by).with(batch_id: "bbb").and_return(record)

      result = data_source.batch_detail("bbb")

      expect(result[:on_finish_class]).to eq("ReportJob")
      expect(result[:on_success_class]).to be_nil
    end

    it "returns nil when batch not found" do
      allow(Pgbus::BatchEntry).to receive(:find_by).with(batch_id: "missing").and_return(nil)

      expect(data_source.batch_detail("missing")).to be_nil
    end

    it "returns nil on error" do
      allow(Pgbus::BatchEntry).to receive(:find_by).and_raise(StandardError, "boom")

      expect(data_source.batch_detail("err")).to be_nil
    end
  end

  describe "#batches_count" do
    it "returns the total count of batches" do
      allow(Pgbus::BatchEntry).to receive(:count).and_return(42)

      expect(data_source.batches_count).to eq(42)
    end

    it "returns zero on error" do
      allow(Pgbus::BatchEntry).to receive(:count).and_raise(StandardError, "boom")

      expect(data_source.batches_count).to eq(0)
    end
  end

  describe "#active_batches_count" do
    it "returns count of non-finished batches" do
      not_scope = double("not_scope", count: 5)
      where_scope = double("where_scope", not: not_scope)
      allow(Pgbus::BatchEntry).to receive(:where).and_return(where_scope)

      expect(data_source.active_batches_count).to eq(5)
    end

    it "returns zero on error" do
      allow(Pgbus::BatchEntry).to receive(:where).and_raise(StandardError, "boom")

      expect(data_source.active_batches_count).to eq(0)
    end
  end
end
