# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Web::DataSource::ListCounts do
  subject(:data_source) { Pgbus::Web::DataSource.new(client: double("Pgbus::Client")) }

  let(:cap) { described_class::COUNT_CAP }
  let(:mock_connection) { double("ActiveRecord::Connection") }

  before { allow(Pgbus::BusRecord).to receive(:connection).and_return(mock_connection) }

  def bounded_relation(model, count)
    scope = double("#{model.name} scope")
    allow(model).to receive(:limit).with(cap + 1).and_return(scope)
    allow(scope).to receive(:count).and_return(count)
  end

  describe "#list_count" do
    {
      batches: Pgbus::BatchEntry,
      job_locks: Pgbus::UniquenessKey,
      outbox: Pgbus::OutboxEntry,
      recurring_tasks: Pgbus::RecurringTask
    }.each do |list, model|
      it "counts #{list} through a LIMIT cap + 1 probe" do
        bounded_relation(model, 42)

        count = data_source.list_count(list)

        expect(count.total).to eq(42)
        expect(count).not_to be_capped
      end

      it "reports #{list} as capped when the probe returns cap + 1 rows" do
        bounded_relation(model, cap + 1)

        count = data_source.list_count(list)

        expect(count.total).to eq(cap)
        expect(count).to be_capped
      end
    end

    it "counts concurrency keys over the same FULL OUTER JOIN as the key list, bounded" do
      sql = nil
      allow(mock_connection).to receive(:select_value) do |query, _label|
        sql = query
        7
      end

      count = data_source.list_count(:concurrency_keys)

      expect(count.total).to eq(7)
      expect(sql).to include("FULL OUTER JOIN", "LIMIT #{cap + 1}")
    end

    it "returns an empty count when the query fails" do
      allow(Pgbus::BatchEntry).to receive(:limit).and_raise(StandardError, "boom")

      expect(data_source.list_count(:batches)).to have_attributes(total: 0, capped?: false)
    end

    it "rejects an unknown list" do
      expect { data_source.list_count(:nope) }.to raise_error(ArgumentError)
    end
  end

  describe "#batches" do
    it "pages with an offset" do
      scope = double("scope")
      allow(Pgbus::BatchEntry).to receive(:order).with(created_at: :desc, id: :desc).and_return(scope)
      allow(scope).to receive(:limit).with(25).and_return(scope)
      allow(scope).to receive(:offset).with(25).and_return([])

      expect(data_source.batches(page: 2, per_page: 25)).to eq([])
      expect(scope).to have_received(:offset).with(25)
    end
  end

  describe "#job_locks" do
    let(:scope) { double("scope") }

    before do
      allow(Pgbus::UniquenessKey).to receive(:order).with(created_at: :desc, lock_key: :asc).and_return(scope)
      allow(scope).to receive_messages(limit: scope, offset: [])
    end

    it "keeps the 100-row first page when called with no arguments (MCP default)" do
      data_source.job_locks

      expect(scope).to have_received(:limit).with(100)
      expect(scope).to have_received(:offset).with(0)
    end

    it "pages with an offset" do
      data_source.job_locks(page: 3, per_page: 25)

      expect(scope).to have_received(:limit).with(25)
      expect(scope).to have_received(:offset).with(50)
    end
  end

  describe "#concurrency_stats" do
    let(:keys_sql) { [] }

    before do
      allow(mock_connection).to receive(:select_all) do |query, label|
        keys_sql << query if label == "Pgbus Concurrency Keys"
        []
      end
    end

    it "keeps the 100-key first page and the same hash keys when called with no arguments" do
      stats = data_source.concurrency_stats

      expect(stats.keys).to eq(%i[parked_total oldest_parked_age_sec slots_held keys_at_limit keys])
      expect(keys_sql.last).to include("LIMIT 100 OFFSET 0")
      # A unique last sort key, so OFFSET pages never skip or repeat a tied row.
      expect(keys_sql.last).to match(/ORDER BY .*COALESCE\(s\.key, b\.concurrency_key\)\s+LIMIT/m)
    end

    it "pages the key rows" do
      data_source.concurrency_stats(page: 2, per_page: 25)

      expect(keys_sql.last).to include("LIMIT 25 OFFSET 25")
    end
  end

  describe "#recurring_tasks" do
    let(:scope) { double("scope", to_a: []) }

    before do
      allow(Pgbus::RecurringTask).to receive(:order).with(:key).and_return(scope)
      allow(scope).to receive_messages(limit: scope, offset: scope)
      allow(Pgbus::RecurringExecution).to receive(:where).and_return(double(select: double(group: double(index_by: {}))))
    end

    it "starts at page 1 when only a page size is given" do
      data_source.recurring_tasks(per_page: 10)

      expect(scope).to have_received(:offset).with(0)
    end

    it "returns every row when called with no arguments (MCP default)" do
      data_source.recurring_tasks

      expect(scope).not_to have_received(:limit)
    end

    it "pages when given a page size" do
      data_source.recurring_tasks(page: 2, per_page: 10)

      expect(scope).to have_received(:limit).with(10)
      expect(scope).to have_received(:offset).with(10)
    end
  end
end
