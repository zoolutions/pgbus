# frozen_string_literal: true

require_relative "../integration_helper"

# The paged dashboard lists (issue #496) against the real tables: OFFSET paging
# and the bounded counts behind the pager.
RSpec.describe "Dashboard list paging (integration)", :integration do
  let(:data_source) { Pgbus::Web::DataSource.new(client: Pgbus.client) }

  before do
    Pgbus::Semaphore.delete_all
    Pgbus::BlockedExecution.delete_all
    Pgbus::UniquenessKey.delete_all
  end

  describe "concurrency keys" do
    before do
      3.times { |i| Pgbus::Concurrency::Semaphore.acquire("paging-key-#{i}", 2, 900) }
      Pgbus::Concurrency::BlockedExecution.insert(
        concurrency_key: "paging-parked-only", queue_name: "default",
        payload: { "job_class" => "PagingJob", "job_id" => SecureRandom.uuid, "arguments" => [] }, duration: 900
      )
    end

    it "counts every key of the FULL OUTER JOIN, parked-only keys included" do
      count = data_source.list_count(:concurrency_keys)

      expect(count.total).to eq(4)
      expect(count).not_to be_capped
    end

    it "pages the key rows without overlap, busiest first" do
      first = data_source.concurrency_stats(page: 1, per_page: 2)[:keys].map { |k| k[:key] }
      second = data_source.concurrency_stats(page: 2, per_page: 2)[:keys].map { |k| k[:key] }

      expect(first.first).to eq("paging-parked-only")
      expect(first.size + second.size).to eq(4)
      expect(first & second).to be_empty
    end
  end

  describe "uniqueness locks" do
    before do
      3.times { |i| Pgbus::UniquenessKey.acquire!("paging-lock-#{i}", queue_name: "default", msg_id: i) }
    end

    it "counts through the bounded probe and pages with an offset" do
      expect(data_source.list_count(:job_locks).total).to eq(3)
      expect(data_source.job_locks(page: 2, per_page: 2).size).to eq(1)
      expect(data_source.job_locks.size).to eq(3)
    end
  end
end
