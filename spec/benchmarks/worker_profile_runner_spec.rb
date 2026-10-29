# frozen_string_literal: true

require "spec_helper"
require_relative "../../benchmarks/support/worker_profile_runner"

# WorkerProfileRunner.configure! (issue #602): the fleet cell's connection
# budget holds only if the pool sizes it passes reach ActiveRecord and pgbus.
# No database: establish_connection is stubbed and the configuration is read
# from the block pgbus would run.
RSpec.describe WorkerProfileRunner do
  describe ".configure!" do
    let(:config) { Pgbus::Configuration.new }

    before do
      allow(ActiveRecord::Base).to receive(:establish_connection)
      allow(ActiveJob::Base).to receive(:logger=)
      allow(Pgbus).to receive(:configure).and_yield(config)
    end

    it "passes pool_size to pgbus and ar_pool to ActiveRecord" do
      described_class.configure!("postgres://localhost/bench", threads: 12, pool_size: 14, ar_pool: 2)

      expect(config.pool_size).to eq(14)
      expect(ActiveRecord::Base).to have_received(:establish_connection).with("postgres://localhost/bench?pool=2")
    end

    it "keeps pgbus's pool size and sizes ActiveRecord from the threads by default" do
      described_class.configure!("postgres://localhost/bench", threads: 12)

      expect(config.pool_size).to be_nil
      expect(ActiveRecord::Base).to have_received(:establish_connection).with("postgres://localhost/bench?pool=20")
    end
  end
end
