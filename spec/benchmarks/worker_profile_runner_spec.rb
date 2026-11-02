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

    # Issue #618: WP_BENCH_AJ_LOGS picks the ActiveJob log-line cell.
    context "with WP_BENCH_AJ_LOGS" do
      around do |example|
        previous = ENV.fetch("WP_BENCH_AJ_LOGS", nil)
        example.run
      ensure
        ENV["WP_BENCH_AJ_LOGS"] = previous
      end

      it "sets quiet_active_job_logs for quiet and keeps the plain logger" do
        ENV["WP_BENCH_AJ_LOGS"] = "quiet"
        described_class.configure!("postgres://localhost/bench", threads: 12)

        expect(config.quiet_active_job_logs).to be(true)
        expect(ActiveJob::Base).to have_received(:logger=).with(an_instance_of(Logger))
      end

      it "gives ActiveJob a TaggedLogging logger at warn for warn" do
        ENV["WP_BENCH_AJ_LOGS"] = "warn"
        described_class.configure!("postgres://localhost/bench", threads: 12)

        expect(config.quiet_active_job_logs).to be(false)
        expect(ActiveJob::Base).to have_received(:logger=)
          .with(satisfy { |l| l.respond_to?(:tagged) && l.level == Logger::WARN })
      end

      it "leaves both alone by default" do
        ENV["WP_BENCH_AJ_LOGS"] = nil
        described_class.configure!("postgres://localhost/bench", threads: 12)

        expect(config.quiet_active_job_logs).to be(false)
        expect(ActiveJob::Base).to have_received(:logger=)
          .with(satisfy { |l| !l.respond_to?(:tagged) && l.level == Logger::DEBUG })
      end
    end
  end
end
