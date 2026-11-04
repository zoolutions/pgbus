# frozen_string_literal: true

require "spec_helper"

# Issue #645: a pooled PG::Connection whose reset was interrupted mid-poll sits
# in CONNECTION_STARTED. pgmq-ruby's checkout health check only resets a
# finished or CONNECTION_BAD connection, so every stale retry got the same
# socket back. Before the LAST stale retry, with_stale_connection_retry drops
# both pools' idle connections (dedicated path only), so that retry checks out
# a fresh one.
RSpec.describe Pgbus::Client do
  describe "pool reload before the last stale-connection retry" do
    before do
      allow(described_class).to receive(:load_pgmq_gem!)
      stub_const("PGMQ::Client", Class.new { def initialize(*args, **kwargs); end })
      real_pgmq_connection_error
      stub_pg_library_version
      allow(Pgbus.logger).to receive(:warn) { |&blk| warnings << blk.call }
    end

    let(:warnings) { [] }
    let(:stale) { "Database connection error: PQsendQueryParams no connection to the server" }

    # Raises +errors+ in order, one per call, then returns 1.
    def produce_failing(pgmq, *errors)
      calls = 0
      allow(pgmq).to receive(:produce) do
        calls += 1
        raise PGMQ::Errors::ConnectionError, errors[calls - 1] if calls <= errors.size

        1
      end
      -> { calls }
    end

    def reload_warnings
      warnings.grep(/Dropping pooled connections/)
    end

    context "when on the dedicated-connection path" do
      subject(:client) do
        # Client#initialize builds the job pool first, the streams pool second.
        allow(PGMQ::Client).to receive(:new).and_return(job_pgmq, streams_pgmq)
        c = described_class.new(config, schema_ensured: true)
        allow(c).to receive(:tune_autovacuum)
        allow(c).to receive(:notify_trigger_current?).and_return(false)
        allow(c).to receive(:sleep)
        c
      end

      let(:job_pgmq) { build_mock_pgmq }
      let(:streams_pgmq) { build_mock_pgmq }
      let(:config) do
        Pgbus::Configuration.new.tap do |c|
          c.database_url = "postgres://localhost/pgbus_test"
        end
      end

      it "does not reload when the first retry succeeds" do
        calls = produce_failing(job_pgmq, stale)

        expect(client.send_message("default", { "k" => "v" })).to eq(1)
        expect(calls.call).to eq(2)
        expect(job_pgmq).not_to have_received(:reload)
        expect(streams_pgmq).not_to have_received(:reload)
        expect(reload_warnings).to be_empty
      end

      it "reloads both pools exactly once, before the last retry, when the first retry fails stale" do
        calls = produce_failing(job_pgmq, stale, stale)
        allow(job_pgmq).to receive(:reload) { expect(calls.call).to eq(2) }

        expect(client.send_message("default", { "k" => "v" })).to eq(1)
        expect(calls.call).to eq(3)
        expect(job_pgmq).to have_received(:reload).once
        expect(streams_pgmq).to have_received(:reload).once
      end

      it "logs one warning naming the attempt and the error" do
        produce_failing(job_pgmq, stale, stale)

        client.send_message("default", { "k" => "v" })

        expect(reload_warnings).to contain_exactly(
          a_string_including("before the last stale-connection retry (attempt 2/2)", "no connection to the server")
        )
      end

      it "reloads once and re-raises the stale error when the last retry fails too" do
        calls = produce_failing(job_pgmq, stale, stale, stale)

        expect { client.send_message("default", { "k" => "v" }) }
          .to raise_error(PGMQ::Errors::ConnectionError, /no connection to the server/)
        expect(calls.call).to eq(3)
        expect(job_pgmq).to have_received(:reload).once
      end

      it "never reloads on a non-stale ConnectionError" do
        calls = produce_failing(job_pgmq, "Connection pool timeout: waited 5.00s")
        allow(job_pgmq).to receive(:stats).and_return({ size: 5, available: 0 })

        expect { client.send_message("default", { "k" => "v" }) }
          .to raise_error(PGMQ::Errors::ConnectionError, /pool timeout/)
        expect(calls.call).to eq(1)
        expect(job_pgmq).not_to have_received(:reload)
        expect(streams_pgmq).not_to have_received(:reload)
      end

      it "logs a reload failure and still runs the last retry" do
        calls = produce_failing(job_pgmq, stale, stale)
        allow(job_pgmq).to receive(:reload).and_raise(StandardError, "pool exploded")

        expect(client.send_message("default", { "k" => "v" })).to eq(1)
        expect(calls.call).to eq(3)
        expect(warnings).to include(a_string_including("pool reload before the last stale-connection retry failed",
                                                       "pool exploded"))
      end

      it "re-raises the stale error, not the reload failure, when the last retry fails too" do
        produce_failing(job_pgmq, stale, stale, stale)
        allow(streams_pgmq).to receive(:reload).and_raise(StandardError, "pool exploded")

        expect { client.send_message("default", { "k" => "v" }) }
          .to raise_error(PGMQ::Errors::ConnectionError, /no connection to the server/)
      end
    end

    # The Proc path's pool slots wrap ActiveRecord's own socket: pgbus never
    # closes it, and says nothing about not doing so on every retry.
    context "when on the shared-connection (Proc) path" do
      subject(:shared_client) do
        allow(PGMQ::Client).to receive(:new).and_return(mock_pgmq)
        c = described_class.new(shared_config, schema_ensured: true)
        allow(c).to receive(:tune_autovacuum)
        allow(c).to receive(:notify_trigger_current?).and_return(false)
        allow(c).to receive(:sleep)
        c
      end

      let(:mock_pgmq) { build_mock_pgmq }
      let(:shared_config) do
        Pgbus::Configuration.new.tap do |c|
          c.connection_params = -> { :raw_conn }
        end
      end

      it "never reloads and logs no reload line" do
        calls = produce_failing(mock_pgmq, stale, stale)

        expect(shared_client.send_message("default", { "k" => "v" })).to eq(1)
        expect(calls.call).to eq(3)
        expect(mock_pgmq).not_to have_received(:reload)
        expect(reload_warnings).to be_empty
        expect(warnings.join).not_to include("reload skipped")
      end
    end
  end
end
