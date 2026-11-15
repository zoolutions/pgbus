# frozen_string_literal: true

require_relative "../integration_helper"

# End-to-end checks for the pooler-safety primary-affinity feature (issue #332).
# A real single-node Postgres is always a primary (pg_is_in_recovery() => f), so
# these prove the happy path against a live DB: require_primary verifies
# successfully on a primary, and GUC forwarding round-trips onto the dedicated
# pgmq connection. The replica-REJECTION path (pg_is_in_recovery() => t) is
# covered by the unit spec, which stubs the recovery query — a local single node
# can't be forced into recovery.
RSpec.describe "Pooler primary-affinity (integration)", :integration do
  let(:config) do
    Pgbus::Configuration.new.tap do |c|
      c.database_url = PGBUS_DATABASE_URL
      c.queue_prefix = "pgbus_pooler_test"
    end
  end

  describe "require_primary against a live primary" do
    it "verifies successfully (pg_is_in_recovery is false on a primary)" do
      config.require_primary = true
      client = Pgbus::Client.new(config, schema_ensured: true)

      expect(client.verify_connection!).to be_truthy
    ensure
      client&.close
    end
  end

  describe "GUC forwarding in :options mode" do
    it "applies a forwarded GUC onto the dedicated connection" do
      # Hash params carrying a :variables block, :options mode → baked into the
      # libpq `options` startup param, so the session actually adopts the GUC.
      # Use statement_timeout (a plain SET-able GUC, not libpq-special-cased
      # like application_name) so SHOW round-trips exactly what was forwarded.
      uri = URI.parse(PGBUS_DATABASE_URL)
      config.database_url = nil
      config.connection_params = {
        host: uri.host, port: uri.port, dbname: uri.path.delete_prefix("/"),
        user: uri.user, password: uri.password,
        variables: { "statement_timeout" => "12345" }
      }.compact
      client = Pgbus::Client.new(config, schema_ensured: true)

      applied = client.send(:with_raw_connection) do |conn|
        conn.exec("SHOW statement_timeout").getvalue(0, 0)
      end

      expect(applied).to eq("12345ms")
    ensure
      client&.close
    end

    it "applies a forwarded GUC in :session mode too (post-connect SET, pooler-safe)" do
      uri = URI.parse(PGBUS_DATABASE_URL)
      config.database_url = nil
      config.connection_guc_mode = :session
      config.connection_params = {
        host: uri.host, port: uri.port, dbname: uri.path.delete_prefix("/"),
        user: uri.user, password: uri.password,
        variables: { "statement_timeout" => "23456" }
      }.compact
      client = Pgbus::Client.new(config, schema_ensured: true)

      applied = client.send(:with_raw_connection) do |conn|
        conn.exec("SHOW statement_timeout").getvalue(0, 0)
      end

      expect(applied).to eq("23456ms")
    ensure
      client&.close
    end

    # pgmq-ruby's checkout health check resets an unhealthy pooled connection
    # (finished?, CONNECTION_BAD, or an intermediate state since 0.9.0). A reset
    # opens a new backend and does not re-run the connect lambda, so the SETs
    # are re-applied by the reset itself (issue #671). A closed connection takes
    # the same conn.reset path as the intermediate states.
    context "when pgmq-ruby resets a pooled connection at checkout (issue #671)" do
      let(:session_config) do
        uri = URI.parse(PGBUS_DATABASE_URL)
        config.database_url = nil
        config.connection_guc_mode = :session
        config.pool_size = 1
        config.streams_pool_size = 1
        config.connection_params = {
          host: uri.host, port: uri.port, dbname: uri.path.delete_prefix("/"),
          user: uri.user, password: uri.password,
          variables: { "statement_timeout" => "34567" }
        }.compact
        config
      end

      it "keeps the database.yml variables on the job pool" do
        client = Pgbus::Client.new(session_config, schema_ensured: true)
        pgmq = client.instance_variable_get(:@pgmq)
        first_pid = pgmq.with_connection do |conn|
          conn.backend_pid.tap { conn.close }
        end

        pid, applied = pgmq.with_connection do |conn|
          [conn.backend_pid, conn.exec("SHOW statement_timeout").getvalue(0, 0)]
        end

        expect(pid).not_to eq(first_pid)
        expect(applied).to eq("34567ms")
      ensure
        client&.close
      end

      it "keeps the database.yml variables on the streams pool" do
        client = Pgbus::Client.new(session_config, schema_ensured: true)
        first_pid = client.send(:with_streams_connection) do |conn|
          conn.backend_pid.tap { conn.close }
        end

        pid, applied = client.send(:with_streams_connection) do |conn|
          [conn.backend_pid, conn.exec("SHOW statement_timeout").getvalue(0, 0)]
        end

        expect(pid).not_to eq(first_pid)
        expect(applied).to eq("34567ms")
      ensure
        client&.close
      end
    end
  end
end
