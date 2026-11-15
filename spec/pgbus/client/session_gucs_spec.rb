# frozen_string_literal: true

require "spec_helper"
require "pg"

# connection_guc_mode :session applies database.yml variables with SET after
# connect. PG::Connection#reset opens a new backend without them, and
# pgmq-ruby resets an unhealthy pooled connection at checkout (issue #671), so
# the connection's own reset applies them again.
RSpec.describe Pgbus::Client::SessionGucs do
  subject(:hook) { described_class.new("statement_timeout" => "5s", "lock_timeout" => "2s") }

  let(:base_connection_class) do
    Class.new do
      attr_reader :calls

      def initialize = @calls = []
      def reset = (@calls << [:reset]) && self
      def async_reset = (@calls << [:async_reset]) && self
      def sync_reset = (@calls << [:sync_reset]) && self
      def finished? = @calls.include?([:close])

      def exec(sql)
        @calls << [:exec, sql]
        nil
      end

      def close
        @calls << [:close]
        nil
      end
    end
  end

  let(:conn) { base_connection_class.new.extend(hook) }
  let(:set_statements) { [[:exec, "SET statement_timeout = '5s'"], [:exec, "SET lock_timeout = '2s'"]] }

  describe "#connect" do
    it "applies each variable with SET on a fresh connection" do
      raw = base_connection_class.new
      allow(PG).to receive(:connect).with({ host: "db" }).and_return(raw)

      expect(hook.connect({ host: "db" })).to be(raw)
      expect(raw.calls).to eq(set_statements)
    end

    it "closes the connection and raises when a SET fails" do
      raw = base_connection_class.new
      allow(raw).to receive(:exec).and_raise(PG::InvalidParameterValue, "bad value")
      allow(PG).to receive(:connect).and_return(raw)

      expect { hook.connect({ host: "db" }) }.to raise_error(PG::InvalidParameterValue)
      expect(raw.calls).to eq([[:close]])
    end
  end

  describe "#reset on a connection it extends" do
    it "resets, then applies each variable again" do
      expect(conn.reset).to be(conn)
      expect(conn.calls).to eq([[:reset], *set_statements])
    end

    # pg defines async_reset as an alias of reset, bound to PG::Connection's
    # own method, and sync_reset in C: each would bypass a hook on reset alone.
    it "applies them again after async_reset and sync_reset too" do
      conn.async_reset
      conn.sync_reset

      expect(conn.calls).to eq([[:async_reset], *set_statements, [:sync_reset], *set_statements])
    end

    it "closes the connection when a SET fails, so it is never pooled without them" do
      allow(conn).to receive(:exec).and_call_original
      allow(conn).to receive(:exec).with("SET lock_timeout = '2s'").and_raise(PG::InvalidParameterValue, "bad value")

      expect { conn.reset }.to raise_error(PG::InvalidParameterValue)
      expect(conn.calls).to eq([[:reset], [:exec, "SET statement_timeout = '5s'"], [:close]])
    end

    it "closes the connection when an interrupt lands between two SETs" do
      allow(conn).to receive(:exec).and_call_original
      allow(conn).to receive(:exec).with("SET lock_timeout = '2s'").and_raise(Interrupt)

      expect { conn.reset }.to raise_error(Interrupt)
      expect(conn.calls.last).to eq([:close])
    end

    it "leaves a connection the failure already closed as it is" do
      allow(conn).to receive(:exec).and_wrap_original do |original, sql|
        conn.close
        original.call(sql)
        raise PG::ConnectionBad, "server closed the connection"
      end

      expect { conn.reset }.to raise_error(PG::ConnectionBad)
      expect(conn.calls.count([:close])).to eq(1)
    end
  end
end
