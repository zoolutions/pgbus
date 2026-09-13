# frozen_string_literal: true

require "spec_helper"

# EventBus consumers are long-lived threads. A pooler restart, an admin
# disconnect or a brief failover can kill the leased ActiveRecord socket
# between `handle` returning and the phase-2 claim stamp, and Rails does not
# reconnect `Relation#update_all` for you (`allow_retry: false`), so the drop
# surfaced to the host app as a paging exception on a message that had in fact
# been handled successfully.
RSpec.describe Pgbus::EventBus::StaleConnectionRetry do
  def connection_failed(message)
    ActiveRecord::ConnectionFailed.new(message)
  end

  before { allow(described_class).to receive(:reconnect_leased!) }

  describe ".call" do
    it "returns the block's value when nothing goes wrong" do
      expect(described_class.call { :ok }).to eq(:ok)
    end

    it "does not reconnect when nothing goes wrong" do
      described_class.call { :ok }

      expect(described_class).not_to have_received(:reconnect_leased!)
    end

    # The three shapes the drop actually arrives as. Each is the socket dying
    # underneath a statement, not a server that refused us.
    [
      "PQconsumeInput() SSL error: unexpected eof while reading",
      "server closed the connection unexpectedly",
      "SSL error: unexpected eof while reading"
    ].each do |message|
      it "reconnects and retries once on #{message.split(":").first}" do
        attempts = 0

        result = described_class.call do
          attempts += 1
          raise connection_failed(message) if attempts == 1

          :recovered
        end

        expect(result).to eq(:recovered)
        expect(attempts).to eq(2)
        expect(described_class).to have_received(:reconnect_leased!).once
      end
    end

    it "reads the pattern off the cause when the wrapper message is bare" do
      attempts = 0
      wrapped = connection_failed("connection failed")
      allow(wrapped).to receive(:cause).and_return(
        StandardError.new("PQconsumeInput() SSL error: unexpected eof while reading")
      )

      described_class.call do
        attempts += 1
        raise wrapped if attempts == 1

        :recovered
      end

      expect(attempts).to eq(2)
    end

    # A refused connection is the database being unreachable, not a socket that
    # went stale mid-statement. Retrying it in-process buys nothing and hides a
    # real outage behind a doubled statement timeout.
    it "re-raises a ConnectionFailed that is not a transient drop" do
      expect { described_class.call { raise connection_failed("Connection refused") } }
        .to raise_error(ActiveRecord::ConnectionFailed, "Connection refused")

      expect(described_class).not_to have_received(:reconnect_leased!)
    end

    it "re-raises anything that is not a ConnectionFailed" do
      expect { described_class.call { raise ActiveRecord::StatementInvalid, "boom" } }
        .to raise_error(ActiveRecord::StatementInvalid)

      expect(described_class).not_to have_received(:reconnect_leased!)
    end

    # One retry, not a loop: a second drop means the socket is not coming back
    # in this attempt, and PGMQ's visibility timeout is the right place to
    # recover from that — it redelivers the message to a healthy consumer.
    it "raises the second drop rather than retrying again" do
      attempts = 0

      expect do
        described_class.call do
          attempts += 1
          raise connection_failed("PQconsumeInput() SSL error: unexpected eof while reading")
        end
      end.to raise_error(ActiveRecord::ConnectionFailed)

      expect(attempts).to eq(2)
    end

    it "logs the retry so a recovered drop is still visible" do
      allow(Pgbus.logger).to receive(:warn)
      attempts = 0

      described_class.call(context: "MilestoneHandler") do
        attempts += 1
        raise connection_failed("PQconsumeInput() SSL error: unexpected eof while reading") if attempts == 1

        :ok
      end

      expect(Pgbus.logger).to have_received(:warn)
    end
  end

  describe ".transient_drop?" do
    it "is false for an error of another class carrying a matching message" do
      expect(described_class.transient_drop?(StandardError.new("PQconsumeInput() SSL error"))).to be false
    end
  end
end
