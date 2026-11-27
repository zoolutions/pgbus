# frozen_string_literal: true

require_relative "../../integration_helper"

# Issue #695: a durable broadcast (and a durable coalesced one) waits for the
# COMMIT of the caller's open, joinable transaction, through
# OpenTransaction.defer as Pgbus.signal does:
#
# - a transaction that is not joinable (the fixture transaction of an app's
#   transactional tests) never commits, so a broadcast deferred to it was
#   lost: it now publishes at once;
# - a `transaction do ... end` inside it is joinable, and its commit publishes;
# - on Rails 7.1, whose transactions have no public after_commit, the deferral
#   raised NoMethodError: it now registers a commit hook.
#
# The broadcast lands in PGMQ over pgbus's own connection, so read_after sees
# it as soon as it is sent, whatever the AR transaction is doing.
RSpec.describe "Streams: durable broadcasts wait for a joinable transaction's commit", :integration do
  let(:config) { Pgbus.configuration }
  let(:stream) { Pgbus.stream("txdefer_#{SecureRandom.hex(4)}") }

  before { stream.ensure! }

  after { config.streams_coalesce_scheduler = nil }

  def published
    stream.read_after(after_id: 0).map do |envelope|
      payload = envelope.payload
      (payload.is_a?(String) ? JSON.parse(payload) : payload)["html"]
    end
  end

  def broadcast(html, **)
    stream.broadcast(html, durable: true, **)
  end

  shared_examples "the commit gate" do
    it "publishes at once inside a transaction that is not joinable" do
      ActiveRecord::Base.transaction(joinable: false) do
        send_frame("<a/>")
        expect(published).to eq(["<a/>"])
      end
    end

    it "publishes at the commit of a transaction opened inside a non-joinable one" do
      ActiveRecord::Base.transaction(joinable: false) do
        ActiveRecord::Base.transaction do
          send_frame("<b/>")
          expect(published).to be_empty
        end
        expect(published).to eq(["<b/>"])
      end
    end

    it "publishes nothing when that inner transaction rolls back" do
      ActiveRecord::Base.transaction(joinable: false) do
        ActiveRecord::Base.transaction do
          send_frame("<c/>")
          raise ActiveRecord::Rollback
        end
      end

      expect(published).to be_empty
    end

    it "publishes after the commit of a plain transaction" do
      ActiveRecord::Base.transaction do
        send_frame("<d/>")
        expect(published).to be_empty
      end

      expect(published).to eq(["<d/>"])
    end

    it "publishes nothing when a plain transaction rolls back" do
      ActiveRecord::Base.transaction do
        send_frame("<e/>")
        raise ActiveRecord::Rollback
      end

      expect(published).to be_empty
    end
  end

  describe "a durable broadcast" do
    def send_frame(html) = broadcast(html)

    it_behaves_like "the commit gate"

    it "reports deferred: in the pgbus.stream.broadcast instrumentation" do
      events = []
      callback = ->(*, payload) { events << payload[:deferred] }
      ActiveSupport::Notifications.subscribed(callback, "pgbus.stream.broadcast") do
        ActiveRecord::Base.transaction { broadcast("<f/>") }
        ActiveRecord::Base.transaction(joinable: false) { broadcast("<g/>") }
        broadcast("<h/>")
      end

      expect(events).to eq([true, false, false])
    end
  end

  # A stream's first broadcast ensures its queue and records it in the
  # registry. When that runs inside the caller's transaction and the
  # transaction rolls back, the registration must not be memoized as done.
  describe "a stream's first broadcast inside a transaction that rolls back" do
    let(:fresh) { Pgbus.stream("txfirst_#{SecureRandom.hex(4)}") }
    let(:full_name) { config.queue_name(fresh.name) }

    it "registers the stream on the next broadcast" do
      Pgbus::StreamQueue.reset_cache!
      ActiveRecord::Base.transaction do
        fresh.broadcast("<x/>", durable: true)
        raise ActiveRecord::Rollback
      end
      fresh.broadcast("<y/>", durable: true)
      Pgbus.stream(fresh.name).broadcast("<z/>", durable: true)

      Pgbus::StreamQueue.reset_cache!
      expect(Pgbus::StreamQueue.stream?(full_name)).to be(true)
    end

    it "is registered and memoized only after the commit of the caller's transaction" do
      memo = Pgbus.client.instance_variable_get(:@streams_registered)
      ActiveRecord::Base.transaction do
        fresh.broadcast("<x/>", durable: true)
        expect(memo[fresh.name]).to be_nil
      end

      expect(memo[fresh.name]).to be(true)
      Pgbus::StreamQueue.reset_cache!
      expect(Pgbus::StreamQueue.stream?(full_name)).to be(true)
    end

    # The row is not written inside the caller's transaction, so another
    # thread recording the same stream does not wait for that commit.
    it "does not make another thread's first broadcast wait for the caller's commit" do
      inside = Queue.new
      release = Queue.new
      caller = Thread.new do # rubocop:disable ThreadSafety/NewThread -- joined below
        ActiveRecord::Base.connection_pool.with_connection do
          ActiveRecord::Base.transaction do
            Pgbus.stream(fresh.name).broadcast("<a/>", durable: true)
            inside << true
            release.pop
          end
        end
      end
      inside.pop

      # In a thread of its own, so a regression fails here instead of hanging.
      other = Thread.new { Pgbus::StreamQueue.record!(full_name) } # rubocop:disable ThreadSafety/NewThread -- joined below

      expect(other.join(2)&.value).to be(true), "the other thread's record! waited for the caller's commit"
    ensure
      release << true
      caller&.join
    end
  end

  # The inline scheduler flushes on the submitting thread, so the submit time
  # (commit or now) is the publish time.
  describe "a durable coalesced broadcast" do
    before { config.streams_coalesce_scheduler = Pgbus::Streams::Coalescer::InlineScheduler.new }

    def send_frame(html) = broadcast(html, coalesce: 10, target: "t-#{html}")

    it_behaves_like "the commit gate"
  end
end
