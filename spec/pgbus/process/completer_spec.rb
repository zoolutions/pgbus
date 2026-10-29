# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Process::Completer do
  subject(:completer) { described_class.new(client: client, batch_size: batch_size) }

  let(:client) { instance_double(Pgbus::Client) }
  let(:batch_size) { 100 }
  # Every archive_batch call, in order, as [queue, ids].
  let(:calls) { Thread::Queue.new }

  # archive_batch answers like pgmq-ruby: the archived ids as Strings, ids
  # already gone simply absent.
  def archive_all
    allow(client).to receive(:archive_batch) do |queue, ids, prefixed:|
      raise ArgumentError, "expected prefixed: false" unless prefixed == false

      calls << [queue, ids]
      ids.map(&:to_s)
    end
  end

  def recorded_calls
    Array.new(calls.size) { calls.pop }
  end

  # A job thread stand-in; the examples assert on what it raises, so it does
  # not report it.
  def background(*args)
    Thread.new(*args) do |*thread_args| # rubocop:disable ThreadSafety/NewThread -- joined by the example (#value)
      Thread.current.report_on_exception = false
      yield(*thread_args)
    end
  end

  # Runs the block on count threads and returns them.
  def in_threads(count, &block)
    Array.new(count) { |i| background(i, &block) }
  end

  after { completer.stop }

  it "archives a lone id at once with one archive_batch and reports :archived" do
    archive_all

    expect(completer.complete("pgbus_default", 7)).to eq(:archived)
    expect(recorded_calls).to eq([["pgbus_default", [7]]])
  end

  it "coalesces the ids that arrive while a flush is in flight into the next archive_batch" do
    gate = Thread::Queue.new
    allow(client).to receive(:archive_batch) do |queue, ids, prefixed:| # rubocop:disable Lint/UnusedBlockArgument
      calls << [queue, ids]
      gate.pop if ids == [1] # hold the first flush until every other id is pending
      ids.map(&:to_s)
    end

    lone = background { completer.complete("pgbus_default", 1) }
    sleep 0.01 until calls.size == 1
    others = in_threads(5) { |i| completer.complete("pgbus_default", 10 + i) }
    sleep 0.01 until completer.pending_count == 5
    gate << :go

    expect(([lone] + others).map(&:value)).to all(eq(:archived))
    first, second, *rest = recorded_calls
    expect([first, rest]).to eq([["pgbus_default", [1]], []])
    expect(second.last).to contain_exactly(10, 11, 12, 13, 14)
  end

  it "reports :already_archived for an id missing from the result, :archived for the rest" do
    gate = Thread::Queue.new
    allow(client).to receive(:archive_batch) do |_queue, ids, prefixed:| # rubocop:disable Lint/UnusedBlockArgument
      gate.pop if ids == [0]
      ids.reject { |id| id == 2 }.map(&:to_s)
    end

    holder = background { completer.complete("q", 0) }
    sleep 0.01 until completer.flushing?
    waiters = [1, 2, 3].to_h { |id| [id, background { completer.complete("q", id) }] }
    sleep 0.01 until completer.pending_count == 3
    gate << :go

    expect(holder.value).to eq(:archived)
    expect(waiters.transform_values(&:value)).to eq(1 => :archived, 2 => :already_archived, 3 => :archived)
  end

  # Two threads of one worker can hold the same message when its heartbeat
  # lapsed and it was redelivered to this worker. pgmq archives the id once;
  # only one of them may own the completion signals (rails/solid_queue#761).
  it "gives :archived to one waiter of a duplicated id and :already_archived to the other" do
    gate = Thread::Queue.new
    allow(client).to receive(:archive_batch) do |_queue, ids, prefixed:| # rubocop:disable Lint/UnusedBlockArgument
      gate.pop if ids == [0]
      ids.uniq.map(&:to_s)
    end

    holder = background { completer.complete("q", 0) }
    sleep 0.01 until completer.flushing?
    twins = in_threads(2) { completer.complete("q", 5) }
    sleep 0.01 until completer.pending_count == 2
    gate << :go

    holder.join
    expect(twins.map(&:value)).to contain_exactly(:archived, :already_archived)
  end

  it "raises the flush's exception on every waiter of that queue and on no other" do
    boom = Class.new(StandardError)
    gate = Thread::Queue.new
    allow(client).to receive(:archive_batch) do |queue, ids, prefixed:| # rubocop:disable Lint/UnusedBlockArgument
      gate.pop if ids == [0]
      raise boom, "archive failed" if queue == "bad"

      ids.map(&:to_s)
    end

    holder = background { completer.complete("good", 0) }
    sleep 0.01 until completer.flushing?
    bad = in_threads(2) { |i| completer.complete("bad", i + 1) }
    good = background { completer.complete("good", 9) }
    sleep 0.01 until completer.pending_count == 3
    gate << :go

    expect(holder.value).to eq(:archived)
    expect(good.value).to eq(:archived)
    bad.each { |thread| expect { thread.value }.to raise_error(boom, "archive failed") }
  end

  it "flushes two queues as two statements" do
    gate = Thread::Queue.new
    allow(client).to receive(:archive_batch) do |queue, ids, prefixed:| # rubocop:disable Lint/UnusedBlockArgument
      calls << [queue, ids]
      gate.pop if ids == [0]
      ids.map(&:to_s)
    end

    holder = background { completer.complete("a", 0) }
    sleep 0.01 until calls.size == 1
    waiters = [background { completer.complete("a", 1) }, background { completer.complete("b", 2) },
               background { completer.complete("b", 3) }]
    sleep 0.01 until completer.pending_count == 3
    gate << :go

    [holder, *waiters].each(&:join)
    later = recorded_calls.drop(1).to_h { |queue, ids| [queue, ids.sort] }
    expect(later).to eq("a" => [1], "b" => [2, 3])
  end

  context "with a batch size smaller than the pending group" do
    let(:batch_size) { 2 }

    it "splits the group into statements of at most batch_size ids" do
      gate = Thread::Queue.new
      allow(client).to receive(:archive_batch) do |queue, ids, prefixed:| # rubocop:disable Lint/UnusedBlockArgument
        calls << [queue, ids]
        gate.pop if ids == [0]
        ids.map(&:to_s)
      end

      holder = background { completer.complete("q", 0) }
      sleep 0.01 until calls.size == 1
      waiters = in_threads(5) { |i| completer.complete("q", i + 1) }
      sleep 0.01 until completer.pending_count == 5
      gate << :go

      expect(waiters.map(&:value)).to all(eq(:archived))
      holder.join
      sizes = recorded_calls.drop(1).map { |_queue, ids| ids.size }
      expect(sizes).to eq([2, 2, 1])
    end
  end

  describe "#stop" do
    it "flushes what is pending, then joins the flusher" do
      gate = Thread::Queue.new
      allow(client).to receive(:archive_batch) do |queue, ids, prefixed:| # rubocop:disable Lint/UnusedBlockArgument
        calls << [queue, ids]
        gate.pop if ids == [0]
        ids.map(&:to_s)
      end

      holder = background { completer.complete("q", 0) }
      sleep 0.01 until calls.size == 1
      late = background { completer.complete("q", 4) }
      sleep 0.01 until completer.pending_count == 1
      stopper = background { completer.stop }
      gate << :go

      expect(stopper.join(5)).to eq(stopper)
      expect([holder.value, late.value]).to eq(%i[archived archived])
      expect(completer).not_to be_alive
    end

    it "is idempotent" do
      archive_all

      completer.stop

      expect { completer.stop }.not_to raise_error
    end

    # Worker#immediate_shutdown stops the completer before it gives the
    # running jobs their grace: a job finishing inside the grace is still
    # archived, on its own thread, with the same results.
    it "archives on the caller's thread after the completer stopped" do
      archive_all
      completer.stop

      expect(completer.complete("q", 3)).to eq(:archived)
      expect(recorded_calls).to eq([["q", [3]]])
    end

    it "fails a waiter with Stopped when the flusher dies before serving it" do
      gate = Thread::Queue.new
      allow(client).to receive(:archive_batch) do |_queue, ids, prefixed:| # rubocop:disable Lint/UnusedBlockArgument
        gate.pop
        ids.map(&:to_s)
      end

      waiter = background { completer.complete("q", 1) }
      sleep 0.01 until completer.flushing?
      completer.stop(timeout: 0.05)

      expect { waiter.value }.to raise_error(described_class::Stopped)
    end
  end

  it "keeps serving after a flush raised something that is not a StandardError" do
    calls_made = 0
    allow(client).to receive(:archive_batch) do |_queue, ids, prefixed:| # rubocop:disable Lint/UnusedBlockArgument
      calls_made += 1
      raise NotImplementedError, "not a StandardError" if calls_made == 1

      ids.map(&:to_s)
    end
    allow(Pgbus.logger).to receive(:error)

    expect { completer.complete("q", 1) }.to raise_error(NotImplementedError)
    expect(completer.complete("q", 2)).to eq(:archived)
    expect(Pgbus.logger).to have_received(:error)
  end
end
