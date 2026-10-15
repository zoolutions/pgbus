# frozen_string_literal: true

require "spec_helper"

# A constant holding a Mutex cannot be read from a non-main Ractor, and a
# constant is visible to the whole process; the pgmq require lock is class
# state built at load instead, like the install lock (issue #527).
RSpec.describe Pgbus::Client do
  it "keeps the pgmq require lock in class state built at load" do
    expect(described_class.instance_variable_get(:@pgmq_require_mutex)).to be_a(Thread::Mutex)
    expect(described_class.const_defined?(:PGMQ_REQUIRE_MUTEX, false)).to be(false)
  end

  it "loads pgmq under that lock" do
    lock = described_class.instance_variable_get(:@pgmq_require_mutex)
    allow(lock).to receive(:synchronize).and_call_original

    described_class.load_pgmq_gem!

    expect(lock).to have_received(:synchronize)
  end

  # The lock is Client's class state, which a subclass does not inherit.
  it "loads pgmq under Client's lock when called on a subclass" do
    lock = described_class.instance_variable_get(:@pgmq_require_mutex)
    allow(lock).to receive(:synchronize).and_call_original

    Class.new(described_class).load_pgmq_gem!

    expect(lock).to have_received(:synchronize)
  end
end
