# frozen_string_literal: true

require "spec_helper"

# Pgbus::Streams.filters and .coalescer are process-wide: every thread must get
# the same one, also right after a reset (issue #527). A lazy `@x ||= X.new`
# let two first callers each build one, and a filter registered on the loser
# was gone.
RSpec.describe Pgbus::Streams, :first_call_race do
  after do
    described_class.reset_filters!
    described_class.reset_coalescer!
  end

  it "hands every thread the same filters registry" do
    described_class.reset_filters!
    hold_first_call(Pgbus::Streams::Filters, :new)

    registries = race(threads: 10) { described_class.filters }

    expect(registries.uniq.size).to eq(1)
  end

  it "hands every thread the same coalescer" do
    described_class.reset_coalescer!
    hold_first_call(Pgbus::Streams::Coalescer, :new)

    coalescers = race(threads: 10) { described_class.coalescer }

    expect(coalescers.uniq.size).to eq(1)
  end

  it "keeps a filter registered before other threads first read the registry" do
    described_class.reset_filters!
    described_class.filters.register(:admins) { |user| user == :admin }

    seen = race(threads: 4) { described_class.filters.lookup(:admins) }

    expect(seen).to all(respond_to(:call))
  end
end
