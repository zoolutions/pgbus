# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Deploy::Schema do
  let(:conn) { double("connection") }
  let(:pool) { double("pool") }
  let(:clock) { [1_000.0] }
  let(:all_columns) do
    [%i[pgbus_deploy_task_runs completed_at], %i[pgbus_deploy_task_runs failed_at], %i[pgbus_deploy_tasks dependencies]]
  end

  def columns(*present)
    allow(conn).to receive(:column_exists?) { |table, column| present.include?([table, column]) }
  end

  before do
    described_class.reset!
    allow(Pgbus::BusRecord).to receive(:connection_pool).and_return(pool)
    allow(pool).to receive(:with_connection).and_yield(conn)
    allow(described_class).to receive(:monotonic_now) { clock.first }
  end

  after { described_class.reset! }

  it "is true once all three columns exist, and keeps that answer" do
    columns(*all_columns)

    2.times { expect(described_class.outcomes?).to be(true) }
    expect(pool).to have_received(:with_connection).once
  end

  it "is false while a column is missing, and probes again after RECHECK_SECONDS" do
    columns(*all_columns.first(2))
    expect(described_class.outcomes?).to be(false)
    columns(*all_columns)

    expect(described_class.outcomes?).to be(false)
    clock[0] += described_class::RECHECK_SECONDS
    expect(described_class.outcomes?).to be(true)
  end

  it "raises a failed probe to the caller and keeps no answer" do
    allow(conn).to receive(:column_exists?).and_raise(StandardError, "db blip")
    expect { described_class.outcomes? }.to raise_error(StandardError, "db blip")

    columns(*all_columns)
    expect(described_class.outcomes?).to be(true)
  end
end
