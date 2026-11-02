# frozen_string_literal: true

require "spec_helper"
require "rake"

# Issue #633: rake pgbus:queues:fold_case folds mixed-case queues an older
# process recreated after the pgbus:fold_queue_name_case migration ran.
RSpec.describe "pgbus:queues:fold_case rake task" do # rubocop:disable RSpec/DescribeClass
  let(:rake) { Rake::Application.new }
  let(:conn) { double("connection", current_database: "app_db") }
  let(:pool) { double("connection_pool") }
  let(:names) { %w[pgbus_Orders] }
  let(:pgmq_meta) { "pgmq.meta" }
  let(:in_transaction) { [] }
  let(:folded_inside) { [] }

  around do |example|
    original = Rake.application
    Rake.application = rake
    example.run
  ensure
    Rake.application = original
  end

  before do
    Rake::Task.define_task(:environment)
    load File.expand_path("../../lib/tasks/pgbus_queues.rake", __dir__)

    allow(Pgbus.configuration).to receive(:connects_to).and_return(nil)
    allow(ActiveRecord::Base).to receive(:connection_pool).and_return(pool)
    allow(pool).to receive(:with_connection).and_yield(conn)
    allow(conn).to receive(:quote) { |value| "'#{value}'" }
    allow(conn).to receive(:select_value).with("SELECT to_regclass('pgmq.meta')").and_return(pgmq_meta)
    allow(conn).to receive(:select_values).and_return(names)
    allow(conn).to receive(:transaction) do |&block|
      in_transaction << true
      block.call
    ensure
      in_transaction.pop
    end
    allow(Pgbus::QueueNameFold).to receive(:run!) { folded_inside << in_transaction.any? }
  end

  def invoke(output)
    expect { Rake::Task["pgbus:queues:fold_case"].invoke }.to output(output).to_stdout
  end

  it "folds inside one transaction and names the queues it folded" do
    invoke(/Folded: pgbus_Orders/)

    expect(Pgbus::QueueNameFold).to have_received(:run!).with(conn)
    expect(folded_inside).to eq([true])
  end

  it "lists pgbus's own queues from the connection it folds through" do
    invoke(/Folded/)

    prefix = "#{Pgbus.configuration.queue_prefix}_"
    expect(conn).to have_received(:select_values).with(including("= '#{prefix}'"))
  end

  context "when every queue is already lower case" do
    let(:names) { [] }

    it "says so" do
      expect { Rake::Task["pgbus:queues:fold_case"].invoke }.to output(/already spelled in lower case/).to_stdout
    end
  end

  context "when pgmq.meta is not in the database pgbus's models use" do
    let(:pgmq_meta) { nil }

    it "refuses, folding nothing" do
      expect { Rake::Task["pgbus:queues:fold_case"].invoke }
        .to raise_error(SystemExit).and output(/pgmq.meta is not in the database/).to_stderr

      expect(Pgbus::QueueNameFold).not_to have_received(:run!)
    end
  end

  context "with a dedicated pgbus database (connects_to set)" do
    before do
      allow(Pgbus.configuration).to receive(:connects_to).and_return({ database: { writing: :pgbus } })
      allow(Pgbus::BusRecord).to receive(:connection_pool).and_return(pool)
    end

    it "folds on the pgbus database" do
      invoke(/Folded/)

      expect(Pgbus::BusRecord).to have_received(:connection_pool)
    end
  end
end
