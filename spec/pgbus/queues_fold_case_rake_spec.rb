# frozen_string_literal: true

require "spec_helper"
require "rake"

# Issue #633: rake pgbus:queues:fold_case folds mixed-case queues an older
# process recreated after the pgbus:fold_queue_name_case migration ran.
RSpec.describe "pgbus:queues:fold_case rake task" do # rubocop:disable RSpec/DescribeClass
  let(:rake) { Rake::Application.new }
  let(:conn) { double("connection") }
  let(:pool) { double("connection_pool") }
  let(:client) { double("Pgbus::Client", mixed_case_queue_names: names) }
  let(:names) { %w[pgbus_Orders] }

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

    allow(Pgbus).to receive(:client).and_return(client)
    allow(Pgbus.configuration).to receive(:connects_to).and_return(nil)
    allow(ActiveRecord::Base).to receive(:connection_pool).and_return(pool)
    allow(pool).to receive(:with_connection).and_yield(conn)
    allow(conn).to receive(:transaction).and_yield
    allow(Pgbus::QueueNameFold).to receive(:run!)
  end

  def invoke(output)
    expect { Rake::Task["pgbus:queues:fold_case"].invoke }.to output(output).to_stdout
  end

  it "folds inside one transaction and names the queues it folded" do
    invoke(/Folded: pgbus_Orders/)

    expect(Pgbus::QueueNameFold).to have_received(:run!).with(conn)
    expect(conn).to have_received(:transaction)
  end

  context "when every queue is already lower case" do
    let(:names) { [] }

    it "says so" do
      expect { Rake::Task["pgbus:queues:fold_case"].invoke }.to output(/already spelled in lower case/).to_stdout
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
