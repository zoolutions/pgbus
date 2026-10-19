# frozen_string_literal: true

require_relative "../integration_helper"
require "active_job"

# Issue #583: a gem upgraded before pgbus:add_failed_events_source_queue is
# migrated still records, reads and clears failures, with the (queue_name,
# msg_id) shape the unmigrated table's unique index expects. The table is put
# back in the old shape for the example and restored after it, as the
# integration bootstrap does (a rolled-back transaction would hold the queue
# tables the suite purges against the client's own connection).
RSpec.describe "Failed events before the source_queue migration (integration)", :integration do
  let(:queue) { "unmigrated_failed_events" }
  let(:client) { Pgbus.client }
  let(:executor) { Pgbus::ActiveJob::Executor.new(client: client) }
  let(:data_source) { Pgbus::Web::DataSource.new(client: client) }
  let(:conn) { ActiveRecord::Base.connection }
  let(:attempts) { [] }
  let(:job_class) do
    seen = attempts
    Class.new(ActiveJob::Base) do
      self.queue_adapter = :inline
      def self.name = "UnmigratedFailedEventsSpec::FlakyJob"
      define_method(:perform) do
        seen << :run
        raise "boom on first attempt" if seen.one?
      end
    end
  end

  around do |example|
    conn.execute(<<~SQL)
      DELETE FROM pgbus_failed_events;
      DROP INDEX IF EXISTS idx_pgbus_failed_events_queue_msg_source;
      ALTER TABLE pgbus_failed_events DROP COLUMN IF EXISTS source_queue;
      CREATE UNIQUE INDEX IF NOT EXISTS idx_pgbus_failed_events_queue_msg ON pgbus_failed_events (queue_name, msg_id);
    SQL
    Pgbus::FailedEventRecorder.reset_source_queue_column_check!
    example.run
  ensure
    conn.execute(<<~SQL)
      ALTER TABLE pgbus_failed_events ADD COLUMN IF NOT EXISTS source_queue VARCHAR;
      CREATE UNIQUE INDEX IF NOT EXISTS idx_pgbus_failed_events_queue_msg_source
        ON pgbus_failed_events (queue_name, msg_id, source_queue);
      DROP INDEX IF EXISTS idx_pgbus_failed_events_queue_msg;
    SQL
    Pgbus::FailedEventRecorder.reset_source_queue_column_check!
  end

  before do
    ActiveJob::Base.logger = Logger.new(IO::NULL)
    stub_const("UnmigratedFailedEventsSpec", Module.new)
    stub_const("UnmigratedFailedEventsSpec::FlakyJob", job_class)
    client.ensure_queue(queue)
    client.purge_queue(queue)
  end

  def failed_rows
    conn.select_all("SELECT msg_id, error_message FROM pgbus_failed_events WHERE queue_name = $1", "spec", [queue]).to_a
  end

  it "records the failure, shows it as retrying, and clears it when the redelivered job succeeds" do
    allow(Pgbus.logger).to receive(:warn)
    msg_id = client.send_message(queue, job_class.new.serialize).to_i

    expect(executor.execute(client.read_batch(queue, qty: 1, vt: 30).first, queue)).to eq(:failed)
    expect(Pgbus::FailedEventRecorder.source_queue_column?).to be(false)
    expect(failed_rows).to eq([{ "msg_id" => msg_id, "error_message" => "boom on first attempt" }])

    row = data_source.job_rows(queue_name: Pgbus.configuration.queue_name(queue)).find { |r| r[:msg_id] == msg_id }
    expect(row).to include(state: "retrying", error_message: "boom on first attempt")

    client.set_visibility_timeout(queue, msg_id, vt: 0)
    expect(executor.execute(client.read_batch(queue, qty: 1, vt: 30).first, queue)).to eq(:success)

    expect(failed_rows).to be_empty
    expect(Pgbus.logger).to have_received(:warn).with(no_args).once
  end
end
