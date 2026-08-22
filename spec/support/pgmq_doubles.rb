# frozen_string_literal: true

module PgmqDoubles
  def build_mock_pgmq
    double("PGMQ::Client").tap do |pgmq|
      allow(pgmq).to receive_messages(
        create: nil,
        enable_notify_insert: nil,
        produce: 1,
        produce_batch: [1, 2],
        read: nil,
        read_batch: [],
        read_with_poll: [],
        read_multi: [],
        read_grouped: [],
        read_grouped_rr: [],
        read_grouped_head: [],
        delete: true,
        archive: true,
        set_vt: nil,
        metrics: nil,
        metrics_all: [],
        list_queues: [],
        purge_queue: nil,
        drop_queue: true,
        bind_topic: nil,
        produce_topic: nil,
        create_fifo_index: nil,
        create_fifo_indexes_all: nil,
        wait_for_notify: nil,
        update_notify_insert: nil,
        list_notify_insert_throttles: [],
        convert_archive_partitioned: nil,
        tune_autovacuum: nil,
        stats: { size: 5, available: 5 },
        close: nil,
        reload: nil
      )
      # Batch operations echo back the ids to mirror pgmq-ruby behavior
      allow(pgmq).to receive(:delete_batch) { |_queue, ids| ids.map(&:to_s) }
      allow(pgmq).to receive(:archive_batch) { |_queue, ids| ids.map(&:to_s) }
      allow(pgmq).to receive(:transaction).and_yield(pgmq)
    end
  end

  def build_mock_client(pgmq: nil)
    pgmq ||= build_mock_pgmq
    double("Pgbus::Client", pgmq: pgmq).tap do |client|
      allow(client).to receive_messages(
        ensure_queue: nil,
        ensure_all_queues: nil,
        ensure_dead_letter_queue: nil,
        send_message: 1,
        send_batch: [1, 2],
        read_message: nil,
        read_batch: [],
        read_multi: [],
        read_grouped: [],
        read_grouped_rr: [],
        read_grouped_head: [],
        delete_message: true,
        archive_message: true,
        set_visibility_timeout: nil,
        move_to_dead_letter: nil,
        metrics: nil,
        pool_stats: { size: 5, available: 5, pool_timeout: 5 },
        list_queues: [],
        uniqueness_keys_present: Set.new,
        message_exists?: nil,
        purge_queue: nil,
        drop_queue: true,
        bind_topic: nil,
        publish_to_topic: nil,
        create_fifo_index: nil,
        create_fifo_indexes_all: nil,
        wait_for_notify: nil,
        update_notify_insert: nil,
        list_notify_insert_throttles: [],
        convert_archive_partitioned: nil,
        close: nil,
        transaction: nil
      )
      # Batch operations echo back the ids to mirror pgmq-ruby behavior
      allow(client).to receive(:delete_batch) { |_queue, ids| ids.map(&:to_s) }
      allow(client).to receive(:archive_batch) { |_queue, ids| ids.map(&:to_s) }
    end
  end

  def build_message_double(msg_id: 1, message: "{}", read_ct: 0, headers: nil, enqueued_at: :default)
    enqueued_at = Time.current.utc.iso8601(6) if enqueued_at == :default
    double("PGMQ::Message", msg_id: msg_id, message: message, read_ct: read_ct,
                            headers: headers, enqueued_at: enqueued_at)
  end

  def build_job_double(job_class: "TestJob", queue_name: "default", job_id: SecureRandom.uuid)
    double("ActiveJob").tap do |job|
      allow(job).to receive_messages(
        queue_name: queue_name,
        job_id: job_id,
        scheduled_at: nil,
        provider_job_id: nil,
        serialize: { "job_class" => job_class, "job_id" => job_id, "queue_name" => queue_name, "arguments" => [] }
      )
      allow(job).to receive(:provider_job_id=)
      allow(job).to receive(:perform_now)
    end
  end
end

RSpec.configure do |config|
  config.include PgmqDoubles
end
