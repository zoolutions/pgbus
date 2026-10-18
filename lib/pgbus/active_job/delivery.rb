# frozen_string_literal: true

module Pgbus
  module ActiveJob
    # The PGMQ delivery of the job running in this fiber:
    #
    #   Pgbus::ActiveJob::Delivery.current
    #   # => { msg_id: 42, read_ct: 2, queue_name: "default", enqueued_at: "2026-10-18 10:30:00+00" }
    #
    # Set by Executor#execute around perform_now and cleared after, also when
    # the job raises; nil outside the executor (perform_now in a test, a
    # top-level inline-adapter job). A job run inline from inside an executing
    # job sees the enclosing job's delivery; StepJob only acts on it when the
    # step row's msg_id matches, so that is harmless. Fiber-local (Thread.current[]), so concurrent jobs under
    # execution_mode: :async each see their own. Pgbus::Workflow::StepJob reads
    # read_ct to tell a redelivery after a worker died from a first delivery;
    # the adapter reads the message identity and enqueued_at to move the
    # job's uniqueness lock onto a retry the job enqueues (issue #564).
    #
    # The values sit in one fiber-local each and the Hash is built only
    # when read: setting them allocates nothing, which keeps the executor's
    # per-job allocation budget (spec/pgbus/active_job/executor_allocation_budget_spec.rb).
    module Delivery
      MSG_ID_KEY = :pgbus_delivery_msg_id
      READ_CT_KEY = :pgbus_delivery_read_ct
      QUEUE_NAME_KEY = :pgbus_delivery_queue_name
      ENQUEUED_AT_KEY = :pgbus_delivery_enqueued_at

      module_function

      def current
        msg_id = Thread.current[MSG_ID_KEY]
        return nil if msg_id.nil?

        { msg_id: msg_id, read_ct: Thread.current[READ_CT_KEY], queue_name: Thread.current[QUEUE_NAME_KEY],
          enqueued_at: Thread.current[ENQUEUED_AT_KEY] }
      end

      def with(msg_id:, read_ct:, queue_name:, enqueued_at: nil)
        previous_msg_id = Thread.current[MSG_ID_KEY]
        previous_read_ct = Thread.current[READ_CT_KEY]
        previous_queue_name = Thread.current[QUEUE_NAME_KEY]
        previous_enqueued_at = Thread.current[ENQUEUED_AT_KEY]
        Thread.current[MSG_ID_KEY] = msg_id
        Thread.current[READ_CT_KEY] = read_ct
        Thread.current[QUEUE_NAME_KEY] = queue_name
        Thread.current[ENQUEUED_AT_KEY] = enqueued_at
        yield
      ensure
        Thread.current[MSG_ID_KEY] = previous_msg_id
        Thread.current[READ_CT_KEY] = previous_read_ct
        Thread.current[QUEUE_NAME_KEY] = previous_queue_name
        Thread.current[ENQUEUED_AT_KEY] = previous_enqueued_at
      end
    end
  end
end
