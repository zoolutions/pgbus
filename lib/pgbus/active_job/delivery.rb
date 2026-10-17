# frozen_string_literal: true

module Pgbus
  module ActiveJob
    # The PGMQ delivery of the job running in this fiber:
    #
    #   Pgbus::ActiveJob::Delivery.current # => { msg_id: 42, read_ct: 2, queue_name: "default" }
    #
    # Set by Executor#execute around perform_now and cleared after, also when
    # the job raises; nil outside the executor (perform_now in a test, the
    # inline adapter). Fiber-local (Thread.current[]), so concurrent jobs under
    # execution_mode: :async each see their own. Pgbus::Workflow::StepJob reads
    # read_ct to tell a redelivery after a worker died from a first delivery.
    #
    # The three values sit in three fiber-locals and the Hash is built only
    # when read: setting them allocates nothing, which keeps the executor's
    # per-job allocation budget (spec/pgbus/active_job/executor_allocation_budget_spec.rb).
    module Delivery
      MSG_ID_KEY = :pgbus_delivery_msg_id
      READ_CT_KEY = :pgbus_delivery_read_ct
      QUEUE_NAME_KEY = :pgbus_delivery_queue_name

      module_function

      def current
        msg_id = Thread.current[MSG_ID_KEY]
        return nil if msg_id.nil?

        { msg_id: msg_id, read_ct: Thread.current[READ_CT_KEY], queue_name: Thread.current[QUEUE_NAME_KEY] }
      end

      def with(msg_id:, read_ct:, queue_name:)
        previous_msg_id = Thread.current[MSG_ID_KEY]
        previous_read_ct = Thread.current[READ_CT_KEY]
        previous_queue_name = Thread.current[QUEUE_NAME_KEY]
        Thread.current[MSG_ID_KEY] = msg_id
        Thread.current[READ_CT_KEY] = read_ct
        Thread.current[QUEUE_NAME_KEY] = queue_name
        yield
      ensure
        Thread.current[MSG_ID_KEY] = previous_msg_id
        Thread.current[READ_CT_KEY] = previous_read_ct
        Thread.current[QUEUE_NAME_KEY] = previous_queue_name
      end
    end
  end
end
