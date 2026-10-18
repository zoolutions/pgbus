# frozen_string_literal: true

module Pgbus
  class Workflow
    # Signals (issue #541): what a wait_for: step waits for.
    #
    # deliver (signal!) stores the signal under the run's row lock and, when
    # the run's current step is waiting for a signal it matches, claims it
    # for that step and wakes it. Otherwise it stays pending (buffered).
    #
    # gate runs in the StepJob after the claim and the guards, under the same
    # lock: a signal this step already holds (woken by signal!, or claimed by
    # an earlier attempt that reattempted or paused on a failure), else the
    # oldest pending one its matcher takes, else the step parks as waiting.
    # The guards run first; woken? and signal_ready? (issue #572) show them
    # what the gate holds and would take, through SignalPeek, claiming nothing.
    #
    # A claimed signal is consumed when its step ends, except when that
    # attempt reattempts or fails into a pause: the next attempt runs with it.
    module Signals
      module_function

      # Returns [record, signal]. A redelivery with a stored idempotency key
      # returns the stored signal (duplicate_delivery? true) and does nothing
      # else. A blank key is no key: "" would otherwise dedup every signal
      # sent with it against the first.
      def deliver(workflow_id, name, payload:, idempotency_key:, actor:, reason: nil)
        name = signal_name!(name)
        serialized = Payload.dump(payload || {}, what: "payload of signal #{name}")
        key = idempotency_key.to_s.presence
        Runner.locked(workflow_id) do |record, notes|
          raise ActiveRecord::RecordNotFound, "workflow #{workflow_id} is gone" unless record

          States.check!(:signal, record.status)
          signal = WorkflowSignal.insert_signal!(workflow_id: workflow_id, name: name, payload: serialized, idempotency_key: key)
          signal ||= duplicate(record, key)
          received!(record, signal, actor: actor, reason: reason, notes: notes)
          [record, signal]
        end
      end

      # The gate of a wait_for: step. True when the body runs (with a signal,
      # or timed out), false when the step parked. Sets received_signal,
      # timed_out?, woken? and signal_ready? on +workflow+. Which signal it
      # takes is SignalPeek#ready: given the attempt's +peek+ (the StepJob
      # passes the one its guards read), the very answer signal_ready? gave.
      def gate(record, step, definition, workflow, now: Time.current, peek: nil)
        peek ||= SignalPeek.new(record, step, definition, now: now)
        if peek.timed_out?
          workflow.send(:enter_wait, signal: nil, timed_out: true)
          return true
        end

        signal = peek.ready
        return park(record, step, definition, workflow, now) unless signal

        woken = !peek.held.nil?
        claim!(record, step, signal, actor: Runner::SYSTEM_ACTOR) if signal.status == "pending"
        step.update_columns(signal_id: signal.id) unless step.signal_id == signal.id
        workflow.send(:enter_wait, signal: signal, timed_out: false, woken: woken)
        true
      end

      # An attempt that ended before its gate (cancel_if, skip_if, a guard
      # that raised, a removed_step) records +held+, the signal the step
      # holds, on its row, so settle treats it as for any attempt: consumed
      # when the step ends, kept when it reattempts or fails into a pause. A
      # post-reattempt row carries no signal_id until its gate runs. A
      # pending signal stays pending: no step took it, and a later wait_for:
      # step may.
      def adopt_held(step, held)
        step.update_columns(signal_id: held.id) if held && step.signal_id != held.id
      end

      # The step's attempt ended (StepOutcome.apply): consume the signal it
      # holds, unless the next attempt runs with it.
      def settle(step, status, column, now:)
        return unless step.respond_to?(:signal_id) && step.signal_id
        return if column == "reattempted" || (status == "failed" && column == "paused")

        WorkflowSignal.consume!(step.signal_id, now: now)
      end

      def signal_name!(name)
        text = name.to_s.strip
        raise ArgumentError, "signal! needs a signal name, got #{name.inspect}" if text.empty?

        -text
      end

      def duplicate(record, key)
        WorkflowSignal.find_by!(workflow_id: record.workflow_id, idempotency_key: key).tap do |signal|
          signal.duplicate_delivery = true
        end
      end

      def received!(record, signal, actor:, notes:, reason: nil)
        duplicate = signal.duplicate_delivery?
        Runner.event!(record, "signal_received", actor: actor, reason: reason,
                                                 details: { name: signal.name, idempotency_key: signal.idempotency_key,
                                                            signal_id: signal.id, duplicate: duplicate }.compact)
        woke = !duplicate && wake_waiting(record, signal, actor: actor)
        notes.instrument("pgbus.workflow.signal",
                         Runner.payload_for(record, name: signal.name, duplicate: duplicate, woke: woke))
      end

      def wake_waiting(record, signal, actor:) # rubocop:disable Naming/PredicateMethod -- true when it woke the step
        step = WorkflowStep.latest_for(record.workflow_id)
        return false unless step&.status == "waiting" && step.batch_id.nil?

        definition = record.workflow_klass&.workflow_steps&.fetch(step.step_name)
        return false unless definition&.waits_for_signal? && definition.wait_for.matches?(signal)

        claim!(record, step, signal, actor: actor)
        Waiting.wake!(record, step, now: Time.current, signal_id: signal.id)
        true
      end

      def claim!(record, step, signal, actor:)
        now = Time.current
        WorkflowSignal.claim!(signal.id, step_id: step.id, now: now)
        signal.assign_attributes(status: "claimed", claimed_by_step_id: step.id, claimed_at: now)
        Runner.event!(record, "signal_claimed", actor: actor, step: step, details: { name: signal.name, signal_id: signal.id })
      end

      # The oldest pending signal +matcher+ takes. A SignalMatcher narrows the
      # query to its name; any other matcher sees every pending signal.
      def match(record, matcher)
        name = matcher.respond_to?(:signal_name) ? matcher.signal_name : nil
        WorkflowSignal.pending_for(record.workflow_id, name: name).find_each(batch_size: 100) do |signal|
          return signal if matcher.matches?(signal)
        end
        nil
      end

      # A per-run wait_timeout: is evaluated here, before Waiting.park! writes
      # the waiting state: one that raises (an invalid return raises
      # ArgumentError) leaves the attempt in_progress for the step's exception
      # policies; nil parks it without a deadline.
      def park(record, step, definition, workflow, now) # rubocop:disable Naming/PredicateMethod -- the gate's false: the body does not run
        timeout_at = definition.timeout_at_for(workflow, now)
        Waiting.park!(record, step, now: now, wait_for: definition.wait_for_name, timeout_at: timeout_at)
        false
      end

      # This attempt waited, holds no signal, and its wait_timeout passed: the
      # sweep woke it with on_timeout: :run (SignalPeek#timed_out?).
      def timed_out?(step, now)
        !step.waiting_since.nil? && step.signal_id.nil? && !step.timeout_at.nil? && step.timeout_at <= now + 1
      end
    end
  end
end
