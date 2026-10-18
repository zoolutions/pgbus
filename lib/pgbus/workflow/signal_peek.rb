# frozen_string_literal: true

module Pgbus
  class Workflow
    # What a wait_for: step's attempt holds and what its gate would run it
    # with (issue #572), read without claiming anything. The StepJob builds
    # one per attempt: the step readers woken? and signal_ready? read it in
    # the guards, and the gate decides with the same one, so the gate takes
    # exactly the signal signal_ready? promised. All of it runs under the
    # run's row lock, so each answer is looked up (and a matcher run) once.
    class SignalPeek
      def initialize(record, step, definition, now: Time.current)
        @record = record
        @step = step
        @definition = definition
        @now = now
      end

      # The claimed signal the step holds (signal! woke it, or an earlier
      # attempt took one and reattempted or paused), or nil. Always nil for a
      # step without wait_for:.
      def held
        return @held if defined?(@held)

        @held = waits? ? WorkflowSignal.held_by_step(@record.workflow_id, @step.step_name) : nil
      end

      # The sweep woke this attempt with on_timeout: :run: its body runs
      # without a signal (Signals.timed_out?).
      def timed_out? = Signals.timed_out?(@step, @now)

      # The signal the gate runs this attempt with: the one the step holds,
      # else the oldest pending one its matcher takes. nil when the wait timed
      # out (the body runs without one), when nothing matches (the step
      # parks), and for a step without wait_for:.
      def ready
        return @ready if defined?(@ready)

        @ready = waits? && !timed_out? ? held || Signals.match(@record, @definition.wait_for) : nil
      end

      private

      def waits? = @definition.waits_for_signal?
    end
  end
end
