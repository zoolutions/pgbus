# frozen_string_literal: true

# Forces the check-then-act race a lazy memo or a check-then-install has
# (issue #527): the first racer to reach +receiver.method+ is held there until
# a second racer reaches it too, so unguarded code lets both through every
# run. Code that takes a lock first never lets the second racer get there; the
# first then goes on after +hold+ seconds (a hang guard, not an assertion).
# Only threads started by #race are held.
module FirstCallRace
  RACER = :pgbus_first_call_racer

  def hold_first_call(receiver, method, hold: 1)
    count_lock = Mutex.new
    count = 0
    second = Queue.new
    allow(receiver).to receive(method).and_wrap_original do |original, *args, **kwargs, &block|
      if Thread.current[RACER]
        mine = count_lock.synchronize { count += 1 }
        second.pop(timeout: hold) if mine == 1
        second << true if mine == 2
      end
      original.call(*args, **kwargs, &block)
    end
  end

  # Runs the block on +threads+ threads released at once; returns their values.
  def race(threads: 2)
    start = Queue.new
    racers = Array.new(threads) do
      Thread.new do
        Thread.current[RACER] = true
        start.pop
        yield
      end
    end
    threads.times { start << true }
    racers.map(&:value)
  end
end

RSpec.configure { |config| config.include FirstCallRace, :first_call_race }
