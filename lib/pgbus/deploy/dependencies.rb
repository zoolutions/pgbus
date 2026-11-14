# frozen_string_literal: true

require "tsort"

module Pgbus
  module Deploy
    # after: (issue #660): which declared tasks can wait on which, and whether
    # a task's dependencies have finished.
    #
    # A dependency is satisfied by a completed run (rollover or manual) at its
    # current version, at any revision; an every_deploy dependency needs one on
    # this revision. A run before the outcomes migration has no outcome, so it
    # satisfies nothing.
    class Dependencies
      NOT_RUN = "not_run"
      RUNNING = "running"
      FAILED = "failed"

      # +tasks+ (valid Deploy::Task objects) minus those whose after: can
      # never be met: an unknown key, a cycle, or a dependency that is itself
      # skipped. Returns [kept tasks, { skipped key => reason }].
      def self.prune(tasks, declared_keys)
        valid = tasks.to_h { |task| [task.key, task] }
        skipped = {}
        loop do
          unmet = valid.values.filter_map { |task| (reason = unmet(task, valid, declared_keys)) && [task.key, reason] }
          unmet = cycles(valid) if unmet.empty?
          break if unmet.empty?

          unmet.each do |key, reason|
            skipped[key] = reason
            valid.delete(key)
          end
        end
        [valid.values, skipped]
      end

      def self.unmet(task, valid, declared_keys)
        missing = task.dependencies.find { |dep| !valid.key?(dep) }
        return unless missing

        declared_keys.include?(missing) ? "after: '#{missing}' is skipped" : "after: unknown task '#{missing}'"
      end

      # [key, reason] for every task on a cycle (Task#valid? already rejects
      # a task naming itself).
      def self.cycles(valid)
        graph = valid.transform_values { |task| task.dependencies.select { |dep| valid.key?(dep) }.sort }
        components = TSort.strongly_connected_components(graph.method(:each_key), ->(key, &blk) { graph[key].each(&blk) })
        components.select { |keys| keys.size > 1 }.flat_map do |keys|
          reason = "after: cycle #{cycle_path(keys.min, graph, keys.to_set).join(" → ")}"
          keys.sort.map { |key| [key, reason] }
        end
      end

      # A path from +start+ back to itself inside its strongly connected
      # component, which always has one.
      def self.cycle_path(start, graph, members)
        stack = [[start, [start]]]
        seen = Set[start]
        until stack.empty?
          key, path = stack.pop
          graph[key].each do |dep|
            next unless members.include?(dep)
            return path + [start] if dep == start
            next unless seen.add?(dep)

            stack << [dep, path + [dep]]
          end
        end
      end
      private_class_method :unmet, :cycles, :cycle_path

      # The outcomes of +dependencies+ (anything with key, version and
      # every_deploy: Deploy::Task or DeployTask), in one query.
      def self.load(dependencies, revision:)
        new(DeployTaskRun.outcomes_for(dependencies.map(&:key).uniq), revision: revision)
      end

      def initialize(outcomes, revision:)
        @outcomes = outcomes.group_by(&:task_key)
        @revision = revision
      end

      # nil when +dependency+ is satisfied, else NOT_RUN, RUNNING (the newest
      # run at its version has no outcome yet) or FAILED.
      def state(dependency)
        current = @outcomes.fetch(dependency.key, []).select { |outcome| current?(dependency, outcome) }
        return if current.any?(&:completed)
        return NOT_RUN if current.empty?

        current.max_by(&:latest_id).latest_failed ? FAILED : RUNNING
      end

      # [{ key:, state: }] for each of +keys+ (a task's after:) that is not
      # satisfied. +by_key+ maps a key to its dependency; a key missing from
      # it (no longer declared) reads as not run.
      def waiting_on(keys, by_key)
        keys.filter_map do |key|
          dependency = by_key[key]
          state = dependency ? state(dependency) : NOT_RUN
          { key: key, state: state } if state
        end
      end

      private

      def current?(dependency, outcome)
        return !@revision.nil? && outcome.revision == @revision if dependency.every_deploy

        outcome.version == dependency.version.to_s
      end
    end
  end
end
