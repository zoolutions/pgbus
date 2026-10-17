# frozen_string_literal: true

module Pgbus
  class Workflow
    # The ordered step list of one workflow class: StepDefinitions and
    # RemovedStep gravestones, in declaration order. Immutable: every
    # change (`step`, `removed_step`) builds a new frozen Steps that the
    # class stores whole, so a subclass extends its parent's list without
    # touching it and a reader never sees a list change under it
    # (docs/concurrency.md).
    class Steps
      attr_reader :entries

      def initialize(entries = [])
        @entries = entries.dup.freeze
        freeze
      end

      EMPTY = new

      def add(definition, before: nil, after: nil)
        raise ArgumentError, "use before_step: or after_step:, not both" if before && after
        raise ArgumentError, "#{definition.name} is a removed step (removed_step); pick another name" if removed?(definition.name)
        raise ArgumentError, "duplicate step #{definition.name}" if fetch(definition.name)

        list = entries.dup
        anchor = before || after
        if anchor
          index = position(anchor)
          raise ArgumentError, "#{before ? "before_step" : "after_step"}: #{anchor.to_sym.inspect} — no step named #{anchor}" unless index

          list.insert(before ? index : index + 1, definition)
        else
          list << definition
        end
        self.class.new(list)
      end

      # Replaces a live step with a gravestone at its position, or appends a
      # gravestone for a step that is already gone from the class.
      def remove(name)
        name = -name.to_s
        raise ArgumentError, "#{name} is already a removed step" if removed?(name)

        list = entries.dup
        index = position(name)
        gravestone = RemovedStep.new(name: name)
        index ? list[index] = gravestone : list << gravestone
        self.class.new(list)
      end

      def definitions = entries.reject(&:removed?)

      def names = definitions.map(&:name)

      def removed_names = entries.select(&:removed?).map(&:name)

      def fetch(name)
        name = name.to_s
        definitions.find { |entry| entry.name == name }
      end

      def removed?(name)
        name = name.to_s
        entries.any? { |entry| entry.removed? && entry.name == name }
      end

      def first = definitions.first

      # The next live step declared after +name+ (a step or a gravestone),
      # or nil when +name+ is the last one or unknown.
      def next_after(name)
        index = position(name)
        return nil unless index

        entries.drop(index + 1).find { |entry| !entry.removed? }
      end

      def empty? = definitions.empty?

      # Name for an anonymous `step { ... }`: step_<n>, the first n from
      # entries.size + 1 not taken by a step or a gravestone.
      def anonymous_name
        number = entries.size + 1
        number += 1 while entries.any? { |entry| entry.name == "step_#{number}" }
        "step_#{number}"
      end

      # What definition_digest hashes: the ordered [name, kind] pairs.
      def digest_pairs = entries.map { |entry| [entry.name, entry.kind] }

      private

      def position(name)
        name = name.to_s
        entries.index { |entry| entry.name == name }
      end
    end
  end
end
