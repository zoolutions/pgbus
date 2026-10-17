# frozen_string_literal: true

require "digest"
require "json"

module Pgbus
  class Workflow
    # Every Pgbus::Workflow subclass, registered from `inherited` (what the
    # insights page lists). The list is a frozen Array replaced whole under a
    # lock built at load (docs/concurrency.md): a reader never sees it change
    # under it, and two classes defined at once are both kept.
    #
    # Classes are kept as objects, not names: `Class.new(Pgbus::Workflow)`
    # has no name yet when `inherited` runs. Names are read when listing, and
    # a name defined again (code reloading) lists its latest class only.
    module Registry
      @lock = Mutex.new
      @classes = [].freeze

      class << self
        def register(klass)
          @lock.synchronize { @classes = merged(@classes, klass) }
          klass
        end

        def classes
          named = @lock.synchronize { @classes }.select(&:name)
          named.reverse.uniq(&:name).reverse
        end

        # Specs only.
        def reset!
          @lock.synchronize { @classes = [].freeze }
        end

        # SHA256 of the ordered [name, kind] pairs of a Steps list.
        def digest(steps)
          Digest::SHA256.hexdigest(JSON.generate(steps.digest_pairs))
        end

        private

        # A class registered again by name (code reloading) replaces the old
        # one, so reloads do not accumulate. Anonymous classes are kept until
        # they get a name; classes lists them then.
        def merged(list, klass)
          name = klass.name
          (list.reject { |existing| existing.equal?(klass) || (name && existing.name == name) } << klass).freeze
        end
      end
    end
  end
end
