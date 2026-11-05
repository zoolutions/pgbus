# frozen_string_literal: true

module RuboCop
  module Cop
    module Pgbus
      # Flags an ActiveRecord lease nobody returns on pgbus's job path (issue
      # #649). The executor's bookkeeping (uniqueness, concurrency, batches,
      # failed events, job stats) runs after the Rails executor gave the job's
      # lease back, so a bare `Model.connection` there pins a connection to
      # the pool thread for good, or leaves it on a dead fiber for the reaper
      # to reset. The only sanctioned handle is the block parameter of
      # `connection_pool.with_connection`: it reuses a lease the caller holds
      # and otherwise returns the one it took.
      #
      # - `connection` on no receiver, `self`, a constant or `connection_pool`, and
      #   `lease_connection` on any receiver (`connection_pool.lease_connection`
      #   too): always an offense, inside `with_connection` too, since on
      #   Rails 7.2+ it marks the lease sticky and `with_connection` then keeps
      #   it.
      # - `transaction` on no receiver, `self` or a constant whose nearest
      #   enclosing block (within the method) is not
      #   `connection_pool.with_connection`: on Rails 7.1 it leases through
      #   `connection`.
      #
      # Scoped by .rubocop.yml to the files on the job path. The dashboard is
      # left out (the Rails executor returns a web request's lease), and so
      # is the workflow runner, a later sweep. Without an Include the cop does
      # nothing: the nested docs app loads pgbus's cops through the ancestor
      # config but not that list, and its Phlex helpers (`transaction(s)` in a
      # diagram) are not ActiveRecord.
      #
      # @example
      #   # bad
      #   connection.exec_query(sql, "Name", binds)
      #   Pgbus::Semaphore.transaction { release(key) }
      #
      #   # good
      #   connection_pool.with_connection { |conn| conn.exec_query(sql, "Name", binds) }
      #   Pgbus::Semaphore.connection_pool.with_connection do
      #     Pgbus::Semaphore.transaction { release(key) }
      #   end
      class BareConnectionLease < Base
        LEASE_MSG = "Use the block parameter of `connection_pool.with_connection`, never `%<method>s`: " \
                    "a bare lease is never returned (issue #649)."
        TRANSACTION_MSG = "Wrap `transaction` in `connection_pool.with_connection`: on Rails 7.1 it " \
                          "leases through `connection` and nothing returns it (issue #649)."

        RESTRICT_ON_SEND = %i[connection lease_connection transaction].freeze

        def on_send(node)
          return unless cop_config.key?("Include")
          # lease_connection is permanent on any receiver, a pool's included.
          return lease_offense(node) if node.method?(:lease_connection)
          return unless bare_receiver?(node)

          if node.method?(:transaction)
            add_offense(node.loc.selector, message: TRANSACTION_MSG) unless inside_with_connection?(node)
          else
            add_offense(node.loc.selector, message: format(LEASE_MSG, method: node.method_name))
          end
        end
        alias on_csend on_send

        private

        def lease_offense(node)
          add_offense(node.loc.selector, message: format(LEASE_MSG, method: node.method_name))
        end

        # A pool's own `connection` (Rails 7.1's lease) counts as bare too.
        def bare_receiver?(node)
          receiver = node.receiver
          return true if receiver.nil? || receiver.self_type? || receiver.const_type?

          node.method?(:connection) && receiver.send_type? && receiver.method?(:connection_pool)
        end

        def inside_with_connection?(node)
          start = node.block_node || node
          start.each_ancestor do |ancestor|
            return false if ancestor.type?(:def, :defs)
            return with_connection_block?(ancestor) if ancestor.any_block_type?
          end
          false
        end

        def with_connection_block?(block)
          call = block.send_node
          return false unless call.method?(:with_connection)

          pool = call.receiver
          pool&.send_type? && pool.method?(:connection_pool)
        end
      end
    end
  end
end
