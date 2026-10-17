# frozen_string_literal: true

module Pgbus
  class Configuration
    # The settings a non-main Ractor reads, as one Ractor-shareable value
    # (issue #527). The configuration itself stays on the main Ractor: it holds
    # the logger and the app's callables. Pgbus.finalize! and Pgbus.configure
    # publish a snapshot of it as Pgbus.configuration_snapshot, which any
    # Ractor can read.
    #
    # Only what pgbus's Ractor-safe code reads is here: the queue_prefix, for
    # the stream-key budget (Streams::Key). A setting joins when such code
    # needs it.
    Snapshot = Data.define(:queue_prefix) do
      # A shareable snapshot of +configuration+. The configuration's own
      # values are copied, never frozen in place. Nil when a setting is not
      # plain data (Shareable.data?), which only a configuration that skipped
      # validation can hold: the caller keeps the snapshot it has.
      def self.of(configuration)
        prefix = configuration.queue_prefix
        return unless Shareable.data?(prefix)

        new(queue_prefix: Shareable.freeze_copy(prefix))
      end
    end
  end
end
