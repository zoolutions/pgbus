# frozen_string_literal: true

module Pgbus
  # Loads the process rows and their words (issue #493) for the Processes
  # page and the home page's panel, so both order and describe them alike.
  module ProcessListing
    private

    def load_processes
      rows = data_source.processes
      context = data_source.process_context(rows: rows)
      @processes = Web::ProcessState.order(rows, context)
      @process_states = @processes.to_h { |p| [p[:id], Web::ProcessState.present(p, context)] }
    end
  end
end
