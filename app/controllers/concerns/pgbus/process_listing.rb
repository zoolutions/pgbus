# frozen_string_literal: true

module Pgbus
  # Loads the process rows and their words (issue #493) for the Processes
  # page and the home page's panel, so both order and describe them alike.
  module ProcessListing
    private

    def load_processes
      thresholds = data_source.process_thresholds
      rows = data_source.processes(thresholds: thresholds)
      context = data_source.process_context(rows: rows, thresholds: thresholds)
      @processes = Web::ProcessState.order(rows, context)
      @process_states = @processes.to_h { |p| [p[:id], Web::ProcessState.present(p, context)] }
    end
  end
end
