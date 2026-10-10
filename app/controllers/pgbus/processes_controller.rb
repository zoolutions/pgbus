# frozen_string_literal: true

module Pgbus
  class ProcessesController < ApplicationController
    include ProcessListing

    def index
      load_processes
      render_frame("pgbus/processes/processes_table") if params[:frame] == "list"
    end
  end
end
