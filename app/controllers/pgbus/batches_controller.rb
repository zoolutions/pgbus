# frozen_string_literal: true

module Pgbus
  class BatchesController < ApplicationController
    def index
      @batches = data_source.batches
      render_frame("pgbus/batches/batches_table") if params[:frame] == "list"
    end

    def show
      @batch = data_source.batch_detail(params[:id])
      return redirect_to batches_path, alert: t("pgbus.batches.show.not_found") unless @batch

      render_frame("pgbus/batches/progress") if params[:frame] == "progress"
    end
  end
end
