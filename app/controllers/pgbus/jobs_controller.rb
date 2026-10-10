# frozen_string_literal: true

module Pgbus
  class JobsController < ApplicationController
    include JobListing

    def index
      @queue = params[:queue].presence
      load_job_list(queue_name: @queue, list_path: ->(extra) { jobs_path({ queue: @queue }.merge(extra)) })
      @failed_total = @queue ? data_source.failed_events_count : @counts["retrying"]
      render_frame("pgbus/jobs/list") if params[:frame] == "list"
    end

    def show
      @job = data_source.failed_event(params[:id])
    end

    def retry
      if data_source.retry_failed_event(params[:id])
        redirect_back fallback_location: jobs_path, notice: "Job re-enqueued."
      else
        redirect_back fallback_location: jobs_path, alert: "Could not retry job."
      end
    end

    def discard
      if data_source.discard_failed_event(params[:id])
        redirect_back fallback_location: jobs_path, notice: "Job discarded."
      else
        redirect_back fallback_location: jobs_path, alert: "Could not discard job."
      end
    end

    def retry_all
      count = data_source.retry_all_failed
      redirect_to jobs_path, notice: "Re-enqueued #{count} jobs."
    end

    def discard_all
      count = data_source.discard_all_failed
      redirect_to jobs_path, notice: "Discarded #{count} jobs."
    end

    def discard_all_enqueued
      count = data_source.discard_all_enqueued
      redirect_back fallback_location: jobs_path, notice: t("pgbus.jobs.index.discard_all_enqueued_notice", count: count)
    end

    def discard_selected_failed
      ids = Array(params[:ids]).map(&:to_i).reject(&:zero?)
      if ids.empty?
        redirect_to jobs_path, alert: t("pgbus.jobs.index.none_selected")
        return
      end

      count = 0
      ids.each do |id|
        count += 1 if data_source.discard_failed_event(id)
      end
      redirect_to jobs_path, notice: t("pgbus.jobs.index.discarded_selected", count: count)
    end

    # The unified list posts every selected row here: queue messages as
    # messages[], rows backed by a failed event (retrying, orphaned) as ids[]
    # so discarding them also clears the failed-event row.
    def discard_selected_enqueued
      selections = selected_messages
      ids = Array(params[:ids]).map(&:to_i).reject(&:zero?)
      if selections.empty? && ids.empty?
        redirect_back fallback_location: jobs_path, alert: t("pgbus.jobs.index.none_selected")
        return
      end

      count = ids.count { |id| data_source.discard_failed_event(id) }
      count += selections.count { |sel| data_source.discard_job(sel[:queue_name], sel[:msg_id]) }
      redirect_back fallback_location: jobs_path, notice: t("pgbus.jobs.index.discarded_selected", count: count)
    end

    private

    def selected_messages
      Array(params[:messages]).filter_map do |s|
        next unless s.respond_to?(:[])

        queue_name = s[:queue_name]
        msg_id = s[:msg_id]
        next if queue_name.blank? || msg_id.blank?

        { queue_name: queue_name, msg_id: msg_id }
      end
    end
  end
end
