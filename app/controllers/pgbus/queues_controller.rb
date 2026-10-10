# frozen_string_literal: true

module Pgbus
  class QueuesController < ApplicationController
    def index
      @queues = data_source.queues_with_metrics
    end

    include JobListing

    def show
      name = params[:name]
      @queue = data_source.queue_detail(name)
      redirect_to queues_path, alert: "Queue not found." and return unless @queue

      @dlq = name.end_with?(Pgbus::DEAD_LETTER_SUFFIX)
      # The unified list leaves DLQ tables out (they are the Dead Letter page's).
      unless @dlq
        load_job_list(queue_name: name, list_path: ->(extra) { queue_path({ name: name }.merge(extra)) })
        render_frame("pgbus/jobs/list") and return if params[:frame] == "list"
      end

      @paused = data_source.queue_paused?(name)
      @pause_state = data_source.queue_pause_state(name)
      @drainers = data_source.queue_drainers(name)
      @summary = Web::QueueSummary.present(@queue, @pause_state, @drainers,
                                           max_retries: Pgbus.configuration.max_retries)
      @health = data_source.queue_health_detail(name)
    end

    def purge
      data_source.purge_queue(params[:name])
      redirect_to queue_path(name: params[:name]), notice: "Queue purged."
    end

    def destroy
      data_source.drop_queue(params[:name])
      redirect_to queues_path, notice: t("pgbus.queues.destroy.success", name: params[:name])
    end

    def pause
      data_source.pause_queue(params[:name], reason: params[:reason])
      redirect_to queue_path(name: params[:name]), notice: "Queue paused."
    end

    def resume
      data_source.resume_queue(params[:name])
      redirect_to queue_path(name: params[:name]), notice: "Queue resumed."
    end

    def retry_message
      if data_source.retry_job(params[:name], params[:msg_id])
        redirect_back fallback_location: queue_path(name: params[:name]), notice: t("pgbus.queues.show.message_retried")
      else
        redirect_back fallback_location: queue_path(name: params[:name]), alert: t("pgbus.queues.show.message_retry_failed")
      end
    end

    def discard_message
      if data_source.discard_job(params[:name], params[:msg_id])
        redirect_back fallback_location: queue_path(name: params[:name]), notice: t("pgbus.queues.show.message_discarded")
      else
        redirect_back fallback_location: queue_path(name: params[:name]), alert: t("pgbus.queues.show.message_discard_failed")
      end
    end
  end
end
