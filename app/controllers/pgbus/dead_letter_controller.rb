# frozen_string_literal: true

module Pgbus
  class DeadLetterController < ApplicationController
    ERROR_CLASS_MAX = 200

    def index
      @page = page_param
      @per_page = per_page
      @dlq = dlq_param
      @error_class = error_class_param
      filters = { dlq: @dlq, error_class: @error_class }
      @messages = data_source.dlq_messages(page: @page, per_page: @per_page, **filters)
      @total_count = data_source.dlq_total_count(**filters)
      @total_pages = (@total_count.to_f / @per_page).ceil
      @dlq_counts = data_source.dlq_counts_by_queue
      @dlq_rows = @messages.map { |m| [m, Pgbus::Web::DeadLetterReason.present(m)] }
      # One place builds every list URL (frame source, pager, chips), so the
      # filters survive refresh and paging.
      @list_path = ->(extra = {}) { pgbus.dead_letter_index_path(filters.merge(extra).compact) }
      render_frame("pgbus/dead_letter/messages_table") if params[:frame] == "list"
    end

    def show
      @message = data_source.dlq_message_detail(params[:id].to_i)
      @reason = Pgbus::Web::DeadLetterReason.present(@message) if @message
    end

    def retry
      queue_name = params[:queue_name].to_s
      return redirect_to dead_letter_index_path, alert: "Invalid DLQ queue." unless queue_name.end_with?(Pgbus::DEAD_LETTER_SUFFIX)

      if data_source.retry_dlq_message(queue_name, params[:id])
        redirect_to dead_letter_index_path, notice: "Message re-enqueued to original queue."
      else
        redirect_to dead_letter_index_path, alert: "Could not retry message."
      end
    end

    def discard
      queue_name = params[:queue_name].to_s
      return redirect_to dead_letter_index_path, alert: "Invalid DLQ queue." unless queue_name.end_with?(Pgbus::DEAD_LETTER_SUFFIX)

      if data_source.discard_dlq_message(queue_name, params[:id])
        redirect_to dead_letter_index_path, notice: "Message discarded."
      else
        redirect_to dead_letter_index_path, alert: "Could not discard message."
      end
    end

    def retry_all
      count = data_source.retry_all_dlq
      redirect_to dead_letter_index_path, notice: "Re-enqueued #{count} DLQ messages."
    end

    def discard_all
      count = data_source.discard_all_dlq
      redirect_to dead_letter_index_path, notice: "Discarded #{count} DLQ messages."
    end

    def discard_selected
      selections = Array(params[:messages]).reject { |s| s[:queue_name].blank? || s[:msg_id].blank? }
      if selections.empty?
        redirect_to dead_letter_index_path, alert: t("pgbus.dead_letter.index.none_selected")
        return
      end

      count = 0
      selections.each do |sel|
        queue_name = sel[:queue_name].to_s
        next unless queue_name.end_with?(Pgbus::DEAD_LETTER_SUFFIX)

        count += 1 if data_source.discard_dlq_message(queue_name, sel[:msg_id])
      end
      redirect_to dead_letter_index_path, notice: t("pgbus.dead_letter.index.discarded_selected", count: count)
    end

    private

    # A full DLQ name or nil; the data source also checks it is a known DLQ.
    def dlq_param
      name = params[:dlq].to_s
      name if name.end_with?(Pgbus::DEAD_LETTER_SUFFIX) && name.match?(/\A\w+\z/)
    end

    def error_class_param
      value = params[:error_class].to_s
      value if value.present? && value.length <= ERROR_CLASS_MAX
    end
  end
end
