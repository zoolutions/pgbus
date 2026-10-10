# frozen_string_literal: true

module Pgbus
  class OutboxController < ApplicationController
    def index
      @stats = data_source.outbox_stats
      @page = page_param
      @per_page = per_page
      @entries = data_source.outbox_entries(page: @page, per_page: @per_page)
    end
  end
end
