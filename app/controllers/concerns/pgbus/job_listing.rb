# frozen_string_literal: true

module Pgbus
  # Loads the unified Jobs list (issue #489) for any page that shows it: the
  # Jobs page across every queue, a queue page for one physical queue (#491).
  module JobListing
    # Tabs of the unified list; "all" is no filter.
    STATES = ["all", *Web::JobState::STATES].freeze

    private

    # list_path: ->(extra) { url } for the tabs, the pager and the frame's
    # auto-refresh source, so each page keeps them on its own URL. extra is
    # symbol-keyed, never request.query_parameters (string keys would repeat
    # a param in the URL).
    #
    # exclude: physical queues to leave out (the Jobs page leaves the EventBus
    # handler queues to the Events page, issue #494).
    def load_job_list(queue_name:, list_path:, exclude: nil)
      @state = job_state_param
      @page = page_param
      @per_page = per_page
      @rows = data_source.job_rows(state: (@state unless @state == "all"), queue_name: queue_name, exclude: exclude,
                                   page: @page, per_page: @per_page)
      @counts = data_source.job_state_counts(queue_name: queue_name, exclude: exclude)
      @ahead = data_source.jobs_ahead(@rows)
      @state_context = data_source.job_list_context
      @list_scoped = !queue_name.nil?
      @list_path = ->(extra) { list_path.call({ state: (@state unless @state == "all") }.merge(extra)) }
    end

    def job_state_param
      state = params[:state].presence || ("retrying" if params[:status] == "failed")
      STATES.include?(state) ? state : "all"
    end
  end
end
