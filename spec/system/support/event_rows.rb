# frozen_string_literal: true

# system_helper does not load spec/support; the Events builder lives there so
# request specs share it.
require_relative "../../support/event_row_builder"
