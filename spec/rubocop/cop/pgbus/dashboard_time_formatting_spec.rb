# frozen_string_literal: true

require "spec_helper"
require "rubocop"
# See no_ruby_timeout_spec.rb for why these are required one by one.
require "rubocop/rspec/cop_helper"
require "rubocop/rspec/expect_offense"
require "rubocop/rspec/shared_contexts"
require_relative "../../../../rubocop/pgbus"

RSpec.describe RuboCop::Cop::Pgbus::DashboardTimeFormatting do
  include CopHelper
  include RuboCop::RSpec::ExpectOffense

  include_context "config"

  let(:config) { RuboCop::Config.new }

  it "flags strftime" do
    expect_offense(<<~RUBY)
      time.strftime("%H:%M")
           ^^^^^^^^ Pgbus/DashboardTimeFormatting: Format dashboard times with pgbus_time / pgbus_timestamp / pgbus_absolute_time / pgbus_duration (Pgbus::Web::TimeFormat), never `strftime`.
    RUBY
  end

  it "flags a safe-navigation strftime" do
    expect_offense(<<~RUBY)
      task[:next_run_at]&.strftime("%Y-%m-%d")
                          ^^^^^^^^ Pgbus/DashboardTimeFormatting: Format dashboard times with pgbus_time / pgbus_timestamp / pgbus_absolute_time / pgbus_duration (Pgbus::Web::TimeFormat), never `strftime`.
    RUBY
  end

  it "flags Rails' distance helpers and to_fs" do
    expect_offense(<<~RUBY)
      time_ago_in_words(created_at)
      ^^^^^^^^^^^^^^^^^ Pgbus/DashboardTimeFormatting: Format dashboard times with pgbus_time / pgbus_timestamp / pgbus_absolute_time / pgbus_duration (Pgbus::Web::TimeFormat), never `time_ago_in_words`.
      distance_of_time_in_words(a, b)
      ^^^^^^^^^^^^^^^^^^^^^^^^^ Pgbus/DashboardTimeFormatting: Format dashboard times with pgbus_time / pgbus_timestamp / pgbus_absolute_time / pgbus_duration (Pgbus::Web::TimeFormat), never `distance_of_time_in_words`.
      created_at.to_fs(:short)
                 ^^^^^ Pgbus/DashboardTimeFormatting: Format dashboard times with pgbus_time / pgbus_timestamp / pgbus_absolute_time / pgbus_duration (Pgbus::Web::TimeFormat), never `to_fs`.
    RUBY
  end

  it "flags I18n localize, which formats times outside TimeFormat" do
    expect_offense(<<~RUBY)
      l(created_at, format: :short)
      ^ Pgbus/DashboardTimeFormatting: Format dashboard times with pgbus_time / pgbus_timestamp / pgbus_absolute_time / pgbus_duration (Pgbus::Web::TimeFormat), never `l`.
    RUBY
  end

  it "accepts the pgbus helpers and wire formats" do
    expect_no_offenses(<<~RUBY)
      pgbus_time(row[:vt], clock: true)
      Pgbus::Web::TimeFormat.absolute(value)
      Time.now.utc.iso8601(6)
    RUBY
  end
end
