# frozen_string_literal: true

require "spec_helper"
require "action_view"
require "nokogiri"

require_relative "../../../app/helpers/pgbus/button_helper"

RSpec.describe Pgbus::ButtonHelper do
  subject(:view) do
    Class.new(ActionView::Base) do
      include Pgbus::ButtonHelper

      def protect_against_forgery? = false
    end.empty
  end

  def html(fragment) = Nokogiri::HTML.fragment(fragment)

  # button_to renders <button> or <input type="submit"> depending on the host
  # app's button_to_generates_button_tag default.
  def submit(fragment) = html(fragment).at_css("form [type=submit]")

  def classes(variant, size, **opts) = view.pgbus_button_classes(variant, size, **opts).split

  describe "#pgbus_button_classes" do
    {
      primary: %w[bg-indigo-600 text-white hover:bg-indigo-700],
      danger: %w[bg-red-600 text-white hover:bg-red-700],
      success: %w[bg-green-700 text-white hover:bg-green-800],
      warning: %w[bg-yellow-400 text-yellow-950 hover:bg-yellow-300],
      destructive: %w[bg-red-800 text-white hover:bg-red-900],
      secondary: %w[bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-200 hover:bg-gray-200 dark:hover:bg-gray-600]
    }.each do |variant, tokens|
      it "styles #{variant} with its colours, a focus ring and a 24 px minimum target in every size" do
        %i[sm md].each do |size|
          expect(classes(variant, size)).to include(*tokens, "min-h-6", "inline-flex", "focus-visible:outline-2")
        end
      end
    end

    {
      indigo: %w[text-indigo-600 dark:text-indigo-300 hover:text-indigo-800 dark:hover:text-indigo-200],
      red: %w[text-red-600 dark:text-red-300 hover:text-red-800 dark:hover:text-red-200],
      green: %w[text-green-700 dark:text-green-300 hover:text-green-900 dark:hover:text-green-200],
      yellow: %w[text-yellow-700 dark:text-yellow-300 hover:text-yellow-900 dark:hover:text-yellow-200]
    }.each do |tone, tokens|
      it "styles a #{tone} link with light and dark text and hover colours" do
        expect(classes(:link, :sm, tone: tone)).to include(*tokens, "min-h-6", "text-xs")
      end
    end

    it "pads small links up to the 24 px target without changing their type size" do
      expect(classes(:link, :sm)).to include("min-h-6", "py-1", "text-xs")
      expect(classes(:link, :md)).to include("min-h-6", "text-sm")
    end

    it "sizes buttons" do
      expect(classes(:primary, :sm)).to include("px-3", "py-1.5", "text-xs")
      expect(classes(:primary, :md)).to include("px-3", "py-2", "text-sm")
    end

    it "appends extra classes" do
      expect(view.pgbus_button_classes(:link, :md, extra: "font-mono")).to end_with(" font-mono")
    end

    it "rejects an unknown variant, size or tone" do
      expect { view.pgbus_button_classes(:nope, :md) }.to raise_error(ArgumentError, /variant/)
      expect { view.pgbus_button_classes(:primary, :xl) }.to raise_error(ArgumentError, /size/)
      expect { view.pgbus_button_classes(:link, :md, tone: :blue) }.to raise_error(ArgumentError, /tone/)
    end
  end

  describe "#pgbus_button_to" do
    it "renders a button_to form with the variant classes" do
      button = submit(view.pgbus_button_to("Retry", "/retry", variant: :primary, size: :sm))

      expect(button.text.presence || button["value"]).to eq("Retry")
      expect(button["class"].split).to include("bg-indigo-600", "px-3", "text-xs")
      expect(html(view.pgbus_button_to("Retry", "/retry")).at_css("form")["action"]).to eq("/retry")
    end

    it "emits confirm: as data-turbo-confirm, merged with the caller's data" do
      button = submit(view.pgbus_button_to("Discard", "/d", variant: :danger, confirm: "Sure?",
                                                            data: { turbo_frame: "_top" }))

      expect(button["data-turbo-confirm"]).to eq("Sure?")
      expect(button["data-turbo-frame"]).to eq("_top")
    end

    it "passes method:, params: and form: through to button_to" do
      fragment = html(view.pgbus_button_to("Release", "/r", method: :delete, params: { key: "k" },
                                                            form: { data: { x: "1" } }))

      expect(fragment.at_css("input[name=_method]")["value"]).to eq("delete")
      expect(fragment.at_css("input[name=key]")["value"]).to eq("k")
      expect(fragment.at_css("form")["data-x"]).to eq("1")
    end
  end

  describe "#pgbus_link_to" do
    it "renders an <a> styled as a link by default" do
      link = html(view.pgbus_link_to("Back", "/back", data: { turbo_frame: "_top" })).at_css("a")

      expect(link["href"]).to eq("/back")
      expect(link["data-turbo-frame"]).to eq("_top")
      expect(link["class"].split).to include("text-indigo-600", "dark:text-indigo-300", "min-h-6")
    end

    it "takes a variant and a tone" do
      expect(html(view.pgbus_link_to("Back", "/b", variant: :secondary)).at_css("a")["class"]).to include("bg-gray-100")
      expect(html(view.pgbus_link_to("Go", "/g", size: :sm, tone: :red)).at_css("a")["class"]).to include("text-red-600")
    end
  end

  describe "#pgbus_button_tag" do
    it "renders a submit button bound to an outside form, with the confirm attribute the dialog reads" do
      button = html(view.pgbus_button_tag("Discard selected", variant: :danger, form: "bulk-discard-jobs-form",
                                                              confirm: "Discard them?")).at_css("button")

      expect(button["type"]).to eq("submit")
      expect(button["form"]).to eq("bulk-discard-jobs-form")
      expect(button["data-turbo-confirm"]).to eq("Discard them?")
      expect(button["name"]).to be_nil
      expect(button["class"].split).to include("bg-red-600")
    end
  end
end
