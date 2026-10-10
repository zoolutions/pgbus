# frozen_string_literal: true

module Pgbus
  # Every button and action link on the dashboard (issue #496): one class table
  # with light and dark colours, a focus ring, and a 24 px minimum target
  # (WCAG 2.5.8) in every size. Class lists are spelled out in full so Tailwind
  # and the static specs (dark_mode_contrast_spec, compiled_css_coverage_spec)
  # can see every token; never interpolate a colour name.
  module ButtonHelper
    BASE = "inline-flex items-center min-h-6 font-medium focus-visible:outline-2 focus-visible:outline-offset-2 " \
           "focus-visible:outline-indigo-600 dark:focus-visible:outline-indigo-400"

    VARIANTS = {
      # Hover goes darker, never lighter: white on red-500 is 3.8:1, on
      # green-600 3.3:1, both under 4.5:1 for these small labels.
      primary: "bg-indigo-600 text-white hover:bg-indigo-700",
      danger: "bg-red-600 text-white hover:bg-red-700",
      # Permanent and irreversible (Delete queue), set apart from danger.
      destructive: "bg-red-800 text-white hover:bg-red-900",
      success: "bg-green-700 text-white hover:bg-green-800",
      # White on yellow fails; yellow buttons carry dark text in both themes.
      warning: "bg-yellow-400 text-yellow-950 hover:bg-yellow-300",
      secondary: "bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-200 hover:bg-gray-200 dark:hover:bg-gray-600",
      # Bordered, on the card's own surface (the pager).
      outline: "border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-700 dark:text-gray-300 " \
               "hover:bg-gray-50 dark:hover:bg-gray-700",
      # A row in a dropdown menu (the Events reroute picker).
      menu: "w-full justify-start text-gray-700 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-700",
      link: nil
    }.freeze

    # Link colours. Dark text is -300: 12 px row actions sit on the hovered row
    # tint (dark:hover:bg-gray-700/50), where -400 drops below 4.5:1.
    TONES = {
      indigo: "text-indigo-600 dark:text-indigo-300 hover:text-indigo-800 dark:hover:text-indigo-200",
      red: "text-red-600 dark:text-red-300 hover:text-red-800 dark:hover:text-red-200",
      green: "text-green-700 dark:text-green-300 hover:text-green-900 dark:hover:text-green-200",
      yellow: "text-yellow-700 dark:text-yellow-300 hover:text-yellow-900 dark:hover:text-yellow-200"
    }.freeze

    SIZES = {
      sm: "rounded-md px-3 py-1.5 text-xs",
      md: "rounded-md px-3 py-2 text-sm"
    }.freeze

    # Links keep their type size; the padding on sm gives the 24 px target.
    LINK_SIZES = {
      sm: "rounded px-1.5 py-1 text-xs",
      md: "text-sm"
    }.freeze

    def pgbus_button_classes(variant, size = :md, tone: :indigo, extra: nil)
      raise ArgumentError, "unknown button variant: #{variant.inspect}" unless VARIANTS.key?(variant)
      raise ArgumentError, "unknown button size: #{size.inspect}" unless SIZES.key?(size)

      colours = variant == :link ? TONES.fetch(tone) { raise ArgumentError, "unknown link tone: #{tone.inspect}" } : VARIANTS[variant]
      sizing = variant == :link ? LINK_SIZES[size] : SIZES[size]
      [BASE, sizing, colours, extra].compact.join(" ")
    end

    def pgbus_button_to(label, path, variant: :primary, size: :md, tone: :indigo, confirm: nil, **options)
      options[:class] = pgbus_button_classes(variant, size, tone: tone, extra: options[:class])
      options[:data] = pgbus_confirm_data(options[:data], confirm)
      button_to(label, path, options)
    end

    def pgbus_link_to(label, path, variant: :link, size: :md, tone: :indigo, **options)
      options[:class] = pgbus_button_classes(variant, size, tone: tone, extra: options[:class])
      link_to(label, path, options)
    end

    # A submit button outside its form (form: "bulk-…"), for the bulk actions.
    def pgbus_button_tag(label, variant: :primary, size: :md, tone: :indigo, confirm: nil, **options)
      options[:class] = pgbus_button_classes(variant, size, tone: tone, extra: options[:class])
      options[:data] = pgbus_confirm_data(options[:data], confirm)
      content_tag(:button, label, type: "submit", **options)
    end

    private

    # application.js's confirm dialog reads data-turbo-confirm.
    def pgbus_confirm_data(data, confirm)
      data = (data || {}).dup
      data[:turbo_confirm] = confirm if confirm
      data.presence
    end
  end
end
