# frozen_string_literal: true

require "system_helper"

# Issue #529: Tailwind 4's preflight zeroes every element's margin, so a modal
# <dialog> keeps the user-agent `inset: 0` but loses the `margin: auto` that
# centres it inside that inset, and opens in the top-left corner.
RSpec.describe "Dashboard dialogs", type: :system do
  # Gaps between the open dialog and each viewport edge. clientWidth/Height
  # leave out the scrollbar, as the modal's fixed containing block does.
  def viewport_gaps(dialog_id)
    page.evaluate_script(<<~JS)
      (() => {
        const rect = document.getElementById("#{dialog_id}").getBoundingClientRect();
        const viewport = document.documentElement;
        return {
          left: rect.left, right: viewport.clientWidth - rect.right,
          top: rect.top, bottom: viewport.clientHeight - rect.bottom
        };
      })()
    JS
  end

  def expect_centred(dialog_id)
    gaps = viewport_gaps(dialog_id)

    aggregate_failures do
      expect(gaps["left"]).to be > 1
      expect(gaps["top"]).to be > 1
      expect(gaps["left"]).to be_within(1).of(gaps["right"])
      expect(gaps["top"]).to be_within(1).of(gaps["bottom"])
    end
  end

  %i[light dark].each do |mode|
    context "with the #{mode} theme" do
      before { mode == :dark ? visit_dark("/pgbus/queues") : visit_light("/pgbus/queues") }

      it "opens the confirm dialog in the centre of the viewport" do
        click_on "Delete", match: :first

        expect(page).to have_css("dialog#pgbus-confirm-dialog[open]", text: "pgbus_default")
        expect_centred("pgbus-confirm-dialog")
      end

      it "opens the alert dialog in the centre of the viewport" do
        page.execute_script('document.getElementById("pgbus-alert-dialog").showModal()')

        expect(page).to have_css("dialog#pgbus-alert-dialog[open]")
        expect_centred("pgbus-alert-dialog")
      end
    end
  end
end
