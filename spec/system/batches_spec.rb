# frozen_string_literal: true

require "system_helper"

RSpec.describe "Batches", type: :system do
  let(:in_flight) do
    {
      batch_id: "a1b2c3d4-e5f6-7890-abcd-ef1234567890", description: "Registry backfill",
      status: "processing", total_jobs: 807, completed_jobs: 483, failed_jobs: 0,
      pending_jobs: 324, progress_pct: 59, properties: nil,
      created_at: Time.current, finished_at: nil
    }
  end

  before { @stub_data_source.batch_detail_hash = in_flight }

  # The bar is sized by an inline style but coloured by a Tailwind class. When
  # the compiled style.css drifted from the views, bg-green-500 resolved to no
  # rule at all: the fill was the right width and completely invisible, so a
  # draining batch looked stuck at 0% no matter how often you reloaded.
  describe "the progress bar" do
    it "fills proportionally to completed jobs" do
      visit "/pgbus/batches/#{in_flight[:batch_id]}"

      fill = find("turbo-frame#batch-progress div[style*='width']")
      expect(fill[:style]).to include("59%")
    end

    it "paints the fill with a visible background colour" do
      visit "/pgbus/batches/#{in_flight[:batch_id]}"

      background = page.evaluate_script(<<~JS)
        getComputedStyle(
          document.querySelector("turbo-frame#batch-progress div[style*='width']")
        ).backgroundColor
      JS

      expect(background).not_to eq("rgba(0, 0, 0, 0)")
    end

    it "paints the track behind the fill" do
      visit "/pgbus/batches/#{in_flight[:batch_id]}"

      background = page.evaluate_script(<<~JS)
        getComputedStyle(
          document.querySelector("turbo-frame#batch-progress div[style*='width']").parentElement
        ).backgroundColor
      JS

      expect(background).not_to eq("rgba(0, 0, 0, 0)")
    end
  end

  it "shows the job counters" do
    visit "/pgbus/batches/#{in_flight[:batch_id]}"

    within("turbo-frame#batch-progress") do
      expect(page).to have_content("807")
      expect(page).to have_content("483")
      expect(page).to have_content("324")
    end
  end
end
