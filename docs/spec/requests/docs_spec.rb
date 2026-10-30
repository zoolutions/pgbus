# frozen_string_literal: true

require "rails_helper"

# Every authored reference page is a hand-written Phlex class registered in Doc.
# This spec renders each one through the docs shell and asserts it comes back
# 200 with its masthead heading — so a page that fails to render (a bad component
# call, a missing method) fails the build. Unwritten registry entries (no
# view_class yet) are skipped, so this grows automatically as pages land.
RSpec.describe "Docs pages", type: :request do
  Doc.all.select(&:view_class).each do |doc|
    it "renders the #{doc.slug} page" do
      get "/docs/#{doc.slug}"
      expect(response).to have_http_status(:ok)
      expect(response.body).to include("<h1") # the doc Header masthead
      expect(response.body).to include(doc.title)
    end

    it "serves a Markdown twin for #{doc.slug}" do
      get "/docs/#{doc.slug}.md"
      expect(response).to have_http_status(:ok)
      expect(response.media_type).to eq("text/markdown")
    end
  end

  it "404s an unknown doc slug" do
    get "/docs/does-not-exist"
    expect(response).to have_http_status(:not_found)
  end

  it "opens the performance page with the throughput checklist" do
    get "/docs/performance-tuning"
    expect(response.body).to include('id="throughput-checklist"')

    get "/docs/running-workers"
    expect(response.body.scan("/docs/performance-tuning#throughput-checklist").size).to eq(2)
  end

  it "renders the landing page" do
    get "/"
    expect(response).to have_http_status(:ok)
  end
end
