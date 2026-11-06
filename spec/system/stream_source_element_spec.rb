# frozen_string_literal: true

require "system_helper"

# Issue #644: <pgbus-stream-source> keeps its resume cursor when it is moved in
# the DOM (disconnected and reconnected as the same instance), so the reconnect
# replays only what it has not applied yet. The page and the fake SSE server:
# spec/system/support/stream_source_fixture.rb.
RSpec.describe "<pgbus-stream-source>", type: :system do
  include StreamSourceFixture

  # A durable frame sent after a reconnect lands after anything the reconnect
  # replayed (one ordered stream), so once it has arrived the list is final.
  def expect_deliveries_after_marker(stream, expected)
    marker = broadcast(stream, append_item("marker"))
    expect(page).to have_css("#deliveries li", count: expected.size + 1)
    expect(deliveries).to eq((expected + [marker]).map(&:to_s))
  end

  before { serve_fixture }

  it "connects the first time with ?since= from the since-id attribute" do
    expect_request(1, "fetch /fake-sse/demo since=100")
  end

  it "resumes from the last durable id after a move and applies nothing twice", :aggregate_failures do
    expect_request(1, "fetch /fake-sse/demo since=100")
    ids = [1, 2, 3].map { |n| broadcast("/fake-sse/demo", append_item(n)) }
    ids << broadcast("/fake-sse/demo", replace_status("server"))
    expect(page).to have_css("#deliveries li", count: 4)
    expect(page).to have_css("#status", exact_text: "server")

    page.execute_script('document.getElementById("status").textContent = "edited locally"')
    move_element

    expect_request(2, "fetch /fake-sse/demo since=#{ids.last}")
    expect_deliveries_after_marker("/fake-sse/demo", ids)
    expect(page).to have_css("#status", exact_text: "edited locally")
    expect(page).to have_css("#items li", count: 4)
  end

  it "delivers a frame broadcast while the element was detached exactly once", :aggregate_failures do
    expect_request(1, "fetch /fake-sse/demo since=100")
    first = broadcast("/fake-sse/demo", append_item(1))
    expect(page).to have_css("#deliveries li", count: 1)

    page.execute_script('window.detached = document.getElementById("source"); window.detached.remove()')
    missed = broadcast("/fake-sse/demo", append_item(2))
    page.execute_script("document.body.appendChild(window.detached)")

    expect_request(2, "fetch /fake-sse/demo since=#{first}")
    expect_deliveries_after_marker("/fake-sse/demo", [first, missed])
  end

  it "keeps the cursor on the last durable id when an ephemeral frame came last", :aggregate_failures do
    expect_request(1, "fetch /fake-sse/demo since=100")
    durable = broadcast("/fake-sse/demo", append_item(1))
    ephemeral = broadcast("/fake-sse/demo", replace_status("typing"), ephemeral: true)
    expect(page).to have_css("#deliveries li", count: 2)
    expect(ephemeral).to be_negative

    move_element

    expect_request(2, "fetch /fake-sse/demo since=#{durable}")
  end

  it "resumes from the last durable id when Turbo Drive carries a permanent element to the next page",
     :aggregate_failures do
    expect_request(1, "fetch /fake-sse/demo since=100")
    ids = [1, 2].map { |n| broadcast("/fake-sse/demo", append_item(n)) }
    expect(page).to have_css("#deliveries li", count: 2)

    click_on "next page"

    expect(page).to have_css("h1", exact_text: "second")
    expect_request(2, "fetch /fake-sse/demo since=#{ids.last}")
    expect_deliveries_after_marker("/fake-sse/demo", ids)
  end

  it "uses the higher since-id when the attribute moved past the cursor while detached" do
    expect_request(1, "fetch /fake-sse/demo since=100")
    broadcast("/fake-sse/demo", append_item(1))
    expect(page).to have_css("#deliveries li", count: 1)

    page.execute_script(<<~JS)
      const el = document.getElementById("source")
      el.remove()
      el.setAttribute("since-id", "250")
      document.body.appendChild(el)
    JS

    expect_request(2, "fetch /fake-sse/demo since=250")
  end

  it "drops the cursor when the element is re-pointed at another stream" do
    expect_request(1, "fetch /fake-sse/demo since=100")
    [1, 2].each { |n| broadcast("/fake-sse/demo", append_item(n)) }
    expect(page).to have_css("#deliveries li", count: 2)

    page.execute_script(<<~JS)
      const el = document.getElementById("source")
      el.remove()
      el.setAttribute("src", "/fake-sse/other")
      el.setAttribute("since-id", "5")
      document.body.appendChild(el)
    JS

    expect_request(2, "fetch /fake-sse/other since=5")
  end
end
