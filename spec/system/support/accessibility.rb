# frozen_string_literal: true

require "json"

# The accessibility gate (#498). A dashboard page is accessible when axe-core
# finds no WCAG 2.1 AA violation (plus 2.2 AA target size, which Lighthouse also
# scores) in the rendered DOM, in light AND dark mode.
#
#   it_behaves_like "an accessible page", "/pgbus/queues", :dark
#
# axe-core is driven directly rather than through the axe-core-rspec gem: its
# page adapters are Selenium-only and its driver-agnostic fallback interpolates
# Ruby hashes into JavaScript. Capybara's evaluate_async_script is all this
# needs. There is deliberately no `except:` — a red page is fixed, not excused.
module Accessibility
  ROOT = Pathname.new(__dir__).join("../../..").expand_path
  SOURCE_PATH = ROOT.join("node_modules/axe-core/axe.min.js")
  CONFIG_PATH = ROOT.join("lighthouserc.dashboard.json")
  TAGS = %w[wcag2a wcag2aa wcag21aa wcag22aa].freeze

  # Targets are capped so one broken component cannot bury the report; the
  # cap is stated rather than silently applied.
  TARGET_CAP = 5

  Violation = Struct.new(:id, :impact, :help, :help_url, :targets, :target_count) do
    def to_s
      more = target_count > targets.size ? ["…and #{target_count - targets.size} more"] : []
      "  [#{impact}] #{id}: #{help}\n    #{(targets + more).join("\n    ")}\n    #{help_url}"
    end
  end

  # The pages Lighthouse audits, so the two gates cannot drift:
  # lighthouserc.dashboard.json is the single list. The query string is kept
  # so each Jobs state tab is its own example.
  def self.audited_paths
    JSON.parse(CONFIG_PATH.read).dig("ci", "collect", "url").map { |url| URI.parse(url).request_uri }
  end

  def self.source
    @source ||= SOURCE_PATH.read
  rescue Errno::ENOENT
    raise "axe-core is missing — run `bun install` (node_modules/axe-core/axe.min.js)"
  end

  # Injects axe into the current page and audits it. Returns [] when clean.
  def self.audit(page)
    page.execute_script(source) unless page.evaluate_script("typeof window.axe === 'object'")

    results = page.evaluate_async_script(<<~JS, TAGS)
      const done = arguments[arguments.length - 1];
      axe.run(document, { runOnly: { type: "tag", values: arguments[0] } })
        .then((r) => done(JSON.parse(JSON.stringify(r.violations))))
        .catch((e) => done([{ id: "axe-run-failed", impact: "critical", help: String(e), helpUrl: "", nodes: [] }]));
    JS

    Array(results).map do |violation|
      targets = Array(violation["nodes"]).flat_map { |node| Array(node["target"]) }
      Violation.new(violation["id"], violation["impact"], violation["help"], violation["helpUrl"],
                    targets.first(TARGET_CAP), targets.size)
    end
  end
end

RSpec::Matchers.define :be_accessible do
  match do |page|
    @violations = Accessibility.audit(page)
    @violations.empty?
  end

  failure_message do |_page|
    "expected no WCAG 2.1 AA / 2.2 AA target-size violations, found #{@violations.size}:\n#{@violations.join("\n")}"
  end
end

module DarkModeHelpers
  # The real entry path: a stored preference and a fresh page, so the head
  # script adds `.dark` before first paint and the body's transition-colors
  # never runs under the audit (toggling in place measures mid-transition).
  def visit_dark(path)
    visit_theme(path, dark: true)
  end

  # Light is pinned the same way: with no stored value the head script
  # follows prefers-color-scheme, so a dark-preferring browser (or a
  # preference left behind by an earlier example) would audit dark DOM.
  def visit_light(path)
    visit_theme(path, dark: false)
  end

  def visit_theme(path, dark:)
    visit path
    page.execute_script("localStorage.setItem('pgbus-dark', '#{dark}')")
    visit path
    expect(page).to(dark ? have_css("html.dark") : have_no_css("html.dark"))
  end
end

RSpec.configure do |config|
  config.include DarkModeHelpers, type: :system
end

RSpec.shared_examples "an accessible page" do |path, mode|
  # A caller may add `let(:interaction) { -> { … } }` in the it_behaves_like
  # block (RSpec evaluates that block inside the nested group, so a `let` is
  # the way to pass behaviour in — a `&block` parameter never receives it).
  it "has no WCAG AA violations on #{path} (#{mode})", :a11y do
    mode == :dark ? visit_dark(path) : visit_light(path)

    # A 404 or 500 page can be accessible, so without this a URL the stub
    # cannot serve passes the audit while measuring nothing.
    expect(page.status_code).to eq(200),
                                "#{path} returned #{page.status_code}; the sample data does not match the audited URL"
    instance_exec(&interaction) if respond_to?(:interaction)
    expect(page).to be_accessible
  end
end
