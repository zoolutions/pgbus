# frozen_string_literal: true

# docs-kit synced: v1.2.1
# docs-kit configuration — everything that makes this site look like "pgbus"
# rather than any other docs site. The shared chrome (Shell/Sidebar/ThemeSwitcher/
# Code/Page) comes from the gem; only this config differs per site. The `themes`
# MUST match the @plugin "daisyui" { themes: ... } block in
# app/assets/stylesheets/application.tailwind.css, or the switcher offers a theme
# the compiled CSS never generated.
Rails.application.config.to_prepare do
  DocsKit.configure do |c|
    c.brand        = "pgbus"
    c.title_suffix = "pgbus"

    # The one-line summary agents read first in /llms.txt (the llmstxt.org
    # blockquote under the H1).
    c.tagline = "PostgreSQL-native job processing and event bus for Rails, " \
                "built on PGMQ — ActiveJob adapter, topic-routed events, worker " \
                "recycling, SSE streams. One database, no Redis."

    c.themes = %w[dark light synthwave retro cyberpunk dracula night nord sunset]

    # The version badge in the sidebar header tracks the documented gem. A lambda
    # (not a String) so it re-reads Pgbus::VERSION on every reload — the pgbus
    # path-gem is required as "pgbus/version" (Gemfile), so only the constant is
    # loaded; the engine never boots inside the docs app.
    c.version_badge = -> { "v#{Pgbus::VERSION}" }

    # Code blocks: a light base with a dark override, so the highlight stays
    # readable when the switcher lands on a dark daisyUI theme. CSS-only scoping
    # ([data-theme=X]) — no JS, no flash.
    c.code_theme      = "Rouge::Themes::Github"  # light themes
    c.code_theme_dark = "Rouge::Themes::Monokai" # dark themes

    # A link to the source repo in the topbar, rendered with the shipped GitHub
    # brand mark.
    c.topbar_links = [
      { href: "https://github.com/zoolutions/pgbus", label: "GitHub", icon: :github }
    ]

    # SEO + social sharing. docs-kit emits the full <head> (description, Open
    # Graph, Twitter Card, canonical, favicon, theme-color) from these knobs.
    # og_image resolves through THIS site's asset pipeline (app/assets/images/) to
    # the digested /assets URL — regenerate the card with `bin/rails docs_kit:og`.
    c.seo.description  = "PostgreSQL-native job processing and event bus for " \
                         "Rails — ActiveJob on PGMQ, AMQP-style topic routing, " \
                         "worker recycling, dead-letter queues, transactional " \
                         "outbox, and Postgres-SSE streams. One database, no Redis."
    c.seo.site_url     = "https://pgbus.zoolutions.llc"
    c.seo.og_image     = "og/og.png"
    c.seo.og_type      = "website"
    c.seo.twitter_card = "summary_large_image"
    c.seo.twitter_site = "@mhenrixon"
    c.seo.locale       = "en_US"
    c.seo.theme_color  = "#1d232a" # daisyUI dark base-100 (themes.first)
    # favicon href is used verbatim (not through the asset pipeline), so it's a
    # public/ path served at a stable root URL — see public/favicon.svg.
    c.seo.favicon      = "/favicon.svg"

    # The sidebar nav derives from the registry — one heading → one registry.
    # Each registry's authored pages become NavItems automatically (an unwritten
    # page is skipped, so no dead links); the page `group:` values render as the
    # collapsible sub-groups. This also feeds the AI surfaces (/llms.txt,
    # /llms-full.txt, search, MCP) with zero extra code.
    c.nav_registries = { "Docs" => Doc }
  end
end
