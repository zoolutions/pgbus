# frozen_string_literal: true

# In-memory registry of the reference docs. One line per page — slug and view
# derive from the title (both overridable), and the sidebar nav derives from this
# registry with zero extra code (see config/initializers/docs_kit.rb's
# `nav_registries`). It also feeds the AI surfaces (/llms.txt, /llms-full.txt,
# search, MCP): an unwritten page (whose view class doesn't resolve yet) is
# silently skipped everywhere, so the whole list can be declared up front as a
# burn-down of pages to author.
#
# Add a page with `rails g docs_kit:page "Title" --group=…`, which appends the
# `page` line here and writes the class under app/views/docs/pages/. Uses
# DocsKit::Registry for the shared all/from_slug/grouped/nav_items API.
class Doc
  extend DocsKit::Registry
  path_prefix    "/docs"
  view_namespace "Views::Docs::Pages"

  # Getting started
  page "Overview",      group: "Getting started"
  page "Installation",  group: "Getting started"
  page "Quick start",   group: "Getting started"
  page "Configuration", group: "Getting started"

  # Guide
  page "Architecture",             group: "Guide"
  page "ActiveJob adapter",        group: "Guide", slug: "active-job", view: "ActiveJob"
  page "Event bus",                group: "Guide"
  page "Retries & dead letters",   group: "Guide", slug: "retries-dead-letters", view: "RetriesDeadLetters"
  page "Concurrency & uniqueness", group: "Guide", slug: "concurrency-uniqueness", view: "ConcurrencyUniqueness"
  page "Routing & ordering",       group: "Guide", slug: "routing-ordering", view: "RoutingOrdering"
  page "Batches",                  group: "Guide"
  page "Recurring tasks",          group: "Guide", slug: "recurring-tasks", view: "RecurringTasks"
  page "Transactional outbox",     group: "Guide", slug: "outbox", view: "Outbox"
  page "Real-time streams",        group: "Guide", slug: "streams", view: "Streams"

  # Operations
  page "Running workers",       group: "Operations", slug: "running-workers", view: "RunningWorkers"
  page "Sizing capsules",       group: "Operations", slug: "sizing-capsules", view: "SizingCapsules"
  page "Dashboard",             group: "Operations"
  page "Observability",         group: "Operations"
  page "Performance & tuning",  group: "Operations", slug: "performance-tuning", view: "PerformanceTuning"
  page "Separate database",     group: "Operations", slug: "separate-database", view: "SeparateDatabase"
  page "Rolling restarts",      group: "Operations"

  # Testing
  page "Testing", group: "Testing"

  # Migrate
  page "Upgrading pgbus", group: "Migrate", slug: "upgrading-pgbus", view: "UpgradingPgbus"
  page "From Sidekiq",    group: "Migrate", slug: "from-sidekiq", view: "FromSidekiq"
  page "From SolidQueue", group: "Migrate", slug: "from-solid-queue", view: "FromSolidQueue"
  page "From GoodJob",    group: "Migrate", slug: "from-good-job", view: "FromGoodJob"

  # Reference
  page "Configuration reference", group: "Reference", slug: "configuration-reference", view: "ConfigurationReference"
  page "CLI & generators",        group: "Reference", slug: "cli-generators", view: "CliGenerators"
end
