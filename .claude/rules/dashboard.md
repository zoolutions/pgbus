# Dashboard Rules

The dashboard (`app/views/pgbus/`, `app/controllers/pgbus/`, `app/frontend/pgbus/`, the badge
helpers in `app/helpers/pgbus/application_helper.rb`) meets WCAG 2.1 AA (plus 2.2 AA target size) in light and dark mode on
every page. These checks keep it there, and a change is not done until all of them are green:

| Check | File | Runs |
|---|---|---|
| Static contrast guard | `spec/pgbus/web/dark_mode_contrast_spec.rb` | Ruby matrix, every push |
| View conventions guard | `spec/pgbus/web/view_conventions_spec.rb` | Ruby matrix, every push |
| Time conventions guard (ERB) | `spec/pgbus/web/time_conventions_spec.rb` | Ruby matrix, every push |
| Time formatting cop (Ruby) | `Pgbus/DashboardTimeFormatting` (`rubocop/cop/pgbus/`) | Lint job, every push |
| axe gate (light + dark, every page) | `spec/system/accessibility_spec.rb` | `system_test` job, every push |
| Lighthouse budgets | `lighthouserc.dashboard.json`, `bin/lighthouse` | monthly + `workflow_dispatch`, report-only |

**No accepted debt.** The `be_accessible` matcher has no `except:`. A page that fails is fixed in
the same PR; nothing is skipped, tagged out or excused.

## Order of work

1. **System spec first.** Assert what the operator reads: state words, reasons, the
   what-happens-next text, the empty state, the pager, bulk actions. Visual changes get a light and a
   dark example.
2. **Sample data.** A new `Web::DataSource` method gets a sparse default in
   `Pgbus::Test::StubDataSource` (`spec/support/pgbus/stub_data_source.rb`) and rich rows in
   `StubDataSource::SampleData#fill_sample_data!` (`spec/support/pgbus/stub_data_source/sample_data.rb`).
   Anything a screenshot shows comes from the sample data, not from per-spec fixtures, so the dummy
   server, the axe gate, Lighthouse and the PR screenshots show the same rows.
   `spec/system/sample_data_spec.rb` fails if a list goes empty.
3. **Static guard + axe gate** green.
4. `bundle exec rake frontend:css` after any new Tailwind class (`compiled_css_coverage_spec` fails
   otherwise), `bun run lint:herb`.
5. **New page:** add its URL to `lighthouserc.dashboard.json`. The route-coverage example in the
   accessibility spec fails when an HTML GET route has no audited URL, and a URL that does not
   answer 200 against the sample data fails its own example.
6. Before/after screenshots, light + dark, on the PR (`gh pr create --attach`, see `AGENTS.md`).
7. `bin/lighthouse` on demand when the change touches page weight (a new vendored library, a new
   chart).

## Buttons, tables and lists (`view_conventions_spec.rb`)

- Every button and action link goes through `Pgbus::ButtonHelper`: `pgbus_button_to`, `pgbus_link_to`,
  `pgbus_button_tag` (a submit outside its form, `form: "bulk-…"`) or `pgbus_button_classes` (for a
  `<summary>`). Variants `primary danger success warning secondary outline menu link`, sizes `sm md`,
  link tones `indigo red green yellow`; `confirm:` emits `data-turbo-confirm`. No hand-written colour
  classes on `button_to`, `link_to` or `<button>`. A new variant spells its classes out in full.
- Every `<table>` is a `pgbus-table`; every cell carries `data-label="<%= t("…headers.<col>") %>"`,
  the same key as its column header (checkbox and `colspan` cells excepted); empty states render
  `shared/_empty_row`; thead/tbody/th use the one chrome the spec lists.
- A list that can grow renders `shared/_pager` against `DataSource#list_count` (bounded, `has_more`
  when capped). A second list on one page pages on `<list>_page`, parsed by `page_param(:<list>_page)`.

## Timestamps, ages and durations (`time_conventions_spec.rb`, `Pgbus/DashboardTimeFormatting`)

Every time the dashboard prints goes through `Pgbus::Web::TimeFormat` and its view helpers. They are
in `Time.zone`, translated in all 12 locales (`pgbus.helpers.time`), past/future-aware, and they accept
every shape a data source returns (`Time`, `TimeWithZone`, `DateTime`, ISO strings, epoch seconds, nil).

| What you print | Helper | Renders |
|---|---|---|
| A moment in a list cell | `pgbus_time(value)` | `<time datetime="UTC" title="absolute">5m ago</time>` |
| A future moment whose clock time matters | `pgbus_time(value, clock: true)` | `in 2h (21:40)` |
| A moment in an expanded row or on a show page | `pgbus_timestamp(value)` | `2026-10-10 02:08:54 CEST (5m ago)` |
| The absolute next to a relative already shown | `pgbus_absolute_time(value)` | `2026-10-10 02:08:54 CEST` |
| An age or duration in seconds | `pgbus_duration(seconds)` | `2m 5s` (never negative; nil → `—`) |
| A duration in milliseconds | `pgbus_ms_duration(millis)` | `1.5s` |
| A moment inside a translated sentence | an `_html` key, the helper as the argument | `t("….running_html", ago: pgbus_time(t))` |

- Never print a time field raw (`<%= row[:enqueued_at] %>`, `<%= q[:oldest_age_sec] || "—" %>`): that is
  `Time#to_s`, an ISO string or bare seconds, and differs between the stub and production.
- Never `strftime`, `time_ago_in_words`, `distance_of_time_in_words`, `to_fs` or `l`/`localize` in a
  view or a dashboard helper, nor `iso8601` in a view (it is a wire format); never a hard-coded unit or "ago"/"in". A new word or format is a key under
  `pgbus.helpers.time` in all 12 locales, read by `TimeFormat`.
- A column header never carries a unit ("Oldest (s)"): the cell prints its own.
- Specs that assert relative times freeze the clock (`travel_to(now)` with `now` truncated to the
  second); fixtures mix `Time` and ISO-string values so both coercions stay exercised.

## The helpers (`spec/system/support/accessibility.rb`)

```ruby
it_behaves_like "an accessible page", "/pgbus/queues", :dark   # visits, asserts 200, audits

it_behaves_like "an accessible page", "/pgbus/jobs", :dark do    # with an interaction first
  let(:interaction) do
    -> { first("details[data-job-toggle] summary").click }
  end
end

visit_dark("/pgbus/queues")     # in any system spec: dark mode the way a user enters it
expect(page).to be_accessible   # axe, tags wcag2a + wcag2aa + wcag21aa + wcag22aa (target size)
```

`visit_dark` stores `localStorage['pgbus-dark'] = 'true'` and visits again, so the layout's head
script sets `html.dark` before first paint. Never call `pgbusToggleDarkMode()` before an audit: the
body's `transition-colors` animates for 150 ms and axe measures white headings against the
light background (1.04:1 on every `h1`).

## Reading a violation

```
[serious] color-contrast: Elements must meet minimum color contrast ratio thresholds
  .dark\:hover\:bg-gray-700\/50:nth-child(1) > … > .text-yellow-600
  https://dequeuniversity.com/rules/axe/4.13/color-contrast
```

Rule id → the help URL explains it → the selector names the element (up to five; "…and N more"
when capped). For contrast, the selector's classes usually name the token to change. Reproduce in
the browser with `bundle exec rake dummy:server` and the same light/dark mode.

## Token rules (the static guard enforces these)

- Every light colour token has a `dark:` partner for the same property; every light `hover:` colour
  a `dark:hover:` partner.
- Never `text-gray-400` in light, never `dark:text-gray-500/600` (disabled controls exempt).
- **Light text:** no `-500` shade except gray (blue-500 3.7:1, red-500 3.8:1, indigo-500 below 4.5:1
  under the pointer), and no `amber/yellow/green/lime/orange-600` (≈3.2:1) — use `-700`. Large text
  (`text-2xl` and up) needs only 3:1 and may keep them.
- **Dark badges:** `dark:text-indigo-400` on `dark:bg-indigo-900/30` is 4.4:1 — use
  `dark:text-indigo-300`. `dark:text-gray-400` on a `dark:bg-gray-700` chip is 4.0:1 — use
  `dark:text-gray-300` or lighter.
- 12 px row actions sit on the hovered row tint (`dark:hover:bg-gray-700/50`): use `-300` text there.
- A yellow button carries dark text (`bg-yellow-400 text-yellow-950`); white on yellow fails.
- `<dt>`/`<dd>` only inside a `<dl>`; headings descend one level at a time from the page `<h1>`.

The static guard is the cheap first line; the axe gate is the truth (it measures inherited colours,
hover states and the always-dark navbar, which the guard cannot see).

## Lighthouse

`lighthouserc.dashboard.json` holds the page list and the budgets (desktop preset; baseline
2026-10-10): performance ≥ 0.9, accessibility 1.0, best-practices ≥ 0.95, LCP ≤ 2 s, TBT ≤ 300 ms,
CLS ≤ 0.1, scripts ≤ 850 KB, total ≤ 1.2 MB (uncompressed: Puma serves without compression).
`unused-javascript` and `unminified-javascript` only warn (apexcharts on every page is a known
follow-up).

```bash
bundle exec rake dummy:server        # terminal 1
bin/lighthouse                       # terminal 2: PAGE PERF A11Y BP FCP LCP TBT CLS
bin/lighthouse -i 3 --pages /pgbus/insights --output tmp/lh.json
```

`.github/workflows/lighthouse.yml` runs `lhci collect → upload → assert` monthly and on
`workflow_dispatch`, writes a score table to the job summary and keeps `.lighthouseci/` as a 30-day
artifact. It never gates a merge.
