# Dashboard Rules

The dashboard (`app/views/pgbus/`, `app/controllers/pgbus/`, `app/frontend/pgbus/`, the badge
helpers in `app/helpers/pgbus/application_helper.rb`) meets WCAG 2.1 AA in light and dark mode on
every page. Three checks keep it there, and a change is not done until all three are green:

| Check | File | Runs |
|---|---|---|
| Static contrast guard | `spec/pgbus/web/dark_mode_contrast_spec.rb` | Ruby matrix, every push |
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

## The helpers (`spec/system/support/accessibility.rb`)

```ruby
it_behaves_like "an accessible page", "/pgbus/queues", :dark   # visits, asserts 200, audits

it_behaves_like "an accessible page", "/pgbus/jobs", :dark do    # with an interaction first
  let(:interaction) do
    -> { first("details[data-job-toggle] summary").click }
  end
end

visit_dark("/pgbus/queues")     # in any system spec: dark mode the way a user enters it
expect(page).to be_accessible   # axe, tags wcag2a + wcag2aa + wcag21aa
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
