// Charts module for the Pgbus Insights page and the workflow timeline.
// Loaded only on those pages via an inline <script type="module">, which
// passes it the series the page embeds. ApexCharts is fetched here, on first
// draw, so no other dashboard page downloads it (issue #517). The page renders each chart's caption,
// data table and empty state; this module only draws into the containers that
// exist, and redraws them when <html class="dark"> toggles.

// Colours for both themes in one place (issue #492). card is the chart card's
// own background (white / gray-800): the donut's slice separators use it so
// they read as gaps, not white lines, in dark mode.
function palette(isDark) {
  return {
    mode: isDark ? "dark" : "light",
    text: isDark ? "#d1d5db" : "#4b5563",
    grid: isDark ? "#374151" : "#e5e7eb",
    axis: isDark ? "#4b5563" : "#d1d5db",
    card: isDark ? "#1f2937" : "#ffffff",
    throughput: isDark ? "#818cf8" : "#6366f1",
    latencyAvg: isDark ? "#818cf8" : "#6366f1",
    latencyP95: isDark ? "#fbbf24" : "#f59e0b",
    status: {
      success: "#10b981",
      failed: "#ef4444",
      dead_lettered: "#f97316",
    },
    other: isDark ? "#9ca3af" : "#6b7280",
    // The workflow timeline's segments (issue #542): the waits in neutral
    // tones, ran coloured by how the attempt ended.
    timeline: {
      waited: isDark ? "#4b5563" : "#d1d5db",
      queued: isDark ? "#64748b" : "#94a3b8",
      waiting: isDark ? "#fbbf24" : "#f59e0b",
      paused: isDark ? "#fb923c" : "#ea580c",
      ran_ok: "#10b981",
      ran_failed: "#ef4444",
      ran_open: isDark ? "#818cf8" : "#6366f1",
      ran_cancelled: isDark ? "#9ca3af" : "#6b7280",
    },
  };
}

function reducedMotion() {
  return window.matchMedia("(prefers-reduced-motion: reduce)").matches;
}

// Options every chart shares: theme, text, axes, grid, tooltip, animation.
function baseOptions(id, type, colors) {
  return {
    chart: {
      id,
      type,
      height: 280,
      toolbar: { show: false },
      background: "transparent",
      foreColor: colors.text,
      animations: { enabled: !reducedMotion() },
    },
    theme: { mode: colors.mode },
    tooltip: { theme: colors.mode },
    grid: { borderColor: colors.grid },
    legend: { labels: { colors: colors.text } },
    dataLabels: { enabled: false },
  };
}

function timeAxis(colors) {
  return {
    type: "datetime",
    labels: { style: { colors: colors.text } },
    axisBorder: { color: colors.axis },
    axisTicks: { color: colors.axis },
    tooltip: { enabled: false },
  };
}

function valueAxis(colors, formatter) {
  return {
    labels: { style: { colors: colors.text }, formatter },
    axisBorder: { show: true, color: colors.axis },
  };
}

function points(series, key) {
  return series.map((p) => ({ x: new Date(p.time).getTime(), y: p[key] }));
}

// vendor/apexcharts.js is a UMD bundle: run as a module it defines the global.
// Memoized, so the theme-toggle redraws reuse the one download. Every caller
// awaits the same promise, and its callbacks run in call order, so a redraw
// can't overtake the first render. A failed import clears the memo, so the
// next draw (a theme toggle) retries instead of awaiting the same rejection.
let apexCharts = null;
function loadApexCharts() {
  apexCharts ||= import("apexcharts")
    .then(() => globalThis.ApexCharts)
    .catch((error) => {
      apexCharts = null;
      throw error;
    });
  return apexCharts;
}

const charts = {};

function draw(ApexCharts, id, options) {
  if (charts[id]) charts[id].destroy();
  delete charts[id];

  const el = document.getElementById(id);
  if (!el) return;

  charts[id] = new ApexCharts(el, options);
  charts[id].render();
}

export async function renderCharts(data, i18n) {
  const ApexCharts = await loadApexCharts();
  const colors = palette(document.documentElement.classList.contains("dark"));

  draw(ApexCharts, "throughput-chart", {
    ...baseOptions("throughput-chart", "area", colors),
    series: [{ name: i18n.seriesName, data: points(data.throughput || [], "count") }],
    stroke: { curve: "smooth", width: 2 },
    fill: { type: "gradient", gradient: { shadeIntensity: 1, opacityFrom: 0.4, opacityTo: 0.05, stops: [0, 100] } },
    colors: [colors.throughput],
    xaxis: timeAxis(colors),
    yaxis: valueAxis(colors, (v) => Math.round(v)),
  });

  const statuses = Object.keys(data.status_counts || {});
  draw(ApexCharts, "status-chart", {
    ...baseOptions("status-chart", "donut", colors),
    series: statuses.map((s) => data.status_counts[s]),
    labels: statuses.map((s) => (i18n.statuses && i18n.statuses[s]) || s),
    colors: statuses.map((s) => colors.status[s] || colors.other),
    stroke: { colors: [colors.card] },
    legend: { position: "bottom", labels: { colors: colors.text } },
    plotOptions: {
      pie: {
        donut: {
          size: "60%",
          labels: {
            show: true,
            name: { color: colors.text },
            value: { color: colors.text },
            total: { show: true, color: colors.text },
          },
        },
      },
    },
  });

  draw(ApexCharts, "latency-chart", {
    ...baseOptions("latency-chart", "line", colors),
    series: [
      { name: i18n.latencyAvg, data: points(data.latency_trend || [], "avg_ms") },
      { name: i18n.latencyP95, data: points(data.latency_trend || [], "p95_ms") },
    ],
    stroke: { curve: "smooth", width: [2, 2], dashArray: [0, 5] },
    colors: [colors.latencyAvg, colors.latencyP95],
    xaxis: timeAxis(colors),
    yaxis: valueAxis(colors, (v) => i18n.milliseconds.replace("%n", Math.round(v))),
    legend: { position: "top", labels: { colors: colors.text } },
  });
}

const TIMELINE_SERIES = ["waited", "queued", "waiting", "ran_ok", "ran_failed", "ran_open", "ran_cancelled", "paused"];

// One range bar per segment, grouped by series so each kind keeps its colour.
// Rows are the attempts (oldest first) plus a "Paused" row for the pauses.
function timelineSeries(data, i18n) {
  const points = Object.fromEntries(TIMELINE_SERIES.map((name) => [name, []]));
  const point = (label, segment, row) => ({
    x: label,
    y: [segment.from, segment.to],
    duration: segment.duration,
    status: row && i18n.statuses ? i18n.statuses[row.status] || row.status : null,
    error: row ? row.error_class : null,
  });

  (data.rows || []).forEach((row) => {
    row.segments.forEach((segment) => {
      if (points[segment.series]) points[segment.series].push(point(row.label, segment, row));
    });
  });
  (data.paused || []).forEach((segment) => points.paused.push(point(i18n.paused_row, segment, null)));

  return TIMELINE_SERIES.filter((name) => points[name].length > 0).map((name) => ({
    name: (i18n.series && i18n.series[name]) || name,
    key: name,
    data: points[name],
  }));
}

// The tooltip: the segment's kind and duration, then the attempt's status and
// error class. Text only (textContent-safe): every value is escaped.
function escapeText(value) {
  const div = document.createElement("div");
  div.textContent = value == null ? "" : String(value);
  return div.innerHTML;
}

function timelineTooltip({ seriesIndex, dataPointIndex, w }) {
  const series = w.config.series[seriesIndex];
  const p = series.data[dataPointIndex];
  const lines = [`${escapeText(series.name)}: ${escapeText(p.duration)}`];
  if (p.status) lines.push(escapeText(p.status));
  if (p.error) lines.push(escapeText(p.error));
  return `<div class="apexcharts-tooltip-title">${escapeText(p.x)}</div><div style="padding: 4px 10px">${lines.join("<br>")}</div>`;
}

export async function renderTimeline(data, i18n) {
  const ApexCharts = await loadApexCharts();
  const colors = palette(document.documentElement.classList.contains("dark"));
  const series = timelineSeries(data, i18n);
  const rows = (data.rows || []).length + ((data.paused || []).length > 0 ? 1 : 0);
  const base = baseOptions("workflow-timeline-chart", "rangeBar", colors);

  draw(ApexCharts, "workflow-timeline-chart", {
    ...base,
    chart: {
      ...base.chart,
      height: Math.max(160, rows * 36 + 90),
      // Waits can span days: zoom and pan along time, reset to see it all.
      zoom: { enabled: true, type: "x" },
      toolbar: { show: true, tools: { download: false, selection: false, zoom: true, zoomin: true, zoomout: true, pan: true, reset: true } },
    },
    series,
    colors: series.map((s) => colors.timeline[s.key] || colors.other),
    plotOptions: { bar: { horizontal: true, rangeBarGroupRows: true, barHeight: "60%" } },
    xaxis: timeAxis(colors),
    yaxis: { labels: { style: { colors: colors.text } } },
    legend: { position: "top", labels: { colors: colors.text } },
    tooltip: { theme: colors.mode, custom: timelineTooltip },
  });
}

let themeObserver = null;

// The page toggles dark mode with a class on <html>; redraw in the new palette.
// render is the page's draw function (renderCharts on Insights, renderTimeline
// on a workflow).
export function observeThemeChanges(getDataFn, i18n, render = renderCharts) {
  if (themeObserver) themeObserver.disconnect();

  themeObserver = new MutationObserver(() => {
    const data = typeof getDataFn === "function" ? getDataFn() : getDataFn;
    if (data) render(data, i18n);
  });
  themeObserver.observe(document.documentElement, { attributes: true, attributeFilter: ["class"] });
}
