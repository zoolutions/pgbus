// Charts module for the Pgbus Insights page.
// Loaded only on the insights page via an inline <script type="module">, which
// passes it the series the page embeds. The page renders each chart's caption,
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

const charts = {};

function draw(id, options) {
  if (charts[id]) charts[id].destroy();
  delete charts[id];

  const el = document.getElementById(id);
  if (!el) return;

  charts[id] = new ApexCharts(el, options);
  charts[id].render();
}

export function renderCharts(data, i18n) {
  const colors = palette(document.documentElement.classList.contains("dark"));

  draw("throughput-chart", {
    ...baseOptions("throughput-chart", "area", colors),
    series: [{ name: i18n.seriesName, data: points(data.throughput || [], "count") }],
    stroke: { curve: "smooth", width: 2 },
    fill: { type: "gradient", gradient: { shadeIntensity: 1, opacityFrom: 0.4, opacityTo: 0.05, stops: [0, 100] } },
    colors: [colors.throughput],
    xaxis: timeAxis(colors),
    yaxis: valueAxis(colors, (v) => Math.round(v)),
  });

  const statuses = Object.keys(data.status_counts || {});
  draw("status-chart", {
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

  draw("latency-chart", {
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

let themeObserver = null;

// The page toggles dark mode with a class on <html>; redraw in the new palette.
export function observeThemeChanges(getDataFn, i18n) {
  if (themeObserver) themeObserver.disconnect();

  themeObserver = new MutationObserver(() => {
    const data = typeof getDataFn === "function" ? getDataFn() : getDataFn;
    if (data) renderCharts(data, i18n);
  });
  themeObserver.observe(document.documentElement, { attributes: true, attributeFilter: ["class"] });
}
