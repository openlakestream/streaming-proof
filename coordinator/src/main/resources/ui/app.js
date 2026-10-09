/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

// ── DOM refs ──────────────────────────────────────────────────────

const proofListEl        = document.getElementById("proof-list");
const refreshButton      = document.getElementById("refresh-button");
const lastUpdatedEl      = document.getElementById("last-updated");
const heroTitleEl        = document.getElementById("hero-title");
const heroSubtitleEl     = document.getElementById("hero-subtitle");
const heroStatusEl       = document.getElementById("hero-status");
const heroMetaEl         = document.getElementById("hero-meta");
const stopProofButton    = document.getElementById("stop-proof-button");
const heroProgressValueEl  = document.getElementById("hero-progress-value");
const heroProgressDetailEl = document.getElementById("hero-progress-detail");
const heroProgressFillEl   = document.getElementById("hero-progress-fill");
const emptyStateEl       = document.getElementById("empty-state");
const detailsViewEl      = document.getElementById("details-view");
const proofJsonLinkEl    = document.getElementById("proof-json-link");

const metricEls = {
  verified:    document.getElementById("metric-verified"),
  missed:      document.getElementById("metric-missed"),
  outOfOrders: document.getElementById("metric-out-of-orders"),
  duplicates:  document.getElementById("metric-duplicates"),
  errors:      document.getElementById("metric-errors"),
  timeouts:    document.getElementById("metric-timeouts"),
  verifyStall: document.getElementById("metric-verify-stall")
};

const perfEls = {
  publishRate:        document.getElementById("perf-publish-rate"),
  consumeRate:        document.getElementById("perf-consume-rate"),
  publishThroughput:  document.getElementById("perf-publish-throughput"),
  consumeThroughput:  document.getElementById("perf-consume-throughput"),
  publishErrorRate:   document.getElementById("perf-publish-error-rate"),
  backlog:            document.getElementById("perf-backlog"),
  publishLatencyP95:  document.getElementById("perf-publish-latency-p95"),
  publishLatencyP99:  document.getElementById("perf-publish-latency-p99"),
  endToEndLatencyP95: document.getElementById("perf-e2e-latency-p95"),
  endToEndLatencyP99: document.getElementById("perf-e2e-latency-p99")
};

const cfgEls = {
  driver:              document.getElementById("cfg-driver"),
  topic:               document.getElementById("cfg-topic"),
  partitions:          document.getElementById("cfg-partitions"),
  producers:           document.getElementById("cfg-producers"),
  consumers:           document.getElementById("cfg-consumers"),
  msgRate:             document.getElementById("cfg-msg-rate"),
  duration:            document.getElementById("cfg-duration"),
  checkpointInterval:  document.getElementById("cfg-checkpoint-interval"),
  timeout:             document.getElementById("cfg-timeout"),
  features:            document.getElementById("cfg-features")
};

const clusterTargetCountEl  = document.getElementById("cluster-target-count");
const clusterTargetsEl      = document.getElementById("cluster-targets");

// ── State ─────────────────────────────────────────────────────────

const initialUrlParams = new URLSearchParams(window.location.search);

let proofs          = [];
let selectedProofId = initialUrlParams.get("proofId");
let hasRequestedProofId = initialUrlParams.has("proofId");
let autoRefreshTimer = null;
let stopRequestProofId = null;
const chartInstances = {};

// ── Theme toggle ─────────────────────────────────────────────────

function getTheme() {
  return localStorage.getItem("sp-theme") || "dark";
}

function applyTheme(theme) {
  if (theme === "light") {
    document.documentElement.setAttribute("data-theme", "light");
  } else {
    document.documentElement.removeAttribute("data-theme");
  }
  // Re-render charts with updated colors
  Object.values(chartInstances).forEach(c => c.dispose());
  Object.keys(chartInstances).forEach(k => delete chartInstances[k]);
  if (selectedProofId) loadProofDetails(selectedProofId);
}

(function initTheme() {
  const saved = getTheme();
  if (saved === "light") {
    document.documentElement.setAttribute("data-theme", "light");
  }
  const toggleBtn = document.getElementById("theme-toggle");
  if (toggleBtn) {
    toggleBtn.addEventListener("click", () => {
      const current = getTheme();
      const next = current === "dark" ? "light" : "dark";
      localStorage.setItem("sp-theme", next);
      applyTheme(next);
    });
  }

  // Sidebar toggle - only available with ?sidebar=1 parameter
  const sidebarToggleBtn = document.getElementById("sidebar-toggle");
  const layoutEl = document.querySelector(".layout");
  if (sidebarToggleBtn && layoutEl) {
    // Check URL param for sidebar visibility
    const urlParams = new URLSearchParams(window.location.search);
    const sidebarParam = urlParams.get("sidebar");
    const showSidebar = sidebarParam === "1" || sidebarParam === "true";
    
    if (!showSidebar) {
      // Hide sidebar completely - remove toggle button
      sidebarToggleBtn.style.display = "none";
      layoutEl.classList.add("sidebar-collapsed");
    } else {
      // Show sidebar with toggle functionality
      // Restore saved state
      const savedCollapsed = localStorage.getItem("sp-sidebar-collapsed") === "true";
      if (savedCollapsed) {
        layoutEl.classList.add("sidebar-collapsed");
      }

      sidebarToggleBtn.addEventListener("click", () => {
        layoutEl.classList.toggle("sidebar-collapsed");
        const isCollapsed = layoutEl.classList.contains("sidebar-collapsed");
        localStorage.setItem("sp-sidebar-collapsed", isCollapsed);

        // Trigger chart resize after transition
        setTimeout(() => {
          Object.values(chartInstances).forEach(chart => chart.resize());
        }, 220);
      });
    }
  }
})();

/** Read a CSS variable from the current theme. */
function cssVar(name) {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

// ── ECharts helpers ───────────────────────────────────────────────

/**
 * Returns an ECharts instance for the given element id.
 * Creates and wires a ResizeObserver on first call.
 */
function getChart(id) {
  if (!chartInstances[id]) {
    const el = document.getElementById(id);
    chartInstances[id] = echarts.init(el, null, {renderer: "canvas"});
    new ResizeObserver(() => chartInstances[id].resize()).observe(el);
  }
  return chartInstances[id];
}

function buildChartTooltipFormatter(useIntegerFormatter = false, valueFormatter = null) {
  const labelColor   = cssVar("--chart-label");
  const tooltipText  = cssVar("--text");
  const mutedColor   = cssVar("--muted");

  return function formatter(params) {
    const sec = params[0]?.data?.[0] ?? 0;
    const timeLabel = formatDuration(sec);
    let rows = `<div style="color:${mutedColor};font-size:11px;margin-bottom:6px">${timeLabel}</div>`;
    for (const p of params) {
      let val;
      if (typeof p.data?.[1] !== "number") {
        val = "—";
      } else if (typeof valueFormatter === "function") {
        val = valueFormatter(p.data[1]);
      } else if (useIntegerFormatter) {
        val = Math.round(p.data[1]).toLocaleString();
      } else {
        val = p.data[1].toFixed(2);
      }
      rows += `<div style="display:flex;align-items:center;gap:8px;margin:2px 0">
        <span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:${p.color};flex-shrink:0"></span>
        <span style="color:${labelColor};flex:1">${p.seriesName}</span>
        <span style="font-weight:600;color:${tooltipText};padding-left:12px">${val}</span>
      </div>`;
    }
    return rows;
  };
}

/** Shared base option applied to every chart. */
function baseChartOption(useIntegerFormatter = false, xAxisMax = null) {
  const gridColor    = cssVar("--chart-grid");
  const labelColor   = cssVar("--chart-label");
  const tooltipBg    = cssVar("--surface");
  const tooltipBorder = cssVar("--border-strong");
  const tooltipText  = cssVar("--text");
  const pointerColor = cssVar("--chart-pointer");

  return {
    backgroundColor: "transparent",
    animation: true,
    animationDuration: 400,
    grid: {top: 12, right: 14, bottom: 58, left: 56},
    tooltip: {
      trigger: "axis",
      backgroundColor: tooltipBg,
      borderColor: tooltipBorder,
      borderWidth: 1,
      padding: [10, 14],
      textStyle: {
        color: tooltipText,
        fontSize: 12,
        fontFamily: "'JetBrains Mono', monospace"
      },
      axisPointer: {
        lineStyle: {color: pointerColor, width: 1}
      },
      formatter: buildChartTooltipFormatter(useIntegerFormatter)
    },
    xAxis: {
      type: "value",
      min: 0,
      max: Number.isFinite(xAxisMax) && xAxisMax > 0 ? xAxisMax : null,
      boundaryGap: [0, 0],
      axisLabel: {
        color: labelColor,
        fontSize: 11,
        fontFamily: "'JetBrains Mono', monospace",
        formatter: (v) => formatSecondsShort(v)
      },
      axisLine: {lineStyle: {color: gridColor}},
      splitLine: {lineStyle: {color: gridColor, type: "dashed"}},
      minorSplitLine: {show: false}
    },
    yAxis: {
      type: "value",
      axisLabel: {
        color: labelColor,
        fontSize: 11,
        fontFamily: "'JetBrains Mono', monospace"
      },
      axisLine: {show: false},
      splitLine: {lineStyle: {color: gridColor, type: "dashed"}}
    }
  };
}

/** Builds a smooth area line series config. */
function areaSeries(name, color, data) {
  return {
    name,
    type: "line",
    smooth: 0.4,
    showSymbol: false,
    data,
    lineStyle: {color, width: 1.8},
    itemStyle: {color},
    areaStyle: {
      color: {
        type: "linear", x: 0, y: 0, x2: 0, y2: 1,
        colorStops: [
          {offset: 0, color: color + "28"},
          {offset: 1, color: color + "04"}
        ]
      }
    }
  };
}

function resolveTimeAxisMax(timeSeries, plannedDurationSeconds = 0) {
  const lastElapsedSeconds = timeSeries.length > 0
    ? Number(timeSeries[timeSeries.length - 1]?.elapsedSeconds || 0)
    : 0;
  const planned = Number(plannedDurationSeconds || 0);
  const axisMax = Math.max(lastElapsedSeconds, planned);
  return axisMax > 0 ? axisMax : null;
}

/** Renders message rate (publish + consume msg/s) chart. */
function renderRateChart(timeSeries, xAxisMax) {
  const chart = getChart("rate-chart");
  const opt = baseChartOption(false, xAxisMax);
  opt.legend = legendOption(["Publish Rate", "Consume Rate"]);
  opt.series = [
    areaSeries("Publish Rate", "#60a5fa", timeSeries.map((p) => [p.elapsedSeconds, Number(p.publishRate || 0)])),
    areaSeries("Consume Rate", "#34d399", timeSeries.map((p) => [p.elapsedSeconds, Number(p.consumeRate || 0)]))
  ];
  chart.setOption(opt, {notMerge: true});
}

/** Renders throughput (publish + consume bytes/s) chart. */
function renderThroughputChart(timeSeries, xAxisMax) {
  const chart = getChart("throughput-chart");
  const opt = baseChartOption(false, xAxisMax);
  opt.legend = legendOption(["Publish", "Consume"]);
  opt.tooltip.valueFormatter = (v) => formatBytes(v) + "/s";
  opt.yAxis.axisLabel = {
    color: cssVar("--chart-label"),
    fontSize: 10,
    formatter: (v) => formatBytes(v) + "/s"
  };
  opt.series = [
    areaSeries("Publish", "#f97316", timeSeries.map((p) => [p.elapsedSeconds, Number(p.publishBytesRate || 0)])),
    areaSeries("Consume", "#a78bfa", timeSeries.map((p) => [p.elapsedSeconds, Number(p.consumeBytesRate || 0)]))
  ];
  chart.setOption(opt, {notMerge: true});
}

function formatBytes(bytes) {
  if (bytes == null || bytes === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB"];
  const i = Math.min(Math.floor(Math.log(Math.abs(bytes)) / Math.log(1024)), units.length - 1);
  const val = bytes / Math.pow(1024, i);
  return val.toFixed(val < 10 ? 2 : 1) + " " + units[i];
}

/** Renders backlog chart. */
function renderBacklogChart(timeSeries, xAxisMax) {
  const chart = getChart("backlog-chart");
  const opt = baseChartOption(true, xAxisMax);
  opt.legend = legendOption(["Backlog"]);
  opt.series = [
    areaSeries("Backlog", "#fbbf24", timeSeries.map((p) => [p.elapsedSeconds, Number(p.backlogMessages || 0)]))
  ];
  chart.setOption(opt, {notMerge: true});
}

/** Renders cumulative messages (published / consumed / verified) chart. */
function renderMessagesChart(timeSeries, xAxisMax) {
  const chart = getChart("messages-chart");
  const opt = baseChartOption(true, xAxisMax);
  opt.legend = legendOption(["Published", "Consumed", "Verified"]);
  opt.series = [
    areaSeries("Published", "#60a5fa", timeSeries.map((p) => [p.elapsedSeconds, Number(p.publishedMessages || 0)])),
    areaSeries("Consumed",  "#34d399", timeSeries.map((p) => [p.elapsedSeconds, Number(p.consumedMessages || 0)])),
    areaSeries("Verified",  "#fbbf24", timeSeries.map((p) => [p.elapsedSeconds, Number(p.verifiedMessages || 0)]))
  ];
  chart.setOption(opt, {notMerge: true});
}

/** Renders cumulative error count chart. */
function renderErrorsChart(timeSeries, xAxisMax) {
  const chart = getChart("errors-chart");
  const opt = baseChartOption(true, xAxisMax);
  opt.legend = legendOption(["Errors", "Timeouts"]);
  opt.series = [
    areaSeries("Errors", "#f97316", timeSeries.map((p) => [p.elapsedSeconds, Number(p.errors || 0)])),
    areaSeries("Timeouts", "#fb7185", timeSeries.map((p) => [p.elapsedSeconds, Number(p.timeouts || 0)]))
  ];
  chart.setOption(opt, {notMerge: true});
}

/** Renders unresolved anomalies (missed / duplicates / out-of-order) chart. */
function renderAnomaliesChart(timeSeries, xAxisMax) {
  const chart = getChart("anomalies-chart");
  const opt = baseChartOption(true, xAxisMax);
  opt.legend = legendOption(["Missed", "Duplicates", "Out-of-Order"]);
  opt.series = [
    areaSeries("Missed",       "#f87171", timeSeries.map((p) => [p.elapsedSeconds, Number(p.missed || 0)])),
    areaSeries("Duplicates",   "#fbbf24", timeSeries.map((p) => [p.elapsedSeconds, Number(p.duplicates || 0)])),
    areaSeries("Out-of-Order", "#a78bfa", timeSeries.map((p) => [p.elapsedSeconds, Number(p.outOfOrders || 0)]))
  ];
  chart.setOption(opt, {notMerge: true});
}

/** Renders publish latency (P95 + P99) chart. */
function renderPublishLatencyChart(timeSeries, xAxisMax) {
  const chart = getChart("publish-latency-chart");
  const opt = baseChartOption(false, xAxisMax);
  opt.legend = legendOption(["P95", "P99"]);
  opt.tooltip.formatter = buildChartTooltipFormatter(false, (value) => formatLatency(value));
  opt.yAxis.axisLabel = {
    color: cssVar("--chart-label"),
    fontSize: 11,
    fontFamily: "'JetBrains Mono', monospace",
    formatter: (value) => formatLatency(value, true)
  };
  opt.series = [
    areaSeries("P95", "#f97316", timeSeries.map((p) => [p.elapsedSeconds, Number(p.publishLatencyP95 || 0)])),
    areaSeries("P99", "#a78bfa", timeSeries.map((p) => [p.elapsedSeconds, Number(p.publishLatencyP99 || 0)]))
  ];
  chart.setOption(opt, {notMerge: true});
}

/** Renders end-to-end latency (P95 + P99) chart. */
function renderE2ELatencyChart(timeSeries, xAxisMax) {
  const chart = getChart("e2e-latency-chart");
  const opt = baseChartOption(false, xAxisMax);
  opt.legend = legendOption(["P95", "P99"]);
  opt.tooltip.formatter = buildChartTooltipFormatter(false, (value) => formatLatency(value));
  opt.yAxis.axisLabel = {
    color: cssVar("--chart-label"),
    fontSize: 11,
    fontFamily: "'JetBrains Mono', monospace",
    formatter: (value) => formatLatency(value, true)
  };
  opt.series = [
    areaSeries("P95", "#f87171", timeSeries.map((p) => [p.elapsedSeconds, Number(p.endToEndLatencyP95 || 0)])),
    areaSeries("P99", "#22d3ee", timeSeries.map((p) => [p.elapsedSeconds, Number(p.endToEndLatencyP99 || 0)]))
  ];
  chart.setOption(opt, {notMerge: true});
}

function legendOption(names) {
  return {
    data: names,
    bottom: 4,
    left: "center",
    textStyle: {color: cssVar("--chart-label"), fontSize: 11},
    itemWidth: 10,
    itemHeight: 10,
    itemGap: 16,
    icon: "circle"
  };
}

// ── Format helpers ────────────────────────────────────────────────

function formatNumber(value) {
  if (typeof value === "number") {
    return new Intl.NumberFormat().format(value);
  }
  return value == null ? "—" : String(value);
}

function formatDecimal(value) {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return "—";
  }
  return new Intl.NumberFormat(undefined, {minimumFractionDigits: 0, maximumFractionDigits: 2}).format(value);
}

function formatScaledNumber(value, maximumFractionDigits) {
  return new Intl.NumberFormat(undefined, {
    minimumFractionDigits: 0,
    maximumFractionDigits
  }).format(value);
}

function formatLatency(valueMillis, compact = false) {
  if (typeof valueMillis !== "number" || Number.isNaN(valueMillis) || valueMillis < 0) {
    return "—";
  }
  const absValue = Math.abs(valueMillis);
  const unitGap = compact ? "" : " ";

  if (absValue < 1000) {
    const digits = absValue < 10 ? 2 : absValue < 100 ? 1 : 0;
    return `${formatScaledNumber(valueMillis, digits)}${unitGap}ms`;
  }

  if (absValue < 60_000) {
    const seconds = valueMillis / 1000;
    const digits = Math.abs(seconds) < 10 ? 2 : 1;
    return `${formatScaledNumber(seconds, digits)}${unitGap}s`;
  }

  const totalSeconds = Math.round(valueMillis / 1000);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;

  if (hours > 0) {
    return minutes > 0 ? `${hours}h ${minutes}m` : `${hours}h`;
  }

  return seconds > 0 ? `${minutes}m ${seconds}s` : `${minutes}m`;
}

function formatDuration(seconds) {
  if (typeof seconds !== "number" || Number.isNaN(seconds) || seconds < 0) {
    return "—";
  }
  const s = Math.round(seconds);
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const r = s % 60;
  if (h > 0) {
    return `${h}h ${m}m ${r}s`;
  }
  if (m > 0) {
    return `${m}m ${r}s`;
  }
  return `${r}s`;
}

function formatSecondsShort(sec) {
  const s = Math.round(sec);
  if (s >= 3600) {
    const h = Math.floor(s / 3600);
    const m = Math.floor((s % 3600) / 60);
    return m > 0 ? `${h}h ${m}m` : `${h}h`;
  }
  if (s >= 60) {
    return `${Math.floor(s / 60)}m`;
  }
  return `${s}s`;
}

function formatValue(value) {
  if (value === null || value === undefined || value === "") {
    return "—";
  }
  if (Array.isArray(value)) {
    return value.join(", ");
  }
  return String(value);
}

// ── KPI card state ────────────────────────────────────────────────

/** Updates KPI card styling based on whether the anomaly count is zero. */
function updateKpiCard(cardId, count) {
  const el = document.getElementById(cardId);
  if (!el) {
    return;
  }
  el.classList.remove("kpi-danger", "kpi-warn");
  if (count > 0) {
    el.classList.add("kpi-danger");
  }
}

function updateKpiCardWarn(cardId, count) {
  const el = document.getElementById(cardId);
  if (!el) {
    return;
  }
  el.classList.remove("kpi-danger", "kpi-warn");
  if (count > 0) {
    el.classList.add("kpi-warn");
  }
}

// ── Config summary ───────────────────────────────────────────────

const cfgDescEl = document.getElementById("cfg-desc");

/** Human-readable feature labels */
const FEATURE_LABELS = {
  exactly_once:  "Exactly-Once Delivery",
  at_least_once: "At-Least-Once Delivery",
  ordering:      "Message Ordering",
};

function featureLabel(f) {
  return FEATURE_LABELS[f] || String(f).replace(/_/g, " ");
}

function formatStartTime(value) {
  if (!value) {
    return null;
  }
  return String(value).replace("T", " ").replace(/\.\d+$/, "");
}

function resolveDriverInfo(proof, clusterTargets) {
  const targets = Array.isArray(clusterTargets) ? clusterTargets : [];
  const roleDrivers = proof.drivers || {};
  const hasRoleDrivers = ["admin", "producer", "consumer"].some((key) => !!roleDrivers[key]);
  const driverNames = [];

  if (hasRoleDrivers) {
    for (const key of ["admin", "producer", "consumer"]) {
      const driverName = roleDrivers[key];
      if (driverName) {
        driverNames.push(driverName);
      }
    }
  } else if (proof.driver) {
    driverNames.push(proof.driver);
  }

  const uniqueDriverNames = [...new Set(driverNames.filter(Boolean))];
  const uniqueDriverTypes = [...new Set(
    uniqueDriverNames.map((driverName) => {
      const target = targets.find((t) => t.driverName === driverName) || {};
      return target.driverType || null;
    }).filter(Boolean)
  )];

  return {
    driverNames: uniqueDriverNames,
    driverTypes: uniqueDriverTypes,
    driverNameText: uniqueDriverNames.length > 0 ? uniqueDriverNames.join(", ") : "unknown",
    driverTypeText: uniqueDriverTypes.length > 0 ? uniqueDriverTypes.join(", ") : "Unknown"
  };
}

function renderConfigSummary(proof, clusterTargets) {
  if (!cfgDescEl) {
    return;
  }

  const p = {
    partitions: proof.partitions ?? proof.pulsar?.initialSegments ?? "?",
    partitionsLabel: proof.pulsar?.scalableTopicEnabled ? "segments" : "partitions",
    producers:  proof.producers ?? "?",
    consumers:  proof.consumers ?? "?",
    rate:       proof.msgRate ?? "?",
    duration:   proof.duration != null ? `${proof.duration}s` : "?",
    checkpoint: proof.checkPointInterval != null ? `${proof.checkPointInterval}s` : "?",
    timeout:    proof.timeout != null ? `${proof.timeout}s` : "?",
    finalWait: proof.finalWaitSeconds != null ? `${proof.finalWaitSeconds}s` : "—",
    maxStall:  proof.maxStallSeconds ? `${proof.maxStallSeconds}s` : "—",
    features:   Array.isArray(proof.features) ? proof.features : [],
  };

  // Build guarantee chips
  const guarantees = p.features.length > 0
    ? p.features.map(f => `<span class="cfg-guarantee">${escapeHtml(featureLabel(f))}</span>`).join("")
    : `<span class="cfg-guarantee cfg-guarantee-dim">No guarantees configured</span>`;

  cfgDescEl.innerHTML = `
    <div class="cfg-compact-row">
      <div class="cfg-compact-item">
        <span class="cfg-compact-label">Topic</span>
        <span class="cfg-compact-value">
          <span class="cfg-compact-em">${escapeHtml(String(proof.topic || "Not set"))}</span>
        </span>
      </div>
      <div class="cfg-compact-item">
        <span class="cfg-compact-label">Publish Rate</span>
        <span class="cfg-compact-value">
          <span class="cfg-compact-em">${escapeHtml(String(p.rate))}</span>
          <span class="cfg-compact-unit">msg/s</span>
        </span>
      </div>
      <div class="cfg-compact-item">
        <span class="cfg-compact-label">Topology</span>
        <span class="cfg-compact-value cfg-compact-value-topology">
          <span><span class="cfg-compact-em">${escapeHtml(String(p.producers))}</span> producers</span>
          <span class="cfg-compact-dot"></span>
          <span><span class="cfg-compact-em">${escapeHtml(String(p.partitions))}</span> ${p.partitionsLabel}</span>
          <span class="cfg-compact-dot"></span>
          <span><span class="cfg-compact-em">${escapeHtml(String(p.consumers))}</span> consumers</span>
        </span>
      </div>
      <div class="cfg-compact-item">
        <span class="cfg-compact-label">Window</span>
        <span class="cfg-compact-value">
          checkpoint <span class="cfg-compact-em">${escapeHtml(p.checkpoint)}</span>
          <span class="cfg-compact-dot"></span>
          timeout <span class="cfg-compact-em">${escapeHtml(p.timeout)}</span>
          <span class="cfg-compact-dot"></span>
          final wait <span class="cfg-compact-em">${escapeHtml(p.finalWait)}</span>
          <span class="cfg-compact-dot"></span>
          max stall <span class="cfg-compact-em">${escapeHtml(p.maxStall)}</span>
        </span>
      </div>
      <div class="cfg-compact-item cfg-compact-item-guarantees">
        <span class="cfg-compact-label">Guarantees</span>
        <div class="cfg-guarantees">${guarantees}</div>
      </div>
    </div>
  `;
}

// ── Cluster targets ───────────────────────────────────────────────

function escapeHtml(str) {
  return String(str)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

/** Builds a condensed one-line summary for a component. e.g. "3 replicas · CPU 4 · MEM 8Gi" */
function componentSummaryLine(component) {
  if (!component || typeof component !== "object") {
    return null;
  }
  const parts = [];
  if (component.replicas != null) {
    parts.push(`<strong>${component.replicas}</strong> replicas`);
  }
  const limits = component.resources?.limits || {};
  if (limits.cpu)    { parts.push(`${escapeHtml(limits.cpu)} vCPU`); }
  if (limits.memory) { parts.push(`${escapeHtml(limits.memory)}`); }
  return parts.length > 0 ? parts.join(" · ") : null;
}

/** Builds a condensed storage summary line. e.g. "Journal ssd-retain 100Gi · Ledger ssd-retain 500Gi" */
function storageSummaryLine(component) {
  if (!component || typeof component !== "object") {
    return null;
  }
  const fields = [
    ["Storage", component.storage],
    ["Journal", component.journal],
    ["Ledger",  component.ledger],
    ["Data",    component.data],
    ["DataLog", component.dataLog]
  ];
  const parts = [];
  for (const [label, block] of fields) {
    if (!block || typeof block !== "object") { continue; }
    const pieces = [label + ":"];
    if (block.storageClassName)  { pieces.push(escapeHtml(block.storageClassName)); }
    if (block.requests?.storage) { pieces.push(escapeHtml(block.requests.storage)); }
    if (pieces.length > 1) { parts.push(pieces.join(" ")); }
  }
  return parts.length > 0 ? parts.join(" · ") : null;
}

/** Builds a JVM options line. e.g. "-Xms 48g / -Xmx 64g / MaxDirectMemory 32g" */
function jvmSummaryLine(component) {
  if (!component || typeof component !== "object") {
    return null;
  }
  const jvm = component.jvmOptions;
  if (!jvm || typeof jvm !== "object" || Object.keys(jvm).length === 0) {
    return null;
  }
  return "JVM: " + Object.entries(jvm).map(([k, v]) => `${escapeHtml(k)} ${escapeHtml(v)}`).join(" / ");
}

/** Renders a single component card (Broker / BookKeeper / ZooKeeper / Oxia). */
function renderComponentCard(title, component) {
  if (!component || typeof component !== "object") {
    return "";
  }
  const main    = componentSummaryLine(component);
  const storage = storageSummaryLine(component);
  const jvm     = jvmSummaryLine(component);
  if (!main && !storage && !jvm) {
    return "";
  }
  return `
    <div class="res-card">
      <div class="res-card-title">${escapeHtml(title)}</div>
      ${main    ? `<div class="res-card-line">${main}</div>` : ""}
      ${storage ? `<div class="res-card-sub">${storage}</div>` : ""}
      ${jvm     ? `<div class="res-card-sub">${jvm}</div>` : ""}
    </div>`;
}

function isSafeWebUrl(value) {
  if (typeof value !== "string") {
    return false;
  }
  try {
    const url = new URL(value.trim());
    return (url.protocol === "http:" || url.protocol === "https:") && Boolean(url.hostname);
  } catch {
    return false;
  }
}

function flattenMetadataEntries(value, prefix = "") {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return prefix ? [[prefix, value]] : [];
  }

  return Object.entries(value).flatMap(([key, child]) => {
    const path = prefix ? `${prefix}.${key}` : key;
    if (child && typeof child === "object" && !Array.isArray(child)) {
      const nested = flattenMetadataEntries(child, path);
      return nested.length > 0 ? nested : [[path, child]];
    }
    return [[path, child]];
  });
}

function renderMetadataValue(value) {
  if (isSafeWebUrl(value)) {
    const url = value.trim();
    return `<a class="custom-metadata-link" href="${escapeHtml(url)}" target="_blank" rel="noopener noreferrer">${escapeHtml(url)}</a>`;
  }
  if (Array.isArray(value)) {
    return value.map(renderMetadataValue).join(`<span class="custom-metadata-separator">, </span>`);
  }
  if (value && typeof value === "object") {
    return escapeHtml(JSON.stringify(value));
  }
  return escapeHtml(formatValue(value));
}

function renderCustomMetadata(metadata) {
  const customMetadata = Object.fromEntries(
    Object.entries(metadata).filter(([key]) => key !== "clusterResources" && key !== "pulsarConfig")
  );
  const entries = flattenMetadataEntries(customMetadata)
    .sort(([a], [b]) => a.localeCompare(b));

  if (entries.length === 0) {
    return "";
  }

  return `
    <div class="custom-metadata-section">
      <div class="adv-config-title">Metadata</div>
      <dl class="custom-metadata-list">
        ${entries.map(([key, value]) => `
          <div class="custom-metadata-row">
            <dt>${escapeHtml(key)}</dt>
            <dd>${renderMetadataValue(value)}</dd>
          </div>
        `).join("")}
      </dl>
    </div>`;
}

function renderClusterTargets(targets) {
  if (!Array.isArray(targets) || targets.length === 0) {
    clusterTargetCountEl.textContent = "0";
    clusterTargetsEl.innerHTML = `<p class="cluster-empty">No cluster target info attached to this run yet.</p>`;
    return;
  }

  clusterTargetCountEl.textContent = targets.length;

  clusterTargetsEl.innerHTML = targets.map((target) => {
    const metadata         = target.metadata || {};
    const clusterResources = metadata.clusterResources || {};
    const pulsarConfig     = metadata.pulsarConfig || {};

    // Metadata card: Oxia or ZooKeeper (they serve the same coordination role)
    const metadataSource = clusterResources.oxia || clusterResources.zookeeper;
    const metadataLabel  = clusterResources.oxia ? "Metadata (Oxia)" : "Metadata (ZooKeeper)";

    // Component resource cards
    const cards = [
      renderComponentCard("Brokers",    clusterResources.broker),
      renderComponentCard("BookKeeper", clusterResources.bookkeeper),
      metadataSource ? renderComponentCard(metadataLabel, metadataSource) : ""
    ].filter(Boolean).join("");

    // Pulsar advanced config (flat key: "value" list)
    const pulsarEntries = Object.entries(pulsarConfig).sort(([a], [b]) => a.localeCompare(b));
    const pulsarBlock = pulsarEntries.length > 0
      ? `<div class="adv-config-section">
           <div class="adv-config-title">Broker Advanced Config</div>
           <div class="adv-config-list">${pulsarEntries.map(([k, v]) =>
             `<div class="adv-config-row">${escapeHtml(k)}: <span class="adv-config-val">"${escapeHtml(formatValue(v))}"</span></div>`
           ).join("")}</div>
         </div>`
      : "";

    const customMetadataBlock = renderCustomMetadata(metadata);
    const resourceCardsBlock = cards.length > 0
      ? `<div class="res-card-row">${cards}</div>`
      : "";
    const hasContent = cards.length > 0 || pulsarEntries.length > 0 || customMetadataBlock.length > 0;

    const emptyNotice = !hasContent
      ? `<p class="cluster-empty">No cluster resource metadata is available for this run.</p>`
      : "";

    return `
      <div class="cluster-target-card">
        <div class="cluster-target-header">
          <span class="cluster-target-role">${escapeHtml(target.role || "default")}</span>
          <span class="cluster-target-name">${escapeHtml(target.driverName || "unnamed")}</span>
          <span class="cluster-target-type">${escapeHtml(target.driverType || "unknown")}</span>
        </div>
        ${hasContent ? `
          ${resourceCardsBlock}
          ${customMetadataBlock}
          ${pulsarBlock}
        ` : emptyNotice}
      </div>`;
  }).join("");
}

// ── URL state ─────────────────────────────────────────────────────

function setSelectedProofId(proofId) {
  selectedProofId = proofId;
  hasRequestedProofId = Boolean(proofId);
  const url = new URL(window.location.href);
  if (proofId) {
    url.searchParams.set("proofId", proofId);
  } else {
    url.searchParams.delete("proofId");
  }
  window.history.replaceState({}, "", url);
}

// ── Auto-refresh ──────────────────────────────────────────────────

function scheduleAutoRefresh(running) {
  clearTimeout(autoRefreshTimer);
  autoRefreshTimer = null;
  if (running) {
    autoRefreshTimer = setTimeout(() => {
      void loadProofs();
    }, 5000);
  }
}

// ── Render proof list ─────────────────────────────────────────────

function renderProofList() {
  if (proofs.length === 0) {
    proofListEl.innerHTML = `
      <div class="proof-item">
        <p class="proof-item-title">No active proofs</p>
        <p class="proof-item-meta">Start a proof and refresh.</p>
      </div>`;
    return;
  }

  proofListEl.innerHTML = proofs.map((proof) => {
    const active = proof.id === selectedProofId ? " active" : "";
    const status = proof._status || "unknown";
    const statusClass = status.toLowerCase();
    return `
      <button class="proof-item${active} status-${statusClass}" data-proof-id="${proof.id}" type="button">
        <span class="status-indicator"></span>
        <div class="proof-item-content">
          <p class="proof-item-title">${proof.name || proof.id}</p>
          <p class="proof-item-meta">${proof.id} · ${resolveDriverInfo(proof, []).driverNameText}</p>
        </div>
      </button>`;
  }).join("");

  proofListEl.querySelectorAll("[data-proof-id]").forEach((btn) => {
    btn.addEventListener("click", () => {
      setSelectedProofId(btn.dataset.proofId);
      renderProofList();
      void loadProofDetails(selectedProofId);
    });
  });
}

function syncProofInList(proof, status) {
  if (!proof?.id) {
    return;
  }

  const index = proofs.findIndex(item => item.id === proof.id);
  if (index === -1) {
    return;
  }

  const previous = proofs[index];
  const next = {
    ...previous,
    ...proof,
    _status: status || previous._status || "unknown"
  };

  proofs[index] = next;

  if (previous._status !== next._status
      || previous.name !== next.name
      || previous.driver !== next.driver) {
    renderProofList();
  }
}

// ── Empty state ───────────────────────────────────────────────────

function showEmptyState(title, subtitle) {
  heroTitleEl.textContent = title;
  heroSubtitleEl.textContent = subtitle;
  heroSubtitleEl.classList.remove("hidden");
  heroStatusEl.textContent = "waiting";
  heroStatusEl.className = "status-badge";
  if (heroMetaEl) {
    heroMetaEl.innerHTML = "";
  }
  heroProgressValueEl.textContent = "—";
  heroProgressDetailEl.innerHTML = "—";
  heroProgressFillEl.style.width = "0%";
  updateStopButton(null, "unknown");
  emptyStateEl.classList.remove("hidden");
  detailsViewEl.classList.add("hidden");
  scheduleAutoRefresh(false);
}

// ── Load proof list ───────────────────────────────────────────────

async function loadProofs() {
  const response = await fetch("/proofs");
  if (!response.ok) {
    throw new Error(`Failed to load proofs: ${response.status}`);
  }
  const proofItems = await response.json();
  proofs = proofItems.map(item => ({
    ...item,
    _status: item.status || "unknown"
  }));

  const selectedProofExists = selectedProofId && proofs.some(p => p.id === selectedProofId);
  renderProofList();

  if (selectedProofId && (selectedProofExists || hasRequestedProofId)) {
    await loadProofDetails(selectedProofId);
  } else {
    showEmptyState("No proof selected", "Select a proof from the sidebar to inspect details.");
  }
}

// ── Load proof details ────────────────────────────────────────────

async function loadProofDetails(proofId) {
  if (!proofId) {
    showEmptyState("No proof selected", "Choose a proof from the sidebar to inspect details.");
    return;
  }

  const response = await fetch(`/proofs/${proofId}/report`);
  if (!response.ok) {
    showEmptyState("Proof not found", `The proof ${proofId} is no longer available from the coordinator.`);
    return;
  }

  const data = await response.json();
  const proof              = data.proof || {};
  const summary            = data.summary || {};
  const performanceSummary = data.performanceSummary || {};
  const timeSeries         = Array.isArray(data.timeSeries) ? data.timeSeries : [];
  const resultStatus       = data.status || "unknown";
  const clusterTargets     = Array.isArray(data.clusterTargets) ? data.clusterTargets : [];

  syncProofInList(proof, resultStatus);
  updateStopButton(proofId, resultStatus);

  const elapsedSeconds          = Number(performanceSummary.elapsedSeconds || 0);
  const plannedDurationSeconds  = Number(performanceSummary.plannedDurationSeconds || proof.duration || 0);
  const remainingSeconds        = Number(
    performanceSummary.remainingSeconds ?? Math.max(0, plannedDurationSeconds - elapsedSeconds)
  );
  const rawProgress = plannedDurationSeconds > 0
    ? Math.max(0, Math.min(100, (elapsedSeconds / plannedDurationSeconds) * 100))
    : Math.max(0, Math.min(100, Number(performanceSummary.progressPercent || 0)));
  // Cap at 99.99% while still running; only show 100% once fully stopped
  const progressPercent = (resultStatus === "running" && rawProgress >= 100) ? 99.99 : rawProgress;
  const startedAt = formatStartTime(proof.startTime);
  const {driverNameText, driverTypeText} = resolveDriverInfo(proof, clusterTargets);

  // Hero
  heroTitleEl.textContent    = proof.name || proof.id || proofId;
  heroSubtitleEl.textContent = [
    resultStatus || "unknown",
    data.resultReason || "Live verification summary"
  ].filter(Boolean).join(" · ");
  heroSubtitleEl.classList.remove("hidden");

  if (heroMetaEl) {
    heroMetaEl.innerHTML = [
      {label: "Driver Type", value: driverTypeText.toUpperCase(), className: "hero-meta-item-type"},
      {label: "Driver", value: driverNameText, className: "hero-meta-item-driver"}
    ].map((item) => `
      <div class="hero-meta-item ${item.className}">
        <span class="hero-meta-label">${escapeHtml(item.label)}</span>
        <span class="hero-meta-value">${escapeHtml(item.value)}</span>
      </div>
    `).join("");
  }

  heroStatusEl.textContent = resultStatus || "unknown";
  heroStatusEl.className   = `status-badge ${String(resultStatus || "").toLowerCase()}`.trim();

  heroProgressValueEl.textContent  = `${formatDecimal(progressPercent)}%`;
  heroProgressDetailEl.innerHTML = [
    {label: "Started", value: startedAt || "Pending"},
    {label: "Elapsed", value: formatDuration(elapsedSeconds)},
    {label: "Remaining", value: formatDuration(remainingSeconds)},
    {label: "Total", value: formatDuration(plannedDurationSeconds)}
  ].map((item) => `
    <span class="progress-detail-item">
      <span class="progress-detail-label">${escapeHtml(item.label)}</span>
      <span class="progress-detail-value">${escapeHtml(item.value)}</span>
    </span>
  `).join('<span class="progress-detail-sep">·</span>');
  heroProgressFillEl.style.width = `${progressPercent}%`;

  // Reliability KPIs
  const missed      = Number(summary.missed      || 0);
  const outOfOrders = Number(summary.outOfOrders || 0);
  const duplicates  = Number(summary.duplicates  || 0);
  const errors      = Number(summary.errors      || 0);
  const timeouts    = Number(summary.timeouts    || 0);
  const verifyStall = Number(summary.verifiedStallSeconds || 0);
  const peakStall = Number(summary.maxVerifiedStallSeconds || 0);
  const stallLimit = Number(proof.maxStallSeconds || 0);

  metricEls.verified.textContent    = formatNumber(summary.verified    || 0);
  metricEls.missed.textContent      = formatNumber(missed);
  metricEls.outOfOrders.textContent = formatNumber(outOfOrders);
  metricEls.duplicates.textContent  = formatNumber(duplicates);
  metricEls.errors.textContent      = formatNumber(errors);
  metricEls.timeouts.textContent    = formatNumber(timeouts);
  metricEls.verifyStall.textContent = formatDuration(verifyStall);
  const stallHintEl = document.getElementById("metric-verify-stall-hint");
  if (stallHintEl) {
    stallHintEl.textContent = "peak " + formatDuration(peakStall);
  }

  updateKpiCard("kpi-card-missed",  missed);
  updateKpiCard("kpi-card-ooo",     outOfOrders);
  updateKpiCardWarn("kpi-card-dups", duplicates);
  updateKpiCard("kpi-card-errors",  errors);
  updateKpiCardWarn("kpi-card-timeouts", timeouts);
  // Highlight as danger only when a stall limit is configured and the peak stall
  // exceeded it — this mirrors the run's stall verdict. With no limit set, the
  // card never highlights.
  updateKpiCard("kpi-card-verify-stall",
    stallLimit > 0 && peakStall > stallLimit ? 1 : 0);

  // Performance
  perfEls.publishRate.textContent        = formatDecimal(performanceSummary.publishRate || 0);
  perfEls.consumeRate.textContent        = formatDecimal(performanceSummary.consumeRate || 0);
  perfEls.publishThroughput.textContent  = formatBytes(performanceSummary.publishBytesRate || 0);
  perfEls.consumeThroughput.textContent  = formatBytes(performanceSummary.consumeBytesRate || 0);
  perfEls.publishErrorRate.textContent   = formatNumber(performanceSummary.publishErrors || 0);
  perfEls.backlog.textContent            = formatNumber(performanceSummary.backlogMessages || 0);
  perfEls.publishLatencyP95.textContent  = formatLatency(performanceSummary.publishLatency?.p95 || 0);
  perfEls.publishLatencyP99.textContent  = formatLatency(performanceSummary.publishLatency?.p99 || 0);
  perfEls.endToEndLatencyP95.textContent = formatLatency(performanceSummary.endToEndLatency?.p95 || 0);
  perfEls.endToEndLatencyP99.textContent = formatLatency(performanceSummary.endToEndLatency?.p99 || 0);

  // Charts
  if (timeSeries.length >= 2) {
    const xAxisMax = resolveTimeAxisMax(timeSeries, plannedDurationSeconds);
    renderRateChart(timeSeries, xAxisMax);
    renderThroughputChart(timeSeries, xAxisMax);
    renderMessagesChart(timeSeries, xAxisMax);
    renderErrorsChart(timeSeries, xAxisMax);
    renderAnomaliesChart(timeSeries, xAxisMax);
    renderBacklogChart(timeSeries, xAxisMax);
    renderPublishLatencyChart(timeSeries, xAxisMax);
    renderE2ELatencyChart(timeSeries, xAxisMax);
  } else {
    renderChartsEmpty();
  }

  // Config summary
  renderConfigSummary(proof, clusterTargets);
  renderClusterTargets(clusterTargets);

  if (proofJsonLinkEl) proofJsonLinkEl.href = `/proofs/${proofId}`;


  // Show details
  emptyStateEl.classList.add("hidden");
  detailsViewEl.classList.remove("hidden");

  // Update timestamp
  lastUpdatedEl.textContent = `Updated ${new Date().toLocaleTimeString()}`;

  // Auto-refresh when running
  scheduleAutoRefresh(resultStatus === "running" || resultStatus === "stopping");
}

/** Shows a minimal placeholder in each chart when there is not enough data yet. */
function renderChartsEmpty() {
  for (const id of ["rate-chart", "throughput-chart", "messages-chart", "errors-chart", "anomalies-chart", "backlog-chart", "publish-latency-chart", "e2e-latency-chart"]) {
    const chart = getChart(id);
    chart.setOption({
      backgroundColor: "transparent",
      graphic: [{
        type: "text",
        left: "center",
        top: "middle",
        style: {
          text: "Waiting for checkpoints…",
          fill: cssVar("--muted"),
          fontSize: 12,
          fontFamily: "'DM Sans', sans-serif"
        }
      }],
      series: []
    }, {notMerge: true});
  }
}

function updateStopButton(proofId, status) {
  if (!stopProofButton) {
    return;
  }

  if (!proofId || status !== "running") {
    stopProofButton.classList.add("hidden");
    stopProofButton.disabled = false;
    stopProofButton.innerHTML = `
      <svg width="12" height="12" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true">
        <rect x="6" y="6" width="12" height="12" rx="2"></rect>
      </svg>
      Stop`;
    if (stopRequestProofId === proofId) {
      stopRequestProofId = null;
    }
    return;
  }

  stopProofButton.classList.remove("hidden");
  const isStopping = stopRequestProofId === proofId;
  stopProofButton.disabled = isStopping;
  stopProofButton.innerHTML = `
      <svg width="12" height="12" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true">
        <rect x="6" y="6" width="12" height="12" rx="2"></rect>
      </svg>
      ${isStopping ? "Stopping..." : "Stop"}`;
}

async function stopSelectedProof() {
  if (!selectedProofId) {
    return;
  }

  const proof = proofs.find((item) => item.id === selectedProofId);
  const proofName = proof?.name || selectedProofId;
  if (!window.confirm(`Stop proof "${proofName}"?`)) {
    return;
  }

  stopRequestProofId = selectedProofId;
  heroStatusEl.textContent = "stopping";
  heroStatusEl.className = "status-badge stopping";
  heroSubtitleEl.textContent = "Stop requested. Final verification is still in progress.";
  updateStopButton(selectedProofId, "stopping");
  scheduleAutoRefresh(true);
  setTimeout(() => {
    void refresh();
  }, 150);

  fetch(`/proofs/${selectedProofId}/stop`, {method: "PUT"})
    .then((response) => {
    if (!response.ok) {
      throw new Error(`Failed to stop proof: ${response.status}`);
    }
    return response;
  })
    .catch((error) => {
    console.error(error);
    stopRequestProofId = null;
    updateStopButton(selectedProofId, "running");
    window.alert(`Failed to stop proof: ${error.message}`);
  });
}

// ── Refresh ───────────────────────────────────────────────────────

async function refresh() {
  refreshButton.disabled     = true;
  refreshButton.textContent  = "Refreshing…";
  try {
    await loadProofs();
  } catch (error) {
    console.error(error);
    showEmptyState("Failed to load proofs", error.message);
  } finally {
    refreshButton.disabled    = false;
    refreshButton.innerHTML   = `
      <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
        <path d="M21 12a9 9 0 0 0-9-9 9.75 9.75 0 0 0-6.74 2.74L3 8"/>
        <path d="M3 3v5h5"/>
        <path d="M3 12a9 9 0 0 0 9 9 9.75 9.75 0 0 0 6.74-2.74L21 16"/>
        <path d="M16 16h5v5"/>
      </svg>
      Refresh`;
  }
}

refreshButton.addEventListener("click", () => {
  void refresh();
});

if (stopProofButton) {
  stopProofButton.addEventListener("click", () => {
    void stopSelectedProof();
  });
}

void refresh();
