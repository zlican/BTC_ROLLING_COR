const detailTitle = document.getElementById("detailTitle");
const detailSubtitle = document.getElementById("detailSubtitle");
const detailLatestTime = document.getElementById("detailLatestTime");
const detailSignal = document.getElementById("detailSignal");
const detailUpdatedAt = document.getElementById("detailUpdatedAt");
const detailErrorBanner = document.getElementById("detailErrorBanner");
const timeframeTabs = document.getElementById("timeframeTabs");
const factorTabs = document.getElementById("factorTabs");
const factorCards = document.getElementById("factorCards");
const chartElement = document.getElementById("chart");

const AUTO_REFRESH_MS = 60 * 60 * 1000;
const factorMap = {
  corr: { label: "Corr", key: "corr_points", latestKey: "corr", color: "#c9632b" },
  beta: { label: "Beta", key: "beta_points", latestKey: "beta", color: "#2f7d6a" },
  residual: { label: "Residual", key: "residual_points", latestKey: "residual", color: "#8757d7" },
  lag_corr: { label: "Lag Corr", key: "lag_corr_points", latestKey: "lag_corr", color: "#a23d36" },
};
const signalMeta = {
  follow: { label: "跟随" },
  strong_follow: { label: "强跟随" },
  independent: { label: "独立" },
};
const statusMeta = {
  ok: { label: "正常" },
  insufficient_history: { label: "历史不足" },
  low_variance: { label: "波动过低" },
  alignment_failed: { label: "对齐失败" },
  unavailable: { label: "数据不足" },
};

let chartInstance;
let detailPayload;
let selectedTimeframe;
let selectedFactor = "corr";

function getQueryParam(key) {
  const params = new URLSearchParams(window.location.search);
  return params.get(key) || "";
}

function getSymbol() {
  return getQueryParam("symbol").toUpperCase();
}

function getRequestedTimeframe() {
  return getQueryParam("timeframe").toUpperCase();
}

function setTimeframeQuery(timeframe) {
  const url = new URL(window.location.href);
  url.searchParams.set("timeframe", timeframe);
  window.history.replaceState({}, "", url);
}

function formatDateTime(value) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleString("zh-CN", { hour12: false });
}

function formatFactor(value) {
  return Number(value).toFixed(4);
}

function formatPct(value) {
  return `${Number(value).toFixed(2)}%`;
}

function showError(message) {
  detailErrorBanner.textContent = message;
  detailErrorBanner.classList.remove("hidden");
}

function getFrameBadge(frame) {
  if (!frame) {
    return "-";
  }
  if (frame.status && frame.status !== "ok") {
    return statusMeta[frame.status]?.label || frame.status;
  }
  return signalMeta[frame.signal_code]?.label || "-";
}

function hideError() {
  detailErrorBanner.classList.add("hidden");
}

function getCurrentFrame() {
  if (!detailPayload) {
    return null;
  }
  return detailPayload.asset.frames.find((frame) => frame.timeframe === selectedTimeframe) || detailPayload.asset.frames[0] || null;
}

function renderTimeframeTabs() {
  timeframeTabs.innerHTML = "";
  for (const frame of detailPayload.asset.frames) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = `pill-button${frame.timeframe === selectedTimeframe ? " active" : ""}`;
    button.textContent = frame.timeframe;
    button.addEventListener("click", () => {
      selectedTimeframe = frame.timeframe;
      setTimeframeQuery(frame.timeframe);
      render();
    });
    timeframeTabs.appendChild(button);
  }
}

function renderFactorTabs() {
  factorTabs.innerHTML = "";
  for (const [factorKey, meta] of Object.entries(factorMap)) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = `pill-button${factorKey === selectedFactor ? " active" : ""}`;
    button.textContent = meta.label;
    button.addEventListener("click", () => {
      selectedFactor = factorKey;
      renderChart();
      renderMetricCards();
      renderHeader();
      renderFactorTabs();
    });
    factorTabs.appendChild(button);
  }
}

function renderMetricCards() {
  const frame = getCurrentFrame();
  if (!frame) {
    factorCards.innerHTML = "";
    return;
  }

  factorCards.innerHTML = "";
  for (const [factorKey, meta] of Object.entries(factorMap)) {
    const card = document.createElement("article");
    card.className = `metric-card${factorKey === selectedFactor ? " active" : ""}`;
    card.innerHTML = `
      <span class="metric-label">${meta.label}</span>
      <strong>${formatFactor(frame[meta.latestKey])}</strong>
    `;
    card.addEventListener("click", () => {
      selectedFactor = factorKey;
      renderChart();
      renderMetricCards();
      renderHeader();
      renderFactorTabs();
    });
    factorCards.appendChild(card);
  }
}

function renderHeader() {
  const frame = getCurrentFrame();
  if (!frame) {
    return;
  }

  detailTitle.textContent = `${detailPayload.asset.display_name || detailPayload.asset.symbol} ${frame.timeframe} 四因子详情`;
  detailSubtitle.textContent = `${frame.pair_label} | 数据源 ${(frame.data_source || detailPayload.asset.data_source || "-").toUpperCase()} | 基准 ${frame.benchmark_inst} | 8H 涨跌幅 ${formatPct(detailPayload.asset.eight_hour_pct)} | 当前图表 ${factorMap[selectedFactor].label}`;
  detailLatestTime.textContent = formatDateTime(frame.latest_time);
  detailSignal.textContent = getFrameBadge(frame);
  detailUpdatedAt.textContent = formatDateTime(detailPayload.updated_at);
}

function renderChart() {
  const frame = getCurrentFrame();
  if (!frame) {
    return;
  }

  const meta = factorMap[selectedFactor];
  const values = (frame[meta.key] || []).map((point) => [point.time, Number(point.value.toFixed(6))]);

  if (!chartInstance) {
    chartInstance = echarts.init(chartElement);
    window.addEventListener("resize", () => chartInstance.resize());
  }

  chartInstance.setOption({
    backgroundColor: "transparent",
    animationDuration: 500,
    color: [meta.color],
    tooltip: {
      trigger: "axis",
      backgroundColor: "rgba(33, 26, 21, 0.92)",
      borderWidth: 0,
      textStyle: { color: "#f8f4ee" },
      valueFormatter: (value) => Number(value).toFixed(4),
    },
    grid: {
      left: 48,
      right: 24,
      top: 38,
      bottom: 50,
    },
    xAxis: {
      type: "time",
      axisLabel: { color: "#6c5b4c" },
      axisLine: { lineStyle: { color: "rgba(94, 70, 44, 0.18)" } },
      splitLine: { show: false },
    },
    yAxis: {
      type: "value",
      axisLabel: { color: "#6c5b4c" },
      axisLine: { show: false },
      splitLine: { lineStyle: { color: "rgba(94, 70, 44, 0.08)" } },
    },
    series: [
      {
        name: `${frame.timeframe} ${meta.label}`,
        type: "line",
        smooth: true,
        symbol: "none",
        lineStyle: { width: 3 },
        areaStyle: {
          color: {
            type: "linear",
            x: 0,
            y: 0,
            x2: 0,
            y2: 1,
            colorStops: [
              { offset: 0, color: `${meta.color}55` },
              { offset: 1, color: `${meta.color}08` },
            ],
          },
        },
        data: values,
      },
    ],
  });
}

function render() {
  renderTimeframeTabs();
  renderFactorTabs();
  renderMetricCards();
  renderHeader();
  renderChart();
}

async function loadDetail() {
  const symbol = getSymbol();
  if (!symbol) {
    showError("URL 中缺少 symbol 参数");
    detailTitle.textContent = "无法加载标的";
    return;
  }

  hideError();

  try {
    const response = await fetch(`/api/detail?symbol=${encodeURIComponent(symbol)}`, {
      headers: { Accept: "application/json" },
    });
    if (!response.ok) {
      const payload = await response.json().catch(() => ({}));
      throw new Error(payload.error || "获取详情失败");
    }

    detailPayload = await response.json();
    const requestedTimeframe = getRequestedTimeframe();
    selectedTimeframe = detailPayload.asset.frames.find((frame) => frame.timeframe === requestedTimeframe)?.timeframe
      || detailPayload.asset.frames.find((frame) => frame.timeframe === "1D")?.timeframe
      || detailPayload.asset.frames[0]?.timeframe
      || "1D";
    setTimeframeQuery(selectedTimeframe);
    render();
  } catch (error) {
    showError(error.message || "加载失败");
    detailTitle.textContent = `${symbol} 数据加载失败`;
    detailSubtitle.textContent = "请稍后重试。";
  }
}

loadDetail();
setInterval(loadDetail, AUTO_REFRESH_MS);
