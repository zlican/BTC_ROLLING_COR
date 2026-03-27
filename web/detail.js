const detailTitle = document.getElementById("detailTitle");
const detailSubtitle = document.getElementById("detailSubtitle");
const detailLatestTime = document.getElementById("detailLatestTime");
const detailLatestValue = document.getElementById("detailLatestValue");
const detailUpdatedAt = document.getElementById("detailUpdatedAt");
const detailErrorBanner = document.getElementById("detailErrorBanner");
const chartElement = document.getElementById("chart");

function getSymbol() {
  const params = new URLSearchParams(window.location.search);
  return (params.get("symbol") || "").toUpperCase();
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

function formatVolume(value) {
  return `${(Number(value) / 1e8).toFixed(2)}亿 USDT`;
}

function showError(message) {
  detailErrorBanner.textContent = message;
  detailErrorBanner.classList.remove("hidden");
}

function hideError() {
  detailErrorBanner.classList.add("hidden");
}

function renderChart(payload) {
  const chart = echarts.init(chartElement);
  const values = payload.asset.points.map((point) => [point.time, Number(point.value.toFixed(6))]);

  chart.setOption({
    backgroundColor: "transparent",
    animationDuration: 600,
    color: ["#c9632b"],
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
      min: -1,
      max: 1,
      axisLabel: { color: "#6c5b4c" },
      axisLine: { show: false },
      splitLine: { lineStyle: { color: "rgba(94, 70, 44, 0.08)" } },
    },
    series: [
      {
        name: payload.asset.pair_label,
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
              { offset: 0, color: "rgba(201, 99, 43, 0.35)" },
              { offset: 1, color: "rgba(201, 99, 43, 0.02)" },
            ],
          },
        },
        data: values,
      },
    ],
  });

  window.addEventListener("resize", () => chart.resize());
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

    const payload = await response.json();
    detailTitle.textContent = `${payload.asset.display_name || payload.asset.symbol} 1D 滚动因子详情`;
    detailSubtitle.textContent = `${payload.asset.pair_label} | 数据源 ${payload.asset.data_source.toUpperCase()} | 24h 成交额 ${formatVolume(payload.asset.quote_volume)}`;
    detailLatestTime.textContent = formatDateTime(payload.asset.latest_time);
    detailLatestValue.textContent = formatFactor(payload.asset.latest_value);
    detailUpdatedAt.textContent = formatDateTime(payload.updated_at);
    renderChart(payload);
  } catch (error) {
    showError(error.message || "加载失败");
    detailTitle.textContent = `${symbol} 数据加载失败`;
    detailSubtitle.textContent = "请稍后重试。";
  }
}

loadDetail();
