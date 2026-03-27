const benchmarkValue = document.getElementById("benchmarkValue");
const updatedAtValue = document.getElementById("updatedAtValue");
const windowValue = document.getElementById("windowValue");
const factorTableBody = document.getElementById("factorTableBody");
const errorBanner = document.getElementById("errorBanner");
const refreshButton = document.getElementById("refreshButton");
const factorGuideButton = document.getElementById("factorGuideButton");
const closeGuideButton = document.getElementById("closeGuideButton");
const guideModal = document.getElementById("guideModal");

const AUTO_REFRESH_MS = 60 * 60 * 1000;

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

function factorClass(value) {
  if (value > 0) {
    return "factor-positive";
  }
  if (value < 0) {
    return "factor-negative";
  }
  return "factor-neutral";
}

function showError(message) {
  errorBanner.textContent = message;
  errorBanner.classList.remove("hidden");
}

function hideError() {
  errorBanner.classList.add("hidden");
}

function openGuide() {
  guideModal.classList.remove("hidden");
  guideModal.setAttribute("aria-hidden", "false");
}

function closeGuide() {
  guideModal.classList.add("hidden");
  guideModal.setAttribute("aria-hidden", "true");
}

function createFrameRow(asset, frame, rowSpan, isFirstRow) {
  const row = document.createElement("tr");
  row.className = "asset-row";

  if (isFirstRow) {
    const symbolCell = document.createElement("td");
    symbolCell.rowSpan = rowSpan;
    symbolCell.className = "sticky-group-cell";
    symbolCell.innerHTML = `
      <div class="asset-cell">
        <span class="symbol-chip" title="${asset.symbol}">${asset.display_name || asset.symbol}</span>
        <small>${asset.symbol}</small>
      </div>
    `;
    row.appendChild(symbolCell);

    const metaCell = document.createElement("td");
    metaCell.rowSpan = rowSpan;
    metaCell.className = "sticky-group-cell";
    metaCell.innerHTML = `
      <div class="source-cell">
        <strong>${asset.data_source.toUpperCase()}</strong>
        <small>${formatVolume(asset.quote_volume)}</small>
      </div>
    `;
    row.appendChild(metaCell);
  }

  const timeframeCell = document.createElement("td");
  timeframeCell.innerHTML = `<span class="timeframe-chip">${frame.timeframe}</span>`;
  row.appendChild(timeframeCell);

  const latestTimeCell = document.createElement("td");
  latestTimeCell.textContent = formatDateTime(frame.latest_time);
  row.appendChild(latestTimeCell);

  const corrCell = document.createElement("td");
  corrCell.className = factorClass(frame.corr);
  corrCell.textContent = formatFactor(frame.corr);
  row.appendChild(corrCell);

  const betaCell = document.createElement("td");
  betaCell.className = factorClass(frame.beta);
  betaCell.textContent = formatFactor(frame.beta);
  row.appendChild(betaCell);

  const residualCell = document.createElement("td");
  residualCell.className = factorClass(frame.residual);
  residualCell.textContent = formatFactor(frame.residual);
  row.appendChild(residualCell);

  const lagCorrCell = document.createElement("td");
  lagCorrCell.className = factorClass(frame.lag_corr);
  lagCorrCell.textContent = formatFactor(frame.lag_corr);
  row.appendChild(lagCorrCell);

  const signalCell = document.createElement("td");
  signalCell.innerHTML = `<span class="signal-badge">${frame.signal}</span>`;
  row.appendChild(signalCell);

  row.addEventListener("click", () => {
    window.location.href = `/detail?symbol=${encodeURIComponent(asset.symbol)}`;
  });
  row.addEventListener("keydown", (event) => {
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      window.location.href = `/detail?symbol=${encodeURIComponent(asset.symbol)}`;
    }
  });
  row.tabIndex = 0;
  return row;
}

function renderRows(items) {
  factorTableBody.innerHTML = "";
  for (const asset of items) {
    const frames = asset.frames || [];
    if (frames.length === 0) {
      continue;
    }

    frames.forEach((frame, index) => {
      factorTableBody.appendChild(createFrameRow(asset, frame, frames.length, index === 0));
    });
  }

  if (!factorTableBody.children.length) {
    factorTableBody.innerHTML = `
      <tr>
        <td colspan="9" class="loading-cell">暂无可展示数据</td>
      </tr>
    `;
  }
}

async function loadOverview() {
  hideError();
  refreshButton.disabled = true;
  factorTableBody.innerHTML = `
    <tr>
      <td colspan="9" class="loading-cell">正在刷新数据...</td>
    </tr>
  `;

  try {
    const response = await fetch("/api/overview", { headers: { Accept: "application/json" } });
    if (!response.ok) {
      const payload = await response.json().catch(() => ({}));
      throw new Error(payload.error || "获取总览数据失败");
    }

    const payload = await response.json();
    benchmarkValue.textContent = payload.benchmark;
    updatedAtValue.textContent = `${formatDateTime(payload.updated_at)} | 标的池 ${formatDateTime(payload.universe_updated_at)}`;
    windowValue.textContent = `${payload.rolling_window} Days | ${payload.asset_count} Symbols`;
    renderRows(payload.items || []);
  } catch (error) {
    showError(error.message || "加载失败");
    factorTableBody.innerHTML = `
      <tr>
        <td colspan="9" class="loading-cell">暂无可展示数据</td>
      </tr>
    `;
  } finally {
    refreshButton.disabled = false;
  }
}

factorGuideButton.addEventListener("click", openGuide);
closeGuideButton.addEventListener("click", closeGuide);
guideModal.addEventListener("click", (event) => {
  if (event.target.dataset.closeModal === "true") {
    closeGuide();
  }
});
document.addEventListener("keydown", (event) => {
  if (event.key === "Escape" && !guideModal.classList.contains("hidden")) {
    closeGuide();
  }
});
refreshButton.addEventListener("click", loadOverview);

loadOverview();
setInterval(loadOverview, AUTO_REFRESH_MS);
