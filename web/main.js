const benchmarkValue = document.getElementById("benchmarkValue");
const updatedAtValue = document.getElementById("updatedAtValue");
const windowValue = document.getElementById("windowValue");
const factorTableBody = document.getElementById("factorTableBody");
const errorBanner = document.getElementById("errorBanner");
const refreshButton = document.getElementById("refreshButton");
const factorGuideButton = document.getElementById("factorGuideButton");
const closeGuideButton = document.getElementById("closeGuideButton");
const guideModal = document.getElementById("guideModal");
const symbolSearchInput = document.getElementById("symbolSearchInput");
const sortButtons = Array.from(document.querySelectorAll(".sort-button"));
const timeframeBoards = document.getElementById("timeframeBoards");

const AUTO_REFRESH_MS = 60 * 60 * 1000;
const FALLBACK_TIMEFRAMES = ["4H", "1D", "1W"];

let overviewItems = [];
let availableTimeframes = [...FALLBACK_TIMEFRAMES];
let activeTimeframe = "1D";
let sortState = { field: null, order: null };
let searchKeyword = "";

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

function getFrame(asset, timeframe = activeTimeframe) {
  const frames = asset.frames || [];
  return frames.find((frame) => frame.timeframe === timeframe) || null;
}

function matchesSearch(asset) {
  const keyword = searchKeyword.trim().toUpperCase();
  if (!keyword) {
    return true;
  }

  const displayName = String(asset.display_name || "").toUpperCase();
  const symbol = String(asset.symbol || "").toUpperCase();
  return displayName.includes(keyword) || symbol.includes(keyword);
}

function renderBoardTabs() {
  timeframeBoards.innerHTML = "";
  for (const timeframe of availableTimeframes) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = `pill-button${timeframe === activeTimeframe ? " active" : ""}`;
    button.textContent = timeframe;
    button.addEventListener("click", () => {
      activeTimeframe = timeframe;
      renderBoardTabs();
      renderRows(overviewItems);
    });
    timeframeBoards.appendChild(button);
  }
}

function createRow(asset, frame) {
  const row = document.createElement("tr");
  row.className = "asset-row";

  row.innerHTML = `
    <td>
      <div class="asset-cell">
        <span class="symbol-chip" title="${asset.symbol}">${asset.display_name || asset.symbol}</span>
        <small>${asset.symbol}</small>
      </div>
    </td>
    <td>
      <div class="source-cell">
        <strong class="${factorClass(asset.eight_hour_pct)}">${formatPct(asset.eight_hour_pct)}</strong>
        <small>8H</small>
      </div>
    </td>
    <td><span class="timeframe-chip">${frame.timeframe}</span></td>
    <td class="${factorClass(frame.corr)}">${formatFactor(frame.corr)}</td>
    <td class="${factorClass(frame.beta)}">${formatFactor(frame.beta)}</td>
    <td class="${factorClass(frame.residual)}">${formatFactor(frame.residual)}</td>
    <td class="${factorClass(frame.lag_corr)}">${formatFactor(frame.lag_corr)}</td>
    <td><span class="signal-badge">${frame.signal}</span></td>
  `;

  const href = `/detail?symbol=${encodeURIComponent(asset.symbol)}&timeframe=${encodeURIComponent(frame.timeframe)}`;
  row.addEventListener("click", () => {
    window.location.href = href;
  });
  row.addEventListener("keydown", (event) => {
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      window.location.href = href;
    }
  });
  row.tabIndex = 0;
  return row;
}

function getSortMetric(asset, field) {
  if (field === "symbol") {
    return String(asset.display_name || asset.symbol || "").toUpperCase();
  }

  if (field === "eight_hour_pct") {
    return Number(asset.eight_hour_pct || 0);
  }

  const frame = getFrame(asset);
  if (!frame) {
    return Number.NEGATIVE_INFINITY;
  }
  return Number(frame[field] || 0);
}

function getSortedItems(items) {
  const visible = items.filter((asset) => getFrame(asset) && matchesSearch(asset));
  if (!sortState.field || !sortState.order) {
    return visible;
  }

  return [...visible].sort((left, right) => {
    const leftValue = getSortMetric(left, sortState.field);
    const rightValue = getSortMetric(right, sortState.field);

    if (sortState.field === "symbol") {
      const compare = String(leftValue).localeCompare(String(rightValue));
      if (compare === 0) {
        return left.symbol.localeCompare(right.symbol);
      }
      return sortState.order === "desc" ? -compare : compare;
    }

    if (leftValue === rightValue) {
      return left.symbol.localeCompare(right.symbol);
    }
    return sortState.order === "desc" ? rightValue - leftValue : leftValue - rightValue;
  });
}

function updateSortButtons() {
  for (const button of sortButtons) {
    const isActive = button.dataset.sortField === sortState.field && button.dataset.sortOrder === sortState.order;
    button.classList.toggle("active", isActive);
  }
}

function renderRows(items) {
  factorTableBody.innerHTML = "";
  for (const asset of getSortedItems(items)) {
    const frame = getFrame(asset);
    if (!frame) {
      continue;
    }
    factorTableBody.appendChild(createRow(asset, frame));
  }

  if (!factorTableBody.children.length) {
    factorTableBody.innerHTML = `
      <tr>
        <td colspan="8" class="loading-cell">${activeTimeframe} 面板暂无匹配标的</td>
      </tr>
    `;
  }
}

async function loadOverview() {
  hideError();
  refreshButton.disabled = true;
  factorTableBody.innerHTML = `
    <tr>
      <td colspan="8" class="loading-cell">正在刷新数据...</td>
    </tr>
  `;

  try {
    const response = await fetch("/api/overview", { headers: { Accept: "application/json" } });
    if (!response.ok) {
      const payload = await response.json().catch(() => ({}));
      throw new Error(payload.error || "获取总览数据失败");
    }

    const payload = await response.json();
    overviewItems = payload.items || [];
    availableTimeframes = payload.timeframes?.length ? payload.timeframes : [...FALLBACK_TIMEFRAMES];
    if (!availableTimeframes.includes(activeTimeframe)) {
      activeTimeframe = availableTimeframes.includes("1D") ? "1D" : availableTimeframes[0];
    }

    benchmarkValue.textContent = payload.benchmark;
    updatedAtValue.textContent = `${formatDateTime(payload.updated_at)} | 标的池 ${formatDateTime(payload.universe_updated_at)}`;
    windowValue.textContent = `${payload.rolling_window} Bars | ${payload.asset_count} Symbols`;

    renderBoardTabs();
    renderRows(overviewItems);
    updateSortButtons();
  } catch (error) {
    showError(error.message || "加载失败");
    factorTableBody.innerHTML = `
      <tr>
        <td colspan="8" class="loading-cell">暂无可展示数据</td>
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
for (const button of sortButtons) {
  button.addEventListener("click", () => {
    sortState = {
      field: button.dataset.sortField,
      order: button.dataset.sortOrder,
    };
    updateSortButtons();
    renderRows(overviewItems);
  });
}
symbolSearchInput.addEventListener("input", (event) => {
  searchKeyword = event.target.value || "";
  renderRows(overviewItems);
});

loadOverview();
setInterval(loadOverview, AUTO_REFRESH_MS);
