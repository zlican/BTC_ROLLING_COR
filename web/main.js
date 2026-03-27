const benchmarkValue = document.getElementById("benchmarkValue");
const updatedAtValue = document.getElementById("updatedAtValue");
const windowValue = document.getElementById("windowValue");
const factorTableBody = document.getElementById("factorTableBody");
const errorBanner = document.getElementById("errorBanner");
const refreshButton = document.getElementById("refreshButton");

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

function showError(message) {
  errorBanner.textContent = message;
  errorBanner.classList.remove("hidden");
}

function hideError() {
  errorBanner.classList.add("hidden");
}

function renderRows(items) {
  factorTableBody.innerHTML = "";
  for (const item of items) {
    const row = document.createElement("tr");
    row.tabIndex = 0;
    row.innerHTML = `
      <td><span class="symbol-chip">${item.symbol}</span></td>
      <td>${item.pair_label}</td>
      <td>${formatDateTime(item.latest_time)}</td>
      <td class="${item.latest_value >= 0 ? "factor-positive" : "factor-negative"}">${formatFactor(item.latest_value)}</td>
    `;
    row.addEventListener("click", () => {
      window.location.href = `/detail?symbol=${encodeURIComponent(item.symbol)}`;
    });
    row.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        window.location.href = `/detail?symbol=${encodeURIComponent(item.symbol)}`;
      }
    });
    factorTableBody.appendChild(row);
  }
}

async function loadOverview() {
  hideError();
  refreshButton.disabled = true;
  factorTableBody.innerHTML = `
    <tr>
      <td colspan="4" class="loading-cell">正在刷新数据...</td>
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
    updatedAtValue.textContent = formatDateTime(payload.updated_at);
    windowValue.textContent = `${payload.rolling_window} Days`;
    renderRows(payload.items || []);
  } catch (error) {
    showError(error.message || "加载失败");
    factorTableBody.innerHTML = `
      <tr>
        <td colspan="4" class="loading-cell">暂无可展示数据</td>
      </tr>
    `;
  } finally {
    refreshButton.disabled = false;
  }
}

refreshButton.addEventListener("click", loadOverview);
loadOverview();
