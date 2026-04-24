# Rolling Correlation Factor Dashboard / 滚动相关双因子看板

## Snapshot / 项目快照

This project is a Go backend plus static frontend dashboard for crypto factor monitoring.  
该项目是一个使用 Go 后端加静态前端实现的加密货币因子监控看板。

Current scope / 当前范围：

- Fixed symbol universe loaded from `config/symbols.json`
- 固定标的池通过 `config/symbols.json` 加载
- Binance dynamic momentum universe
- Binance 动态强势候选池
- Universe members / 当前标的名单见：
- `config/symbols.json`
- Multi-timeframe factor boards / 多周期因子面板：
- `1H`
- `4H`
- `1D`
- `3D`
- Two return-based factors / 两个基于收益率的因子：
- `corr`
- `beta`
- 8-hour change still enriched from Binance futures `8h` klines
- 8 小时涨跌幅仍通过 Binance 合约 `8h` K 线补充
- Dynamic Binance candidates must satisfy:
- Binance 动态候选需满足：
- `quoteVolume > 250,000,000`
- `24h priceChangePercent > 0`
- `8h change > 0`
- latest price above `1D EMA25`, `1D MA60`, `8H EMA25`, `8H MA60`
- 最新价格站上 `1D EMA25`、`1D MA60`、`8H EMA25`、`8H MA60`
- Dynamic Binance candidates only enter the `1H` factor board
- Binance 动态候选只进入 `1H` 因子面板
- Main page / 主页面支持：
- timeframe board switch / 周期面板切换
- column sort / 列排序
- fuzzy symbol search / 标的模糊搜索
- signal filter / 信号过滤
- Detail page / 详情页支持：
- timeframe switch / 周期切换
- factor switch / 因子切换
- ECharts time series / ECharts 时序图

## Signal Design / 信号设计

Backend returns English codes instead of Chinese labels.  
后端接口返回英文代码字段，而不是直接返回中文信号文案。

Frame fields / 单个周期帧字段：

- `status`
- `ok`
- `insufficient_history`
- `low_variance`
- `alignment_failed`
- `unavailable`
- `signal_code`
- `independent`
- `follow`
- `strong_follow`

Signal rules / 信号规则：

- `corr < 0.75` -> `independent`
- `corr >= 0.75` -> `follow`
- `corr >= 0.75` and `beta > 1.5` -> `strong_follow`

Frontend maps these codes to Chinese labels.  
前端负责将这些英文代码映射为中文标签展示。

Recommended interpretation / 当前简化解读：

- `independent`：相关性低于 0.75，认为更偏独立
- `follow`：相关性高于等于 0.75，认为跟随 BTC
- `strong_follow`：相关性高于等于 0.75 且 Beta 大于 1.5，认为强跟随 BTC

## Data Source Strategy / 数据源策略

Preferred order / 优先级顺序：

1. Binance
2. Bybit
3. OKX

Rules / 规则：

- If Binance has the symbol and timeframe data, backend uses Binance directly.
- 只要 Binance 对该标的该周期拿到数据，就直接使用 Binance。
- Fallback to Bybit or OKX only when Binance fails for that frame.
- 只有 Binance 在该标的该周期拿不到数据时，才会回退到 Bybit 或 OKX。
- Unsupported symbols on Bybit and OKX are cached and skipped silently later.
- Bybit 和 OKX 上“不支持该 symbol”的情况会被缓存，后续静默跳过，不重复报错。
- Request layer includes retry, jitter, and per-provider pacing to reduce failure rate and over-request risk.
- 请求层带有重试、随机抖动和按数据源节流，降低失败率和过量请求风险。
- Dataset refresh starts automatically on service boot and keeps running in an internal background context.
- 数据刷新会在服务启动时自动预热，并在后端独立上下文中持续运行。

## Frontend Display Rules / 前端展示规则

Main page hides placeholder frames by default when / 主页面默认隐藏以下占位帧：

- `status != ok`
- 或者 `status != ok`
- or all factor values are zero
- 或四个因子值全为 0
- `1H` board also hides `ETHUSDT`, `SOLUSDT`, `BNBUSDT`, `DOGEUSDT`, `XRPUSDT`, `HYPEUSDT`, `ZECUSDT`, `NEARUSDT`
- `1H` 面板还会额外隐藏 `ETHUSDT`、`SOLUSDT`、`BNBUSDT`、`DOGEUSDT`、`XRPUSDT`、`HYPEUSDT`、`ZECUSDT`、`NEARUSDT`

This keeps backend data complete while avoiding noisy rows in the main board.  
这样可以保证后端保留完整数据，同时避免主面板出现无意义的噪音行。

Main page signal filter / 主页面信号过滤：

- `all`
- `follow`
- `strong_follow`
- `independent`

Only one signal tag can be selected at a time.  
同一时间只能选择一个信号标签。

Main page default board / 主页面默认周期面板：

- `1H`

## Project Structure / 项目结构

- `main.go`
- thin app bootstrap / 轻量程序入口
- `internal/app/service.go`
- service orchestration and cache / 服务编排与缓存
- market data providers / 多数据源提供器
- retries and pacing / 重试与节流
- factor calculation / 因子计算
- `config/symbols.json`
- fixed symbol list config / 固定标的名单配置
- `internal/app/server.go`
- HTTP routes / HTTP 路由
- API serialization / API 输出结构
- `internal/app/web/index.html`
- overview page / 总览页面
- `internal/app/web/main.js`
- overview interactions / 总览页交互逻辑
- `internal/app/web/detail.html`
- detail page / 详情页面
- `internal/app/web/detail.js`
- detail interactions / 详情页交互逻辑
- `internal/app/web/styles.css`
- shared styles / 公共样式

## API / 接口

### `GET /api/overview`

Returns / 返回内容：

- benchmark metadata / 基准信息
- supported timeframes / 支持的周期
- rolling window / 滚动窗口
- refresh status / 当前是否在后台刷新
- dataset update times / 数据更新时间
- asset list / 标的列表
- per-frame values / 每个周期帧的最新值

Example frame fields / 示例周期帧字段：

```json
{
  "timeframe": "1D",
  "status": "ok",
  "signal_code": "follow",
  "corr": 0.9123,
  "beta": 1.1023
}
```

### `GET /api/detail?symbol=ETHUSDT`

Returns / 返回内容：

- one asset / 单个标的
- all available frames / 该标的所有可用周期帧
- factor point arrays for charting / 用于画图的因子时间序列点

## Run / 运行方式

Requirements / 环境要求：

- Go 1.24+
- outbound internet access to Binance, Bybit, and OKX
- 需要能访问 Binance、Bybit、OKX 的外网环境

Run / 运行：

```powershell
go run .
```

Edit symbols / 修改标的：

```json
{
  "symbols": ["ETHUSDT", "SOLUSDT", "XRPUSDT"]
}
```

- Update `config/symbols.json` instead of editing Go code.
- 以后修改标的时直接更新 `config/symbols.json`，不需要改 Go 代码。
- Restart the service after editing `config/symbols.json`.
- 修改 `config/symbols.json` 后重启服务即可生效。

Open / 打开：

```text
http://127.0.0.1:8080/
```

Build / 构建：

```powershell
go build .
```

## Refresh and Caching / 刷新与缓存

- Dataset cache TTL: `5 minutes`
- 数据集缓存 TTL：`5 分钟`
- Universe cache TTL: `5 minutes`
- 标的池缓存 TTL：`5 分钟`
- Frontend auto refresh: `5 minutes`
- 前端自动刷新：`5 分钟`
- When a refresh is still running, API continues serving stale cached data first.
- 当后台刷新尚未完成时，接口会优先继续返回旧缓存，避免页面真空期。

## Notes / 说明

- Some fixed-list symbols may still lack enough `3D` history.
- 固定名单中的部分标的仍可能缺少足够的 `3D` 历史。
- Some symbols can be present only on Binance and absent on Bybit or OKX.
- 有些标的可能只在 Binance 可用，在 Bybit 或 OKX 不存在。
- For short-history assets, backend may return `insufficient_history` for some frames.
- 对于历史太短的标的，后端会在部分周期返回 `insufficient_history`。
