from matplotlib.dates import DateFormatter
import matplotlib.pyplot as plt
import pandas as pd
import requests


OKX_HISTORY_CANDLES_URL = "https://www.okx.com/api/v5/market/history-candles"
PROXY_URL = "http://127.0.0.1:10809"
PROXIES = {"http": PROXY_URL, "https": PROXY_URL}
TIMEFRAME = "1Dutc"
REQUEST_TIMEOUT = 20
PAGE_LIMIT = 100
ROLLING_WINDOW = 30


def fetch_historical_data(inst_id, start_date, end_date, session):
    candles = []
    cursor = None
    start_ts = pd.Timestamp(start_date)
    end_ts = pd.Timestamp(end_date)
    if start_ts.tzinfo is None:
        start_ts = start_ts.tz_localize("UTC")
    else:
        start_ts = start_ts.tz_convert("UTC")
    if end_ts.tzinfo is None:
        end_ts = end_ts.tz_localize("UTC")
    else:
        end_ts = end_ts.tz_convert("UTC")

    while True:
        params = {"instId": inst_id, "bar": TIMEFRAME, "limit": str(PAGE_LIMIT)}
        if cursor is not None:
            params["after"] = cursor

        response = session.get(OKX_HISTORY_CANDLES_URL, params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

        payload = response.json()
        if payload.get("code") != "0":
            raise RuntimeError(f"OKX API error for {inst_id}: {payload.get('msg', 'unknown error')}")

        batch = payload.get("data", [])
        if not batch:
            break

        for row in batch:
            timestamp = pd.to_datetime(int(row[0]), unit="ms", utc=True)
            if start_ts <= timestamp <= end_ts:
                candles.append((timestamp, float(row[4])))

        oldest_timestamp = pd.to_datetime(int(batch[-1][0]), unit="ms", utc=True)
        if oldest_timestamp < start_ts or len(batch) < PAGE_LIMIT:
            break

        next_cursor = batch[-1][0]
        if next_cursor == cursor:
            break
        cursor = next_cursor

    if not candles:
        raise RuntimeError(f"No candles returned for {inst_id} between {start_ts.date()} and {end_ts.date()}.")

    df = pd.DataFrame(candles, columns=["timestamp", "close"])
    df = df.drop_duplicates(subset="timestamp").sort_values("timestamp")
    df.set_index("timestamp", inplace=True)
    return df["close"].rename(inst_id)


def main():
    end_date = pd.Timestamp.utcnow().normalize()
    start_date = end_date - pd.Timedelta(days=89)

    benchmark_inst_id = "BTC-USD"
    altcoin_pairs = {
        "ETH-BTC": "ETH-USD",
        "SOL-BTC": "SOL-USD",
        "POL-BTC": "POL-USD",
        "ARB-BTC": "ARB-USD",
        "LDO-BTC": "LDO-USD",
    }

    session = requests.Session()
    session.proxies.update(PROXIES)

    try:
        bitcoin_prices = fetch_historical_data(benchmark_inst_id, start_date, end_date, session)
    except Exception as exc:
        raise RuntimeError("Failed to fetch BTC benchmark data from OKX.") from exc

    price_series = {"BTC-USD": bitcoin_prices}
    for pair_label, inst_id in altcoin_pairs.items():
        try:
            price_series[pair_label] = fetch_historical_data(inst_id, start_date, end_date, session)
        except Exception as exc:
            raise RuntimeError(f"Failed to fetch {pair_label} data from OKX.") from exc

    prices = pd.concat(price_series, axis=1).dropna()

    rolling_correlations = {
        pair_label: prices[pair_label].rolling(window=ROLLING_WINDOW).corr(prices["BTC-USD"])
        for pair_label in altcoin_pairs
    }

    plt.figure(figsize=(10, 6))
    for pair_label, correlation in rolling_correlations.items():
        plt.plot(correlation.index, correlation.values, label=pair_label)

    plt.title("Rolling 30-day Correlation: ETH-BTC, SOL-BTC, POL-BTC, ARB-BTC, and LDO-BTC")
    plt.xlabel("Date")
    plt.ylabel("Correlation")
    plt.legend()

    date_formatter = DateFormatter("%m-%d")
    plt.gca().xaxis.set_major_formatter(date_formatter)
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()
