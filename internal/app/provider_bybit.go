package app

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

func (p *BybitProvider) Name() string {
	return "bybit"
}

func (p *BybitProvider) FetchBenchmarkHistory(ctx context.Context, timeframe TimeframeConfig, startDate, endDate time.Time) (string, []dataPoint, error) {
	series, err := p.fetchKlines(ctx, "BTCUSDT", timeframe, startDate, endDate)
	if err != nil {
		return "", nil, err
	}
	return "BTCUSDT", series, nil
}

func (p *BybitProvider) FetchAssetHistory(ctx context.Context, symbol string, timeframe TimeframeConfig, startDate, endDate time.Time) (string, string, []dataPoint, error) {
	if _, exists := p.unsupported.Load(symbol); exists {
		return "", "", nil, errSymbolUnsupported
	}

	series, err := p.fetchKlines(ctx, symbol, timeframe, startDate, endDate)
	if err != nil {
		if errors.Is(err, errSymbolUnsupported) {
			p.unsupported.Store(symbol, true)
		}
		return "", "", nil, err
	}
	return symbol, fmt.Sprintf("%s vs BTCUSDT", symbol), series, nil
}

func (p *BybitProvider) fetchKlines(ctx context.Context, symbol string, timeframe TimeframeConfig, startDate, endDate time.Time) ([]dataPoint, error) {
	if strings.TrimSpace(timeframe.BybitInterval) == "" {
		return nil, errSymbolUnsupported
	}

	payload, err := doJSONRequest[bybitKlineResponse](
		ctx,
		p.client,
		fmt.Sprintf("bybit klines %s %s", symbol, timeframe.Name),
		"bybit",
		bybitMinRequestGap,
		func() (*http.Request, error) {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, bybitKlinesURL, nil)
			if err != nil {
				return nil, err
			}

			query := req.URL.Query()
			query.Set("category", "linear")
			query.Set("symbol", symbol)
			query.Set("interval", timeframe.BybitInterval)
			query.Set("start", strconv.FormatInt(startDate.UnixMilli(), 10))
			query.Set("end", strconv.FormatInt(endDate.Add(timeframe.CandleDuration-time.Millisecond).UnixMilli(), 10))
			query.Set("limit", strconv.Itoa(timeframe.HistoryBars+32))
			req.URL.RawQuery = query.Encode()
			return req, nil
		},
	)
	if err != nil {
		return nil, err
	}
	if payload.RetCode != 0 {
		if strings.Contains(strings.ToLower(payload.RetMsg), "symbol") && strings.Contains(strings.ToLower(payload.RetMsg), "invalid") {
			return nil, errSymbolUnsupported
		}
		return nil, fmt.Errorf("bybit api error: %s", payload.RetMsg)
	}
	if len(payload.Result.List) == 0 {
		return nil, fmt.Errorf("no bybit klines returned for %s %s", symbol, timeframe.Name)
	}

	candles := make([]dataPoint, 0, len(payload.Result.List))
	for _, row := range payload.Result.List {
		if len(row) < 5 {
			return nil, errors.New("bybit kline row does not contain close price")
		}

		tsMillis, err := strconv.ParseInt(row[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse bybit open time %q: %w", row[0], err)
		}
		closePrice, err := strconv.ParseFloat(row[4], 64)
		if err != nil {
			return nil, fmt.Errorf("parse bybit close price %q: %w", row[4], err)
		}

		timestamp := time.UnixMilli(tsMillis).UTC()
		if !timestamp.Before(startDate) && !timestamp.After(endDate) {
			candles = append(candles, dataPoint{
				Time:  timestamp,
				Close: closePrice,
			})
		}
	}

	if len(candles) == 0 {
		return nil, fmt.Errorf("no bybit candles remained after filtering for %s %s", symbol, timeframe.Name)
	}

	sort.Slice(candles, func(i, j int) bool {
		return candles[i].Time.Before(candles[j].Time)
	})
	return deduplicateSeries(candles), nil
}
