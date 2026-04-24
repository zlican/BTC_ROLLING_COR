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

func (p *OKXProvider) Name() string {
	return "okx"
}

func (p *OKXProvider) FetchBenchmarkHistory(ctx context.Context, timeframe TimeframeConfig, startDate, endDate time.Time) (string, []dataPoint, error) {
	series, err := p.fetchHistoricalData(ctx, "BTC-USDT", timeframe, startDate, endDate)
	if err != nil {
		return "", nil, err
	}
	return "BTC-USDT", series, nil
}

func (p *OKXProvider) FetchAssetHistory(ctx context.Context, symbol string, timeframe TimeframeConfig, startDate, endDate time.Time) (string, string, []dataPoint, error) {
	if !strings.HasSuffix(symbol, "USDT") {
		return "", "", nil, errSymbolUnsupported
	}

	base := strings.TrimSuffix(symbol, "USDT")
	if base == "" {
		return "", "", nil, errSymbolUnsupported
	}

	instID := base + "-USDT"
	if _, exists := p.unsupported.Load(instID); exists {
		return "", "", nil, errSymbolUnsupported
	}

	series, err := p.fetchHistoricalData(ctx, instID, timeframe, startDate, endDate)
	if err != nil {
		if errors.Is(err, errSymbolUnsupported) {
			p.unsupported.Store(instID, true)
		}
		return "", "", nil, err
	}
	return instID, fmt.Sprintf("%s vs BTC-USDT", instID), series, nil
}

func (p *OKXProvider) fetchHistoricalData(ctx context.Context, instID string, timeframe TimeframeConfig, startDate, endDate time.Time) ([]dataPoint, error) {
	var candles []dataPoint
	var cursor string

	for {
		payload, err := doJSONRequest[okxResponse](
			ctx,
			p.client,
			fmt.Sprintf("okx klines %s %s", instID, timeframe.Name),
			"okx",
			okxMinRequestGap,
			func() (*http.Request, error) {
				req, err := http.NewRequestWithContext(ctx, http.MethodGet, okxHistoryCandlesURL, nil)
				if err != nil {
					return nil, err
				}

				query := req.URL.Query()
				query.Set("instId", instID)
				query.Set("bar", timeframe.OKXBar)
				query.Set("limit", strconv.Itoa(pageLimit))
				if cursor != "" {
					query.Set("after", cursor)
				}
				req.URL.RawQuery = query.Encode()
				return req, nil
			},
		)
		if err != nil {
			return nil, err
		}
		if payload.Code != "0" {
			if strings.Contains(strings.ToLower(payload.Msg), "doesn't exist") {
				return nil, errSymbolUnsupported
			}
			return nil, fmt.Errorf("OKX API error: %s", payload.Msg)
		}
		if len(payload.Data) == 0 {
			break
		}

		for _, row := range payload.Data {
			if len(row) < 5 {
				return nil, errors.New("OKX row does not contain close price")
			}

			tsMillis, err := strconv.ParseInt(row[0], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("parse OKX timestamp %q: %w", row[0], err)
			}
			closePrice, err := strconv.ParseFloat(row[4], 64)
			if err != nil {
				return nil, fmt.Errorf("parse OKX close price %q: %w", row[4], err)
			}

			timestamp := time.UnixMilli(tsMillis).UTC()
			if !timestamp.Before(startDate) && !timestamp.After(endDate) {
				candles = append(candles, dataPoint{Time: timestamp, Close: closePrice})
			}
		}

		oldestMillis, err := strconv.ParseInt(payload.Data[len(payload.Data)-1][0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse OKX oldest timestamp: %w", err)
		}
		oldestTimestamp := time.UnixMilli(oldestMillis).UTC()
		if oldestTimestamp.Before(startDate) || len(payload.Data) < pageLimit {
			break
		}

		nextCursor := payload.Data[len(payload.Data)-1][0]
		if nextCursor == cursor {
			break
		}
		cursor = nextCursor
	}

	if len(candles) == 0 {
		return nil, fmt.Errorf("no OKX candles returned for %s %s", instID, timeframe.Name)
	}

	sort.Slice(candles, func(i, j int) bool {
		return candles[i].Time.Before(candles[j].Time)
	})
	return deduplicateSeries(candles), nil
}
