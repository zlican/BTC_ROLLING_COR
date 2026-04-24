package app

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

func (p *BinanceProvider) Name() string {
	return "binance"
}

func (p *BinanceProvider) FetchFixedUniverse(ctx context.Context, symbols []string) ([]AssetConfig, time.Time, error) {
	payload, err := p.fetchTicker24h(ctx)
	if err != nil {
		assets, buildErr := p.buildFixedUniverseFromTicker(ctx, symbols, nil)
		return assets, time.Now().UTC(), buildErr
	}
	assets, buildErr := p.buildFixedUniverseFromTicker(ctx, symbols, payload)
	return assets, time.Now().UTC(), buildErr
}

func (p *BinanceProvider) buildFixedUniverseFromTicker(ctx context.Context, symbols []string, payload []binanceTickerItem) ([]AssetConfig, error) {
	if len(symbols) == 0 {
		return nil, errors.New("fixed universe is empty")
	}

	requested := make(map[string]struct{}, len(symbols))
	assetsBySymbol := make(map[string]AssetConfig, len(symbols))
	for index, symbol := range symbols {
		normalized := strings.ToUpper(strings.TrimSpace(symbol))
		if normalized == "" {
			continue
		}
		requested[normalized] = struct{}{}
		displayName := strings.TrimSuffix(normalized, "USDT")
		if displayName == "" {
			displayName = normalized
		}
		assetsBySymbol[normalized] = AssetConfig{
			Symbol:       normalized,
			DisplayName:  displayName,
			UniverseRank: index + 1,
			Timeframes:   append([]string(nil), supportedTimeframes...),
		}
	}

	for _, item := range payload {
		if _, ok := requested[item.Symbol]; ok {
			quoteVolume, err := strconv.ParseFloat(item.QuoteVolume, 64)
			if err != nil {
				continue
			}
			lastPrice, err := strconv.ParseFloat(item.LastPrice, 64)
			if err != nil || lastPrice <= 0 {
				continue
			}

			cfg := assetsBySymbol[item.Symbol]
			cfg.QuoteVolume = quoteVolume
			cfg.LastPrice = lastPrice
			assetsBySymbol[item.Symbol] = cfg
		}
	}

	assets := flattenAssetConfigMap(assetsBySymbol, symbols)

	if err := p.attachEightHourChanges(ctx, assets, skipFixedEightHourSymbols); err != nil {
		log.Printf("fixed universe 8h enrichment completed with fallbacks: %v", err)
	}

	return assets, nil
}

func (p *BinanceProvider) fetchTicker24h(ctx context.Context) ([]binanceTickerItem, error) {
	return doJSONRequest[[]binanceTickerItem](
		ctx,
		p.client,
		"binance ticker 24hr",
		"binance",
		binanceMinRequestGap,
		func() (*http.Request, error) {
			return http.NewRequestWithContext(ctx, http.MethodGet, binanceTicker24hrURL, nil)
		},
	)
}

func flattenAssetConfigMap(items map[string]AssetConfig, order []string) []AssetConfig {
	assets := make([]AssetConfig, 0, len(order))
	for _, symbol := range order {
		normalized := strings.ToUpper(strings.TrimSpace(symbol))
		cfg, ok := items[normalized]
		if !ok {
			continue
		}
		assets = append(assets, cfg)
	}
	return assets
}

func (p *BinanceProvider) attachEightHourChanges(ctx context.Context, assets []AssetConfig, skipSymbols map[string]struct{}) error {
	if len(assets) == 0 {
		return nil
	}

	startTimeMs := currentEightHourSegmentStart().UnixMilli()
	sem := make(chan struct{}, 8)
	errCh := make(chan error, len(assets))
	var wg sync.WaitGroup

	for i := range assets {
		if _, shouldSkip := skipSymbols[assets[i].Symbol]; shouldSkip {
			continue
		}

		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			changePct, err := p.fetchEightHourChange(ctx, assets[index].Symbol, assets[index].LastPrice, startTimeMs)
			if err != nil {
				errCh <- fmt.Errorf("%s 8h change: %w", assets[index].Symbol, err)
				return
			}
			assets[index].EightHourPct = changePct
		}(i)
	}

	wg.Wait()
	close(errCh)

	var failed int
	for err := range errCh {
		failed++
		log.Printf("8h change fallback to zero: %v", err)
	}
	if failed > 0 {
		log.Printf("8h change completed with %d fallback symbols", failed)
	}
	return nil
}

func currentEightHourSegmentStart() time.Time {
	now := time.Now().UTC()
	segmentStartHour := (now.Hour() / 8) * 8
	return time.Date(now.Year(), now.Month(), now.Day(), segmentStartHour, 0, 0, 0, time.UTC)
}

func (p *BinanceProvider) fetchEightHourChange(ctx context.Context, symbol string, lastPrice float64, startTimeMs int64) (float64, error) {
	payload, err := doJSONRequest[[][]any](
		ctx,
		p.client,
		fmt.Sprintf("binance 8h kline %s", symbol),
		"binance",
		binanceMinRequestGap,
		func() (*http.Request, error) {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, binanceKlinesURL, nil)
			if err != nil {
				return nil, err
			}
			query := req.URL.Query()
			query.Set("symbol", symbol)
			query.Set("interval", "8h")
			query.Set("startTime", strconv.FormatInt(startTimeMs, 10))
			query.Set("limit", "1")
			req.URL.RawQuery = query.Encode()
			return req, nil
		},
	)
	if err != nil {
		return 0, err
	}
	if len(payload) == 0 || len(payload[0]) < 2 {
		return 0, errors.New("8h kline payload is empty")
	}

	openPrice, err := jsonNumberToFloat64(payload[0][1])
	if err != nil {
		return 0, fmt.Errorf("parse 8h open price: %w", err)
	}
	if openPrice == 0 {
		return 0, nil
	}

	return (lastPrice - openPrice) / openPrice * 100, nil
}

func (p *BinanceProvider) FetchBenchmarkHistory(ctx context.Context, timeframe TimeframeConfig, startDate, endDate time.Time) (string, []dataPoint, error) {
	series, err := p.fetchKlines(ctx, "BTCUSDT", timeframe, startDate, endDate)
	if err != nil {
		return "", nil, err
	}
	return "BTCUSDT", series, nil
}

func (p *BinanceProvider) FetchAssetHistory(ctx context.Context, symbol string, timeframe TimeframeConfig, startDate, endDate time.Time) (string, string, []dataPoint, error) {
	series, err := p.fetchKlines(ctx, symbol, timeframe, startDate, endDate)
	if err != nil {
		return "", "", nil, err
	}
	return symbol, fmt.Sprintf("%s vs BTCUSDT", symbol), series, nil
}

func (p *BinanceProvider) fetchKlines(ctx context.Context, symbol string, timeframe TimeframeConfig, startDate, endDate time.Time) ([]dataPoint, error) {
	payload, err := doJSONRequest[[][]any](
		ctx,
		p.client,
		fmt.Sprintf("binance klines %s %s", symbol, timeframe.Name),
		"binance",
		binanceMinRequestGap,
		func() (*http.Request, error) {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, binanceKlinesURL, nil)
			if err != nil {
				return nil, err
			}

			query := req.URL.Query()
			query.Set("symbol", symbol)
			query.Set("interval", timeframe.BinanceInterval)
			query.Set("limit", strconv.Itoa(timeframe.HistoryBars+32))
			query.Set("startTime", strconv.FormatInt(startDate.UnixMilli(), 10))
			query.Set("endTime", strconv.FormatInt(endDate.Add(timeframe.CandleDuration-time.Millisecond).UnixMilli(), 10))
			req.URL.RawQuery = query.Encode()
			return req, nil
		},
	)
	if err != nil {
		return nil, err
	}

	candles := make([]dataPoint, 0, len(payload))
	for _, row := range payload {
		if len(row) < 5 {
			return nil, errors.New("binance kline row does not contain close price")
		}

		tsMillis, err := jsonNumberToInt64(row[0])
		if err != nil {
			return nil, fmt.Errorf("parse binance open time: %w", err)
		}
		closePrice, err := jsonNumberToFloat64(row[4])
		if err != nil {
			return nil, fmt.Errorf("parse binance close price: %w", err)
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
		return nil, fmt.Errorf("no binance klines returned for %s %s", symbol, timeframe.Name)
	}

	sort.Slice(candles, func(i, j int) bool {
		return candles[i].Time.Before(candles[j].Time)
	})
	return deduplicateSeries(candles), nil
}
