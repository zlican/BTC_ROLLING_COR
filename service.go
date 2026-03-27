package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	binanceTicker24hrURL = "https://fapi.binance.com/fapi/v1/ticker/24hr"
	binanceKlinesURL     = "https://fapi.binance.com/fapi/v1/klines"
	okxHistoryCandlesURL = "https://www.okx.com/api/v5/market/history-candles"
	defaultProxyURL      = "http://127.0.0.1:10809"

	binanceInterval  = "1d"
	okxTimeframe     = "1Dutc"
	requestTimeout   = 20 * time.Second
	pageLimit        = 100
	binanceKlineSize = 150
)

type AssetConfig struct {
	Symbol       string
	DisplayName  string
	QuoteVolume  float64
	UniverseRank int
}

type FactorPoint struct {
	Time  time.Time `json:"time"`
	Value float64   `json:"value"`
}

type AssetSeries struct {
	Symbol        string        `json:"symbol"`
	DisplayName   string        `json:"display_name"`
	InstID        string        `json:"inst_id"`
	PairLabel     string        `json:"pair_label"`
	BenchmarkInst string        `json:"benchmark_inst"`
	DataSource    string        `json:"data_source"`
	QuoteVolume   float64       `json:"quote_volume"`
	LatestTime    time.Time     `json:"latest_time"`
	LatestValue   float64       `json:"latest_value"`
	Points        []FactorPoint `json:"points,omitempty"`
}

type FactorDataset struct {
	Benchmark           string    `json:"benchmark"`
	Timeframe           string    `json:"timeframe"`
	RollingWindow       int       `json:"rolling_window"`
	UpdatedAt           time.Time `json:"updated_at"`
	UniverseUpdatedAt   time.Time `json:"universe_updated_at"`
	UniverseMinQuoteVol float64   `json:"universe_min_quote_vol"`
	Assets              map[string]*AssetSeries
	Order               []string `json:"order"`
}

type dataPoint struct {
	Time  time.Time
	Close float64
}

type okxResponse struct {
	Code string     `json:"code"`
	Msg  string     `json:"msg"`
	Data [][]string `json:"data"`
}

type binanceTickerItem struct {
	Symbol      string `json:"symbol"`
	QuoteVolume string `json:"quoteVolume"`
}

type MarketDataProvider interface {
	Name() string
	FetchBenchmarkHistory(ctx context.Context, startDate, endDate time.Time) (string, []dataPoint, error)
	FetchAssetHistory(ctx context.Context, symbol string, startDate, endDate time.Time) (string, string, []dataPoint, error)
}

type BinanceProvider struct {
	client *http.Client
}

type OKXProvider struct {
	client *http.Client
}

type benchmarkData struct {
	InstID string
	Series []dataPoint
}

type assetResult struct {
	Asset  *AssetSeries
	Skip   bool
	Reason string
}

type FactorService struct {
	universeProvider  *BinanceProvider
	providers         []MarketDataProvider
	datasetTTL        time.Duration
	universeTTL       time.Duration
	lookbackDays      int
	rollingWindow     int
	minUniverseVolume float64

	datasetMu       sync.RWMutex
	datasetCachedAt time.Time
	datasetCached   *FactorDataset

	universeMu         sync.RWMutex
	universeCachedAt   time.Time
	universeUpdatedAt  time.Time
	universeCachedList []AssetConfig
}

func newHTTPClient() (*http.Client, error) {
	proxyURL := os.Getenv("HTTP_PROXY")
	if proxyURL == "" {
		proxyURL = os.Getenv("HTTPS_PROXY")
	}
	if proxyURL == "" && isTCPReachable("127.0.0.1:10809", 500*time.Millisecond) {
		proxyURL = defaultProxyURL
	}

	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout: requestTimeout,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	if proxyURL != "" {
		parsedProxyURL, err := url.Parse(proxyURL)
		if err != nil {
			return nil, fmt.Errorf("invalid proxy URL %q: %w", proxyURL, err)
		}
		transport.Proxy = http.ProxyURL(parsedProxyURL)
	}

	return &http.Client{
		Timeout:   requestTimeout,
		Transport: transport,
	}, nil
}

func isTCPReachable(address string, timeout time.Duration) bool {
	conn, err := net.DialTimeout("tcp", address, timeout)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func (s *FactorService) GetDataset(ctx context.Context) (*FactorDataset, error) {
	s.datasetMu.RLock()
	if s.datasetCached != nil && time.Since(s.datasetCachedAt) < s.datasetTTL {
		cached := s.datasetCached
		s.datasetMu.RUnlock()
		return cached, nil
	}
	s.datasetMu.RUnlock()

	s.datasetMu.Lock()
	defer s.datasetMu.Unlock()

	if s.datasetCached != nil && time.Since(s.datasetCachedAt) < s.datasetTTL {
		return s.datasetCached, nil
	}

	dataset, err := s.refresh(ctx)
	if err != nil {
		return nil, err
	}
	s.datasetCached = dataset
	s.datasetCachedAt = time.Now().UTC()
	return dataset, nil
}

func (s *FactorService) refresh(ctx context.Context) (*FactorDataset, error) {
	assets, universeUpdatedAt, err := s.getDynamicUniverse(ctx)
	if err != nil {
		return nil, err
	}
	if len(assets) == 0 {
		return nil, errors.New("dynamic universe is empty after filtering")
	}

	endDate := time.Now().UTC().Truncate(24 * time.Hour)
	startDate := endDate.AddDate(0, 0, -s.lookbackDays)

	benchmarks := make(map[string]benchmarkData, len(s.providers))
	for _, provider := range s.providers {
		instID, series, fetchErr := provider.FetchBenchmarkHistory(ctx, startDate, endDate)
		if fetchErr != nil {
			continue
		}
		benchmarks[provider.Name()] = benchmarkData{
			InstID: instID,
			Series: series,
		}
	}
	if len(benchmarks) == 0 {
		return nil, errors.New("failed to fetch benchmark history from all providers")
	}

	results := make(chan assetResult, len(assets))
	var wg sync.WaitGroup
	sem := make(chan struct{}, 8)

	for _, asset := range assets {
		wg.Add(1)
		go func(cfg AssetConfig) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			results <- s.buildAssetSeries(ctx, cfg, startDate, endDate, benchmarks)
		}(asset)
	}

	wg.Wait()
	close(results)

	dataset := &FactorDataset{
		Benchmark:           "BTC benchmark by provider",
		Timeframe:           "1D",
		RollingWindow:       s.rollingWindow,
		UpdatedAt:           time.Now().UTC(),
		UniverseUpdatedAt:   universeUpdatedAt,
		UniverseMinQuoteVol: s.minUniverseVolume,
		Assets:              make(map[string]*AssetSeries),
	}

	for result := range results {
		if result.Skip || result.Asset == nil {
			continue
		}
		dataset.Assets[result.Asset.Symbol] = result.Asset
		dataset.Order = append(dataset.Order, result.Asset.Symbol)
	}

	if len(dataset.Order) == 0 {
		return nil, errors.New("no assets remained after historical data and rolling-window filtering")
	}

	sort.Slice(dataset.Order, func(i, j int) bool {
		left := dataset.Assets[dataset.Order[i]]
		right := dataset.Assets[dataset.Order[j]]
		if left.QuoteVolume == right.QuoteVolume {
			return left.Symbol < right.Symbol
		}
		return left.QuoteVolume > right.QuoteVolume
	})

	return dataset, nil
}

func (s *FactorService) buildAssetSeries(ctx context.Context, cfg AssetConfig, startDate, endDate time.Time, benchmarks map[string]benchmarkData) assetResult {
	for _, provider := range s.providers {
		benchmark, ok := benchmarks[provider.Name()]
		if !ok {
			continue
		}

		instID, pairLabel, series, err := provider.FetchAssetHistory(ctx, cfg.Symbol, startDate, endDate)
		if err != nil {
			continue
		}

		dates, assetValues, benchmarkValues, err := alignPairSeries(series, benchmark.Series)
		if err != nil {
			continue
		}

		points, err := rollingCorrelation(dates, assetValues, benchmarkValues, s.rollingWindow)
		if err != nil || len(points) == 0 {
			continue
		}

		latest := points[len(points)-1]
		return assetResult{
			Asset: &AssetSeries{
				Symbol:        cfg.Symbol,
				DisplayName:   cfg.DisplayName,
				InstID:        instID,
				PairLabel:     pairLabel,
				BenchmarkInst: benchmark.InstID,
				DataSource:    provider.Name(),
				QuoteVolume:   cfg.QuoteVolume,
				LatestTime:    latest.Time,
				LatestValue:   latest.Value,
				Points:        points,
			},
		}
	}

	return assetResult{
		Skip:   true,
		Reason: "all providers failed or aligned data is insufficient",
	}
}

func (s *FactorService) getDynamicUniverse(ctx context.Context) ([]AssetConfig, time.Time, error) {
	s.universeMu.RLock()
	if len(s.universeCachedList) > 0 && time.Since(s.universeCachedAt) < s.universeTTL {
		cached := cloneAssetConfigs(s.universeCachedList)
		updatedAt := s.universeUpdatedAt
		s.universeMu.RUnlock()
		return cached, updatedAt, nil
	}
	s.universeMu.RUnlock()

	s.universeMu.Lock()
	defer s.universeMu.Unlock()

	if len(s.universeCachedList) > 0 && time.Since(s.universeCachedAt) < s.universeTTL {
		return cloneAssetConfigs(s.universeCachedList), s.universeUpdatedAt, nil
	}

	assets, updatedAt, err := s.universeProvider.FetchDynamicUniverse(ctx, s.minUniverseVolume)
	if err != nil {
		return nil, time.Time{}, err
	}

	s.universeCachedList = cloneAssetConfigs(assets)
	s.universeCachedAt = time.Now().UTC()
	s.universeUpdatedAt = updatedAt

	return cloneAssetConfigs(assets), updatedAt, nil
}

func cloneAssetConfigs(items []AssetConfig) []AssetConfig {
	out := make([]AssetConfig, len(items))
	copy(out, items)
	return out
}

func (p *BinanceProvider) Name() string {
	return "binance"
}

func (p *BinanceProvider) FetchDynamicUniverse(ctx context.Context, minQuoteVolume float64) ([]AssetConfig, time.Time, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, binanceTicker24hrURL, nil)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("build binance ticker request: %w", err)
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("request binance ticker 24hr: %w", err)
	}

	var payload []binanceTickerItem
	decodeErr := json.NewDecoder(resp.Body).Decode(&payload)
	closeErr := resp.Body.Close()
	if decodeErr != nil {
		return nil, time.Time{}, fmt.Errorf("decode binance ticker 24hr: %w", decodeErr)
	}
	if closeErr != nil {
		return nil, time.Time{}, fmt.Errorf("close binance ticker body: %w", closeErr)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, time.Time{}, fmt.Errorf("binance ticker 24hr returned HTTP %d", resp.StatusCode)
	}

	var assets []AssetConfig
	for _, item := range payload {
		if !strings.HasSuffix(item.Symbol, "USDT") || item.Symbol == "BTCUSDT" {
			continue
		}

		quoteVolume, err := strconv.ParseFloat(item.QuoteVolume, 64)
		if err != nil || quoteVolume < minQuoteVolume {
			continue
		}

		displayName := strings.TrimSuffix(item.Symbol, "USDT")
		if displayName == "" {
			displayName = item.Symbol
		}

		assets = append(assets, AssetConfig{
			Symbol:      item.Symbol,
			DisplayName: displayName,
			QuoteVolume: quoteVolume,
		})
	}

	sort.Slice(assets, func(i, j int) bool {
		if assets[i].QuoteVolume == assets[j].QuoteVolume {
			return assets[i].Symbol < assets[j].Symbol
		}
		return assets[i].QuoteVolume > assets[j].QuoteVolume
	})

	for i := range assets {
		assets[i].UniverseRank = i + 1
	}

	return assets, time.Now().UTC(), nil
}

func (p *BinanceProvider) FetchBenchmarkHistory(ctx context.Context, startDate, endDate time.Time) (string, []dataPoint, error) {
	series, err := p.fetchKlines(ctx, "BTCUSDT", startDate, endDate)
	if err != nil {
		return "", nil, err
	}
	return "BTCUSDT", series, nil
}

func (p *BinanceProvider) FetchAssetHistory(ctx context.Context, symbol string, startDate, endDate time.Time) (string, string, []dataPoint, error) {
	series, err := p.fetchKlines(ctx, symbol, startDate, endDate)
	if err != nil {
		return "", "", nil, err
	}
	return symbol, fmt.Sprintf("%s vs BTCUSDT", symbol), series, nil
}

func (p *BinanceProvider) fetchKlines(ctx context.Context, symbol string, startDate, endDate time.Time) ([]dataPoint, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, binanceKlinesURL, nil)
	if err != nil {
		return nil, fmt.Errorf("build binance klines request: %w", err)
	}

	query := req.URL.Query()
	query.Set("symbol", symbol)
	query.Set("interval", binanceInterval)
	query.Set("limit", strconv.Itoa(binanceKlineSize))
	query.Set("startTime", strconv.FormatInt(startDate.UnixMilli(), 10))
	query.Set("endTime", strconv.FormatInt(endDate.Add(24*time.Hour-time.Millisecond).UnixMilli(), 10))
	req.URL.RawQuery = query.Encode()

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request binance klines: %w", err)
	}

	var payload [][]any
	decodeErr := json.NewDecoder(resp.Body).Decode(&payload)
	closeErr := resp.Body.Close()
	if decodeErr != nil {
		return nil, fmt.Errorf("decode binance klines: %w", decodeErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close binance klines body: %w", closeErr)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("binance klines returned HTTP %d", resp.StatusCode)
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
		return nil, fmt.Errorf("no binance klines returned for %s", symbol)
	}

	sort.Slice(candles, func(i, j int) bool {
		return candles[i].Time.Before(candles[j].Time)
	})
	return deduplicateSeries(candles), nil
}

func (p *OKXProvider) Name() string {
	return "okx"
}

func (p *OKXProvider) FetchBenchmarkHistory(ctx context.Context, startDate, endDate time.Time) (string, []dataPoint, error) {
	series, err := p.fetchHistoricalData(ctx, "BTC-USDT", startDate, endDate)
	if err != nil {
		return "", nil, err
	}
	return "BTC-USDT", series, nil
}

func (p *OKXProvider) FetchAssetHistory(ctx context.Context, symbol string, startDate, endDate time.Time) (string, string, []dataPoint, error) {
	if !strings.HasSuffix(symbol, "USDT") {
		return "", "", nil, fmt.Errorf("unsupported OKX symbol %s", symbol)
	}

	base := strings.TrimSuffix(symbol, "USDT")
	if base == "" {
		return "", "", nil, fmt.Errorf("invalid OKX symbol %s", symbol)
	}

	instID := base + "-USDT"
	series, err := p.fetchHistoricalData(ctx, instID, startDate, endDate)
	if err != nil {
		return "", "", nil, err
	}
	return instID, fmt.Sprintf("%s vs BTC-USDT", instID), series, nil
}

func (p *OKXProvider) fetchHistoricalData(ctx context.Context, instID string, startDate, endDate time.Time) ([]dataPoint, error) {
	var candles []dataPoint
	var cursor string

	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, okxHistoryCandlesURL, nil)
		if err != nil {
			return nil, fmt.Errorf("build OKX request: %w", err)
		}

		query := req.URL.Query()
		query.Set("instId", instID)
		query.Set("bar", okxTimeframe)
		query.Set("limit", strconv.Itoa(pageLimit))
		if cursor != "" {
			query.Set("after", cursor)
		}
		req.URL.RawQuery = query.Encode()

		resp, err := p.client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("request OKX history candles: %w", err)
		}

		var payload okxResponse
		decodeErr := json.NewDecoder(resp.Body).Decode(&payload)
		closeErr := resp.Body.Close()
		if decodeErr != nil {
			return nil, fmt.Errorf("decode OKX response: %w", decodeErr)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("close OKX body: %w", closeErr)
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("OKX returned HTTP %d", resp.StatusCode)
		}
		if payload.Code != "0" {
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
		return nil, fmt.Errorf("no OKX candles returned for %s", instID)
	}

	sort.Slice(candles, func(i, j int) bool {
		return candles[i].Time.Before(candles[j].Time)
	})
	return deduplicateSeries(candles), nil
}

func deduplicateSeries(items []dataPoint) []dataPoint {
	unique := make(map[time.Time]float64, len(items))
	for _, item := range items {
		unique[item.Time] = item.Close
	}

	out := make([]dataPoint, 0, len(unique))
	for ts, closePrice := range unique {
		out = append(out, dataPoint{
			Time:  ts,
			Close: closePrice,
		})
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].Time.Before(out[j].Time)
	})
	return out
}

func alignPairSeries(assetSeries, benchmarkSeries []dataPoint) ([]time.Time, []float64, []float64, error) {
	if len(assetSeries) == 0 || len(benchmarkSeries) == 0 {
		return nil, nil, nil, errors.New("one side of pair series is empty")
	}

	assetMap := make(map[time.Time]float64, len(assetSeries))
	for _, point := range assetSeries {
		assetMap[point.Time] = point.Close
	}

	var dates []time.Time
	for _, point := range benchmarkSeries {
		if _, ok := assetMap[point.Time]; ok {
			dates = append(dates, point.Time)
		}
	}
	if len(dates) == 0 {
		return nil, nil, nil, errors.New("no overlapping timestamps between asset and benchmark")
	}

	sort.Slice(dates, func(i, j int) bool {
		return dates[i].Before(dates[j])
	})

	benchmarkMap := make(map[time.Time]float64, len(benchmarkSeries))
	for _, point := range benchmarkSeries {
		benchmarkMap[point.Time] = point.Close
	}

	assetValues := make([]float64, 0, len(dates))
	benchmarkValues := make([]float64, 0, len(dates))
	for _, ts := range dates {
		assetValues = append(assetValues, assetMap[ts])
		benchmarkValues = append(benchmarkValues, benchmarkMap[ts])
	}

	return dates, assetValues, benchmarkValues, nil
}

func rollingCorrelation(dates []time.Time, seriesA, seriesB []float64, window int) ([]FactorPoint, error) {
	if len(dates) != len(seriesA) || len(seriesA) != len(seriesB) {
		return nil, errors.New("series length mismatch")
	}
	if len(seriesA) < window {
		return nil, fmt.Errorf("series length %d is smaller than rolling window %d", len(seriesA), window)
	}

	points := make([]FactorPoint, 0, len(seriesA)-window+1)
	for end := window; end <= len(seriesA); end++ {
		start := end - window
		points = append(points, FactorPoint{
			Time:  dates[end-1],
			Value: correlation(seriesA[start:end], seriesB[start:end]),
		})
	}
	return points, nil
}

func correlation(a, b []float64) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}

	var sumA float64
	var sumB float64
	for i := range a {
		sumA += a[i]
		sumB += b[i]
	}

	meanA := sumA / float64(len(a))
	meanB := sumB / float64(len(b))

	var covariance float64
	var varianceA float64
	var varianceB float64
	for i := range a {
		deltaA := a[i] - meanA
		deltaB := b[i] - meanB
		covariance += deltaA * deltaB
		varianceA += deltaA * deltaA
		varianceB += deltaB * deltaB
	}

	if varianceA == 0 || varianceB == 0 {
		return 0
	}

	value := covariance / math.Sqrt(varianceA*varianceB)
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0
	}
	return value
}

func jsonNumberToInt64(value any) (int64, error) {
	switch v := value.(type) {
	case float64:
		return int64(v), nil
	case string:
		return strconv.ParseInt(v, 10, 64)
	case json.Number:
		return v.Int64()
	default:
		return 0, fmt.Errorf("unsupported numeric type %T", value)
	}
}

func jsonNumberToFloat64(value any) (float64, error) {
	switch v := value.(type) {
	case float64:
		return v, nil
	case string:
		return strconv.ParseFloat(v, 64)
	case json.Number:
		return v.Float64()
	default:
		return 0, fmt.Errorf("unsupported numeric type %T", value)
	}
}
