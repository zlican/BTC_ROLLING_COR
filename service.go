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
	"sync"
	"time"
)

const (
	okxHistoryCandlesURL = "https://www.okx.com/api/v5/market/history-candles"
	defaultProxyURL      = "http://127.0.0.1:10809"
	timeframe            = "1Dutc"
	requestTimeout       = 20 * time.Second
	pageLimit            = 100
)

type AssetConfig struct {
	Symbol    string
	InstID    string
	PairLabel string
}

type okxResponse struct {
	Code string     `json:"code"`
	Msg  string     `json:"msg"`
	Data [][]string `json:"data"`
}

type dataPoint struct {
	Time  time.Time
	Close float64
}

type FactorPoint struct {
	Time  time.Time `json:"time"`
	Value float64   `json:"value"`
}

type AssetSeries struct {
	Symbol      string        `json:"symbol"`
	InstID      string        `json:"inst_id"`
	PairLabel   string        `json:"pair_label"`
	LatestTime  time.Time     `json:"latest_time"`
	LatestValue float64       `json:"latest_value"`
	Points      []FactorPoint `json:"points,omitempty"`
}

type FactorDataset struct {
	Benchmark     string    `json:"benchmark"`
	Timeframe     string    `json:"timeframe"`
	RollingWindow int       `json:"rolling_window"`
	UpdatedAt     time.Time `json:"updated_at"`
	Assets        map[string]*AssetSeries
	Order         []string `json:"order"`
}

type FactorService struct {
	client          *http.Client
	ttl             time.Duration
	lookbackDays    int
	rollingWindow   int
	benchmarkInstID string
	assets          []AssetConfig

	mu       sync.RWMutex
	cachedAt time.Time
	cached   *FactorDataset
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
	s.mu.RLock()
	if s.cached != nil && time.Since(s.cachedAt) < s.ttl {
		cached := s.cached
		s.mu.RUnlock()
		return cached, nil
	}
	s.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cached != nil && time.Since(s.cachedAt) < s.ttl {
		return s.cached, nil
	}

	dataset, err := s.refresh(ctx)
	if err != nil {
		return nil, err
	}
	s.cached = dataset
	s.cachedAt = time.Now().UTC()
	return dataset, nil
}

func (s *FactorService) refresh(ctx context.Context) (*FactorDataset, error) {
	endDate := time.Now().UTC().Truncate(24 * time.Hour)
	startDate := endDate.AddDate(0, 0, -s.lookbackDays)

	seriesByLabel := make(map[string][]dataPoint, len(s.assets)+1)

	benchmarkSeries, err := fetchHistoricalData(ctx, s.client, s.benchmarkInstID, startDate, endDate)
	if err != nil {
		return nil, fmt.Errorf("fetch %s: %w", s.benchmarkInstID, err)
	}
	seriesByLabel[s.benchmarkInstID] = benchmarkSeries

	type assetResult struct {
		cfg    AssetConfig
		series []dataPoint
		err    error
	}

	results := make(chan assetResult, len(s.assets))
	var wg sync.WaitGroup
	for _, asset := range s.assets {
		wg.Add(1)
		go func(cfg AssetConfig) {
			defer wg.Done()
			series, fetchErr := fetchHistoricalData(ctx, s.client, cfg.InstID, startDate, endDate)
			results <- assetResult{cfg: cfg, series: series, err: fetchErr}
		}(asset)
	}
	wg.Wait()
	close(results)

	for result := range results {
		if result.err != nil {
			return nil, fmt.Errorf("fetch %s: %w", result.cfg.InstID, result.err)
		}
		seriesByLabel[result.cfg.PairLabel] = result.series
	}

	alignedDates, alignedValues, err := alignSeries(seriesByLabel)
	if err != nil {
		return nil, err
	}

	benchmarkValues := alignedValues[s.benchmarkInstID]
	assets := make(map[string]*AssetSeries, len(s.assets))
	order := make([]string, 0, len(s.assets))

	for _, asset := range s.assets {
		points, calcErr := rollingCorrelation(alignedDates, alignedValues[asset.PairLabel], benchmarkValues, s.rollingWindow)
		if calcErr != nil {
			return nil, fmt.Errorf("calculate %s: %w", asset.PairLabel, calcErr)
		}
		if len(points) == 0 {
			return nil, fmt.Errorf("%s has no correlation points", asset.PairLabel)
		}

		latest := points[len(points)-1]
		assets[asset.Symbol] = &AssetSeries{
			Symbol:      asset.Symbol,
			InstID:      asset.InstID,
			PairLabel:   asset.PairLabel,
			LatestTime:  latest.Time,
			LatestValue: latest.Value,
			Points:      points,
		}
		order = append(order, asset.Symbol)
	}

	return &FactorDataset{
		Benchmark:     s.benchmarkInstID,
		Timeframe:     "1D",
		RollingWindow: s.rollingWindow,
		UpdatedAt:     time.Now().UTC(),
		Assets:        assets,
		Order:         order,
	}, nil
}

func fetchHistoricalData(ctx context.Context, client *http.Client, instID string, startDate, endDate time.Time) ([]dataPoint, error) {
	var candles []dataPoint
	var cursor string

	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, okxHistoryCandlesURL, nil)
		if err != nil {
			return nil, fmt.Errorf("build request: %w", err)
		}

		query := req.URL.Query()
		query.Set("instId", instID)
		query.Set("bar", timeframe)
		query.Set("limit", strconv.Itoa(pageLimit))
		if cursor != "" {
			query.Set("after", cursor)
		}
		req.URL.RawQuery = query.Encode()

		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("request OKX: %w", err)
		}

		var payload okxResponse
		decodeErr := json.NewDecoder(resp.Body).Decode(&payload)
		closeErr := resp.Body.Close()
		if decodeErr != nil {
			return nil, fmt.Errorf("decode OKX response: %w", decodeErr)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("close response body: %w", closeErr)
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
				return nil, errors.New("OKX response row does not contain close price")
			}

			tsMillis, err := strconv.ParseInt(row[0], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("parse timestamp %q: %w", row[0], err)
			}
			closePrice, err := strconv.ParseFloat(row[4], 64)
			if err != nil {
				return nil, fmt.Errorf("parse close price %q: %w", row[4], err)
			}

			timestamp := time.UnixMilli(tsMillis).UTC()
			if !timestamp.Before(startDate) && !timestamp.After(endDate) {
				candles = append(candles, dataPoint{Time: timestamp, Close: closePrice})
			}
		}

		oldestMillis, err := strconv.ParseInt(payload.Data[len(payload.Data)-1][0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse oldest timestamp: %w", err)
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
		return nil, fmt.Errorf("no candles returned for %s between %s and %s", instID, startDate.Format("2006-01-02"), endDate.Format("2006-01-02"))
	}

	unique := make(map[time.Time]float64, len(candles))
	for _, candle := range candles {
		unique[candle.Time] = candle.Close
	}

	series := make([]dataPoint, 0, len(unique))
	for ts, closePrice := range unique {
		series = append(series, dataPoint{Time: ts, Close: closePrice})
	}
	sort.Slice(series, func(i, j int) bool {
		return series[i].Time.Before(series[j].Time)
	})
	return series, nil
}

func alignSeries(seriesByLabel map[string][]dataPoint) ([]time.Time, map[string][]float64, error) {
	dateCounts := make(map[time.Time]int)
	valuesByLabel := make(map[string]map[time.Time]float64, len(seriesByLabel))

	for label, series := range seriesByLabel {
		perDate := make(map[time.Time]float64, len(series))
		for _, point := range series {
			perDate[point.Time] = point.Close
		}
		valuesByLabel[label] = perDate
		for ts := range perDate {
			dateCounts[ts]++
		}
	}

	var alignedDates []time.Time
	for ts, count := range dateCounts {
		if count == len(seriesByLabel) {
			alignedDates = append(alignedDates, ts)
		}
	}
	if len(alignedDates) == 0 {
		return nil, nil, errors.New("no aligned dates across benchmark and assets")
	}

	sort.Slice(alignedDates, func(i, j int) bool {
		return alignedDates[i].Before(alignedDates[j])
	})

	alignedValues := make(map[string][]float64, len(seriesByLabel))
	for label, perDate := range valuesByLabel {
		values := make([]float64, 0, len(alignedDates))
		for _, ts := range alignedDates {
			values = append(values, perDate[ts])
		}
		alignedValues[label] = values
	}

	return alignedDates, alignedValues, nil
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
