package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand/v2"
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
	bybitKlinesURL       = "https://api.bybit.com/v5/market/kline"
	okxHistoryCandlesURL = "https://www.okx.com/api/v5/market/history-candles"
	defaultProxyURL      = "http://127.0.0.1:10809"

	requestTimeout       = 20 * time.Second
	refreshTimeout       = 3 * time.Minute
	pageLimit            = 100
	minReturnVar         = 1e-10
	maxRequestAttempts   = 4
	maxErrorBodyPreview  = 256
	binanceMinRequestGap = 120 * time.Millisecond
	bybitMinRequestGap   = 120 * time.Millisecond
	okxMinRequestGap     = 150 * time.Millisecond
)

var errSymbolUnsupported = errors.New("symbol unsupported by provider")

type TimeframeConfig struct {
	Name            string
	BinanceInterval string
	BybitInterval   string
	OKXBar          string
	CandleDuration  time.Duration
	HistoryBars     int
}

var timeframeConfigs = map[string]TimeframeConfig{
	"1H": {
		Name:            "1H",
		BinanceInterval: "1h",
		BybitInterval:   "60",
		OKXBar:          "1H",
		CandleDuration:  time.Hour,
		HistoryBars:     240,
	},
	"4H": {
		Name:            "4H",
		BinanceInterval: "4h",
		BybitInterval:   "240",
		OKXBar:          "4H",
		CandleDuration:  4 * time.Hour,
		HistoryBars:     240,
	},
	"1D": {
		Name:            "1D",
		BinanceInterval: "1d",
		BybitInterval:   "D",
		OKXBar:          "1Dutc",
		CandleDuration:  24 * time.Hour,
		HistoryBars:     120,
	},
	"1W": {
		Name:            "1W",
		BinanceInterval: "1w",
		BybitInterval:   "W",
		OKXBar:          "1Wutc",
		CandleDuration:  7 * 24 * time.Hour,
		HistoryBars:     104,
	},
}

var supportedTimeframes = []string{"1H", "4H", "1D", "1W"}

const (
	statusOK                  = "ok"
	statusInsufficientHistory = "insufficient_history"
	statusLowVariance         = "low_variance"
	statusAlignmentFailed     = "alignment_failed"
	statusUnavailable         = "unavailable"

	signalIndependent  = "independent"
	signalFollow       = "follow"
	signalStrongFollow = "strong_follow"
)

type AssetConfig struct {
	Symbol       string
	DisplayName  string
	QuoteVolume  float64
	LastPrice    float64
	EightHourPct float64
	UniverseRank int
}

type FactorPoint struct {
	Time  time.Time `json:"time"`
	Value float64   `json:"value"`
}

type FactorFrame struct {
	Timeframe     string        `json:"timeframe"`
	InstID        string        `json:"inst_id"`
	PairLabel     string        `json:"pair_label"`
	BenchmarkInst string        `json:"benchmark_inst"`
	DataSource    string        `json:"data_source"`
	Status        string        `json:"status"`
	SignalCode    string        `json:"signal_code"`
	LatestTime    time.Time     `json:"latest_time"`
	LatestCorr    float64       `json:"latest_corr"`
	LatestBeta    float64       `json:"latest_beta"`
	CorrPoints    []FactorPoint `json:"corr,omitempty"`
	BetaPoints    []FactorPoint `json:"beta,omitempty"`
}

type AssetSeries struct {
	Symbol        string  `json:"symbol"`
	DisplayName   string  `json:"display_name"`
	InstID        string  `json:"inst_id"`
	PairLabel     string  `json:"pair_label"`
	BenchmarkInst string  `json:"benchmark_inst"`
	DataSource    string  `json:"data_source"`
	QuoteVolume   float64 `json:"quote_volume"`
	EightHourPct  float64 `json:"eight_hour_pct"`
	Frames        map[string]*FactorFrame
	FrameOrder    []string `json:"frame_order"`
}

type FactorDataset struct {
	Benchmark           string    `json:"benchmark"`
	Timeframes          []string  `json:"timeframes"`
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

type bybitKlineResponse struct {
	RetCode int    `json:"retCode"`
	RetMsg  string `json:"retMsg"`
	Result  struct {
		Category string     `json:"category"`
		Symbol   string     `json:"symbol"`
		List     [][]string `json:"list"`
	} `json:"result"`
}

type binanceTickerItem struct {
	Symbol      string `json:"symbol"`
	QuoteVolume string `json:"quoteVolume"`
	LastPrice   string `json:"lastPrice"`
}

type MarketDataProvider interface {
	Name() string
	FetchBenchmarkHistory(ctx context.Context, timeframe TimeframeConfig, startDate, endDate time.Time) (string, []dataPoint, error)
	FetchAssetHistory(ctx context.Context, symbol string, timeframe TimeframeConfig, startDate, endDate time.Time) (string, string, []dataPoint, error)
}

type BinanceProvider struct {
	client *http.Client
}

type BybitProvider struct {
	client      *http.Client
	unsupported sync.Map
}

type OKXProvider struct {
	client      *http.Client
	unsupported sync.Map
}

type benchmarkData struct {
	InstID string
	Series []dataPoint
}

type assetResult struct {
	Asset *AssetSeries
	Skip  bool
}

type frameFetchResult struct {
	Frame  *FactorFrame
	Reason string
}

type FactorService struct {
	universeProvider  *BinanceProvider
	providers         []MarketDataProvider
	datasetTTL        time.Duration
	universeTTL       time.Duration
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

var outboundRequestSchedule = struct {
	mu   sync.Mutex
	next map[string]time.Time
}{
	next: make(map[string]time.Time),
}

func waitForRequestSlot(ctx context.Context, key string, minGap time.Duration) error {
	if minGap <= 0 {
		return nil
	}

	jitter := time.Duration(rand.Int64N(int64(minGap / 3)))
	outboundRequestSchedule.mu.Lock()
	slot := time.Now().UTC()
	if next := outboundRequestSchedule.next[key]; next.After(slot) {
		slot = next
	}
	outboundRequestSchedule.next[key] = slot.Add(minGap + jitter)
	outboundRequestSchedule.mu.Unlock()

	wait := time.Until(slot)
	if wait <= 0 {
		return nil
	}

	timer := time.NewTimer(wait)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func requestBackoff(attempt int, minGap time.Duration) time.Duration {
	if attempt <= 1 {
		return minGap + time.Duration(rand.Int64N(int64(minGap)))
	}
	backoff := minGap * time.Duration(1<<(attempt-1))
	if backoff > 2*time.Second {
		backoff = 2 * time.Second
	}
	return backoff + time.Duration(rand.Int64N(int64(minGap)))
}

func sleepWithContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func shouldRetryHTTPStatus(status int) bool {
	switch status {
	case http.StatusTooManyRequests,
		http.StatusRequestTimeout,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout:
		return true
	default:
		return status >= 500
	}
}

func doJSONRequest[T any](ctx context.Context, client *http.Client, requestName, rateKey string, minGap time.Duration, buildRequest func() (*http.Request, error)) (T, error) {
	var zero T
	var lastErr error

	for attempt := 1; attempt <= maxRequestAttempts; attempt++ {
		if err := waitForRequestSlot(ctx, rateKey, minGap); err != nil {
			return zero, fmt.Errorf("%s wait rate slot: %w", requestName, err)
		}

		req, err := buildRequest()
		if err != nil {
			return zero, fmt.Errorf("%s build request: %w", requestName, err)
		}

		resp, err := client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("%s request: %w", requestName, err)
		} else {
			body, readErr := io.ReadAll(resp.Body)
			closeErr := resp.Body.Close()
			if readErr != nil {
				lastErr = fmt.Errorf("%s read body: %w", requestName, readErr)
			} else if closeErr != nil {
				lastErr = fmt.Errorf("%s close body: %w", requestName, closeErr)
			} else if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				preview := strings.TrimSpace(string(body))
				if len(preview) > maxErrorBodyPreview {
					preview = preview[:maxErrorBodyPreview]
				}
				lastErr = fmt.Errorf("%s returned HTTP %d: %s", requestName, resp.StatusCode, preview)
				if !shouldRetryHTTPStatus(resp.StatusCode) {
					return zero, lastErr
				}
			} else {
				var payload T
				if err := json.Unmarshal(body, &payload); err != nil {
					return zero, fmt.Errorf("%s decode response: %w", requestName, err)
				}
				return payload, nil
			}
		}

		if attempt == maxRequestAttempts {
			break
		}
		if err := sleepWithContext(ctx, requestBackoff(attempt, minGap)); err != nil {
			return zero, fmt.Errorf("%s retry backoff: %w", requestName, err)
		}
	}

	return zero, lastErr
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

	refreshCtx, cancel := context.WithTimeout(context.Background(), refreshTimeout)
	defer cancel()

	dataset, err := s.refresh(refreshCtx)
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

	benchmarkSets := make(map[string]map[string]benchmarkData, len(s.providers))
	for _, provider := range s.providers {
		frames := make(map[string]benchmarkData, len(supportedTimeframes))
		for _, timeframeName := range supportedTimeframes {
			cfg := timeframeConfigs[timeframeName]
			startDate, endDate := timeframeRange(cfg)
			instID, series, fetchErr := provider.FetchBenchmarkHistory(ctx, cfg, startDate, endDate)
			if fetchErr != nil {
				continue
			}
			frames[timeframeName] = benchmarkData{
				InstID: instID,
				Series: series,
			}
		}
		if len(frames) > 0 {
			benchmarkSets[provider.Name()] = frames
		}
	}
	if len(benchmarkSets) == 0 {
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
			results <- s.buildAssetSeries(ctx, cfg, benchmarkSets)
		}(asset)
	}

	wg.Wait()
	close(results)

	dataset := &FactorDataset{
		Benchmark:           "BTC factor benchmark",
		Timeframes:          append([]string(nil), supportedTimeframes...),
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
		return nil, errors.New("no assets remained after factor calculation and rolling-window filtering")
	}

	timeframeCounts := make(map[string]int, len(supportedTimeframes))
	for _, symbol := range dataset.Order {
		for _, timeframeName := range dataset.Assets[symbol].FrameOrder {
			timeframeCounts[timeframeName]++
		}
	}
	log.Printf("dataset refreshed: universe=%d assets=%d 1H=%d 4H=%d 1D=%d 1W=%d",
		len(assets),
		len(dataset.Order),
		timeframeCounts["1H"],
		timeframeCounts["4H"],
		timeframeCounts["1D"],
		timeframeCounts["1W"],
	)

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

func (s *FactorService) buildAssetSeries(ctx context.Context, cfg AssetConfig, benchmarkSets map[string]map[string]benchmarkData) assetResult {
	frames := make(map[string]*FactorFrame, len(supportedTimeframes))
	frameOrder := make([]string, 0, len(supportedTimeframes))

	for _, timeframeName := range supportedTimeframes {
		cfgTimeframe := timeframeConfigs[timeframeName]
		frameResult := s.fetchFrame(ctx, cfg, cfgTimeframe, benchmarkSets)
		if frameResult.Frame != nil {
			frames[timeframeName] = frameResult.Frame
			frameOrder = append(frameOrder, timeframeName)
		}
	}

	if len(frameOrder) == 0 {
		return assetResult{Skip: true}
	}

	primaryFrame := pickPrimaryFrame(frames)
	return assetResult{
		Asset: &AssetSeries{
			Symbol:        cfg.Symbol,
			DisplayName:   cfg.DisplayName,
			InstID:        primaryFrame.InstID,
			PairLabel:     primaryFrame.PairLabel,
			BenchmarkInst: primaryFrame.BenchmarkInst,
			DataSource:    primaryFrame.DataSource,
			QuoteVolume:   cfg.QuoteVolume,
			EightHourPct:  cfg.EightHourPct,
			Frames:        frames,
			FrameOrder:    frameOrder,
		},
	}
}

func (s *FactorService) fetchFrame(ctx context.Context, cfg AssetConfig, cfgTimeframe TimeframeConfig, benchmarkSets map[string]map[string]benchmarkData) frameFetchResult {
	startDate, endDate := timeframeRange(cfgTimeframe)
	var lastErr error
	var fallback *FactorFrame

	for _, provider := range s.providers {
		benchmarkFrames, ok := benchmarkSets[provider.Name()]
		if !ok {
			continue
		}
		benchmark, ok := benchmarkFrames[cfgTimeframe.Name]
		if !ok {
			continue
		}

		instID, pairLabel, priceSeries, err := provider.FetchAssetHistory(ctx, cfg.Symbol, cfgTimeframe, startDate, endDate)
		if err != nil {
			if errors.Is(err, errSymbolUnsupported) {
				continue
			}
			lastErr = err
			continue
		}

		priceDates, assetPrices, benchmarkPrices, err := alignPairSeries(priceSeries, benchmark.Series)
		if err != nil {
			lastErr = err
			if fallback == nil {
				fallback = placeholderFrame(cfgTimeframe.Name, priceDates, instID, pairLabel, benchmark.InstID, provider.Name(), statusAlignmentFailed)
			}
			return frameFetchResult{Frame: fallback, Reason: err.Error()}
		}

		returnDates, assetReturns, benchmarkReturns, err := computeReturns(priceDates, assetPrices, benchmarkPrices)
		if err != nil {
			lastErr = err
			if fallback == nil {
				fallback = placeholderFrame(cfgTimeframe.Name, priceDates, instID, pairLabel, benchmark.InstID, provider.Name(), statusInsufficientHistory)
			}
			return frameFetchResult{Frame: fallback, Reason: err.Error()}
		}

		frame, err := buildFactorFrame(cfgTimeframe.Name, returnDates, assetReturns, benchmarkReturns, s.rollingWindow)
		if err != nil {
			lastErr = err
			if fallback == nil {
				fallback = placeholderFrame(cfgTimeframe.Name, returnDates, instID, pairLabel, benchmark.InstID, provider.Name(), placeholderSignalForFactorError(err))
			}
			return frameFetchResult{Frame: fallback, Reason: err.Error()}
		}

		frame.InstID = instID
		frame.PairLabel = pairLabel
		frame.BenchmarkInst = benchmark.InstID
		frame.DataSource = provider.Name()
		return frameFetchResult{Frame: frame}
	}

	if fallback != nil {
		if lastErr != nil && !errors.Is(lastErr, errSymbolUnsupported) {
			log.Printf("frame fallback used: symbol=%s timeframe=%s err=%v", cfg.Symbol, cfgTimeframe.Name, lastErr)
		}
		return frameFetchResult{Frame: fallback, Reason: fallback.Status}
	}

	if lastErr != nil && !errors.Is(lastErr, errSymbolUnsupported) {
		log.Printf("frame fetch failed: symbol=%s timeframe=%s err=%v", cfg.Symbol, cfgTimeframe.Name, lastErr)
		return frameFetchResult{Reason: lastErr.Error()}
	}
	return frameFetchResult{Reason: "no provider available"}
}

func placeholderSignalForFactorError(err error) string {
	if err == nil {
		return statusUnavailable
	}
	message := err.Error()
	switch {
	case strings.Contains(message, "return variance is too small"):
		return statusLowVariance
	case strings.Contains(message, "smaller than rolling window"):
		return statusInsufficientHistory
	default:
		return statusUnavailable
	}
}

func placeholderFrame(timeframe string, dates []time.Time, instID, pairLabel, benchmarkInst, dataSource, status string) *FactorFrame {
	latest := time.Time{}
	if len(dates) > 0 {
		latest = dates[len(dates)-1]
	}
	return &FactorFrame{
		Timeframe:     timeframe,
		InstID:        instID,
		PairLabel:     pairLabel,
		BenchmarkInst: benchmarkInst,
		DataSource:    dataSource,
		Status:        status,
		LatestTime:    latest,
	}
}

func pickPrimaryFrame(frames map[string]*FactorFrame) *FactorFrame {
	for _, timeframeName := range []string{"1D", "1H", "4H", "1W"} {
		if frame, ok := frames[timeframeName]; ok {
			return frame
		}
	}
	for _, frame := range frames {
		return frame
	}
	return &FactorFrame{}
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
	payload, err := doJSONRequest[[]binanceTickerItem](
		ctx,
		p.client,
		"binance ticker 24hr",
		"binance",
		binanceMinRequestGap,
		func() (*http.Request, error) {
			return http.NewRequestWithContext(ctx, http.MethodGet, binanceTicker24hrURL, nil)
		},
	)
	if err != nil {
		return nil, time.Time{}, err
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
		lastPrice, err := strconv.ParseFloat(item.LastPrice, 64)
		if err != nil || lastPrice <= 0 {
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
			LastPrice:   lastPrice,
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

	if err := p.attachEightHourChanges(ctx, assets); err != nil {
		return nil, time.Time{}, err
	}

	return assets, time.Now().UTC(), nil
}

func (p *BinanceProvider) attachEightHourChanges(ctx context.Context, assets []AssetConfig) error {
	if len(assets) == 0 {
		return nil
	}

	startTimeMs := currentEightHourSegmentStart().UnixMilli()
	sem := make(chan struct{}, 8)
	errCh := make(chan error, len(assets))
	var wg sync.WaitGroup

	for i := range assets {
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

func timeframeRange(cfg TimeframeConfig) (time.Time, time.Time) {
	endDate := timeframeCompletedBoundary(cfg, time.Now().UTC())
	startDate := endDate.Add(-time.Duration(cfg.HistoryBars-1) * cfg.CandleDuration)
	return startDate, endDate
}

func timeframeCompletedBoundary(cfg TimeframeConfig, now time.Time) time.Time {
	now = now.UTC()

	switch cfg.Name {
	case "1H":
		current := now.Truncate(time.Hour)
		return current.Add(-time.Hour)
	case "4H":
		current := time.Date(now.Year(), now.Month(), now.Day(), (now.Hour()/4)*4, 0, 0, 0, time.UTC)
		return current.Add(-4 * time.Hour)
	case "1D":
		startOfDay := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
		return startOfDay.Add(-24 * time.Hour)
	case "1W":
		startOfDay := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
		offset := (int(startOfDay.Weekday()) + 6) % 7
		startOfWeek := startOfDay.AddDate(0, 0, -offset)
		return startOfWeek.AddDate(0, 0, -7)
	default:
		return now.Truncate(cfg.CandleDuration).Add(-cfg.CandleDuration)
	}
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

func buildFactorFrame(timeframe string, dates []time.Time, assetReturns, benchmarkReturns []float64, window int) (*FactorFrame, error) {
	if variance(assetReturns) < minReturnVar || variance(benchmarkReturns) < minReturnVar {
		return nil, errors.New("return variance is too small for stable factor calculation")
	}

	corrPoints, err := rollingCorrelationPoints(dates, assetReturns, benchmarkReturns, window)
	if err != nil {
		return nil, err
	}

	betaPoints, _, err := rollingBetaPoints(dates, assetReturns, benchmarkReturns, window)
	if err != nil {
		return nil, err
	}

	latestCorr := corrPoints[len(corrPoints)-1]
	latestBeta := betaPoints[len(betaPoints)-1]

	return &FactorFrame{
		Timeframe:  timeframe,
		Status:     statusOK,
		SignalCode: classifySignalCode(latestCorr.Value, latestBeta.Value),
		LatestTime: latestCorr.Time,
		LatestCorr: latestCorr.Value,
		LatestBeta: latestBeta.Value,
		CorrPoints: corrPoints,
		BetaPoints: betaPoints,
	}, nil
}

func classifySignalCode(corr, beta float64) string {
	if corr < 0.75 {
		return signalIndependent
	}
	if beta > 1.5 {
		return signalStrongFollow
	}
	return signalFollow
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

func computeReturns(dates []time.Time, assetPrices, benchmarkPrices []float64) ([]time.Time, []float64, []float64, error) {
	if len(dates) != len(assetPrices) || len(assetPrices) != len(benchmarkPrices) {
		return nil, nil, nil, errors.New("price series length mismatch")
	}
	if len(dates) < 2 {
		return nil, nil, nil, errors.New("at least two price points are required to compute returns")
	}

	returnDates := append([]time.Time(nil), dates[1:]...)
	assetReturns := computeReturnSeries(assetPrices)
	benchmarkReturns := computeReturnSeries(benchmarkPrices)
	return returnDates, assetReturns, benchmarkReturns, nil
}

func computeReturnSeries(prices []float64) []float64 {
	returns := make([]float64, 0, len(prices)-1)
	for i := 1; i < len(prices); i++ {
		if prices[i-1] == 0 {
			returns = append(returns, 0)
			continue
		}
		returns = append(returns, (prices[i]-prices[i-1])/prices[i-1])
	}
	return returns
}

func rollingCorrelationPoints(dates []time.Time, seriesA, seriesB []float64, window int) ([]FactorPoint, error) {
	values, err := rollingCorrelationRaw(seriesA, seriesB, window)
	if err != nil {
		return nil, err
	}
	return valuesToPoints(dates, values, window), nil
}

func rollingCorrelationRaw(seriesA, seriesB []float64, window int) ([]float64, error) {
	if len(seriesA) != len(seriesB) {
		return nil, errors.New("series length mismatch")
	}
	if len(seriesA) < window {
		return nil, fmt.Errorf("series length %d is smaller than rolling window %d", len(seriesA), window)
	}

	values := make([]float64, 0, len(seriesA)-window+1)
	for end := window; end <= len(seriesA); end++ {
		start := end - window
		values = append(values, correlation(seriesA[start:end], seriesB[start:end]))
	}
	return values, nil
}

func rollingBetaPoints(dates []time.Time, assetReturns, benchmarkReturns []float64, window int) ([]FactorPoint, []float64, error) {
	if len(assetReturns) != len(benchmarkReturns) {
		return nil, nil, errors.New("series length mismatch")
	}
	if len(assetReturns) < window {
		return nil, nil, fmt.Errorf("series length %d is smaller than rolling window %d", len(assetReturns), window)
	}

	values := make([]float64, 0, len(assetReturns)-window+1)
	for end := window; end <= len(assetReturns); end++ {
		start := end - window
		cov := covariance(assetReturns[start:end], benchmarkReturns[start:end])
		varBenchmark := variance(benchmarkReturns[start:end])
		if varBenchmark == 0 {
			values = append(values, 0)
			continue
		}
		values = append(values, cov/varBenchmark)
	}

	return valuesToPoints(dates, values, window), values, nil
}

func valuesToPoints(dates []time.Time, values []float64, window int) []FactorPoint {
	points := make([]FactorPoint, 0, len(values))
	for i, value := range values {
		dateIndex := i + window - 1
		points = append(points, FactorPoint{
			Time:  dates[dateIndex],
			Value: value,
		})
	}
	return points
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

	var covarianceValue float64
	var varianceA float64
	var varianceB float64
	for i := range a {
		deltaA := a[i] - meanA
		deltaB := b[i] - meanB
		covarianceValue += deltaA * deltaB
		varianceA += deltaA * deltaA
		varianceB += deltaB * deltaB
	}

	if varianceA == 0 || varianceB == 0 {
		return 0
	}

	value := covarianceValue / math.Sqrt(varianceA*varianceB)
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0
	}
	return value
}

func covariance(a, b []float64) float64 {
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

	var value float64
	for i := range a {
		value += (a[i] - meanA) * (b[i] - meanB)
	}
	return value / float64(len(a))
}

func variance(series []float64) float64 {
	if len(series) == 0 {
		return 0
	}

	var sum float64
	for _, value := range series {
		sum += value
	}
	mean := sum / float64(len(series))

	var acc float64
	for _, value := range series {
		delta := value - mean
		acc += delta * delta
	}
	return acc / float64(len(series))
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
