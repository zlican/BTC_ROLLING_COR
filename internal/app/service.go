package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
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

var skipFixedEightHourSymbols = map[string]struct{}{
	"ETHUSDT":  {},
	"SOLUSDT":  {},
	"BNBUSDT":  {},
	"XRPUSDT":  {},
	"DOGEUSDT": {},
}

type TimeframeConfig struct {
	Name            string
	BinanceInterval string
	BybitInterval   string
	OKXBar          string
	CandleDuration  time.Duration
	HistoryBars     int
}

var timeframeConfigs = map[string]TimeframeConfig{
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
	"3D": {
		Name:            "3D",
		BinanceInterval: "3d",
		BybitInterval:   "",
		OKXBar:          "3Dutc",
		CandleDuration:  3 * 24 * time.Hour,
		HistoryBars:     120,
	},
}

var supportedTimeframes = []string{"4H", "1D", "3D"}

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
	Timeframes   []string
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
	Symbol             string `json:"symbol"`
	QuoteVolume        string `json:"quoteVolume"`
	LastPrice          string `json:"lastPrice"`
	PriceChangePercent string `json:"priceChangePercent"`
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

type benchmarkFetchResult struct {
	Provider  string
	Timeframe string
	Data      benchmarkData
	Err       error
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
	fixedUniversePath string

	datasetMu          sync.RWMutex
	datasetCachedAt    time.Time
	datasetCached      *FactorDataset
	datasetRefreshing  bool
	datasetRefreshDone chan struct{}

	universeMu          sync.RWMutex
	universeCachedAt    time.Time
	universeUpdatedAt   time.Time
	universeCachedList  []AssetConfig
	universeRefreshing  bool
	universeRefreshDone chan struct{}
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
		MaxIdleConnsPerHost:   32,
		MaxConnsPerHost:       64,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: 15 * time.Second,
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
	cached := s.datasetCached
	cachedAt := s.datasetCachedAt
	refreshing := s.datasetRefreshing
	s.datasetMu.RUnlock()

	if cached != nil {
		if time.Since(cachedAt) < s.datasetTTL {
			return cached, nil
		}
		if !refreshing {
			s.triggerDatasetRefresh()
		}
		return cached, nil
	}

	s.datasetMu.Lock()
	if s.datasetCached != nil {
		cached = s.datasetCached
		cachedAt = s.datasetCachedAt
		refreshing = s.datasetRefreshing
		s.datasetMu.Unlock()

		if time.Since(cachedAt) < s.datasetTTL {
			return cached, nil
		}
		if !refreshing {
			s.triggerDatasetRefresh()
		}
		return cached, nil
	}
	if s.datasetRefreshing {
		s.datasetMu.Unlock()
		return s.waitForDataset(ctx)
	}
	s.startDatasetRefreshLocked()
	s.datasetMu.Unlock()

	refreshCtx, cancel := context.WithTimeout(context.Background(), refreshTimeout)
	defer cancel()

	dataset, err := s.refresh(refreshCtx)

	s.finishDatasetRefresh(dataset)
	if err != nil {
		return nil, err
	}

	return dataset, nil
}

func (s *FactorService) waitForDataset(ctx context.Context) (*FactorDataset, error) {
	s.datasetMu.RLock()
	cached := s.datasetCached
	refreshing := s.datasetRefreshing
	done := s.datasetRefreshDone
	s.datasetMu.RUnlock()

	if cached != nil {
		return cached, nil
	}
	if !refreshing || done == nil {
		return nil, errors.New("dataset refresh finished without cached data")
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-done:
	}

	s.datasetMu.RLock()
	defer s.datasetMu.RUnlock()
	if s.datasetCached != nil {
		return s.datasetCached, nil
	}
	return nil, errors.New("dataset refresh finished without cached data")
}

func (s *FactorService) triggerDatasetRefresh() {
	s.datasetMu.Lock()
	if s.datasetRefreshing {
		s.datasetMu.Unlock()
		return
	}
	s.startDatasetRefreshLocked()
	s.datasetMu.Unlock()

	go s.refreshDatasetAsync()
}

func (s *FactorService) refreshDatasetAsync() {
	startedAt := time.Now()
	log.Printf("dataset background refresh started")
	refreshCtx, cancel := context.WithTimeout(context.Background(), refreshTimeout)
	defer cancel()

	dataset, err := s.refresh(refreshCtx)

	s.finishDatasetRefresh(dataset)
	if err != nil {
		log.Printf("dataset background refresh failed after %s, keep stale cache: %v", time.Since(startedAt).Round(time.Second), err)
		return
	}

	log.Printf("dataset background refresh finished in %s", time.Since(startedAt).Round(time.Second))
}

func (s *FactorService) startDatasetRefreshLocked() {
	s.datasetRefreshing = true
	s.datasetRefreshDone = make(chan struct{})
}

func (s *FactorService) finishDatasetRefresh(dataset *FactorDataset) {
	s.datasetMu.Lock()
	done := s.datasetRefreshDone
	s.datasetRefreshing = false
	s.datasetRefreshDone = nil
	if dataset != nil {
		s.datasetCached = dataset
		s.datasetCachedAt = time.Now().UTC()
	}
	s.datasetMu.Unlock()

	if done != nil {
		close(done)
	}
}

func (s *FactorService) IsDatasetRefreshing() bool {
	s.datasetMu.RLock()
	defer s.datasetMu.RUnlock()
	return s.datasetRefreshing
}

func (s *FactorService) StartBackgroundRefresh(ctx context.Context) {
	log.Printf("background refresh loop started: interval=%s", s.datasetTTL)
	s.triggerDatasetRefresh()

	if s.datasetTTL <= 0 {
		return
	}

	go func() {
		ticker := time.NewTicker(s.datasetTTL)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				log.Printf("background refresh loop stopped")
				return
			case <-ticker.C:
				s.triggerDatasetRefresh()
			}
		}
	}()
}

func (s *FactorService) refresh(ctx context.Context) (*FactorDataset, error) {
	startedAt := time.Now()
	log.Printf("dataset refresh started")

	assets, universeUpdatedAt, err := s.getFixedUniverse(ctx)
	if err != nil {
		log.Printf("dataset refresh failed in %s during universe fetch: %v", time.Since(startedAt).Round(time.Second), err)
		return nil, err
	}
	if len(assets) == 0 {
		log.Printf("dataset refresh failed in %s: fixed universe empty", time.Since(startedAt).Round(time.Second))
		return nil, errors.New("fixed universe is empty")
	}

	benchmarkSets := s.fetchBenchmarkSets(ctx)
	if len(benchmarkSets) == 0 {
		log.Printf("dataset refresh failed in %s: benchmark fetch failed on all providers", time.Since(startedAt).Round(time.Second))
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
		UniverseMinQuoteVol: 0,
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
		log.Printf("dataset refresh failed in %s: no assets remained after factor calculation", time.Since(startedAt).Round(time.Second))
		return nil, errors.New("no assets remained after factor calculation and rolling-window filtering")
	}

	timeframeCounts := make(map[string]int, len(supportedTimeframes))
	for _, symbol := range dataset.Order {
		for _, timeframeName := range dataset.Assets[symbol].FrameOrder {
			timeframeCounts[timeframeName]++
		}
	}
	log.Printf("dataset refreshed: universe=%d assets=%d 4H=%d 1D=%d 3D=%d",
		len(assets),
		len(dataset.Order),
		timeframeCounts["4H"],
		timeframeCounts["1D"],
		timeframeCounts["3D"],
	)
	log.Printf("dataset refresh finished in %s", time.Since(startedAt).Round(time.Second))

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

func (s *FactorService) fetchBenchmarkSets(ctx context.Context) map[string]map[string]benchmarkData {
	results := make(chan benchmarkFetchResult, len(s.providers)*len(supportedTimeframes))
	sem := make(chan struct{}, 6)
	var wg sync.WaitGroup

	for _, provider := range s.providers {
		provider := provider
		for _, timeframeName := range supportedTimeframes {
			cfg := timeframeConfigs[timeframeName]
			wg.Add(1)
			go func() {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()

				startDate, endDate := timeframeRange(cfg)
				instID, series, err := provider.FetchBenchmarkHistory(ctx, cfg, startDate, endDate)
				if err != nil {
					results <- benchmarkFetchResult{
						Provider:  provider.Name(),
						Timeframe: cfg.Name,
						Err:       err,
					}
					return
				}

				results <- benchmarkFetchResult{
					Provider:  provider.Name(),
					Timeframe: cfg.Name,
					Data: benchmarkData{
						InstID: instID,
						Series: series,
					},
				}
			}()
		}
	}

	wg.Wait()
	close(results)

	benchmarkSets := make(map[string]map[string]benchmarkData, len(s.providers))
	for result := range results {
		if result.Err != nil {
			log.Printf("benchmark fetch failed: provider=%s timeframe=%s err=%v", result.Provider, result.Timeframe, result.Err)
			continue
		}
		frames := benchmarkSets[result.Provider]
		if frames == nil {
			frames = make(map[string]benchmarkData, len(supportedTimeframes))
			benchmarkSets[result.Provider] = frames
		}
		frames[result.Timeframe] = result.Data
	}

	return benchmarkSets
}

func (s *FactorService) buildAssetSeries(ctx context.Context, cfg AssetConfig, benchmarkSets map[string]map[string]benchmarkData) assetResult {
	timeframes := cfg.Timeframes
	if len(timeframes) == 0 {
		timeframes = supportedTimeframes
	}

	frames := make(map[string]*FactorFrame, len(timeframes))
	frameOrder := make([]string, 0, len(timeframes))

	for _, timeframeName := range timeframes {
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
			continue
		}

		returnDates, assetReturns, benchmarkReturns, err := computeReturns(priceDates, assetPrices, benchmarkPrices)
		if err != nil {
			lastErr = err
			if fallback == nil {
				fallback = placeholderFrame(cfgTimeframe.Name, priceDates, instID, pairLabel, benchmark.InstID, provider.Name(), statusInsufficientHistory)
			}
			continue
		}

		frame, err := buildFactorFrame(cfgTimeframe.Name, returnDates, assetReturns, benchmarkReturns, s.rollingWindow)
		if err != nil {
			lastErr = err
			if fallback == nil {
				fallback = placeholderFrame(cfgTimeframe.Name, returnDates, instID, pairLabel, benchmark.InstID, provider.Name(), placeholderSignalForFactorError(err))
			}
			continue
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
	for _, timeframeName := range []string{"1D", "4H", "3D"} {
		if frame, ok := frames[timeframeName]; ok {
			return frame
		}
	}
	for _, frame := range frames {
		return frame
	}
	return &FactorFrame{}
}

func (s *FactorService) getFixedUniverse(ctx context.Context) ([]AssetConfig, time.Time, error) {
	s.universeMu.RLock()
	cached := cloneAssetConfigs(s.universeCachedList)
	cachedAt := s.universeCachedAt
	updatedAt := s.universeUpdatedAt
	s.universeMu.RUnlock()

	if len(cached) > 0 && time.Since(cachedAt) < s.universeTTL {
		return cached, updatedAt, nil
	}

	s.universeMu.Lock()
	if len(s.universeCachedList) > 0 && time.Since(s.universeCachedAt) < s.universeTTL {
		cached = cloneAssetConfigs(s.universeCachedList)
		updatedAt = s.universeUpdatedAt
		s.universeMu.Unlock()
		return cached, updatedAt, nil
	}
	if s.universeRefreshing {
		s.universeMu.Unlock()
		return s.waitForUniverse(ctx)
	}
	s.startUniverseRefreshLocked()
	s.universeMu.Unlock()

	startedAt := time.Now()
	log.Printf("universe refresh started")

	symbols, err := loadFixedUniverseSymbols(s.fixedUniversePath)
	if err != nil {
		s.finishUniverseRefresh(nil, time.Time{})
		log.Printf("universe refresh failed in %s while loading symbols: %v", time.Since(startedAt).Round(time.Second), err)
		return nil, time.Time{}, err
	}

	assets, updatedAt, err := s.universeProvider.FetchFixedUniverse(ctx, symbols)
	if err != nil {
		s.finishUniverseRefresh(nil, time.Time{})
		log.Printf("universe refresh failed in %s while fetching fixed universe: %v", time.Since(startedAt).Round(time.Second), err)
		return nil, time.Time{}, err
	}

	s.finishUniverseRefresh(assets, updatedAt)
	log.Printf("universe refresh finished in %s: symbols=%d", time.Since(startedAt).Round(time.Second), len(assets))
	return cloneAssetConfigs(assets), updatedAt, nil
}

func (s *FactorService) waitForUniverse(ctx context.Context) ([]AssetConfig, time.Time, error) {
	s.universeMu.RLock()
	cached := cloneAssetConfigs(s.universeCachedList)
	updatedAt := s.universeUpdatedAt
	refreshing := s.universeRefreshing
	done := s.universeRefreshDone
	s.universeMu.RUnlock()

	if len(cached) > 0 {
		return cached, updatedAt, nil
	}
	if !refreshing || done == nil {
		return nil, time.Time{}, errors.New("universe refresh finished without cached data")
	}

	select {
	case <-ctx.Done():
		return nil, time.Time{}, ctx.Err()
	case <-done:
	}

	s.universeMu.RLock()
	defer s.universeMu.RUnlock()
	cached = cloneAssetConfigs(s.universeCachedList)
	if len(cached) > 0 {
		return cached, s.universeUpdatedAt, nil
	}
	return nil, time.Time{}, errors.New("universe refresh finished without cached data")
}

func (s *FactorService) startUniverseRefreshLocked() {
	s.universeRefreshing = true
	s.universeRefreshDone = make(chan struct{})
}

func (s *FactorService) finishUniverseRefresh(assets []AssetConfig, updatedAt time.Time) {
	s.universeMu.Lock()
	done := s.universeRefreshDone
	s.universeRefreshing = false
	s.universeRefreshDone = nil
	if assets == nil {
		s.universeMu.Unlock()
		if done != nil {
			close(done)
		}
		return
	}

	s.universeCachedList = cloneAssetConfigs(assets)
	s.universeCachedAt = time.Now().UTC()
	s.universeUpdatedAt = updatedAt
	s.universeMu.Unlock()

	if done != nil {
		close(done)
	}
}

func cloneAssetConfigs(items []AssetConfig) []AssetConfig {
	out := make([]AssetConfig, len(items))
	copy(out, items)
	return out
}

func mergeAssetConfigs(base, extra []AssetConfig) []AssetConfig {
	if len(extra) == 0 {
		return base
	}

	out := make([]AssetConfig, 0, len(base)+len(extra))
	indexBySymbol := make(map[string]int, len(base)+len(extra))

	for _, item := range base {
		cloned := item
		if len(item.Timeframes) > 0 {
			cloned.Timeframes = append([]string(nil), item.Timeframes...)
		}
		indexBySymbol[item.Symbol] = len(out)
		out = append(out, cloned)
	}

	for _, item := range extra {
		cloned := item
		if len(item.Timeframes) > 0 {
			cloned.Timeframes = append([]string(nil), item.Timeframes...)
		}

		if idx, exists := indexBySymbol[item.Symbol]; exists {
			existing := out[idx]
			if existing.QuoteVolume <= 0 && cloned.QuoteVolume > 0 {
				existing.QuoteVolume = cloned.QuoteVolume
			}
			if existing.LastPrice <= 0 && cloned.LastPrice > 0 {
				existing.LastPrice = cloned.LastPrice
			}
			if existing.EightHourPct == 0 && cloned.EightHourPct != 0 {
				existing.EightHourPct = cloned.EightHourPct
			}
			if len(existing.Timeframes) == 0 && len(cloned.Timeframes) > 0 {
				existing.Timeframes = cloned.Timeframes
			}
			out[idx] = existing
			continue
		}

		indexBySymbol[item.Symbol] = len(out)
		out = append(out, cloned)
	}

	return out
}

type fixedUniverseFile struct {
	Symbols []string `json:"symbols"`
}

func loadFixedUniverseSymbols(path string) ([]string, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("fixed universe path is empty")
	}

	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read fixed universe config %q: %w", path, err)
	}

	var wrapped fixedUniverseFile
	if err := json.Unmarshal(content, &wrapped); err == nil && len(wrapped.Symbols) > 0 {
		return normalizeUniverseSymbols(wrapped.Symbols)
	}

	var symbols []string
	if err := json.Unmarshal(content, &symbols); err != nil {
		return nil, fmt.Errorf("decode fixed universe config %q: %w", path, err)
	}

	return normalizeUniverseSymbols(symbols)
}

func normalizeUniverseSymbols(symbols []string) ([]string, error) {
	normalized := make([]string, 0, len(symbols))
	seen := make(map[string]struct{}, len(symbols))

	for _, symbol := range symbols {
		value := strings.ToUpper(strings.TrimSpace(symbol))
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		normalized = append(normalized, value)
	}

	if len(normalized) == 0 {
		return nil, errors.New("fixed universe config does not contain any valid symbols")
	}
	return normalized, nil
}

func timeframeRange(cfg TimeframeConfig) (time.Time, time.Time) {
	endDate := timeframeCompletedBoundary(cfg, time.Now().UTC())
	startDate := endDate.Add(-time.Duration(cfg.HistoryBars-1) * cfg.CandleDuration)
	return startDate, endDate
}

func timeframeCompletedBoundary(cfg TimeframeConfig, now time.Time) time.Time {
	now = now.UTC()

	switch cfg.Name {
	case "4H":
		current := time.Date(now.Year(), now.Month(), now.Day(), (now.Hour()/4)*4, 0, 0, 0, time.UTC)
		return current.Add(-4 * time.Hour)
	case "1D":
		startOfDay := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
		return startOfDay.Add(-24 * time.Hour)
	case "3D":
		startOfDay := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
		epochStart := time.Unix(0, 0).UTC()
		daysSinceEpoch := int(startOfDay.Sub(epochStart) / (24 * time.Hour))
		segmentStart := startOfDay.AddDate(0, 0, -(daysSinceEpoch % 3))
		return segmentStart.AddDate(0, 0, -3)
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
