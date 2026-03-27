package main

import (
	"embed"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"
)

//go:embed web/*
var webFS embed.FS

type Server struct {
	service *FactorService
}

type OverviewResponse struct {
	Benchmark           string         `json:"benchmark"`
	Timeframe           string         `json:"timeframe"`
	RollingWindow       int            `json:"rolling_window"`
	UpdatedAt           string         `json:"updated_at"`
	UniverseUpdatedAt   string         `json:"universe_updated_at"`
	UniverseMinQuoteVol float64        `json:"universe_min_quote_vol"`
	AssetCount          int            `json:"asset_count"`
	Items               []OverviewItem `json:"items"`
}

type OverviewItem struct {
	Symbol        string  `json:"symbol"`
	DisplayName   string  `json:"display_name"`
	PairLabel     string  `json:"pair_label"`
	BenchmarkInst string  `json:"benchmark_inst"`
	DataSource    string  `json:"data_source"`
	QuoteVolume   float64 `json:"quote_volume"`
	LatestTime    string  `json:"latest_time"`
	LatestValue   float64 `json:"latest_value"`
}

type DetailResponse struct {
	Benchmark     string        `json:"benchmark"`
	Timeframe     string        `json:"timeframe"`
	RollingWindow int           `json:"rolling_window"`
	UpdatedAt     string        `json:"updated_at"`
	Asset         AssetResponse `json:"asset"`
}

type AssetResponse struct {
	Symbol        string        `json:"symbol"`
	DisplayName   string        `json:"display_name"`
	InstID        string        `json:"inst_id"`
	PairLabel     string        `json:"pair_label"`
	BenchmarkInst string        `json:"benchmark_inst"`
	DataSource    string        `json:"data_source"`
	QuoteVolume   float64       `json:"quote_volume"`
	LatestTime    string        `json:"latest_time"`
	LatestValue   float64       `json:"latest_value"`
	Points        []PointOutput `json:"points"`
}

type PointOutput struct {
	Time  string  `json:"time"`
	Value float64 `json:"value"`
}

const timeLayout = time.RFC3339

func NewServer(service *FactorService) *Server {
	return &Server{service: service}
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/detail", s.handleDetailPage)
	mux.HandleFunc("/api/overview", s.handleOverview)
	mux.HandleFunc("/api/detail", s.handleDetail)
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.FS(webFS))))
	return mux
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	http.ServeFileFS(w, r, webFS, "web/index.html")
}

func (s *Server) handleDetailPage(w http.ResponseWriter, r *http.Request) {
	http.ServeFileFS(w, r, webFS, "web/detail.html")
}

func (s *Server) handleOverview(w http.ResponseWriter, r *http.Request) {
	dataset, err := s.service.GetDataset(r.Context())
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}

	items := make([]OverviewItem, 0, len(dataset.Order))
	for _, symbol := range dataset.Order {
		asset := dataset.Assets[symbol]
		items = append(items, OverviewItem{
			Symbol:        asset.Symbol,
			DisplayName:   asset.DisplayName,
			PairLabel:     asset.PairLabel,
			BenchmarkInst: asset.BenchmarkInst,
			DataSource:    asset.DataSource,
			QuoteVolume:   asset.QuoteVolume,
			LatestTime:    asset.LatestTime.Format(timeLayout),
			LatestValue:   asset.LatestValue,
		})
	}

	writeJSON(w, http.StatusOK, OverviewResponse{
		Benchmark:           dataset.Benchmark,
		Timeframe:           dataset.Timeframe,
		RollingWindow:       dataset.RollingWindow,
		UpdatedAt:           dataset.UpdatedAt.Format(timeLayout),
		UniverseUpdatedAt:   dataset.UniverseUpdatedAt.Format(timeLayout),
		UniverseMinQuoteVol: dataset.UniverseMinQuoteVol,
		AssetCount:          len(items),
		Items:               items,
	})
}

func (s *Server) handleDetail(w http.ResponseWriter, r *http.Request) {
	symbol := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("symbol")))
	if symbol == "" {
		writeError(w, http.StatusBadRequest, errors.New("symbol is required"))
		return
	}

	dataset, err := s.service.GetDataset(r.Context())
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}

	asset, ok := dataset.Assets[symbol]
	if !ok {
		writeError(w, http.StatusNotFound, errors.New("symbol not found"))
		return
	}

	points := make([]PointOutput, 0, len(asset.Points))
	for _, point := range asset.Points {
		points = append(points, PointOutput{
			Time:  point.Time.Format(timeLayout),
			Value: point.Value,
		})
	}

	writeJSON(w, http.StatusOK, DetailResponse{
		Benchmark:     dataset.Benchmark,
		Timeframe:     dataset.Timeframe,
		RollingWindow: dataset.RollingWindow,
		UpdatedAt:     dataset.UpdatedAt.Format(timeLayout),
		Asset: AssetResponse{
			Symbol:        asset.Symbol,
			DisplayName:   asset.DisplayName,
			InstID:        asset.InstID,
			PairLabel:     asset.PairLabel,
			BenchmarkInst: asset.BenchmarkInst,
			DataSource:    asset.DataSource,
			QuoteVolume:   asset.QuoteVolume,
			LatestTime:    asset.LatestTime.Format(timeLayout),
			LatestValue:   asset.LatestValue,
			Points:        points,
		},
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}
