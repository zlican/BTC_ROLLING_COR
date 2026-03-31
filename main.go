package main

import (
	"log"
	"net/http"
	"os"
	"time"
)

func main() {
	client, err := newHTTPClient()
	if err != nil {
		log.Fatalf("create HTTP client: %v", err)
	}

	binanceProvider := &BinanceProvider{client: client}
	bybitProvider := &BybitProvider{client: client}
	okxProvider := &OKXProvider{client: client}

	service := &FactorService{
		universeProvider:  binanceProvider,
		providers:         []MarketDataProvider{binanceProvider, bybitProvider, okxProvider},
		datasetTTL:        time.Hour,
		universeTTL:       time.Hour,
		rollingWindow:     30,
		minUniverseVolume: 100000000,
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	server := NewServer(service)
	log.Printf("listening on http://127.0.0.1:%s", port)
	log.Fatal(http.ListenAndServe(":"+port, server.routes()))
}
