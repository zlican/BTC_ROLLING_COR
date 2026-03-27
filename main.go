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

	service := &FactorService{
		client:          client,
		ttl:             5 * time.Minute,
		lookbackDays:    89,
		rollingWindow:   30,
		benchmarkInstID: "BTC-USD",
		assets: []AssetConfig{
			{Symbol: "ETH", InstID: "ETH-USD", PairLabel: "ETH-BTC"},
			{Symbol: "SOL", InstID: "SOL-USD", PairLabel: "SOL-BTC"},
			{Symbol: "BNB", InstID: "BNB-USD", PairLabel: "BNB-BTC"},
			{Symbol: "XRP", InstID: "XRP-USD", PairLabel: "XRP-BTC"},
			{Symbol: "DOGE", InstID: "DOGE-USD", PairLabel: "DOGE-BTC"},
		},
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	server := NewServer(service)
	log.Printf("listening on http://127.0.0.1:%s", port)
	log.Fatal(http.ListenAndServe(":"+port, server.routes()))
}
