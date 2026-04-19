package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
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
		datasetTTL:        5 * time.Minute,
		universeTTL:       5 * time.Minute,
		rollingWindow:     30,
		fixedUniversePath: "symbols.json",
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	service.StartBackgroundRefresh(ctx)

	server := NewServer(service)
	log.Printf("listening on http://127.0.0.1:%s", port)

	httpServer := &http.Server{
		Addr:              ":" + port,
		Handler:           server.routes(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		log.Printf("shutdown signal received")
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			log.Printf("server shutdown error: %v", err)
		}
	}()

	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
