// main.go
package main

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"sieve-cache/sieve"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	config := sieve.Config[string, []byte]{
		Capacity:       100 * 1024 * 1024,
		GhostSize:      50,
		AdmitThreshold: 0.3,
		Logger:         logger,
		MaxEntrySize:   10 * 1024 * 1024,
		TTL:            time.Hour,
	}

	cache, err := sieve.NewSieveCacheWithConfig(config)
	if err != nil {
		logger.Error("Failed to create cache", "error", err)
		os.Exit(1)
	}
	defer cache.Close()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
}
