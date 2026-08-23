// Command memserved runs the SOC memory service HTTP API.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"socmem/internal/api"
	"socmem/internal/ch"
	"socmem/internal/config"
	"socmem/internal/embed"
	"socmem/internal/entity"
	"socmem/internal/memory"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg := config.Load()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	conn, err := ch.Connect(ctx, cfg.ChAddr, cfg.ChUser, cfg.ChPassword, "default")
	if err != nil {
		logger.Error("clickhouse connect failed", "addr", cfg.ChAddr, "err", err)
		os.Exit(1)
	}
	defer conn.Close()
	if err := conn.Ping(ctx); err != nil {
		logger.Error("clickhouse ping failed", "err", err)
		os.Exit(1)
	}
	if err := ch.Migrate(ctx, conn, cfg); err != nil {
		logger.Error("migrations failed", "err", err)
		os.Exit(1)
	}
	logger.Info("storage ready", "addr", cfg.ChAddr)

	// Generous client timeout: local embedding servers lazily load models on
	// first request, which routinely exceeds 30s on CPU-only setups.
	embedder := embed.NewOpenAI(embed.Config{
		BaseURL: cfg.EmbedURL,
		Model:   cfg.EmbedModel,
		HTTP:    &http.Client{Timeout: 120 * time.Second},
	})

	resolver := entity.NewResolver(conn)
	svc := memory.New(conn, resolver, embedder, cfg)

	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           api.New(svc, conn, cfg).Routes(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("listening", "addr", cfg.ListenAddr)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server failed", "err", err)
			os.Exit(1)
		}
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	}

	shCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Shutdown(shCtx); err != nil {
		logger.Error("graceful shutdown failed", "err", err)
	}
	logger.Info("stopped")
}
