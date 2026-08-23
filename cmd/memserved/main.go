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

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"socmem/internal/api"
	"socmem/internal/ch"
	"socmem/internal/config"
	"socmem/internal/embed"
	"socmem/internal/entity"
	"socmem/internal/extract"
	"socmem/internal/graph"
	"socmem/internal/memory"
)

// extractionBatch bounds observations proposed per extraction tick.
const extractionBatch = 32

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

	// Phase-2 graph store. Optional by design: when Dgraph is unreachable
	// the service starts anyway and Traverse degrades to the ≤1-hop
	// ClickHouse fallback (design §6 failure table) — only a warn marks the
	// degraded mode. Schema install is idempotent and keeps a fresh
	// deployment projection-ready without a separate bootstrap step.
	var gstore *graph.Store
	if g, err := graph.Connect(ctx, cfg.DgraphAddr); err != nil {
		logger.Warn("dgraph unavailable; traverse degrades to clickhouse fallback",
			"addr", cfg.DgraphAddr, "err", err)
	} else {
		if err := g.InstallSchema(ctx); err != nil {
			logger.Warn("dgraph schema install failed; edge projection may fail",
				"err", err)
		}
		svc = svc.WithGraph(g)
		defer g.Close()
		gstore = g
		logger.Info("graph store attached", "addr", cfg.DgraphAddr)
	}

	// Background workers. Both stop when ctx cancels (signal shutdown);
	// neither ever touches the request path.
	if gstore != nil {
		go runProjectionWorker(ctx, logger, gstore, conn,
			time.Duration(cfg.ProjectIntervalSeconds)*time.Second)
	} else {
		logger.Info("projection worker idle: dgraph not attached")
	}
	if cfg.ExtractEnabled {
		// Same LM Studio endpoint serves embeddings and chat completions in
		// the dev topology, so EmbedURL doubles as the chat base URL (the
		// env name is historical; no separate chat URL exists to configure).
		chat := extract.NewChat(extract.Config{
			BaseURL: cfg.EmbedURL,
			Model:   cfg.ExtractModel,
		})
		go runExtractionWorker(ctx, logger, svc, chat, cfg.ExtractModel,
			time.Duration(cfg.ExtractIntervalSeconds)*time.Second)
	} else {
		logger.Info("extraction worker disabled")
	}

	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           api.New(svc, conn, cfg).Routes(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       60 * time.Second,
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

	shCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := srv.Shutdown(shCtx); err != nil {
		logger.Error("graceful shutdown failed", "err", err)
	}
	logger.Info("stopped")
}

// runProjectionWorker ships ClickHouse changes into Dgraph every interval
// until ctx cancels: entities first (nodes must exist before edges reference
// them), then edges — each with its own error log carrying counts, so one
// failing leg never hides the other's progress. batch 0 selects the
// projector default.
func runProjectionWorker(ctx context.Context, logger *slog.Logger, g *graph.Store, conn driver.Conn, interval time.Duration) {
	defer func() {
		if r := recover(); r != nil {
			logger.Error("projection worker panicked", "panic", r)
		}
	}()
	if interval <= 0 {
		logger.Error("projection worker refuses non-positive interval", "interval", interval)
		return
	}
	logger.Info("projection worker started", "interval", interval.String())
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			projectTick(ctx, logger, g, conn)
		}
	}
}

// projectTick runs one entities+edges pass. Logging is content-free: counts
// and errors only. Ticks aborted by shutdown log nothing — cancellation is
// not an operational problem.
func projectTick(ctx context.Context, logger *slog.Logger, g *graph.Store, conn driver.Conn) {
	defer func() {
		if r := recover(); r != nil {
			logger.Error("projection tick panicked; recovered until next tick", "panic", r)
		}
	}()

	nEnt, err := graph.ProjectEntities(ctx, g, conn, 0)
	switch {
	case err != nil && ctx.Err() != nil: // shutdown raced the tick
		return
	case err != nil:
		logger.Error("entity projection failed; will retry next tick",
			"projected_before_err", nEnt, "err", err)
	case nEnt > 0:
		logger.Info("entities projected", "count", nEnt)
	}

	nEdge, err := graph.ProjectEdges(ctx, g, conn, 0)
	switch {
	case err != nil && ctx.Err() != nil: // shutdown raced the tick
	case err != nil:
		logger.Error("edge projection failed; will retry next tick",
			"processed_before_err", nEdge, "err", err)
	case nEdge > 0:
		logger.Info("edges projected", "count", nEdge)
	}
}

// runExtractionWorker turns uncovered observations into proposed facts via
// the chat model (design §10). The service loop already recovers panics per
// tick and validates its own arguments; this outer recover covers everything
// around the ticks so no panic can kill the goroutine silently.
func runExtractionWorker(ctx context.Context, logger *slog.Logger, svc *memory.Service, chat extract.ChatClient, model string, interval time.Duration) {
	defer func() {
		if r := recover(); r != nil {
			logger.Error("extraction worker panicked", "panic", r)
		}
	}()
	logger.Info("extraction worker started",
		"interval", interval.String(), "batch", extractionBatch, "model", model)
	svc.RunExtractionLoop(ctx, chat, interval, extractionBatch)
}
