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

	_ "socmem/docs"

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

// @title						SOC Memory Layer API
// @version					1.0
// @description				Shared memory layer for security operations: episodic observations, versioned facts, hybrid recall, graph traversal.
// @contact.name				SOC Platform Team
// @BasePath					/
//
// @securityDefinitions.apikey	IdentityHeaders
// @in							header
// @name						X-Actor-Type
// @description				Plus X-Actor-ID and X-Scope headers; identity never travels in bodies.
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
		APIKey:  cfg.EmbedAPIKey,
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
		// ExtractBaseURL defaults to EmbedURL so a single LM Studio endpoint
		// serves both roles in dev. In production, point them at different
		// providers/models (e.g. EmbedURL -> OpenAI embeddings model,
		// ExtractBaseURL -> hosted chat model) and supply API keys.
		chat := extract.NewChat(extract.Config{
			BaseURL: cfg.ExtractBaseURL,
			Model:   cfg.ExtractModel,
			APIKey:  cfg.ExtractAPIKey,
			HTTP:    &http.Client{Timeout: 120 * time.Second},
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

	stEdge, err := graph.ProjectEdges(ctx, g, conn, 0)
	switch {
	case err != nil && ctx.Err() != nil: // shutdown raced the tick
	case err != nil:
		logger.Error("edge projection failed; will retry next tick",
			"processed_before_err", stEdge.Processed, "err", err)
	case stEdge.Total() > 0:
		logger.Info("edges projected", "processed", stEdge.Processed,
			"orphans_deleted", stEdge.DeletedOrphans, "deferred", stEdge.Deferred)
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
