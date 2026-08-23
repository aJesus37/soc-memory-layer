// Command memmcp runs the SOC memory service as an MCP stdio server for
// LLM clients (Claude Desktop, MCP inspector, ...).
//
// # Trust boundary
//
// There is no authentication on this transport. Whoever can talk to this
// process's stdin/stdout IS the identity configured below — every read is
// scoped to it and every write is attributed to it:
//
//	MEM_MCP_ACTOR_TYPE   human|agent      (default human)
//	MEM_MCP_ACTOR_ID     free-form label  (default mcp-client)
//	MEM_MCP_SCOPE        scope key        (default default)
//
// Run one process per client/identity; per-user identity arrives with
// OIDC/remote transports in a future phase.
//
// # Protocol hygiene
//
// stdout carries ONLY the JSON-RPC protocol stream. Every log line,
// including mcp-go's own error logger, goes to stderr.
package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mark3labs/mcp-go/server"

	"socmem/internal/ch"
	"socmem/internal/config"
	"socmem/internal/embed"
	"socmem/internal/entity"
	"socmem/internal/graph"
	"socmem/internal/mcpserver"
	"socmem/internal/memory"
)

func main() {
	// stderr ONLY: stdout is the MCP protocol channel and any stray write
	// there corrupts an active session.
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	slog.SetDefault(logger)

	id, err := mcpserver.LoadIdentity()
	if err != nil {
		logger.Error("identity configuration invalid", "err", err)
		os.Exit(1)
	}

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

	embedder := embed.NewOpenAI(embed.Config{
		BaseURL: cfg.EmbedURL,
		Model:   cfg.EmbedModel,
		HTTP:    &http.Client{Timeout: 120 * time.Second},
	})

	resolver := entity.NewResolver(conn)
	svc := memory.New(conn, resolver, embedder, cfg)

	// Graph store optional exactly like memserved: unreachable Dgraph logs a
	// warn and traverse degrades to the ≤1-hop ClickHouse fallback.
	if g, err := graph.Connect(ctx, cfg.DgraphAddr); err != nil {
		logger.Warn("dgraph unavailable; memory_traverse degrades to clickhouse fallback",
			"addr", cfg.DgraphAddr, "err", err)
	} else {
		if err := g.InstallSchema(ctx); err != nil {
			logger.Warn("dgraph schema install failed; edge projection may fail", "err", err)
		}
		svc = svc.WithGraph(g)
		defer g.Close()
		logger.Info("graph store attached", "addr", cfg.DgraphAddr)
	}

	srv := mcpserver.New(mcpserver.Deps{
		Svc:      svc,
		Resolver: resolver,
		Identity: id,
	})

	stdio := server.NewStdioServer(srv)
	stdio.SetErrorLogger(log.New(os.Stderr, "memmcp: ", log.LstdFlags))

	// Serve until the client closes stdin or SIGINT/SIGTERM cancels ctx.
	// Listen returns ctx.Err() after cancellation and nil on clean EOF.
	errCh := make(chan error, 1)
	go func() {
		logger.Info("mcp server listening on stdio",
			"name", mcpserver.ServerName, "version", mcpserver.Version,
			"scope", id.Scope, "actor_type", id.ActorType, "actor_id", id.ActorID)
		errCh <- stdio.Listen(ctx, os.Stdin, os.Stdout)
	}()

	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("stdio server failed", "err", err)
			os.Exit(1)
		}
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	}
	logger.Info("stopped")
}
