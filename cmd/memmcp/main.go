// Command memmcp runs the SOC memory service as an MCP server for LLM
// clients (Claude Desktop, MCP inspector, remote analysts, ...) over one of
// two transports:
//
// # stdio (default)
//
// One process per client; whoever can talk to this process's stdin/stdout IS
// the identity configured below — every read is scoped to it and every write
// is attributed to it:
//
//	MEM_MCP_ACTOR_TYPE   human|agent      (default human)
//	MEM_MCP_ACTOR_ID     free-form label  (default mcp-client)
//	MEM_MCP_SCOPE        scope key        (default default)
//
// # Streamable HTTP (set MEM_MCP_HTTP_ADDR, e.g. ":8443")
//
// Many analysts on different machines share one memory through a single
// authenticated endpoint at /mcp. Every request must present a bearer token
// from MEM_MCP_TOKENS_FILE (see internal/mcpserver/auth.go for the format);
// each token maps to its own actor/scope identity, so writes carry the real
// analyst's attribution instead of a shared robot account:
//
//	MEM_MCP_HTTP_ADDR     listen address; presence switches to HTTP mode
//	MEM_MCP_TOKENS_FILE   REQUIRED in HTTP mode; refuses to start without
//	                      a strictly-valid, non-empty tokens file
//
// # Trust boundary — READ BEFORE EXPOSING THIS SERVER
//
// This listener serves PLAINTEXT HTTP: TLS is deliberately NOT terminated
// here. A REVERSE PROXY MUST terminate TLS in front of memmcp — bearer
// tokens over plaintext HTTP are unacceptable outside loopback/trusted
// networks, since anyone on the path captures working credentials. Tokens
// are static long-lived API-key-style credentials (rotation = edit file +
// restart); keep the file mode tight (it is a credential store) and never
// log or echo token values.
//
// # Protocol hygiene
//
// stdout carries ONLY the JSON-RPC protocol stream (stdio mode). Every log
// line, including mcp-go's own error logger, goes to stderr.
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

	httpAddr := os.Getenv("MEM_MCP_HTTP_ADDR")
	tokensFile := os.Getenv("MEM_MCP_TOKENS_FILE")

	// stdio's fixed env identity. HTTP mode ignores it (identity rides the
	// bearer token per request) and therefore must not fail startup over it.
	var fixedID *mcpserver.Identity
	if httpAddr == "" {
		if tokensFile != "" {
			// Only a warn: harmless misconfiguration, but operators should
			// know their tokens file is not protecting anything here.
			logger.Warn("MEM_MCP_TOKENS_FILE set but unused: stdio mode has no HTTP surface to authenticate")
		}
		id, err := mcpserver.LoadIdentity()
		if err != nil {
			logger.Error("identity configuration invalid", "err", err)
			os.Exit(1)
		}
		fixedID = &id
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

	deps := mcpserver.Deps{Svc: svc, Resolver: resolver}
	if fixedID != nil {
		// Single-user stdio deployment: instructions name the fixed scope/
		// actor. HTTP mode leaves it zero for per-caller wording.
		deps.Identity = *fixedID
	}
	srv := mcpserver.New(deps)

	if httpAddr != "" {
		runHTTP(ctx, logger, srv, httpAddr, tokensFile)
		return
	}

	stdio := server.NewStdioServer(srv)
	stdio.SetErrorLogger(log.New(os.Stderr, "memmcp: ", log.LstdFlags))
	// Handlers take identity from the request context (see mcpserver
	// package doc); stdio's single env-loaded identity is injected here.
	// The context func runs once per connection — correct, because this
	// transport has exactly one identity by construction.
	stdio.SetContextFunc(func(ctx context.Context) context.Context {
		return mcpserver.WithIdentity(ctx, *fixedID)
	})

	// Serve until the client closes stdin or SIGINT/SIGTERM cancels ctx.
	// Listen returns ctx.Err() after cancellation and nil on clean EOF.
	errCh := make(chan error, 1)
	go func() {
		logger.Info("mcp server listening on stdio",
			"name", mcpserver.ServerName, "version", mcpserver.Version,
			"scope", fixedID.Scope, "actor_type", fixedID.ActorType, "actor_id", fixedID.ActorID)
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

// runHTTP serves the MCP endpoint over Streamable HTTP at addr until
// SIGINT/SIGTERM, with bearer-token auth in front of every request.
//
// Auth is layered twice on purpose: TokenAuth.Middleware rejects
// unauthenticated requests with 401 + WWW-Authenticate: Bearer BEFORE any
// MCP handling (an HTTPContextFunc cannot abort a request — it only returns
// a context), and the WithHTTPContextFunc injector re-resolves the identity
// into the exact context mcp-go hands the tool handlers. Both layers read
// the same immutable token mapping, so they cannot disagree.
func runHTTP(ctx context.Context, logger *slog.Logger, srv *server.MCPServer, addr, tokensFile string) {
	records, err := mcpserver.LoadTokenFile(tokensFile) // fail-closed: empty/missing/unparseable/duplicate all abort here
	if err != nil {
		logger.Error("remote mcp requires a valid MEM_MCP_TOKENS_FILE", "err", err)
		os.Exit(1)
	}
	auth := mcpserver.NewTokenAuth(records)

	// WithDisableLocalhostProtection consciously turns off mcp-go's
	// DNS-rebinding guard, which would otherwise 403 any request whose
	// Host header is not a localhost value — silently breaking the
	// DOCUMENTED topology (bind loopback + terminate TLS at a reverse
	// proxy), where the proxy forwards the original public Host header.
	// It is safe to disable here because it is SUBSUMED by the auth gate:
	// every request passes TokenAuth.Middleware (bearer-token check)
	// before reaching the MCP handler, so a rebinding attacker without a
	// token gets 401 regardless of what Host they present. Do not reuse
	// this option on an unauthenticated MCP endpoint.
	streamable := server.NewStreamableHTTPServer(srv,
		server.WithDisableLocalhostProtection(true),
		server.WithHTTPContextFunc(auth.HTTPContextInjector()))

	mux := http.NewServeMux()
	mux.Handle("/mcp", auth.Middleware(streamable))

	httpSrv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		// Log the listener shape and HOW MANY identities loaded — never the
		// token values themselves; the file is a credential store.
		logger.Info("mcp server listening on http",
			"name", mcpserver.ServerName, "version", mcpserver.Version,
			"addr", addr, "endpoint", "/mcp", "identities", len(records),
			"tls", "terminated upstream at reverse proxy")
		errCh <- httpSrv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server failed", "err", err)
			os.Exit(1)
		}
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	}

	shCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shCtx); err != nil {
		logger.Error("graceful shutdown failed", "err", err)
	}
	if err := streamable.Shutdown(shCtx); err != nil {
		logger.Warn("mcp session teardown failed", "err", err)
	}
	logger.Info("stopped")
}
