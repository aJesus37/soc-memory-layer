package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/client/transport"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	"socmem/internal/ch"
	"socmem/internal/config"
	"socmem/internal/embed"
	"socmem/internal/entity"
	"socmem/internal/graph"
	"socmem/internal/memory"
)

// --- harness -----------------------------------------------------------------

var httpScopeSeq atomic.Int64

// buildService wires a memory.Service against the integration DB with a fake
// embedder, running migrations. Skips when MEM_TEST_CH_ADDR is unset; mirrors
// the api-package helper of the same name.
func buildService(t *testing.T) (*memory.Service, *entity.Resolver, driver.Conn) {
	t.Helper()
	addr := os.Getenv("MEM_TEST_CH_ADDR")
	if addr == "" {
		t.Skip("set MEM_TEST_CH_ADDR to run")
	}
	ctx := context.Background()
	conn, err := ch.Connect(ctx, addr,
		envOr("MEM_CH_USER", "mem"), envOr("MEM_CH_PASSWORD", "memdev"), "default")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	if err := ch.Migrate(ctx, conn, config.Load()); err != nil {
		t.Fatal(err)
	}
	res := entity.NewResolver(conn)
	svc := memory.New(conn, res, embed.NewFake(8), config.Load())
	if dgAddr := os.Getenv("MEM_TEST_DGRAPH_ADDR"); dgAddr != "" {
		g, err := graph.Connect(ctx, dgAddr)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = g.Close() })
		if err := g.InstallSchema(ctx); err != nil {
			t.Fatal(err)
		}
		svc = svc.WithGraph(g)
	}
	return svc, res, conn
}

// envOr is defined in server.go for the production default path.

// startRemoteMCP mounts a real StreamableHTTPServer behind TokenAuth
// middleware on an httptest server, exactly as runHTTP wires production.
type remoteMCP struct {
	srv    *httptest.Server
	auth   *TokenAuth
	conn   driver.Conn
	scopeA string // token A's scope
	scopeB string // token B's scope
	tokenA string
	tokenB string
}

func startRemoteMCP(t *testing.T) remoteMCP {
	t.Helper()
	svc, res, conn := buildService(t)

	tokenA, tokenB := GenerateToken(), GenerateToken()
	records, err := LoadTokenFile(writeTokens(t, fmt.Sprintf(`[
		{"token": %q, "actor_type": "human", "actor_id": "analyst-a", "scope": "team-a-%d"},
		{"token": %q, "actor_type": "agent", "actor_id": "triage-bot-b", "scope": "team-b-%d"}
	]`, tokenA, time.Now().UnixNano(), tokenB, time.Now().UnixNano())))
	if err != nil {
		t.Fatal(err)
	}

	mcpSrv := New(Deps{Svc: svc, Resolver: res}) // zero Identity: multi-user HTTP wording
	auth := NewTokenAuth(records)
	streamable := server.NewStreamableHTTPServer(mcpSrv,
		server.WithHTTPContextFunc(auth.HTTPContextInjector()))
	mux := http.NewServeMux()
	mux.Handle("/mcp", auth.Middleware(streamable))
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	return remoteMCP{
		srv: ts, auth: auth, conn: conn,
		scopeA: records[0].Scope, scopeB: records[1].Scope,
		tokenA: tokenA, tokenB: tokenB,
	}
}

// dial opens an authenticated MCP client against the remote endpoint and
// completes the initialize handshake. Empty token means NO Authorization
// header at all.
func (rm remoteMCP) dial(t *testing.T, token string) *client.Client {
	t.Helper()
	opts := []transport.StreamableHTTPCOption(nil)
	if token != "" {
		opts = append(opts, transport.WithHTTPHeaders(
			map[string]string{"Authorization": "Bearer " + token}))
	}
	cli, err := client.NewStreamableHttpClient(rm.srv.URL+"/mcp", opts...)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	t.Cleanup(func() { _ = cli.Close() })
	if err := cli.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	initReq := mcp.InitializeRequest{}
	initReq.Params.ProtocolVersion = mcp.LATEST_PROTOCOL_VERSION
	initReq.Params.ClientInfo = mcp.Implementation{Name: "streamable-http-test", Version: "0"}
	if _, err := cli.Initialize(ctx, initReq); err != nil {
		t.Fatalf("initialize: %v", err)
	}
	return cli
}

func callTool(t *testing.T, cli *client.Client, name string, args map[string]any) (string, bool) {
	t.Helper()
	req := mcp.CallToolRequest{}
	req.Params.Name = name
	req.Params.Arguments = args
	res, err := cli.CallTool(context.Background(), req)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	if len(res.Content) == 0 {
		t.Fatalf("%s: no content", name)
	}
	tc, ok := res.Content[0].(mcp.TextContent)
	if !ok {
		t.Fatalf("%s: content[0] is not text: %#v", name, res.Content[0])
	}
	return tc.Text, res.IsError
}

// storedAttribution reads back who owns an observation straight from
// ClickHouse — returned structs are projections; the row is the truth.
func storedAttribution(t *testing.T, conn driver.Conn, obsID string) (scope, actorType, actorID string) {
	t.Helper()
	id, err := uuid.Parse(obsID)
	if err != nil {
		t.Fatalf("bad obs id %q: %v", obsID, err)
	}
	if err := conn.QueryRow(context.Background(),
		"SELECT scope, actor_type, actor_id FROM mem.observations WHERE obs_id = ?",
		id).Scan(&scope, &actorType, &actorID); err != nil {
		t.Fatal(err)
	}
	return scope, actorType, actorID
}

// --- tests -------------------------------------------------------------------

// Without a credential the middleware answers 401 + WWW-Authenticate: Bearer
// at the HTTP layer: nothing — including the initialize handshake — reaches
// MCP handling. The mcp-go client therefore fails during Initialize with the
// raw HTTP status surfaced by its transport (behavior asserted below and
// reported verbatim in the failure message if it drifts).
func TestStreamableHTTPRejectsAnonymousAndGarbage(t *testing.T) {
	rm := startRemoteMCP(t)

	for _, tc := range []struct {
		name   string
		header string // full Authorization value; empty omits it
	}{
		{name: "no authorization header"},
		{name: "wrong bearer token", header: "Bearer " + GenerateToken()},
		{name: "garbage scheme", header: "Basic YW5hbHlzdDpwYXNz"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var resp *http.Response
			var err error
			if tc.header == "" {
				resp, err = http.Post(rm.srv.URL+"/mcp", "application/json",
					strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`))
			} else {
				req, _ := http.NewRequest("POST", rm.srv.URL+"/mcp",
					strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`))
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Authorization", tc.header)
				resp, err = http.DefaultClient.Do(req)
			}
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", resp.StatusCode)
			}
			if wa := resp.Header.Get("WWW-Authenticate"); !strings.Contains(wa, "Bearer") {
				t.Fatalf("WWW-Authenticate = %q, want a Bearer challenge", wa)
			}
		})
	}

	// Same story through the real mcp-go client: the handshake itself is
	// refused, so there is no JSON-RPC-level error to inspect — the HTTP
	// status arrives first. dialExpectFailure asserts the client surfaces a
	// 401-flavored error from Initialize and reports the exact message.
	rm.dialExpectFailure(t, "no authorization header", "")
	rm.dialExpectFailure(t, "wrong bearer token", "Bearer "+GenerateToken())
}

// dialExpectFailure drives Start+Initialize with the given credential and
// requires the failure to be an HTTP-401 surfacing from the transport (not a
// JSON-RPC result — auth happens before MCP handling).
func (rm remoteMCP) dialExpectFailure(t *testing.T, name, token string) {
	t.Helper()
	opts := []transport.StreamableHTTPCOption(nil)
	if token != "" {
		opts = append(opts, transport.WithHTTPHeaders(
			map[string]string{"Authorization": token}))
	}
	cli, err := client.NewStreamableHttpClient(rm.srv.URL+"/mcp", opts...)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	defer cli.Close()
	if err := cli.Start(ctx); err != nil {
		t.Fatalf("%s: start: %v", name, err)
	}
	initReq := mcp.InitializeRequest{}
	initReq.Params.ProtocolVersion = mcp.LATEST_PROTOCOL_VERSION
	initReq.Params.ClientInfo = mcp.Implementation{Name: "anon-probe", Version: "0"}
	_, err = cli.Initialize(ctx, initReq)
	if err == nil {
		t.Fatalf("%s: anonymous initialize succeeded; auth gate leaked", name)
	}
	// mcp-go's streamable transport translates the HTTP 401 into its
	// ErrAuthorizationRequired sentinel ("transport error: authorization
	// required") before any JSON-RPC parsing — auth happens at the HTTP
	// layer, exactly as designed.
	if !errors.Is(err, transport.ErrAuthorizationRequired) {
		t.Fatalf("%s: initialize error %v is not the 401 sentinel", name, err)
	}
}

// Token A (human, scope A): record observation + enrich round-trip over the
// wire, then verify ClickHouse attributes the ROW to token A's identity.
func TestStreamableHTTPTokenARoundTripAttribution(t *testing.T) {
	rm := startRemoteMCP(t)
	cli := rm.dial(t, rm.tokenA)

	domain := fmt.Sprintf("remotea%x.example.net", time.Now().UnixNano())
	recText, isErr := callTool(t, cli, "memory_record_observation",
		map[string]any{"content": domain + " beaconed to 203.0.113.9"})
	if isErr {
		t.Fatalf("record_observation failed: %s", recText)
	}
	var rec struct {
		ID        string `json:"id"`
		Scope     string `json:"scope"`
		ActorType string `json:"actor_type"`
		ActorID   string `json:"actor_id"`
	}
	if err := json.Unmarshal([]byte(recText), &rec); err != nil {
		t.Fatalf("record_observation result not JSON: %s", recText)
	}
	if rec.Scope != rm.scopeA || rec.ActorType != "human" || rec.ActorID != "analyst-a" {
		t.Fatalf("write result attribution = %+v, want analyst-a under %s", rec, rm.scopeA)
	}

	enrText, isErr := callTool(t, cli, "memory_enrich",
		map[string]any{"type": "ioc_domain", "key": domain})
	if isErr {
		t.Fatalf("memory_enrich failed: %s", enrText)
	}
	if !strings.HasPrefix(enrText, fenceOpen+"\n") || !strings.Contains(enrText, `"found":true`) ||
		!strings.Contains(enrText, "beaconed to 203.0.113.9") {
		t.Fatalf("enrich round-trip incomplete: %.200s", enrText)
	}

	scope, actorType, actorID := storedAttribution(t, rm.conn, rec.ID)
	if scope != rm.scopeA || actorType != "human" || actorID != "analyst-a" {
		t.Fatalf("DB attribution = (%s,%s,%s), want (%s,human,analyst-a)",
			scope, actorType, actorID, rm.scopeA)
	}
}

// THE point of Phase-4: two different tokens produce two different
// attributions in shared storage. Token A is human/analyst-a/scope-A, token
// B is agent/triage-bot-b/scope-B; their writes land in disjoint scopes with
// distinct actors, and the agent's facts arrive 'proposed' while the human's
// are 'active' — per-identity trust policy surviving the remote hop.
func TestStreamableHTTPTwoTokensTwoAttributions(t *testing.T) {
	rm := startRemoteMCP(t)
	cliA := rm.dial(t, rm.tokenA)
	cliB := rm.dial(t, rm.tokenB)

	recordVia := func(cli *client.Client, domain string) string {
		t.Helper()
		text, isErr := callTool(t, cli, "memory_record_observation",
			map[string]any{"content": domain + " seen in joint operation"})
		if isErr {
			t.Fatalf("record_observation failed: %s", text)
		}
		var rec struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal([]byte(text), &rec); err != nil {
			t.Fatalf("not JSON: %s", text)
		}
		return rec.ID
	}

	domainA := fmt.Sprintf("jointop%x.example.net", time.Now().UnixNano())
	domainB := fmt.Sprintf("jointop%x.example.net", time.Now().UnixNano()+1)
	obsA := recordVia(cliA, domainA)
	obsB := recordVia(cliB, domainB)

	scopeA, typeA, actorA := storedAttribution(t, rm.conn, obsA)
	scopeB, typeB, actorB := storedAttribution(t, rm.conn, obsB)
	if actorA == actorB || scopeA == scopeB {
		t.Fatalf("attributions collided: A=(%s,%s) B=(%s,%s)", scopeA, actorA, scopeB, actorB)
	}
	if scopeA != rm.scopeA || actorA != "analyst-a" || typeA != "human" {
		t.Fatalf("token A attributed (%s,%s,%s)", scopeA, typeA, actorA)
	}
	if scopeB != rm.scopeB || actorB != "triage-bot-b" || typeB != "agent" {
		t.Fatalf("token B attributed (%s,%s,%s)", scopeB, typeB, actorB)
	}

	// Trust policy rides identity too: human asserts ACTIVE, agent PROPOSED.
	factA, isErr := callTool(t, cliA, "memory_assert_fact", map[string]any{
		"subject_key": domainA, "predicate": "beaconed_to", "object_value": "203.0.113.9",
	})
	if isErr || !strings.Contains(factA, `"status":"active"`) {
		t.Fatalf("human-asserted fact should be active: %s (isErr=%v)", factA, isErr)
	}
	factB, isErr := callTool(t, cliB, "memory_assert_fact", map[string]any{
		"subject_key": domainB, "predicate": "beaconed_to", "object_value": "203.0.113.10",
	})
	if isErr || !strings.Contains(factB, `"status":"proposed"`) {
		t.Fatalf("agent-asserted fact should be proposed: %s (isErr=%v)", factB, isErr)
	}

	// Shared-knowledge contract (flipped from the Phase-1 isolation rule):
	// token B CAN recall team A's org-wide knowledge about domainA — facts
	// and internal observations are org-visible by default. Attribution
	// always travels with each row; only restricted material stays home,
	// and nothing written in this scenario is restricted.
	enrB, isErr := callTool(t, cliB, "memory_enrich", map[string]any{
		"type": "ioc_domain", "key": domainA,
	})
	if isErr {
		t.Fatalf("enrich failed: %s", enrB)
	}
	if !strings.Contains(enrB, `"found":true`) || !strings.Contains(enrB, domainA) {
		t.Fatalf("org-wide sharing broken: token B missed team A's knowledge: %.200s", enrB)
	}
}
