package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	_ "socmem/docs" // registers the swagger spec for the /swagger/doc.json route

	"socmem/internal/ch"
	"socmem/internal/config"
	"socmem/internal/embed"
	"socmem/internal/entity"
	"socmem/internal/graph"
	"socmem/internal/memory"
)

var apiScopeSeq atomic.Int64

// buildService wires a memory.Service against the integration DB with a fake
// embedder, running migrations. Skips when MEM_TEST_CH_ADDR is unset. When
// MEM_TEST_DGRAPH_ADDR is also set, a graph store is attached so Traverse
// runs in graph mode; otherwise tests exercise the CH fallback.
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

// testServer builds an API server on top of buildService.
func testServer(t *testing.T) (*Server, http.Handler, *memory.Service, *entity.Resolver) {
	t.Helper()
	svc, res, conn := buildService(t)
	srv := New(svc, conn, config.Load())
	return srv, srv.Routes(), svc, res
}

// apiEnv bundles everything a graph-mode API test needs: the server under
// test plus the raw service/connection/store handles for seeding and
// projection. g is nil when MEM_TEST_DGRAPH_ADDR is unset.
type apiEnv struct {
	srv  *Server
	h    http.Handler
	svc  *memory.Service
	res  *entity.Resolver
	conn driver.Conn
	g    *graph.Store
}

// graphStoreOf re-attaches a throwaway store handle for tests that must
// call the projector directly; nil when no Dgraph addr is configured.
func graphStoreOf(t *testing.T) *graph.Store {
	t.Helper()
	dgAddr := os.Getenv("MEM_TEST_DGRAPH_ADDR")
	if dgAddr == "" {
		return nil
	}
	g, err := graph.Connect(context.Background(), dgAddr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g
}

// testServerFull is testServer plus direct access to the underlying
// connection and an independent graph-store handle (nil without
// MEM_TEST_DGRAPH_ADDR).
func testServerFull(t *testing.T) apiEnv {
	t.Helper()
	svc, res, conn := buildService(t)
	srv := New(svc, conn, config.Load())
	return apiEnv{srv: srv, h: srv.Routes(), svc: svc, res: res, conn: conn, g: graphStoreOf(t)}
}

// newIdentity returns header set bound to a unique per-call scope so tests
// never collide with prior runs on the persistent dev volume.
func newIdentity(t *testing.T, actorType string) map[string]string {
	t.Helper()
	scope := fmt.Sprintf("api-%s-%d-%s", t.Name(), apiScopeSeq.Add(1),
		strings.ReplaceAll(uuid.NewString(), "-", ""))
	hdrs := map[string]string{
		"X-Actor-Type": actorType,
		"X-Actor-ID":   "actor-" + actorType,
		"X-Scope":      scope,
	}
	if actorType == "agent" {
		hdrs["X-On-Behalf-Of"] = "analyst-j"
	}
	return hdrs
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// do performs an authenticated request and decodes the JSON response into
// out (when non-nil). Returns the recorder for status/header checks.
func do(t *testing.T, h http.Handler, method, path string, body any, headers map[string]string, out any) *httptest.ResponseRecorder {
	t.Helper()
	buf := &bytes.Buffer{}
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		buf.Write(b)
	}
	req := httptest.NewRequest(method, path, buf)
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if out != nil && rec.Code < 300 {
		if err := json.NewDecoder(rec.Body).Decode(out); err != nil {
			t.Fatalf("decode %s %s: %v — body: %s", method, path, err, rec.Body.String())
		}
	}
	return rec
}

func TestIdentityMiddleware(t *testing.T) {
	_, h, _, _ := testServer(t)

	cases := []struct {
		name    string
		headers map[string]string
		want    int
	}{
		{"missing actor type", map[string]string{"X-Actor-ID": "a", "X-Scope": "s"}, 400},
		{"bad actor type", map[string]string{"X-Actor-Type": "robot", "X-Actor-ID": "a", "X-Scope": "s"}, 400},
		{"missing actor id", map[string]string{"X-Actor-Type": "human", "X-Scope": "s"}, 400},
		{"missing scope", map[string]string{"X-Actor-Type": "human", "X-Actor-ID": "a"}, 400},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := do(t, h, "POST", "/v1/observations",
				map[string]string{"kind": "alert", "content": "x"},
				c.headers, nil)
			if rec.Code != c.want {
				t.Fatalf("got %d want %d: %s", rec.Code, c.want, rec.Body.String())
			}
		})
	}
}

func TestObservationRoundTrip(t *testing.T) {
	_, h, _, _ := testServer(t)
	hdrs := newIdentity(t, "human")

	var created struct {
		ID        string   `json:"id"`
		EntityIDs []string `json:"entity_ids"`
		Embedded  bool     `json:"embedded"`
		Scope     string   `json:"scope"`
	}
	rec := do(t, h, "POST", "/v1/observations", map[string]any{
		"kind":    "human_statement",
		"content": "observed 4.4.4.4 talking to roundtrip.example.com",
	}, hdrs, &created)
	if rec.Code != http.StatusOK {
		t.Fatalf("create failed %d: %s", rec.Code, rec.Body.String())
	}
	if !created.Embedded || len(created.EntityIDs) != 2 || created.Scope != hdrs["X-Scope"] {
		t.Fatalf("unexpected observation: %+v", created)
	}

	var er struct {
		Found        bool `json:"found"`
		Observations []struct {
			Excerpt string `json:"excerpt"`
		} `json:"observations"`
	}
	rec = do(t, h, "GET", "/v1/enrich?type=ioc_ip&key=4.4.4.4", nil, hdrs, &er)
	if rec.Code != http.StatusOK {
		t.Fatalf("enrich failed %d: %s", rec.Code, rec.Body.String())
	}
	if !er.Found || len(er.Observations) != 1 ||
		!strings.Contains(er.Observations[0].Excerpt, "4.4.4.4") {
		t.Fatalf("enrich wrong: %+v", er)
	}

	// enrich miss: unknown key → found=false, still 200
	var miss struct {
		Found bool `json:"found"`
	}
	rec = do(t, h, "GET", "/v1/enrich?type=ioc_ip&key=5.5.5.5", nil, hdrs, &miss)
	if rec.Code != http.StatusOK || miss.Found {
		t.Fatalf("unknown enrich got %d found=%v", rec.Code, miss.Found)
	}

	// Hyphenated hostnames link as ONE full-key ioc_domain — never split at
	// '-' into partial labels — and are reachable through enrich.
	var hyp struct {
		EntityIDs []string `json:"entity_ids"`
	}
	rec = do(t, h, "POST", "/v1/observations", map[string]any{
		"kind":    "human_statement",
		"content": "mcp-smoke-x.example.net resolved_to 9.9.9.9",
	}, hdrs, &hyp)
	if rec.Code != http.StatusOK {
		t.Fatalf("hyphen create failed %d: %s", rec.Code, rec.Body.String())
	}
	if len(hyp.EntityIDs) != 2 {
		t.Fatalf("hyphen observation linked %v, want exactly 2 entities (full domain + ip)", hyp.EntityIDs)
	}
	var ed struct {
		Found  bool `json:"found"`
		Entity struct {
			Type string `json:"type"`
			Key  string `json:"key"`
		} `json:"entity"`
	}
	rec = do(t, h, "GET", "/v1/enrich?type=ioc_domain&key=mcp-smoke-x.example.net", nil, hdrs, &ed)
	if rec.Code != http.StatusOK || !ed.Found ||
		ed.Entity.Type != "ioc_domain" || ed.Entity.Key != "mcp-smoke-x.example.net" {
		t.Fatalf("hyphen enrich got %d found=%v entity=%+v, want full hyphenated key",
			rec.Code, ed.Found, ed.Entity)
	}
}

// TestCrossScopeAttributionOverHTTP seeds an observation and an active
// org-visibility fact in team A's scope over HTTP, then reads them back
// through a SECOND identity's scope and asserts every response surface
// attributes to team-A: enrich's entity.scope plus per-fact /
// per-observation origin_scope, the org-wide entity timeline's event labels,
// and similar's per-hit scope.
func TestCrossScopeAttributionOverHTTP(t *testing.T) {
	_, h, _, res := testServer(t)
	teamA := newIdentity(t, "human")
	teamB := newIdentity(t, "human")
	scopeA := teamA["X-Scope"]

	key := "cross-origin.example.com"

	subj, _, err := res.Resolve(context.Background(), scopeA, key)
	if err != nil {
		t.Fatal(err)
	}

	var f struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}
	rec := do(t, h, "POST", "/v1/facts", map[string]any{
		"subject_id": subj.EntityID, "predicate": "attributed_to",
		"object_value": "actor-cross", "confidence": 0.8,
	}, teamA, &f)
	if rec.Code != http.StatusOK || f.Status != "active" {
		t.Fatalf("assert failed %d status=%s: %s", rec.Code, f.Status, rec.Body.String())
	}

	rec = do(t, h, "POST", "/v1/observations", map[string]any{
		"kind":    "human_statement",
		"content": key + " beaconed toward actor-controlled.example.net",
	}, teamA, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("obs create failed %d: %s", rec.Code, rec.Body.String())
	}

	var er struct {
		Found  bool `json:"found"`
		Entity struct {
			ID    string `json:"id"`
			Scope string `json:"scope"`
		} `json:"entity"`
		Facts []struct {
			ID          string `json:"id"`
			OriginScope string `json:"origin_scope"`
		} `json:"facts"`
		Observations []struct {
			ID          string `json:"id"`
			OriginScope string `json:"origin_scope"`
		} `json:"observations"`
	}
	rec = do(t, h, "GET", "/v1/enrich?type=ioc_domain&key="+key, nil, teamB, &er)
	if rec.Code != http.StatusOK {
		t.Fatalf("cross-scope enrich failed %d: %s", rec.Code, rec.Body.String())
	}
	if !er.Found || er.Entity.ID != subj.EntityID || er.Entity.Scope != scopeA {
		t.Fatalf("entity attribution wrong: found=%v entity=%+v, want scope %q",
			er.Found, er.Entity, scopeA)
	}
	if len(er.Facts) != 1 || er.Facts[0].ID != f.ID || er.Facts[0].OriginScope != scopeA {
		t.Fatalf("facts wrong: %+v, want origin_scope %q", er.Facts, scopeA)
	}
	if len(er.Observations) == 0 {
		t.Fatal("no observations on cross-scope enrich")
	}
	for _, o := range er.Observations {
		if o.OriginScope != scopeA {
			t.Fatalf("observation origin = %q, want %q", o.OriginScope, scopeA)
		}
	}

	// Org-wide by-entity timeline read from B: A's fact event arrives
	// labeled with A's origin scope.
	var events []struct {
		Source      string `json:"source"`
		Kind        string `json:"kind"`
		OriginScope string `json:"origin_scope"`
	}
	rec = do(t, h, "GET", "/v1/timeline?entity_id="+subj.EntityID, nil, teamB, &events)
	if rec.Code != http.StatusOK {
		t.Fatalf("timeline failed %d: %s", rec.Code, rec.Body.String())
	}
	foundFactEvent := false
	for _, e := range events {
		if e.Source == "fact" && e.Kind == "fact:attributed_to" {
			foundFactEvent = true
			if e.OriginScope != scopeA {
				t.Fatalf("fact event origin = %q, want %q", e.OriginScope, scopeA)
			}
		}
	}
	if !foundFactEvent {
		t.Fatalf("no attributed_to fact event in cross-scope timeline: %+v", events)
	}

	// Org-wide similar read from B: hits carry their originating scope.
	var hits []struct {
		Scope   string `json:"scope"`
		Excerpt string `json:"excerpt"`
	}
	rec = do(t, h, "GET", "/v1/similar?q=beaconed&k=5", nil, teamB, &hits)
	if rec.Code != http.StatusOK {
		t.Fatalf("similar failed %d: %s", rec.Code, rec.Body.String())
	}
	foundHit := false
	for _, hit := range hits {
		if strings.Contains(hit.Excerpt, key) {
			foundHit = true
			if hit.Scope != scopeA {
				t.Fatalf("hit scope = %q, want %q", hit.Scope, scopeA)
			}
		}
	}
	if !foundHit {
		t.Fatalf("similar missed seeded cross-scope observation: %+v", hits)
	}
}

func TestFactLifecycleOverHTTP(t *testing.T) {
	_, h, svc, res := testServer(t)
	human := newIdentity(t, "human")
	agent := newIdentity(t, "agent")

	subj, _, err := res.Resolve(context.Background(), human["X-Scope"], "lifecycle.example.com")
	if err != nil {
		t.Fatal(err)
	}
	_ = svc

	// agent asserts non-whitelisted predicate → proposed
	var f struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}
	rec := do(t, h, "POST", "/v1/facts", map[string]any{
		"subject_id": subj.EntityID, "predicate": "attributed_to",
		"object_value": "actor-x", "confidence": 0.9,
	}, agent, &f)
	if rec.Code != http.StatusOK {
		t.Fatalf("assert failed %d: %s", rec.Code, rec.Body.String())
	}
	if f.Status != "proposed" {
		t.Fatalf("agent fact should be proposed: %+v", f)
	}

	// agent promote → 403
	rec = do(t, h, "POST", "/v1/facts/"+f.ID+"/promote", nil, agent, nil)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("agent promote got %d want 403: %s", rec.Code, rec.Body.String())
	}

	// human promote → 200 active; response carries the CURRENT version id
	var tr struct {
		Fact struct {
			ID     string `json:"id"`
			Status string `json:"status"`
		} `json:"fact"`
	}
	rec = do(t, h, "POST", "/v1/facts/"+f.ID+"/promote", nil, human, &tr)
	if rec.Code != http.StatusOK || tr.Fact.Status != "active" {
		t.Fatalf("promote failed %d: %s %+v", rec.Code, rec.Body.String(), tr)
	}

	// promoting the ORIGINAL id again → 404: version-at-read means the old
	// fact_id was consumed by the transition (FINAL dedups before WHERE)
	rec = do(t, h, "POST", "/v1/facts/"+f.ID+"/promote", nil, human, nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("consumed id got %d want 404", rec.Code)
	}

	// promoting the CURRENT version again → 409 (it is active, not proposed)
	rec = do(t, h, "POST", "/v1/facts/"+tr.Fact.ID+"/promote", nil, human, nil)
	if rec.Code != http.StatusConflict {
		t.Fatalf("double promote got %d want 409", rec.Code)
	}

	// retract current version with reason → 200; again → 404 (id consumed)
	rec = do(t, h, "POST", "/v1/facts/"+tr.Fact.ID+"/retract",
		map[string]string{"reason": "false positive"}, human, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("retract failed %d: %s", rec.Code, rec.Body.String())
	}
	// same consumed id again → 404 (version-at-read; the retraction replaced it)
	rec = do(t, h, "POST", "/v1/facts/"+tr.Fact.ID+"/retract", nil, human, nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("consumed id retract got %d want 404", rec.Code)
	}

	// unknown fact → 404
	rec = do(t, h, "POST", "/v1/facts/00000000-0000-0000-0000-000000000000/promote", nil, human, nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown fact got %d want 404", rec.Code)
	}
}

func TestSimilarAndTimelineOverHTTP(t *testing.T) {
	_, h, _, _ := testServer(t)
	hdrs := newIdentity(t, "human")

	do(t, h, "POST", "/v1/observations", map[string]any{
		"kind":    "alert",
		"content": "beaconing detected from timeline-sim.example.com",
	}, hdrs, nil)

	var hits []struct {
		Excerpt   string   `json:"excerpt"`
		MatchedBy []string `json:"matched_by"`
	}
	rec := do(t, h, "GET", "/v1/similar?q=beaconing&k=5", nil, hdrs, &hits)
	if rec.Code != http.StatusOK {
		t.Fatalf("similar failed %d: %s", rec.Code, rec.Body.String())
	}
	foundTxt := false
	for _, hit := range hits {
		for _, m := range hit.MatchedBy {
			if m == "txt" && strings.Contains(hit.Excerpt, "beaconing") {
				foundTxt = true
			}
		}
	}
	if !foundTxt {
		t.Fatalf("txt leg missed seeded observation: %+v", hits)
	}

	rec = do(t, h, "GET", "/v1/similar?q=x&k=99", nil, hdrs, nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("k=99 got %d want 400", rec.Code)
	}

	// timeline by case over HTTP
	var caseCreated struct {
		ID string `json:"id"`
	}
	do(t, h, "POST", "/v1/observations", map[string]any{
		"kind":    "triage_decision",
		"case_id": caseIDFor(t),
		"content": "closed as duplicate of earlier alert",
	}, hdrs, &caseCreated)

	rec = do(t, h, "GET", "/v1/timeline?case_id="+caseIDFor(t), nil, hdrs, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("timeline failed %d: %s", rec.Code, rec.Body.String())
	}

	rec = do(t, h, "GET", "/v1/timeline?case_id=x&entity_id=y", nil, hdrs, nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("both ids got %d want 400", rec.Code)
	}
}

// caseIDFor returns a distinct valid UUID string per call.
func caseIDFor(t *testing.T) string {
	t.Helper()
	n := apiScopeSeq.Add(1)
	return fmt.Sprintf("00000000-0000-0000-0000-%012d", n)
}

// TestRetractBodyEdgeCases pins the retract body contract: chunked-style
// unknown-length bodies and premature EOF degrade to an empty reason, while
// trailing garbage after the JSON value is a loud 400.
func TestRetractBodyEdgeCases(t *testing.T) {
	svc, _, conn := buildService(t)
	h := New(svc, conn, config.Load()).Routes()

	newFact := func(t *testing.T) string {
		t.Helper()
		var f struct {
			ID string `json:"id"`
		}
		rec := do(t, h, "POST", "/v1/facts", map[string]any{
			"subject_id":   "00000000-0000-0000-0000-000000000002",
			"predicate":    "edge_case_probe",
			"object_value": uuid.NewString(),
		}, newIdentity(t, "human"), &f)
		if rec.Code != http.StatusOK {
			t.Fatalf("assert failed %d: %s", rec.Code, rec.Body.String())
		}
		return f.ID
	}

	t.Run("unknown length empty body", func(t *testing.T) {
		id := newFact(t)
		req := httptest.NewRequest("POST", "/v1/facts/"+id+"/retract", nil)
		req.ContentLength = -1 // chunked-style: length unknown
		for k, v := range newIdentity(t, "human") {
			req.Header.Set(k, v)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("chunked-style empty retract got %d want 200: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("premature EOF on declared length", func(t *testing.T) {
		id := newFact(t)
		req := httptest.NewRequest("POST", "/v1/facts/"+id+"/retract", strings.NewReader(""))
		req.ContentLength = 64 // declares bytes that never arrive
		for k, v := range newIdentity(t, "human") {
			req.Header.Set(k, v)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("premature-EOF retract got %d want 200: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("trailing garbage rejected", func(t *testing.T) {
		id := newFact(t)
		req := httptest.NewRequest("POST", "/v1/facts/"+id+"/retract",
			strings.NewReader(`{"reason":"x"}trailing`))
		for k, v := range newIdentity(t, "human") {
			req.Header.Set(k, v)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("trailing garbage retract got %d want 400: %s", rec.Code, rec.Body.String())
		}
	})
}

// TestOnBehalfOfHeaderOnly pins delegation identity to the X-On-Behalf-Of
// header only. A body-borne on_behalf_of is an unknown field now and must be
// rejected loudly (same treatment as body-borne scope), while the header
// value is what actually persists.
func TestOnBehalfOfHeaderOnly(t *testing.T) {
	svc, _, conn := buildService(t)
	h := New(svc, conn, config.Load()).Routes()

	agent := newIdentity(t, "agent") // carries X-On-Behalf-Of: analyst-j; override below
	agent["X-On-Behalf-Of"] = "analyst-y"

	// Spoof attempt: body claims on_behalf_of=X, header says Y → rejected,
	// nothing persisted.
	rec := do(t, h, "POST", "/v1/observations", map[string]any{
		"kind":         "alert",
		"content":      "spoof attempt from spoofed.example.com",
		"on_behalf_of": "analyst-x",
	}, agent, nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("body-borne on_behalf_of got %d want 400: %s", rec.Code, rec.Body.String())
	}

	// Header-only path: Y persists.
	var created struct {
		ID string `json:"id"`
	}
	rec = do(t, h, "POST", "/v1/observations", map[string]any{
		"kind":    "alert",
		"content": "delegated action touching header-only.example.com",
	}, agent, &created)
	if rec.Code != http.StatusOK {
		t.Fatalf("create failed %d: %s", rec.Code, rec.Body.String())
	}
	obsID, err := uuid.Parse(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	var stored string
	if err := conn.QueryRow(context.Background(),
		"SELECT on_behalf_of FROM mem.observations WHERE obs_id = ?", obsID,
	).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != "analyst-y" {
		t.Fatalf("on_behalf_of stored %q, want header value analyst-y", stored)
	}
}

func TestHealthzAndSpoofing(t *testing.T) {
	_, h, _, _ := testServer(t)

	req := httptest.NewRequest("GET", "/healthz", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("healthz %d: %s", rec.Code, rec.Body.String())
	}

	// body-borne scope must be rejected: identity comes only from headers
	rec = do(t, h, "POST", "/v1/observations", map[string]any{
		"kind": "alert", "content": "x", "scope": "spoofed-scope",
	}, newIdentity(t, "human"), nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("spoofed scope got %d want 400", rec.Code)
	}

	// actor echo header present
	rec = do(t, h, "GET", "/v1/enrich?type=ioc_ip&key=1.1.1.1", nil, newIdentity(t, "human"), nil)
	if got := rec.Header().Get("X-Mem-Actor"); got != "human:actor-human" {
		t.Fatalf("X-Mem-Actor echo wrong: %q", got)
	}

	// Swagger spec is served from the registry the generated docs package
	// populates at init — no service/DB interaction, but routed through the
	// same mux, so assert it as part of this integration flow.
	rec = do(t, h, "GET", "/swagger/doc.json", nil, nil, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("swagger doc.json got %d want 200", rec.Code)
	}
	var spec struct {
		Paths map[string]json.RawMessage `json:"paths"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &spec); err != nil {
		t.Fatalf("doc.json not parseable JSON: %v — body: %s", err, rec.Body.String())
	}
	if _, ok := spec.Paths["/v1/enrich"]; !ok {
		t.Fatalf("doc.json paths missing /v1/enrich")
	}
}

// TestTraverseOverHTTP seeds A→B→C through the REAL writers on a
// graph-mode server, projects both stores, and walks from A over HTTP.
// Skips cleanly when MEM_TEST_DGRAPH_ADDR is unset (fallback-mode server:
// the endpoint still answers, capped at one hop).
func TestTraverseOverHTTP(t *testing.T) {
	env := testServerFull(t)
	if env.g == nil {
		t.Skip("MEM_TEST_DGRAPH_ADDR not set; skipping graph-mode traverse API test")
	}
	hdrs := newIdentity(t, "human")
	ctx := context.Background()
	scope := hdrs["X-Scope"]

	resolve := func(raw string) entity.Entity {
		e, created, err := env.res.Resolve(ctx, scope, raw)
		if err != nil || !created {
			t.Fatalf("resolve %q: created=%v err=%v", raw, created, err)
		}
		return e
	}
	a := resolve("api-traverse-a.example.com")
	b := resolve("api-traverse-b.example.com")
	c := resolve("api-traverse-c.example.com")

	assertEdge := func(subject entity.Entity, predicate, objVal string, object entity.Entity) {
		f, err := env.svc.AssertFact(ctx, memory.FactInput{
			Scope: scope, SubjectID: subject.EntityID, Predicate: predicate,
			ObjectValue: objVal, ObjectID: object.EntityID,
			Confidence: 0.9, ActorType: "human", ActorID: "analyst-api",
		})
		if err != nil || f.Status != memory.Active {
			t.Fatalf("assert %s: status=%s err=%v", predicate, f.Status, err)
		}
	}
	assertEdge(a, "communicates_with", "beacon", b)
	assertEdge(b, "resolved_to", "infra", c)

	// Project until drained so the walk sees the full chain.
	for i := 0; i < 100; i++ {
		nEnt, err := graph.ProjectEntities(ctx, env.g, env.conn, 500)
		if err != nil {
			t.Fatal(err)
		}
		nEdge, err := graph.ProjectEdges(ctx, env.g, env.conn, 500)
		if err != nil {
			t.Fatal(err)
		}
		if nEnt == 0 && nEdge == 0 {
			break
		}
	}

	type apiPath struct {
		Nodes []struct {
			ID          string `json:"id"`
			Type        string `json:"type"`
			Key         string `json:"key"`
			DisplayName string `json:"display_name"`
		} `json:"nodes"`
		Relations []string `json:"relations"`
	}
	var resp struct {
		Paths []apiPath `json:"paths"`
	}
	rec := do(t, env.h, "GET",
		"/v1/traverse?key=api-traverse-a.example.com&type=ioc_domain&hops=2",
		nil, hdrs, &resp)
	if rec.Code != http.StatusOK {
		t.Fatalf("traverse failed %d: %s", rec.Code, rec.Body.String())
	}
	found := false
	for _, p := range resp.Paths {
		if len(p.Nodes) == 3 && len(p.Relations) == 2 &&
			p.Nodes[0].ID == a.EntityID && p.Nodes[1].ID == b.EntityID &&
			p.Nodes[2].ID == c.EntityID &&
			p.Relations[0] == "communicates_with" && p.Relations[1] == "resolved_to" {
			found = true
			for _, n := range p.Nodes {
				if n.DisplayName == "" || n.Type != string(entity.IocDomain) {
					t.Fatalf("unhydrated node over HTTP: %+v", p.Nodes)
				}
			}
		}
	}
	if !found {
		t.Fatalf("no A->B->C path in %d HTTP paths: %+v", len(resp.Paths), resp.Paths)
	}

	// hops default = 1: every returned path is single-hop.
	resp.Paths = nil
	rec = do(t, env.h, "GET",
		"/v1/traverse?key=api-traverse-a.example.com&type=ioc_domain", nil, hdrs, &resp)
	if rec.Code != http.StatusOK {
		t.Fatalf("default-hops traverse failed %d: %s", rec.Code, rec.Body.String())
	}
	if len(resp.Paths) == 0 {
		t.Fatal("default hops returned no paths")
	}
	for _, p := range resp.Paths {
		if len(p.Relations) != 1 {
			t.Fatalf("default hops returned multi-hop path: %+v", p)
		}
	}

	// Out-of-range hops surfaces as 400 via ErrInvalidInput mapping.
	rec = do(t, env.h, "GET",
		"/v1/traverse?key=api-traverse-a.example.com&type=ioc_domain&hops=4",
		nil, hdrs, nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("hops=4 got %d want 400: %s", rec.Code, rec.Body.String())
	}

	// Malformed relation charset → 400.
	rec = do(t, env.h, "GET",
		"/v1/traverse?key=api-traverse-a.example.com&type=ioc_domain&relation=BAD",
		nil, hdrs, nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("bad relation got %d want 400: %s", rec.Code, rec.Body.String())
	}

	// Missing key/type → 400 before reaching the service.
	rec = do(t, env.h, "GET", "/v1/traverse?type=ioc_domain", nil, hdrs, nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("missing key got %d want 400", rec.Code)
	}
}
