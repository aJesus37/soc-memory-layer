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
	"socmem/internal/ch"
	"socmem/internal/config"
	"socmem/internal/embed"
	"socmem/internal/entity"
	"socmem/internal/memory"
)

var apiScopeSeq atomic.Int64

// buildService wires a memory.Service against the integration DB with a fake
// embedder, running migrations. Skips when MEM_TEST_CH_ADDR is unset.
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
	return memory.New(conn, res, embed.NewFake(8), config.Load()), res, conn
}

// testServer builds an API server on top of buildService.
func testServer(t *testing.T) (*Server, http.Handler, *memory.Service, *entity.Resolver) {
	t.Helper()
	svc, res, conn := buildService(t)
	srv := New(svc, conn, config.Load())
	return srv, srv.Routes(), svc, res
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
}
