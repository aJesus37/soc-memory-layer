// External test package: internal/memory imports graph (Traverse), so
// keeping these tests outside the package avoids a test-only import cycle.
package graph_test

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/dgraph-io/dgo/v250/protos/api"

	"socmem/internal/graph"
)

// itestCtx bounds each integration test's work so a wedged container fails
// fast instead of hanging the suite.
func itestCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func itestGraph(t *testing.T) *graph.Store {
	t.Helper()
	addr := os.Getenv("MEM_TEST_DGRAPH_ADDR")
	if addr == "" {
		t.Skip("MEM_TEST_DGRAPH_ADDR not set; skipping Dgraph integration test")
	}
	s, err := graph.Connect(itestCtx(t), addr)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// TestPingPreSchema pins the bootstrap contract: Ping must succeed on a
// cluster with no schema and no data yet (DropData leaves predicates empty;
// has(key) matches nothing but the read still exercises storage).
func TestPingPreSchema(t *testing.T) {
	s := itestGraph(t)
	ctx := itestCtx(t)
	if err := s.DropData(ctx); err != nil {
		t.Fatalf("drop data: %v", err)
	}
	if err := s.Ping(ctx); err != nil {
		t.Fatalf("ping on pre-schema cluster: %v", err)
	}
}

func TestConnectPings(t *testing.T) {
	s := itestGraph(t)
	if err := s.Ping(itestCtx(t)); err != nil {
		t.Fatal(err)
	}
	q := s.Dgraph().NewReadOnlyTxn()
	resp, err := q.Query(itestCtx(t), `{ me(func: has(key)) { uid } }`)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Json == nil {
		t.Fatal("query returned nil response body")
	}
}

func TestInstallSchemaIdempotent(t *testing.T) {
	ctx := itestCtx(t)
	s := itestGraph(t)
	if err := s.InstallSchema(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.InstallSchema(ctx); err != nil {
		t.Fatalf("second install: %v", err)
	}
	schema := readSchema(t, s)
	for _, want := range []string{"scope", "key", "ch_id", "entity_type", "display_name", "related_to"} {
		if _, ok := schema.Predicates[want]; !ok {
			t.Errorf("schema missing predicate %q; got %v", want, predicateNames(schema))
		}
	}
	if _, ok := schema.Types["Entity"]; !ok {
		t.Errorf("schema missing type Entity; got %v", typeNames(schema))
	}

	// Upsert directives are load-bearing: Task-5 projection upserts key every
	// write on ch_id and (later) scope/key lookups. A silent drift to a
	// non-upsert predicate makes conflicting mutations fail at runtime.
	for name, wantTok := range map[string]string{
		"ch_id": "exact",
		"scope": "hash",
		"key":   "hash",
	} {
		p, ok := schema.Predicates[name]
		if !ok {
			t.Errorf("schema missing predicate %q", name)
			continue
		}
		if len(p.Tokenizer) != 1 || p.Tokenizer[0] != wantTok {
			t.Errorf("predicate %q tokenizer = %v, want [%s]", name, p.Tokenizer, wantTok)
		}
		if !p.Upsert {
			t.Errorf("predicate %q lost @upsert; raw entry %+v", name, p)
		}
		if p.Type != "string" {
			t.Errorf("predicate %q type = %q, want string", name, p.Type)
		}
	}
}

func TestDropData(t *testing.T) {
	ctx := itestCtx(t)
	s := itestGraph(t)
	if err := s.InstallSchema(ctx); err != nil {
		t.Fatal(err)
	}

	const seedKey = "graph-dropdata-seed"
	_, err := s.Dgraph().NewTxn().Mutate(ctx, &api.Mutation{
		SetNquads: []byte("_:n <scope> \"test\" .\n_:n <key> \"" + seedKey + "\" .\n"),
		CommitNow: true,
	})
	if err != nil {
		t.Fatalf("seed mutation: %v", err)
	}
	if n := countKeyNodes(t, s); n < 1 {
		t.Fatalf("seed node not visible before drop: has(key) count = %d", n)
	}

	if err := s.DropData(ctx); err != nil {
		t.Fatalf("DropData: %v", err)
	}

	if n := countKeyNodes(t, s); n != 0 {
		t.Errorf("has(key) count after DropData = %d, want 0", n)
	}
	schema := readSchema(t, s)
	for _, want := range []string{"scope", "key", "ch_id", "entity_type", "display_name", "related_to"} {
		if _, ok := schema.Predicates[want]; !ok {
			t.Errorf("schema lost predicate %q after DropData; got %v", want, predicateNames(schema))
		}
	}
}

// schemaEntry is one row of the DQL `schema {}` response.
type schemaEntry struct {
	Predicate string   `json:"predicate"`
	Type      string   `json:"type"`
	Index     bool     `json:"index"`
	Tokenizer []string `json:"tokenizer"`
	Upsert    bool     `json:"upsert"`
	Reverse   bool     `json:"reverse"`
	List      bool     `json:"list"`
}

type schemaType struct {
	Name   string `json:"name"`
	Fields []struct {
		Name string `json:"name"`
	} `json:"fields"`
}

type dqlSchema struct {
	Predicates map[string]schemaEntry
	Types      map[string]schemaType
}

func predicateNames(s dqlSchema) []string {
	names := make([]string, 0, len(s.Predicates))
	for n := range s.Predicates {
		names = append(names, n)
	}
	return names
}

func typeNames(s dqlSchema) []string {
	names := make([]string, 0, len(s.Types))
	for n := range s.Types {
		names = append(names, n)
	}
	return names
}

func readSchema(t *testing.T, s *graph.Store) dqlSchema {
	t.Helper()
	resp, err := s.Dgraph().NewReadOnlyTxn().Query(itestCtx(t), "schema {}")
	if err != nil {
		t.Fatalf("schema query: %v", err)
	}
	var parsed struct {
		Schema []schemaEntry `json:"schema"`
		Types  []schemaType  `json:"types"`
	}
	if err := json.Unmarshal(resp.Json, &parsed); err != nil {
		t.Fatalf("decode schema response %s: %v", resp.Json, err)
	}
	out := dqlSchema{
		Predicates: make(map[string]schemaEntry, len(parsed.Schema)),
		Types:      make(map[string]schemaType, len(parsed.Types)),
	}
	for _, p := range parsed.Schema {
		out.Predicates[p.Predicate] = p
	}
	for _, ty := range parsed.Types {
		out.Types[ty.Name] = ty
	}
	return out
}

func countKeyNodes(t *testing.T, s *graph.Store) int {
	t.Helper()
	resp, err := s.Dgraph().NewReadOnlyTxn().Query(itestCtx(t), `{ q(func: has(key)) { count(uid) } }`)
	if err != nil {
		t.Fatalf("count query: %v", err)
	}
	var parsed struct {
		Q []struct {
			Count int `json:"count"`
		} `json:"q"`
	}
	if err := json.Unmarshal(resp.Json, &parsed); err != nil {
		t.Fatalf("decode count response %s: %v", resp.Json, err)
	}
	if len(parsed.Q) != 1 {
		t.Fatalf("count response = %s, want single aggregate row", resp.Json)
	}
	return parsed.Q[0].Count
}
