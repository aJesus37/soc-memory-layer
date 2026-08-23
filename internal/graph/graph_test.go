package graph

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/dgraph-io/dgo/v250/protos/api"
)

func itestGraph(t *testing.T) *Store {
	t.Helper()
	addr := os.Getenv("MEM_TEST_DGRAPH_ADDR")
	if addr == "" {
		t.Skip("MEM_TEST_DGRAPH_ADDR not set; skipping Dgraph integration test")
	}
	s, err := Connect(context.Background(), addr)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestConnectPings(t *testing.T) {
	s := itestGraph(t)
	if err := s.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
	q := s.Dgraph().NewReadOnlyTxn()
	resp, err := q.Query(context.Background(), `{ me(func: has(key)) { uid } }`)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Json == nil {
		t.Fatal("query returned nil response body")
	}
}

func TestInstallSchemaIdempotent(t *testing.T) {
	ctx := context.Background()
	s := itestGraph(t)
	if err := s.InstallSchema(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.InstallSchema(ctx); err != nil {
		t.Fatalf("second install: %v", err)
	}
	preds := schemaPredicates(t, s)
	for _, want := range []string{"scope", "key", "ch_id", "entity_type", "display_name", "related_to"} {
		if !preds[want] {
			t.Errorf("schema missing predicate %q; got %v", want, preds)
		}
	}
}

func TestDropData(t *testing.T) {
	ctx := context.Background()
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
	preds := schemaPredicates(t, s)
	for _, want := range []string{"scope", "key", "ch_id", "entity_type", "display_name", "related_to"} {
		if !preds[want] {
			t.Errorf("schema lost predicate %q after DropData; got %v", want, preds)
		}
	}
}

func schemaPredicates(t *testing.T, s *Store) map[string]bool {
	t.Helper()
	resp, err := s.Dgraph().NewReadOnlyTxn().Query(context.Background(), "schema {}")
	if err != nil {
		t.Fatalf("schema query: %v", err)
	}
	var parsed struct {
		Schema []struct {
			Predicate string `json:"predicate"`
		} `json:"schema"`
	}
	if err := json.Unmarshal(resp.Json, &parsed); err != nil {
		t.Fatalf("decode schema response %s: %v", resp.Json, err)
	}
	set := make(map[string]bool, len(parsed.Schema))
	for _, p := range parsed.Schema {
		set[p.Predicate] = true
	}
	return set
}

func countKeyNodes(t *testing.T, s *Store) int {
	t.Helper()
	resp, err := s.Dgraph().NewReadOnlyTxn().Query(context.Background(), `{ q(func: has(key)) { count(uid) } }`)
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
