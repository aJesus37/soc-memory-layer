package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/dgraph-io/dgo/v250/protos/api"

	"socmem/internal/ch"
	"socmem/internal/config"
	"socmem/internal/embed"
	"socmem/internal/entity"
	"socmem/internal/graph"
	"socmem/internal/memory"
)

// itestCtx bounds the test's work so a wedged container fails fast.
func itestCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// itestBoth wires both stores fresh: empty mem.entities/mem.edges, epoch
// watermarks, and a dropped Dgraph graph. The suite runs package binaries
// serially (make itest -p 1) and this setup wipes its inputs first, so
// assertions on global counts are exact and stable across runs.
func itestBoth(t *testing.T) (*graph.Store, driver.Conn) {
	t.Helper()
	chAddr, dgAddr := os.Getenv("MEM_TEST_CH_ADDR"), os.Getenv("MEM_TEST_DGRAPH_ADDR")
	if chAddr == "" || dgAddr == "" {
		t.Skip("MEM_TEST_CH_ADDR / MEM_TEST_DGRAPH_ADDR not set; skipping rebuild integration test")
	}
	cfg := config.Load()
	conn, err := ch.Connect(itestCtx(t), chAddr, cfg.ChUser, cfg.ChPassword, "default")
	if err != nil {
		t.Fatalf("ch connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	st, err := graph.Connect(itestCtx(t), dgAddr)
	if err != nil {
		t.Fatalf("dgraph connect: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	ctx := itestCtx(t)
	if err := st.InstallSchema(ctx); err != nil {
		t.Fatal(err)
	}
	if err := st.DropData(ctx); err != nil {
		t.Fatal(err)
	}
	if err := ch.Migrate(ctx, conn, cfg); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	for _, table := range []string{"mem.entities", "mem.edges"} {
		if err := conn.Exec(ctx,
			"ALTER TABLE "+table+" DELETE WHERE 1 SETTINGS mutations_sync = 1"); err != nil {
			t.Fatalf("wipe %s: %v", table, err)
		}
	}
	// Dogfood the command's own reset for suite setup; it is asserted
	// directly in the test body as well.
	if err := resetWatermarks(ctx, conn); err != nil {
		t.Fatalf("reset watermarks: %v", err)
	}
	return st, conn
}

// seedEntity resolves one raw value into mem.entities via the real writer.
// The sleep keeps each row's DateTime64(3) updated_at strictly greater than
// the previous one so pagination order is deterministic.
func seedEntity(t *testing.T, ctx context.Context, conn driver.Conn, scope, raw string) entity.Entity {
	t.Helper()
	time.Sleep(5 * time.Millisecond)
	e, created, err := entity.NewResolver(conn).Resolve(ctx, scope, raw)
	if err != nil {
		t.Fatalf("resolve %q: %v", raw, err)
	}
	if !created {
		t.Fatalf("resolve %q: expected create, got refresh of existing entity", raw)
	}
	return e
}

// assertFactWithObject asserts an active human fact carrying an object
// endpoint through the REAL writer, minting the mem.edges row.
func assertFactWithObject(t *testing.T, svc *memory.Service, scope, subjectID, predicate, objectValue, objectID string) memory.Fact {
	t.Helper()
	f, err := svc.AssertFact(context.Background(), memory.FactInput{
		Scope:       scope,
		SubjectID:   subjectID,
		Predicate:   predicate,
		ObjectValue: objectValue,
		ObjectID:    objectID,
		Confidence:  0.9,
		ActorType:   "human",
		ActorID:     "analyst-rebuild-test",
	})
	if err != nil {
		t.Fatalf("assert fact %s/%s: %v", predicate, objectValue, err)
	}
	if f.Status != memory.Active {
		t.Fatalf("fact %s status = %s, want active", f.ID, f.Status)
	}
	return f
}

// fetchUIDs returns projected nodes' uids keyed by ch_id; ids without a node
// are absent from the map.
func fetchUIDs(t *testing.T, s *graph.Store, ctx context.Context, chIDs ...string) map[string]string {
	t.Helper()
	literals := make([]string, len(chIDs))
	for i, id := range chIDs {
		literals[i] = `"` + id + `"`
	}
	q := fmt.Sprintf(`{ q(func: eq(ch_id, [%s])) { uid ch_id } }`, strings.Join(literals, ", "))
	resp, err := s.Dgraph().NewReadOnlyTxn().Query(ctx, q)
	if err != nil {
		t.Fatalf("fetch uids %v: %v", chIDs, err)
	}
	var parsed struct {
		Q []struct {
			Uid  string `json:"uid"`
			ChID string `json:"ch_id"`
		} `json:"q"`
	}
	if err := json.Unmarshal(resp.Json, &parsed); err != nil {
		t.Fatalf("decode uid response %s: %v", resp.Json, err)
	}
	out := make(map[string]string, len(parsed.Q))
	for _, n := range parsed.Q {
		out[n.ChID] = n.Uid
	}
	return out
}

// relatedEdgeView is one outgoing related_to target with its facets.
type relatedEdgeView struct {
	Uid           string `json:"uid"`
	ChID          string `json:"ch_id"`
	FacetRelation string `json:"related_to|relation"`
}

type outgoingView struct {
	Uid       string            `json:"uid"`
	RelatedTo []relatedEdgeView `json:"related_to"`
}

// fetchOutgoing returns one ch_id's node with its outgoing related_to edges;
// ok=false when the node does not exist.
func fetchOutgoing(t *testing.T, s *graph.Store, ctx context.Context, chID string) (outgoingView, bool) {
	t.Helper()
	q := fmt.Sprintf(
		`{ q(func: eq(ch_id, %q)) { uid related_to @facets(relation) { uid ch_id } } }`, chID)
	resp, err := s.Dgraph().NewReadOnlyTxn().Query(ctx, q)
	if err != nil {
		t.Fatalf("fetch outgoing of %s: %v", chID, err)
	}
	var parsed struct {
		Q []outgoingView `json:"q"`
	}
	if err := json.Unmarshal(resp.Json, &parsed); err != nil {
		t.Fatalf("decode outgoing response %s: %v", resp.Json, err)
	}
	if len(parsed.Q) == 0 {
		return outgoingView{}, false
	}
	return parsed.Q[0], true
}

// watermarkView is one named cursor row read straight from ClickHouse. A
// single logical row exists per name, so max() reads that row's values.
type watermarkView struct {
	Ts     time.Time
	LastID string
}

func readWatermark(t *testing.T, ctx context.Context, conn driver.Conn, name string) watermarkView {
	t.Helper()
	var w watermarkView
	if err := conn.QueryRow(ctx,
		"SELECT max(ts), max(last_id) FROM mem.projection_watermark WHERE name = ?", name).
		Scan(&w.Ts, &w.LastID); err != nil {
		t.Fatalf("read %s watermark: %v", name, err)
	}
	return w
}

// corruptGraph pollutes Dgraph by hand: a junk Entity node whose ch_id has
// no ClickHouse counterpart, plus a stale related_to edge between two real
// seeded entities that no live edge row backs.
func corruptGraph(t *testing.T, s *graph.Store, ctx context.Context, junkChID, srcUID, dstUID string) {
	t.Helper()
	nquads := fmt.Sprintf(
		"_:junk <dgraph.type> \"Entity\" .\n"+
			"_:junk <ch_id> %q .\n"+
			"_:junk <scope> \"rebuild-test\" .\n"+
			"_:junk <key> \"junk-node\" .\n"+
			"_:junk <entity_type> \"ip\" .\n"+
			"<%s> <related_to> <%s> (relation=\"stale\",valid_from=\"2020-01-01T00:00:00Z\",valid_to=\"2105-12-31T23:59:59Z\") .\n",
		junkChID, srcUID, dstUID)
	if _, err := s.Dgraph().NewTxn().Mutate(ctx, &api.Mutation{
		SetNquads: []byte(nquads),
		CommitNow: true,
	}); err != nil {
		t.Fatalf("corrupt dgraph: %v", err)
	}
}

// TestRebuildFromCorruptedGraph runs the full corruption scenario: seed via
// the real writers, project once, pollute Dgraph by hand, then verify the
// rebuild wipes junk, drops the stale link, keeps the live edge exactly
// once, and leaves both cursors re-advanced past epoch.
func TestRebuildFromCorruptedGraph(t *testing.T) {
	ctx := itestCtx(t)
	s, conn := itestBoth(t)
	scope := fmt.Sprintf("itest-rebuild-%x", time.Now().UnixNano())

	const junkChID = "00000000-0000-0000-0000-0000deadbeef"

	eSubj := seedEntity(t, ctx, conn, scope, "c2.beacon.example")
	eObj := seedEntity(t, ctx, conn, scope, "198.51.100.9")
	eThird := seedEntity(t, ctx, conn, scope, "203.0.113.77")
	seeded := []string{eSubj.EntityID, eObj.EntityID, eThird.EntityID}

	svc := memory.New(conn, entity.NewResolver(conn), embed.NewFake(8), config.Load())
	assertFactWithObject(t, svc, scope, eSubj.EntityID, "communicates_with", "beacon", eObj.EntityID)

	// Initial live projection through the library path.
	if n, err := graph.ProjectEntities(ctx, s, conn, 10); err != nil || n != 3 {
		t.Fatalf("ProjectEntities = (%d, %v), want (3, nil)", n, err)
	}
	if n, err := graph.ProjectEdges(ctx, s, conn, 10); err != nil || n.Processed != 1 {
		t.Fatalf("ProjectEdges = (%+v, %v), want processed 1", n, err)
	}

	uids := fetchUIDs(t, s, ctx, seeded...)
	if len(uids) != 3 {
		t.Fatalf("pre-corruption nodes = %d (%v), want 3", len(uids), uids)
	}
	for _, name := range []string{"entities", "edges"} {
		if w := readWatermark(t, ctx, conn, name); w.Ts.UnixMilli() <= 0 || w.LastID == "" {
			t.Fatalf("pre-corruption %s cursor not advanced past epoch: %+v", name, w)
		}
	}

	corruptGraph(t, s, ctx, junkChID, uids[eSubj.EntityID], uids[eThird.EntityID])
	if got := fetchUIDs(t, s, ctx, junkChID); len(got) != 1 {
		t.Fatalf("junk node not visible before rebuild")
	}

	// Direct check of step 4 in isolation: the reset zeroes BOTH cursors.
	if err := resetWatermarks(ctx, conn); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"entities", "edges"} {
		w := readWatermark(t, ctx, conn, name)
		if w.Ts.UnixMilli() != 0 || w.LastID != "" {
			t.Errorf("%s cursor after reset = %+v, want epoch with empty last_id", name, w)
		}
	}

	base := config.Load()
	cfg := base
	cfg.ChAddr = os.Getenv("MEM_TEST_CH_ADDR")
	cfg.DgraphAddr = os.Getenv("MEM_TEST_DGRAPH_ADDR")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	entities, edges, err := rebuild(ctx, logger, cfg, 2)
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if entities != 3 {
		t.Errorf("rebuild projected %d entities, want 3", entities)
	}
	if edges.Processed != 1 {
		t.Errorf("rebuild processed %d edge rows, want 1", edges.Processed)
	}
	if edges.DeletedOrphans != 0 || edges.Deferred != 0 {
		t.Errorf("rebuild edge stats = %+v, want no orphans or deferrals", edges)
	}

	// Junk gone: nothing in CH backs it, DropData erased it, replay cannot
	// resurrect it.
	if got := fetchUIDs(t, s, ctx, junkChID); len(got) != 0 {
		t.Errorf("junk node survived rebuild")
	}

	// Live edge present EXACTLY once with facets; stale link gone.
	out, ok := fetchOutgoing(t, s, ctx, eSubj.EntityID)
	if !ok {
		t.Fatal("subject node missing after rebuild")
	}
	if len(out.RelatedTo) != 1 {
		t.Fatalf("subject outgoing after rebuild = %+v, want exactly one live edge", out)
	}
	if out.RelatedTo[0].ChID != eObj.EntityID {
		t.Errorf("live edge target = %s, want %s", out.RelatedTo[0].ChID, eObj.EntityID)
	}
	if out.RelatedTo[0].FacetRelation != "communicates_with" {
		t.Errorf("live edge relation facet = %q, want communicates_with", out.RelatedTo[0].FacetRelation)
	}
	third, ok := fetchOutgoing(t, s, ctx, eThird.EntityID)
	if !ok {
		t.Error("third entity node missing after rebuild")
	} else if len(third.RelatedTo) != 0 {
		t.Errorf("third entity outgoing after rebuild = %+v, want none (stale link must be gone)", third.RelatedTo)
	}

	// Both cursors were reset then re-advanced past epoch by the replay —
	// proof the reset forced a full pass rather than a no-op.
	for _, name := range []string{"entities", "edges"} {
		w := readWatermark(t, ctx, conn, name)
		if w.Ts.UnixMilli() <= 0 || w.LastID == "" {
			t.Errorf("%s cursor after rebuild = %+v, want advanced past epoch", name, w)
		}
	}
}
