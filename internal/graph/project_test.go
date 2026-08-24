// External test package: these tests import internal/memory (real-writer
// seeding), and memory imports graph for Traverse — an in-package test
// would form a test-only import cycle.
package graph_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"socmem/internal/ch"
	"socmem/internal/config"
	"socmem/internal/embed"
	"socmem/internal/entity"
	"socmem/internal/graph"
	"socmem/internal/memory"
)

// wmEntities / wmEdges duplicate the unexported projection watermark names
// (project.go watermarkEntities / watermarkEdges); keep in sync.
const (
	wmEntities = "entities"
	wmEdges    = "edges"
)

// itestBoth wires the full projection stack against live services: fresh
// Dgraph data, empty mem.entities and mem.edges, and epoch watermarks.
// Tests seeded after this get exact global counts because the suite runs
// package binaries serially (make itest -p 1) and nothing else writes these
// tables.
func itestBoth(t *testing.T) (*graph.Store, driver.Conn) {
	t.Helper()
	if os.Getenv("MEM_TEST_CH_ADDR") == "" {
		t.Skip("MEM_TEST_CH_ADDR / MEM_TEST_DGRAPH_ADDR not set; skipping projection integration test")
	}
	s := itestGraph(t)
	cfg := config.Load()
	conn, err := ch.Connect(itestCtx(t), os.Getenv("MEM_TEST_CH_ADDR"),
		cfg.ChUser, cfg.ChPassword, "default")
	if err != nil {
		t.Fatalf("ch connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	ctx := itestCtx(t)
	if err := s.InstallSchema(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.DropData(ctx); err != nil {
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
	for _, name := range []string{wmEntities, wmEdges} {
		if err := conn.Exec(ctx,
			"ALTER TABLE mem.projection_watermark "+
				"UPDATE ts = toDateTime64(0, 3), last_id = '' "+
				"WHERE name = ? SETTINGS mutations_sync = 1", name); err != nil {
			t.Fatalf("reset %s watermark: %v", name, err)
		}
	}
	return s, conn
}

// edgeService builds the real memory write path (facts + edges) over the
// test connection so projection exercises actual writer behavior, never
// raw inserts.
func edgeService(t *testing.T, conn driver.Conn) *memory.Service {
	t.Helper()
	return memory.New(conn, entity.NewResolver(conn), embed.NewFake(8), config.Load())
}

// seedEntity resolves one raw value into mem.entities. The sleep keeps each
// row's DateTime64(3) updated_at strictly greater than the previous one so
// watermark boundaries are deterministic.
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

// nodeView mirrors a projected Entity node as returned by DQL.
type nodeView struct {
	Uid         string    `json:"uid"`
	ChID        string    `json:"ch_id"`
	Scope       string    `json:"scope"`
	Key         string    `json:"key"`
	EntityType  string    `json:"entity_type"`
	DisplayName string    `json:"display_name"`
	DgraphType  []string  `json:"dgraph.type"`
	FirstSeen   time.Time `json:"first_seen"`
	LastSeen    time.Time `json:"last_seen"`
}

// fetchNodes returns the projected nodes for the given ch_ids keyed by ch_id;
// ids without a node are absent from the map.
func fetchNodes(t *testing.T, s *graph.Store, ctx context.Context, chIDs ...string) map[string]nodeView {
	t.Helper()
	literals := make([]string, len(chIDs))
	for i, id := range chIDs {
		literals[i] = `"` + id + `"`
	}
	q := fmt.Sprintf(
		`{ q(func: eq(ch_id, [%s])) { uid ch_id scope key entity_type display_name dgraph.type first_seen last_seen } }`,
		strings.Join(literals, ", "))
	resp, err := s.Dgraph().NewReadOnlyTxn().Query(ctx, q)
	if err != nil {
		t.Fatalf("fetch nodes %v: %v", chIDs, err)
	}
	var parsed struct {
		Q []nodeView `json:"q"`
	}
	if err := json.Unmarshal(resp.Json, &parsed); err != nil {
		t.Fatalf("decode nodes response %s: %v", resp.Json, err)
	}
	out := make(map[string]nodeView, len(parsed.Q))
	for _, n := range parsed.Q {
		out[n.ChID] = n
	}
	return out
}

// readWatermarkValue reads one named cursor's ts directly from ClickHouse.
func readWatermarkValue(t *testing.T, ctx context.Context, conn driver.Conn, name string) time.Time {
	t.Helper()
	var ts time.Time
	if err := conn.QueryRow(ctx,
		"SELECT max(ts) FROM mem.projection_watermark WHERE name = ?", name).Scan(&ts); err != nil {
		t.Fatalf("read watermark: %v", err)
	}
	return ts
}

// readWatermarkCursor reads one named cursor's exact (ts, last_id) pair;
// single logical row per name, so max() reads that row's values.
func readWatermarkCursor(t *testing.T, ctx context.Context, conn driver.Conn, name string) (time.Time, string) {
	t.Helper()
	var (
		ts     time.Time
		lastID string
	)
	if err := conn.QueryRow(ctx,
		"SELECT max(ts), max(last_id) FROM mem.projection_watermark WHERE name = ?", name).
		Scan(&ts, &lastID); err != nil {
		t.Fatalf("read %s watermark: %v", name, err)
	}
	return ts, lastID
}

// insertRawEntity plants one mem.entities row directly, bypassing the
// resolver — used to craft deterministic endpoint-presence scenarios.
func insertRawEntity(t *testing.T, ctx context.Context, conn driver.Conn,
	id uuid.UUID, scope, key, entityType string, updatedAt time.Time) {
	t.Helper()
	b, err := conn.PrepareBatch(ctx,
		"INSERT INTO mem.entities "+
			"(entity_id, scope, entity_type, key, display_name, attrs, first_seen, last_seen, updated_at)")
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Append(id, scope, entityType, key, key,
		map[string]string{}, updatedAt, updatedAt, updatedAt); err != nil {
		t.Fatal(err)
	}
	if err := b.Send(); err != nil {
		t.Fatal(err)
	}
}

// insertRawEdge plants one OPEN mem.edges row directly (valid_to sentinel),
// bypassing the writer — used to craft deterministic edge scenarios.
func insertRawEdge(t *testing.T, ctx context.Context, conn driver.Conn,
	id, src, dst uuid.UUID, scope, relation string, updatedAt time.Time) {
	t.Helper()
	b, err := conn.PrepareBatch(ctx,
		"INSERT INTO mem.edges "+
			"(edge_id, scope, src_id, dst_id, relation, from_fact, valid_from, valid_to, updated_at)")
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Append(id, scope, src, dst, relation,
		uuid.Nil, updatedAt, time.Date(2105, 12, 31, 23, 59, 59, 0, time.UTC), updatedAt); err != nil {
		t.Fatal(err)
	}
	if err := b.Send(); err != nil {
		t.Fatal(err)
	}
}

// countEdgesByID counts live mem.edges rows carrying one edge_id.
func countEdgesByID(t *testing.T, ctx context.Context, conn driver.Conn, id uuid.UUID) int {
	t.Helper()
	var n uint64
	if err := conn.QueryRow(ctx,
		"SELECT count() FROM mem.edges WHERE edge_id = ?", id).Scan(&n); err != nil {
		t.Fatalf("count edge %s: %v", id, err)
	}
	return int(n)
}

func TestProjectEntities(t *testing.T) {
	ctx := itestCtx(t)
	s, conn := itestBoth(t)

	scope := "test-entities"
	const (
		rawIP   = "203.0.113.7"
		rawTech = "t1098"
		rawHash = "E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855"
	)
	eIP := seedEntity(t, ctx, conn, scope, rawIP)
	eTech := seedEntity(t, ctx, conn, scope, rawTech)
	eHash := seedEntity(t, ctx, conn, scope, rawHash)
	all := []string{eIP.EntityID, eTech.EntityID, eHash.EntityID}

	n, err := graph.ProjectEntities(ctx, s, conn, 10)
	if err != nil {
		t.Fatalf("ProjectEntities: %v", err)
	}
	if n != 3 {
		t.Fatalf("first projection wrote %d nodes, want 3", n)
	}

	nodes := fetchNodes(t, s, ctx, all...)
	wantByKey := map[string]entity.Entity{
		eIP.EntityID: eIP, eTech.EntityID: eTech, eHash.EntityID: eHash,
	}
	if len(nodes) != 3 {
		t.Fatalf("projected %d distinct nodes, want 3: %+v", len(nodes), nodes)
	}
	for id, want := range wantByKey {
		got, ok := nodes[id]
		if !ok {
			t.Errorf("ch_id %s missing from projection", id)
			continue
		}
		if got.Scope != want.Scope {
			t.Errorf("%s scope = %q, want %q", id, got.Scope, want.Scope)
		}
		if got.Key != want.Key {
			t.Errorf("%s key = %q, want %q", id, got.Key, want.Key)
		}
		if got.EntityType != string(want.EntityType) {
			t.Errorf("%s entity_type = %q, want %q", id, got.EntityType, want.EntityType)
		}
		if got.DisplayName != want.DisplayName {
			t.Errorf("%s display_name = %q, want %q", id, got.DisplayName, want.DisplayName)
		}
		if !hasString(got.DgraphType, "Entity") {
			t.Errorf("%s dgraph.type = %v, want Entity", id, got.DgraphType)
		}
		if got.FirstSeen.Unix() != want.FirstSeen.Unix() {
			t.Errorf("%s first_seen = %v, want %v", id, got.FirstSeen, want.FirstSeen)
		}
		if got.LastSeen.Unix() != want.LastSeen.Unix() {
			t.Errorf("%s last_seen = %v, want %v", id, got.LastSeen, want.LastSeen)
		}
	}
	uidsBefore := map[string]string{}
	for id, nd := range nodes {
		uidsBefore[id] = nd.Uid
	}

	// Drained replay: nothing new in CH, so nothing is written and every uid
	// is untouched.
	n, err = graph.ProjectEntities(ctx, s, conn, 10)
	if err != nil {
		t.Fatalf("second ProjectEntities: %v", err)
	}
	if n != 0 {
		t.Fatalf("drained replay rewrote %d nodes, want 0", n)
	}
	nodes = fetchNodes(t, s, ctx, all...)
	if len(nodes) != 3 {
		t.Fatalf("nodes after drained replay = %d, want 3", len(nodes))
	}
	for id, u := range uidsBefore {
		if got := nodes[id].Uid; got != u {
			t.Errorf("ch_id %s uid changed across drained replay: %s -> %s", id, u, got)
		}
	}

	// Refresh flow: re-resolving the same raw bumps last_seen via a new
	// ReplacingMergeTree version. Sleep past one second first so the
	// second-precision last_seen strictly advances.
	time.Sleep(1100 * time.Millisecond)
	eIP2, created, err := entity.NewResolver(conn).Resolve(ctx, scope, rawIP)
	if err != nil {
		t.Fatalf("refresh resolve: %v", err)
	}
	if created || eIP2.EntityID != eIP.EntityID {
		t.Fatalf("refresh returned created=%v id=%s, want refresh of %s", created, eIP2.EntityID, eIP.EntityID)
	}
	prevLastSeen := nodes[eIP.EntityID].LastSeen

	n, err = graph.ProjectEntities(ctx, s, conn, 10)
	if err != nil {
		t.Fatalf("refresh projection: %v", err)
	}
	if n < 1 {
		t.Fatalf("refresh projection rewrote %d nodes, want >= 1", n)
	}
	nodes = fetchNodes(t, s, ctx, all...)
	got := nodes[eIP.EntityID]
	if got.Uid != uidsBefore[eIP.EntityID] {
		t.Errorf("refresh changed uid for ch_id %s: %s -> %s", eIP.EntityID, uidsBefore[eIP.EntityID], got.Uid)
	}
	if got.LastSeen.Unix() <= prevLastSeen.Unix() {
		t.Errorf("refresh did not advance last_seen: %v -> %v", prevLastSeen, got.LastSeen)
	}
	if got.LastSeen.Unix() != eIP2.LastSeen.Unix() {
		t.Errorf("node last_seen = %v, want refreshed %v", got.LastSeen, eIP2.LastSeen)
	}
}

func TestProjectEntitiesBatchBoundary(t *testing.T) {
	ctx := itestCtx(t)
	s, conn := itestBoth(t)

	scope := "test-batch"
	eA := seedEntity(t, ctx, conn, scope, "198.51.100.1")
	eB := seedEntity(t, ctx, conn, scope, "198.51.100.2")
	eC := seedEntity(t, ctx, conn, scope, "T1059")
	all := []string{eA.EntityID, eB.EntityID, eC.EntityID}

	// Expected total order matches the projector's deterministic sort.
	rows, err := conn.Query(ctx,
		"SELECT entity_id FROM mem.entities FINAL WHERE updated_at >= toDateTime64(0,3) "+
			"ORDER BY updated_at ASC, entity_id ASC")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var ordered []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ordered = append(ordered, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(ordered) != 3 {
		t.Fatalf("expected 3 seeded entities, got %d (%v)", len(ordered), ordered)
	}

	wmPrev := readWatermarkValue(t, ctx, conn, wmEntities)
	type step struct {
		wantN int
		want  []string // ch_ids expected present after this call
	}
	steps := []step{
		{wantN: 2, want: []string{ordered[0], ordered[1]}},
		{wantN: 1, want: ordered},
		{wantN: 0, want: ordered},
	}
	for i, stp := range steps {
		n, err := graph.ProjectEntities(ctx, s, conn, 2)
		if err != nil {
			t.Fatalf("step %d: ProjectEntities: %v", i, err)
		}
		if n != stp.wantN {
			t.Errorf("step %d wrote %d nodes, want %d", i, n, stp.wantN)
		}
		wmNow := readWatermarkValue(t, ctx, conn, wmEntities)
		if wmNow.Before(wmPrev) {
			t.Errorf("step %d rewound watermark: %v -> %v", i, wmPrev, wmNow)
		}
		wmPrev = wmNow

		nodes := fetchNodes(t, s, ctx, all...)
		if len(nodes) != len(stp.want) {
			t.Errorf("step %d has %d nodes, want %d", i, len(nodes), len(stp.want))
			continue
		}
		present := make(map[string]bool, len(nodes))
		for id := range nodes {
			present[id] = true
		}
		for _, id := range stp.want {
			if !present[id] {
				t.Errorf("step %d missing expected node %s", i, id)
			}
		}
	}
}

func hasString(xs []string, want string) bool {
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
}

// relatedEdgeView is one outgoing related_to target of a node. Dgraph
// returns per-edge facets as sibling keys ON THE TARGET OBJECT (e.g.
// "related_to|relation"), not on the parent node.
type relatedEdgeView struct {
	Uid            string `json:"uid"`
	ChID           string `json:"ch_id"`
	FacetRelation  string `json:"related_to|relation"`
	FacetValidFrom string `json:"related_to|valid_from"`
	FacetValidTo   string `json:"related_to|valid_to"`
}

// outgoingView is the projected subj node plus its outgoing edges.
type outgoingView struct {
	Uid       string            `json:"uid"`
	RelatedTo []relatedEdgeView `json:"related_to"`
}

// fetchOutgoing returns the projected view of one ch_id's node, including
// its outgoing related_to edges and facets. A missing node yields ok=false.
func fetchOutgoing(t *testing.T, s *graph.Store, ctx context.Context, chID string) (outgoingView, bool) {
	t.Helper()
	q := fmt.Sprintf(
		`{ q(func: eq(ch_id, %q)) { uid related_to @facets(relation, valid_from, valid_to) { uid ch_id } } }`,
		chID)
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
		ActorID:     "analyst-edge-test",
	})
	if err != nil {
		t.Fatalf("assert fact %s/%s: %v", predicate, objectValue, err)
	}
	if f.Status != memory.Active {
		t.Fatalf("fact %s status = %s, want active", f.ID, f.Status)
	}
	return f
}

func TestProjectEdges(t *testing.T) {
	ctx := itestCtx(t)
	s, conn := itestBoth(t)

	scope := fmt.Sprintf("itest-edges-%x", time.Now().UnixNano())
	svc := edgeService(t, conn)

	eSubj := seedEntity(t, ctx, conn, scope, "c2.beacon.example")
	eObj1 := seedEntity(t, ctx, conn, scope, "198.51.100.9")
	eObj2 := seedEntity(t, ctx, conn, scope, "198.51.100.10")

	if n, err := graph.ProjectEntities(ctx, s, conn, 10); err != nil || n != 3 {
		t.Fatalf("ProjectEntities = (%d, %v), want (3, nil)", n, err)
	}

	// Open-edge projection: the activated fact's edge appears with facets.
	f1 := assertFactWithObject(t, svc, scope, eSubj.EntityID, "communicates_with", "c2", eObj1.EntityID)
	n, err := graph.ProjectEdges(ctx, s, conn, 10)
	if err != nil {
		t.Fatalf("ProjectEdges: %v", err)
	}
	if n.Processed != 1 {
		t.Fatalf("first edge projection processed %d rows, want 1", n.Processed)
	}
	out, ok := fetchOutgoing(t, s, ctx, eSubj.EntityID)
	if !ok || len(out.RelatedTo) != 1 {
		t.Fatalf("projected edge missing after first projection: %+v (ok=%v)", out, ok)
	}
	if got := out.RelatedTo[0].ChID; got != eObj1.EntityID {
		t.Errorf("edge target ch_id = %s, want %s", got, eObj1.EntityID)
	}
	edge := out.RelatedTo[0]
	if edge.FacetRelation != "communicates_with" {
		t.Errorf("relation facet = %q, want communicates_with", edge.FacetRelation)
	}
	vf, err := time.Parse(time.RFC3339, edge.FacetValidFrom)
	if err != nil {
		t.Fatalf("parse valid_from facet %q: %v", edge.FacetValidFrom, err)
	}
	if vf.Unix() != f1.ValidFrom.Truncate(time.Second).Unix() {
		t.Errorf("valid_from facet = %v, want ~%v", vf, f1.ValidFrom)
	}
	if vt, perr := time.Parse(time.RFC3339, edge.FacetValidTo); perr != nil || vt.Year() < 2100 {
		t.Errorf("valid_to facet = %q (err=%v), want open-ended sentinel", edge.FacetValidTo, perr)
	}

	// Supersede path through the REAL writer: asserting a new value for the
	// same (scope, subject, predicate) closes the prior edge via mutation —
	// which MOVES updated_at (migration 003), so the projector whose cursor
	// already passed the original insert still sees the closure. Both rows
	// process in one call: old closed -> delete triple, new open -> set.
	time.Sleep(1100 * time.Millisecond)
	f2 := assertFactWithObject(t, svc, scope, eSubj.EntityID, "communicates_with", "benign-parked", eObj2.EntityID)
	n, err = graph.ProjectEdges(ctx, s, conn, 10)
	if err != nil {
		t.Fatalf("supersede ProjectEdges: %v", err)
	}
	if n.Processed != 2 {
		t.Fatalf("supersede projection processed %d rows, want 2 (closed prior + new)", n.Processed)
	}
	out, _ = fetchOutgoing(t, s, ctx, eSubj.EntityID)
	if len(out.RelatedTo) != 1 {
		t.Fatalf("after supersede outgoing = %+v, want exactly the new link", out)
	}
	if got := out.RelatedTo[0].ChID; got != eObj2.EntityID {
		t.Errorf("after supersede target = %s, want %s (old link gone, new present)", got, eObj2.EntityID)
	}
	if out.RelatedTo[0].FacetRelation != "communicates_with" {
		t.Errorf("after supersede relation facet = %q", out.RelatedTo[0].FacetRelation)
	}

	// Retract path through the REAL writer: RetractFact fires
	// closeEdgesByFromFact, bumping updated_at on the closed row; the next
	// projection deletes the triple outright.
	if _, err := svc.RetractFact(ctx, f2.ID, "false positive", "human", "analyst-edge-test"); err != nil {
		t.Fatalf("retract: %v", err)
	}
	n, err = graph.ProjectEdges(ctx, s, conn, 10)
	if err != nil {
		t.Fatalf("retract ProjectEdges: %v", err)
	}
	if n.Processed != 1 {
		t.Fatalf("retract projection processed %d rows, want 1", n.Processed)
	}
	out, _ = fetchOutgoing(t, s, ctx, eSubj.EntityID)
	if len(out.RelatedTo) != 0 {
		t.Fatalf("edge still present after retraction: %+v", out)
	}

	// Drained replay: nothing new, nothing written.
	n, err = graph.ProjectEdges(ctx, s, conn, 10)
	if err != nil {
		t.Fatalf("drained ProjectEdges: %v", err)
	}
	if n.Total() != 0 {
		t.Fatalf("drained replay processed %d rows, want 0", n.Total())
	}
}

func TestProjectEdgesBatch(t *testing.T) {
	ctx := itestCtx(t)
	s, conn := itestBoth(t)

	scope := fmt.Sprintf("itest-edgebatch-%x", time.Now().UnixNano())
	svc := edgeService(t, conn)

	eSubj := seedEntity(t, ctx, conn, scope, "198.51.100.21")
	eObj1 := seedEntity(t, ctx, conn, scope, "T1059")
	eObj2 := seedEntity(t, ctx, conn, scope, "203.0.113.31")
	all := []string{eSubj.EntityID, eObj1.EntityID, eObj2.EntityID}

	if n, err := graph.ProjectEntities(ctx, s, conn, 10); err != nil || n != 3 {
		t.Fatalf("ProjectEntities = (%d, %v), want (3, nil)", n, err)
	}

	// Two projectable edges: distinct predicates so neither supersedes the
	// other. Each mints one open mem.edges row via the real writer.
	assertFactWithObject(t, svc, scope, eSubj.EntityID, "communicates_with", "x", eObj1.EntityID)
	time.Sleep(5 * time.Millisecond) // pin pagination order across the three edges
	assertFactWithObject(t, svc, scope, eSubj.EntityID, "resolves_to", "y", eObj2.EntityID)
	time.Sleep(5 * time.Millisecond)

	// Third edge whose dst entity deliberately has NO projected node: the
	// pre-fix projector skipped it AND advanced the cursor past it,
	// permanently losing the edge whenever projection lagged. It must now
	// DEFER — hold the cursor just before itself — until ProjectEntities
	// catches up on a later tick.
	eLate := seedEntity(t, ctx, conn, scope, "E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855")
	assertFactWithObject(t, svc, scope, eSubj.EntityID, "drops", "z", eLate.EntityID)

	wmPrev := readWatermarkValue(t, ctx, conn, wmEdges)
	for i, wantN := range []int{1, 1} {
		n, err := graph.ProjectEdges(ctx, s, conn, 1)
		if err != nil {
			t.Fatalf("step %d: ProjectEdges: %v", i, err)
		}
		if n.Processed != wantN {
			t.Errorf("step %d processed %d rows, want %d", i, n.Processed, wantN)
		}
		wmNow := readWatermarkValue(t, ctx, conn, wmEdges)
		if wmNow.Before(wmPrev) {
			t.Errorf("step %d rewound watermark: %v -> %v", i, wmPrev, wmNow)
		}
		wmPrev = wmNow
	}

	// The unprojectable head edge defers: zero progress, cursor pinned.
	statsDeferred, err := graph.ProjectEdges(ctx, s, conn, 1)
	if err != nil {
		t.Fatalf("deferring ProjectEdges: %v", err)
	}
	if statsDeferred.Processed != 0 || statsDeferred.DeletedOrphans != 0 || statsDeferred.Deferred != 1 {
		t.Fatalf("deferral stats = %+v, want exactly one deferred row and no other movement", statsDeferred)
	}
	wmDeferred := readWatermarkValue(t, ctx, conn, wmEdges)
	if !wmDeferred.Equal(wmPrev) {
		t.Errorf("cursor advanced past deferred edge: %v -> %v", wmPrev, wmDeferred)
	}

	// No dangling node may appear for the unprojected endpoint.
	nodes := fetchNodes(t, s, ctx, all...)
	if len(nodes) != 3 {
		t.Fatalf("node count = %d, want 3 (deferral must not create nodes)", len(nodes))
	}
	if late := fetchNodes(t, s, ctx, eLate.EntityID); len(late) != 0 {
		t.Errorf("dangling node created for unprojected endpoint: %+v", late)
	}

	// Heal: the next entity tick projects the missing node, and the pinned
	// edge then links on the following edge tick — self-healing, no loss.
	if n, err := graph.ProjectEntities(ctx, s, conn, 10); err != nil || n != 1 {
		t.Fatalf("healing ProjectEntities = (%d, %v), want (1, nil)", n, err)
	}
	statsFinal, err := graph.ProjectEdges(ctx, s, conn, 10)
	if err != nil {
		t.Fatalf("healed ProjectEdges: %v", err)
	}
	if statsFinal.Processed != 1 || statsFinal.Deferred != 0 || statsFinal.DeletedOrphans != 0 {
		t.Fatalf("healed stats = %+v, want the deferred edge processed once", statsFinal)
	}

	nodes = fetchNodes(t, s, ctx, all...)
	if len(nodes) != 3 {
		t.Fatalf("node count after heal = %d, want 3", len(nodes))
	}
	out, ok := fetchOutgoing(t, s, ctx, eSubj.EntityID)
	if !ok || len(out.RelatedTo) != 3 {
		t.Fatalf("outgoing after heal = %+v, want 3 links", out)
	}
	targets := map[string]string{}
	for _, r := range out.RelatedTo {
		targets[r.ChID] = r.FacetRelation
	}
	for id, rel := range map[string]string{
		eObj1.EntityID: "communicates_with",
		eObj2.EntityID: "resolves_to",
		eLate.EntityID: "drops",
	} {
		if targets[id] != rel {
			t.Errorf("target %s relation = %q, want %q", id, targets[id], rel)
		}
	}
}

// Regression: pagination and the watermark CAS must order id tiebreakers the
// SAME way, or a page boundary inside a same-millisecond cluster freezes the
// cursor in an infinite re-read loop.
//
// mem.edges.edge_id is UUID; ClickHouse filters/compares UUIDs in its
// internal byte order, which disagrees with canonical text order — while the
// watermark's String last_id is compared as text by updateCursor's CAS. The
// two rows below sit on opposite sides of that disagreement (pair-order says
// A < B; text order says B < A), so with batch=1 the pre-fix projector
// served [A] then [B], then had its CAS ('df49…' < '5169…' textually false)
// reject every subsequent advance forever.
func TestProjectEdgesTiebreakerOrderRegression(t *testing.T) {
	ctx := itestCtx(t)
	s, conn := itestBoth(t)

	scope := fmt.Sprintf("itest-tiebreak-%x", time.Now().UnixNano())
	eSubj := seedEntity(t, ctx, conn, scope, "198.51.100.41")
	eObj := seedEntity(t, ctx, conn, scope, "T1098")
	if n, err := graph.ProjectEntities(ctx, s, conn, 10); err != nil || n != 2 {
		t.Fatalf("ProjectEntities = (%d, %v), want (2, nil)", n, err)
	}

	// Two open edges sharing ONE updated_at instant, with engineered ids:
	// pairUUID sorts before textUUID in ClickHouse's internal UUID order,
	// but AFTER it in canonical text order.
	const (
		pairUUID = "df49d9d2-779f-4e9d-8181-62d43121fa57"
		textUUID = "516926c9-b1ae-46c9-83d6-c98105182371"
	)
	sameMs := time.Now().UTC().Truncate(time.Millisecond)
	for _, id := range []string{pairUUID, textUUID} {
		eid, err := uuid.Parse(id)
		if err != nil {
			t.Fatal(err)
		}
		b, err := conn.PrepareBatch(ctx,
			"INSERT INTO mem.edges "+
				"(edge_id, scope, src_id, dst_id, relation, from_fact, valid_from, valid_to, updated_at)")
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Append(eid, scope,
			mustParseUUID(t, eSubj.EntityID), mustParseUUID(t, eObj.EntityID),
			"communicates_with", uuid.Nil, sameMs,
			time.Date(2105, 12, 31, 23, 59, 59, 0, time.UTC), sameMs); err != nil {
			t.Fatal(err)
		}
		if err := b.Send(); err != nil {
			t.Fatal(err)
		}
	}

	var total int
	drained := false
	for i := 0; i < 10; i++ { // cap: the pre-fix code never drains
		n, err := graph.ProjectEdges(ctx, s, conn, 1)
		if err != nil {
			t.Fatalf("step %d: ProjectEdges: %v", i, err)
		}
		total += n.Processed
		if n.Total() == 0 {
			drained = true
			break
		}
	}
	if !drained || total != 2 {
		t.Fatalf("edge projection did not drain cleanly (drained=%v total=%d): cursor frozen on tiebreaker mismatch", drained, total)
	}
	wm := readWatermarkValue(t, ctx, conn, wmEdges)
	if wm.Unix() == 0 {
		t.Error("edges watermark still at epoch after drain")
	}
}

// Entity-projector twin of TestProjectEdgesTiebreakerOrderRegression: two
// entities share ONE updated_at millisecond while their ids sit on opposite
// sides of the byte-order vs canonical-text-order disagreement (pairUUID
// sorts before textUUID in ClickHouse's internal UUID order, AFTER it in
// text). With batch=1 both must project across calls — neither skipped nor
// duplicated (asserted via ch_id presence) and a clean drain.
func TestProjectEntitiesTiebreakerOrderRegression(t *testing.T) {
	ctx := itestCtx(t)
	s, conn := itestBoth(t)

	scope := "test-entity-tiebreak"
	const (
		pairUUID = "df49d9d2-779f-4e9d-8181-62d43121fa57"
		textUUID = "516926c9-b1ae-46c9-83d6-c98105182371"
	)
	sameMs := time.Now().UTC().Truncate(time.Millisecond)
	for _, tc := range []struct{ id, key string }{
		{pairUUID, "tiebreak-a.example.net"},
		{textUUID, "tiebreak-b.example.net"},
	} {
		eid, err := uuid.Parse(tc.id)
		if err != nil {
			t.Fatal(err)
		}
		b, err := conn.PrepareBatch(ctx,
			"INSERT INTO mem.entities "+
				"(entity_id, scope, entity_type, key, display_name, attrs, first_seen, last_seen, updated_at)")
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Append(eid, scope, "ioc_domain", tc.key, tc.key,
			map[string]string{}, sameMs, sameMs, sameMs); err != nil {
			t.Fatal(err)
		}
		if err := b.Send(); err != nil {
			t.Fatal(err)
		}
	}

	var total int
	drained := false
	for i := 0; i < 10; i++ { // cap: the pre-fix code never drains
		n, err := graph.ProjectEntities(ctx, s, conn, 1)
		if err != nil {
			t.Fatalf("step %d: ProjectEntities: %v", i, err)
		}
		total += n
		if n == 0 {
			drained = true
			break
		}
	}
	if !drained || total != 2 {
		t.Fatalf("entity projection did not drain cleanly (drained=%v total=%d): cursor frozen on tiebreaker mismatch", drained, total)
	}
	nodes := fetchNodes(t, s, ctx, pairUUID, textUUID)
	if len(nodes) != 2 {
		t.Fatalf("projected %d distinct ch_ids (%+v), want 2", len(nodes), nodes)
	}
	for _, id := range []string{pairUUID, textUUID} {
		if n, ok := nodes[id]; !ok {
			t.Errorf("ch_id %s missing from projection", id)
		} else if n.Scope != scope || n.EntityType != "ioc_domain" {
			t.Errorf("ch_id %s projected wrong content: %+v", id, n)
		}
	}
}

func mustParseUUID(t *testing.T, s string) uuid.UUID {
	t.Helper()
	u, err := uuid.Parse(s)
	if err != nil {
		t.Fatalf("parse uuid %q: %v", s, err)
	}
	return u
}

// Regression for the live permanent-skip bug: an edge whose endpoints exist
// in ClickHouse but have no Dgraph nodes yet must DEFER — the cursor holds
// just before the edge and the batch stops — never skip-and-advance. The
// setup replays the live failure shape: endpoint rows planted upstream by
// raw SQL, ProjectEdges invoked while entities are still unprojected, then
// healing once the entity projector catches up.
func TestProjectEdgesDefersUntilEndpointsProjected(t *testing.T) {
	ctx := itestCtx(t)
	s, conn := itestBoth(t)

	scope := fmt.Sprintf("itest-defer-%x", time.Now().UnixNano())
	srcID := mustParseUUID(t, "11111111-1111-4111-8111-111111111111")
	dstID := mustParseUUID(t, "22222222-2222-4222-8222-222222222222")
	edgeID := mustParseUUID(t, "33333333-3333-4333-8333-333333333333")

	// Endpoints exist UPSTREAM only (raw INSERTs); nothing is projected.
	base := time.Now().UTC().Truncate(time.Millisecond)
	insertRawEntity(t, ctx, conn, srcID, scope, "defer-src.example.com", "ioc_domain", base)
	insertRawEntity(t, ctx, conn, dstID, scope, "defer-dst.example.net", "ioc_domain",
		base.Add(time.Millisecond))
	insertRawEdge(t, ctx, conn, edgeID, srcID, dstID, scope, "communicates_with",
		base.Add(2*time.Millisecond))

	// 1. Entities lagging (no Dgraph nodes yet): the edge must defer with
	// zero progress and the cursor must stay at epoch — NOT advance past
	// the edge.
	statsDeferred, err := graph.ProjectEdges(ctx, s, conn, 10)
	if err != nil {
		t.Fatalf("deferred ProjectEdges: %v", err)
	}
	if statsDeferred.Processed != 0 || statsDeferred.DeletedOrphans != 0 || statsDeferred.Deferred != 1 {
		t.Fatalf("stats = %+v, want exactly one deferred row", statsDeferred)
	}
	ts, lastID := readWatermarkCursor(t, ctx, conn, wmEdges)
	if ts.UnixMilli() != 0 || lastID != "" {
		t.Fatalf("cursor advanced past deferred edge: (%v, %q), want epoch/empty", ts, lastID)
	}

	// The stall is stable: a repeated tick defers again without moving.
	statsAgain, err := graph.ProjectEdges(ctx, s, conn, 10)
	if err != nil {
		t.Fatalf("repeated deferred ProjectEdges: %v", err)
	}
	if statsAgain.Deferred != 1 || statsAgain.Processed != 0 {
		t.Fatalf("repeat stats = %+v, want the same single deferral", statsAgain)
	}

	// No dangling node creation for either endpoint.
	if nodes := fetchNodes(t, s, ctx, srcID.String(), dstID.String()); len(nodes) != 0 {
		t.Fatalf("dangling nodes created: %+v", nodes)
	}

	// 2. The entity projector ticks: both nodes appear.
	if n, err := graph.ProjectEntities(ctx, s, conn, 10); err != nil || n != 2 {
		t.Fatalf("ProjectEntities = (%d, %v), want (2, nil)", n, err)
	}
	nodes := fetchNodes(t, s, ctx, srcID.String(), dstID.String())
	if len(nodes) != 2 {
		t.Fatalf("healed node count = %d, want 2: %+v", len(nodes), nodes)
	}

	// 3. The pinned edge now links in Dgraph from the untouched position.
	statsHealed, err := graph.ProjectEdges(ctx, s, conn, 10)
	if err != nil {
		t.Fatalf("healed ProjectEdges: %v", err)
	}
	if statsHealed.Processed != 1 || statsHealed.Deferred != 0 || statsHealed.DeletedOrphans != 0 {
		t.Fatalf("healed stats = %+v, want the deferred edge processed once", statsHealed)
	}
	out, ok := fetchOutgoing(t, s, ctx, srcID.String())
	if !ok || len(out.RelatedTo) != 1 {
		t.Fatalf("outgoing after heal = %+v (ok=%v), want the linked edge", out, ok)
	}
	if got := out.RelatedTo[0].ChID; got != dstID.String() {
		t.Errorf("linked target = %s, want %s", got, dstID.String())
	}
	if out.RelatedTo[0].FacetRelation != "communicates_with" {
		t.Errorf("relation facet = %q, want communicates_with", out.RelatedTo[0].FacetRelation)
	}
	_, lastID = readWatermarkCursor(t, ctx, conn, wmEdges)
	if lastID != edgeID.String() {
		t.Errorf("cursor last_id = %q, want past the healed edge %s", lastID, edgeID)
	}
}

// Orphan edges — endpoints absent from mem.entities FINAL altogether — are
// derived-state garbage (ADR-001): the row must be DELETED from ClickHouse
// and the cursor advanced normally past it, never deferred forever. Covers
// all three detection shapes: both endpoints missing, one endpoint present-
// upstream-but-unprojected, and one endpoint already resolved to a uid.
func TestProjectEdgesDeletesOrphans(t *testing.T) {
	ctx := itestCtx(t)
	s, conn := itestBoth(t)

	scope := fmt.Sprintf("itest-orphan-%x", time.Now().UnixNano())
	const (
		ghostA = "44444444-4444-4444-8444-444444444444" // never inserted anywhere
		ghostB = "55555555-5555-4555-8555-555555555555" // never inserted anywhere
		realID = "66666666-6666-4666-8666-666666666666" // planted upstream only
	)
	eA := mustParseUUID(t, "77777777-7777-4777-8777-777777777777") // ghostA -> ghostB
	eB := mustParseUUID(t, "88888888-8888-4888-8888-888888888888") // real   -> ghostB

	base := time.Now().UTC().Truncate(time.Millisecond)
	insertRawEdge(t, ctx, conn, eA,
		mustParseUUID(t, ghostA), mustParseUUID(t, ghostB), scope, "resolves_to", base)
	insertRawEntity(t, ctx, conn, mustParseUUID(t, realID),
		scope, "orphan-src.example.com", "ioc_ip", base.Add(time.Millisecond))
	insertRawEdge(t, ctx, conn, eB,
		mustParseUUID(t, realID), mustParseUUID(t, ghostB), scope, "communicates_with",
		base.Add(2*time.Millisecond))

	// Only the real entity projects; the ghosts stay absent everywhere.
	if n, err := graph.ProjectEntities(ctx, s, conn, 10); err != nil || n != 1 {
		t.Fatalf("ProjectEntities = (%d, %v), want (1, nil)", n, err)
	}

	stats, err := graph.ProjectEdges(ctx, s, conn, 10)
	if err != nil {
		t.Fatalf("orphan ProjectEdges: %v", err)
	}
	if stats.Processed != 0 || stats.DeletedOrphans != 2 || stats.Deferred != 0 {
		t.Fatalf("stats = %+v, want both rows deleted as orphans", stats)
	}

	for _, id := range []uuid.UUID{eA, eB} {
		if n := countEdgesByID(t, ctx, conn, id); n != 0 {
			t.Errorf("orphan edge %s still in mem.edges (%d rows)", id, n)
		}
	}
	// Orphan GC touches only mem.edges: the real entity row survives...
	var ents uint64
	if err := conn.QueryRow(ctx,
		"SELECT count() FROM mem.entities FINAL WHERE entity_id = ?",
		mustParseUUID(t, realID)).Scan(&ents); err != nil {
		t.Fatal(err)
	}
	if ents != 1 {
		t.Errorf("real entity row count = %d, want 1 (only edges may be deleted)", ents)
	}
	// ...and its projected node carries no link to the shared ghost.
	if out, ok := fetchOutgoing(t, s, ctx, realID); ok && len(out.RelatedTo) != 0 {
		t.Errorf("resolved-src orphan left a triple behind: %+v", out.RelatedTo)
	}
	if ghosts := fetchNodes(t, s, ctx, ghostA, ghostB); len(ghosts) != 0 {
		t.Errorf("ghost nodes materialized: %+v", ghosts)
	}

	// Cursor advanced normally PAST both deleted rows (eB sorts last).
	ts, lastID := readWatermarkCursor(t, ctx, conn, wmEdges)
	if ts.UnixMilli() < base.Add(2*time.Millisecond).UnixMilli() || lastID != eB.String() {
		t.Fatalf("cursor after orphan pass = (%v, %q), want at/past eB %s", ts, lastID, eB)
	}

	// Drained replay: nothing left to do, nothing rewritten.
	statsAgain, err := graph.ProjectEdges(ctx, s, conn, 10)
	if err != nil {
		t.Fatalf("drained replay: %v", err)
	}
	if statsAgain.Total() != 0 {
		t.Fatalf("drained replay stats = %+v, want zero activity", statsAgain)
	}
}
