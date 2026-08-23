package memory

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"socmem/internal/entity"
	"socmem/internal/graph"
)

// itestTwoStores wires a graph-mode service over BOTH stores, reset fresh:
// Dgraph data dropped (schema kept), mem.entities/mem.edges wiped and both
// projection watermarks zeroed — the same contract as the graph package's
// itestBoth, so seeded rows project deterministically. The wipes stay safe
// under sequential test order: every file registered before this one has
// finished by then, and trust_test.go (registered after) touches none of
// the wiped tables.
func itestTwoStores(t *testing.T) (*Service, *graph.Store, driver.Conn) {
	t.Helper()
	dgAddr := os.Getenv("MEM_TEST_DGRAPH_ADDR")
	if dgAddr == "" {
		t.Skip("MEM_TEST_DGRAPH_ADDR not set; skipping two-store traverse test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	g, err := graph.Connect(ctx, dgAddr)
	if err != nil {
		t.Fatalf("dgraph connect: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	if err := g.InstallSchema(ctx); err != nil {
		t.Fatal(err)
	}
	if err := g.DropData(ctx); err != nil {
		t.Fatal(err)
	}
	conn := itestConn(t)
	for _, table := range []string{"mem.entities", "mem.edges"} {
		if err := conn.Exec(ctx,
			"ALTER TABLE "+table+" DELETE WHERE 1 SETTINGS mutations_sync = 1"); err != nil {
			t.Fatalf("wipe %s: %v", table, err)
		}
	}
	for _, name := range []string{"entities", "edges"} {
		if err := conn.Exec(ctx,
			"ALTER TABLE mem.projection_watermark "+
				"UPDATE ts = toDateTime64(0, 3), last_id = '' "+
				"WHERE name = ? SETTINGS mutations_sync = 1", name); err != nil {
			t.Fatalf("reset %s watermark: %v", name, err)
		}
	}
	return testService(t, conn).WithGraph(g), g, conn
}

// seedEdge asserts an ACTIVE human fact carrying an object endpoint through
// the real writer — minting exactly one open mem.edges row — the same
// seeding discipline the projection tests use.
func seedEdge(t *testing.T, ctx context.Context, svc *Service, scope, subjectID, predicate, objectValue, objectID string) {
	t.Helper()
	f, err := svc.AssertFact(ctx, FactInput{
		Scope:       scope,
		SubjectID:   subjectID,
		Predicate:   predicate,
		ObjectValue: objectValue,
		ObjectID:    objectID,
		Confidence:  0.9,
		ActorType:   "human",
		ActorID:     "analyst-traverse",
	})
	if err != nil {
		t.Fatalf("assert fact %s: %v", predicate, err)
	}
	if f.Status != Active {
		t.Fatalf("fact %s status = %s, want active", f.ID, f.Status)
	}
}

// drainProjections runs ProjectEntities then ProjectEdges until both are
// drained, so a test never depends on page-size arithmetic.
func drainProjections(t *testing.T, ctx context.Context, g *graph.Store, conn driver.Conn) {
	t.Helper()
	for i := 0; i < 100; i++ {
		nEnt, err := graph.ProjectEntities(ctx, g, conn, 500)
		if err != nil {
			t.Fatalf("ProjectEntities: %v", err)
		}
		nEdge, err := graph.ProjectEdges(ctx, g, conn, 500)
		if err != nil {
			t.Fatalf("ProjectEdges: %v", err)
		}
		if nEnt == 0 && nEdge == 0 {
			return
		}
	}
	t.Fatal("projections did not drain in 100 rounds")
}

// pathShape renders a Path as "id0 -(rel)-> id1 ..." for assertion messages.
func pathShape(p Path) string {
	s := p.Nodes[0].EntityID
	for i, rel := range p.Relations {
		s += fmt.Sprintf(" -%s-> %s", rel, p.Nodes[i+1].EntityID)
	}
	return s
}

// findPath returns the path whose node ids and relation labels match
// wantIDs/wantRels exactly, or nil.
func findPath(paths []Path, wantIDs []string, wantRels []string) *Path {
	for i := range paths {
		p := paths[i]
		if len(p.Nodes) != len(wantIDs) || len(p.Relations) != len(wantRels) {
			continue
		}
		match := true
		for j, id := range wantIDs {
			if p.Nodes[j].EntityID != id {
				match = false
				break
			}
		}
		for j, rel := range wantRels {
			if p.Relations[j] != rel {
				match = false
				break
			}
		}
		if match {
			return &p
		}
	}
	return nil
}

// resolveOne creates one entity through the resolver (first sight).
func resolveOne(t *testing.T, ctx context.Context, conn driver.Conn, scope, raw string) entity.Entity {
	t.Helper()
	e, created, err := entity.NewResolver(conn).Resolve(ctx, scope, raw)
	if err != nil || !created {
		t.Fatalf("resolve %q: created=%v err=%v", raw, created, err)
	}
	return e
}

// noRepeatedNodes asserts every path is simple (cycle-safety witness).
func noRepeatedNodes(t *testing.T, paths []Path) {
	t.Helper()
	for _, p := range paths {
		seen := map[string]bool{}
		for _, n := range p.Nodes {
			if seen[n.EntityID] {
				t.Fatalf("repeated node %s within one path: %s", n.EntityID, pathShape(p))
			}
			seen[n.EntityID] = true
		}
	}
}

// TestTraverseGraphMode seeds A -[communicates_with]-> B -[resolved_to]-> C
// through the real writers, projects both stores, and walks from A: a 2-hop
// path must come back fully hydrated, hops must be honored exactly, the
// relation filter must apply per edge, and a back-edge C→A must terminate
// without repeating nodes.
func TestTraverseGraphMode(t *testing.T) {
	svc, g, conn := itestTwoStores(t)
	ctx := context.Background()
	scope := itestScope()

	a := resolveOne(t, ctx, conn, scope, "traverse-a.example.com")
	b := resolveOne(t, ctx, conn, scope, "traverse-b.example.com")
	c := resolveOne(t, ctx, conn, scope, "traverse-c.example.com")

	drainProjections(t, ctx, g, conn) // entities only so far

	seedEdge(t, ctx, svc, scope, a.EntityID, "communicates_with", "beacon", b.EntityID)
	seedEdge(t, ctx, svc, scope, b.EntityID, "resolved_to", "infra", c.EntityID)
	drainProjections(t, ctx, g, conn)

	t.Run("two hops reach C hydrated", func(t *testing.T) {
		paths, err := svc.Traverse(ctx, scope, "traverse-a.example.com", entity.IocDomain, "", 2)
		if err != nil {
			t.Fatal(err)
		}
		p := findPath(paths, []string{a.EntityID, b.EntityID, c.EntityID},
			[]string{"communicates_with", "resolved_to"})
		if p == nil {
			t.Fatalf("no A->B->C path among %d paths: %v", len(paths), shapes(paths))
		}
		for _, n := range p.Nodes {
			if n.DisplayName == "" || n.Key == "" || n.EntityType != entity.IocDomain || n.Scope != scope {
				t.Fatalf("unhydrated node in path %s: %+v", pathShape(*p), n)
			}
		}
		noRepeatedNodes(t, paths)
	})

	t.Run("hops=1 returns only the first hop", func(t *testing.T) {
		paths, err := svc.Traverse(ctx, scope, "traverse-a.example.com", entity.IocDomain, "", 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(paths) != 1 {
			t.Fatalf("hops=1 got %d paths: %v", len(paths), shapes(paths))
		}
		if findPath(paths, []string{a.EntityID, b.EntityID}, []string{"communicates_with"}) == nil {
			t.Fatalf("missing A->B path: %v", shapes(paths))
		}
	})

	t.Run("relation filter excludes non-matching first hop", func(t *testing.T) {
		paths, err := svc.Traverse(ctx, scope, "traverse-a.example.com",
			entity.IocDomain, "resolved_to", 2)
		if err != nil {
			t.Fatal(err)
		}
		if len(paths) != 0 {
			t.Fatalf("relation filter leaked edges: %v", shapes(paths))
		}
	})

	t.Run("cycle back-edge terminates cleanly", func(t *testing.T) {
		// C -[resolved_to]-> A makes C a DIRECT neighbor of A when edges are
		// walked in both directions (~related_to): C now joins the tree at
		// level 1, and because every uid enters the BFS exactly once the
		// walk must terminate without revisiting A.
		seedEdge(t, ctx, svc, scope, c.EntityID, "resolved_to", "loop", a.EntityID)
		drainProjections(t, ctx, g, conn)

		done := make(chan struct{})
		var (
			paths []Path
			err   error
		)
		go func() {
			defer close(done)
			paths, err = svc.Traverse(ctx, scope, "traverse-a.example.com",
				entity.IocDomain, "", 3)
		}()
		select {
		case <-done:
		case <-time.After(20 * time.Second):
			t.Fatal("traverse did not terminate with cyclic graph")
		}
		if err != nil {
			t.Fatal(err)
		}
		if len(paths) != 2 {
			t.Fatalf("cyclic graph got %d paths, want exactly 2: %v", len(paths), shapes(paths))
		}
		if p := findPath(paths, []string{a.EntityID, b.EntityID}, []string{"communicates_with"}); p == nil {
			t.Fatalf("out-edge path lost: %v", shapes(paths))
		}
		if p := findPath(paths, []string{a.EntityID, c.EntityID}, []string{"resolved_to"}); p == nil {
			t.Fatalf("cycle back-edge (reverse) path missing or mislabeled: %v", shapes(paths))
		}
		noRepeatedNodes(t, paths)
		for _, p := range paths {
			last := p.Nodes[len(p.Nodes)-1]
			if last.EntityID == a.EntityID {
				t.Fatalf("start entity leaked as endpoint: %s", pathShape(p))
			}
		}
	})
}

// shapes renders all paths for assertion messages.
func shapes(paths []Path) []string {
	out := make([]string, 0, len(paths))
	for _, p := range paths {
		out = append(out, pathShape(p))
	}
	return out
}

// TestTraverseFallback pins the CH-only contract: without a graph store the
// walk answers ONE hop through mem.edges regardless of the requested depth
// (documented ≤1-hop ceiling), using the same neighbor semantics as Enrich.
func TestTraverseFallback(t *testing.T) {
	conn := itestConn(t)
	ctx := context.Background()
	svc := testService(t, conn) // NO WithGraph: fallback mode
	scope := itestScope()

	a := resolveOne(t, ctx, conn, scope, "fallback-a.example.com")
	b := resolveOne(t, ctx, conn, scope, "fallback-b.example.com")
	c := resolveOne(t, ctx, conn, scope, "fallback-c.example.com")

	// Real writers only; no projection call exists in this mode.
	seedEdge(t, ctx, svc, scope, a.EntityID, "communicates_with", "x", b.EntityID)
	seedEdge(t, ctx, svc, scope, b.EntityID, "resolved_to", "y", c.EntityID)

	paths, err := svc.Traverse(ctx, scope, "fallback-a.example.com", entity.IocDomain, "", 1)
	if err != nil {
		t.Fatal(err)
	}
	if findPath(paths, []string{a.EntityID, b.EntityID}, []string{"communicates_with"}) == nil {
		t.Fatalf("fallback hops=1 missing A->B: %v", shapes(paths))
	}
	for _, p := range paths {
		if len(p.Relations) != 1 {
			t.Fatalf("fallback returned multi-hop path: %s", pathShape(p))
		}
	}

	// hops>=2 STILL yields the same 1-hop result set — the fallback never
	// walks deeper than one hop by design.
	for _, h := range []int{2, 3} {
		deep, err := svc.Traverse(ctx, scope, "fallback-a.example.com", entity.IocDomain, "", h)
		if err != nil {
			t.Fatal(err)
		}
		if len(deep) != len(paths) {
			t.Fatalf("fallback hops=%d returned %d paths, want same as 1-hop (%d): %v vs %v",
				h, len(deep), len(paths), shapes(deep), shapes(paths))
		}
		for _, p := range deep {
			if len(p.Relations) != 1 {
				t.Fatalf("fallback hops=%d produced %d-relation path: %s", h, len(p.Relations), pathShape(p))
			}
		}
	}

	// Honest miss: unknown key → empty, no error.
	miss, err := svc.Traverse(ctx, scope, "never-seen.example.com", entity.IocDomain, "", 1)
	if err != nil || len(miss) != 0 {
		t.Fatalf("unknown key = (%v, %v), want empty+nil", miss, err)
	}
}

// TestTraverseValidation pins input rejection: blank scope/key, malformed
// relation charset and out-of-range hops are ErrInvalidInput errors (never
// silent clamps).
func TestTraverseValidation(t *testing.T) {
	conn := itestConn(t)
	ctx := context.Background()
	svc := testService(t, conn)
	scope := itestScope()

	cases := map[string]struct {
		scope, key, relation string
		hops                 int
	}{
		"blank scope":    {scope: "   ", key: "a.example.com", hops: 1},
		"empty key":      {scope: scope, key: "", hops: 1},
		"blank key":      {scope: scope, key: "   ", hops: 1},
		"uppercase rel":  {scope: scope, key: "a.example.com", relation: "Resolved_To", hops: 1},
		"leading digit":  {scope: scope, key: "a.example.com", relation: "1abc", hops: 1},
		"hyphen rel":     {scope: scope, key: "a.example.com", relation: "has-hyphen", hops: 1},
		"too long rel":   {scope: scope, key: "a.example.com", relation: "a" + strings.Repeat("b", 41), hops: 1},
		"hops zero":      {scope: scope, key: "a.example.com", hops: 0},
		"hops four":      {scope: scope, key: "a.example.com", hops: 4},
		"hops negative":  {scope: scope, key: "a.example.com", hops: -2},
		"hops too large": {scope: scope, key: "a.example.com", hops: 99},
	}
	for name, tc := range cases {
		_, err := svc.Traverse(ctx, tc.scope, tc.key, entity.IocDomain, tc.relation, tc.hops)
		if !errors.Is(err, ErrInvalidInput) {
			t.Errorf("%s: err = %v, want ErrInvalidInput", name, err)
		}
	}

	// Boundary acceptance: hops=1 and hops=3 are valid inputs (they fail on
	// lookup, not validation).
	for _, h := range []int{1, 3} {
		if _, err := svc.Traverse(ctx, scope, "never-seen.example.com", entity.IocDomain, "", h); err != nil {
			t.Errorf("hops=%d rejected: %v", h, err)
		}
	}
}

// TestTraverseProjectionLag documents eventual consistency: with the graph
// store attached but edges NOT yet projected, Traverse reports an honest
// empty result with NO error — and does NOT silently fall back to CH (the
// graph is authoritative when present; lag resolves on the next tick).
func TestTraverseProjectionLag(t *testing.T) {
	svc, g, conn := itestTwoStores(t)
	ctx := context.Background()
	scope := itestScope()

	a := resolveOne(t, ctx, conn, scope, "lag-a.example.com")
	b := resolveOne(t, ctx, conn, scope, "lag-b.example.com")

	// Entities projected so the start node exists; edges deliberately NOT
	// projected (skip the ProjectEdges call).
	nEnt, err := graph.ProjectEntities(ctx, g, conn, 500)
	if err != nil || nEnt < 2 {
		t.Fatalf("ProjectEntities = (%d, %v)", nEnt, err)
	}

	seedEdge(t, ctx, svc, scope, a.EntityID, "communicates_with", "z", b.EntityID)

	paths, err := svc.Traverse(ctx, scope, "lag-a.example.com", entity.IocDomain, "", 2)
	if err != nil {
		t.Fatalf("projection lag must not error: %v", err)
	}
	if len(paths) != 0 {
		t.Fatalf("unprojected edges leaked into traverse: %v", shapes(paths))
	}
}

// TestTraverseCrossScopeRoot pins org-wide root resolution (shared-knowledge
// model): team-b seeds an A→B chain; team-a — holding no local row at
// first — traverses A and still finds the chain, with every node labeled
// team-b (origin attribution travels with shared knowledge). Once team-a
// resolves its OWN row for the same key, its local entity must win root
// resolution and the walk starts there instead.
func TestTraverseCrossScopeRoot(t *testing.T) {
	svc, g, conn := itestTwoStores(t)
	ctx := context.Background()
	teamB := itestScope()
	teamA := itestScope()

	aB := resolveOne(t, ctx, conn, teamB, "xscope-a.example.com")
	bB := resolveOne(t, ctx, conn, teamB, "xscope-b.example.com")

	drainProjections(t, ctx, g, conn) // entities only so far
	seedEdge(t, ctx, svc, teamB, aB.EntityID, "communicates_with", "beacon", bB.EntityID)
	drainProjections(t, ctx, g, conn)

	t.Run("foreign root discovered and labeled", func(t *testing.T) {
		paths, err := svc.Traverse(ctx, teamA, "xscope-a.example.com", entity.IocDomain, "", 1)
		if err != nil {
			t.Fatal(err)
		}
		p := findPath(paths, []string{aB.EntityID, bB.EntityID}, []string{"communicates_with"})
		if p == nil {
			t.Fatalf("cross-scope root missed team-b's chain: %v", shapes(paths))
		}
		for _, n := range p.Nodes {
			if n.Scope != teamB {
				t.Errorf("node %s origin scope = %q, want %q", n.EntityID, n.Scope, teamB)
			}
			if n.Key == "" || n.DisplayName == "" {
				t.Fatalf("unhydrated node in path %s: %+v", pathShape(*p), n)
			}
		}
	})

	t.Run("local root preferred when present", func(t *testing.T) {
		aA := resolveOne(t, ctx, conn, teamA, "xscope-a.example.com")
		cA := resolveOne(t, ctx, conn, teamA, "xscope-c.example.com")
		drainProjections(t, ctx, g, conn)
		seedEdge(t, ctx, svc, teamA, aA.EntityID, "resolved_to", "local", cA.EntityID)
		drainProjections(t, ctx, g, conn)

		paths, err := svc.Traverse(ctx, teamA, "xscope-a.example.com", entity.IocDomain, "", 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(paths) != 1 {
			t.Fatalf("local-root traverse got %d paths, want exactly 1: %v", len(paths), shapes(paths))
		}
		p := findPath(paths, []string{aA.EntityID, cA.EntityID}, []string{"resolved_to"})
		if p == nil {
			t.Fatalf("walk did not start from team-a's local entity: %v", shapes(paths))
		}
		for _, n := range p.Nodes {
			if n.Scope != teamA {
				t.Errorf("node %s origin scope = %q, want local %q", n.EntityID, n.Scope, teamA)
			}
		}
	})
}

// TestTraverseMixedScopeChain pins boundary-crossing expansion: a fact
// asserted in team-a links its subject to an entity that lives in team-b,
// and team-b chains onward to a third entity. The walk must cross the
// scope boundary naturally — no per-node scope gate — and every node must
// arrive labeled with its true origin scope.
func TestTraverseMixedScopeChain(t *testing.T) {
	svc, g, conn := itestTwoStores(t)
	ctx := context.Background()
	teamA := itestScope()
	teamB := itestScope()

	aA := resolveOne(t, ctx, conn, teamA, "mixed-a.example.com")
	bB := resolveOne(t, ctx, conn, teamB, "mixed-b.example.com")
	cB := resolveOne(t, ctx, conn, teamB, "mixed-c.example.com")

	drainProjections(t, ctx, g, conn) // entities only so far

	// The bridging edge is minted by a TEAM-A fact whose object endpoint is
	// TEAM-B's entity: one edge, two scopes.
	seedEdge(t, ctx, svc, teamA, aA.EntityID, "communicates_with", "beacon", bB.EntityID)
	seedEdge(t, ctx, svc, teamB, bB.EntityID, "resolved_to", "infra", cB.EntityID)
	drainProjections(t, ctx, g, conn)

	paths, err := svc.Traverse(ctx, teamA, "mixed-a.example.com", entity.IocDomain, "", 2)
	if err != nil {
		t.Fatal(err)
	}
	p := findPath(paths, []string{aA.EntityID, bB.EntityID, cB.EntityID},
		[]string{"communicates_with", "resolved_to"})
	if p == nil {
		t.Fatalf("walk did not cross the scope boundary: %v", shapes(paths))
	}
	wantScope := []string{teamA, teamB, teamB}
	for i, n := range p.Nodes {
		if n.Scope != wantScope[i] {
			t.Errorf("node %d (%s) origin scope = %q, want %q", i, n.EntityID, n.Scope, wantScope[i])
		}
	}
	noRepeatedNodes(t, paths)
}
