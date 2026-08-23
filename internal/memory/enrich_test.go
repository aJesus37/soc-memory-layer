package memory

import (
	"context"
	"fmt"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"socmem/internal/entity"
)

// insertEdge raw-INSERTs one mem.edges row, bypassing the service — used
// to fabricate graph topology directly; the lifecycle-driven writers live
// on AssertFact/PromoteFact (insertEdgeIfObject).
func insertEdge(t *testing.T, ctx context.Context, conn driver.Conn, scope, src, dst, relation string) {
	t.Helper()
	srcU, err := uuid.Parse(src)
	if err != nil {
		t.Fatal(err)
	}
	dstU, err := uuid.Parse(dst)
	if err != nil {
		t.Fatal(err)
	}
	b, err := conn.PrepareBatch(ctx,
		"INSERT INTO mem.edges "+
			"(edge_id, scope, src_id, dst_id, relation, from_fact, valid_from, valid_to)")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	if err := b.Append(uuid.New(), scope, srcU, dstU, relation, uuid.Nil, now, farFuture); err != nil {
		t.Fatal(err)
	}
	if err := b.Send(); err != nil {
		t.Fatal(err)
	}
}

// TestEnrich seeds one entity through the public write path with an open
// active fact, a superseded fact (same predicate, different value asserted
// later), a retracted fact (assert then retract) and a raw-planted EXPIRED
// fact (valid_to already in the past), plus three linked observations of
// different ages. Enrich must return exactly the open active fact, all
// three observations newest-first, no neighbors yet — and handle unknown
// keys and foreign scopes as a clean Found=false miss with empty slices
// and no error.
func TestEnrich(t *testing.T) {
	conn := itestConn(t)
	ctx := context.Background()
	s := testService(t, conn)
	scope := itestScope()
	subj := mustResolveEntity(t, conn, scope, "bad.example.com")

	// One active human fact...
	f1, err := s.AssertFact(ctx, FactInput{
		Scope:       scope,
		SubjectID:   subj.EntityID,
		Predicate:   "verdict_malicious",
		ObjectValue: "c2",
		Confidence:  0.9,
		ActorType:   "human",
		ActorID:     "analyst-j",
	})
	if err != nil {
		t.Fatal(err)
	}

	// ...superseded by a later assertion of the same predicate with a
	// different value (close-then-insert is mutations_sync=1, so f1 is
	// already closed when f2's insert returns).
	f2, err := s.AssertFact(ctx, FactInput{
		Scope:       scope,
		SubjectID:   subj.EntityID,
		Predicate:   "verdict_malicious",
		ObjectValue: "phishing-infra",
		Confidence:  0.95,
		ActorType:   "human",
		ActorID:     "analyst-k",
	})
	if err != nil {
		t.Fatal(err)
	}

	// One retracted fact on a sibling predicate: distinct sort key from f1/f2,
	// but the retraction replacement shares ITS key, so keep updated_at waves
	// strictly ordered for a deterministic FINAL survivor.
	hosting, err := s.AssertFact(ctx, FactInput{
		Scope:       scope,
		SubjectID:   subj.EntityID,
		Predicate:   "hosting",
		ObjectValue: "bulletproof",
		Confidence:  0.9,
		ActorType:   "human",
		ActorID:     "analyst-j",
	})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond)
	if _, err := s.RetractFact(ctx, hosting.ID, "false positive", "human", "analyst-k"); err != nil {
		t.Fatal(err)
	}

	// Three observations linked to subj at now-1h, now-2h, now-3h.
	for i, age := range []time.Duration{time.Hour, 2 * time.Hour, 3 * time.Hour} {
		if _, err := s.RecordObservation(ctx, Input{
			Scope:     scope,
			Kind:      "alert",
			ActorType: "agent",
			ActorID:   "sensor-7",
			Ts:        time.Now().Add(-age),
			Content:   fmt.Sprintf("beacon %d seen toward bad.example.com", i),
		}); err != nil {
			t.Fatal(err)
		}
	}

	// Plant an EXPIRED fact directly (bypasses AssertFact): status still
	// 'active' and valid_from in the past, but valid_to already elapsed —
	// only the enrich validity window (valid_to > now) can exclude it.
	// Distinct predicate keeps it its own FINAL survivor, so no updated_at
	// wave ordering is needed.
	expBatch, err := conn.PrepareBatch(ctx,
		"INSERT INTO mem.facts "+
			"(fact_id, scope, subject_id, predicate, object_value, object_id, "+
			"status, confidence, source_obs, written_by, valid_from, valid_to, updated_at)")
	if err != nil {
		t.Fatal(err)
	}
	bornAt := time.Now().UTC().Add(-2 * time.Hour)
	if err := expBatch.Append(
		uuid.New(), scope, uuid.MustParse(subj.EntityID),
		"expired_canary", "must-not-leak",
		nil, string(Active), float32(0.99), uuid.Nil, "sensor-7",
		bornAt, bornAt.Add(time.Hour), bornAt,
	); err != nil {
		t.Fatal(err)
	}
	if err := expBatch.Send(); err != nil {
		t.Fatal(err)
	}

	res, err := s.Enrich(ctx, scope, "bad.example.com", entity.IocDomain)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Found {
		t.Fatal("should be found")
	}
	if res.Entity.EntityID != subj.EntityID || res.Entity.Key != "bad.example.com" ||
		res.Entity.EntityType != entity.IocDomain || res.Entity.Scope != scope {
		t.Fatalf("entity projection wrong: %+v", res.Entity)
	}

	// Exactly ONE open fact: the superseding f2. The superseded f1 (closed
	// by mutation) and the retracted hosting fact are provably excluded.
	if len(res.Facts) != 1 {
		t.Fatalf("facts wrong: %+v", res.Facts)
	}
	fv := res.Facts[0]
	if fv.ID != f2.ID || fv.ObjectValue != "phishing-infra" ||
		fv.Predicate != "verdict_malicious" || fv.Status != Active ||
		fv.Confidence != 0.95 || fv.WrittenBy != "analyst-k" || fv.ValidFrom.IsZero() {
		t.Fatalf("open fact view wrong: %+v", fv)
	}
	if fv.ID == f1.ID {
		t.Fatal("superseded fact leaked into open-fact view")
	}
	for _, f := range res.Facts {
		if f.Predicate == "expired_canary" {
			t.Fatalf("expired fact leaked into open-fact view: %+v", f)
		}
	}

	// Observations newest first.
	if len(res.Observations) != 3 || !res.Observations[0].Ts.After(res.Observations[2].Ts) {
		t.Fatalf("observations order wrong: %+v", res.Observations)
	}
	seenObs := map[string]bool{}
	for _, o := range res.Observations {
		if o.ID == "" {
			t.Fatal("observation view missing id")
		}
		if o.Kind != "alert" {
			t.Errorf("observation kind = %q, want alert", o.Kind)
		}
		if n := utf8.RuneCountInString(o.Excerpt); n > maxExcerptRunes {
			t.Errorf("excerpt longer than %d runes (%d): %q", maxExcerptRunes, n, o.Excerpt)
		}
		if seenObs[o.ID] {
			t.Errorf("duplicate observation %s in view", o.ID)
		}
		seenObs[o.ID] = true
	}

	// No edges exist here: none of this test's facts carries an object_id,
	// and facts without an object endpoint never mint edges.
	if len(res.Neighbors) != 0 {
		t.Fatalf("no neighbors expected without object endpoints: %+v", res.Neighbors)
	}

	// Unknown key: Found=false, no error, empty slices.
	miss, err := s.Enrich(ctx, scope, "never-seen.example.com", entity.IocDomain)
	if err != nil {
		t.Fatalf("unknown key must not error: %v", err)
	}
	if miss.Found {
		t.Fatalf("unknown key reported as found: %+v", miss)
	}
	if len(miss.Facts) != 0 || len(miss.Observations) != 0 || len(miss.Neighbors) != 0 {
		t.Fatalf("miss must carry empty slices: %+v", miss)
	}

	// Scope isolation: same key in another scope → Found=false.
	other := itestScope()
	iso, err := s.Enrich(ctx, other, "bad.example.com", entity.IocDomain)
	if err != nil {
		t.Fatalf("foreign-scope lookup must not error: %v", err)
	}
	if iso.Found || len(iso.Facts) != 0 || len(iso.Observations) != 0 || len(iso.Neighbors) != 0 {
		t.Fatalf("scope isolation broken: %+v", iso)
	}

	// Reads are NOT audited: no audit row may target the enriched entity.
	var nAudit uint64
	if err := conn.QueryRow(ctx,
		"SELECT count() FROM mem.audit WHERE target_id = ?",
		uuid.MustParse(subj.EntityID),
	).Scan(&nAudit); err != nil {
		t.Fatal(err)
	}
	if nAudit != 0 {
		t.Errorf("reads must not be audited; found %d audit rows targeting %s", nAudit, subj.EntityID)
	}
}

// TestEnrichNeighbors raw-inserts mem.edges rows around a hub entity and
// checks direction labeling, physical-row dedupe, deterministic ordering
// and the 50-neighbor cap.
func TestEnrichNeighbors(t *testing.T) {
	conn := itestConn(t)
	ctx := context.Background()
	s := testService(t, conn)

	t.Run("both directions, deduped, sorted", func(t *testing.T) {
		scope := itestScope()
		subj := mustResolveEntity(t, conn, scope, "hub.example.com")
		a := mustResolveEntity(t, conn, scope, "peer-a.example.com")
		b := mustResolveEntity(t, conn, scope, "peer-b.example.com")

		insertEdge(t, ctx, conn, scope, subj.EntityID, a.EntityID, "communicates_with")
		insertEdge(t, ctx, conn, scope, b.EntityID, subj.EntityID, "resolved_to")
		// Physical duplicate of the first edge (MergeTree has no dedup):
		// must collapse on read into one Neighbor.
		insertEdge(t, ctx, conn, scope, subj.EntityID, a.EntityID, "communicates_with")

		res, err := s.Enrich(ctx, scope, "hub.example.com", entity.IocDomain)
		if err != nil {
			t.Fatal(err)
		}
		if !res.Found {
			t.Fatal("hub should be found")
		}
		want := map[string]Neighbor{
			a.EntityID + "|out": {EntityID: a.EntityID, Relation: "communicates_with", Direction: "out"},
			b.EntityID + "|in":  {EntityID: b.EntityID, Relation: "resolved_to", Direction: "in"},
		}
		if len(res.Neighbors) != len(want) {
			t.Fatalf("neighbors = %+v, want exactly %v", res.Neighbors, want)
		}
		got := map[string]Neighbor{}
		for _, n := range res.Neighbors {
			got[n.EntityID+"|"+n.Direction] = n
		}
		for k, w := range want {
			if g, ok := got[k]; !ok || g != w {
				t.Errorf("neighbor %s = %+v, want %+v", k, g, w)
			}
		}
		// Deterministic order: ascending EntityID.
		for i := 1; i < len(res.Neighbors); i++ {
			if res.Neighbors[i-1].EntityID > res.Neighbors[i].EntityID {
				t.Fatalf("neighbors not sorted by EntityID: %+v", res.Neighbors)
			}
		}
	})

	t.Run("capped at 50 deterministically", func(t *testing.T) {
		scope := itestScope()
		hub := mustResolveEntity(t, conn, scope, "fanout.example.com")

		// 60 out-edges to fabricated peer ids (the neighbor query reads
		// mem.edges only; peer entity rows are irrelevant to the cap).
		b, err := conn.PrepareBatch(ctx,
			"INSERT INTO mem.edges "+
				"(edge_id, scope, src_id, dst_id, relation, from_fact, valid_from, valid_to)")
		if err != nil {
			t.Fatal(err)
		}
		hubU, err := uuid.Parse(hub.EntityID)
		if err != nil {
			t.Fatal(err)
		}
		now := time.Now().UTC().Truncate(time.Second)
		for i := 0; i < 60; i++ {
			if err := b.Append(uuid.New(), scope, hubU, uuid.New(),
				"communicates_with", uuid.Nil, now, farFuture); err != nil {
				t.Fatal(err)
			}
		}
		if err := b.Send(); err != nil {
			t.Fatal(err)
		}

		res, err := s.Enrich(ctx, scope, "fanout.example.com", entity.IocDomain)
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Neighbors) != 50 {
			t.Fatalf("neighbors = %d, want capped 50", len(res.Neighbors))
		}
		distinct := map[string]bool{}
		for i, n := range res.Neighbors {
			if n.Direction != "out" {
				t.Errorf("neighbor %d direction = %q, want out", i, n.Direction)
			}
			if distinct[n.EntityID] {
				t.Errorf("duplicate neighbor %s after dedupe", n.EntityID)
			}
			distinct[n.EntityID] = true
			if i > 0 && res.Neighbors[i-1].EntityID > n.EntityID {
				t.Fatalf("neighbors not sorted by EntityID: %+v", res.Neighbors)
			}
		}
	})

	// Closure visibility: an edge minted through the REAL lifecycle writers
	// (AssertFact active-with-object) shows up as a neighbor; once the fact
	// is retracted, the edge is mutate-closed and must vanish from the
	// neighbor view immediately — the validity-window filter, not any
	// cleanup pass, is what hides it.
	t.Run("closed edges invisible to neighbors", func(t *testing.T) {
		scope := itestScope()
		hub := mustResolveEntity(t, conn, scope, "closure-hub.example.com")
		peer := mustResolveEntity(t, conn, scope, "closure-peer.example.com")

		f, err := s.AssertFact(ctx, FactInput{
			Scope:       scope,
			SubjectID:   hub.EntityID,
			Predicate:   "communicates_with",
			ObjectValue: "closure-peer.example.com",
			ObjectID:    peer.EntityID,
			Confidence:  0.9,
			ActorType:   "human",
			ActorID:     "analyst-j",
		})
		if err != nil {
			t.Fatal(err)
		}
		if f.Status != Active {
			t.Fatalf("fact = %s, want active", f.Status)
		}

		res, err := s.Enrich(ctx, scope, "closure-hub.example.com", entity.IocDomain)
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Neighbors) != 1 || res.Neighbors[0].EntityID != peer.EntityID ||
			res.Neighbors[0].Relation != "communicates_with" || res.Neighbors[0].Direction != "out" {
			t.Fatalf("open-edge neighbors = %+v, want exactly the peer out-edge", res.Neighbors)
		}

		if _, err := s.RetractFact(ctx, f.ID, "wrong direction", "human", "analyst-k"); err != nil {
			t.Fatal(err)
		}

		res, err = s.Enrich(ctx, scope, "closure-hub.example.com", entity.IocDomain)
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Neighbors) != 0 {
			t.Fatalf("closed edge leaked into neighbor view: %+v", res.Neighbors)
		}
	})
}
