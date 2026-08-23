package memory

import (
	"context"
	"fmt"
	"strings"
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
// three observations newest-first, no neighbors yet — handle unknown keys
// as a clean Found=false miss with empty slices and no error, and surface
// the shared-knowledge contract from a foreign scope: org-wide facts,
// internal observations and the entity itself arrive labeled with their
// OriginScope.
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

	// Shared-knowledge contract (flipped from the Phase-1 isolation rule):
	// the same key enriched from a DIFFERENT scope still finds the entity —
	// org-wide by default — with attribution labeling every shared row.
	other := itestScope()
	iso, err := s.Enrich(ctx, other, "bad.example.com", entity.IocDomain)
	if err != nil {
		t.Fatalf("foreign-scope lookup must not error: %v", err)
	}
	if !iso.Found {
		t.Fatal("org-wide sharing broken: foreign enrich missed a stored key")
	}
	if iso.OriginScope != scope || iso.Entity.Scope != scope {
		t.Fatalf("foreign enrich must return the originating entity labeled: origin=%q entity=%+v",
			iso.OriginScope, iso.Entity)
	}
	if len(iso.Facts) != 1 || iso.Facts[0].ID != f2.ID || iso.Facts[0].OriginScope != scope {
		t.Fatalf("org fact not shared cross-scope (or unlabeled): %+v", iso.Facts)
	}
	if len(iso.Observations) != 3 {
		t.Fatalf("internal observations not shared cross-scope: %+v", iso.Observations)
	}
	for _, o := range iso.Observations {
		if o.OriginScope != scope {
			t.Errorf("observation origin = %q, want %q", o.OriginScope, scope)
		}
	}
	if len(iso.Neighbors) != 0 {
		t.Fatalf("no neighbors expected without object endpoints: %+v", iso.Neighbors)
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

// TestCrossScopeEnrichSharesOrgFacts pins the org-wide default: team-a
// asserts an ordinary (visibility="" → 'org') fact on domain X; team-b,
// enriching X without any local presence, must find it — Found=true —
// with the fact visible and labeled OriginScope=team-a, plus team-a's
// internal observation shared the same way.
func TestCrossScopeEnrichSharesOrgFacts(t *testing.T) {
	conn := itestConn(t)
	ctx := context.Background()
	s := testService(t, conn)
	teamA := itestScope()
	teamB := itestScope()
	domain := fmt.Sprintf("shared-%x.example.net", time.Now().UnixNano())

	subjA := mustResolveEntity(t, conn, teamA, domain)
	if _, err := s.AssertFact(ctx, FactInput{
		Scope:       teamA,
		SubjectID:   subjA.EntityID,
		Predicate:   "verdict_malicious",
		ObjectValue: "c2",
		Confidence:  0.9,
		ActorType:   "human",
		ActorID:     "analyst-a",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RecordObservation(ctx, Input{
		Scope:     teamA,
		Kind:      "hunt_finding",
		ActorType: "human",
		ActorID:   "analyst-a",
		Content:   domain + " beaconed every sixty seconds",
	}); err != nil {
		t.Fatal(err)
	}

	// Team-b has never touched this key: no local row exists, so the
	// result is team-a's entity, found via org-wide sharing.
	resB, err := s.Enrich(ctx, teamB, domain, entity.IocDomain)
	if err != nil {
		t.Fatal(err)
	}
	if !resB.Found {
		t.Fatal("org-wide sharing broken: team-b enrich missed team-a's key")
	}
	if resB.OriginScope != teamA || resB.Entity.Scope != teamA || resB.Entity.EntityID != subjA.EntityID {
		t.Fatalf("entity attribution wrong: origin=%q entity=%+v", resB.OriginScope, resB.Entity)
	}
	if len(resB.Facts) != 1 || resB.Facts[0].Predicate != "verdict_malicious" ||
		resB.Facts[0].ObjectValue != "c2" || resB.Facts[0].OriginScope != teamA {
		t.Fatalf("org fact not visible cross-scope (or unlabeled): %+v", resB.Facts)
	}
	if len(resB.Observations) != 1 || resB.Observations[0].OriginScope != teamA ||
		!strings.Contains(resB.Observations[0].Excerpt, domain) {
		t.Fatalf("internal observation not visible cross-scope (or unlabeled): %+v", resB.Observations)
	}

	// The home team sees exactly the same knowledge, labeled with itself.
	resA, err := s.Enrich(ctx, teamA, domain, entity.IocDomain)
	if err != nil {
		t.Fatal(err)
	}
	if !resA.Found || resA.OriginScope != teamA || len(resA.Facts) != 1 ||
		resA.Facts[0].OriginScope != teamA {
		t.Fatalf("home-team view wrong: %+v", resA)
	}
}

// TestRestrictedObsInvisibleCrossScope enforces the confidentiality gate:
// a RESTRICTED observation mentioning X is invisible to another team's
// enrich but visible to its own; an internal sibling observation from the
// same team stays org-visible either way.
func TestRestrictedObsInvisibleCrossScope(t *testing.T) {
	conn := itestConn(t)
	ctx := context.Background()
	s := testService(t, conn)
	teamB := itestScope()
	teamA := itestScope()
	domain := fmt.Sprintf("restricted-%x.example.net", time.Now().UnixNano())

	if _, err := s.RecordObservation(ctx, Input{
		Scope:           teamB,
		Kind:            "investigation_note",
		ActorType:       "human",
		ActorID:         "analyst-b",
		Confidentiality: "restricted",
		Content:         domain + " under covert restricted handling",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RecordObservation(ctx, Input{
		Scope:     teamB,
		Kind:      "alert",
		ActorType: "agent",
		ActorID:   "sensor-7",
		Content:   domain + " seen beaconing openly",
	}); err != nil {
		t.Fatal(err)
	}

	// Cross-team: only the internal observation may surface.
	resA, err := s.Enrich(ctx, teamA, domain, entity.IocDomain)
	if err != nil {
		t.Fatal(err)
	}
	if !resA.Found || resA.OriginScope != teamB {
		t.Fatalf("team-a enrich should find team-b's entity: %+v", resA)
	}
	if len(resA.Observations) != 1 || resA.Observations[0].OriginScope != teamB ||
		!strings.Contains(resA.Observations[0].Excerpt, "openly") {
		t.Fatalf("cross-team observations wrong (restricted leaked or internal lost): %+v",
			resA.Observations)
	}

	// Home team sees both, newest first.
	resB, err := s.Enrich(ctx, teamB, domain, entity.IocDomain)
	if err != nil {
		t.Fatal(err)
	}
	if len(resB.Observations) != 2 {
		t.Fatalf("home-team observations = %d (%+v), want both", len(resB.Observations), resB.Observations)
	}
	for _, o := range resB.Observations {
		if o.OriginScope != teamB {
			t.Errorf("observation origin = %q, want %q", o.OriginScope, teamB)
		}
	}
}

// TestScopeVisibilityFactHidden covers the fact-side opt-out: a
// Visibility="scope" fact is hidden from other teams' enrich while the
// home team sees it; the column round-trips explicitly through the write
// path, survives retraction (replacement preserves it), and the audit
// trail records vis=<v> without ever carrying content.
func TestScopeVisibilityFactHidden(t *testing.T) {
	conn := itestConn(t)
	ctx := context.Background()
	s := testService(t, conn)
	teamA := itestScope()
	teamB := itestScope()
	domain := fmt.Sprintf("scoped-%x.example.net", time.Now().UnixNano())

	subjA := mustResolveEntity(t, conn, teamA, domain)

	// Org-wide control fact on one predicate...
	orgFact, err := s.AssertFact(ctx, FactInput{
		Scope:       teamA,
		SubjectID:   subjA.EntityID,
		Predicate:   "verdict_malicious",
		ObjectValue: "c2",
		Confidence:  0.9,
		ActorType:   "human",
		ActorID:     "analyst-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	// ...and a scope-restricted fact on a sibling predicate.
	scopeFact, err := s.AssertFact(ctx, FactInput{
		Scope:       teamA,
		SubjectID:   subjA.EntityID,
		Predicate:   "internal_owner_note",
		ObjectValue: "handled-by-team-a-only",
		Visibility:  "scope",
		Confidence:  0.8,
		ActorType:   "human",
		ActorID:     "analyst-a",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Storage check: visibility round-trips exactly as asserted, and the
	// empty-Visibility control stored the explicit org default.
	scopeUUID, _ := uuid.Parse(scopeFact.ID)
	var storedVis string
	if err := conn.QueryRow(ctx,
		"SELECT visibility FROM mem.facts WHERE fact_id = ?", scopeUUID,
	).Scan(&storedVis); err != nil {
		t.Fatal(err)
	}
	if storedVis != "scope" {
		t.Fatalf("stored visibility = %q, want scope", storedVis)
	}
	orgUUID, _ := uuid.Parse(orgFact.ID)
	if err := conn.QueryRow(ctx,
		"SELECT visibility FROM mem.facts WHERE fact_id = ?", orgUUID,
	).Scan(&storedVis); err != nil {
		t.Fatal(err)
	}
	if storedVis != "org" {
		t.Fatalf("default stored visibility = %q, want org", storedVis)
	}

	// Cross-team enrich: the org fact crosses, the scope fact does not.
	resB, err := s.Enrich(ctx, teamB, domain, entity.IocDomain)
	if err != nil {
		t.Fatal(err)
	}
	if !resB.Found || resB.OriginScope != teamA {
		t.Fatalf("team-b enrich wrong: %+v", resB)
	}
	if len(resB.Facts) != 1 || resB.Facts[0].Predicate != "verdict_malicious" {
		t.Fatalf("scope-visibility fact leaked across teams: %+v", resB.Facts)
	}
	for _, f := range resB.Facts {
		if f.Predicate == "internal_owner_note" || strings.Contains(f.ObjectValue, "team-a-only") {
			t.Fatalf("scope-visibility fact content leaked: %+v", f)
		}
	}

	// Home team sees both facts.
	resA, err := s.Enrich(ctx, teamA, domain, entity.IocDomain)
	if err != nil {
		t.Fatal(err)
	}
	preds := map[string]bool{}
	for _, f := range resA.Facts {
		preds[f.Predicate] = true
	}
	if !preds["verdict_malicious"] || !preds["internal_owner_note"] {
		t.Fatalf("home team missing its own facts: %+v", resA.Facts)
	}

	// Retraction preserves the original visibility on the replacement row,
	// and the audit summary carries the constant vis=<v> token.
	retracted, err := s.RetractFact(ctx, scopeFact.ID, "cleanup", "human", "analyst-k")
	if err != nil {
		t.Fatal(err)
	}
	retrUUID, _ := uuid.Parse(retracted.ID)
	if err := conn.QueryRow(ctx,
		"SELECT visibility FROM mem.facts WHERE fact_id = ?", retrUUID,
	).Scan(&storedVis); err != nil {
		t.Fatal(err)
	}
	if storedVis != "scope" {
		t.Fatalf("retraction changed visibility to %q, want preserved scope", storedVis)
	}
	_, _, _, summary := queryAudit(t, conn, ctx, retrUUID)
	if !strings.Contains(summary, "vis=scope") {
		t.Errorf("retract audit missing vis=scope token: %q", summary)
	}
	if strings.Contains(summary, "handled-by-team-a-only") {
		t.Errorf("audit payload_summary leaks object value: %q", summary)
	}
}
