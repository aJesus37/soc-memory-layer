package memory

import (
	"context"
	"errors"
	"math"
	"sort"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"socmem/internal/embed"
)

// cosineDist mirrors ClickHouse cosineDistance for the fake embedder's
// L2-normalized vectors: 1 - dot(a,b)/(|a||b|). The fake carries NO
// semantic structure, so expected vec-leg membership MUST be derived from
// these recomputed outputs, never from text intuition.
func cosineDist(a, b []float32) float64 {
	var dot, na, nb float64
	for i := range a {
		x, y := float64(a[i]), float64(b[i])
		dot += x * y
		na += x * x
		nb += y * y
	}
	return 1 - dot/(math.Sqrt(na)*math.Sqrt(nb))
}

// seedObs records one observation through the real write path and fails
// the test on any error.
func seedObs(t *testing.T, s *Service, scope, kind, actorType, content string) string {
	t.Helper()
	o, err := s.RecordObservation(context.Background(), Input{
		Scope:     scope,
		Kind:      kind,
		ActorType: actorType,
		ActorID:   "analyst-j",
		Content:   content,
	})
	if err != nil {
		t.Fatalf("seed %q: %v", content, err)
	}
	return o.ID
}

// TestSimilarHybrid seeds A-E through RecordObservation (the real write
// path), then fuses both legs for a 'beaconing' query.
//
// Leg expectations are derived locally from embed.NewFake(8) outputs:
//
//	txt leg: hasAnyTokens is exact-token and case-sensitive on the CH side
//	         (verified against the live tokenizer, not assumed); the query
//	         tokens {beaconing,detection,analysis} therefore match exactly
//	         A and E ("beaconing") while B's "beacon"/"c2"/"traffic" do NOT.
//	vec leg: legDepth=20 exceeds the five seeded rows with non-empty
//	         content_vec, so every seeded observation gets a vec rank; the
//	         ranks themselves come from recomputed fake embeddings.
//
// A second phase seeds a separate scope through a failing embedder (all
// content_vec empty) and queries it with the WORKING embedder: the vec leg
// runs but matches nothing, proving the empty-vec-scope edge degrades to
// text-only results rather than erroring.
func TestSimilarHybrid(t *testing.T) {
	conn := itestConn(t)
	ctx := context.Background()
	s := testService(t, conn)
	scope := itestScope()

	texts := map[string]string{
		"A": "ERROR beaconing detected from 9.9.9.9",
		"B": "c2 beacon traffic observed on host web-01",
		"C": "unrelated note about office pizza party",
		"D": "nightly backup completed successfully",
		"E": "analyst confirmed beaconing pattern benign",
	}
	order := []string{"A", "B", "C", "D", "E"}
	ids := map[string]string{}
	for _, key := range order {
		ids[key] = seedObs(t, s, scope, "alert", "agent", texts[key])
	}
	idToKey := map[string]string{}
	for key, id := range ids {
		idToKey[id] = key
	}

	const query = "beaconing detection analysis"

	// Derive vec-leg membership from the fake embedder itself: documents
	// were embedded as kind="document", the query is embedded as
	// kind="query" by Similar. Rank ascending by distance.
	fake := embed.NewFake(8)
	docVecs, err := fake.Embed(ctx, "document", []string{
		texts["A"], texts["B"], texts["C"], texts["D"], texts["E"],
	})
	if err != nil {
		t.Fatal(err)
	}
	qVecs, err := fake.Embed(ctx, "query", []string{query})
	if err != nil {
		t.Fatal(err)
	}
	type dist struct {
		key string
		d   float64
	}
	dists := make([]dist, 0, len(order))
	for i, key := range order {
		dists = append(dists, dist{key, cosineDist(docVecs[i], qVecs[0])})
	}
	sort.Slice(dists, func(i, j int) bool {
		if dists[i].d != dists[j].d {
			return dists[i].d < dists[j].d
		}
		return dists[i].key < dists[j].key
	})
	vecRank := map[string]int{}
	for i, d := range dists {
		vecRank[d.key] = i + 1 // row_number() OVER (...) starts at 1
	}

	hits, err := s.Similar(ctx, scope, query, 5)
	if err != nil {
		t.Fatal(err)
	}

	// All five rows carry a non-empty content_vec and legDepth=20 > 5, so
	// every seeded observation must surface with its RRF score.
	if len(hits) != 5 {
		t.Fatalf("hits = %d (%+v), want all 5 seeded observations", len(hits), hits)
	}

	seen := map[string]bool{}
	for _, h := range hits {
		key, ok := idToKey[h.ObsID]
		if !ok {
			t.Fatalf("unexpected obs %s in results; union must stay within seeded set", h.ObsID)
		}
		if seen[h.ObsID] {
			t.Errorf("duplicate hit for %s", h.ObsID)
		}
		seen[h.ObsID] = true

		if h.Scope != scope {
			t.Errorf("hit %s scope = %q, want %q", key, h.Scope, scope)
		}
		if h.Kind != "alert" {
			t.Errorf("hit %s kind = %q, want alert", key, h.Kind)
		}
		if n := utf8.RuneCountInString(h.Excerpt); n > maxExcerptRunes {
			t.Errorf("hit %s excerpt longer than %d runes: %d", key, maxExcerptRunes, n)
		}
		if want := texts[key]; h.Excerpt != want {
			t.Errorf("hit %s excerpt = %q, want full short content %q", key, h.Excerpt, want)
		}
		if d := h.Ts.Sub(time.Now()); d < -time.Hour || d > time.Hour {
			t.Errorf("hit %s ts outside now±1h: %v", key, h.Ts)
		}
		if len(h.MatchedBy) == 0 {
			t.Errorf("hit %s has empty matched_by", key)
		}
		for _, src := range h.MatchedBy {
			if src != "vec" && src != "txt" {
				t.Errorf("hit %s unexpected matched_by entry %q", key, src)
			}
		}

		inTxt := strings.Contains(texts[key], "beaconing")
		gotTxt := false
		for _, src := range h.MatchedBy {
			if src == "txt" {
				gotTxt = true
			}
		}
		if gotTxt != inTxt {
			t.Errorf("hit %s matched_by=%v txt membership=%t, want %t (token presence)", key, h.MatchedBy, gotTxt, inTxt)
		}
		// Every txt-matched excerpt must actually contain 'beaconing'
		// (case-insensitive check per contract).
		if gotTxt && !strings.Contains(strings.ToLower(h.Excerpt), "beaconing") {
			t.Errorf("txt-matched hit %s excerpt lacks 'beaconing': %q", key, h.Excerpt)
		}

		wantMatched := []string{"vec"}
		if inTxt {
			wantMatched = []string{"txt", "vec"}
		}
		sorted := append([]string(nil), h.MatchedBy...)
		sort.Strings(sorted)
		if strings.Join(sorted, ",") != strings.Join(wantMatched, ",") {
			t.Errorf("hit %s matched_by = %v, want %v (both legs cover all seeded rows here)", key, sorted, wantMatched)
		}
	}

	// Both-leg hits must report exactly {"txt","vec"} after sorting.
	for _, h := range hits {
		key := idToKey[h.ObsID]
		if strings.Contains(texts[key], "beaconing") {
			sorted := append([]string(nil), h.MatchedBy...)
			sort.Strings(sorted)
			if strings.Join(sorted, ",") != "txt,vec" {
				t.Errorf("overlap hit %s matched_by = %v, want [txt vec]", key, h.MatchedBy)
			}
		}
	}

	// Scores strictly non-increasing; equal scores tie-break deterministically
	// by ObsID ascending (RRF sums can legitimately tie across legs).
	for i := 1; i < len(hits); i++ {
		if hits[i-1].Score < hits[i].Score {
			t.Fatalf("scores not descending: [%d]=%v > [%d]=%v violated", i-1, hits[i-1].Score, i, hits[i].Score)
		}
		if hits[i-1].Score == hits[i].Score && hits[i-1].ObsID >= hits[i].ObsID {
			t.Fatalf("equal scores must tie-break by ObsID asc: %s then %s", hits[i-1].ObsID, hits[i].ObsID)
		}
	}

	// k is respected.
	fewer, err := s.Similar(ctx, scope, query, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(fewer) != 2 {
		t.Fatalf("k=2 returned %d hits, want exactly 2", len(fewer))
	}
	subset := map[string]bool{}
	for _, h := range hits {
		subset[h.ObsID] = true
	}
	for _, h := range fewer {
		if !subset[h.ObsID] {
			t.Errorf("k=2 result %s not among k=5 results", h.ObsID)
		}
	}

	// Phase 2: scope whose rows were ALL written through a failing embedder
	// (empty content_vec). The query embedding succeeds, so the vec leg runs
	// but must match zero rows; recall degrades to text-only without error.
	scopeNoVec := itestScope()
	sf := testService(t, conn)
	sf.embedder = failingEmbedder{err: errors.New("embedder down during seeding")}
	noVecIDs := map[string]bool{}
	for _, c := range []string{
		"beaconing seen again from 10.1.1.1",
		"second beaconing sighting on dmz host",
	} {
		noVecIDs[seedObs(t, sf, scopeNoVec, "hunt_finding", "human", c)] = true
	}
	degradedHits, err := s.Similar(ctx, scopeNoVec, query, 10)
	if err != nil {
		t.Fatalf("all-empty content_vec scope must not error: %v", err)
	}
	if len(degradedHits) == 0 {
		t.Fatal("expected text-leg results despite empty vec leg")
	}
	for _, h := range degradedHits {
		if !noVecIDs[h.ObsID] {
			t.Errorf("hit %s outside degraded-scope seeds", h.ObsID)
		}
		sorted := append([]string(nil), h.MatchedBy...)
		sort.Strings(sorted)
		if strings.Join(sorted, ",") != "txt" {
			t.Errorf("degraded-scope hit matched_by = %v, want [txt] only", h.MatchedBy)
		}
	}
}

// TestSimilarDegradesToTextOnly injects a failing embedder: the query can't
// be embedded, so Similar must NOT fail and every hit must come from the
// text leg alone.
func TestSimilarDegradesToTextOnly(t *testing.T) {
	conn := itestConn(t)
	ctx := context.Background()
	s := testService(t, conn)
	s.embedder = failingEmbedder{err: errors.New("embedder down")}
	scope := itestScope()

	matchIDs := map[string]bool{}
	matchIDs[seedObs(t, s, scope, "alert", "agent", "beaconing burst from 10.9.9.9")] = true
	matchIDs[seedObs(t, s, scope, "alert", "agent", "clean beaconing summary note")] = true
	other := seedObs(t, s, scope, "alert", "agent", "totally unrelated pizza lunch")

	hits, err := s.Similar(ctx, scope, "beaconing detection analysis", 10)
	if err != nil {
		t.Fatalf("embedder failure must degrade, not error: %v", err)
	}
	if len(hits) == 0 {
		t.Fatal("text-only degradation returned no hits")
	}
	for _, h := range hits {
		if h.ObsID == other {
			t.Errorf("non-matching observation %s surfaced via text leg", other)
		}
		if !matchIDs[h.ObsID] {
			t.Errorf("hit %s outside seeded matching set", h.ObsID)
		}
		if strings.Join(h.MatchedBy, ",") != "txt" {
			t.Errorf("matched_by = %v, want exactly [txt]", h.MatchedBy)
		}
	}

	// Both legs lost: embedder down AND a token-free query yields an empty
	// result without erroring or touching ClickHouse.
	empty, err := s.Similar(ctx, scope, "你好世界！！！", 10)
	if err != nil {
		t.Fatalf("both legs unavailable must not error: %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("both legs unavailable returned %+v, want empty", empty)
	}
}

// TestSimilarValidation pins the caller-side contract: trimmed-non-empty
// query, k within [1,50]; there is NO default k (Task 15 owns defaulting).
func TestSimilarValidation(t *testing.T) {
	conn := itestConn(t)
	ctx := context.Background()
	s := testService(t, conn)
	scope := itestScope()

	for _, q := range []string{"", "   \t "} {
		if _, err := s.Similar(ctx, scope, q, 10); err == nil {
			t.Errorf("query %q: expected validation error, got nil", q)
		}
	}
	for _, k := range []int{0, -3, 51} {
		if _, err := s.Similar(ctx, scope, "beaconing", k); err == nil {
			t.Errorf("k=%d: expected validation error, got nil", k)
		}
	}
}

// TestSimilarNoTokens queries pure punctuation/CJK: no ASCII-alphanumeric
// tokens exist, so the text leg must be skipped cleanly (never even
// attempted) and results may only carry the vec source.
func TestSimilarNoTokens(t *testing.T) {
	conn := itestConn(t)
	ctx := context.Background()
	s := testService(t, conn)
	scope := itestScope()

	seedObs(t, s, scope, "hunt_finding", "human", "some ordinary english content")

	hits, err := s.Similar(ctx, scope, "你好世界！！！", 10)
	if err != nil {
		t.Fatalf("CJK-only query must not error: %v", err)
	}
	for _, h := range hits {
		for _, src := range h.MatchedBy {
			if src == "txt" {
				t.Errorf("no-token query produced txt match: %+v", h)
			}
		}
	}
}
