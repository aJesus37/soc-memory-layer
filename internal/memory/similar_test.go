package memory

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

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

// uniqueToken returns a lowercase ASCII token that has never appeared in
// any other test run: under org-wide recall the txt leg spans every scope
// ever written to the shared store, so only never-before-seen tokens make
// token-match membership deterministic.
func uniqueToken(prefix string) string {
	return fmt.Sprintf("%s%x", prefix, time.Now().UnixNano())
}

// txtOnlyService returns a service whose embedder always fails, forcing
// Similar down the txt-only degrade path: recall becomes exact-token
// matching, immune to the vec leg's global top-legDepth crowding by
// foreign rows on a shared store.
func txtOnlyService(t *testing.T, conn driver.Conn) *Service {
	t.Helper()
	s := testService(t, conn)
	s.embedder = failingEmbedder{err: errors.New("embedder down for txt-only recall")}
	return s
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
// Leg expectations:
//
//	txt leg: hasAnyTokens is exact-token and case-sensitive on the CH side
//	         (verified against the live tokenizer, not assumed); the query
//	         tokens {beaconing,detection,analysis} therefore match exactly
//	         A and E ("beaconing") while B's "beacon"/"c2"/"traffic" do NOT.
//	vec leg: org-wide and therefore SHARED with every other scope's
//	         internal rows, so no per-row membership can be assumed — the
//	         global top-legDepth window may be crowded by foreign rows on
//	         a store that outlives this test. Our rows are guaranteed only
//	         through the txt leg: it ranks ts DESC and nothing older than
//	         this run's seeds can outrank them.
//
// Assertions therefore split by origin: hits from OUR scope must be exactly
// the seeded shape (kind, excerpt, matched_by rules); foreign hits — the
// shared-knowledge model surfacing other scopes' internal observations —
// must merely carry attribution and legal matched_by entries. Ordering and
// k stay globally assertable.
//
// A second phase seeds a separate scope through a failing embedder (all
// content_vec empty) and queries it with the WORKING embedder: rows from
// that scope must surface text-only (the vec leg excludes empty vectors),
// proving the empty-vec-scope edge degrades to text-only results rather
// than erroring.
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

	hits, err := s.Similar(ctx, scope, query, 50)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) == 0 {
		t.Fatal("org-wide hybrid recall returned no hits")
	}

	wantTxt := map[string]bool{"A": true, "E": true}
	gotTxtHits := 0
	seen := map[string]bool{}
	prevScore := math.Inf(1)
	var prevID string
	for i, h := range hits {
		if len(h.MatchedBy) == 0 {
			t.Errorf("hit %d has empty matched_by", i)
		}
		for _, src := range h.MatchedBy {
			if src != "vec" && src != "txt" {
				t.Errorf("hit %d unexpected matched_by entry %q", i, src)
			}
		}
		if h.Scope == "" {
			t.Errorf("hit %d missing origin attribution", i)
		}
		if h.Score > prevScore || (h.Score == prevScore && prevID != "" && h.ObsID <= prevID) {
			t.Fatalf("ordering broken at %d: score %v after %v (ids %s then %s)",
				i, h.Score, prevScore, prevID, h.ObsID)
		}
		prevScore, prevID = h.Score, h.ObsID

		if h.Scope != scope {
			continue // foreign internal row shared by design; shape checked above
		}
		key, ok := idToKey[h.ObsID]
		if !ok {
			t.Fatalf("unexpected obs %s in OUR scope; union must stay within seeded set", h.ObsID)
		}
		if seen[h.ObsID] {
			t.Errorf("duplicate hit for %s", key)
		}
		seen[h.ObsID] = true

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
		if inTxt {
			gotTxtHits++
		}
	}

	// The token-matching seeds must ALL have surfaced via the txt leg:
	// they are the newest rows matching the query in any scope.
	if gotTxtHits != len(wantTxt) {
		t.Fatalf("token-matching seeds surfaced %d/%d (%+v)", gotTxtHits, len(wantTxt), hits)
	}

	// k is respected and paginates the same fused stream: a smaller k is a
	// prefix of the larger result regardless of which scopes feed the legs.
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
			t.Errorf("k=2 result %s not among k=50 results", h.ObsID)
		}
	}

	// Phase 2: scope whose rows were ALL written through a failing embedder
	// (empty content_vec). The query embedding succeeds, so the vec leg runs
	// but cannot admit these rows (length(content_vec) > 0 gate); whatever
	// surfaces FROM that scope must be text-only — recall degraded without
	// error. Foreign hits may carry the vec source and are skipped here.
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
	foundDegraded := 0
	for _, h := range degradedHits {
		if h.Scope != scopeNoVec {
			continue // foreign row; may legitimately carry the vec leg
		}
		if !noVecIDs[h.ObsID] {
			t.Errorf("hit %s outside degraded-scope seeds", h.ObsID)
		}
		sorted := append([]string(nil), h.MatchedBy...)
		sort.Strings(sorted)
		if strings.Join(sorted, ",") != "txt" {
			t.Errorf("degraded-scope hit matched_by = %v, want [txt] only", h.MatchedBy)
		}
		foundDegraded++
	}
	if foundDegraded != len(noVecIDs) {
		t.Fatalf("text-only degradation surfaced %d/%d seeded rows (%+v)", foundDegraded, len(noVecIDs), degradedHits)
	}
}

// TestSimilarDegradesToTextOnly injects a failing embedder: the query can't
// be embedded, so Similar must NOT fail, every hit must come from the text
// leg alone (the vec leg is skipped entirely — true regardless of origin
// scope), and our scope's matching seeds must surface while the non-matching
// local row must not.
func TestSimilarDegradesToTextOnly(t *testing.T) {
	conn := itestConn(t)
	ctx := context.Background()
	s := txtOnlyService(t, conn)
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
	found := 0
	for _, h := range hits {
		if strings.Join(h.MatchedBy, ",") != "txt" {
			t.Errorf("matched_by = %v, want exactly [txt]", h.MatchedBy)
		}
		if h.Scope != scope {
			continue // foreign internal rows share the org-wide txt leg by design
		}
		if h.ObsID == other {
			t.Errorf("non-matching observation %s surfaced via text leg", other)
		}
		if !matchIDs[h.ObsID] {
			t.Errorf("hit %s outside seeded matching set", h.ObsID)
		}
		found++
	}
	if found != len(matchIDs) {
		t.Fatalf("our matching seeds surfaced %d/%d (%+v)", found, len(matchIDs), hits)
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
// query AND scope (mirroring RecordObservation), k within [1,50]; there is
// NO default k (Task 15 owns defaulting).
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
	for _, sc := range []string{"", "   "} {
		if _, err := s.Similar(ctx, sc, "beaconing", 10); err == nil {
			t.Errorf("scope %q: expected validation error, got nil", sc)
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
//
// The vec leg is org-wide, so on a shared store the global top-legDepth
// window can be crowded by foreign rows and our seeds are NOT guaranteed to
// surface — membership is therefore not asserted. What stays pinned: every
// hit (ours or foreign) carries [vec] alone, and among adjacent hits that
// ARE our seeded contents the fused order follows the recomputed cosine
// ranking (ascending distance), which is exactly what the vec leg ranks by
// before RRF turns it into a 1/(rrfK+rank) score sequence.
func TestSimilarNoTokens(t *testing.T) {
	conn := itestConn(t)
	ctx := context.Background()
	s := testService(t, conn)
	scope := itestScope()

	texts := []string{
		"some ordinary english content",
		"another plain note about backups",
		"dns resolver latency spike on edge-03",
		"quarterly access review checklist",
	}
	for _, c := range texts {
		seedObs(t, s, scope, "hunt_finding", "human", c)
	}

	query := "你好世界！！！"
	hits, err := s.Similar(ctx, scope, query, 10)
	if err != nil {
		t.Fatalf("CJK-only query must not error: %v", err)
	}
	for _, h := range hits {
		if strings.Join(h.MatchedBy, ",") != "vec" {
			t.Errorf("no-token query hit matched_by = %v, want [vec] only", h.MatchedBy)
		}
	}

	// Recompute the fake embedder's distances locally and demand the fused
	// order agrees across ADJACENT hits of ours. Foreign rows interleave at
	// their own distances, so only consecutive our-row pairs are comparable;
	// equal recomputed distances are tolerated in any order because CH may
	// rank equal-distance ties arbitrarily inside the window function.
	fake := embed.NewFake(8)
	docVecs, err := fake.Embed(ctx, "document", texts)
	if err != nil {
		t.Fatal(err)
	}
	qVecs, err := fake.Embed(ctx, "query", []string{query})
	if err != nil {
		t.Fatal(err)
	}
	distOf := map[string]float64{}
	for i, c := range texts {
		distOf[c] = cosineDist(docVecs[i], qVecs[0])
	}
	prevD := math.Inf(-1)
	prevOurs := false
	for _, h := range hits {
		d, ok := distOf[h.Excerpt]
		if !ok {
			prevOurs = false
			continue // foreign excerpt
		}
		if prevOurs && d < prevD {
			t.Errorf("distance %v breaks ascending recomputed ranking (prev %v)", d, prevD)
		}
		prevD = d
		prevOurs = true
	}
}

// TestSimilarDedupesRetriedObsID pins the retry-duplicate contract: two
// physical rows sharing one obs_id (same ClientEventID submitted twice,
// storage does not collapse them) must fuse to exactly ONE hit whose
// matched_by carries each source at most once and whose score counts each
// leg once. Org-wide recall means foreign hits may share the result window,
// so everything is asserted on the eventID hit alone; its score formula is
// checked against whichever legs actually admitted it (the txt leg is
// guaranteed — the retried row is the newest match for its token — while
// the org-wide vec window may crowd it out on a shared store).
func TestSimilarDedupesRetriedObsID(t *testing.T) {
	conn := itestConn(t)
	ctx := context.Background()
	s := testService(t, conn)
	scope := itestScope()

	const content = "ERROR beaconing detected from 9.9.9.9"
	eventID := uuid.NewString()
	for i := 0; i < 2; i++ { // ClientEventID retry -> second physical row, same obs_id
		o, err := s.RecordObservation(ctx, Input{
			Scope:         scope,
			Kind:          "alert",
			ActorType:     "agent",
			ActorID:       "sensor-7",
			ClientEventID: eventID,
			Content:       content,
		})
		if err != nil {
			t.Fatal(err)
		}
		if o.ID != eventID {
			t.Fatalf("retry %d: obs id %q, want canonical %q", i, o.ID, eventID)
		}
	}

	hits, err := s.Similar(ctx, scope, "beaconing detection analysis", 50)
	if err != nil {
		t.Fatal(err)
	}
	var (
		dupes int
		hit   SearchHit
	)
	for _, h := range hits {
		if h.ObsID != eventID {
			continue // foreign row sharing the org-wide window
		}
		dupes++
		hit = h
	}
	if dupes != 1 {
		t.Fatalf("hits for duplicated obs_id = %d (%+v), want exactly ONE", dupes, hits)
	}
	if hit.Excerpt != content {
		t.Errorf("excerpt = %q, want %q", hit.Excerpt, content)
	}
	sorted := append([]string(nil), hit.MatchedBy...)
	sort.Strings(sorted)
	for i := 1; i < len(sorted); i++ {
		if sorted[i] == sorted[i-1] {
			t.Errorf("matched_by = %v repeats source %q; each leg must count once", hit.MatchedBy, sorted[i])
		}
	}
	var wantScore float64
	switch strings.Join(sorted, ",") {
	case "txt,vec":
		wantScore = 2.0 / float64(rrfK+1)
	case "txt", "vec":
		wantScore = 1.0 / float64(rrfK+1)
	default:
		t.Fatalf("matched_by = %v outside the legal single/dual-leg shapes", hit.MatchedBy)
	}
	if math.Abs(hit.Score-wantScore) > 1e-9 {
		t.Errorf("score = %v, want %v (each contributing leg counted exactly once)", hit.Score, wantScore)
	}
}

// TestSimilarSQLAssembly is a pure unit test (no ClickHouse) guarding the
// leg-fragment SQL: the visibility gate, the vec dimension guard and the
// rank LIMITs must survive any edit, since recall correctness rests on them
// (the fusion itself lives in Go now — see similar.go for why).
func TestSimilarSQLAssembly(t *testing.T) {
	// Both legs carry the org-wide visibility gate...
	for _, sql := range []string{vecLegSQL, txtLegSQL} {
		if n := strings.Count(sql, "(scope = ? OR confidentiality = 'internal')"); n != 1 {
			t.Errorf("leg %q carries %d visibility gates, want exactly 1", sql, n)
		}
	}
	// ...the txt leg probes through hasAnyTokens ranked by recency...
	if !strings.Contains(txtLegSQL, "hasAnyTokens(content, ?)") ||
		!strings.Contains(txtLegSQL, "ORDER BY ts DESC") {
		t.Errorf("txt leg lost its token probe or recency ranking: %q", txtLegSQL)
	}
	// ...and the vec leg guards its array dimension before ranking.
	if !strings.Contains(vecLegSQL, "length(content_vec) = ?") ||
		!strings.Contains(vecLegSQL, "ORDER BY cosineDistance(content_vec, ?)") {
		t.Errorf("vec leg lost its dimension guard or distance ranking: %q", vecLegSQL)
	}

	// Placeholder/argument symmetry per fragment (static shapes now).
	for name, want := range map[string]int{
		vecLegSQL: 3, // scope gate, dim guard, query vector
		txtLegSQL: 2, // scope gate, token array
	} {
		if n := strings.Count(name, "?"); n != want {
			t.Errorf("fragment %q: %d placeholders, want %d", name, n, want)
		}
	}

	// Hydration: gate + one placeholder per id, ids always bound.
	sql := hydrateSQL(4)
	if n := strings.Count(sql, "?"); n != 5 { // gate + 4 ids
		t.Errorf("hydrateSQL(4): %d placeholders, want 5", n)
	}
	if !strings.Contains(sql, "(scope = ? OR confidentiality = 'internal') AND obs_id IN (") {
		t.Errorf("hydration lost its visibility gate: %q", sql)
	}
}

// TestSimilarCrossScopeSharesOrgObs pins org-wide recall: team-b records an
// INTERNAL observation mentioning a never-before-seen token X; team-a's
// similar(X) must find it with hit.Scope=team-b (attribution travels), and
// team-b finds the same row labeled as its own. Both queries run through
// txt-only services so the unique token alone decides membership — the vec
// leg's global window would otherwise admit unpredictable foreign rows on
// a shared store.
func TestSimilarCrossScopeSharesOrgObs(t *testing.T) {
	conn := itestConn(t)
	ctx := context.Background()
	s := testService(t, conn)
	teamB := itestScope()
	teamA := itestScope()

	token := uniqueToken("xshare")
	seedObs(t, s, teamB, "hunt_finding", "human",
		"internal sweep notes mention "+token+" repeatedly")

	sa := txtOnlyService(t, conn)
	hits, err := sa.Similar(ctx, teamA, token, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 {
		t.Fatalf("team-a hits = %d (%+v), want exactly team-b's shared observation", len(hits), hits)
	}
	h := hits[0]
	if h.Scope != teamB {
		t.Errorf("cross-scope hit scope = %q, want %q", h.Scope, teamB)
	}
	if !strings.Contains(h.Excerpt, token) {
		t.Errorf("cross-scope hit excerpt %q lacks token %q", h.Excerpt, token)
	}
	if strings.Join(h.MatchedBy, ",") != "txt" {
		t.Errorf("matched_by = %v, want [txt]", h.MatchedBy)
	}

	sb := txtOnlyService(t, conn)
	hitsB, err := sb.Similar(ctx, teamB, token, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(hitsB) != 1 || hitsB[0].ObsID != h.ObsID || hitsB[0].Scope != teamB {
		t.Fatalf("home-team view = %+v, want the same row labeled %q", hitsB, teamB)
	}
}

// TestSimilarRestrictedInvisible enforces the confidentiality gate on
// recall: a RESTRICTED team-b observation mentioning token Y is absent from
// team-a's results but present in team-b's. Txt-only services keep the
// verdict exact: Y matches only this one row anywhere in the org.
func TestSimilarRestrictedInvisible(t *testing.T) {
	conn := itestConn(t)
	ctx := context.Background()
	s := testService(t, conn)
	teamB := itestScope()
	teamA := itestScope()

	tokenY := uniqueToken("yrstr")
	if _, err := s.RecordObservation(ctx, Input{
		Scope:           teamB,
		Kind:            "investigation_note",
		ActorType:       "human",
		ActorID:         "analyst-b",
		Confidentiality: "restricted",
		Content:         "covert handling of " + tokenY + " must stay home",
	}); err != nil {
		t.Fatal(err)
	}

	foreign := txtOnlyService(t, conn)
	hits, err := foreign.Similar(ctx, teamA, tokenY, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 0 {
		t.Fatalf("restricted observation leaked cross-scope: %+v", hits)
	}

	home := txtOnlyService(t, conn)
	hitsB, err := home.Similar(ctx, teamB, tokenY, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(hitsB) != 1 {
		t.Fatalf("home-team hits = %d (%+v), want exactly the restricted observation", len(hitsB), hitsB)
	}
	if hitsB[0].Scope != teamB {
		t.Errorf("hit scope = %q, want %q", hitsB[0].Scope, teamB)
	}
}
