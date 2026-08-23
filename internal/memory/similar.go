package memory

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

// rrfK is the Reciprocal Rank Fusion smoothing constant: each leg
// contributes 1/(rrfK + rank), so early ranks are rewarded but no single
// leg can dominate. k=60 is the standard value from the RRF literature.
const rrfK = 60

// legDepth caps how deep each independent leg ranks before fusion. Both
// legs must share it: a deeper vec leg would outvote the txt leg purely by
// contributing more (small) rank terms.
const legDepth = 20

// maxQueryTokens caps how many query tokens feed hasAnyTokens; more tokens
// dilute the OR-match toward "matches anything" and grow the text-index
// probe for no recall gain.
const maxQueryTokens = 8

// similarK bounds the caller-supplied result count. There is deliberately
// NO default: Similar errors on out-of-range k, and defaulting belongs to
// the API layer (Task 15).
const (
	similarKMin = 1
	similarKMax = 50
)

// SearchHit is one fused recall result. Score is the RRF sum over the legs
// where the observation appeared — NOT comparable across scopes or queries.
// MatchedBy carries the contributing sources ("vec", "txt"), sorted. Scope
// is the ORIGINATING scope of the observation: under org-wide recall it
// names the team that produced a foreign hit (attribution always travels
// with shared knowledge), and equals the caller's own scope for local rows.
type SearchHit struct {
	ObsID     string
	Scope     string
	Ts        time.Time
	Kind      string
	Excerpt   string   // same rune-safe truncation as enrich views
	EntityIDs []string // entities the observation references (stable across retries)
	Score     float64  // RRF score
	MatchedBy []string // sorted subset of {"vec","txt"}
}

// Recall runs as THREE simple single-table statements — one probe per live
// leg, then one hydration read — fused in Go, instead of one compound
// statement. Which legs run depends on two independent facts: did the query
// embed, and does the query carry any indexable tokens? A missing fact skips
// that leg entirely rather than running a degenerate one:
//
//   - hybrid: both legs fuse (normal path)
//   - vec-only: zero tokens (e.g. CJK-only query); hasAnyTokens with an
//     empty array matches nothing, so probing the text index would be waste
//   - txt-only: embedder failed; mirrors RecordObservation's write-side
//     degrade so recall survives an embedding outage
//
// WHY split: Phase 1 fused inside one SQL statement (window-ranked CTEs +
// join). Carrying the visibility gate in those array/windowed scans makes
// ClickHouse 26.x intermittently drop rows from the final projection — a
// planner defect reproduced with bound parameters even after restructuring
// (OR-gates, UNION-split branches, subquery wraps all flaked under load).
// Plain top-K scans over one table with the same predicate are stable, the
// legs were never snapshot-consistent with each other anyway, and RRF
// fusion is trivial arithmetic — so the fusion lives in Go now.
//
// Visibility (shared-knowledge model): every probe filters through
// `(scope = caller OR confidentiality = 'internal')` — internal
// observations are org-readable, restricted ones stay visible only inside
// their originating scope.
//
// The vec probes' dimension guard `length(content_vec) = ?` (the query's
// dim, bound) replaces Phase-1's `length > 0`: scope-local reads could
// assume one embedder per scope and therefore uniform dimensions, but an
// org-wide scan meets rows written through OTHER embedders, and
// cosineDistance hard-errors on size mismatch. Rows of any other dimension
// — including degraded empty vectors — simply never enter the leg.
//
// Retry duplicates: storage does not collapse repeated ClientEventIDs (see
// Input), so a physical MergeTree duplicate can enter a leg several times
// at several ranks and the hydration read can return several rows per id.
// Fusion keeps each (obs_id, src) pair's BEST (lowest) rank before scoring,
// hydration folds duplicate rows to one deterministic winner (earliest ts,
// lexicographically smallest content), so every observation contributes at
// most one term per leg, matched_by never repeats a source, and one obs_id
// yields exactly one hit.
//
// Rank provenance: the vec leg ranks by ascending cosineDistance; the txt
// leg ranks by ts DESC so its implicit prior favors recent rows (the vec
// ordering already encodes semantic proximity). Both start at 1.
//
// Constants bind via %d (ints, injection-safe); all user-controlled values
// are server-bound ? parameters. Final order is score DESC breaking ties on
// obs_id ASC because distinct observations can tie on RRF sums across legs;
// the tie-break makes pagination deterministic.
var (
	vecLegSQL = "SELECT obs_id FROM mem.observations " +
		"WHERE (scope = ? OR confidentiality = 'internal') AND length(content_vec) = ? " +
		fmt.Sprintf("ORDER BY cosineDistance(content_vec, ?) LIMIT %d", legDepth)

	txtLegSQL = "SELECT obs_id FROM mem.observations " +
		"WHERE (scope = ? OR confidentiality = 'internal') AND hasAnyTokens(content, ?) " +
		fmt.Sprintf("ORDER BY ts DESC LIMIT %d", legDepth)
)

// hydrateSQL renders the winner-row read for n obs ids. The visibility gate
// repeats here: the legs pre-filter, so only visible ids are requested in
// practice (duplicate obs_id rows carry identical confidentiality), but
// re-reading by id without the gate would admit a hypothetical hidden
// duplicate of a visible id — defense in depth, uniform row contract.
func hydrateSQL(n int) string {
	ph := strings.TrimSuffix(strings.Repeat("?, ", n), ", ")
	return "SELECT obs_id, scope, ts, kind, content, entity_refs FROM mem.observations " +
		"WHERE (scope = ? OR confidentiality = 'internal') AND obs_id IN (" + ph + ")"
}

// truncateRunes is the Go-side twin of the SQL substringUTF8 truncation used
// by the enrich views: a rune-safe content prefix, never split mid-rune.
func truncateRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}

// Similar recalls observations ORG-WIDE by fusing two independent legs
// with Reciprocal Rank Fusion, score = Σ 1/(rrfK + rank):
//
//   - vec leg: top-legDepth rows by cosineDistance(content_vec, qvec),
//     where qvec embeds the trimmed query as kind="query";
//   - txt leg: top-legDepth rows whose content shares any token with the
//     query, ranked ts DESC so the implicit prior favors recent rows,
//     probed through the ft_idx text index (splitByNonAlpha
//     tokenizer, exact whole-token match, case-sensitive on the storage
//     side — the Go side lowercases the query, but content casing must
//     match exactly to hit).
//
// Shared-knowledge visibility model (single-org deployment): recall spans
// every team's INTERNAL observations — each row must pass
// `(scope = caller OR confidentiality = 'internal')`, so restricted
// material stays visible only inside its originating scope. SearchHit.Scope
// attributes every hit to its originating scope, so a foreign hit is always
// distinguishable from local knowledge.
//
// An embedder failure DEGRADES to the txt-only leg (logged, never a caller
// error), mirroring RecordObservation's write-side fallback; a query with
// zero indexable tokens (e.g. pure punctuation or CJK) degrades to the
// vec-only leg; losing both yields an empty result without touching CH.
// Rows written through a degraded write path (empty content_vec) simply
// never enter the vec leg.
//
// Physical retry duplicates sharing an obs_id are collapsed during fusion
// (see the assembly notes above): one obs contributes at most one term per
// leg and surfaces as exactly one hit.
//
// k is the maximum number of hits returned and MUST be in [1,50]; there is
// no default (the API layer owns defaulting). Ordering is deterministic:
// score DESC with obs_id ASC breaking RRF ties.
//
// Consistency: the leg probes and hydration read are separate statements
// without a shared snapshot; a concurrent insert may appear in one leg only,
// or rank but miss hydration until a later call. That skew is acceptable for
// recall (a later call sees it fully).
func (s *Service) Similar(ctx context.Context, scope, query string, k int) ([]SearchHit, error) {
	q := strings.TrimSpace(query)
	if q == "" {
		return nil, fmt.Errorf("%w: query required", ErrInvalidInput)
	}
	scopedScope := strings.TrimSpace(scope)
	if scopedScope == "" {
		// Same contract as RecordObservation: scope is mandatory, not a
		// silent miss.
		return nil, fmt.Errorf("%w: scope required", ErrInvalidInput)
	}
	if k < similarKMin || k > similarKMax {
		return nil, fmt.Errorf("%w: k %d outside [%d,%d]", ErrInvalidInput, k, similarKMin, similarKMax)
	}

	tokens := queryTokens(q)
	qvec, vecOK := s.embedQuery(ctx, q)
	if !vecOK && len(tokens) == 0 { // embedder down AND nothing for the text index to bite on
		return []SearchHit{}, nil
	}

	// Leg probes: each returns its top-legDepth ids already ordered by its
	// rank criterion; the implicit rank is the row position starting at 1.
	type term struct {
		id  uuid.UUID
		src string
		rnk int
	}
	var terms []term

	if vecOK {
		rows, err := s.conn.Query(ctx, vecLegSQL, scopedScope, len(qvec), qvec)
		if err != nil {
			return nil, fmt.Errorf("memory: similar vec leg scope %q: %w", scopedScope, err)
		}
		rnk := 0
		for rows.Next() {
			var id uuid.UUID
			rnk++
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return nil, fmt.Errorf("memory: similar scan vec leg scope %q: %w", scopedScope, err)
			}
			terms = append(terms, term{id: id, src: "vec", rnk: rnk})
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, fmt.Errorf("memory: similar iterate vec leg scope %q: %w", scopedScope, err)
		}
		rows.Close()
	}

	if len(tokens) > 0 {
		rows, err := s.conn.Query(ctx, txtLegSQL, scopedScope, tokens)
		if err != nil {
			return nil, fmt.Errorf("memory: similar txt leg scope %q: %w", scopedScope, err)
		}
		rnk := 0
		for rows.Next() {
			var id uuid.UUID
			rnk++
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return nil, fmt.Errorf("memory: similar scan txt leg scope %q: %w", scopedScope, err)
			}
			terms = append(terms, term{id: id, src: "txt", rnk: rnk})
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, fmt.Errorf("memory: similar iterate txt leg scope %q: %w", scopedScope, err)
		}
		rows.Close()
	}

	if len(terms) == 0 {
		return []SearchHit{}, nil
	}

	// fused: best (lowest) rank per (obs_id, src), then per-id score and
	// sorted source list — the RRF sum over the legs where the id appeared.
	type termKey struct {
		id  uuid.UUID
		src string
	}
	best := make(map[termKey]int, len(terms))
	for _, t := range terms {
		key := termKey{id: t.id, src: t.src}
		if cur, ok := best[key]; !ok || t.rnk < cur {
			best[key] = t.rnk
		}
	}
	scores := make(map[uuid.UUID]float64, len(best))
	matchedBy := make(map[uuid.UUID][]string, len(best))
	for key, rnk := range best {
		scores[key.id] += 1.0 / float64(rrfK+rnk)
		matchedBy[key.id] = append(matchedBy[key.id], key.src)
	}
	for id := range matchedBy {
		sort.Strings(matchedBy[id]) // deterministic ["txt","vec"] shape regardless of probe order
	}

	// Deterministic result order before hydration so the IN list stays
	// capped: score DESC, obs_id ASC breaking RRF ties.
	ids := make([]uuid.UUID, 0, len(scores))
	for id := range scores {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		if scores[ids[i]] != scores[ids[j]] {
			return scores[ids[i]] > scores[ids[j]]
		}
		return ids[i].String() < ids[j].String()
	})
	if len(ids) > k {
		ids = ids[:k]
	}

	// Hydration: one winner row per id. Physical retry duplicates collapse
	// to earliest ts then lexicographically smallest content (a stricter
	// deterministic variant of the Phase-1 folds; differs only for
	// divergent-content retries); kind/refs ride along from that winner.
	args := make([]any, 0, len(ids)+1)
	args = append(args, scopedScope)
	for _, id := range ids {
		args = append(args, id)
	}
	rows, err := s.conn.Query(ctx, hydrateSQL(len(ids)), args...)
	if err != nil {
		return nil, fmt.Errorf("memory: similar hydrate scope %q: %w", scopedScope, err)
	}
	defer rows.Close()
	type winner struct {
		scope   string
		ts      time.Time
		kind    string
		content string
		refs    []uuid.UUID
	}
	winners := make(map[uuid.UUID]*winner, len(ids))
	for rows.Next() {
		var (
			id   uuid.UUID
			w    winner
			kind string
		)
		if err := rows.Scan(&id, &w.scope, &w.ts, &kind, &w.content, &w.refs); err != nil {
			return nil, fmt.Errorf("memory: similar scan hydrated row scope %q: %w", scopedScope, err)
		}
		w.kind = kind
		cur, ok := winners[id]
		if !ok || w.ts.Before(cur.ts) ||
			(w.ts.Equal(cur.ts) && w.content < cur.content) {
			winners[id] = &w
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("memory: similar iterate hydrated rows scope %q: %w", scopedScope, err)
	}

	hits := []SearchHit{}
	for _, id := range ids { // preserve score order; skip ids lost between fusion and hydration
		w, ok := winners[id]
		if !ok {
			continue
		}
		h := SearchHit{
			ObsID:     id.String(),
			Scope:     w.scope,
			Ts:        w.ts.UTC(),
			Kind:      w.kind,
			Excerpt:   truncateRunes(w.content, maxExcerptRunes),
			Score:     scores[id],
			MatchedBy: matchedBy[id],
		}
		for _, r := range w.refs {
			if r != uuid.Nil {
				h.EntityIDs = append(h.EntityIDs, r.String())
			}
		}
		hits = append(hits, h)
	}
	return hits, nil
}

// embedQuery embeds the search query as kind="query". ANY failure or odd
// shape degrades to ok=false so Similar falls back to the text-only leg —
// recall must survive an embedding outage just like writes do.
func (s *Service) embedQuery(ctx context.Context, text string) (vec []float32, ok bool) {
	vecs, err := s.embedder.Embed(ctx, "query", []string{text})
	if err != nil {
		s.log.Warn("memory: similar embed failed, falling back to text-only recall", "err", err)
		return nil, false
	}
	if len(vecs) != 1 || len(vecs[0]) == 0 {
		s.log.Warn("memory: similar embed returned unexpected shape, falling back to text-only recall",
			"vectors", len(vecs))
		return nil, false
	}
	return vecs[0], true
}

// queryTokens lowercases the query and splits it into runs of ASCII letters
// and digits, deduplicated in first-appearance order and capped at
// maxQueryTokens. Every other rune (whitespace, punctuation, CJK
// ideographs, …) acts as a separator, so a CJK-only query yields zero
// tokens — matching the storage side, where the splitByNonAlpha tokenizer
// cannot produce such tokens either. Callers skip the txt leg when this
// returns empty.
func queryTokens(query string) []string {
	lower := strings.ToLower(query)
	seen := make(map[string]bool, maxQueryTokens)
	tokens := make([]string, 0, maxQueryTokens)
	cur := make([]rune, 0, 16)
	flush := func() {
		if len(cur) == 0 {
			return
		}
		tok := string(cur)
		cur = cur[:0]
		if seen[tok] || len(tokens) >= maxQueryTokens {
			return
		}
		seen[tok] = true
		tokens = append(tokens, tok)
	}
	for _, r := range lower {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			cur = append(cur, r)
		default:
			flush()
		}
	}
	flush()
	return tokens
}
