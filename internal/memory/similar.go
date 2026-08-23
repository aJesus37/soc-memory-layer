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
// MatchedBy carries the contributing sources ("vec", "txt"), sorted.
type SearchHit struct {
	ObsID     string
	Scope     string
	Ts        time.Time
	Kind      string
	Excerpt   string   // same rune-safe truncation as enrich views
	Score     float64  // RRF score
	MatchedBy []string // sorted subset of {"vec","txt"}
}

// The fusion statement is assembled from single-source fragments: each
// leg and the shared dedup+fusion suffix exist exactly once below, and the
// three run-time variants are combinations built by buildSimilar rather
// than hand-copied statements, so a change to one fragment cannot drift
// between variants. Which legs run depends on two independent facts: did
// the query embed, and does the query carry any indexable tokens? A
// missing fact skips that leg entirely rather than running a degenerate
// one:
//
//   - hybrid: both legs fuse (normal path)
//   - vec-only: zero tokens (e.g. CJK-only query); hasAnyTokens with an
//     empty array matches nothing, so probing the text index would be waste
//   - txt-only: embedder failed; mirrors RecordObservation's write-side
//     degrade so recall survives an embedding outage
//
// Retry duplicates: storage does not collapse repeated ClientEventIDs (see
// Input), so a physical MergeTree duplicate can enter either leg several
// times at several ranks. fused collapses each (obs_id, src) pair to its
// best (lowest) rank BEFORE scoring, and obs folds duplicate rows to one
// deterministic winner, so every observation contributes at most one term
// per leg, matched_by never repeats a source, and one obs_id yields
// exactly one hit.
//
// Rank provenance: the vec leg ranks by ascending cosineDistance; the txt
// leg ranks by ts DESC so its implicit prior favors recent rows (the vec
// ordering already encodes semantic proximity). Both start at 1.
//
// Constants bind via %d (ints, injection-safe); all user-controlled values
// are server-bound ? parameters. ORDER BY score DESC breaks ties on obs_id
// ASC because distinct observations can tie on RRF sums across legs; the
// tie-break makes pagination deterministic.
var (
	vecLegSQL = fmt.Sprintf(
		"vec_leg AS ("+
			"SELECT obs_id, row_number() OVER (ORDER BY cosineDistance(content_vec, ?)) AS rnk "+
			"FROM mem.observations WHERE scope = ? AND length(content_vec) > 0 LIMIT %d)", legDepth)

	txtLegSQL = fmt.Sprintf(
		"txt_leg AS ("+
			"SELECT obs_id, row_number() OVER (ORDER BY ts DESC) AS rnk "+
			"FROM mem.observations WHERE scope = ? AND hasAnyTokens(content, ?) LIMIT %d)", legDepth)

	vecTermSQL = "SELECT obs_id, 'vec' AS src, rnk FROM vec_leg"
	txtTermSQL = "SELECT obs_id, 'txt' AS src, rnk FROM txt_leg"
)

// fusionSQL renders the shared suffix over the given union-of-terms
// fragments: fused dedupes to the best rank per (obs_id, src); obs
// collapses physical retry duplicates sharing an obs_id into one winner
// row — earliest ts, lexicographically smallest content — so the final
// join cannot emit two hits for one ObsID. kind uses any(): retries carry
// identical kinds in practice, and no stable alternative exists without
// inventing an arbitrary total order. The scope aggregate is aliased
// obs_scope (NOT scope): ClickHouse substitutes aliases globally, so a
// bare `scope` in this CTE's WHERE clause would resolve to the aggregate.
func fusionSQL(terms []string) string {
	return fmt.Sprintf(
		"fused AS ("+
			"SELECT obs_id, src, min(rnk) AS rnk FROM (%s) "+
			"GROUP BY obs_id, src), "+
			"obs AS ("+
			"SELECT obs_id, min(scope) AS obs_scope, min(ts) AS ts, any(kind) AS kind, min(content) AS content "+
			"FROM mem.observations WHERE scope = ? AND obs_id IN (SELECT obs_id FROM fused) "+
			"GROUP BY obs_id) "+
			"SELECT o.obs_id, o.obs_scope, o.ts, o.kind, substringUTF8(o.content, 1, %d) AS excerpt, "+
			"sum(1.0 / (%d + f.rnk)) AS score, groupArray(f.src) AS matched_by "+
			"FROM fused f JOIN obs o ON o.obs_id = f.obs_id "+
			"GROUP BY o.obs_id, o.obs_scope, o.ts, o.kind, excerpt "+
			"ORDER BY score DESC, o.obs_id ASC LIMIT ?",
		strings.Join(terms, " UNION ALL "), maxExcerptRunes, rrfK)
}

// buildSimilar assembles the statement and its arguments for whichever
// legs are live. Placeholder order follows SQL text order — each leg's
// bindings are appended exactly where its fragment lands, then the obs
// scope filter and the LIMIT — so args and ? positions can never drift
// apart.
func buildSimilar(scope string, qvec []float32, vecOK bool, tokens []string, k int) (string, []any) {
	var (
		ctes  []string
		terms []string
		args  []any
	)
	if vecOK {
		ctes = append(ctes, vecLegSQL)
		terms = append(terms, vecTermSQL)
		args = append(args, qvec, scope)
	}
	if len(tokens) > 0 {
		ctes = append(ctes, txtLegSQL)
		terms = append(terms, txtTermSQL)
		args = append(args, scope, tokens)
	}
	args = append(args, scope, k) // obs CTE scope filter, then result cap
	return "WITH " + strings.Join(ctes, ", ") + ", " + fusionSQL(terms), args
}

// Similar recalls observations from one scope by fusing two independent
// legs with Reciprocal Rank Fusion, score = Σ 1/(rrfK + rank):
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
// An embedder failure DEGRADES to the txt-only leg (logged, never a caller
// error), mirroring RecordObservation's write-side fallback; a query with
// zero indexable tokens (e.g. pure punctuation or CJK) degrades to the
// vec-only leg; losing both yields an empty result without touching CH.
// Rows written through a degraded write path (empty content_vec) simply
// never enter the vec leg.
//
// Physical retry duplicates sharing an obs_id are collapsed before fusion
// (see the SQL assembly notes above): one obs contributes at most one term
// per leg and surfaces as exactly one hit.
//
// k is the maximum number of hits returned and MUST be in [1,50]; there is
// no default (the API layer owns defaulting). Ordering is deterministic:
// score DESC with obs_id ASC breaking RRF ties.
//
// Consistency: the two legs are read in one query, but not under a shared
// snapshot with anything else; a concurrent insert may appear in one leg
// only. That skew is acceptable for recall (a later call sees it fully).
func (s *Service) Similar(ctx context.Context, scope, query string, k int) ([]SearchHit, error) {
	q := strings.TrimSpace(query)
	if q == "" {
		return nil, fmt.Errorf("memory: query required")
	}
	scopedScope := strings.TrimSpace(scope)
	if scopedScope == "" {
		// Same contract as RecordObservation: scope is mandatory, not a
		// silent miss.
		return nil, fmt.Errorf("memory: scope required")
	}
	if k < similarKMin || k > similarKMax {
		return nil, fmt.Errorf("memory: k %d outside [%d,%d]", k, similarKMin, similarKMax)
	}

	tokens := queryTokens(q)
	qvec, vecOK := s.embedQuery(ctx, q)
	if !vecOK && len(tokens) == 0 { // embedder down AND nothing for the text index to bite on
		return []SearchHit{}, nil
	}

	sqlText, args := buildSimilar(scopedScope, qvec, vecOK, tokens, k)

	hits := []SearchHit{}
	rows, err := s.conn.Query(ctx, sqlText, args...)
	if err != nil {
		return nil, fmt.Errorf("memory: similar query scope %q: %w", scopedScope, err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			h       SearchHit
			obsID   uuid.UUID
			matched []string
		)
		if err := rows.Scan(&obsID, &h.Scope, &h.Ts, &h.Kind, &h.Excerpt, &h.Score, &matched); err != nil {
			return nil, fmt.Errorf("memory: similar scan hit scope %q: %w", scopedScope, err)
		}
		h.ObsID = obsID.String()
		sort.Strings(matched) // deterministic ["txt","vec"] shape regardless of union order
		h.MatchedBy = matched
		hits = append(hits, h)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("memory: similar iterate hits scope %q: %w", scopedScope, err)
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
