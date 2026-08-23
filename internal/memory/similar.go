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

// The three leg-combination variants of the fusion query. Which variant
// runs depends on two independent facts: did the query embed, and does the
// query carry any indexable tokens? A missing fact skips that leg entirely
// rather than running a degenerate one:
//
//   - hybrid: both legs fuse (normal path)
//   - vec-only: zero tokens (e.g. CJK-only query); hasAnyTokens with an
//     empty array matches nothing, so probing the text index would be waste
//   - txt-only: embedder failed; mirrors RecordObservation's write-side
//     degrade so recall survives an embedding outage
//
// Constants bind via %d (ints, injection-safe); all user-controlled values
// are server-bound ? parameters. ORDER BY score DESC breaks ties on obs_id
// ASC because distinct observations can tie on RRF sums across legs; the
// tie-break makes pagination deterministic.
var (
	similarHybridSQL = fmt.Sprintf(
		"WITH vec_leg AS ("+
			"SELECT obs_id, row_number() OVER (ORDER BY cosineDistance(content_vec, ?)) AS rnk "+
			"FROM mem.observations WHERE scope = ? AND length(content_vec) > 0 LIMIT %d), "+
			"txt_leg AS ("+
			"SELECT obs_id, row_number() OVER () AS rnk FROM mem.observations "+
			"WHERE scope = ? AND hasAnyTokens(content, ?) LIMIT %d) "+
			"SELECT o.obs_id, o.scope, o.ts, o.kind, substringUTF8(o.content, 1, %d) AS excerpt, "+
			"sum(1.0 / (%d + f.rnk)) AS score, groupArray(f.src) AS matched_by "+
			"FROM (SELECT obs_id, 'vec' AS src, rnk FROM vec_leg "+
			"UNION ALL "+
			"SELECT obs_id, 'txt' AS src, rnk FROM txt_leg) f "+
			"JOIN mem.observations o ON o.obs_id = f.obs_id AND o.scope = ? "+
			"GROUP BY o.obs_id, o.scope, o.ts, o.kind, excerpt "+
			"ORDER BY score DESC, o.obs_id ASC LIMIT ?",
		legDepth, legDepth, maxExcerptRunes, rrfK)

	similarVecOnlySQL = fmt.Sprintf(
		"WITH vec_leg AS ("+
			"SELECT obs_id, row_number() OVER (ORDER BY cosineDistance(content_vec, ?)) AS rnk "+
			"FROM mem.observations WHERE scope = ? AND length(content_vec) > 0 LIMIT %d) "+
			"SELECT o.obs_id, o.scope, o.ts, o.kind, substringUTF8(o.content, 1, %d) AS excerpt, "+
			"sum(1.0 / (%d + f.rnk)) AS score, groupArray(f.src) AS matched_by "+
			"FROM (SELECT obs_id, 'vec' AS src, rnk FROM vec_leg) f "+
			"JOIN mem.observations o ON o.obs_id = f.obs_id AND o.scope = ? "+
			"GROUP BY o.obs_id, o.scope, o.ts, o.kind, excerpt "+
			"ORDER BY score DESC, o.obs_id ASC LIMIT ?",
		legDepth, maxExcerptRunes, rrfK)

	similarTxtOnlySQL = fmt.Sprintf(
		"WITH txt_leg AS ("+
			"SELECT obs_id, row_number() OVER () AS rnk FROM mem.observations "+
			"WHERE scope = ? AND hasAnyTokens(content, ?) LIMIT %d) "+
			"SELECT o.obs_id, o.scope, o.ts, o.kind, substringUTF8(o.content, 1, %d) AS excerpt, "+
			"sum(1.0 / (%d + f.rnk)) AS score, groupArray(f.src) AS matched_by "+
			"FROM (SELECT obs_id, 'txt' AS src, rnk FROM txt_leg) f "+
			"JOIN mem.observations o ON o.obs_id = f.obs_id AND o.scope = ? "+
			"GROUP BY o.obs_id, o.scope, o.ts, o.kind, excerpt "+
			"ORDER BY score DESC, o.obs_id ASC LIMIT ?",
		legDepth, maxExcerptRunes, rrfK)
)

// Similar recalls observations from one scope by fusing two independent
// legs with Reciprocal Rank Fusion, score = Σ 1/(rrfK + rank):
//
//   - vec leg: top-legDepth rows by cosineDistance(content_vec, qvec),
//     where qvec embeds the trimmed query as kind="query";
//   - txt leg: top-legDepth rows whose content shares any token with the
//     query, probed through the ft_idx text index (splitByNonAlpha
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
	if k < similarKMin || k > similarKMax {
		return nil, fmt.Errorf("memory: k %d outside [%d,%d]", k, similarKMin, similarKMax)
	}

	scopedScope := strings.TrimSpace(scope)
	tokens := queryTokens(q)
	qvec, vecOK := s.embedQuery(ctx, q)

	var sqlText string
	args := make([]any, 0, 6)
	switch {
	case vecOK && len(tokens) > 0: // hybrid
		sqlText = similarHybridSQL
		args = append(args, qvec, scopedScope, scopedScope, tokens, scopedScope, k)
	case vecOK: // vec-only: nothing the text index could match
		sqlText = similarVecOnlySQL
		args = append(args, qvec, scopedScope, scopedScope, k)
	case len(tokens) > 0: // txt-only: embedder degraded
		sqlText = similarTxtOnlySQL
		args = append(args, scopedScope, tokens, scopedScope, k)
	default: // embedder down AND nothing for the text index to bite on
		return []SearchHit{}, nil
	}

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
