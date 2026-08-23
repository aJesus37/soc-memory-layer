package memory

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"socmem/internal/entity"
)

// maxExcerptRunes is the rune-safe content prefix carried per observation
// view; the SQL side truncates via substringUTF8 so the count is runes,
// not bytes.
const maxExcerptRunes = 200

// maxEnrichObservations caps the recent-observations window.
const maxEnrichObservations = 10

// maxEnrichFacts caps the open-facts window: entities accumulating more
// open predicates need lifecycle hygiene (supersede/retract), not bigger
// payloads.
const maxEnrichFacts = 200

// maxEnrichNeighbors caps the 1-hop neighbor fanout after dedupe.
const maxEnrichNeighbors = 50

// maxEnrichNeighborRows is the SQL-side row budget for the neighbor query,
// applied AFTER DISTINCT so physical MergeTree duplicates cannot crowd out
// distinct neighbors below the Go-side cap of maxEnrichNeighbors.
const maxEnrichNeighborRows = 200

// FactView is the read-side projection of one open fact (NOT the write-side
// Fact): only the fields a triage/hunting agent needs, with "" standing in
// for the zero-UUID source_obs sentinel (unknown provenance). OriginScope
// names the team whose scope the fact was asserted in — attribution always
// travels with shared knowledge.
type FactView struct {
	ID          string
	Predicate   string
	ObjectValue string
	Status      Status
	Confidence  float32
	ValidFrom   time.Time
	WrittenBy   string
	SourceObs   string
	OriginScope string
}

// ObsView is the read-side projection of one observation: metadata plus a
// 200-rune content excerpt (never the full body). OriginScope names the
// originating team's scope.
type ObsView struct {
	ID          string
	Ts          time.Time
	Kind        string
	Excerpt     string // first maxExcerptRunes runes
	OriginScope string
}

// Neighbor is one 1-hop graph edge touching the enriched entity or any of
// its cross-scope siblings, in whichever direction it points.
type Neighbor struct {
	EntityID  string
	Relation  string
	Direction string // "out" | "in"
}

// EnrichResult is the assembled context for one entity. On a miss every
// field is empty and Found=false with NO error: the caller decides the 404
// semantics.
type EnrichResult struct {
	Found  bool
	Entity entity.Entity

	// OriginScope is the scope the returned Entity row lives in: the
	// caller's own scope when a local match exists, otherwise the
	// originating scope of the first foreign match.
	OriginScope string

	// Open active facts only, confidence DESC, capped at maxEnrichFacts
	// (200): an entity accumulating more open predicates needs lifecycle
	// hygiene (supersede/retract), not bigger payloads. Includes org-wide
	// facts from other scopes; each carries its own OriginScope.
	Facts []FactView

	Observations []ObsView  // newest first, max 10
	Neighbors    []Neighbor // ≤1 hop, both directions, deduped, max 50
}

// uuidBinds boxes entity ids as bind parameters.
func uuidBinds(ids []uuid.UUID) []any {
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	return args
}

// uuidPlaceholders renders "?, ?, ..." for n bind parameters plus the
// matching arg slice. Every IN list in this file is assembled through it —
// values are always bound, never interpolated.
func uuidPlaceholders(ids []uuid.UUID) (string, []any) {
	return strings.TrimSuffix(strings.Repeat("?, ", len(ids)), ", "), uuidBinds(ids)
}

// Enrich is the primary read path for triage and hunting agents. Given a
// scope plus a raw key and entity type, it returns the entity, its open
// active facts (confidence DESC), its ten most recent observations, and up
// to fifty deduplicated 1-hop neighbors.
//
// Shared-knowledge visibility model (single-org deployment): facts and
// observations are ORG-VISIBLE by default — every team benefits from every
// other team's knowledge. Restricted material stays home:
//
//   - Entities are matched across ALL scopes sharing (entity_type, key).
//     The caller's local entity wins when present; otherwise the first
//     foreign match (deterministic: lexicographically smallest scope) is
//     returned. Found=true when ANY matching entity exists in any scope.
//   - Facts: the subject may be any sibling id; rows must pass the
//     visibility gate `(scope = caller OR visibility = 'org')` on top of
//     status/validity filters. A visibility='scope' fact is readable only
//     inside its originating scope.
//   - Observations: must reference a sibling id and pass
//     `(scope = caller OR confidentiality = 'internal')` — the schema's
//     confidentiality column is finally ENFORCED here: internal =
//     org-readable, restricted = originating-scope-only, newest-first.
//   - Neighbors: the id set spans all sibling entities across scopes, and
//     the edges query deliberately carries NO scope filter: edge
//     EXISTENCE is structural metadata, org-visible by design. Edge
//     CONTENT requires reading facts, which ARE visibility-filtered above.
//
// Attribution always travels: cross-scope FactViews/ObsViews carry
// OriginScope naming the team that produced them, and the result-level
// OriginScope labels the returned Entity's home scope.
//
// The key is normalized exactly like the write path (entity.Normalize), so
// callers may pass raw indicator spellings; an unnormalizable key can never
// match a stored entity because writes always store normalized keys, so it
// yields Found=false rather than an error. An empty/whitespace scope keeps
// the honest-miss contract — nothing was ever written to it, and an
// unscoped caller sees nothing at all.
//
// entityType is the caller's claim about the entity discriminator; writes
// store the type Normalize derives from the key, so a caller-supplied type
// disagreeing with that derived type finds no row and yields a silent miss
// by design (the caller is expected to know what kind of indicator it is
// holding).
//
// Consistency: each section (entity, facts, observations, neighbors) is an
// individually consistent snapshot, but the sections are read sequentially
// without a shared snapshot, so a concurrent supersede between reads can
// yield a torn view across sections.
//
// TODO(p99): run the facts/observations/neighbors queries concurrently via
// errgroup as the first p99 lever; the sibling-entity lookup must still
// precede them to produce the id set.
//
// Neighbors come straight from mem.edges (Phase-1 fallback path). This CH
// query is also the production fallback when the Phase-2 graph store is
// down (design §6): the graph holds only a rebuildable copy of edges, never
// a second source of truth.
//
// Reads are NOT audited, by design: Enrich runs at triage/hunt volume on
// every agent turn, has no side effects, and provenance is already covered
// by the write-path audit trail (every fact/observation write recorded who
// wrote what). Auditing reads would multiply audit volume without adding
// attributable state changes.
func (s *Service) Enrich(ctx context.Context, scope, rawKey string, entityType entity.Type) (EnrichResult, error) {
	res := EnrichResult{
		Facts:        []FactView{},
		Observations: []ObsView{},
		Neighbors:    []Neighbor{},
	}

	n, err := entity.Normalize(rawKey)
	if err != nil {
		// Not entity-shaped ⇒ no row can exist; an honest miss, not an error.
		return res, nil
	}
	key := n.Key
	caller := strings.TrimSpace(scope)
	if caller == "" {
		// Unscoped callers see nothing at all — an honest miss, not an error.
		return res, nil
	}

	// Sibling resolution: every entity row sharing (entity_type, key),
	// across ALL scopes. FINAL resolves one row per (scope, type, key);
	// the caller's own scope may or may not be among them. Dropping the
	// scope predicate gives up the primary-index prefix — an accepted
	// scan at Phase-1/SOC volumes, revisitable with a (type, key) index.
	type entMatch struct {
		id  uuid.UUID
		sc  string
		ent entity.Entity
	}
	var matches []entMatch
	entRows, err := s.conn.Query(ctx,
		"SELECT entity_id, scope, display_name, attrs, first_seen, last_seen "+
			"FROM mem.entities FINAL "+
			"WHERE entity_type = ? AND key = ?",
		string(entityType), key,
	)
	if err != nil {
		return EnrichResult{}, fmt.Errorf("memory: enrich sibling lookup %s/%s: %w",
			entityType, key, err)
	}
	defer entRows.Close()
	for entRows.Next() {
		var (
			m         entMatch
			entID     uuid.UUID
			sc        string
			displayNm string
			attrs     map[string]string
			fs, ls    time.Time
		)
		if err := entRows.Scan(&entID, &sc, &displayNm, &attrs, &fs, &ls); err != nil {
			return EnrichResult{}, fmt.Errorf("memory: enrich scan sibling %s/%s: %w",
				entityType, key, err)
		}
		m.id = entID
		m.sc = sc
		m.ent = entity.Entity{
			EntityID:    entID.String(),
			Scope:       sc,
			EntityType:  entityType,
			Key:         key,
			DisplayName: displayNm,
			Attrs:       attrs,
			FirstSeen:   fs,
			LastSeen:    ls,
		}
		matches = append(matches, m)
	}
	if err := entRows.Err(); err != nil {
		return EnrichResult{}, fmt.Errorf("memory: enrich iterate siblings %s/%s: %w",
			entityType, key, err)
	}
	if len(matches) == 0 {
		return res, nil // honest miss in every scope
	}

	// Deterministic Entity selection: the caller's local row first, then
	// foreign rows by lexicographically smallest scope (entity_id breaks
	// impossible-in-practice FINAL ties).
	sort.Slice(matches, func(i, j int) bool {
		li, lj := matches[i].sc == caller, matches[j].sc == caller
		if li != lj {
			return li
		}
		if matches[i].sc != matches[j].sc {
			return matches[i].sc < matches[j].sc
		}
		return matches[i].id.String() < matches[j].id.String()
	})
	chosen := matches[0]
	res.Found = true
	res.OriginScope = chosen.sc
	res.Entity = chosen.ent

	ids := make([]uuid.UUID, len(matches))
	for i := range matches {
		ids[i] = matches[i].id
	}

	now := time.Now().UTC()

	// Open facts through the authoritative read path (FINAL + validity
	// window + status='active'): expired, superseded (closed by supersede
	// mutation) and retracted facts are all excluded here. The subject may
	// live in any scope sharing the key; the visibility gate admits only
	// the caller's own rows plus org-wide ones. All values bound via ?
	// placeholders; the LIMIT is bound via %d (int, injection-safe).
	factPh, factBind := uuidPlaceholders(ids)
	factRows, err := s.conn.Query(ctx, fmt.Sprintf(
		"SELECT fact_id, scope, predicate, object_value, status, confidence, valid_from, written_by, source_obs "+
			"FROM mem.facts FINAL "+
			"WHERE subject_id IN ("+factPh+") AND status = 'active' "+
			"AND valid_from <= ? AND valid_to > ? "+
			"AND (scope = ? OR visibility = 'org') "+
			"ORDER BY confidence DESC, fact_id ASC LIMIT %d", maxEnrichFacts),
		append(factBind, now, now, caller)...,
	)
	if err != nil {
		return EnrichResult{}, fmt.Errorf("memory: enrich facts %s: %w", chosen.id, err)
	}
	defer factRows.Close()
	for factRows.Next() {
		var (
			v      FactView
			st     string
			srcObs uuid.UUID
		)
		if err := factRows.Scan(&v.ID, &v.OriginScope, &v.Predicate, &v.ObjectValue, &st,
			&v.Confidence, &v.ValidFrom, &v.WrittenBy, &srcObs); err != nil {
			return EnrichResult{}, fmt.Errorf("memory: enrich scan fact %s: %w", chosen.id, err)
		}
		v.Status = Status(st)
		if srcObs != uuid.Nil {
			v.SourceObs = srcObs.String()
		}
		res.Facts = append(res.Facts, v)
	}
	if err := factRows.Err(); err != nil {
		return EnrichResult{}, fmt.Errorf("memory: enrich iterate facts %s: %w", chosen.id, err)
	}

	// substringUTF8 truncates by runes at the SQL side, matching
	// maxExcerptRunes. The confidentiality gate enforces the observation
	// side of the model: internal observations are org-readable;
	// restricted ones stay visible only inside their originating scope.
	// Both LIMITs are bound via %d (int, injection-safe).
	obsArr := strings.TrimSuffix(strings.Repeat("toUUID(?), ", len(ids)), ", ")
	obsRows, err := s.conn.Query(ctx, fmt.Sprintf(
		"SELECT obs_id, scope, ts, kind, substringUTF8(content, 1, %d) FROM mem.observations "+
			"WHERE hasAny(entity_refs, ["+obsArr+"]) "+
			"AND (scope = ? OR confidentiality = 'internal') "+
			"ORDER BY ts DESC LIMIT %d", maxExcerptRunes, maxEnrichObservations),
		append(uuidBinds(ids), caller)...,
	)
	if err != nil {
		return EnrichResult{}, fmt.Errorf("memory: enrich observations %s: %w", chosen.id, err)
	}
	defer obsRows.Close()
	for obsRows.Next() {
		var (
			o     ObsView
			obsID uuid.UUID
			kind  string
		)
		if err := obsRows.Scan(&obsID, &o.OriginScope, &o.Ts, &kind, &o.Excerpt); err != nil {
			return EnrichResult{}, fmt.Errorf("memory: enrich scan observation %s: %w", chosen.id, err)
		}
		o.ID = obsID.String()
		o.Kind = kind
		res.Observations = append(res.Observations, o)
	}
	if err := obsRows.Err(); err != nil {
		return EnrichResult{}, fmt.Errorf("memory: enrich iterate observations %s: %w", chosen.id, err)
	}

	// SQL DISTINCT collapses physical MergeTree duplicates (retry inserts
	// are an accepted pattern) BEFORE LIMIT fires, so raw duplicate rows
	// cannot crowd distinct neighbors out of the window. Both legs filter
	// on the validity window (valid_to > now64(3)): closed edges —
	// superseded or retracted — must vanish from the graph view the moment
	// their lifecycle wave commits, not after some cleanup job. NO scope
	// filter on either leg: edge existence is structural metadata,
	// org-visible by design (edge CONTENT is gated by the fact query
	// above). The budget is maxEnrichNeighborRows (bound via %d,
	// injection-safe), headroom above the Go-side cap of maxEnrichNeighbors
	// below.
	nidPh, nidArgs := uuidPlaceholders(ids)
	edgeRows, err := s.conn.Query(ctx, fmt.Sprintf(
		"SELECT DISTINCT nid, relation, dir FROM ("+
			"SELECT dst_id AS nid, relation, 'out' AS dir FROM mem.edges "+
			"WHERE src_id IN ("+nidPh+") AND valid_to > now64(3) "+
			"UNION ALL "+
			"SELECT src_id, relation, 'in' FROM mem.edges "+
			"WHERE dst_id IN ("+nidPh+") AND valid_to > now64(3)"+
			") LIMIT %d", maxEnrichNeighborRows),
		append(nidArgs, nidArgs...)...,
	)
	if err != nil {
		return EnrichResult{}, fmt.Errorf("memory: enrich neighbors %s: %w", chosen.id, err)
	}
	defer edgeRows.Close()
	type edgeKey struct {
		nid uuid.UUID
		rel string
		dir string
	}
	seen := make(map[edgeKey]bool)
	for edgeRows.Next() {
		var (
			nid uuid.UUID
			rel string
			dir string
		)
		if err := edgeRows.Scan(&nid, &rel, &dir); err != nil {
			return EnrichResult{}, fmt.Errorf("memory: enrich scan neighbor %s: %w", chosen.id, err)
		}
		k := edgeKey{nid: nid, rel: rel, dir: dir}
		if seen[k] {
			continue // defense in depth; SQL DISTINCT already collapsed physical dupes
		}
		seen[k] = true
		res.Neighbors = append(res.Neighbors, Neighbor{
			EntityID:  nid.String(),
			Relation:  rel,
			Direction: dir,
		})
	}
	if err := edgeRows.Err(); err != nil {
		return EnrichResult{}, fmt.Errorf("memory: enrich iterate neighbors %s: %w", chosen.id, err)
	}
	// Sort then cap so the returned window is deterministic. The post-
	// DISTINCT SQL budget (maxEnrichNeighborRows) keeps this complete for
	// realistic fanouts; the API cap remains maxEnrichNeighbors.
	sort.Slice(res.Neighbors, func(i, j int) bool {
		if res.Neighbors[i].EntityID != res.Neighbors[j].EntityID {
			return res.Neighbors[i].EntityID < res.Neighbors[j].EntityID
		}
		if res.Neighbors[i].Relation != res.Neighbors[j].Relation {
			return res.Neighbors[i].Relation < res.Neighbors[j].Relation
		}
		return res.Neighbors[i].Direction < res.Neighbors[j].Direction
	})
	if len(res.Neighbors) > maxEnrichNeighbors {
		res.Neighbors = res.Neighbors[:maxEnrichNeighbors]
	}

	return res, nil
}
