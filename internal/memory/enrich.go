package memory

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"socmem/internal/entity"
)

// maxExcerptRunes is the content prefix carried per observation view.
const maxExcerptRunes = 200

// maxEnrichObservations caps the recent-observations window.
const maxEnrichObservations = 10

// maxEnrichNeighbors caps the 1-hop neighbor fanout.
const maxEnrichNeighbors = 50

// FactView is the read-side projection of one open fact (NOT the write-side
// Fact): only the fields a triage/hunting agent needs, with "" standing in
// for the zero-UUID source_obs sentinel (unknown provenance).
type FactView struct {
	ID          string
	Predicate   string
	ObjectValue string
	Status      Status
	Confidence  float32
	ValidFrom   time.Time
	WrittenBy   string
	SourceObs   string
}

// ObsView is the read-side projection of one observation: metadata plus a
// 200-char content excerpt (never the full body).
type ObsView struct {
	ID      string
	Ts      time.Time
	Kind    string
	Excerpt string // first 200 chars
}

// Neighbor is one 1-hop graph edge touching the enriched entity, in
// whichever direction it points.
type Neighbor struct {
	EntityID  string
	Relation  string
	Direction string // "out" | "in"
}

// EnrichResult is the assembled context for one entity. On a miss every
// field is empty and Found=false with NO error: the caller decides the 404
// semantics.
type EnrichResult struct {
	Found        bool
	Entity       entity.Entity
	Facts        []FactView // open active facts only, confidence DESC
	Observations []ObsView  // newest first, max 10
	Neighbors    []Neighbor // ≤1 hop, both directions, deduped, max 50
}

// Enrich is the primary read path for triage and hunting agents: given a
// scope plus a raw key and entity type, it returns the entity, its open
// active facts (confidence DESC), its ten most recent observations, and up
// to fifty deduplicated 1-hop neighbors.
//
// The key is normalized exactly like the write path (entity.Normalize), so
// callers may pass raw indicator spellings; an unnormalizable key can never
// match a stored entity because writes always store normalized keys, so it
// yields Found=false rather than an error. An empty/whitespace scope is
// likewise an honest miss — nothing was ever written to it.
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
	scopedScope := strings.TrimSpace(scope)

	var (
		entID     uuid.UUID
		firstSeen time.Time
		lastSeen  time.Time
		displayNm string
		attrs     map[string]string
	)
	err = s.conn.QueryRow(ctx,
		"SELECT entity_id, display_name, attrs, first_seen, last_seen "+
			"FROM mem.entities FINAL "+
			"WHERE scope = ? AND entity_type = ? AND key = ?",
		scopedScope, string(entityType), key,
	).Scan(&entID, &displayNm, &attrs, &firstSeen, &lastSeen)
	if err != nil {
		// clickhouse-go signals an empty result with io.EOF on some paths
		// and sql.ErrNoRows on others; both mean "no such entity".
		if errors.Is(err, io.EOF) || errors.Is(err, sql.ErrNoRows) {
			return res, nil
		}
		return EnrichResult{}, fmt.Errorf("memory: enrich lookup %s/%s in scope %q: %w",
			entityType, key, scopedScope, err)
	}
	res.Found = true
	res.Entity = entity.Entity{
		EntityID:    entID.String(),
		Scope:       scopedScope,
		EntityType:  entityType,
		Key:         key,
		DisplayName: displayNm,
		Attrs:       attrs,
		FirstSeen:   firstSeen,
		LastSeen:    lastSeen,
	}

	now := time.Now().UTC()

	// Open facts through the authoritative read path (FINAL + validity
	// window + status='active'): expired, superseded (closed by supersede
	// mutation) and retracted facts are all excluded here. Scope filter is
	// mandatory — subject UUIDs are not trusted to be globally unique.
	factRows, err := s.conn.Query(ctx,
		"SELECT fact_id, predicate, object_value, status, confidence, valid_from, written_by, source_obs "+
			"FROM mem.facts FINAL "+
			"WHERE scope = ? AND subject_id = ? AND status = 'active' "+
			"AND valid_from <= ? AND valid_to > ? "+
			"ORDER BY confidence DESC, fact_id ASC",
		scopedScope, entID, now, now,
	)
	if err != nil {
		return EnrichResult{}, fmt.Errorf("memory: enrich facts %s: %w", entID, err)
	}
	defer factRows.Close()
	for factRows.Next() {
		var (
			v      FactView
			st     string
			srcObs uuid.UUID
		)
		if err := factRows.Scan(&v.ID, &v.Predicate, &v.ObjectValue, &st,
			&v.Confidence, &v.ValidFrom, &v.WrittenBy, &srcObs); err != nil {
			return EnrichResult{}, fmt.Errorf("memory: enrich scan fact %s: %w", entID, err)
		}
		v.Status = Status(st)
		if srcObs != uuid.Nil {
			v.SourceObs = srcObs.String()
		}
		res.Facts = append(res.Facts, v)
	}
	if err := factRows.Err(); err != nil {
		return EnrichResult{}, fmt.Errorf("memory: enrich iterate facts %s: %w", entID, err)
	}

	obsRows, err := s.conn.Query(ctx,
		"SELECT obs_id, ts, kind, substring(content, 1, 200) FROM mem.observations "+
			"WHERE scope = ? AND hasAny(entity_refs, [toUUID(?)]) "+
			"ORDER BY ts DESC LIMIT 10",
		scopedScope, entID,
	)
	if err != nil {
		return EnrichResult{}, fmt.Errorf("memory: enrich observations %s: %w", entID, err)
	}
	defer obsRows.Close()
	for obsRows.Next() {
		var (
			o     ObsView
			obsID uuid.UUID
			kind  string
		)
		if err := obsRows.Scan(&obsID, &o.Ts, &kind, &o.Excerpt); err != nil {
			return EnrichResult{}, fmt.Errorf("memory: enrich scan observation %s: %w", entID, err)
		}
		o.ID = obsID.String()
		o.Kind = kind
		res.Observations = append(res.Observations, o)
	}
	if err := obsRows.Err(); err != nil {
		return EnrichResult{}, fmt.Errorf("memory: enrich iterate observations %s: %w", entID, err)
	}

	edgeRows, err := s.conn.Query(ctx,
		"SELECT dst_id AS nid, relation, 'out' AS dir FROM mem.edges WHERE scope = ? AND src_id = ? "+
			"UNION ALL "+
			"SELECT src_id, relation, 'in' FROM mem.edges WHERE scope = ? AND dst_id = ? "+
			"LIMIT 50",
		scopedScope, entID, scopedScope, entID,
	)
	if err != nil {
		return EnrichResult{}, fmt.Errorf("memory: enrich neighbors %s: %w", entID, err)
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
			return EnrichResult{}, fmt.Errorf("memory: enrich scan neighbor %s: %w", entID, err)
		}
		k := edgeKey{nid: nid, rel: rel, dir: dir}
		if seen[k] {
			continue // MergeTree keeps physical duplicates; collapse on read
		}
		seen[k] = true
		res.Neighbors = append(res.Neighbors, Neighbor{
			EntityID:  nid.String(),
			Relation:  rel,
			Direction: dir,
		})
	}
	if err := edgeRows.Err(); err != nil {
		return EnrichResult{}, fmt.Errorf("memory: enrich iterate neighbors %s: %w", entID, err)
	}
	// Sort then cap so the returned window is deterministic even when the
	// SQL LIMIT cut landed mid-duplicate-run.
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
