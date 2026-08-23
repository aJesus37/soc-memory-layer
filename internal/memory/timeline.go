package memory

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

// maxTimelineLimit bounds one Timeline page. The caller supplies limit
// explicitly; anything outside [1, maxTimelineLimit] is a validation error
// rather than a silent clamp, so API-layer defaults stay visible at the
// call site.
const maxTimelineLimit = 500

// timelineSubjectsCap bounds the by-case bridge: how many distinct fact
// subjects are collected from case observations before the facts leg runs.
// A case beyond this cap has entities whose facts silently drop off its
// timeline — acceptable Phase-1 posture given SOC-scale cases carry
// hundreds of observations, and preferable to an unbounded IN list.
const timelineSubjectsCap = 200

// siblingEntitiesCap bounds the org-wide sibling set: how many entity rows
// sharing (entity_type, key) may feed the by-entity IN lists, mirroring
// timelineSubjectsCap. A key seen in more scopes than this silently drops
// the overflow's rows from the timeline — preferable to an unbounded IN
// list at SOC-scale key fanout.
const siblingEntitiesCap = 200

// Event is one reconstructed moment on a Timeline. Facts have no
// ts-of-event other than valid_from, so per the DECISION they surface with
// Ts=valid_from, Kind="fact:<predicate>" and ActorID=written_by — an honest
// union of both sources without schema changes. OriginScope names the scope
// the underlying row lives in: on the org-wide by-entity path it attributes
// foreign events to the team that produced them; on the by-case path every
// event is scope-local, so OriginScope always equals the caller's own scope.
type Event struct {
	ID          string // stable row identity: obs_id or fact_id
	Ts          time.Time
	Source      string // "observation" | "fact"
	Kind        string // observation kind, or "fact:<predicate>"
	ActorID     string // observer actor_id, or fact written_by
	OriginScope string // originating scope (attribution travels with shared knowledge)
	Text        string // 200-rune excerpt for observations; "predicate: object_value" for facts
}

// timelineObsProjection is the observations leg's SELECT list shared by the
// by-entity and by-case paths. substringUTF8 truncates by runes at the SQL
// side, matching maxExcerptRunes; toString(kind) normalizes the Enum8 so
// the UNION unifies cleanly against the facts leg's String columns. The %d
// is bound via fmt (int, injection-safe) like every other LIMIT here. The
// seven columns must stay aligned with the facts legs and scanTimeline.
const timelineObsProjection = "ts, 'observation' AS src, toString(kind) AS kind, " +
	"actor_id AS actor_id, scope AS origin_scope, substringUTF8(content, 1, %d) AS txt"

// Timeline returns events interleaved newest-first across BOTH sources.
// Exactly one of caseID / entityID must be set. The two paths follow
// DIFFERENT visibility models, deliberately:
//
//   - By-ENTITY is org-wide (shared-knowledge model): the subject set is
//     extended across all sibling entities sharing (entity_type, key) in
//     every scope — the same resolution Enrich uses — and each leg admits
//     rows through its visibility gate: observations via
//     `(scope = caller OR confidentiality = 'internal')`, facts via
//     `(scope = caller OR visibility = 'org')`. Foreign events arrive
//     labeled with their OriginScope; restricted observations and
//     scope-visibility facts stay home. An anchor id that exists in NO
//     scope yields an empty timeline without error.
//   - By-CASE stays strictly scope-local: cases are team artifacts — a
//     case_id minted by one team carries no meaning for another team, and
//     sharing case timelines would bridge unrelated investigations. The
//     existing `scope = ?` bindings are kept unchanged on both the subject
//     scan and the observations leg, so everything the bridge reaches is
//     local by construction.
//
// Fact rendering (DECISION): Ts = valid_from, Kind = "fact:<predicate>",
// ActorID = written_by, Text = "predicate: object_value". Retracted facts
// are excluded. A superseded fact's prior version still appears at its
// original valid_from — the timeline reconstructs what was asserted over
// time, not merely current belief.
//
// By-case bridging: facts carry no case_id, so the bridge first collects
// distinct entity refs from EVERY observation matching the case (an
// unpaginated scan by design — pagination happens after the union, so the
// subject set must cover the whole case or early pages could miss facts).
// At SOC scale a case holds hundreds of rows, so the scan is bounded in
// practice; timelineSubjectsCap caps the resulting IN list anyway. An empty
// subject set skips the facts leg entirely.
//
// Ordering is fully deterministic: ts DESC, then source ASC, then text ASC,
// then row id ASC as the final tie-breaker (DateTime second granularity,
// retry-duplicated rows and now cross-scope siblings make full ties
// reachable, and OFFSET pagination requires a total order to avoid
// skipping/duplicating tied rows across pages). LIMIT/OFFSET apply AFTER
// union+order via subquery wrap. Note that offset pagination can shift
// under concurrent inserts between pages — inherent to OFFSET; keyset
// pagination is future work if that bites.
//
// Consistency: both paths read their id sets (siblings / subjects) and the
// union in sequential statements, so a concurrent write can yield a torn
// view (a fact missing its just-written linking observation); each
// statement is individually consistent. Reads are NOT audited, matching
// Enrich: no side effects, provenance already covered by write-path audit.
func (s *Service) Timeline(ctx context.Context, scope, caseID, entityID string, limit, offset int) ([]Event, error) {
	scope = strings.TrimSpace(scope)
	if scope == "" {
		return nil, fmt.Errorf("%w: scope required", ErrInvalidInput)
	}
	caseID = strings.TrimSpace(caseID)
	entityID = strings.TrimSpace(entityID)
	if (caseID == "") == (entityID == "") { // both set, or neither
		return nil, fmt.Errorf("%w: exactly one of case id or entity id required", ErrInvalidInput)
	}
	if limit < 1 || limit > maxTimelineLimit {
		return nil, fmt.Errorf("%w: limit must be within [1, %d], got %d", ErrInvalidInput, maxTimelineLimit, limit)
	}
	if offset < 0 {
		return nil, fmt.Errorf("%w: offset must be >= 0, got %d", ErrInvalidInput, offset)
	}

	if entityID != "" {
		entU, err := uuid.Parse(entityID)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid entity id %q", ErrInvalidInput, entityID)
		}
		// Org-wide sibling extension: each scope resolves its own entity
		// row for a shared key, and observations may reference ANY sibling
		// id, so both legs must span the whole sibling set rather than the
		// caller's local row alone.
		sibs, err := s.siblingEntityIDs(ctx, entU)
		if err != nil {
			return nil, err
		}
		if len(sibs) == 0 {
			// The anchor exists in no scope: neither leg can match.
			return []Event{}, nil
		}
		idPh := strings.TrimSuffix(strings.Repeat("toUUID(?), ", len(sibs)), ", ")
		obsLeg := fmt.Sprintf(
			"SELECT obs_id AS id, "+timelineObsProjection+" FROM mem.observations "+
				"WHERE hasAny(entity_refs, ["+idPh+"]) "+
				"AND (scope = ? OR confidentiality = 'internal')",
			maxExcerptRunes)
		factLeg := "SELECT fact_id AS id, valid_from AS ts, 'fact' AS src, concat('fact:', predicate) AS kind, " +
			"written_by AS actor_id, scope AS origin_scope, concat(predicate, ': ', object_value) AS txt " +
			"FROM mem.facts FINAL WHERE subject_id IN (" + idPh + ") AND status != 'retracted' " +
			"AND (scope = ? OR visibility = 'org')"
		obsArgs := append(uuidBinds(sibs), scope)
		factArgs := append(uuidBinds(sibs), scope)
		return s.timelineUnion(ctx,
			fmt.Sprintf("entity %s scope %q", entityID, scope),
			obsLeg, obsArgs, factLeg, factArgs,
			limit, offset)
	}

	caseU, err := uuid.Parse(caseID)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid case id %q", ErrInvalidInput, caseID)
	}

	subjects, err := s.timelineCaseSubjects(ctx, scope, caseU)
	if err != nil {
		return nil, err
	}
	obsLeg := fmt.Sprintf(
		"SELECT obs_id AS id, "+timelineObsProjection+" FROM mem.observations "+
			"WHERE scope = ? AND case_id = ?",
		maxExcerptRunes)
	obsArgs := []any{scope, caseU}
	if len(subjects) == 0 {
		// No linked entities → nothing can bridge to facts; observations only.
		return s.timelineOrdered(ctx,
			fmt.Sprintf("case %s scope %q", caseID, scope),
			obsLeg, obsArgs, limit, offset)
	}
	ph := strings.TrimSuffix(strings.Repeat("toUUID(?), ", len(subjects)), ", ")
	factLeg := "SELECT fact_id AS id, valid_from AS ts, 'fact' AS src, concat('fact:', predicate) AS kind, " +
		"written_by AS actor_id, scope AS origin_scope, concat(predicate, ': ', object_value) AS txt " +
		"FROM mem.facts FINAL WHERE scope = ? AND subject_id IN (" + ph + ") " +
		"AND status != 'retracted'"
	factArgs := make([]any, 0, len(subjects)+1)
	factArgs = append(factArgs, scope)
	for _, su := range subjects {
		factArgs = append(factArgs, su)
	}
	return s.timelineUnion(ctx,
		fmt.Sprintf("case %s scope %q", caseID, scope),
		obsLeg, obsArgs, factLeg, factArgs, limit, offset)
}

// timelineCaseSubjects collects the distinct entity ids referenced by ALL
// observations of one case, flattened in Go (arrayJoin deliberately
// avoided), deduped, order-stable, capped at timelineSubjectsCap. It stays
// scope-local on purpose: it serves only the by-case path, which is
// strictly scope-local (see Timeline).
func (s *Service) timelineCaseSubjects(ctx context.Context, scope string, caseID uuid.UUID) ([]uuid.UUID, error) {
	rows, err := s.conn.Query(ctx,
		"SELECT DISTINCT entity_refs FROM mem.observations "+
			"WHERE scope = ? AND case_id = ?",
		scope, caseID)
	if err != nil {
		return nil, fmt.Errorf("memory: timeline subject scan case %s: %w", caseID, err)
	}
	defer rows.Close()
	var (
		subs []uuid.UUID
		seen = make(map[uuid.UUID]bool)
	)
	for rows.Next() {
		var refs []uuid.UUID
		if err := rows.Scan(&refs); err != nil {
			return nil, fmt.Errorf("memory: timeline scan subject refs case %s: %w", caseID, err)
		}
		for _, r := range refs {
			if r == uuid.Nil || seen[r] {
				continue
			}
			seen[r] = true
			subs = append(subs, r)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("memory: timeline iterate subject refs case %s: %w", caseID, err)
	}
	// Deterministic cap order: pages must agree on WHICH subjects survive
	// truncation, so sort by canonical string form before slicing.
	sort.Slice(subs, func(i, j int) bool { return subs[i].String() < subs[j].String() })
	if len(subs) > timelineSubjectsCap {
		subs = subs[:timelineSubjectsCap]
	}
	return subs, nil
}

// siblingEntityIDs resolves every entity row sharing (entity_type, key)
// with the anchor id — across ALL scopes, mirroring Enrich's sibling
// resolution. One round trip: a tuple IN subquery first recovers the
// anchor's own (type, key), then matches every row sharing the pair; an
// anchor that exists in no scope therefore yields an empty (not nil-error)
// result. FINAL collapses ReplacingMergeTree versions to one row per
// (scope, type, key); ids are sorted so bind order is deterministic, then
// capped at siblingEntitiesCap.
func (s *Service) siblingEntityIDs(ctx context.Context, anchor uuid.UUID) ([]uuid.UUID, error) {
	rows, err := s.conn.Query(ctx,
		"SELECT entity_id FROM mem.entities FINAL "+
			"WHERE (entity_type, key) IN ("+
			"SELECT entity_type, key FROM mem.entities FINAL WHERE entity_id = ?)",
		anchor)
	if err != nil {
		return nil, fmt.Errorf("memory: timeline sibling lookup %s: %w", anchor, err)
	}
	defer rows.Close()
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("memory: timeline scan sibling %s: %w", anchor, err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("memory: timeline iterate siblings %s: %w", anchor, err)
	}
	// Deterministic cap order: pages must agree on WHICH siblings survive
	// truncation, so sort by canonical string form before slicing.
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })
	if len(ids) > siblingEntitiesCap {
		ids = ids[:siblingEntitiesCap]
	}
	return ids, nil
}

// timelineUnion wraps the two legs in a subquery, orders the merged result
// deterministically and paginates AFTER union+order. Args must arrive
// obs-leg first, facts-leg second, matching leg placement in the UNION.
func (s *Service) timelineUnion(ctx context.Context, label string, obsLeg string, obsArgs []any, factLeg string, factArgs []any, limit, offset int) ([]Event, error) {
	q := "SELECT id, ts, src, kind, actor_id, origin_scope, txt FROM (" +
		obsLeg + " UNION ALL " + factLeg + ")" +
		fmt.Sprintf(" ORDER BY ts DESC, src ASC, txt ASC, id ASC LIMIT %d OFFSET %d", limit, offset)
	args := append(append([]any{}, obsArgs...), factArgs...)
	return s.scanTimeline(ctx, label, q, args)
}

// timelineOrdered paginates a single leg (the entity-less by-case path).
func (s *Service) timelineOrdered(ctx context.Context, label string, leg string, args []any, limit, offset int) ([]Event, error) {
	q := leg + fmt.Sprintf(" ORDER BY ts DESC, src ASC, txt ASC, id ASC LIMIT %d OFFSET %d", limit, offset)
	return s.scanTimeline(ctx, label, q, args)
}

// scanTimeline runs one assembled query and scans its seven-column rows.
// Always returns a non-nil slice: an empty timeline is []Event{}, not nil.
func (s *Service) scanTimeline(ctx context.Context, label string, q string, args []any) ([]Event, error) {
	rows, err := s.conn.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("memory: timeline query %s: %w", label, err)
	}
	defer rows.Close()
	events := []Event{}
	for rows.Next() {
		var e Event
		if err := rows.Scan(&e.ID, &e.Ts, &e.Source, &e.Kind, &e.ActorID, &e.OriginScope, &e.Text); err != nil {
			return nil, fmt.Errorf("memory: timeline scan %s: %w", label, err)
		}
		e.Ts = e.Ts.UTC()
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("memory: timeline iterate %s: %w", label, err)
	}
	return events, nil
}
