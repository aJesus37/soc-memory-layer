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

// Event is one reconstructed moment on a Timeline. Facts have no
// ts-of-event other than valid_from, so per the DECISION they surface with
// Ts=valid_from, Kind="fact:<predicate>" and ActorID=written_by — an honest
// union of both sources without schema changes.
type Event struct {
	ID      string // stable row identity: obs_id or fact_id
	Ts      time.Time
	Source  string // "observation" | "fact"
	Kind    string // observation kind, or "fact:<predicate>"
	ActorID string // observer actor_id, or fact written_by
	Text    string // 200-rune excerpt for observations; "predicate: object_value" for facts
}

// timelineObsProjection is the observations leg's SELECT list shared by the
// by-entity and by-case paths. substringUTF8 truncates by runes at the SQL
// side, matching maxExcerptRunes; toString(kind) normalizes the Enum8 so
// the UNION unifies cleanly against the facts leg's String columns. The %d
// is bound via fmt (int, injection-safe) like every other LIMIT here.
const timelineObsProjection = "ts, 'observation' AS src, toString(kind) AS kind, " +
	"actor_id AS actor_id, substringUTF8(content, 1, %d) AS txt"

// Timeline returns events interleaved newest-first across BOTH sources:
// observations linked to the entity/case, plus non-retracted facts on the
// same subject(s). Exactly one of caseID / entityID must be set.
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
// then row id ASC as the final tie-breaker (DateTime second granularity and
// retry-duplicated rows make full ties reachable, and OFFSET pagination
// requires a total order to avoid skipping/duplicating tied rows across
// pages). LIMIT/OFFSET apply AFTER union+order via subquery wrap. Note that
// offset pagination can shift under concurrent inserts between pages —
// inherent to OFFSET; keyset pagination is future work if that bites.
//
// Consistency: the by-case path reads observations (for subjects) and the
// union in two sequential statements, so a concurrent write can yield a
// torn view (a fact missing its just-written linking observation); each
// statement is individually consistent. Reads are NOT audited, matching
// Enrich: no side effects, provenance already covered by write-path audit.
func (s *Service) Timeline(ctx context.Context, scope, caseID, entityID string, limit, offset int) ([]Event, error) {
	scope = strings.TrimSpace(scope)
	if scope == "" {
		return nil, fmt.Errorf("memory: scope required")
	}
	caseID = strings.TrimSpace(caseID)
	entityID = strings.TrimSpace(entityID)
	if (caseID == "") == (entityID == "") { // both set, or neither
		return nil, fmt.Errorf("memory: exactly one of case id or entity id required")
	}
	if limit < 1 || limit > maxTimelineLimit {
		return nil, fmt.Errorf("memory: limit must be within [1, %d], got %d", maxTimelineLimit, limit)
	}
	if offset < 0 {
		return nil, fmt.Errorf("memory: offset must be >= 0, got %d", offset)
	}

	if entityID != "" {
		entU, err := uuid.Parse(entityID)
		if err != nil {
			return nil, fmt.Errorf("memory: invalid entity id %q", entityID)
		}
		obsLeg := fmt.Sprintf(
			"SELECT obs_id AS id, "+timelineObsProjection+" FROM mem.observations "+
				"WHERE scope = ? AND hasAny(entity_refs, [toUUID(?)])",
			maxExcerptRunes)
		factLeg := "SELECT fact_id AS id, valid_from AS ts, 'fact' AS src, concat('fact:', predicate) AS kind, " +
			"written_by AS actor_id, concat(predicate, ': ', object_value) AS txt " +
			"FROM mem.facts FINAL WHERE scope = ? AND subject_id = ? AND status != 'retracted'"
		return s.timelineUnion(ctx,
			fmt.Sprintf("entity %s scope %q", entityID, scope),
			obsLeg, []any{scope, entU},
			factLeg, []any{scope, entU},
			limit, offset)
	}

	caseU, err := uuid.Parse(caseID)
	if err != nil {
		return nil, fmt.Errorf("memory: invalid case id %q", caseID)
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
		"written_by AS actor_id, concat(predicate, ': ', object_value) AS txt " +
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
// avoided), deduped, order-stable, capped at timelineSubjectsCap.
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

// timelineUnion wraps the two legs in a subquery, orders the merged result
// deterministically and paginates AFTER union+order. Args must arrive
// obs-leg first, facts-leg second, matching leg placement in the UNION.
func (s *Service) timelineUnion(ctx context.Context, label string, obsLeg string, obsArgs []any, factLeg string, factArgs []any, limit, offset int) ([]Event, error) {
	q := "SELECT id, ts, src, kind, actor_id, txt FROM (" +
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

// scanTimeline runs one assembled query and scans its six-column rows.
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
		if err := rows.Scan(&e.ID, &e.Ts, &e.Source, &e.Kind, &e.ActorID, &e.Text); err != nil {
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
