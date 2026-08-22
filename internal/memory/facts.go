package memory

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"
)

// FactInput is one fact assertion. SubjectID, Predicate and ObjectValue are
// required (trimmed non-empty; SubjectID must parse as a UUID); ObjectID is
// an optional UUID stored as NULL when empty. Confidence clamps into
// [0,1] instead of erroring — callers may pass raw model scores. SourceObs
// optionally links the originating observation; empty stores the zero UUID
// (the column is non-nullable). ActorType must be human|agent.
//
// ClientEventID follows the same rules as Input.ClientEventID: it becomes
// the row's fact_id when set (canonical UUID form). There is NO
// storage-level dedup on fact_id — a retry is accepted, never rejected as
// a duplicate. Whether it leaves a second physical row depends on the
// value asserted: a same-value retry shares the full ReplacingMergeTree
// sort key (scope, subject_id, predicate, object_value) and may therefore
// be merged away entirely by a background merge, while a different-value
// retry forms a new key and stays physically distinct until superseded
// and eventually collapsed. Either way each active write supersedes open
// priors first, so the authoritative FINAL read path stays correct.
type FactInput struct {
	Scope       string
	SubjectID   string  // required, uuid
	Predicate   string  // required non-empty
	ObjectValue string  // required non-empty
	ObjectID    string  // optional uuid -> Nullable(UUID), "" = NULL
	Confidence  float32 // clamped to [0,1]
	SourceObs   string  // optional uuid -> zero uuid when empty
	ActorType   string  // human|agent (validated)
	ActorID     string  // required
	// OnBehalfOf is accepted but NOT persisted in Phase 1 (no facts
	// column); delegation attribution lands with the API layer / schema
	// evolution.
	OnBehalfOf    string
	ClientEventID string // optional idempotency hint, same rules as observations
}

// Fact is the persisted result of an AssertFact call. SourceObs and
// ObjectID echo what was stored (canonical UUID form), with "" standing in
// for the stored sentinels: unknown provenance (zero-UUID source_obs) and
// NULL object_id respectively.
type Fact struct {
	ID          string
	Scope       string
	SubjectID   string
	Predicate   string
	ObjectValue string
	ObjectID    string
	SourceObs   string
	Status      Status
	Confidence  float32
	ValidFrom   time.Time
	WrittenBy   string
}

// farFuture mirrors the schema's open-validity sentinel
// toDateTime('2105-12-31 23:59:59').
var farFuture = time.Date(2105, 12, 31, 23, 59, 59, 0, time.UTC)

// ErrHumanGated marks a human-only operation attempted by a non-human
// actor. Promoting and retracting facts are reserved for humans; ANY other
// actor type — unrecognized ones included — fails closed into this error
// (wrapped with context; check with errors.Is). The API layer maps it to
// HTTP 403 in a later phase.
var ErrHumanGated = errors.New("memory: human actor required")

// maxRetractReasonRunes caps the free-text retract reason kept in the
// audit trail's payload_summary. Counted in runes so multibyte text is
// never split mid-character.
const maxRetractReasonRunes = 120

// AssertFact validates input, applies trust policy via ApplyTrust to decide
// the new fact's status, persists it, and returns it.
//
// Supersede semantics ("version-at-read"): mem.facts is a
// ReplacingMergeTree(updated_at) keyed by
// (scope, subject_id, predicate, object_value), so a changed object_value
// forms a NEW key that FINAL will never collapse against the old one. A
// superseding ACTIVE write therefore first closes all currently-open
// active facts for the same (scope, subject_id, predicate) by mutating
// their valid_to to now (SETTINGS mutations_sync = 1), then inserts its own
// row. PROPOSED facts never close prior actives — proposals must not
// silently replace human assertions. The physical history still collapses
// over time under ReplacingMergeTree + mutations; the durable audit trail
// of who-wrote-what lives in mem.audit (design doc §3/§5).
//
// The status column is ALWAYS written explicitly: its schema default is
// 'active' (fail-open) and must never be relied upon.
//
// Concurrency: the close-then-insert sequence is not atomic across
// processes. Two concurrent AssertFact calls on the same
// (scope, subject, predicate) can interleave as close/close(0 rows)/
// insert/insert, leaving TWO open facts with different object_value —
// FINAL never collapses distinct sort keys, so both stay open. This is
// self-healing: whichever write supersedes next closes every currently
// open fact for the key in one wave. Phase 1 accepts this window;
// a future remedy is per-key serialization or compare-and-retry.
//
// Failure window: closure via mutation is wall-clock and unrecoverable —
// if the insert after a successful supersede fails, the prior facts stay
// closed and no replacement exists. Accepted for Phase 1; the call logs
// the lost window and returns the error.
//
// Audit is best-effort like RecordObservation's: one content-free summary
// row per write (operation='assert_fact'), noting how many prior facts the
// mutation closed (superseded=N); failures are logged and swallowed.
func (s *Service) AssertFact(ctx context.Context, in FactInput) (Fact, error) {
	fact, err := s.validateFact(in)
	if err != nil {
		return Fact{}, err
	}
	conf := clampConfidence(in.Confidence)

	status := ApplyTrust(fact.actorType, fact.predicate, conf, s.trust)

	superseded := uint64(0)
	if status == Active {
		n, err := s.closeOpenFacts(ctx, fact.scope, fact.subjectUUID, fact.predicate)
		if err != nil {
			return Fact{}, err
		}
		superseded = n
	}

	validFrom := time.Now().UTC()
	if err := s.insertFact(ctx, fact, status, conf, validFrom); err != nil {
		if superseded > 0 {
			// The supersede wave already committed: the priors are closed
			// for good and no replacement row exists. Log the lost window
			// (content-safe fields only), then surface the insert error.
			s.log.Warn("memory: fact supersede without insert",
				"scope", fact.scope,
				"subject", fact.subjectUUID,
				"predicate", fact.predicate,
				"superseded", superseded,
				"err", err)
		}
		return Fact{}, err
	}

	summary := fmt.Sprintf("status=%s confidence=%.2f superseded=%d",
		status, conf, superseded)
	if err := s.conn.Exec(ctx,
		"INSERT INTO mem.audit "+
			"(actor_type, actor_id, operation, target_table, target_id, payload_summary) "+
			"VALUES (?, ?, ?, ?, ?, ?)",
		fact.actorType, fact.actorID, "assert_fact", "facts", fact.factUUID, summary,
	); err != nil {
		s.log.Warn("memory: audit insert failed; fact stands",
			"operation", "assert_fact",
			"target_table", "facts",
			"target_id", fact.factUUID,
			"err", err)
	}

	return factResult(fact, status, conf, validFrom), nil
}

// factResult renders a persisted row back as the public Fact shape, with
// "" standing in for the stored sentinels: NULL object_id and zero-UUID
// source_obs (unknown provenance).
func factResult(f factArgs, status Status, conf float32, validFrom time.Time) Fact {
	objIDStr := ""
	if f.objectID != nil {
		objIDStr = f.objectID.String()
	}
	srcObsStr := ""
	if f.sourceObs != uuid.Nil {
		srcObsStr = f.sourceObs.String()
	}
	return Fact{
		ID:          f.factUUID.String(),
		Scope:       f.scope,
		SubjectID:   f.subjectUUID.String(),
		Predicate:   f.predicate,
		ObjectValue: f.objectValue,
		ObjectID:    objIDStr,
		SourceObs:   srcObsStr,
		Status:      status,
		Confidence:  conf,
		ValidFrom:   validFrom,
		WrittenBy:   f.actorID,
	}
}

// loadedFact is one mem.facts version as loaded through FINAL.
type loadedFact struct {
	factID      uuid.UUID
	scope       string
	subjectUUID uuid.UUID
	predicate   string
	objectValue string
	objectID    *uuid.UUID // nil = NULL
	status      Status
	confidence  float32
	sourceObs   uuid.UUID
	writtenBy   string
	validFrom   time.Time
}

// loadFactByID reads one fact version through the authoritative read path
// (FINAL). Because FINAL collapses duplicate sort keys BEFORE the WHERE
// filter applies, a fact_id whose version has been replaced by a promote/
// retract transition resolves to NOTHING — consumed ids surface as "not
// found", and current state lives under the replacement row's own fact_id.
// If several physical rows share one fact_id across different sort keys
// (possible via repeated ClientEventIDs with differing values), the newest
// updated_at wins.
func (s *Service) loadFactByID(ctx context.Context, factID string) (loadedFact, error) {
	id, err := uuid.Parse(strings.TrimSpace(factID))
	if err != nil {
		return loadedFact{}, fmt.Errorf("memory: invalid fact id %q: %w", factID, err)
	}
	var f loadedFact
	var status string // Enum8 must scan into plain string, then convert
	err = s.conn.QueryRow(ctx,
		"SELECT scope, subject_id, predicate, object_value, object_id, "+
			"status, confidence, source_obs, written_by, valid_from "+
			"FROM mem.facts FINAL "+
			"WHERE fact_id = ? ORDER BY updated_at DESC LIMIT 1",
		id,
	).Scan(&f.scope, &f.subjectUUID, &f.predicate, &f.objectValue, &f.objectID,
		&status, &f.confidence, &f.sourceObs, &f.writtenBy, &f.validFrom)
	if err != nil {
		// clickhouse-go signals an empty result with io.EOF on some paths
		// and sql.ErrNoRows on others; both mean "no such fact".
		if errors.Is(err, io.EOF) || errors.Is(err, sql.ErrNoRows) {
			return loadedFact{}, fmt.Errorf("memory: fact %s not found", id)
		}
		return loadedFact{}, fmt.Errorf("memory: load fact %s: %w", id, err)
	}
	f.factID = id
	f.status = Status(status)
	return f, nil
}

// PromoteFact flips one proposed fact to active. HUMAN-GATED: any other
// actor type — unrecognized ones included — fails with an error wrapping
// ErrHumanGated.
//
// Version-at-read mechanics: the proposal is loaded through FINAL and must
// exist with status='proposed' (promoting an active or retracted fact is
// an error). The replacement row then carries the SAME ReplacingMergeTree
// sort key (scope, subject_id, predicate, object_value) plus the
// proposal's object_id, source_obs and confidence unchanged, but
// status='active', written_by set to the promoting actor, and a strictly
// newer updated_at — so FINAL resolves to the promoted version from the
// moment the insert lands. Only afterwards is the old proposal row
// mutate-closed by ITS OWN fact_id (SETTINGS mutations_sync = 1): scoped
// to one row, never the business key, so sibling open facts sharing
// (scope, subject_id, predicate) are untouched. A failed closure cannot
// corrupt the read path — the replacement already wins FINAL — so it is
// logged and swallowed, leaving at worst a shadowed row for a later merge
// to collapse.
//
// The insert-first ordering (opposite of AssertFact's close-then-insert)
// is deliberate: closing the proposal before its replacement existed would
// destroy it if the insert then failed; here a failed insert leaves the
// proposal untouched and retryable.
//
// The replacement always gets a fresh fact_id: ClientEventID idempotency
// belongs to assertions, not to state transitions on existing rows.
//
// Audit is best-effort as everywhere: operation='promote_fact',
// target_id = the row the transition wrote, payload_summary carries
// statuses and confidence only — never content.
func (s *Service) PromoteFact(ctx context.Context, factID, actorType, actorID string) (Fact, error) {
	if actorType != actorHuman {
		return Fact{}, fmt.Errorf("%w: promote_fact by actor type %q", ErrHumanGated, actorType)
	}
	author := strings.TrimSpace(actorID)
	if author == "" {
		return Fact{}, fmt.Errorf("memory: actor id required")
	}
	old, err := s.loadFactByID(ctx, factID)
	if err != nil {
		return Fact{}, err
	}
	if old.status != Proposed {
		return Fact{}, fmt.Errorf("memory: fact %s is %q, not proposed", old.factID, old.status)
	}

	now := time.Now().UTC()
	next := factArgs{
		factUUID:    uuid.New(),
		scope:       old.scope,
		subjectUUID: old.subjectUUID,
		predicate:   old.predicate,
		objectValue: old.objectValue,
		objectID:    old.objectID,
		sourceObs:   old.sourceObs,
		actorType:   actorHuman,
		actorID:     author,
	}
	if err := s.insertFact(ctx, next, Active, old.confidence, now); err != nil {
		return Fact{}, err
	}
	s.closePriorVersion(ctx, old.factID, "promote_fact")

	if err := s.conn.Exec(ctx,
		"INSERT INTO mem.audit "+
			"(actor_type, actor_id, operation, target_table, target_id, payload_summary) "+
			"VALUES (?, ?, ?, ?, ?, ?)",
		actorHuman, author, "promote_fact", "facts", next.factUUID,
		fmt.Sprintf("from=proposed to=active confidence=%.2f", old.confidence),
	); err != nil {
		s.log.Warn("memory: audit insert failed; fact stands",
			"operation", "promote_fact",
			"target_table", "facts",
			"target_id", next.factUUID,
			"err", err)
	}

	return factResult(next, Active, old.confidence, now), nil
}

// RetractFact supersedes any non-retracted fact (active or proposed) with
// a retracted version. HUMAN-GATED like PromoteFact: any non-human actor
// type fails closed with an error wrapping ErrHumanGated.
//
// The replacement row keeps the business key and provenance columns but is
// BORN CLOSED: valid_to = now, never later than its own valid_from plus
// the DateTime columns' one-second granularity. Even a reader that ignores
// FINAL and trusts only the validity window therefore never sees a
// retracted fact — and FINAL, which prefers the newest version of the
// shared sort key, agrees. The prior row is then mutate-closed by its own
// fact_id; failures there are logged and swallowed for the same reason as
// in PromoteFact: the replacement already owns the read path.
//
// Reason handling (documented decision): the free-text reason is
// human-authored justification ABOUT this decision — metadata for
// investigators, not observed content — so storing it in the audit
// payload_summary is an accepted exception to the content-free rule. It is
// trimmed and hard-capped at maxRetractReasonRunes; an empty reason
// contributes nothing. No other free text is ever persisted by this call.
//
// Audit is best-effort: operation='retract_fact', target_id = the row the
// transition wrote, summary "from=<prior status>" plus the capped reason.
func (s *Service) RetractFact(ctx context.Context, factID, reason, actorType, actorID string) (Fact, error) {
	if actorType != actorHuman {
		return Fact{}, fmt.Errorf("%w: retract_fact by actor type %q", ErrHumanGated, actorType)
	}
	author := strings.TrimSpace(actorID)
	if author == "" {
		return Fact{}, fmt.Errorf("memory: actor id required")
	}
	old, err := s.loadFactByID(ctx, factID)
	if err != nil {
		return Fact{}, err
	}
	if old.status == Retracted {
		return Fact{}, fmt.Errorf("memory: fact %s is already retracted", old.factID)
	}

	now := time.Now().UTC()
	next := factArgs{
		factUUID:    uuid.New(),
		scope:       old.scope,
		subjectUUID: old.subjectUUID,
		predicate:   old.predicate,
		objectValue: old.objectValue,
		objectID:    old.objectID,
		sourceObs:   old.sourceObs,
		actorType:   actorHuman,
		actorID:     author,
	}
	// Self-closing: born with valid_to = now.
	if err := s.insertFactRow(ctx, next, Retracted, old.confidence, now, now); err != nil {
		return Fact{}, err
	}
	s.closePriorVersion(ctx, old.factID, "retract_fact")

	summary := fmt.Sprintf("from=%s", old.status)
	if r := sanitizeReason(reason); r != "" {
		summary += " reason=" + r
	}
	if err := s.conn.Exec(ctx,
		"INSERT INTO mem.audit "+
			"(actor_type, actor_id, operation, target_table, target_id, payload_summary) "+
			"VALUES (?, ?, ?, ?, ?, ?)",
		actorHuman, author, "retract_fact", "facts", next.factUUID, summary,
	); err != nil {
		s.log.Warn("memory: audit insert failed; fact stands",
			"operation", "retract_fact",
			"target_table", "facts",
			"target_id", next.factUUID,
			"err", err)
	}

	return factResult(next, Retracted, old.confidence, now), nil
}

// sanitizeReason prepares a human-authored retract reason for the audit
// summary: surrounding whitespace trimmed, hard-capped at
// maxRetractReasonRunes (rune-safe for multibyte text).
func sanitizeReason(reason string) string {
	r := strings.TrimSpace(reason)
	if runes := []rune(r); len(runes) > maxRetractReasonRunes {
		r = string(runes[:maxRetractReasonRunes])
	}
	return r
}

// closePriorVersion mutate-closes the prior fact version after a promote
// or retract inserted its replacement. Best-effort BY DESIGN: the
// replacement shares the prior row's ReplacingMergeTree sort key with a
// strictly newer updated_at, so FINAL already resolves to the replacement
// regardless; a failed closure merely leaves the shadowed prior physically
// open until a later merge or manual mutation collapses it. Logged, never
// returned: the transition itself stands.
func (s *Service) closePriorVersion(ctx context.Context, factID uuid.UUID, operation string) {
	if err := s.closeFactByID(ctx, factID); err != nil {
		s.log.Warn("memory: closing prior fact version failed; FINAL still resolves to the replacement",
			"operation", operation,
			"target_id", factID,
			"err", err)
	}
}

// factArgs is validated, canonicalized FactInput ready for persistence.
type factArgs struct {
	factUUID    uuid.UUID
	scope       string
	subjectUUID uuid.UUID
	predicate   string
	objectValue string
	objectID    *uuid.UUID // nil = NULL
	sourceObs   uuid.UUID  // zero uuid when unknown
	actorType   string
	actorID     string
}

func (s *Service) validateFact(in FactInput) (factArgs, error) {
	scope := strings.TrimSpace(in.Scope)
	if scope == "" {
		return factArgs{}, fmt.Errorf("memory: scope required")
	}
	subjectUUID, err := uuid.Parse(strings.TrimSpace(in.SubjectID))
	if err != nil {
		return factArgs{}, fmt.Errorf("memory: invalid subject id %q: %w", in.SubjectID, err)
	}
	predicate := strings.TrimSpace(in.Predicate)
	if predicate == "" {
		return factArgs{}, fmt.Errorf("memory: predicate required")
	}
	objectValue := strings.TrimSpace(in.ObjectValue)
	if objectValue == "" {
		return factArgs{}, fmt.Errorf("memory: object value required")
	}

	var objectID *uuid.UUID
	if cid := strings.TrimSpace(in.ObjectID); cid != "" {
		u, err := uuid.Parse(cid)
		if err != nil {
			return factArgs{}, fmt.Errorf("memory: invalid object id %q: %w", in.ObjectID, err)
		}
		objectID = &u
	}

	sourceObs := uuid.Nil // unknown provenance stores the zero UUID (column non-nullable)
	if src := strings.TrimSpace(in.SourceObs); src != "" {
		u, err := uuid.Parse(src)
		if err != nil {
			return factArgs{}, fmt.Errorf("memory: invalid source obs %q: %w", in.SourceObs, err)
		}
		sourceObs = u
	}

	if !validActorTypes[in.ActorType] {
		return factArgs{}, fmt.Errorf("memory: invalid actor type %q", in.ActorType)
	}
	actorID := strings.TrimSpace(in.ActorID)
	if actorID == "" {
		return factArgs{}, fmt.Errorf("memory: actor id required")
	}

	factUUID := uuid.New()
	if cid := strings.TrimSpace(in.ClientEventID); cid != "" {
		u, err := uuid.Parse(cid)
		if err != nil {
			return factArgs{}, fmt.Errorf("memory: invalid client event id %q: %w", in.ClientEventID, err)
		}
		factUUID = u
	}

	return factArgs{
		factUUID:    factUUID,
		scope:       scope,
		subjectUUID: subjectUUID,
		predicate:   predicate,
		objectValue: objectValue,
		objectID:    objectID,
		sourceObs:   sourceObs,
		actorType:   in.ActorType,
		actorID:     actorID,
	}, nil
}

// clampConfidence clamps into [0,1], saturating at the nearer bound. NaN
// compares false against everything, so it is matched explicitly and maps
// to 0: an unknown score must fail safe as LOW confidence — clamping NaN
// to 1 would let it auto-activate a whitelisted fact via conf >= floor.
func clampConfidence(c float32) float32 {
	switch {
	case math.IsNaN(float64(c)): // NaN
		return 0
	case c < 0:
		return 0
	case c > 1:
		return 1
	default:
		return c
	}
}

// closeOpenFacts sets valid_to = now on every currently-open active fact
// for one (scope, subject, predicate) and returns how many were open.
//
// The count and the mutation are two statements, so under concurrency n
// is only a best-effort pre-count and can err in BOTH directions. It
// UNDERCOUNTS when rows inserted between the count and the ALTER still
// match the mutation's WHERE clause — closed but unreported. It OVERCOUNTS
// when a racing second wave reports its stale pre-count even though the
// winner's wave did all the actual closing.
//
// mutations_sync = 1 makes the ALTER wait for completion so a subsequent
// insert can never be re-closed by its own supersede wave — at Phase-1
// volumes this synchronous wait is fine. Operators should watch the
// system.mutations backlog (pending/failed mutation entries) for lag or
// stuck waves on mem.facts.
func (s *Service) closeOpenFacts(ctx context.Context, scope string, subject uuid.UUID, predicate string) (uint64, error) {
	var n uint64
	if err := s.conn.QueryRow(ctx,
		"SELECT count() FROM mem.facts "+
			"WHERE scope = ? AND subject_id = ? AND predicate = ? "+
			"AND status = 'active' AND valid_to > now64(3)",
		scope, subject, predicate,
	).Scan(&n); err != nil {
		return 0, fmt.Errorf("memory: count open facts before supersede: %w", err)
	}
	if n == 0 {
		return 0, nil
	}
	err := s.conn.Exec(ctx,
		"ALTER TABLE mem.facts UPDATE valid_to = ? "+
			"WHERE scope = ? AND subject_id = ? AND predicate = ? "+
			"AND status = 'active' AND valid_to > now64(3) "+
			"SETTINGS mutations_sync = 1",
		time.Now().UTC(), scope, subject, predicate,
	)
	if err != nil {
		return 0, fmt.Errorf("memory: supersede open facts (scope=%s): %w", scope, err)
	}
	return n, nil
}

// closeFactByID sets valid_to = now on exactly ONE fact row, identified by
// fact_id — never by business key — so promoting or retracting one fact
// cannot disturb sibling open facts that merely share
// (scope, subject_id, predicate). mutations_sync = 1 like the supersede
// wave, so no caller statement can race the closure. Unlike closeOpenFacts
// there is no pre-count: a single-row mutation has nothing to report.
func (s *Service) closeFactByID(ctx context.Context, factID uuid.UUID) error {
	err := s.conn.Exec(ctx,
		"ALTER TABLE mem.facts UPDATE valid_to = ? "+
			"WHERE fact_id = ? SETTINGS mutations_sync = 1",
		time.Now().UTC(), factID,
	)
	if err != nil {
		return fmt.Errorf("memory: close fact %s: %w", factID, err)
	}
	return nil
}

func (s *Service) insertFact(ctx context.Context, f factArgs, status Status, conf float32, validFrom time.Time) error {
	return s.insertFactRow(ctx, f, status, conf, validFrom, farFuture)
}

// insertFactRow stages one fact row with an explicit validity window;
// insertFact wraps it with the open-ended sentinel for ordinary writes.
// updated_at rides along with valid_from (now), matching the column's
// DEFAULT now64(3).
func (s *Service) insertFactRow(ctx context.Context, f factArgs, status Status, conf float32, validFrom, validTo time.Time) error {
	b, err := s.conn.PrepareBatch(ctx,
		"INSERT INTO mem.facts "+
			"(fact_id, scope, subject_id, predicate, object_value, object_id, "+
			"status, confidence, source_obs, written_by, valid_from, valid_to, updated_at)")
	if err != nil {
		return fmt.Errorf("memory: stage fact insert: %w", err)
	}
	var objID any // nil interface -> NULL in Nullable(UUID)
	if f.objectID != nil {
		objID = *f.objectID
	}
	if err := b.Append(
		f.factUUID, f.scope, f.subjectUUID, f.predicate, f.objectValue, objID,
		string(status), conf, f.sourceObs, f.actorID, validFrom, validTo, validFrom,
	); err != nil {
		return fmt.Errorf("memory: append fact row: %w", err)
	}
	if err := b.Send(); err != nil {
		return fmt.Errorf("memory: commit fact %s: %w", f.factUUID, err)
	}
	return nil
}
