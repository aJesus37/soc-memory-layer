package memory

import (
	"context"
	"fmt"
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
	Scope         string
	SubjectID     string  // required, uuid
	Predicate     string  // required non-empty
	ObjectValue   string  // required non-empty
	ObjectID      string  // optional uuid -> Nullable(UUID), "" = NULL
	Confidence    float32 // clamped to [0,1]
	SourceObs     string  // optional uuid -> zero uuid when empty
	ActorType     string  // human|agent (validated)
	ActorID       string  // required
	OnBehalfOf    string  // optional
	ClientEventID string  // optional idempotency hint, same rules as observations
}

// Fact is the persisted result of an AssertFact call.
type Fact struct {
	ID          string
	Scope       string
	SubjectID   string
	Predicate   string
	ObjectValue string
	Status      Status
	Confidence  float32
	ValidFrom   time.Time
	WrittenBy   string
}

// farFuture mirrors the schema's open-validity sentinel
// toDateTime('2105-12-31 23:59:59').
var farFuture = time.Date(2105, 12, 31, 23, 59, 59, 0, time.UTC)

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

	return Fact{
		ID:          fact.factUUID.String(),
		Scope:       fact.scope,
		SubjectID:   fact.subjectUUID.String(),
		Predicate:   fact.predicate,
		ObjectValue: fact.objectValue,
		Status:      status,
		Confidence:  conf,
		ValidFrom:   validFrom,
		WrittenBy:   fact.actorID,
	}, nil
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
	case c != c: // NaN
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
// for one (scope, subject, predicate) and returns how many were open
// (pre-count; single-process Phase 1 makes the race immaterial for a
// best-effort audit number). mutations_sync = 1 makes the ALTER wait for
// completion so a subsequent insert can never be re-closed by its own
// supersede wave — at Phase-1 volumes this synchronous wait is fine.
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

func (s *Service) insertFact(ctx context.Context, f factArgs, status Status, conf float32, validFrom time.Time) error {
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
		string(status), conf, f.sourceObs, f.actorID, validFrom, farFuture, validFrom,
	); err != nil {
		return fmt.Errorf("memory: append fact row: %w", err)
	}
	if err := b.Send(); err != nil {
		return fmt.Errorf("memory: commit fact %s: %w", f.factUUID, err)
	}
	return nil
}
