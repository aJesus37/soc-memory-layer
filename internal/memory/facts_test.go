package memory

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"socmem/internal/entity"
)

// TestClampConfidence pins the [0,1] saturation rules, including the
// fail-safe NaN mapping: NaN compares false against everything, so it must
// be matched explicitly and become 0 — clamping it to 1 would let a
// NaN-confidence agent fact auto-activate via conf >= floor.
func TestClampConfidence(t *testing.T) {
	cases := []struct {
		name string
		in   float32
		want float32
	}{
		{"in range unchanged", 0.42, 0.42},
		{"zero", 0, 0},
		{"one", 1, 1},
		{"negative saturates low", -0.5, 0},
		{"above range saturates high", 1.7, 1},
		{"NaN fails safe to zero", float32(math.NaN()), 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := clampConfidence(c.in); got != c.want {
				t.Errorf("clampConfidence(%v) = %v, want %v", c.in, got, c.want)
			}
		})
	}

	t.Run("NaN confidence cannot cross the auto-activation floor", func(t *testing.T) {
		tc := trustConfig{floor: 0.8, whitelist: map[string]bool{"resolved_to": true}}
		got := ApplyTrust(actorAgent, "resolved_to", clampConfidence(float32(math.NaN())), tc)
		if got != Proposed {
			t.Errorf("NaN-confidence agent fact status = %v, want proposed (fail safe)", got)
		}
	})
}

// mustResolveEntity resolves raw to its canonical entity via the real
// resolver, creating it on first sight; tests use the EntityID as a fact
// subject.
func mustResolveEntity(t *testing.T, conn driver.Conn, scope, raw string) entity.Entity {
	t.Helper()
	e, _, err := entity.NewResolver(conn).Resolve(context.Background(), scope, raw)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// countOpenFacts counts open active facts for one (scope, subject,
// predicate) through the authoritative read path: FINAL plus validity
// window. This is what Enrich will query later.
func countOpenFacts(t *testing.T, ctx context.Context, conn driver.Conn, scope string, subject uuid.UUID, predicate string) uint64 {
	t.Helper()
	var n uint64
	if err := conn.QueryRow(ctx,
		"SELECT count() FROM mem.facts FINAL "+
			"WHERE scope = ? AND subject_id = ? AND predicate = ? "+
			"AND status = 'active' AND valid_to > now64(3)",
		scope, subject, predicate,
	).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestAssertFactSupersedes(t *testing.T) {
	conn := itestConn(t)
	ctx := context.Background()
	s := testService(t, conn)
	scope := itestScope()
	subj := mustResolveEntity(t, conn, scope, "bad.example.com")
	subjUUID, err := uuid.Parse(subj.EntityID)
	if err != nil {
		t.Fatal(err)
	}

	f1, err := s.AssertFact(ctx, FactInput{
		Scope:       scope,
		SubjectID:   subj.EntityID,
		Predicate:   "verdict_malicious",
		ObjectValue: "c2",
		Confidence:  0.9,
		ActorType:   "human",
		ActorID:     "analyst-j",
	})
	if err != nil {
		t.Fatal(err)
	}
	if f1.Status != Active {
		t.Fatalf("human fact should be active, got %s", f1.Status)
	}
	if _, err := uuid.Parse(f1.ID); err != nil {
		t.Fatalf("fact ID %q not a UUID: %v", f1.ID, err)
	}
	if f1.WrittenBy != "analyst-j" || f1.ObjectValue != "c2" || f1.Predicate != "verdict_malicious" {
		t.Errorf("echoed fields wrong: %+v", f1)
	}

	// DateTime64(3) still ties at ms sometimes; keep 1.1s like other tests
	time.Sleep(1100 * time.Millisecond)

	f2, err := s.AssertFact(ctx, FactInput{
		Scope:       scope,
		SubjectID:   subj.EntityID,
		Predicate:   "verdict_malicious",
		ObjectValue: "benign-parked",
		Confidence:  0.9,
		ActorType:   "human",
		ActorID:     "analyst-k",
	})
	if err != nil {
		t.Fatal(err)
	}

	// The authoritative read path (validity-window query used later by
	// Enrich): exactly ONE open fact for this (subject,predicate): f2's value.
	if n := countOpenFacts(t, ctx, conn, scope, subjUUID, "verdict_malicious"); n != 1 {
		t.Fatalf("want exactly 1 open fact, got %d", n)
	}

	// f1 row physically closed by mutation: valid_to moved off the far-future
	// sentinel, at or before the moment f2 became valid.
	var vt time.Time
	if err := conn.QueryRow(ctx,
		"SELECT valid_to FROM mem.facts WHERE fact_id = ?", f1.ID,
	).Scan(&vt); err != nil {
		t.Fatal(err)
	}
	if year := vt.Year(); year > 2100 {
		t.Fatalf("f1 still open (sentinel valid_to): %v", vt)
	}
	if vt.After(f2.ValidFrom) {
		t.Fatalf("f1 closed after f2 became valid: %v > %v", vt, f2.ValidFrom)
	}

	// f2 itself is open and echoes the new state.
	f2UUID, _ := uuid.Parse(f2.ID)
	var (
		status string
		writer string
	)
	if err := conn.QueryRow(ctx,
		"SELECT status, written_by FROM mem.facts WHERE fact_id = ?", f2UUID,
	).Scan(&status, &writer); err != nil {
		t.Fatal(err)
	}
	if Status(status) != Active || writer != "analyst-k" {
		t.Errorf("f2 stored wrong: status=%q written_by=%q", status, writer)
	}

	// Audit: two rows for this subject (one per write), no content values.
	var nAudit uint64
	if err := conn.QueryRow(ctx,
		"SELECT count() FROM mem.audit WHERE operation = 'assert_fact' "+
			"AND target_table = 'facts' AND target_id IN (?, ?)",
		uuid.MustParse(f1.ID), f2UUID,
	).Scan(&nAudit); err != nil {
		t.Fatal(err)
	}
	if nAudit != 2 {
		t.Fatalf("audit rows for the two writes = %d, want 2", nAudit)
	}
	var summaries []string
	rows, err := conn.Query(ctx,
		"SELECT payload_summary FROM mem.audit WHERE operation = 'assert_fact' "+
			"AND target_table = 'facts' AND target_id IN (?, ?)",
		uuid.MustParse(f1.ID), f2UUID,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var sum string
		if err := rows.Scan(&sum); err != nil {
			t.Fatal(err)
		}
		summaries = append(summaries, sum)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	for _, sum := range summaries {
		for _, secret := range []string{"c2", "benign-parked"} {
			if strings.Contains(sum, secret) {
				t.Errorf("payload_summary leaks object value %q: %q", secret, sum)
			}
		}
	}
	// The superseding write reports how many priors it closed.
	foundSuperseded := false
	for _, sum := range summaries {
		if strings.Contains(sum, "superseded=1") {
			foundSuperseded = true
		}
	}
	if !foundSuperseded {
		t.Errorf("no audit summary notes superseded=1: %q", summaries)
	}
}

func TestAssertFactAgentProposed(t *testing.T) {
	conn := itestConn(t)
	ctx := context.Background()
	s := testService(t, conn)
	scope := itestScope()
	subj := mustResolveEntity(t, conn, scope, "proposed-test.example.com")
	subjUUID, err := uuid.Parse(subj.EntityID)
	if err != nil {
		t.Fatal(err)
	}

	// Prior human assertion is open and active.
	f1, err := s.AssertFact(ctx, FactInput{
		Scope:       scope,
		SubjectID:   subj.EntityID,
		Predicate:   "verdict_malicious",
		ObjectValue: "c2",
		Confidence:  0.9,
		ActorType:   "human",
		ActorID:     "analyst-j",
	})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(1100 * time.Millisecond)

	// Agent proposal with a NON-whitelisted predicate: must NOT close the
	// prior active fact — proposals never silently replace assertions.
	f2, err := s.AssertFact(ctx, FactInput{
		Scope:       scope,
		SubjectID:   subj.EntityID,
		Predicate:   "verdict_malicious",
		ObjectValue: "benign-parked",
		Confidence:  0.99,
		ActorType:   "agent",
		ActorID:     "triage-bot",
	})
	if err != nil {
		t.Fatal(err)
	}
	if f2.Status != Proposed {
		t.Fatalf("agent non-whitelisted fact should be proposed, got %s", f2.Status)
	}

	// Prior fact still open; the proposal does not count as open-active.
	if n := countOpenFacts(t, ctx, conn, scope, subjUUID, "verdict_malicious"); n != 1 {
		t.Fatalf("prior human fact must remain the only open fact, got %d", n)
	}
	var vt time.Time
	if err := conn.QueryRow(ctx,
		"SELECT valid_to FROM mem.facts WHERE fact_id = ?", f1.ID,
	).Scan(&vt); err != nil {
		t.Fatal(err)
	}
	if year := vt.Year(); year <= 2100 {
		t.Fatalf("proposal closed the prior active fact, valid_to=%v", vt)
	}

	// The proposed row exists with explicit status='proposed'.
	f2UUID, _ := uuid.Parse(f2.ID)
	var status string
	if err := conn.QueryRow(ctx,
		"SELECT status FROM mem.facts WHERE fact_id = ?", f2UUID,
	).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if Status(status) != Proposed {
		t.Errorf("stored status = %q, want proposed", status)
	}
}

func TestAssertFactAgentAutoActivate(t *testing.T) {
	conn := itestConn(t)
	ctx := context.Background()
	s := testService(t, conn)
	scope := itestScope()
	subj := mustResolveEntity(t, conn, scope, "8.8.8.8")
	subjUUID, err := uuid.Parse(subj.EntityID)
	if err != nil {
		t.Fatal(err)
	}

	// resolved_to is whitelisted by default and conf >= floor 0.8, so an
	// agent fact ACTIVATES — and therefore supersedes like any active fact.
	time.Sleep(1100 * time.Millisecond) // keep updated_at distinct from any prior write
	f1, err := s.AssertFact(ctx, FactInput{
		Scope:       scope,
		SubjectID:   subj.EntityID,
		Predicate:   "resolved_to",
		ObjectValue: "old.example.net",
		Confidence:  0.9,
		ActorType:   "human",
		ActorID:     "analyst-j",
	})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(1100 * time.Millisecond)

	f2, err := s.AssertFact(ctx, FactInput{
		Scope:       scope,
		SubjectID:   subj.EntityID,
		Predicate:   "resolved_to",
		ObjectValue: "new.example.net",
		Confidence:  0.95,
		ActorType:   "agent",
		ActorID:     "sensor-7",
	})
	if err != nil {
		t.Fatal(err)
	}
	if f2.Status != Active {
		t.Fatalf("agent whitelisted fact above floor should be active, got %s", f2.Status)
	}
	if n := countOpenFacts(t, ctx, conn, scope, subjUUID, "resolved_to"); n != 1 {
		t.Fatalf("want exactly 1 open fact after agent auto-activation, got %d", n)
	}
	var vt time.Time
	if err := conn.QueryRow(ctx,
		"SELECT valid_to FROM mem.facts WHERE fact_id = ?", f1.ID,
	).Scan(&vt); err != nil {
		t.Fatal(err)
	}
	if year := vt.Year(); year > 2100 {
		t.Fatalf("active agent fact did not supersede prior, valid_to=%v", vt)
	}
}

func TestAssertFactValidation(t *testing.T) {
	conn := itestConn(t)
	ctx := context.Background()
	s := testService(t, conn)
	scope := itestScope()

	bad := map[string]FactInput{
		"empty subject id":          {Scope: scope, Predicate: "p", ObjectValue: "v", ActorType: "human", ActorID: "a"},
		"malformed subject id":      {Scope: scope, SubjectID: "not-a-uuid", Predicate: "p", ObjectValue: "v", ActorType: "human", ActorID: "a"},
		"blank predicate":           {Scope: scope, SubjectID: uuid.NewString(), Predicate: "   ", ObjectValue: "v", ActorType: "human", ActorID: "a"},
		"blank object value":        {Scope: scope, SubjectID: uuid.NewString(), Predicate: "p", ObjectValue: " \t ", ActorType: "human", ActorID: "a"},
		"empty scope":               {Predicate: "p", ObjectValue: "v", ActorType: "human", ActorID: "a"},
		"bad actor type":            {Scope: scope, SubjectID: uuid.NewString(), Predicate: "p", ObjectValue: "v", ActorType: "robot", ActorID: "a"},
		"empty actor id":            {Scope: scope, SubjectID: uuid.NewString(), Predicate: "p", ObjectValue: "v", ActorType: "human"},
		"malformed client event id": {Scope: scope, SubjectID: uuid.NewString(), Predicate: "p", ObjectValue: "v", ActorType: "human", ActorID: "a", ClientEventID: "nope"},
		"malformed object id":       {Scope: scope, SubjectID: uuid.NewString(), Predicate: "p", ObjectValue: "v", ActorType: "human", ActorID: "a", ObjectID: "nope"},
		"malformed source obs":      {Scope: scope, SubjectID: uuid.NewString(), Predicate: "p", ObjectValue: "v", ActorType: "human", ActorID: "a", SourceObs: "nope"},
	}
	for name, in := range bad {
		if _, err := s.AssertFact(ctx, in); err == nil {
			t.Errorf("%s: expected validation error, got nil", name)
		}
	}

	var n uint64
	if err := conn.QueryRow(ctx,
		"SELECT count() FROM mem.facts WHERE scope = ?", scope,
	).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("invalid inputs wrote %d fact rows in %s, want 0", n, scope)
	}

	// Confidence clamps into [0,1] instead of erroring (documented decision).
	clamped := mustResolveEntity(t, conn, scope, "clamp.example.com")
	low, err := s.AssertFact(ctx, FactInput{
		Scope: scope, SubjectID: clamped.EntityID, Predicate: "score_low",
		ObjectValue: "x", Confidence: -0.5, ActorType: "human", ActorID: "a",
	})
	if err != nil {
		t.Fatal(err)
	}
	high, err := s.AssertFact(ctx, FactInput{
		Scope: scope, SubjectID: clamped.EntityID, Predicate: "score_high",
		ObjectValue: "y", Confidence: 1.7, ActorType: "human", ActorID: "a",
	})
	if err != nil {
		t.Fatal(err)
	}
	nan, err := s.AssertFact(ctx, FactInput{
		Scope: scope, SubjectID: clamped.EntityID, Predicate: "score_nan",
		ObjectValue: "z", Confidence: float32(math.NaN()), ActorType: "human", ActorID: "a",
	})
	if err != nil {
		t.Fatal(err)
	}
	if low.Confidence != 0 || high.Confidence != 1 || nan.Confidence != 0 {
		t.Errorf("confidence clamp wrong: low=%v high=%v nan=%v, want 0, 1, 0",
			low.Confidence, high.Confidence, nan.Confidence)
	}

	// Unknown source_obs is allowed: empty string stores the zero UUID
	// (source_obs column is non-nullable UUID).
	noSrc := mustResolveEntity(t, conn, scope, "nosrc.example.com")
	f, err := s.AssertFact(ctx, FactInput{
		Scope: scope, SubjectID: noSrc.EntityID, Predicate: "seen_at",
		ObjectValue: "somewhere", ActorType: "human", ActorID: "a",
	})
	if err != nil {
		t.Fatal(err)
	}
	fUUID, _ := uuid.Parse(f.ID)
	var srcObs uuid.UUID
	if err := conn.QueryRow(ctx,
		"SELECT source_obs FROM mem.facts WHERE fact_id = ?", fUUID,
	).Scan(&srcObs); err != nil {
		t.Fatal(err)
	}
	if srcObs != (uuid.UUID{}) {
		t.Errorf("empty SourceObs stored %s, want zero UUID", srcObs)
	}

	// Optional ObjectID round-trips: set → stored, empty → NULL.
	withObj := mustResolveEntity(t, conn, scope, "objid.example.com")
	objUUID := uuid.New()
	got, err := s.AssertFact(ctx, FactInput{
		Scope: scope, SubjectID: withObj.EntityID, Predicate: "related_case",
		ObjectValue: "case-x", ObjectID: objUUID.String(), ActorType: "human", ActorID: "a",
	})
	if err != nil {
		t.Fatal(err)
	}
	gotUUID, _ := uuid.Parse(got.ID)
	var objID *uuid.UUID
	if err := conn.QueryRow(ctx,
		"SELECT object_id FROM mem.facts WHERE fact_id = ?", gotUUID,
	).Scan(&objID); err != nil {
		t.Fatal(err)
	}
	if objID == nil || *objID != objUUID {
		t.Errorf("object_id = %v, want %s", objID, objUUID)
	}

	none, err := s.AssertFact(ctx, FactInput{
		Scope: scope, SubjectID: withObj.EntityID, Predicate: "related_case",
		ObjectValue: "case-null", ActorType: "human", ActorID: "a",
	})
	if err != nil {
		t.Fatal(err)
	}
	noneUUID, _ := uuid.Parse(none.ID)
	var nullCount uint64
	if err := conn.QueryRow(ctx,
		"SELECT count() FROM mem.facts WHERE fact_id = ? AND object_id IS NULL", noneUUID,
	).Scan(&nullCount); err != nil {
		t.Fatal(err)
	}
	if nullCount != 1 {
		t.Errorf("empty ObjectID did not store NULL, matched rows=%d", nullCount)
	}
}

func TestAssertFactClientEventID(t *testing.T) {
	conn := itestConn(t)
	ctx := context.Background()
	s := testService(t, conn)
	scope := itestScope()
	subj := mustResolveEntity(t, conn, scope, "retry.example.com")

	id := uuid.NewString()
	in := func() FactInput { // identical retry payload
		return FactInput{
			Scope:         scope,
			SubjectID:     subj.EntityID,
			Predicate:     "retry_probe",
			ObjectValue:   "same-value",
			ActorType:     "human",
			ActorID:       "analyst-j",
			ClientEventID: id,
		}
	}
	f1, err := s.AssertFact(ctx, in())
	if err != nil {
		t.Fatal(err)
	}
	// DateTime64(3) still ties at ms sometimes; keep 1.1s like other tests
	// so FINAL's newest-version selection between the two retries (same
	// sort key) stays deterministic.
	time.Sleep(1100 * time.Millisecond)
	f2, err := s.AssertFact(ctx, in())
	if err != nil {
		t.Fatalf("resubmitting the same ClientEventID must be accepted: %v", err)
	}
	want, _ := uuid.Parse(id)
	if f1.ID != want.String() || f2.ID != want.String() {
		t.Fatalf("fact IDs = %q, %q; both must equal ClientEventID canonical form", f1.ID, f2.ID)
	}

	// Storage does NOT dedup by fact_id — but it also does NOT guarantee
	// that two physical rows survive: both retries share the full
	// ReplacingMergeTree sort key (scope, subject_id, predicate,
	// object_value), differing only in updated_at, so a background merge
	// may legally collapse them into one row (the newest updated_at wins)
	// at any moment before this count runs. Accept 1 or 2; the invariant
	// that matters lives on the FINAL read path below.
	subjUUID, _ := uuid.Parse(subj.EntityID)
	var (
		physicalRows uint64
		distinctIDs  uint64
		survivorID   uuid.UUID
	)
	if err := conn.QueryRow(ctx,
		"SELECT count(), uniqExact(fact_id), max(fact_id) FROM mem.facts "+
			"WHERE scope = ? AND subject_id = ? AND predicate = ? AND object_value = ?",
		scope, subjUUID, "retry_probe", "same-value",
	).Scan(&physicalRows, &distinctIDs, &survivorID); err != nil {
		t.Fatal(err)
	}
	if physicalRows < 1 || physicalRows > 2 {
		t.Errorf("physical rows for repeated fact_id %s = %d, want 1 or 2 (ReplacingMergeTree merge makes 2 non-guaranteed)", id, physicalRows)
	}
	if distinctIDs != 1 || survivorID != want {
		t.Errorf("surviving row fact_id = %s (distinct=%d), want ClientEventID %s", survivorID, distinctIDs, want)
	}
	if n := countOpenFacts(t, ctx, conn, scope, subjUUID, "retry_probe"); n != 1 {
		t.Errorf("open facts after same-value re-assert = %d, want 1", n)
	}
}

// TestAssertFactDoubleSupersede walks A→B on one (subject, predicate),
// then plants a second OPEN fact for the same key via direct insert —
// simulating the concurrent-assert race documented on AssertFact, which
// can leave two open facts with different object_value. The final C wave
// must close BOTH open priors in a single supersede and leave exactly
// ['value-c'] open on the FINAL read path. A broken closeOpenFacts capped
// at one row per wave would pass the plain A→B→C chain but fails here:
// 'value-race' would survive open.
func TestAssertFactDoubleSupersede(t *testing.T) {
	conn := itestConn(t)
	ctx := context.Background()
	s := testService(t, conn)
	scope := itestScope()
	subj := mustResolveEntity(t, conn, scope, "chain.example.com")
	subjUUID, err := uuid.Parse(subj.EntityID)
	if err != nil {
		t.Fatal(err)
	}

	values := []string{"value-a", "value-b"}
	for i, v := range values {
		if i > 0 {
			// DateTime64(3) still ties at ms sometimes; keep 1.1s like
			// other tests so each wave's validity window is distinct.
			time.Sleep(1100 * time.Millisecond)
		}
		if _, err := s.AssertFact(ctx, FactInput{
			Scope:       scope,
			SubjectID:   subj.EntityID,
			Predicate:   "verdict_malicious",
			ObjectValue: v,
			Confidence:  0.9,
			ActorType:   "human",
			ActorID:     "analyst-j",
		}); err != nil {
			t.Fatal(err)
		}
		if n := countOpenFacts(t, ctx, conn, scope, subjUUID, "verdict_malicious"); n != 1 {
			t.Fatalf("after asserting %q: open facts = %d, want 1", v, n)
		}
	}

	// Plant the race's leftover directly (bypasses AssertFact): an extra
	// OPEN row for the same key, different object_value, sentinel valid_to.
	time.Sleep(1100 * time.Millisecond) // keep updated_at distinct from the B wave
	batch, err := conn.PrepareBatch(ctx,
		"INSERT INTO mem.facts "+
			"(fact_id, scope, subject_id, predicate, object_value, object_id, "+
			"status, confidence, source_obs, written_by, valid_from, valid_to, updated_at)")
	if err != nil {
		t.Fatal(err)
	}
	plantedAt := time.Now().UTC()
	if err := batch.Append(
		uuid.New(), scope, subjUUID, "verdict_malicious", "value-race", nil,
		string(Active), float32(0.9), uuid.Nil, "analyst-j",
		plantedAt, farFuture, plantedAt,
	); err != nil {
		t.Fatal(err)
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
	// The plant really produced a dual-open state; otherwise the final
	// assertion would silently degrade to the single-prior case.
	if n := countOpenFacts(t, ctx, conn, scope, subjUUID, "verdict_malicious"); n != 2 {
		t.Fatalf("after planting value-race: open facts = %d, want 2", n)
	}

	// One healing wave through AssertFact must close BOTH priors.
	time.Sleep(1100 * time.Millisecond)
	last, err := s.AssertFact(ctx, FactInput{
		Scope:       scope,
		SubjectID:   subj.EntityID,
		Predicate:   "verdict_malicious",
		ObjectValue: "value-c",
		Confidence:  0.9,
		ActorType:   "human",
		ActorID:     "analyst-j",
	})
	if err != nil {
		t.Fatal(err)
	}
	if last.ObjectValue != "value-c" || last.Status != Active {
		t.Fatalf("final fact = %+v, want active with object value %q", last, "value-c")
	}

	// The authoritative read path agrees: only value-c survives as open.
	rows, err := conn.Query(ctx,
		"SELECT object_value FROM mem.facts FINAL "+
			"WHERE scope = ? AND subject_id = ? AND predicate = ? "+
			"AND status = 'active' AND valid_to > now64(3)",
		scope, subjUUID, "verdict_malicious",
	)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var open []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		open = append(open, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(open) != 1 || open[0] != "value-c" {
		t.Errorf("open object values = %v, want exactly [value-c]", open)
	}
}

// TestPromoteFact walks a forced agent proposal (non-whitelisted predicate,
// high confidence) through human promotion. The promoted version must own
// the FINAL validity-window read path with the promoter credited in
// written_by, a sibling open fact sharing only (scope, subject, predicate)
// must survive untouched, re-promotion and agent attempts must fail, and
// the audit trail must stay content-free.
func TestPromoteFact(t *testing.T) {
	conn := itestConn(t)
	ctx := context.Background()
	s := testService(t, conn)
	scope := itestScope()
	subj := mustResolveEntity(t, conn, scope, "promote.example.com")
	subjUUID, err := uuid.Parse(subj.EntityID)
	if err != nil {
		t.Fatal(err)
	}

	// Sibling human assertion: stays open through the whole dance and
	// proves the promote mutation closes ONE fact_id, not the business key.
	f1, err := s.AssertFact(ctx, FactInput{
		Scope:       scope,
		SubjectID:   subj.EntityID,
		Predicate:   "verdict_malicious",
		ObjectValue: "c2",
		Confidence:  0.9,
		ActorType:   "human",
		ActorID:     "analyst-j",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Force a proposal: agent + non-whitelisted predicate + high confidence
	// can never auto-activate.
	time.Sleep(1100 * time.Millisecond) // keep updated_at distinct per wave
	prop, err := s.AssertFact(ctx, FactInput{
		Scope:       scope,
		SubjectID:   subj.EntityID,
		Predicate:   "verdict_malicious",
		ObjectValue: "benign-parked",
		Confidence:  0.99,
		ActorType:   "agent",
		ActorID:     "triage-bot",
	})
	if err != nil {
		t.Fatal(err)
	}
	if prop.Status != Proposed {
		t.Fatalf("agent non-whitelisted fact = %s, want proposed", prop.Status)
	}

	time.Sleep(1100 * time.Millisecond)
	promoted, err := s.PromoteFact(ctx, prop.ID, "human", "analyst-k")
	if err != nil {
		t.Fatal(err)
	}
	if promoted.ID == prop.ID {
		t.Error("transition reused the proposal's fact_id, want a fresh one")
	}
	if promoted.Status != Active || promoted.ObjectValue != "benign-parked" ||
		promoted.Predicate != "verdict_malicious" || promoted.Confidence != 0.99 ||
		promoted.WrittenBy != "analyst-k" {
		t.Errorf("promoted fact wrong: %+v", promoted)
	}
	promotedUUID, _ := uuid.Parse(promoted.ID)
	f1UUID, _ := uuid.Parse(f1.ID)

	// Authoritative read path: exactly two open actives — the untouched
	// sibling and the promoted proposal — each credited to its writer.
	type openRow struct {
		id uuid.UUID
		by string
	}
	rows, err := conn.Query(ctx,
		"SELECT fact_id, object_value, written_by FROM mem.facts FINAL "+
			"WHERE scope = ? AND subject_id = ? AND predicate = ? "+
			"AND status = 'active' AND valid_to > now64(3)",
		scope, subjUUID, "verdict_malicious",
	)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	open := map[string]openRow{}
	for rows.Next() {
		var (
			r openRow
			v string
		)
		if err := rows.Scan(&r.id, &v, &r.by); err != nil {
			t.Fatal(err)
		}
		open[v] = r
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(open) != 2 {
		t.Fatalf("open facts after promote = %d (%v), want 2 (promoted + untouched sibling)", len(open), open)
	}
	if r := open["c2"]; r.id != f1UUID || r.by != "analyst-j" {
		t.Errorf("sibling c2 row disturbed by promote: %+v", r)
	}
	if r := open["benign-parked"]; r.id != promotedUUID || r.by != "analyst-k" {
		t.Errorf("promoted row wrong on FINAL read path: %+v", r)
	}

	// The proposal side is gone from every read path: FINAL resolves the
	// shared business key to exactly one version — the promoted replacement
	// — so no open proposed version remains. (A background merge may have
	// collapsed the consumed proposal row away physically, so looking it up
	// by its own fact_id is not merge-safe.)
	var openProp uint64
	if err := conn.QueryRow(ctx,
		"SELECT count() FROM mem.facts FINAL "+
			"WHERE scope = ? AND subject_id = ? AND predicate = ? AND object_value = ? "+
			"AND status = 'proposed' AND valid_to > now64(3)",
		scope, subjUUID, "verdict_malicious", "benign-parked",
	).Scan(&openProp); err != nil {
		t.Fatal(err)
	}
	if openProp != 0 {
		t.Errorf("open proposed versions after promote = %d, want 0", openProp)
	}

	// Re-promotion fails both ways with distinct sentinels: the promoted id
	// resolves to an active fact ("not proposed" → ErrConflict), and the
	// consumed proposal id no longer resolves at all (→ ErrFactNotFound);
	// its version was replaced on FINAL.
	if _, err := s.PromoteFact(ctx, promoted.ID, "human", "analyst-k"); err == nil ||
		!errors.Is(err, ErrConflict) || !strings.Contains(err.Error(), "not proposed") {
		t.Errorf("promoting an active fact: err = %v, want a not-proposed error wrapping ErrConflict", err)
	}
	if _, err := s.PromoteFact(ctx, prop.ID, "human", "analyst-k"); !errors.Is(err, ErrFactNotFound) {
		t.Errorf("re-promoting a consumed proposal id: err = %v, want ErrFactNotFound", err)
	}

	// Human gate fails closed for agents and junk/empty actor types.
	for _, actor := range []string{"agent", "robot", ""} {
		if _, err := s.PromoteFact(ctx, prop.ID, actor, "someone"); !errors.Is(err, ErrHumanGated) {
			t.Errorf("promote as actor type %q: err = %v, want ErrHumanGated", actor, err)
		}
	}

	// Unknown ids fail cleanly.
	if _, err := s.PromoteFact(ctx, uuid.NewString(), "human", "analyst-k"); !errors.Is(err, ErrFactNotFound) {
		t.Errorf("promoting a nonexistent fact: err = %v, want ErrFactNotFound", err)
	}

	// Audit: exactly one promote_fact row naming the row the transition
	// wrote; summary carries statuses, confidence and the prior proposal's
	// id only, never object values.
	op, actorType, table, summary := queryAudit(t, conn, ctx, promotedUUID)
	if op != "promote_fact" || actorType != "human" || table != "facts" {
		t.Errorf("audit shape wrong: op=%q actor=%q table=%q", op, actorType, table)
	}
	if !strings.Contains(summary, "from=proposed") || !strings.Contains(summary, "to=active") {
		t.Errorf("payload_summary missing transition states: %q", summary)
	}
	if !strings.Contains(summary, "prior="+prop.ID) {
		t.Errorf("payload_summary missing prior id %s: %q", prop.ID, summary)
	}
	for _, secret := range []string{"c2", "benign-parked"} {
		if strings.Contains(summary, secret) {
			t.Errorf("payload_summary leaks %q: %q", secret, summary)
		}
	}
}

// TestRetractFact covers human retraction of an active fact and of a
// proposal. The retracted version must be born closed (invisible to
// validity-window readers even without FINAL), the old version physically
// closed, siblings untouched until retracted themselves, re-retraction and
// agent attempts refused, and the audit row must carry exactly the
// trimmed, 120-rune-capped reason.
func TestRetractFact(t *testing.T) {
	conn := itestConn(t)
	ctx := context.Background()
	s := testService(t, conn)
	scope := itestScope()
	subj := mustResolveEntity(t, conn, scope, "retract.example.com")
	subjUUID, err := uuid.Parse(subj.EntityID)
	if err != nil {
		t.Fatal(err)
	}

	active, err := s.AssertFact(ctx, FactInput{
		Scope:       scope,
		SubjectID:   subj.EntityID,
		Predicate:   "verdict_malicious",
		ObjectValue: "c2",
		Confidence:  0.9,
		ActorType:   "human",
		ActorID:     "analyst-j",
	})
	if err != nil {
		t.Fatal(err)
	}

	// A proposal on the same predicate (different value, so its own sort
	// key): retracting the active fact must leave it alone, and it must
	// itself be retractable afterwards.
	time.Sleep(1100 * time.Millisecond)
	prop, err := s.AssertFact(ctx, FactInput{
		Scope:       scope,
		SubjectID:   subj.EntityID,
		Predicate:   "verdict_malicious",
		ObjectValue: "maybe-bad",
		Confidence:  0.99,
		ActorType:   "agent",
		ActorID:     "triage-bot",
	})
	if err != nil {
		t.Fatal(err)
	}
	if prop.Status != Proposed {
		t.Fatalf("agent non-whitelisted fact = %s, want proposed", prop.Status)
	}

	time.Sleep(1100 * time.Millisecond)
	long := strings.Repeat("r", 125)
	retracted, err := s.RetractFact(ctx, active.ID, "  "+long+"-BEYOND-CAP  ", "human", "analyst-k")
	if err != nil {
		t.Fatal(err)
	}
	if retracted.Status != Retracted || retracted.WrittenBy != "analyst-k" ||
		retracted.ObjectValue != "c2" || retracted.ID == active.ID {
		t.Errorf("retracted fact wrong: %+v", retracted)
	}

	// The active side is gone from the authoritative read path; the
	// sibling proposal remains open and proposed, untouched by the
	// single-fact mutation.
	if n := countOpenFacts(t, ctx, conn, scope, subjUUID, "verdict_malicious"); n != 0 {
		t.Fatalf("open active facts after retract = %d, want 0", n)
	}
	var openProps uint64
	if err := conn.QueryRow(ctx,
		"SELECT count() FROM mem.facts FINAL "+
			"WHERE scope = ? AND subject_id = ? AND predicate = ? "+
			"AND status = 'proposed' AND valid_to > now64(3)",
		scope, subjUUID, "verdict_malicious",
	).Scan(&openProps); err != nil {
		t.Fatal(err)
	}
	if openProps != 1 {
		t.Fatalf("open proposed facts after retracting the active one = %d, want 1", openProps)
	}

	// Born closed: the retracted row's valid_to sits at its own birth,
	// never past valid_from+1s, so validity-window readers never see it.
	retrUUID, _ := uuid.Parse(retracted.ID)
	var (
		st     string
		vf, vt time.Time
	)
	if err := conn.QueryRow(ctx,
		"SELECT status, valid_from, valid_to FROM mem.facts WHERE fact_id = ?", retrUUID,
	).Scan(&st, &vf, &vt); err != nil {
		t.Fatal(err)
	}
	if Status(st) != Retracted {
		t.Errorf("stored status = %q, want retracted", st)
	}
	if vt.Year() > 2100 || vt.After(vf.Add(time.Second)) {
		t.Errorf("retracted row valid_to = %v (valid_from %v), want born closed", vt, vf)
	}

	// The old active side is gone from every read path: FINAL resolves the
	// shared business key to exactly one version — the born-closed retracted
	// replacement — so no open active version remains. (A background merge
	// may have collapsed the old row away physically, so looking it up by
	// its own fact_id is not merge-safe.)
	var openOld uint64
	if err := conn.QueryRow(ctx,
		"SELECT count() FROM mem.facts FINAL "+
			"WHERE scope = ? AND subject_id = ? AND predicate = ? AND object_value = ? "+
			"AND status = 'active' AND valid_to > now64(3)",
		scope, subjUUID, "verdict_malicious", "c2",
	).Scan(&openOld); err != nil {
		t.Fatal(err)
	}
	if openOld != 0 {
		t.Errorf("open active versions after retract = %d, want 0", openOld)
	}

	// Re-retraction fails both ways with distinct sentinels: the consumed
	// active id no longer resolves on FINAL (→ ErrFactNotFound), and the
	// retracted id resolves to a retracted row (→ ErrConflict).
	if _, err := s.RetractFact(ctx, active.ID, "again", "human", "analyst-k"); !errors.Is(err, ErrFactNotFound) {
		t.Errorf("re-retracting a consumed fact id: err = %v, want ErrFactNotFound", err)
	}
	if _, err := s.RetractFact(ctx, retracted.ID, "again", "human", "analyst-k"); err == nil ||
		!errors.Is(err, ErrConflict) || !strings.Contains(err.Error(), "already retracted") {
		t.Errorf("retracting a retracted fact: err = %v, want an already-retracted error wrapping ErrConflict", err)
	}

	// Human gate fails closed for agents and junk/empty actor types.
	for _, actor := range []string{"agent", "robot", ""} {
		if _, err := s.RetractFact(ctx, retracted.ID, "x", actor, "bot"); !errors.Is(err, ErrHumanGated) {
			t.Errorf("retract as actor type %q: err = %v, want ErrHumanGated", actor, err)
		}
	}

	// Unknown ids fail cleanly.
	if _, err := s.RetractFact(ctx, uuid.NewString(), "x", "human", "analyst-k"); !errors.Is(err, ErrFactNotFound) {
		t.Errorf("retracting a nonexistent fact: err = %v, want ErrFactNotFound", err)
	}

	// Audit: exactly one retract_fact row; the reason survives trimmed and
	// hard-capped at 120 runes, with the prior fact's id appended and
	// nothing beyond the cap stored.
	op, actorType, table, summary := queryAudit(t, conn, ctx, retrUUID)
	if op != "retract_fact" || actorType != "human" || table != "facts" {
		t.Errorf("audit shape wrong: op=%q actor=%q table=%q", op, actorType, table)
	}
	if want := "from=active reason=" + long[:120] + " prior=" + active.ID; summary != want {
		t.Errorf("payload_summary = %q, want %q", summary, want)
	}

	// Retract the proposal too: empty reason contributes nothing, and the
	// predicate ends with ZERO open facts of any status for any reader.
	time.Sleep(1100 * time.Millisecond)
	retrProp, err := s.RetractFact(ctx, prop.ID, "", "human", "analyst-k")
	if err != nil {
		t.Fatal(err)
	}
	if retrProp.Status != Retracted {
		t.Errorf("retracted proposal status = %s, want retracted", retrProp.Status)
	}
	if n := countOpenFacts(t, ctx, conn, scope, subjUUID, "verdict_malicious"); n != 0 {
		t.Fatalf("open active facts after retracting everything = %d, want 0", n)
	}
	openProps = 0
	if err := conn.QueryRow(ctx,
		"SELECT count() FROM mem.facts FINAL "+
			"WHERE scope = ? AND subject_id = ? AND predicate = ? "+
			"AND status = 'proposed' AND valid_to > now64(3)",
		scope, subjUUID, "verdict_malicious",
	).Scan(&openProps); err != nil {
		t.Fatal(err)
	}
	if openProps != 0 {
		t.Fatalf("open proposed facts after retracting everything = %d, want 0", openProps)
	}
	propRetrUUID, _ := uuid.Parse(retrProp.ID)
	op, _, _, summary = queryAudit(t, conn, ctx, propRetrUUID)
	if want := "from=proposed prior=" + prop.ID; op != "retract_fact" || summary != want {
		t.Errorf("proposal-retract audit: op=%q summary=%q, want %q (empty reason omitted)", op, summary, want)
	}
}
