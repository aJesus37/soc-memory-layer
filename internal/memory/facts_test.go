package memory

import (
	"context"
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
