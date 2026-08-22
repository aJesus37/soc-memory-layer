package memory

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"socmem/internal/ch"
	"socmem/internal/config"
	"socmem/internal/embed"
	"socmem/internal/entity"
)

func itestConn(t *testing.T) driver.Conn {
	t.Helper()
	addr := os.Getenv("MEM_TEST_CH_ADDR")
	if addr == "" {
		t.Skip("set MEM_TEST_CH_ADDR to run")
	}
	cfg := config.Load()
	ctx := context.Background()
	conn, err := ch.Connect(ctx, addr, cfg.ChUser, cfg.ChPassword, "default")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	if err := ch.Migrate(ctx, conn, cfg); err != nil {
		t.Fatal(err)
	}
	return conn
}

// itestScope returns a scope unique per test invocation. Tests isolate via
// scope instead of TRUNCATE so parallel packages sharing one ClickHouse
// never destroy each other's rows mid-test.
func itestScope() string {
	return fmt.Sprintf("itest-%x", time.Now().UnixNano())
}

func testService(t *testing.T, conn driver.Conn) *Service {
	t.Helper()
	return New(conn, entity.NewResolver(conn), embed.NewFake(8), config.Load())
}

type failingEmbedder struct{ err error }

func (f failingEmbedder) Embed(context.Context, string, []string) ([][]float32, error) {
	return nil, f.err
}

func TestRecordObservation(t *testing.T) {
	conn := itestConn(t)
	ctx := context.Background()
	s := testService(t, conn)
	scope := itestScope()

	const content = "Saw 1.2.3.4 beaconing to evil.example.com"
	o, err := s.RecordObservation(ctx, Input{
		Scope:     scope,
		Kind:      "human_statement",
		ActorType: "human",
		ActorID:   "analyst-j",
		Content:   content,
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := uuid.Parse(o.ID); err != nil {
		t.Fatalf("observation ID %q not a UUID: %v", o.ID, err)
	}
	if o.Scope != scope || o.Kind != "human_statement" ||
		o.ActorType != "human" || o.ActorID != "analyst-j" {
		t.Fatalf("echoed fields wrong: %+v", o)
	}
	if !o.Embedded {
		t.Error("Embedded = false, want true with working embedder")
	}
	if len(o.EntityIDs) != 2 {
		t.Fatalf("len(EntityIDs) = %d (%v), want 2", len(o.EntityIDs), o.EntityIDs)
	}

	obsID, err := uuid.Parse(o.ID)
	if err != nil {
		t.Fatal(err)
	}

	var (
		dbContent string
		vecLen    uint64
		refLen    uint64
		refUniq   uint32 // arrayUniq returns UInt32
		dbTS      time.Time
	)
	if err := conn.QueryRow(ctx,
		"SELECT content, length(content_vec), length(entity_refs), ts "+
			"FROM mem.observations WHERE obs_id = ?",
		obsID,
	).Scan(&dbContent, &vecLen, &refLen, &dbTS); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx,
		"SELECT arrayUniq(entity_refs) FROM mem.observations WHERE obs_id = ?",
		obsID,
	).Scan(&refUniq); err != nil {
		t.Fatal(err)
	}
	if dbContent != content {
		t.Errorf("content not stored verbatim:\n got %q\nwant %q", dbContent, content)
	}
	if vecLen != 8 {
		t.Errorf("length(content_vec) = %d, want 8 (fake embedder dim)", vecLen)
	}
	if refLen != 2 || refUniq != 2 {
		t.Errorf("entity refs len=%d uniq=%d, want 2/2 distinct", refLen, refUniq)
	}
	if d := dbTS.Unix() - time.Now().Unix(); d < -5 || d > 5 {
		t.Errorf("ts outside now±5s: %v", dbTS)
	}

	var (
		dbScope   string
		dbKind    string
		actorType string
		actorID   string
		onBehalf  string
		conf      string
		caseID    *string
	)
	if err := conn.QueryRow(ctx,
		"SELECT scope, kind, actor_type, actor_id, on_behalf_of, confidentiality, case_id "+
			"FROM mem.observations WHERE obs_id = ?",
		obsID,
	).Scan(&dbScope, &dbKind, &actorType, &actorID, &onBehalf, &conf, &caseID); err != nil {
		t.Fatal(err)
	}
	if dbScope != scope || dbKind != "human_statement" || actorType != "human" ||
		actorID != "analyst-j" || onBehalf != "" || conf != "internal" {
		t.Errorf("stored columns wrong: scope=%q kind=%q actor_type=%q actor_id=%q on_behalf_of=%q conf=%q",
			dbScope, dbKind, actorType, actorID, onBehalf, conf)
	}
	if caseID != nil {
		t.Errorf("case_id = %v, want NULL", *caseID)
	}

	id0, err := uuid.Parse(o.EntityIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	id1, err := uuid.Parse(o.EntityIDs[1])
	if err != nil {
		t.Fatal(err)
	}
	rows, err := conn.Query(ctx,
		"SELECT entity_type, key FROM mem.entities FINAL WHERE entity_id IN (?, ?)", id0, id1)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	gotEnts := map[string][]string{}
	for rows.Next() {
		var typ, key string
		if err := rows.Scan(&typ, &key); err != nil {
			t.Fatal(err)
		}
		gotEnts[typ] = append(gotEnts[typ], key)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	wantEnts := map[string][]string{
		"ioc_ip":     {"1.2.3.4"},
		"ioc_domain": {"evil.example.com"},
	}
	if len(gotEnts["ioc_ip"]) != 1 || gotEnts["ioc_ip"][0] != wantEnts["ioc_ip"][0] ||
		len(gotEnts["ioc_domain"]) != 1 || gotEnts["ioc_domain"][0] != wantEnts["ioc_domain"][0] {
		t.Errorf("linked entities wrong: got %v, want %v", gotEnts, wantEnts)
	}

	// audit row: present, correct shape, and provably content-free.
	aOp, aActorType, aTable, aSummary := queryAudit(t, conn, ctx, obsID)
	if aOp != "record_observation" || aActorType != "human" || aTable != "observations" {
		t.Errorf("audit shape wrong: op=%q actor_type=%q target_table=%q", aOp, aActorType, aTable)
	}
	for _, secret := range []string{"1.2.3.4", "evil.example.com", "beaconing", "Saw"} {
		if strings.Contains(aSummary, secret) {
			t.Errorf("payload_summary leaks %q: %q", secret, aSummary)
		}
	}
}

func queryAudit(t *testing.T, conn driver.Conn, ctx context.Context, obsID uuid.UUID) (op, actorType, table, summary string) {
	t.Helper()
	var count uint64
	if err := conn.QueryRow(ctx,
		"SELECT count() FROM mem.audit WHERE target_id = ?", obsID,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("audit rows for %s = %d, want exactly 1", obsID, count)
	}
	if err := conn.QueryRow(ctx,
		"SELECT operation, actor_type, target_table, payload_summary "+
			"FROM mem.audit WHERE target_id = ?", obsID,
	).Scan(&op, &actorType, &table, &summary); err != nil {
		t.Fatal(err)
	}
	return op, actorType, table, summary
}

func TestRecordObservationEmbedderDown(t *testing.T) {
	conn := itestConn(t)
	ctx := context.Background()
	s := testService(t, conn)
	s.embedder = failingEmbedder{err: errors.New("embedder down")}

	o, err := s.RecordObservation(ctx, Input{
		Scope:      itestScope(),
		Kind:       "alert",
		ActorType:  "agent",
		ActorID:    "sensor-7",
		OnBehalfOf: "analyst-j",
		Content:    "Suspicious T1059.001 activity from 10.0.0.9",
	})
	if err != nil {
		t.Fatalf("embedder failure must not fail the write, got error: %v", err)
	}
	if o.Embedded {
		t.Error("Embedded = true, want false with failing embedder")
	}
	if len(o.EntityIDs) != 2 {
		t.Errorf("len(EntityIDs) = %d, want 2 (resolution independent of embedding)", len(o.EntityIDs))
	}

	obsID, err := uuid.Parse(o.ID)
	if err != nil {
		t.Fatal(err)
	}
	var vecLen uint64
	var content string
	if err := conn.QueryRow(ctx,
		"SELECT length(content_vec), content FROM mem.observations WHERE obs_id = ?",
		obsID,
	).Scan(&vecLen, &content); err != nil {
		t.Fatal(err)
	}
	if vecLen != 0 {
		t.Errorf("length(content_vec) = %d, want 0 (empty array)", vecLen)
	}
	if content != "Suspicious T1059.001 activity from 10.0.0.9" {
		t.Errorf("content altered on fallback path: %q", content)
	}
}

func TestRecordObservationValidation(t *testing.T) {
	conn := itestConn(t)
	ctx := context.Background()
	s := testService(t, conn)

	scope := itestScope()
	bad := map[string]Input{
		"empty content":    {Scope: scope, Kind: "alert", ActorType: "human", ActorID: "a"},
		"blank content":    {Scope: scope, Kind: "alert", ActorType: "human", ActorID: "a", Content: "   \t "},
		"bad kind":         {Scope: scope, Kind: "gossip", ActorType: "human", ActorID: "a", Content: "x"},
		"bad actor type":   {Scope: scope, Kind: "alert", ActorType: "robot", ActorID: "a", Content: "x"},
		"empty scope":      {Kind: "alert", ActorType: "human", ActorID: "a", Content: "x"},
		"blank scope":      {Scope: "  ", Kind: "alert", ActorType: "human", ActorID: "a", Content: "x"},
		"empty actor id":   {Scope: scope, Kind: "alert", ActorType: "human", Content: "x"},
		"bad conf":         {Scope: scope, Kind: "alert", ActorType: "human", ActorID: "a", Confidentiality: "public", Content: "x"},
		"malformed caseid": {Scope: scope, Kind: "alert", ActorType: "human", ActorID: "a", CaseID: "not-a-uuid", Content: "x"},
	}
	for name, in := range bad {
		if _, err := s.RecordObservation(ctx, in); err == nil {
			t.Errorf("%s: expected validation error, got nil", name)
		}
	}

	var n uint64
	if err := conn.QueryRow(ctx,
		"SELECT count() FROM mem.observations WHERE scope = ?", scope,
	).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("invalid inputs wrote %d observation rows in %s, want 0", n, scope)
	}

	// agent without OnBehalfOf is explicitly allowed (OnBehalfOf optional).
	o, err := s.RecordObservation(ctx, Input{
		Scope:     scope,
		Kind:      "agent_action",
		ActorType: "agent",
		ActorID:   "hunter-1",
		Content:   "automated sweep finished",
	})
	if err != nil {
		t.Fatalf("agent without OnBehalfOf must be accepted, got: %v", err)
	}
	_ = o
}

func TestRecordObservationNoEntities(t *testing.T) {
	conn := itestConn(t)
	ctx := context.Background()
	s := testService(t, conn)
	scope := itestScope()

	o, err := s.RecordObservation(ctx, Input{
		Scope:     scope,
		Kind:      "investigation_note",
		ActorType: "human",
		ActorID:   "analyst-j",
		Content:   "plain prose without any indicators whatsoever here",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(o.EntityIDs) != 0 {
		t.Errorf("EntityIDs = %v, want none", o.EntityIDs)
	}
	if !o.Embedded {
		t.Error("Embedded = false, want true")
	}

	obsID, err := uuid.Parse(o.ID)
	if err != nil {
		t.Fatal(err)
	}
	var refLen uint64
	if err := conn.QueryRow(ctx,
		"SELECT length(entity_refs) FROM mem.observations WHERE obs_id = ?", obsID,
	).Scan(&refLen); err != nil {
		t.Fatal(err)
	}
	if refLen != 0 {
		t.Errorf("entity_refs length = %d, want 0", refLen)
	}
	var ents uint64
	if err := conn.QueryRow(ctx,
		"SELECT uniqExact(entity_id) FROM mem.entities WHERE scope = ?", scope,
	).Scan(&ents); err != nil {
		t.Fatal(err)
	}
	if ents != 0 {
		t.Errorf("entities created in %s = %d, want 0", scope, ents)
	}
}

func TestRecordObservationCaseID(t *testing.T) {
	conn := itestConn(t)
	ctx := context.Background()
	s := testService(t, conn)

	scope := itestScope()
	caseID := uuid.NewString()
	in := Input{
		Scope:     scope,
		Kind:      "triage_decision",
		ActorType: "human",
		ActorID:   "analyst-j",
		CaseID:    caseID,
		Content:   "escalated to case",
	}
	o, err := s.RecordObservation(ctx, in)
	if err != nil {
		t.Fatal(err)
	}

	obsID, _ := uuid.Parse(o.ID)
	var gotCase *string
	if err := conn.QueryRow(ctx,
		"SELECT case_id FROM mem.observations WHERE obs_id = ?", obsID,
	).Scan(&gotCase); err != nil {
		t.Fatal(err)
	}
	if gotCase == nil || *gotCase != caseID {
		t.Fatalf("case_id = %v, want %s", gotCase, caseID)
	}

	o2, err := s.RecordObservation(ctx, Input{
		Scope: itestScope(), Kind: "alert", ActorType: "human", ActorID: "a", Content: "no case",
	})
	if err != nil {
		t.Fatal(err)
	}
	obsID2, _ := uuid.Parse(o2.ID)
	var nullCount uint64
	if err := conn.QueryRow(ctx,
		"SELECT count() FROM mem.observations WHERE obs_id = ? AND case_id IS NULL", obsID2,
	).Scan(&nullCount); err != nil {
		t.Fatal(err)
	}
	if nullCount != 1 {
		t.Errorf("empty CaseID did not store NULL, matched rows=%d", nullCount)
	}
}
