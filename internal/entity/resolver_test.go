package entity

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"socmem/internal/ch"
	"socmem/internal/config"
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

func TestResolveOrCreate(t *testing.T) {
	conn := itestConn(t)
	ctx := context.Background()

	scopeA := itestScope()
	scopeB := itestScope()
	r := NewResolver(conn)

	e1, created, err := r.Resolve(ctx, scopeA, "Example.COM")
	if err != nil {
		t.Fatal(err)
	}
	if !created || e1.Key != "example.com" || e1.EntityType != "ioc_domain" {
		t.Fatalf("first resolve wrong: %+v created=%v", e1, created)
	}
	if e1.FirstSeen.IsZero() || e1.LastSeen.IsZero() {
		t.Fatalf("timestamps not set: %+v", e1)
	}

	// last_seen is a DateTime (second granularity); pause so a refresh on
	// the upsert path is distinguishable from the creation-time value.
	time.Sleep(1100 * time.Millisecond)
	tBefore := time.Now()

	e2, created2, err := r.Resolve(ctx, scopeA, "example.com")
	if err != nil {
		t.Fatal(err)
	}
	if created2 {
		t.Fatalf("second resolve should dedup, got create: %+v", e2)
	}
	if e1.EntityID != e2.EntityID {
		t.Fatalf("dedup failed: %s vs %s", e1.EntityID, e2.EntityID)
	}

	var dbLast time.Time
	if err := conn.QueryRow(ctx,
		"SELECT max(last_seen) FROM mem.entities WHERE scope = ? AND entity_type = ? AND key = ?",
		scopeA, "ioc_domain", "example.com",
	).Scan(&dbLast); err != nil {
		t.Fatal(err)
	}
	// Allow one second of slack for DateTime truncation.
	if dbLast.Before(tBefore.Add(-time.Second)) {
		t.Fatalf("upsert did not refresh last_seen: max=%v, want >= ~%v", dbLast, tBefore)
	}

	// same key different scope = different entity
	e3, created3, err := r.Resolve(ctx, scopeB, "example.com")
	if err != nil {
		t.Fatal(err)
	}
	if !created3 || e3.EntityID == e1.EntityID {
		t.Fatalf("scope isolation failed: %+v vs %+v", e3, e1)
	}
}

func TestResolveBatch(t *testing.T) {
	conn := itestConn(t)
	ctx := context.Background()
	scope := itestScope()
	r := NewResolver(conn)

	pre, _, err := r.Resolve(ctx, scope, "existing.example.com")
	if err != nil {
		t.Fatal(err)
	}
	// last_seen is DateTime (second granularity); pause so the batch's
	// hit-refresh is distinguishable from the creation-time value.
	time.Sleep(1100 * time.Millisecond)

	raws := []string{
		"New1.Example.COM",     // new, will dedupe with later variant
		"1.2.3.4",              // new IP
		"existing.example.com", // pre-existing hit
		"not an entity!!",      // unnormalizable -> zero value
		"new1.example.com",     // duplicate of first
		"5.6.7.8",              // new IP
		"T1059.001",            // new technique
	}
	got, err := r.ResolveBatch(ctx, scope, raws)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(raws) {
		t.Fatalf("len(got) = %d, want %d", len(got), len(raws))
	}

	// order + values
	if got[0].EntityType != "ioc_domain" || got[0].Key != "new1.example.com" {
		t.Fatalf("got[0] wrong: %+v", got[0])
	}
	if got[1].EntityType != "ioc_ip" || got[1].Key != "1.2.3.4" {
		t.Fatalf("got[1] wrong: %+v", got[1])
	}
	if got[2].EntityID != pre.EntityID {
		t.Fatalf("hit not mapped in input order: %+v vs pre %+v", got[2], pre)
	}
	if got[4].EntityID != got[0].EntityID {
		t.Fatalf("dedup failed: %+v vs %+v", got[4], got[0])
	}
	if got[5].Key != "5.6.7.8" || got[6].Key != "T1059.001" {
		t.Fatalf("tail entries wrong: %+v %+v", got[5], got[6])
	}
	if got[3].EntityID != "" || got[3].EntityType != "" || got[3].Key != "" {
		t.Fatalf("unnormalizable input should be zero value, got %+v", got[3])
	}

	// created flags via DB check: fresh entities have first_seen == last_seen
	// (created this second); the refreshed hit has last_seen > first_seen.
	var freshFirst, freshLast time.Time
	if err := conn.QueryRow(ctx,
		"SELECT min(first_seen), max(last_seen) FROM mem.entities WHERE scope = ? AND key = ?",
		scope, "new1.example.com",
	).Scan(&freshFirst, &freshLast); err != nil {
		t.Fatal(err)
	}
	if !freshFirst.Equal(freshLast) {
		t.Fatalf("fresh entity timestamps diverge: first=%v last=%v", freshFirst, freshLast)
	}
	var hitFirst, hitLast time.Time
	if err := conn.QueryRow(ctx,
		"SELECT min(first_seen), max(last_seen) FROM mem.entities WHERE scope = ? AND entity_type = ? AND key = ?",
		scope, "ioc_domain", "existing.example.com",
	).Scan(&hitFirst, &hitLast); err != nil {
		t.Fatal(err)
	}
	if !hitLast.After(hitFirst) {
		t.Fatalf("hit was not refreshed: first=%v last=%v", hitFirst, hitLast)
	}

	var n uint64
	if err := conn.QueryRow(ctx,
		// uniqExact rather than count(): the refreshed hit still exists as
		// two unmerged ReplacingMergeTree rows at this point.
		"SELECT uniqExact(entity_id) FROM mem.entities WHERE scope = ?", scope,
	).Scan(&n); err != nil {
		t.Fatal(err)
	}
	// 4 distinct entities: new1.example.com, 1.2.3.4, existing.example.com,
	// 5.6.7.8, T1059.001 — that is 5; the invalid token must not create one.
	if n != 5 {
		t.Fatalf("entity row count = %d, want 5 (invalid token must not persist)", n)
	}
}

func TestResolveBatchEmpty(t *testing.T) {
	conn := itestConn(t)
	ctx := context.Background()
	scope := itestScope()
	r := NewResolver(conn)
	got, err := r.ResolveBatch(ctx, scope, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("len(got) = %d, want 0", len(got))
	}
	allInvalid, err := r.ResolveBatch(ctx, scope, []string{"nope", "also nope"})
	if err != nil {
		t.Fatal(err)
	}
	for i, e := range allInvalid {
		if e.EntityID != "" || e.EntityType != "" || e.Key != "" {
			t.Fatalf("all-invalid batch[%d] = %+v, want zero value", i, e)
		}
	}
	var n uint64
	if err := conn.QueryRow(ctx,
		"SELECT uniqExact(entity_id) FROM mem.entities WHERE scope = ?", scope,
	).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("entity rows in %s = %d, want 0", scope, n)
	}
}
