package entity

import (
	"context"
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

func TestResolveOrCreate(t *testing.T) {
	conn := itestConn(t)
	ctx := context.Background()
	// clean slate for deterministic test
	if err := conn.Exec(ctx, "TRUNCATE TABLE mem.entities"); err != nil {
		t.Fatal(err)
	}

	r := NewResolver(conn)

	e1, created, err := r.Resolve(ctx, "default", "Example.COM")
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

	e2, created2, err := r.Resolve(ctx, "default", "example.com")
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
		"default", "ioc_domain", "example.com",
	).Scan(&dbLast); err != nil {
		t.Fatal(err)
	}
	// Allow one second of slack for DateTime truncation.
	if dbLast.Before(tBefore.Add(-time.Second)) {
		t.Fatalf("upsert did not refresh last_seen: max=%v, want >= ~%v", dbLast, tBefore)
	}

	// same key different scope = different entity
	e3, created3, err := r.Resolve(ctx, "team-a", "example.com")
	if err != nil {
		t.Fatal(err)
	}
	if !created3 || e3.EntityID == e1.EntityID {
		t.Fatalf("scope isolation failed: %+v vs %+v", e3, e1)
	}
}
