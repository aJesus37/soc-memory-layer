package ch

import (
	"context"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"socmem/internal/config"
)

func testConn(t *testing.T) driver.Conn {
	t.Helper()
	addr := os.Getenv("MEM_TEST_CH_ADDR")
	if addr == "" {
		t.Skip("MEM_TEST_CH_ADDR not set; skipping ClickHouse integration test")
	}
	cfg := config.Load()
	ctx := context.Background()
	conn, err := Connect(ctx, addr, cfg.ChUser, cfg.ChPassword, "default")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := conn.Ping(pingCtx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	return conn
}

func TestConnectPings(t *testing.T) {
	c := testConn(t)
	if err := c.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestMigrateIdempotent(t *testing.T) {
	ctx := context.Background()
	conn := testConn(t)
	if err := Migrate(ctx, conn, config.Load()); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, conn, config.Load()); err != nil {
		t.Fatal(err)
	}
	names, err := migrationNames()
	if err != nil {
		t.Fatal(err)
	}
	var n uint64
	if err := conn.QueryRow(ctx,
		"SELECT count() FROM mem.schema_migrations WHERE database = 'mem'").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != uint64(len(names)) {
		t.Errorf("recorded migrations = %d, want %d (one per embedded .sql file)", n, len(names))
	}
}

// TestProjectionWatermarkSeeded asserts the migration seeds exactly one
// epoch row per projected source, and that a double Migrate (the mid-file
// failure rerun case) does not double them: plain MergeTree never collapses
// duplicate inserts, so the seed's WHERE NOT EXISTS guard is load-bearing.
func TestProjectionWatermarkSeeded(t *testing.T) {
	ctx := context.Background()
	conn := testConn(t)
	// Reset to a pre-migration state so the seed path itself is exercised on
	// every run instead of trusting leftovers from earlier suites.
	if err := conn.Exec(ctx, "DROP TABLE IF EXISTS mem.projection_watermark"); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx,
		"ALTER TABLE mem.schema_migrations DELETE WHERE name = '002_projection.sql' "+
			"SETTINGS mutations_sync = 1"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := Migrate(ctx, conn, config.Load()); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := conn.Query(ctx,
		"SELECT name, ts FROM mem.projection_watermark ORDER BY name")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []struct {
		Name string
		TS   time.Time
	}
	for rows.Next() {
		var r struct {
			Name string
			TS   time.Time
		}
		if err := rows.Scan(&r.Name, &r.TS); err != nil {
			t.Fatal(err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	epoch := time.Unix(0, 0).UTC()
	want := []struct {
		Name string
		TS   time.Time
	}{
		{Name: "edges", TS: epoch},
		{Name: "entities", TS: epoch},
	}
	if len(got) != len(want) {
		t.Fatalf("projection_watermark rows = %v, want exactly %v", got, want)
	}
	for i := range want {
		if got[i].Name != want[i].Name {
			t.Errorf("row %d name = %q, want %q", i, got[i].Name, want[i].Name)
		}
		if !got[i].TS.Equal(epoch) {
			t.Errorf("watermark %q ts = %v, want epoch", got[i].Name, got[i].TS)
		}
	}
}

func TestSchemaTables(t *testing.T) {
	ctx := context.Background()
	conn := testConn(t)
	if err := Migrate(ctx, conn, config.Load()); err != nil {
		t.Fatal(err)
	}
	rows, err := conn.Query(ctx, "SHOW TABLES FROM mem")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		got = append(got, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"entities",
		"observations",
		"facts",
		"edges",
		"audit",
		"projection_watermark",
		"schema_migrations",
	}
	sort.Strings(got)
	sort.Strings(want)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("SHOW TABLES FROM mem = %v, want %v", got, want)
	}
}

func TestSplitStatements(t *testing.T) {
	tests := []struct {
		name   string
		script string
		want   []string
	}{
		{"simple", "A;\nB;", []string{"A", "B"}},
		{"trailing semicolon optional", "A;", []string{"A"}},
		{"semicolon in string", "INSERT INTO t VALUES ('a;b');\nC;",
			[]string{"INSERT INTO t VALUES ('a;b')", "C"}},
		{"escaped quote in string", `INSERT INTO t VALUES ('it\'s;fine');`,
			[]string{`INSERT INTO t VALUES ('it\'s;fine')`}},
		{"doubled quote in string", "INSERT INTO t VALUES ('it''s;fine');",
			[]string{"INSERT INTO t VALUES ('it''s;fine')"}},
		{"semicolon in line comment", "-- note; not a boundary\nA;", []string{"-- note; not a boundary\nA"}},
		{"semicolon in block comment", "/* x;y */ A;", []string{"/* x;y */ A"}},
		{"comment-only tail dropped", "A;\n-- done\n", []string{"A"}},
		{"backtick identifier", "SELECT `col;a` FROM t;", []string{"SELECT `col;a` FROM t"}},
		{"no trailing semicolon", "A;\nB", []string{"A", "B"}},
		{"empty input", "", nil},
		{"whitespace only", "  \n\t ", nil},
		{"nested block comment", "/* a /* b; */ x;y */ A;", []string{"/* a /* b; */ x;y */ A"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := splitStatements(tt.script)
			if strings.Join(got, "|") != strings.Join(tt.want, "|") {
				t.Errorf("splitStatements(%q) = %#v, want %#v", tt.script, got, tt.want)
			}
		})
	}
}
