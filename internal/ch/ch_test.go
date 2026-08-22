package ch

import (
	"context"
	"os"
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
