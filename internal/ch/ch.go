package ch

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"socmem/internal/config"
)

//go:embed all:migrations
var migrationsFS embed.FS

const (
	migrationsDir = "migrations"
	targetDB      = "mem"
	trackingTable = targetDB + ".schema_migrations"
)

// Connect opens a native-protocol connection to ClickHouse and pings it
// before returning. The caller owns the returned conn and must Close it.
func Connect(ctx context.Context, addr, user, pass string, database string) (driver.Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: database,
			Username: user,
			Password: pass,
		},
		DialTimeout: 10 * time.Second,
		Settings: map[string]any{
			"max_execution_time": 60,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("ch: open %s: %w", addr, err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := conn.Ping(pingCtx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("ch: ping %s: %w", addr, err)
	}
	return conn, nil
}

// ConnectConfig connects using the address and credentials loaded into cfg.
// database selects the database to connect to, not the target migration
// database.
func ConnectConfig(ctx context.Context, cfg config.Config, database string) (driver.Conn, error) {
	return Connect(ctx, cfg.ChAddr, cfg.ChUser, cfg.ChPassword, database)
}

// Migrate creates the mem database, ensures the schema_migrations tracking
// table exists, then applies each embedded migration whose filename has not
// been recorded yet. Files run once each in filename order; multi-statement
// files are supported.
//
// Reruns are idempotent at the file level (each file is applied at most once
// per database). ClickHouse has no DDL transactions, so a migration that
// fails mid-file may leave some statements already applied; on rerun those
// statements are re-sent. Migration statements MUST therefore use IF NOT
// EXISTS or other idempotent forms so that a mid-file failure followed by a
// rerun neither errors nor corrupts state. This convention is enforced by
// review, not by the runner.
//
// Concurrent invocations from separate processes may race between the
// applied-check and the record insert and double-record a migration; start
// migrations from a single process.
func Migrate(ctx context.Context, conn driver.Conn, _ config.Config) error {
	if err := conn.Exec(ctx,
		fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", targetDB)); err != nil {
		return fmt.Errorf("create database: %w", err)
	}
	if err := conn.Exec(ctx,
		"CREATE TABLE IF NOT EXISTS "+trackingTable+" ("+
			"`database` String, "+
			"`name` String, "+
			"`applied_at` DateTime DEFAULT now()) "+
			"ENGINE = MergeTree ORDER BY (`database`, `name`)"); err != nil {
		return fmt.Errorf("create tracking table: %w", err)
	}

	names, err := migrationNames()
	if err != nil {
		return err
	}
	for _, name := range names {
		applied, err := migrationApplied(ctx, conn, name)
		if err != nil {
			return err
		}
		if applied {
			continue
		}
		body, err := migrationsFS.ReadFile(migrationsDir + "/" + name)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", name, err)
		}
		for _, stmt := range splitStatements(string(body)) {
			if err := conn.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("apply migration %s: %w", name, err)
			}
		}
		if err := conn.Exec(ctx,
			"INSERT INTO "+trackingTable+" (`database`, `name`) VALUES (?, ?)",
			targetDB, name); err != nil {
			return fmt.Errorf("record migration %s: %w", name, err)
		}
	}
	return nil
}

func migrationNames() ([]string, error) {
	entries, err := fs.ReadDir(migrationsFS, migrationsDir)
	if err != nil {
		return nil, fmt.Errorf("list migrations: %w", err)
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		names = append(names, e.Name())
	}
	sort.Strings(names)
	return names, nil
}

func migrationApplied(ctx context.Context, conn driver.Conn, name string) (bool, error) {
	var n uint64
	if err := conn.QueryRow(ctx,
		"SELECT count() FROM "+trackingTable+" WHERE `database` = ? AND `name` = ?",
		targetDB, name,
	).Scan(&n); err != nil {
		return false, fmt.Errorf("check migration %s: %w", name, err)
	}
	return n > 0, nil
}

// splitStatements splits a migration script into individual statements on
// top-level semicolons. The native protocol rejects multi-statement queries
// (server error 62 "Multi-statements are not allowed"), so statements must be
// sent one Exec per query. Semicolons inside quoted strings, quoted
// identifiers, line comments (-- to end of line) and block comments (/* */,
// nested to a fixed depth) do not split. Fragments holding only whitespace or
// comments are dropped.
func splitStatements(script string) []string {
	var (
		stmts      []string
		current    strings.Builder
		hasContent bool
		quote      byte
	)
	flush := func() {
		if hasContent {
			if s := strings.TrimSpace(current.String()); s != "" {
				stmts = append(stmts, s)
			}
		}
		current.Reset()
		hasContent = false
	}
	for i := 0; i < len(script); i++ {
		c := script[i]
		switch {
		case quote != 0:
			current.WriteByte(c)
			hasContent = true
			switch {
			case c == '\\' && quote == '\'':
				if i+1 < len(script) {
					current.WriteByte(script[i+1])
					i++
				}
			case c == quote && quote == '\'' && i+1 < len(script) && script[i+1] == quote:
				current.WriteByte(quote)
				i++
			case c == quote:
				quote = 0
			}
		case c == '\'' || c == '"' || c == '`':
			quote = c
			current.WriteByte(c)
			hasContent = true
		case c == '-' && i+1 < len(script) && script[i+1] == '-':
			j := strings.IndexByte(script[i:], '\n')
			if j < 0 {
				current.WriteString(script[i:])
				i = len(script)
			} else {
				current.WriteString(script[i : i+j])
				i += j - 1
			}
		case c == '/' && i+1 < len(script) && script[i+1] == '*':
			end := i + 2
			for depth := 1; depth > 0; {
				if end+1 >= len(script) {
					end = len(script)
					break
				}
				switch {
				case script[end] == '/' && script[end+1] == '*':
					depth++
					end += 2
				case script[end] == '*' && script[end+1] == '/':
					depth--
					end += 2
				default:
					end++
				}
			}
			current.WriteString(script[i:end])
			i = end - 1
		case c == ';':
			flush()
		default:
			if c != ' ' && c != '\n' && c != '\r' && c != '\t' {
				hasContent = true
			}
			current.WriteByte(c)
		}
	}
	flush()
	return stmts
}
