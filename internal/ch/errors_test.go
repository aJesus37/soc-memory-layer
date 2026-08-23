package ch

import (
	"errors"
	"fmt"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// TestIsStorageError pins the storage-fault classifier used by the API layer
// to map ClickHouse failures to 502: both server-side exceptions (native
// protocol) and client-side op errors (column conversion) must classify as
// storage errors through arbitrary fmt.Errorf %w wrapping, while service
// sentinels and plain errors must not.
func TestIsStorageError(t *testing.T) {
	serverEx := &clickhouse.Exception{
		Code:    60,
		Name:    "UNKNOWN_TABLE",
		Message: "Table default.nope does not exist",
	}
	opErr := &clickhouse.OpError{
		Op:         "Append",
		ColumnName: "ts",
		Err:        errors.New("cannot convert \"x\" to DateTime64"),
	}

	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"plain error", errors.New("boom"), false},
		{"wrapped plain error", fmt.Errorf("memory: commit observation x: %w", errors.New("boom")), false},
		{"bare exception", serverEx, true},
		{"wrapped exception", fmt.Errorf("memory: similar query scope %q: %w", "team-a", serverEx), true},
		{"doubly wrapped exception", fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", serverEx)), true},
		{"bare op error", opErr, true},
		{"wrapped op error", fmt.Errorf("memory: append fact row: %w", opErr), true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := IsStorageError(c.err); got != c.want {
				t.Errorf("IsStorageError(%v) = %v, want %v", c.err, got, c.want)
			}
		})
	}
}
