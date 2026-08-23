package ch

import (
	"errors"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// IsStorageError reports whether err is — or wraps — a clickhouse-go driver
// error: *clickhouse.Exception for server-side failures (query rejected,
// connection reset, ...) or *clickhouse.OpError for client-side ones (column
// conversion, row append). The API layer uses it to map such faults to 502
// instead of misreading them as bad requests; check with errors.Is-style
// unwrapping through arbitrary fmt.Errorf chains.
func IsStorageError(err error) bool {
	var (
		exc *clickhouse.Exception
		op  *clickhouse.OpError
	)
	return errors.As(err, &exc) || errors.As(err, &op)
}
