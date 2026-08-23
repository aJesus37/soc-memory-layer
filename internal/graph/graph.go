// Package graph provides a thin wrapper around a Dgraph client connection
// used by the projection and traversal layers.
package graph

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/dgraph-io/dgo/v250"
	"github.com/dgraph-io/dgo/v250/protos/api"
)

const pingQuery = `{ ping(func: has(key)) { uid } }`

// Store wraps a connected Dgraph client.
type Store struct{ c *dgo.Dgraph }

// Connect opens a gRPC connection to Dgraph at addr ("host:port" or a full
// dgraph:// URL) and verifies it is usable with a read against storage before
// returning. The caller must Close the returned Store.
func Connect(ctx context.Context, addr string) (*Store, error) {
	dsn := addr
	if !strings.Contains(dsn, "://") {
		dsn = "dgraph://" + dsn
	}
	c, err := dgo.Open(dsn)
	if err != nil {
		return nil, fmt.Errorf("graph: open %s: %w", addr, err)
	}
	s := &Store{c: c}
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := s.Ping(pingCtx); err != nil {
		c.Close()
		return nil, fmt.Errorf("graph: ping %s: %w", addr, err)
	}
	return s, nil
}

// Close releases the underlying gRPC connections.
func (s *Store) Close() error {
	if s == nil || s.c == nil {
		return nil
	}
	s.c.Close() // dgo v250 Close returns no error
	return nil
}

// Dgraph exposes the raw client for code that needs full DQL access.
func (s *Store) Dgraph() *dgo.Dgraph { return s.c }

// Ping runs a cheap read-only has(key) query so it exercises storage, not
// just the gRPC dial.
func (s *Store) Ping(ctx context.Context) error {
	if _, err := s.c.NewReadOnlyTxn().Query(ctx, pingQuery); err != nil {
		return fmt.Errorf("graph: ping: %w", err)
	}
	return nil
}

// DropData removes every node while keeping the installed schema.
func (s *Store) DropData(ctx context.Context) error {
	if err := s.c.Alter(ctx, &api.Operation{DropOp: api.Operation_DATA}); err != nil {
		return fmt.Errorf("graph: drop data: %w", err)
	}
	return nil
}
