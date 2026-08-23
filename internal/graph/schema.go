package graph

import (
	"context"
	"fmt"

	"github.com/dgraph-io/dgo/v250/protos/api"
)

// schemaText is idempotent by design: ALTER applies upsert semantics for
// predicates that already match, and re-applying an identical schema is a
// server-side no-op.
//
// related_to carries facets (relation, valid_from, valid_to) written inline on
// each edge. Dgraph v25 removed the @facets schema directive (ALTER rejects it
// with "Invalid index specification"), and facets never need declarations
// unless indexed: they are stored per-edge automatically. Reads must request a
// sub-selection alongside the directive, e.g.
// related_to @facets(relation) { uid }, which returns keys like
// "related_to|relation".
// first_seen/last_seen back the projected entity nodes (Task 5); first_seen
// is hour-indexed for future time-window traversals.
const schemaText = `
	scope: string @index(hash) @upsert .
	key: string @index(hash) @upsert .
	ch_id: string @index(exact) @upsert .
	entity_type: string @index(hash) .
	display_name: string .
	first_seen: datetime @index(hour) .
	last_seen: datetime .
	related_to: [uid] @reverse .

	type Entity {
		ch_id
		scope
		key
		entity_type
		display_name
		first_seen
		last_seen
	}
`

// InstallSchema creates or updates the projection schema via Alter.
func (s *Store) InstallSchema(ctx context.Context) error {
	if err := s.c.Alter(ctx, &api.Operation{Schema: schemaText}); err != nil {
		return fmt.Errorf("graph: install schema: %w", err)
	}
	return nil
}
