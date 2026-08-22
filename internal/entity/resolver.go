package entity

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// Entity is a canonical deduplicated actor stored in mem.entities.
type Entity struct {
	EntityID    string // uuid string
	Scope       string
	EntityType  Type
	Key         string
	DisplayName string
	Attrs       map[string]string
	FirstSeen   time.Time
	LastSeen    time.Time
}

// Resolver resolves raw values to canonical entities in mem.entities,
// creating rows on first sight (lookup-or-create).
type Resolver struct {
	conn driver.Conn
}

func NewResolver(conn driver.Conn) *Resolver {
	return &Resolver{conn: conn}
}

// Resolve normalizes raw, then returns the canonical entity for
// (scope, type, key). Creates it on first sight.
//
// Lookups use FINAL: the table is small at current scale and dedup
// correctness matters more than read throughput; the alternative
// (ORDER BY updated_at DESC LIMIT 1) can be revisited if it ever grows.
func (r *Resolver) Resolve(ctx context.Context, scope, raw string) (Entity, bool, error) {
	n, err := Normalize(raw)
	if err != nil {
		return Entity{}, false, err
	}

	var e Entity
	var typ string // Enum8 does not scan into named string types
	err = r.conn.QueryRow(ctx,
		"SELECT entity_id, scope, entity_type, key, display_name, attrs, first_seen, last_seen "+
			"FROM mem.entities FINAL "+
			"WHERE scope = ? AND entity_type = ? AND key = ?",
		scope, string(n.Type), n.Key,
	).Scan(&e.EntityID, &e.Scope, &typ, &e.Key, &e.DisplayName, &e.Attrs, &e.FirstSeen, &e.LastSeen)
	e.EntityType = Type(typ)

	if err == nil {
		// Hit: re-insert the same row with refreshed last_seen so the
		// ReplacingMergeTree upserts while keeping the entity_id stable
		// for existing references. updated_at is set explicitly so both
		// paths behave identically (column default is now64(3)).
		now := time.Now()
		if err := r.insert(ctx, e.EntityID, e.Scope, e.EntityType, e.Key, e.DisplayName, e.Attrs, e.FirstSeen, now, now); err != nil {
			return Entity{}, false, err
		}
		e.LastSeen = now
		return e, false, nil
	}
	if err != sql.ErrNoRows {
		return Entity{}, false, err
	}

	// Miss: create the entity.
	now := time.Now()
	id := uuid.NewString()
	display := strings.TrimSpace(raw)
	if err := r.insert(ctx, id, scope, n.Type, n.Key, display, nil, now, now, now); err != nil {
		return Entity{}, false, err
	}
	return Entity{
		EntityID:    id,
		Scope:       scope,
		EntityType:  n.Type,
		Key:         n.Key,
		DisplayName: display,
		Attrs:       map[string]string{},
		FirstSeen:   now,
		LastSeen:    now,
	}, true, nil
}

// insert writes one entities row via a batch insert.
func (r *Resolver) insert(ctx context.Context, id, scope string, typ Type, key, displayName string, attrs map[string]string, firstSeen, lastSeen, updatedAt time.Time) error {
	if attrs == nil {
		attrs = map[string]string{}
	}
	b, err := r.conn.PrepareBatch(ctx,
		"INSERT INTO mem.entities "+
			"(entity_id, scope, entity_type, key, display_name, attrs, first_seen, last_seen, updated_at)")
	if err != nil {
		return err
	}
	if err := b.Append(id, scope, string(typ), key, displayName, attrs, firstSeen, lastSeen, updatedAt); err != nil {
		return err
	}
	return b.Send()
}
