package entity

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"socmem/internal/ids"
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
// (scope, type, key). Creates it on first sight. It is a thin wrapper
// over ResolveBatch (single source of truth); unlike the batch form it
// propagates normalization errors instead of yielding zero values.
//
// Concurrency note: concurrent first-sight creates are last-write-wins
// under ReplacingMergeTree; racing callers may each observe a different
// pre-collapse entity_id for the same key. Phase 1 mitigation is that the
// service is single-process; cross-process reconcile tooling is future
// work.
//
// A "hit" is not a pure read: it performs a write (a last_seen refresh
// upsert).
//
// Lookups use FINAL: the table is small at current scale and dedup
// correctness matters more than read throughput; the alternative
// (ORDER BY updated_at DESC LIMIT 1) can be revisited if it ever grows.
func (r *Resolver) Resolve(ctx context.Context, scope, raw string) (Entity, bool, error) {
	items, err := r.resolveAll(ctx, scope, []string{raw})
	if err != nil {
		return Entity{}, false, err
	}
	it := items[0]
	if it.normErr != nil {
		return Entity{}, false, it.normErr
	}
	return it.ent, it.created, nil
}

// ResolveBatch resolves many raw tokens in one round trip:
// a single SELECT for all known keys + one batch insert for misses/refreshes.
// Returns entities in input order. Unnormalizable inputs yield zero-value
// Entity and no error (they are simply not entities).
func (r *Resolver) ResolveBatch(ctx context.Context, scope string, raws []string) ([]Entity, error) {
	items, err := r.resolveAll(ctx, scope, raws)
	if err != nil {
		return nil, err
	}
	out := make([]Entity, len(items))
	for i, it := range items {
		out[i] = it.ent
	}
	return out, nil
}

type resolveItem struct {
	ent     Entity
	created bool
	normErr error // normalization failure; dropped by batches, surfaced by Resolve
}

// resolveAll is the shared lookup-or-create core behind Resolve and
// ResolveBatch. Normalization failures are recorded per item, not fatal;
// database errors abort the whole call.
func (r *Resolver) resolveAll(ctx context.Context, scope string, raws []string) ([]resolveItem, error) {
	items := make([]resolveItem, len(raws))

	type pair struct {
		n      Normalized
		raw    string // first occurrence, used as display name on create
		itemIx []int
		hit    bool
		found  Entity
	}
	keyOf := func(n Normalized) string { return string(n.Type) + "\x00" + n.Key }

	byKey := map[string]*pair{}
	var pairs []*pair
	for i, raw := range raws {
		n, err := Normalize(raw)
		if err != nil {
			items[i].normErr = err
			continue
		}
		p, ok := byKey[keyOf(n)]
		if !ok {
			p = &pair{n: n, raw: raw}
			byKey[keyOf(n)] = p
			pairs = append(pairs, p)
		}
		p.itemIx = append(p.itemIx, i)
	}
	if len(pairs) == 0 {
		return items, nil
	}

	// One SELECT for every unique key.
	var sb strings.Builder
	sb.WriteString("SELECT entity_id, entity_type, key, display_name, attrs, first_seen, last_seen " +
		"FROM mem.entities FINAL WHERE scope = ? AND (entity_type, key) IN (")
	args := []any{scope}
	for i, p := range pairs {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("(?, ?)")
		args = append(args, string(p.n.Type), p.n.Key)
	}
	sb.WriteString(")")

	rows, err := r.conn.Query(ctx, sb.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("entity: lookup %d keys in scope %q: %w", len(pairs), scope, err)
	}
	defer rows.Close()
	for rows.Next() {
		var e Entity
		var typ string // Enum8 does not scan into named string types
		if err := rows.Scan(&e.EntityID, &typ, &e.Key, &e.DisplayName, &e.Attrs, &e.FirstSeen, &e.LastSeen); err != nil {
			return nil, fmt.Errorf("entity: scan lookup row: %w", err)
		}
		e.Scope = scope
		e.EntityType = Type(typ)
		if p, ok := byKey[keyOf(Normalized{Type: Type(typ), Key: e.Key})]; ok && !p.hit {
			p.hit = true
			p.found = e
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("entity: iterate lookup rows: %w", err)
	}

	now := time.Now()
	var writes []*pair
	for _, p := range pairs {
		if p.hit {
			writes = append(writes, p)
			continue
		}
		// Miss: create the entity.
		p.found = Entity{
			EntityID:    ids.New().String(),
			Scope:       scope,
			EntityType:  p.n.Type,
			Key:         p.n.Key,
			DisplayName: strings.TrimSpace(p.raw),
			Attrs:       map[string]string{},
			FirstSeen:   now,
			LastSeen:    now,
		}
		writes = append(writes, p)
	}

	// Hits (last_seen refresh) and misses (create) share ONE batch insert.
	if len(writes) > 0 {
		b, err := r.conn.PrepareBatch(ctx,
			"INSERT INTO mem.entities "+
				"(entity_id, scope, entity_type, key, display_name, attrs, first_seen, last_seen, updated_at)")
		if err != nil {
			return nil, fmt.Errorf("entity: stage %d entity writes: %w", len(writes), err)
		}
		for _, p := range writes {
			attrs := p.found.Attrs
			if attrs == nil {
				attrs = map[string]string{}
			}
			if err := b.Append(p.found.EntityID, scope, string(p.n.Type), p.n.Key,
				p.found.DisplayName, attrs, p.found.FirstSeen, now, now); err != nil {
				return nil, fmt.Errorf("entity: write %s/%s: %w", p.n.Type, p.n.Key, err)
			}
		}
		if err := b.Send(); err != nil {
			return nil, fmt.Errorf("entity: commit %d entity writes: %w", len(writes), err)
		}
	}

	for _, p := range pairs {
		if p.hit {
			p.found.LastSeen = now
		}
		for _, i := range p.itemIx {
			items[i].ent = p.found
			items[i].created = !p.hit
		}
	}
	return items, nil
}
