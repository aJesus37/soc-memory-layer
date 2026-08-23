package graph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/dgraph-io/dgo/v250"
	"github.com/dgraph-io/dgo/v250/protos/api"
)

const (
	// watermarkEntities is the mem.projection_watermark row driving
	// ProjectEntities; watermarkEdges belongs to Task 7.
	watermarkEntities = "entities"
	watermarkEdges    = "edges"

	defaultBatch = 500

	// nilEntityID stands in for "no tiebreaker yet" when binding the epoch
	// cursor: entity_id is a UUID column, so the tuple comparison needs a
	// parseable value that sorts before every real id.
	nilEntityID = "00000000-0000-0000-0000-000000000000"

	entityPageQuery = "SELECT entity_id, scope, entity_type, key, display_name, " +
		"first_seen, last_seen, updated_at FROM mem.entities FINAL " +
		"WHERE (updated_at, entity_id) > (?, ?) " +
		"ORDER BY updated_at ASC, entity_id ASC LIMIT ?"
)

// ProjectEntities projects mem.entities rows newer than the stored watermark
// into Dgraph nodes keyed by their ClickHouse UUID (the exact-indexed,
// @upsert ch_id predicate), then advances the watermark row named
// "entities". It returns the number of entities projected in this call.
// batch bounds the page size; values <= 0 select defaultBatch.
//
// Watermark invariant (exactly-once per version, idempotent under replay):
//
//   - The cursor is the composite (ts, last_id). Pages are read with strict
//     tuple comparison WHERE (updated_at, entity_id) > (?, ?) ORDER BY
//     updated_at, entity_id LIMIT batch. The secondary entity_id key makes
//     the total order deterministic across runs, so keyset pagination
//     neither skips nor repeats rows: every row whose (updated_at,
//     entity_id) sorts after the cursor is projected exactly once. FINAL
//     collapses ReplacingMergeTree versions first, so an entity appears once
//     in its latest version.
//   - After a non-empty page, the cursor moves to the last row's
//     (updated_at, entity_id) — strictly forward in the total order, never a
//     rewind. An empty page changes nothing.
//   - Node writes are upserts keyed on ch_id inside one atomic txn.Do
//     request (query var + conditional mutations), so replaying any window —
//     e.g. after a manual cursor reset or graphrebuild — converges: same
//     ch_id resolves to the same uid and identical content.
//   - Any error returns BEFORE the cursor advances; a crash between the
//     Dgraph write and the watermark UPDATE replays the same page on the
//     next call, harmlessly.
//
// Consistency contract: a refresh committing with updated_at below the
// advanced cursor while a tick is in flight is picked up by that entity's
// NEXT touch (every Resolve writes a fresh version). The projection is
// eventually consistent by design; ClickHouse remains sole truth and the
// graph is rebuildable at any time via DropData + watermark reset + replay.
func ProjectEntities(ctx context.Context, st *Store, conn driver.Conn, batch int) (int, error) {
	if batch <= 0 {
		batch = defaultBatch
	}
	if st == nil || conn == nil {
		return 0, errors.New("graph: project entities: nil store or connection")
	}

	ts, lastID, exists, err := readCursor(ctx, conn, watermarkEntities)
	if err != nil {
		return 0, err
	}
	if lastID == "" {
		lastID = nilEntityID
	}
	page, err := queryEntityRows(ctx, conn, formatCHTimestamp(ts), lastID, batch)
	if err != nil {
		return 0, err
	}
	for _, e := range page {
		if err := upsertEntityNode(ctx, st.Dgraph(), e); err != nil {
			return 0, err
		}
	}
	if len(page) == 0 {
		return 0, nil
	}
	last := page[len(page)-1]
	stamp := formatCHTimestamp(last.UpdatedAt)
	if exists {
		err = updateCursor(ctx, conn, watermarkEntities, stamp, last.EntityID)
	} else {
		err = seedCursor(ctx, conn, watermarkEntities, stamp, last.EntityID)
	}
	if err != nil {
		return 0, err
	}
	return len(page), nil
}

// chEntity is one mem.entities row (FINAL-collapsed) as needed for
// projection.
type chEntity struct {
	EntityID    string
	Scope       string
	EntityType  string
	Key         string
	DisplayName string
	FirstSeen   time.Time
	LastSeen    time.Time
	UpdatedAt   time.Time
}

func queryEntityRows(ctx context.Context, conn driver.Conn, ts, lastID string, batch int) ([]chEntity, error) {
	rows, err := conn.Query(ctx, entityPageQuery, ts, lastID, batch)
	if err != nil {
		return nil, fmt.Errorf("graph: query entities: %w", err)
	}
	defer rows.Close()
	var out []chEntity
	for rows.Next() {
		var e chEntity
		if err := rows.Scan(&e.EntityID, &e.Scope, &e.EntityType, &e.Key,
			&e.DisplayName, &e.FirstSeen, &e.LastSeen, &e.UpdatedAt); err != nil {
			return nil, fmt.Errorf("graph: scan entity row: %w", err)
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("graph: iterate entity rows: %w", err)
	}
	return out, nil
}

// upsertEntityNode writes one node via a single atomic upsert request:
// v binds the existing uid (if any) for this ch_id; the eq-branch inserts a
// blank-node JSON payload, the gt-branch rewrites the existing node in place.
// Both branches carry identical content, making replay idempotent.
func upsertEntityNode(ctx context.Context, c *dgo.Dgraph, e chEntity) error {
	req := &api.Request{
		Query: `query ent($ch_id: string) {
			v as var(func: eq(ch_id, $ch_id))
		}`,
		Vars:      map[string]string{"$ch_id": e.EntityID},
		CommitNow: true,
		Mutations: []*api.Mutation{
			{SetJson: entityNodeJSON(e, ""), Cond: "@if(eq(len(v),0))"},
			{SetJson: entityNodeJSON(e, "uid(v)"), Cond: "@if(gt(len(v),0))"},
		},
	}
	txn := c.NewTxn()
	defer txn.Discard(context.WithoutCancel(ctx))
	if _, err := txn.Do(ctx, req); err != nil {
		return fmt.Errorf("graph: upsert entity %s: %w", e.EntityID, err)
	}
	return nil
}

// entityNodeJSON marshals the node payload; uidRef is either "" (insert via
// implicit blank node) or a DQL uid expression such as "uid(v)".
func entityNodeJSON(e chEntity, uidRef string) []byte {
	m := map[string]any{
		"dgraph.type":  []string{"Entity"},
		"ch_id":        e.EntityID,
		"scope":        e.Scope,
		"key":          e.Key,
		"entity_type":  e.EntityType,
		"display_name": e.DisplayName,
		"first_seen":   e.FirstSeen.UTC().Format(time.RFC3339),
		"last_seen":    e.LastSeen.UTC().Format(time.RFC3339),
	}
	if uidRef != "" {
		m["uid"] = uidRef
	}
	b, err := json.Marshal(m)
	if err != nil { // unreachable for this shape; guarded anyway
		panic(fmt.Sprintf("graph: marshal entity node: %v", err))
	}
	return b
}

// readCursor returns the stored (ts, last_id) for name and whether the row
// exists. A missing row reads as the epoch with an empty tiebreaker.
func readCursor(ctx context.Context, conn driver.Conn, name string) (time.Time, string, bool, error) {
	rows, err := conn.Query(ctx,
		"SELECT ts, last_id FROM mem.projection_watermark WHERE name = ? "+
			"ORDER BY ts DESC LIMIT 1", name)
	if err != nil {
		return time.Time{}, "", false, fmt.Errorf("graph: read %s watermark: %w", name, err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return time.Time{}, "", false, fmt.Errorf("graph: read %s watermark: %w", name, err)
		}
		return time.Unix(0, 0).UTC(), "", false, nil
	}
	var (
		ts     time.Time
		lastID string
	)
	if err := rows.Scan(&ts, &lastID); err != nil {
		return time.Time{}, "", false, fmt.Errorf("graph: scan %s watermark: %w", name, err)
	}
	return ts, lastID, true, nil
}

// seedCursor creates a missing watermark row at the given position.
func seedCursor(ctx context.Context, conn driver.Conn, name, ts, lastID string) error {
	if err := conn.Exec(ctx,
		"INSERT INTO mem.projection_watermark (name, ts, last_id) VALUES (?, ?, ?)",
		name, ts, lastID); err != nil {
		return fmt.Errorf("graph: seed %s watermark: %w", name, err)
	}
	return nil
}

// updateCursor advances the watermark synchronously (mutations_sync=1) so a
// returned error always means the old cursor still stands.
func updateCursor(ctx context.Context, conn driver.Conn, name, ts, lastID string) error {
	if err := conn.Exec(ctx,
		"ALTER TABLE mem.projection_watermark UPDATE ts = ?, last_id = ? WHERE name = ? "+
			"SETTINGS mutations_sync = 1",
		ts, lastID, name); err != nil {
		return fmt.Errorf("graph: advance %s watermark: %w", name, err)
	}
	return nil
}

func formatCHTimestamp(ts time.Time) string {
	return ts.UTC().Format("2006-01-02 15:04:05.000")
}
