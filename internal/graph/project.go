package graph

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/dgraph-io/dgo/v250"
	"github.com/dgraph-io/dgo/v250/protos/api"
	"github.com/google/uuid"
)

const (
	// watermarkEntities / watermarkEdges are the mem.projection_watermark
	// rows driving ProjectEntities and ProjectEdges respectively.
	watermarkEntities = "entities"
	watermarkEdges    = "edges"

	defaultBatch = 500

	// nilUUID stands in for "no tiebreaker yet" when binding the epoch
	// cursor: both id tiebreakers are UUID columns, so the tuple comparison
	// needs a parseable value that sorts before every real id.
	nilUUID = "00000000-0000-0000-0000-000000000000"

	// toString(entity_id/edge_id) is LOAD-BEARING in both predicates below:
	// those columns are UUID, and ClickHouse compares/filters UUIDs in its
	// INTERNAL byte order, which disagrees with the canonical text order the
	// ORDER BY would suggest AND with the watermark's String-typed last_id
	// that updateCursor's CAS compares as text. A page boundary landing
	// inside a same-millisecond id cluster could then mint a cursor that the
	// CAS judged non-forward (zero rows matched) while pagination kept
	// serving pages past it — freezing the projector in an infinite re-read
	// loop (observed live: hundreds of identical no-op mutations).
	// Casting the tiebreaker to String pins pagination, ORDER BY and the
	// watermark CAS to ONE canonical-text total order.
	entityPageQuery = "SELECT entity_id, scope, entity_type, key, display_name, " +
		"first_seen, last_seen, updated_at FROM mem.entities FINAL " +
		"WHERE (updated_at, toString(entity_id)) > (?, ?) " +
		"ORDER BY updated_at ASC, toString(entity_id) ASC LIMIT ?"

	edgePageQuery = "SELECT edge_id, scope, src_id, dst_id, relation, from_fact, " +
		"valid_from, valid_to, updated_at FROM mem.edges " +
		"WHERE (updated_at, toString(edge_id)) > (?, ?) " +
		"ORDER BY updated_at ASC, toString(edge_id) ASC LIMIT ?"
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
//     tuple comparison WHERE (updated_at, toString(entity_id)) > (?, ?)
//     ORDER BY updated_at, toString(entity_id) LIMIT batch. The secondary
//     entity_id key makes
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
		lastID = nilUUID
	}
	page, err := queryEntityRows(ctx, conn, ts, lastID, batch)
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
	if exists {
		err = updateCursor(ctx, conn, watermarkEntities, last.UpdatedAt, last.EntityID)
	} else {
		err = seedCursor(ctx, conn, watermarkEntities, last.UpdatedAt, last.EntityID)
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

func queryEntityRows(ctx context.Context, conn driver.Conn, ts time.Time, lastID string, batch int) ([]chEntity, error) {
	rows, err := conn.Query(ctx, entityPageQuery, formatCHTimestamp(ts), lastID, batch)
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
func seedCursor(ctx context.Context, conn driver.Conn, name string, ts time.Time, lastID string) error {
	if err := conn.Exec(ctx,
		"INSERT INTO mem.projection_watermark (name, ts, last_id) VALUES (?, ?, ?)",
		name, formatCHTimestamp(ts), lastID); err != nil {
		return fmt.Errorf("graph: seed %s watermark: %w", name, err)
	}
	return nil
}

// updateCursor advances the watermark synchronously (mutations_sync=1) so a
// returned error always means the old cursor still stands. The WHERE clause
// is a compare-and-set — (ts, last_id) < (?, ?) in the same composite order
// the page query sorts by — so an overlapping caller (double-scheduled
// worker, manual replay racing a live tick) can never move the cursor
// backwards; a stale writer's UPDATE matches zero rows and changes nothing.
func updateCursor(ctx context.Context, conn driver.Conn, name string, ts time.Time, lastID string) error {
	stamp := formatCHTimestamp(ts)
	if err := conn.Exec(ctx,
		"ALTER TABLE mem.projection_watermark UPDATE ts = ?, last_id = ? "+
			"WHERE name = ? AND (ts, last_id) < (?, ?) "+
			"SETTINGS mutations_sync = 1",
		stamp, lastID, name, stamp, lastID); err != nil {
		return fmt.Errorf("graph: advance %s watermark: %w", name, err)
	}
	return nil
}

// formatCHTimestamp renders a cursor timestamp for binding against a
// DateTime64(3) column.
//
// DELIBERATE STRING BINDING — do not "fix" this to pass time.Time directly:
// clickhouse-go v2.48 renders bare time.Time bind parameters at SECOND
// precision (and typed {p:DateTime64(3)} placeholders degrade to lossy
// epoch-fraction casts), so any direct binding silently truncates the
// milliseconds the composite cursor depends on. Probed empirically: a
// .123 watermark round-tripped through both binding styles came back .000
// / .600, which makes keyset pagination re-read whole seconds of rows
// forever. An explicitly formatted "2006-01-02 15:04:05.000" literal is
// cast server-side with full millisecond fidelity; the read path (Scan
// into time.Time) preserves ms regardless.
func formatCHTimestamp(ts time.Time) string {
	return ts.UTC().Format("2006-01-02 15:04:05.000")
}

// ProjectEdges projects mem.edges changes into Dgraph related_to edges with
// facets. Open edges (valid_to in the future) upsert the src->dst triple
// with relation/valid_from/valid_to facets; closed edges (valid_to <= now)
// delete the triple. It returns the number of edge rows processed this call
// (including rows skipped for a missing endpoint node); batch bounds the
// page size; values <= 0 select defaultBatch.
//
// # MODELING CONSTRAINT: one relation per (src,dst) pair
//
// All relations share the single related_to predicate, so an ordered
// (src,dst) pair carries AT MOST ONE live relation: two different facts
// linking the same pair overwrite each other's facets at projection time,
// whichever projects later wins, and closing one of the two deletes the
// shared triple even if its sibling is still open — deletion is per-triple,
// not per-facet-set. Acceptable Phase-2 scope (hunts care about
// connectivity plus one verdict-style label); revisit with per-relation
// predicates if hunts ever need parallel relations on one pair.
//
// Watermark invariant: identical composite-cursor mechanics as
// ProjectEntities — pages read WHERE (updated_at, toString(edge_id)) > (?, ?)
// in a deterministic total order; the cursor advances to the last row only
// after
// the Dgraph write commits; any earlier error returns before advancement
// and replays the page harmlessly.
//
// Closure visibility is what makes this correct at all: mem.edges closures
// are MUTATIONS that leave the row physically in place, so the writers bump
// updated_at alongside valid_to (migration 003), turning every closure into
// a fresh position in the pagination order that a cursor which already
// passed the original insert can still see.
//
// Rows whose endpoints have no projected node (ProjectEntities lagging) are
// skipped with a debug log and NOT retried — the cursor moves past them;
// the next touch of the underlying fact re-mints or re-closes the edge row
// and it re-enters the order. Retry-duplicated edge rows converge:
// replaying an open edge re-sets the identical triple and facets.
func ProjectEdges(ctx context.Context, st *Store, conn driver.Conn, batch int) (int, error) {
	if batch <= 0 {
		batch = defaultBatch
	}
	if st == nil || conn == nil {
		return 0, errors.New("graph: project edges: nil store or connection")
	}

	ts, lastID, exists, err := readCursor(ctx, conn, watermarkEdges)
	if err != nil {
		return 0, err
	}
	if lastID == "" {
		lastID = nilUUID
	}
	page, err := queryEdgeRows(ctx, conn, ts, lastID, batch)
	if err != nil {
		return 0, err
	}
	if len(page) == 0 {
		return 0, nil
	}
	if err := applyEdgePage(ctx, st.Dgraph(), page); err != nil {
		return 0, err
	}
	last := page[len(page)-1]
	lastID = last.EdgeID.String()
	if exists {
		err = updateCursor(ctx, conn, watermarkEdges, last.UpdatedAt, lastID)
	} else {
		err = seedCursor(ctx, conn, watermarkEdges, last.UpdatedAt, lastID)
	}
	if err != nil {
		return 0, err
	}
	return len(page), nil
}

// chEdge is one mem.edges row exactly as needed for projection. Edges are
// a plain MergeTree (no FINAL): every physical row is distinct — supersede
// and retract close rows via mutation, never via replacement versions.
type chEdge struct {
	EdgeID    uuid.UUID
	Scope     string
	SrcID     uuid.UUID
	DstID     uuid.UUID
	Relation  string
	FromFact  uuid.UUID
	ValidFrom time.Time
	ValidTo   time.Time
	UpdatedAt time.Time
}

func queryEdgeRows(ctx context.Context, conn driver.Conn, ts time.Time, lastID string, batch int) ([]chEdge, error) {
	rows, err := conn.Query(ctx, edgePageQuery, formatCHTimestamp(ts), lastID, batch)
	if err != nil {
		return nil, fmt.Errorf("graph: query edges: %w", err)
	}
	defer rows.Close()
	var out []chEdge
	for rows.Next() {
		var e chEdge
		if err := rows.Scan(&e.EdgeID, &e.Scope, &e.SrcID, &e.DstID, &e.Relation,
			&e.FromFact, &e.ValidFrom, &e.ValidTo, &e.UpdatedAt); err != nil {
			return nil, fmt.Errorf("graph: scan edge row: %w", err)
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("graph: iterate edge rows: %w", err)
	}
	return out, nil
}

// applyEdgePage applies one page as a single atomic Dgraph transaction:
// all SET nquads (open edges) plus all DELETE nquads (closed edges) commit
// together or not at all, so a crash mid-page replays the whole page.
//
// Endpoints resolve by ch_id in ONE batched eq() lookup — nodes MUST
// already exist via ProjectEntities; missing endpoints are logged at debug
// level and skipped (never created dangling). Facets use SINGLE parentheses
// — the only NQuads facet syntax dgo v250 accepts (double parens fail with
// "Invalid input: )" at lex time, re-verified against live Dgraph v25) —
// with quoted string values carrying RFC3339 timestamps that read back
// verbatim under @facets sub-selections.
func applyEdgePage(ctx context.Context, c *dgo.Dgraph, page []chEdge) error {
	uids, err := resolveNodeUIDs(ctx, c, page)
	if err != nil {
		return err
	}

	var setBuf, delBuf bytes.Buffer
	now := time.Now()
	for _, e := range page {
		srcUID, okSrc := uids[e.SrcID.String()]
		dstUID, okDst := uids[e.DstID.String()]
		if !okSrc || !okDst {
			slog.Debug("graph: skip edge with unprojected endpoint",
				"edge_id", e.EdgeID,
				"src_id", e.SrcID,
				"dst_id", e.DstID,
				"relation", e.Relation)
			continue
		}
		if e.ValidTo.After(now) {
			fmt.Fprintf(&setBuf, "<%s> <related_to> <%s> (relation=%q,"+
				"valid_from=%q,valid_to=%q) .\n",
				srcUID, dstUID, e.Relation,
				e.ValidFrom.UTC().Format(time.RFC3339),
				e.ValidTo.UTC().Format(time.RFC3339))
		} else {
			fmt.Fprintf(&delBuf, "<%s> <related_to> <%s> .\n", srcUID, dstUID)
		}
	}

	if setBuf.Len() == 0 && delBuf.Len() == 0 {
		return nil // every row skipped; nothing to mutate
	}
	req := &api.Request{CommitNow: true}
	if setBuf.Len() > 0 {
		req.Mutations = append(req.Mutations, &api.Mutation{SetNquads: setBuf.Bytes()})
	}
	if delBuf.Len() > 0 {
		req.Mutations = append(req.Mutations, &api.Mutation{DelNquads: delBuf.Bytes()})
	}
	txn := c.NewTxn()
	defer txn.Discard(context.WithoutCancel(ctx))
	if _, err := txn.Do(ctx, req); err != nil {
		return fmt.Errorf("graph: apply edge page: %w", err)
	}
	return nil
}

// resolveNodeUIDs maps the ch_id of every src/dst appearing in the page to
// its projected node's uid, using one indexed eq(ch_id, [...]) lookup.
func resolveNodeUIDs(ctx context.Context, c *dgo.Dgraph, page []chEdge) (map[string]string, error) {
	seen := make(map[string]bool)
	literals := make([]string, 0, 2*len(page))
	add := func(id uuid.UUID) {
		s := id.String()
		if !seen[s] {
			seen[s] = true
			literals = append(literals, `"`+s+`"`)
		}
	}
	for _, e := range page {
		add(e.SrcID)
		add(e.DstID)
	}

	q := fmt.Sprintf(`{ res(func: eq(ch_id, [%s])) { uid ch_id } }`, strings.Join(literals, ", "))
	resp, err := c.NewReadOnlyTxn().Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("graph: resolve edge endpoints: %w", err)
	}
	var parsed struct {
		Res []struct {
			Uid  string `json:"uid"`
			ChID string `json:"ch_id"`
		} `json:"res"`
	}
	if err := json.Unmarshal(resp.Json, &parsed); err != nil {
		return nil, fmt.Errorf("graph: decode edge endpoints response %s: %w", resp.Json, err)
	}
	out := make(map[string]string, len(parsed.Res))
	for _, n := range parsed.Res {
		out[n.ChID] = n.Uid
	}
	return out, nil
}
