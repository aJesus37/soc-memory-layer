// Command graphrebuild reconstructs the Dgraph projection from ClickHouse
// truth.
//
// Sequence: install the projection schema (idempotent), drop all Dgraph data
// (schema preserved), reset both mem.projection_watermark cursors to epoch,
// then replay ProjectEntities until drained followed by ProjectEdges until
// drained. ClickHouse stays sole truth throughout; nothing writes to CH.
//
// The command is safe to re-run and safe to abort: a crashed rebuild leaves
// the cursors wherever they last committed, and every subsequent run starts
// over from epoch and converges to the same graph because every projection
// write is an idempotent upsert keyed on ch_id.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"socmem/internal/ch"
	"socmem/internal/config"
	"socmem/internal/graph"
)

func main() {
	batch := flag.Int("batch", 500, "projection page size")
	flag.Parse()
	if *batch <= 0 {
		fmt.Fprintln(os.Stderr, "graphrebuild: -batch must be positive")
		os.Exit(2)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	cfg := config.Load()

	start := time.Now()
	entities, edges, err := rebuild(context.Background(), logger, cfg, *batch)
	if err != nil {
		logger.Error("rebuild failed", "err", err)
		os.Exit(1)
	}
	logger.Info("rebuild complete",
		"entities_projected", entities,
		"edges_processed", edges.Processed,
		"orphan_edges_deleted", edges.DeletedOrphans,
		"edges_deferred_seen", edges.Deferred,
		"elapsed", time.Since(start).Round(time.Millisecond))
}

// rebuild runs the full destructive reconstruction and returns the number of
// entities projected plus the edge-row outcome totals. Progress is logged
// per page with counts only — never entity keys or content.
//
// Entity and edge passes alternate: an edge page can stop on a DEFERRABLE
// edge (endpoint exists in ClickHouse but its node was not yet written —
// e.g. a row inserted mid-rebuild by a live writer), so the entity pass is
// simply run again; once every deferral heals, the next edge pass drains to
// a zero-progress page with nothing deferred. The round cap turns a
// pathological never-healing deferral into a loud failure instead of a hang.
func rebuild(ctx context.Context, logger *slog.Logger, cfg config.Config, batch int) (int, graph.EdgeProjectionStats, error) {
	conn, err := ch.ConnectConfig(ctx, cfg, "default")
	if err != nil {
		return 0, graph.EdgeProjectionStats{}, fmt.Errorf("clickhouse connect: %w", err)
	}
	defer conn.Close()

	st, err := graph.Connect(ctx, cfg.DgraphAddr)
	if err != nil {
		return 0, graph.EdgeProjectionStats{}, fmt.Errorf("dgraph connect: %w", err)
	}
	defer st.Close()

	if err := st.InstallSchema(ctx); err != nil {
		return 0, graph.EdgeProjectionStats{}, fmt.Errorf("install schema: %w", err)
	}
	if err := st.DropData(ctx); err != nil {
		return 0, graph.EdgeProjectionStats{}, fmt.Errorf("drop data: %w", err)
	}
	if err := resetWatermarks(ctx, conn); err != nil {
		return 0, graph.EdgeProjectionStats{}, err
	}

	const maxRounds = 10
	var entities int
	var edges graph.EdgeProjectionStats
	for round := 1; ; round++ {
		for {
			n, err := graph.ProjectEntities(ctx, st, conn, batch)
			if err != nil {
				return entities, edges, fmt.Errorf("project entities: %w", err)
			}
			entities += n
			logger.Info("entities projected", "page", n, "total", entities)
			if n == 0 {
				break
			}
		}

		var page graph.EdgeProjectionStats
		for {
			page, err = graph.ProjectEdges(ctx, st, conn, batch)
			if err != nil {
				return entities, edges, fmt.Errorf("project edges: %w", err)
			}
			edges.Processed += page.Processed
			edges.DeletedOrphans += page.DeletedOrphans
			edges.Deferred += page.Deferred
			logger.Info("edges processed",
				"page_processed", page.Processed,
				"page_orphans_deleted", page.DeletedOrphans,
				"page_deferred", page.Deferred,
				"total_processed", edges.Processed,
				"total_orphans_deleted", edges.DeletedOrphans)
			if page.Processed == 0 && page.DeletedOrphans == 0 {
				break // drained, or stalled on a deferred head row
			}
		}

		if page.Deferred == 0 {
			return entities, edges, nil
		}
		if round >= maxRounds {
			return entities, edges, fmt.Errorf(
				"edge deferrals persist after %d entity/edge rounds; rerun once writes settle", maxRounds)
		}
		logger.Warn("edges deferred pending entity projection; repeating passes",
			"round", round)
	}
}

// resetWatermarks zeroes both projection cursors so the replay covers the
// entire pagination order. TRUNCATE is atomic and immediately visible;
// UPDATE mutations can lag behind concurrent wipes on the shared dev DB.
func resetWatermarks(ctx context.Context, conn driver.Conn) error {
	if err := conn.Exec(ctx, "TRUNCATE TABLE mem.projection_watermark"); err != nil {
		return fmt.Errorf("truncate watermark: %w", err)
	}
	if err := conn.Exec(ctx,
		"INSERT INTO mem.projection_watermark (name, ts, last_id) VALUES ('entities', toDateTime64(0, 3), ''), ('edges', toDateTime64(0, 3), '')"); err != nil {
		return fmt.Errorf("seed watermarks: %w", err)
	}
	return nil
}
