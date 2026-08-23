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
		"edges_processed", edges,
		"elapsed", time.Since(start).Round(time.Millisecond))
}

// rebuild runs the full destructive reconstruction and returns the number of
// entities projected and edge rows processed. Progress is logged per page
// with counts only — never entity keys or content.
func rebuild(ctx context.Context, logger *slog.Logger, cfg config.Config, batch int) (int, int, error) {
	conn, err := ch.ConnectConfig(ctx, cfg, "default")
	if err != nil {
		return 0, 0, fmt.Errorf("clickhouse connect: %w", err)
	}
	defer conn.Close()

	st, err := graph.Connect(ctx, cfg.DgraphAddr)
	if err != nil {
		return 0, 0, fmt.Errorf("dgraph connect: %w", err)
	}
	defer st.Close()

	if err := st.InstallSchema(ctx); err != nil {
		return 0, 0, fmt.Errorf("install schema: %w", err)
	}
	if err := st.DropData(ctx); err != nil {
		return 0, 0, fmt.Errorf("drop data: %w", err)
	}
	if err := resetWatermarks(ctx, conn); err != nil {
		return 0, 0, err
	}

	var entities, edges int
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
	for {
		n, err := graph.ProjectEdges(ctx, st, conn, batch)
		if err != nil {
			return entities, edges, fmt.Errorf("project edges: %w", err)
		}
		edges += n
		logger.Info("edges processed", "page", n, "total", edges)
		if n == 0 {
			break
		}
	}
	return entities, edges, nil
}

// resetWatermarks zeroes both projection cursors so the replay covers the
// entire pagination order. The watermark names are duplicated here because
// internal/graph keeps them unexported (see project.go watermarkEntities /
// watermarkEdges); keep the literals in sync. mutations_sync=1 means a
// returned error always implies the old cursors still stand.
func resetWatermarks(ctx context.Context, conn driver.Conn) error {
	if err := conn.Exec(ctx,
		"ALTER TABLE mem.projection_watermark "+
			"UPDATE ts = toDateTime64(0, 3), last_id = '' "+
			"WHERE name IN ('entities', 'edges') "+
			"SETTINGS mutations_sync = 1"); err != nil {
		return fmt.Errorf("reset watermarks: %w", err)
	}
	return nil
}
