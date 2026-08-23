// Command memeval measures memory retrieval quality: for each question it
// embeds the query, runs the same hybrid recall the API uses, and checks
// whether expected entities surface in the top-k. Exit code 1 on regression
// vs evals/.last_score.
//
// Eval file format (YAML):
//
//	name: smoke
//	queries:
//	  - q: "had we seen this host before?"
//	    must_reference_entities: ["203.0.113.7"]   # entity KEYS, not ids
//	    k: 10
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"gopkg.in/yaml.v3"

	"socmem/internal/ch"
	"socmem/internal/config"
	"socmem/internal/embed"
	"socmem/internal/entity"
	"socmem/internal/memory"
)

// lookupEntityID resolves an entity KEY (e.g. "203.0.113.7") to its id
// within a scope. Eval files reference keys because ids are unstable across
// re-seeds.
func lookupEntityID(conn driver.Conn, scope, key string) (string, bool) {
	var id string
	err := conn.QueryRow(context.Background(),
		"SELECT entity_id FROM mem.entities FINAL WHERE scope = ? AND key = ? LIMIT 1",
		scope, key).Scan(&id)
	if err != nil {
		return "", false
	}
	return id, true
}

type query struct {
	Q                    string   `yaml:"q"`
	MustReferenceEntities []string `yaml:"must_reference_entities"`
	K                    int      `yaml:"k"`
}

type evalFile struct {
	Name    string  `yaml:"name"`
	Scope   string  `yaml:"scope"`
	Queries []query `yaml:"queries"`
}

func main() {
	dir := flag.String("dir", "evals", "directory containing *.yaml eval files")
	flag.Parse()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	cfg := config.Load()

	ctx := context.Background()
	conn, err := ch.Connect(ctx, cfg.ChAddr, cfg.ChUser, cfg.ChPassword, "default")
	if err != nil {
		logger.Error("clickhouse connect failed", "err", err)
		os.Exit(1)
	}
	defer conn.Close()
	svc := memory.New(conn,
		entity.NewResolver(conn),
		embed.NewOpenAI(embed.Config{BaseURL: cfg.EmbedURL, Model: cfg.EmbedModel}),
		cfg)

	files, err := filepath.Glob(filepath.Join(*dir, "*.yaml"))
	if err != nil || len(files) == 0 {
		logger.Error("no eval files found", "dir", *dir)
		os.Exit(2)
	}

	totalPassed, totalQueries := 0, 0
	for _, path := range files {
		raw, err := os.ReadFile(path)
		if err != nil {
			logger.Error("read failed", "path", path, "err", err)
			os.Exit(2)
		}
		var ef evalFile
		if err := yaml.Unmarshal(raw, &ef); err != nil {
			logger.Error("parse failed", "path", path, "err", err)
			os.Exit(2)
		}
		fmt.Printf("=== %s (%s) ===\n", ef.Name, filepath.Base(path))
		for _, q := range ef.Queries {
			k := q.K
			if k == 0 {
				k = 10
			}
			hits, err := svc.Similar(ctx, ef.Scope, q.Q, k)
			if err != nil {
				logger.Error("search failed", "q", q.Q, "err", err)
				totalQueries++
				continue
			}
			hitSet := map[string]bool{}
			for _, h := range hits {
				for _, id := range h.EntityIDs {
					hitSet[id] = true
				}
			}
			// Empty must_reference_entities = smoke query: passes if search runs.
			passed := len(q.MustReferenceEntities) == 0
			for _, want := range q.MustReferenceEntities {
				eid, ok := lookupEntityID(conn, ef.Scope, want)
				if !ok || !hitSet[eid] {
					passed = false
					break
				}
			}
			status := "PASS"
			if !passed {
				status = "FAIL"
			} else {
				totalPassed++
			}
			totalQueries++
			fmt.Printf("  [%s] %q (k=%d, hits=%d)\n", status, q.Q, k, len(hits))
		}
	}

	score := 0.0
	if totalQueries > 0 {
		score = float64(totalPassed) / float64(totalQueries)
	}
	fmt.Printf("\nrecall: %d/%d = %.2f\n", totalPassed, totalQueries, score)

	lastPath := filepath.Join(*dir, ".last_score")
	lastRaw, _ := os.ReadFile(lastPath)
	if last := strings.TrimSpace(string(lastRaw)); last != "" {
		var lastScore float64
		if _, err := fmt.Sscanf(last, "%f", &lastScore); err == nil && score+1e-9 < lastScore {
			fmt.Printf("REGRESSION: %.2f < previous %.2f\n", score, lastScore)
			os.Exit(1)
		}
	}
	_ = os.WriteFile(lastPath, []byte(fmt.Sprintf("%.4f\n", score)), 0o644)
}
