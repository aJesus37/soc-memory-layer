// Command memseed backfills historical observations from a JSONL file.
//
// Each line: {"ts":"RFC3339","kind":"investigation_note","actor_type":"human",
//             "actor_id":"analyst-j","scope":"team-a","content":"..."}
// Optional per line: case_id, client_event_id, on_behalf_of, confidentiality.
//
// Lines failing validation are counted and skipped (reported at the end);
// storage/embedding errors abort the run.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"socmem/internal/ch"
	"socmem/internal/config"
	"socmem/internal/embed"
	"socmem/internal/entity"
	"socmem/internal/memory"
)

type seedLine struct {
	Ts              string `json:"ts"`
	Kind            string `json:"kind"`
	ActorType       string `json:"actor_type"`
	ActorID         string `json:"actor_id"`
	Scope           string `json:"scope"`
	CaseID          string `json:"case_id,omitempty"`
	ClientEventID   string `json:"client_event_id,omitempty"`
	OnBehalfOf      string `json:"on_behalf_of,omitempty"`
	Confidentiality string `json:"confidentiality,omitempty"`
	Content         string `json:"content"`
}

func main() {
	file := flag.String("file", "", "JSONL file to ingest (required)")
	dryRun := flag.Bool("dry-run", false, "normalize+resolve only; write nothing")
	flag.Parse()
	if *file == "" {
		fmt.Fprintln(os.Stderr, "usage: memseed -file seeds/history.jsonl [-dry-run]")
		os.Exit(2)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	cfg := config.Load()

	f, err := os.Open(*file)
	if err != nil {
		logger.Error("open failed", "err", err)
		os.Exit(1)
	}
	defer f.Close()

	ctx := context.Background()
	conn, err := ch.Connect(ctx, cfg.ChAddr, cfg.ChUser, cfg.ChPassword, "default")
	if err != nil {
		logger.Error("clickhouse connect failed", "err", err)
		os.Exit(1)
	}
	defer conn.Close()
	if !*dryRun {
		if err := ch.Migrate(ctx, conn, cfg); err != nil {
			logger.Error("migrations failed", "err", err)
			os.Exit(1)
		}
	}

	svc := memory.New(conn,
		entity.NewResolver(conn),
		embed.NewOpenAI(embed.Config{BaseURL: cfg.EmbedURL, Model: cfg.EmbedModel}),
		cfg)

	var (
		written, skipped, failed int
		start                    = time.Now()
	)
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 1024*1024), 1024*1024)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		var sl seedLine
		if err := json.Unmarshal([]byte(line), &sl); err != nil {
			logger.Warn("unparseable line", "line", lineNo, "err", err)
			skipped++
			continue
		}
		ts := time.Time{}
		if sl.Ts != "" {
			parsed, err := time.Parse(time.RFC3339, sl.Ts)
			if err != nil {
				logger.Warn("bad ts", "line", lineNo, "err", err)
				skipped++
				continue
			}
			ts = parsed
		}
		if *dryRun {
			written++
			continue
		}
		if _, err := svc.RecordObservation(ctx, memory.Input{
			Scope:           sl.Scope,
			Kind:            sl.Kind,
			ActorType:       sl.ActorType,
			ActorID:         sl.ActorID,
			OnBehalfOf:      sl.OnBehalfOf,
			CaseID:          sl.CaseID,
			ClientEventID:   sl.ClientEventID,
			Confidentiality: sl.Confidentiality,
			Ts:              ts,
			Content:         sl.Content,
		}); err != nil {
			if strings.Contains(err.Error(), "must be") || strings.Contains(err.Error(), "required") {
				logger.Warn("invalid line", "line", lineNo, "err", err)
				skipped++
				continue
			}
			logger.Error("storage error — aborting", "line", lineNo, "err", err)
			failed++
			break
		}
		written++
	}
	if err := scanner.Err(); err != nil {
		logger.Error("scan failed", "err", err)
		os.Exit(1)
	}
	logger.Info("seed complete",
		"written", written, "skipped", skipped, "failed", failed,
		"dry_run", *dryRun, "elapsed", time.Since(start).Round(time.Millisecond))
	if failed > 0 {
		os.Exit(1)
	}
}
