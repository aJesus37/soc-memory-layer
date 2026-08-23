// Command memseed backfills historical observations from a JSONL file.
//
// Each line: {"ts":"RFC3339","kind":"investigation_note","actor_type":"human",
//
//	"actor_id":"analyst-j","scope":"team-a","content":"..."}
//
// Optional per line: case_id, client_event_id, on_behalf_of, confidentiality.
//
// Lines failing input validation (errors.Is memory.ErrInvalidInput) are
// counted and skipped (reported at the end); anything else — e.g. storage
// faults — aborts the run.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
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

// factLine asserts a human fact. subject_key/object_key are normalized
// through the entity resolver (created on first sight), matching how the
// API resolves keys.
type factLine struct {
	Scope        string  `json:"scope"`
	SubjectKey   string  `json:"subject_key"`
	Predicate    string  `json:"predicate"`
	ObjectValue  string  `json:"object_value"`
	ObjectKey    string  `json:"object_key,omitempty"`
	Confidence   float32 `json:"confidence,omitempty"`
	ActorType    string  `json:"actor_type,omitempty"`
	ActorID      string  `json:"actor_id,omitempty"`
	ClientEventID string `json:"client_event_id,omitempty"`
}

func main() {
	file := flag.String("file", "", "JSONL file to ingest (required)")
	factsFile := flag.String("facts", "", "JSONL file of human fact assertions (optional)")
	dryRun := flag.Bool("dry-run", false, "parse-check lines only; write nothing")
	flag.Parse()
	if *file == "" {
		fmt.Fprintln(os.Stderr, "usage: memseed -file seeds/history.jsonl [-facts seeds/facts.jsonl] [-dry-run]")
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

	if *factsFile != "" && !*dryRun {
		n, err := seedFacts(ctx, logger, svc, entity.NewResolver(conn), *factsFile)
		if err != nil {
			logger.Error("fact seeding failed", "err", err)
			os.Exit(1)
		}
		logger.Info("facts asserted", "count", n)
	}

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
			if errors.Is(err, memory.ErrInvalidInput) {
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

// seedFacts asserts human facts from a JSONL file:
// {"scope":"team-a","subject_key":"evil.example.com","predicate":"resolved_to",
//  "object_value":"198.51.100.23","object_key":"198.51.100.23","confidence":0.95,
//  "actor_type":"human","actor_id":"analyst-j"}
func seedFacts(ctx context.Context, logger *slog.Logger, svc *memory.Service, res *entity.Resolver, path string) (int, error) {
	fh, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer fh.Close()

	var asserted int
	scanner := bufio.NewScanner(fh)
	scanner.Buffer(make([]byte, 0, 1024*1024), 1024*1024)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		var fl factLine
		if err := json.Unmarshal([]byte(line), &fl); err != nil {
			logger.Warn("unparseable fact line", "line", lineNo, "err", err)
			continue
		}
		actorType := fl.ActorType
		if actorType == "" {
			actorType = "human"
		}
		actorID := fl.ActorID
		if actorID == "" {
			actorID = "seeder"
		}
		subj, _, err := res.Resolve(ctx, fl.Scope, fl.SubjectKey)
		if err != nil {
			logger.Warn("subject resolve failed", "line", lineNo, "key", fl.SubjectKey, "err", err)
			continue
		}
		var objID string
		if fl.ObjectKey != "" {
			obj, _, err := res.Resolve(ctx, fl.Scope, fl.ObjectKey)
			if err != nil {
				logger.Warn("object resolve failed", "line", lineNo, "key", fl.ObjectKey, "err", err)
				continue
			}
			objID = obj.EntityID
		}
		if _, err := svc.AssertFact(ctx, memory.FactInput{
			Scope:        fl.Scope,
			SubjectID:    subj.EntityID,
			Predicate:    fl.Predicate,
			ObjectValue:  fl.ObjectValue,
			ObjectID:     objID,
			Confidence:   fl.Confidence,
			ActorType:    actorType,
			ActorID:      actorID,
		}); err != nil {
			if errors.Is(err, memory.ErrInvalidInput) {
				logger.Warn("invalid fact line", "line", lineNo, "err", err)
				continue
			}
			return asserted, fmt.Errorf("fact line %d: %w", lineNo, err)
		}
		asserted++
	}
	return asserted, scanner.Err()
}
