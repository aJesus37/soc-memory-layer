package memory

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"socmem/internal/entity"
	"socmem/internal/extract"
)

// Dreaming-lite: the extraction worker turns observations into PROPOSED
// facts via an LLM, batched, never synchronous with ingestion (design doc
// §10). Proposals enter as status='proposed' — the worker asserts as an
// agent and its predicates are non-whitelisted by default — so humans
// promote through the normal PromoteFact path. The one deliberate
// exception is operator policy: a predicate explicitly whitelisted via
// MEM_TRUST_WHITELIST (default: resolved_to) may auto-activate at or above
// MEM_TRUST_FLOOR exactly like any other agent write; the worker adds no
// bypass of its own.
const (
	// extractorActorID stamps every fact this worker writes; fact
	// written_by and audit actor_id carry it so the whole extraction
	// lineage traces back to this binary generation.
	extractorActorID = "extractor-v1"
	// defaultExtractBatch bounds NewExtractor when constructed without a
	// sensible batch size (the constructor cannot fail).
	defaultExtractBatch = 32
)

// Extractor binds a Service to a ChatClient for background extraction.
// It is a thin convenience wrapper: the logic lives on the Service methods
// so tests and callers can drive single ticks directly. svc and chat must
// be non-nil; a non-positive batch falls back to defaultExtractBatch.
type Extractor struct {
	svc   *Service
	chat  extract.ChatClient
	batch int
}

// NewExtractor wires an extractor over svc. See the Extractor type
// contract for field rules.
func NewExtractor(svc *Service, chat extract.ChatClient, batch int) *Extractor {
	if batch <= 0 {
		batch = defaultExtractBatch
	}
	return &Extractor{svc: svc, chat: chat, batch: batch}
}

// RunOnce processes up to the configured batch of uncovered observations.
func (e *Extractor) RunOnce(ctx context.Context) (int, error) {
	return e.svc.RunExtractionOnce(ctx, e.chat, e.batch)
}

// RunLoop ticks extraction until ctx cancels.
func (e *Extractor) RunLoop(ctx context.Context, interval time.Duration) {
	e.svc.RunExtractionLoop(ctx, e.chat, interval, e.batch)
}

// extractObs is one uncovered observation selected for extraction.
type extractObs struct {
	id      uuid.UUID
	scope   string
	content string
}

// RunExtractionOnce converts up to batch uncovered observations into
// proposed facts via chat and returns how many proposals were asserted.
//
// Coverage: mem.extract_log (migration 004) records every ATTEMPTED
// obs_id regardless of outcome — zero proposals included — and the select
// anti-joins against it, so each observation is extracted at most once.
// This is what keeps progress monotonic: without coverage, observations
// yielding no proposals would rescan forever.
//
// Failure semantics, per observation:
//
//   - LLM error (any chat.Propose error while the run's own context is
//     still alive): the observation is STILL covered before the run aborts
//     and the error is returned; remaining observations stay uncovered for
//     the next tick. Covering the errored observation trades one lost
//     extraction for guaranteed queue progress — a model that
//     deterministically fails on specific content must not wedge everything
//     behind it. Client-side timeouts land here, NOT in the cancellation
//     class: an http.Client.Timeout surfaces as an error unwrapping to
//     context.DeadlineExceeded even though nothing is shutting down, so the
//     error's identity alone cannot distinguish a slow model from a
//     shutdown race — only the run context can. Already-processed
//     observations keep their proposals and coverage rows.
//   - Shutdown racing mid-Propose (the RUN context died, ctx.Err() != nil):
//     the observation is deliberately LEFT UNCOVERED when the run aborts,
//     so a shutdown race retries it on the next startup instead of
//     permanently burying a never-attempted observation behind an
//     extract_log row.
//   - Persistence error while applying ONE proposal (resolution or
//     AssertFact failure): log-and-skip that proposal; sibling proposals
//     of the same observation still apply, and the observation is covered.
//   - Persistence error while WRITING coverage itself: abort the run.
//     Continuing would re-select the same head-of-queue observations next
//     tick and duplicate their proposals.
//
// The select orders by ts ASC across ALL scopes — extraction is
// system-level, deliberately not scope-partitioned. At SOC volumes the
// full scan over mem.observations plus per-row probes against
// extract_log's obs_id index are fine; a watermark cursor is the future
// remedy if backlogs ever grow past that.
//
// Logging carries ids and counts only — never observation content or
// proposal values.
func (s *Service) RunExtractionOnce(ctx context.Context, chat extract.ChatClient, batch int) (int, error) {
	if chat == nil {
		return 0, fmt.Errorf("%w: nil chat client", ErrInvalidInput)
	}
	if batch <= 0 {
		return 0, fmt.Errorf("%w: extraction batch %d must be positive", ErrInvalidInput, batch)
	}

	pending, err := s.selectUncoveredObservations(ctx, batch)
	if err != nil {
		return 0, err
	}

	// Known predicates hint — best effort; extraction still works without it.
	var knownPredicates []string
	if stats, err := s.ListPredicates(ctx); err == nil {
		for _, p := range stats {
			knownPredicates = append(knownPredicates, p.Predicate)
		}
	} else {
		s.log.Warn("memory: list predicates for extraction vocabulary failed", "err", err)
	}

	asserted := 0
	for _, o := range pending {
		if err := ctx.Err(); err != nil {
			return asserted, err
		}

		proposals, perr := chat.ProposeWithPredicates(ctx, o.content, knownPredicates)
		switch {
		case perr == nil:
			if len(proposals) == 0 {
				s.log.Debug("memory: extraction yielded no proposals for observation",
					"obs_id", o.id,
					"content_snippet", truncateStr(o.content, 200))
			}
			n, aerr := s.applyProposals(ctx, o, proposals)
			asserted += n
			if aerr != nil {
				s.log.Warn("memory: extraction apply failed; observation covered anyway",
					"obs_id", o.id,
					"err", aerr)
			}
		case ctx.Err() != nil:
			// Shutdown raced the propose: leave the observation UNCOVERED so
			// the next startup retries it — coverage here would permanently
			// bury an observation that was never actually processed. The
			// branch keys on the RUN context alone: perr's identity cannot
			// make this call, because a per-request client timeout ALSO
			// unwraps to context.DeadlineExceeded (classifying it here would
			// leave every observation behind a slow model uncovered forever).
			s.log.Info("memory: extraction canceled mid-propose; observation left uncovered for retry",
				"obs_id", o.id)
			return asserted, fmt.Errorf("memory: propose facts for observation %s: %w", o.id, perr)
		default:
			s.log.Warn("memory: extraction propose failed; aborting run",
				"obs_id", o.id,
				"err", perr)
		}

		// Coverage lands AFTER processing (an attempt counts only once it
		// happened) but BEFORE any error return, so both the errored
		// observation and every earlier one stay covered when the run
		// aborts here — except a shutdown race, which returns above.
		if err := s.logExtractionCoverage(ctx, o.id); err != nil {
			return asserted, fmt.Errorf("memory: cover observation %s: %w", o.id, err)
		}
		if perr != nil {
			return asserted, fmt.Errorf("memory: propose facts for observation %s: %w", o.id, perr)
		}
	}
	return asserted, nil
}

// selectUncoveredObservations returns up to batch observations absent from
// mem.extract_log, oldest first. Duplicate physical rows can share one
// obs_id (RecordObservation performs no storage-level dedup); they dedupe
// here against the seen-set so a retry pair yields at most one extraction
// attempt per run — the first twin's coverage row already covers both.
func (s *Service) selectUncoveredObservations(ctx context.Context, batch int) ([]extractObs, error) {
	rows, err := s.conn.Query(ctx,
		"SELECT o.obs_id, o.scope, o.content "+
			"FROM mem.observations o "+
			"WHERE NOT EXISTS ("+
			"SELECT 1 FROM mem.extract_log x WHERE x.obs_id = o.obs_id"+
			") ORDER BY o.ts ASC LIMIT ?", batch)
	if err != nil {
		return nil, fmt.Errorf("memory: select uncovered observations: %w", err)
	}
	defer rows.Close()

	var out []extractObs
	seen := make(map[uuid.UUID]bool)
	for rows.Next() {
		var (
			id      uuid.UUID
			scope   string
			content string
		)
		if err := rows.Scan(&id, &scope, &content); err != nil {
			return nil, fmt.Errorf("memory: scan observation row: %w", err)
		}
		if seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, extractObs{id: id, scope: scope, content: content})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("memory: iterate uncovered observations: %w", err)
	}
	return out, nil
}

// applyProposals persists every sanitized proposal of one observation as
// an agent assertion attributed to extractor-v1, resolving each subject in
// the OBSERVATION's scope (extraction is global; entity identity stays
// scope-local like everywhere else). Sanitize has already gated subjects
// through entity.Normalize, so resolution failures here are defensive:
// they skip one proposal rather than fail the observation.
//
// A failed AssertFact never fails the whole call — one poison proposal
// must not bury its siblings; the caller covers the observation either way.
func (s *Service) applyProposals(ctx context.Context, o extractObs, proposals []extract.Proposal) (int, error) {
	if len(proposals) == 0 {
		return 0, nil
	}

	subjects := make([]string, len(proposals))
	for i, p := range proposals {
		subjects[i] = p.Subject
	}
	ents, err := s.resolver.ResolveBatch(ctx, o.scope, subjects)
	if err != nil {
		return 0, fmt.Errorf("resolve %d proposal subjects: %w", len(subjects), err)
	}

	asserted := 0
	for i, p := range proposals {
		if ents[i].EntityID == "" {
			s.log.Warn("memory: extraction proposal subject unresolved; skipping proposal",
				"obs_id", o.id,
				"proposal_index", i)
			continue
		}
		// If the object looks like a known entity (IP/domain/hash/technique),
		// resolve it so the fact can mint a graph edge. Non-entity values
		// (e.g. "C2 infrastructure") stay value-only.
		var objectID string
		if p.ObjectValue != "" {
			if _, err := entity.Normalize(p.ObjectValue); err == nil {
				if objEnt, _, objErr := s.resolver.Resolve(ctx, o.scope, p.ObjectValue); objErr == nil && objEnt.EntityID != "" {
					objectID = objEnt.EntityID
				}
			}
		}
		if _, err := s.AssertFact(ctx, FactInput{
			Scope:       o.scope,
			SubjectID:   ents[i].EntityID,
			Predicate:   p.Predicate,
			ObjectValue: p.ObjectValue,
			ObjectID:    objectID,
			Confidence:  p.Confidence, // clamped again inside AssertFact
			SourceObs:   o.id.String(),
			ActorType:   actorAgent,
			ActorID:     extractorActorID,
		}); err != nil {
			s.log.Warn("memory: extraction proposal assert failed; skipping proposal",
				"obs_id", o.id,
				"proposal_index", i,
				"err", err)
			continue
		}
		asserted++
	}
	return asserted, nil
}

// logExtractionCoverage records one attempted obs_id in mem.extract_log.
// The timestamp column rides its DEFAULT; duplicates under reruns or
// concurrent workers are harmless MergeTree rows (see migration 004).
func (s *Service) logExtractionCoverage(ctx context.Context, obsID uuid.UUID) error {
	if err := s.conn.Exec(ctx,
		"INSERT INTO mem.extract_log (obs_id) VALUES (?)", obsID); err != nil {
		return fmt.Errorf("insert extract_log row: %w", err)
	}
	return nil
}

// RunExtractionLoop ticks RunExtractionOnce every interval until ctx
// cancels — plain ticker semantics, so the first tick lands one full
// interval in (the worker is a background poller, not an ingestion-path
// component; design doc §10). Both arguments are validated: a non-positive
// interval or batch is refused fast (logged, loop exits) rather than
// allowed to spin hot against the database.
//
// Each tick is panic-isolated: a recovered panic — third-party chat
// clients included — logs and waits for the next tick instead of killing
// the worker goroutine. Ticks aborted by shutdown (ctx canceled mid-run)
// log nothing: cancellation is not an operational problem.
func (s *Service) RunExtractionLoop(ctx context.Context, chat extract.ChatClient, interval time.Duration, batch int) {
	if interval <= 0 {
		s.log.Error("memory: extraction loop refuses non-positive interval",
			"interval", interval)
		return
	}
	if batch <= 0 {
		s.log.Error("memory: extraction loop refuses non-positive batch",
			"batch", batch)
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.extractionTick(ctx, chat, batch)
		}
	}
}

func truncateStr(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}

// extractionTick guards one loop iteration. The recover wraps the whole
// tick body — RunExtractionOnce and whatever the chat client does inside
// it — so the loop survives to the next tick no matter where a tick blew
// up. Counters and errors are logged content-free.
func (s *Service) extractionTick(ctx context.Context, chat extract.ChatClient, batch int) {
	defer func() {
		if r := recover(); r != nil {
			s.log.Error("memory: extraction tick panicked; recovered until next tick",
				"panic", r)
		}
	}()

	n, err := s.RunExtractionOnce(ctx, chat, batch)
	switch {
	case err != nil && ctx.Err() != nil: // shutdown raced the tick
	case err != nil:
		s.log.Warn("memory: extraction run aborted; will retry next tick",
			"proposals_asserted", n,
			"err", err)
	case n > 0:
		s.log.Info("memory: extraction tick complete", "proposals_asserted", n)
	}
}
