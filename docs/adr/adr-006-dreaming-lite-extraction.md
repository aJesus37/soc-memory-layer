# ADR-006: Async batched fact extraction ("dreaming-lite")

Status: accepted · Date: 2026-08-23

## Context

Observations contain facts worth distilling (company context, verdicts,
relationships). Extraction options: synchronous per-write LLM calls, a
per-turn tool the model invokes, or a background pipeline.

## Decision

Extraction runs as a **background worker** (default every 30s when enabled):
anti-join observations against `mem.extract_log` coverage, ask the local chat
model for structured proposals (strict JSON, content only in user role),
sanitize structurally, resolve subjects, and `AssertFact` them as
`agent/extractor-v1` — landing `proposed` per ADR-004.

## Rejected alternatives

- **Sync at write time:** adds model latency to ingestion; triage writes must
  stay sub-second.
- **Model-invoked tool only:** depends on instruction-following discipline;
  weaker agents would silently never remember anything. (The MCP write tools
  exist for interactive use regardless.)
- **Nightly-only:** starves triage/hunting agents of same-hour context.

## Consequences

- Coverage tracking (`mem.extract_log`, log-after-process) prevents both
  poison wedges and infinite reprocessing; shutdown races deliberately leave
  observations uncovered for retry.
- Model errors cover-and-abort the run; progress resumes next tick.
- Whitelisted predicates can auto-activate from extraction — an operator
  policy decision made visible via config, not a bypass.
- Legacy backlogs queue ahead of fresh rows (ts ASC); drain once with
  coverage backfill if it matters operationally.
