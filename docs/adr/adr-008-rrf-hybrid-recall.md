# ADR-008: Reciprocal Rank Fusion for hybrid recall

Status: accepted · Date: 2026-08-22

## Context

Pure vector search misses exact identifiers; pure keyword search misses
paraphrases. Hybrid search needs to merge rankings produced on incompatible
scales (cosine distances vs token-hit membership). Current ClickHouse releases
ship no built-in BM25 scoring function.

## Decision

`Similar` runs two independent legs — vector (brute-force cosine top-20) and
text (`hasAnyTokens` top-20 over the text index) — and fuses them with
**Reciprocal Rank Fusion**: `score = Σ 1/(60 + rank)` per contributing leg.
No score normalization is needed because only ranks, never scores, are fused.
Both legs are scope-filtered; retry-duplicated rows dedupe to their best rank
per leg before fusion so one observation contributes at most one term per
source.

## Consequences

- An embedder outage degrades gracefully to text-only (matched_by exposes the
  contributing sources for debugging).
- k=60 smoothing makes rank differences soft; a row appearing in both legs
  roughly doubles its contribution — empirically the right prior for SOC
  queries ("agreement between independent evidence ranks first").
- Text-leg ranks are recency-biased by construction (`ORDER BY ts DESC`
  inside the window function); documented rather than fought.
- No BM25 dependency exists; if ranking quality ever demands it, the text leg
  can be upgraded without touching the fusion contract.
