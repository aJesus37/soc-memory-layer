# ADR-001: ClickHouse as sole source of truth; graph as rebuildable projection

Status: accepted · Date: 2026-08-22

## Context

A SOC memory layer needs three storage shapes: episodic events (append-only,
time-ordered), distilled facts (versioned, validity windows), and entity
relationships (traversable). Team scale: 5–20 analysts, thousands of
alerts/day. Platform capacity exists for 2–3 stateful services.

## Decision

ClickHouse holds **all** authoritative state (observations, facts, entities,
edges, audit). The graph store holds a derived projection that can be dropped
and replayed from ClickHouse at any time. A single Memory Service is the only
writer to either store.

## Alternatives considered

1. **Graph-shaped schema in ClickHouse only** — simplest ops; rejected because
   deep/ad-hoc traversal ergonomics (recursive CTEs) are poor and the team
   wanted exploratory query capability they cannot yet predict.
2. **Graph-centric temporal KG as the spine** (Zep/Graphiti pattern) — most
   faithful to CTI modeling; rejected because it puts LLM-extraction errors
   into the source of truth and adds the heaviest infra footprint.
3. **Two independent systems with bidirectional sync** — rejected outright:
   no single writer, no rebuild story, consistency becomes a distributed
   systems problem.

## Consequences

- Graph engine swaps or corruption cost a replay (`cmd/graphrebuild`), never
  data loss.
- Eventual consistency between stores is acceptable; readers get empty results
  during projection lag rather than partial fallbacks (traverse's error
  fallback is the one exception).
- Every edge row lives in ClickHouse first; the graph never invents structure.
