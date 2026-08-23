# ADR-002: Dgraph v25 as the graph engine

Status: accepted · Date: 2026-08-23

## Context

Phase-1 architecture requires a rebuildable graph projection. Candidates:
Dgraph v25, Neo4j Community, FalkorDB/Memgraph.

## Decision

Dgraph v25 (standalone container in dev, single alpha in prod).

## Rationale

- DQL/GraphQL query surface gives agents and humans exploratory traversal
  without hand-built DSLs.
- Actively maintained under Istari Digital since 2025-10: v25.3.x line,
  security fixes flowing, enterprise license removed (fully OSS).
- Predicate-level model + `@upsert` + facets map cleanly onto our
  entity/edge projection.
- Governance risk (two ownership changes 2022–2025) is contained by
  ADR-001's rebuildable-projection architecture — worst case is a replay
  into another engine over a weekend.

## Alternatives considered

- **Neo4j Community** — safest ecosystem; JVM footprint; Cypher lock-in with
  no compensating capability we need at this scale. Chosen fallback if Dgraph
  maintenance regresses.
- **FalkorDB/Memgraph** — lighter, but smaller ecosystems and licensing
  nuances (BSL) without decisive advantages here.

## Consequences

- Dev runs `dgraph/standalone`; prod runs a single alpha (distribution is
  unnecessary at this scale).
- Facet schema directives were removed in v25 — facets work at the data level
  but require `@facets(...)` sub-selections to read (see ADR-003).
- In-memory index sizing must be monitored; the dataset fits comfortably for
  years at team scale.
