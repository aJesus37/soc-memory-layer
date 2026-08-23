# ADR-003: Facet-based edge modeling

Status: accepted · Date: 2026-08-23

## Context

Edges must carry: relation name (open-ended — `communicates_with`,
`resolved_to`, `attributed_to`, …), validity window, and provenance
(`from_fact`). Dgraph predicates are the edge types; declaring one predicate
per relation means runtime schema churn as new relations appear.

## Decision

One predicate, `related_to: [uid] @reverse .`, with **facets** carrying
`(relation, valid_from, valid_to)`. Nodes are keyed by the immutable
ClickHouse `ch_id` predicate so projection upserts and traversal hydration are
idempotent.

## Consequences

- **Constraint: one relation per ordered (src, dst) pair.** Writing a second
  relation between the same pair overwrites the first's facets at projection
  time. Accepted for Phase-2 scope; revisit with per-relation predicates if
  threat-hunting workflows demand coexisting multi-relations.
- Closed edges are *deleted* from Dgraph by the projection (validity windows
  live in ClickHouse; the graph shows only currently-valid links). Deletion is
  direction-specific (`src -> dst` triple).
- Reading facets requires an explicit `@facets(relation)` sub-selection;
  omitting it omits the edge silently — all read code follows this rule.
- v25 removed facet schema directives; facets need no schema declaration but
  NQuads writes use single-paren facet syntax.
