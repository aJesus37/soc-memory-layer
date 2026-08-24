# Architecture

*How the SOC memory layer works end-to-end. Ground truth is the code; this document explains intent and behavior. Companion docs: [data-model](data-model.md), [api](api.md), [development](development.md), [runbook](runbook.md), [ADR index](adr/README.md).*

## System overview

```
                                   ┌────────────────────────────┐
   humans (HTTP headers) ────────▶ │                            │
                                   │      Memory Service        │
   agents (MCP stdio) ───────────▶ │        (memserved)         │◀── recall queries
                                   │  validation · resolution   │    (enrich/similar/
                                   │  embedding · audit         │     timeline/traverse)
                                   └──────────┬─────────────────┘
                                        │     │ sync writes
                          async workers │     ▼
                    ┌───────────────────┤  ┌──────────────────────────┐
                    │  projection (5s)  │  │       ClickHouse          │ ◀── SOURCE OF TRUTH
                    │  extraction (30s) │  │ observations · facts ·    │    entities/facts/
                    └─────────┬─────────┘  │ edges · entities · audit  │    edges/entities/audit
                              ▼            └──────────────────────────┘
                    ┌──────────────────┐
                    │  Dgraph (derived)│  ◀── rebuildable: DropData +
                    │  entities+edges  │      replay watermarks anytime
                    └──────────────────┘
```

Three processes exist:

| Process | Role |
|---|---|
| `memserved` | HTTP API + both background workers |
| `memmcp` | MCP stdio server exposing the same service to LLM clients |
| `graphrebuild` | one-shot: wipe Dgraph, replay everything from ClickHouse |

**One rule governs the whole design: ClickHouse is the only source of truth.** Dgraph is a cache you may drop at any time; `cmd/graphrebuild` reconstructs it byte-for-byte from `mem.entities` + `mem.edges`.

## Components

| Package | Responsibility |
|---|---|
| `internal/memory` | domain logic: observation ingestion, fact lifecycle, recall, traversal, extraction loop, trust rules |
| `internal/entity` | key normalization (IP/domain/hash/technique), entity resolution (lookup-or-create, batched) |
| `internal/ch` | ClickHouse connection + ordered SQL migrations (`migrations/*.sql`) |
| `internal/graph` | Dgraph connection, schema install, projection writers |
| `internal/embed` | embedding client (OpenAI-compatible; TEI sidecar in prod) |
| `internal/extract` | chat client + strict JSON prompt for fact proposals |
| `internal/api` | HTTP transport: routing, identity middleware, rate limiting, Swagger annotations |
| `internal/mcpserver` | MCP tool layer (transport-agnostic), consumed by `cmd/memmcp` |
| `internal/config` | env-based configuration |
| `internal/ids` | UUIDv7 identifier generation (v4 fallback) |

## Data flows

### Ingestion (write path)

```
POST /v1/observations
  1 validate (kind/actor/confidentiality whitelists, content ≤10k chars)
  2 extractEntityCandidates   tokens over [A-Za-z0-9.:-], host:port stripped,
                              hyphens preserved inside tokens, ≤32 candidates
  3 resolver.ResolveBatch     normalize → lookup-or-create mem.entities
  4 embed(content)            TEI BAAI/bge-base-en-v1.5, kind=document;
                              failure ⇒ empty vector + embedded=false (write succeeds)
  5 INSERT mem.observations   append-only
  6 audit row                 counts-only summary (never content); best-effort
```

### Fact lifecycle (semantic memory)

```
AssertFact ─▶ validate ─▶ ApplyTrust ─┬─ active: close prior open facts of the
                                      │  same (scope,subject,predicate) [mutation]
                                      │  + insert new version + mint edge row
                                      │  when object_id present
                                      └─ proposed: insert only (never closes priors)
PromoteFact (human)  proposed ─▶ active  (new version carries provenance)
RetractFact (human)  any ─▶ retracted    (born-closed: valid_from == valid_to)
```

Version-at-read: `mem.facts` is `ReplacingMergeTree(updated_at)`; readers use
`FINAL` + validity window (`valid_from <= now < valid_to`). Superseded versions
physically collapse over time; the durable history of who-wrote-what lives in
`mem.audit` (no TTL, ever).

Trust policy (§ ADR-004): humans assert `active`; agents land `proposed`
unless the predicate is whitelisted (`MEM_TRUST_WHITELIST`, default
`resolved_to`) with confidence ≥ floor (`MEM_TRUST_FLOOR`, default 0.8).

### Recall (read paths)

| Query | Legs | Notes |
|---|---|---|
| `Enrich` | open facts + recent observations + neighbors | 4 sequential scoped queries; honest miss on unknown key |
| `Similar` | RRF fusion of vec leg (cosine top-20) + text leg (hasAnyTokens top-20) | score = Σ 1/(60+rank); degrades text-only when embedder down; dedupes retry-duplicated rows |
| `Timeline` | UNION of observations + facts by case or entity | by-case bridges via observation entity_refs (cap 200 subjects) |
| `Traverse` | Dgraph hop expansion (≤3) with facet filtering; fallback ≤1-hop CH joins when Dgraph absent/erroring | projection lag yields empty result, not an error |

Recall results are **not audited** (no side effects); provenance lives on writes.

### Projection (Dgraph)

Two independent watermark cursors (`mem.projection_watermark`: composite
`(updated_at, id)` keyset, monotonic via compare-and-set) drive:

- **ProjectEntities** — upsert nodes keyed on immutable `ch_id` predicate
  (cond-guarded blank-node insert / uid-stable update; `@upsert` makes
  concurrent commits conflict-safe). UUIDv7 identifiers mean `toString(id)`
  order equals creation order, which is exactly what the cursor tiebreaker
  compares.
- **ProjectEdges** — open edges upsert `src -[related_to]-> dst` with facets
  `(relation, valid_from, valid_to)`; closed edges delete the triple.
  Closures are pagination-visible because every edge writer stamps
  `updated_at`, including the mutation-closers.

Invariant: any failure returns **before** the cursor advances → safe replay
(writes are idempotent). Known gap (documented): a refresh committing under an
already-advanced cursor is picked up on that entity's next touch.

### Extraction ("dreaming-lite")

Every 30s (when `MEM_EXTRACT_ENABLED=true`):

```
anti-join: observations NOT IN mem.extract_log (oldest first, batch 32)
  per obs: extract.ChatClient.Propose(content)
             ├─ strict JSON prompt, content ONLY in user role
             ├─ sanitize: subject normalizable, predicate charset,
             │            object ≤500 chars, confidence clamped
             └─ transport error: cover obs + abort run (retry next tick)
  per proposal: ResolveBatch(scope) → AssertFact{agent, extractor-v1,
             source_obs=obsID} → status='proposed' (whitelisted predicates
             may auto-activate at ≥ floor — operator policy, not a bypass)
  cover obs in mem.extract_log regardless of outcome (poison-wedge prevention)
```

Cancellation nuance: a *shutdown race* (run ctx dead mid-propose) leaves the
observation uncovered so it retries next startup; a slow-model *timeout*
counts as attempted and covers it. Both are tested.

## Consistency model

ClickHouse has no cross-statement transactions. The chosen stances:

- **Fact supersede** = mutation (close priors, `mutations_sync=1`) then insert.
  Failure between them can close priors without inserting the replacement —
  accepted, logged, documented (Phase-1 review).
- **Concurrent asserts** can transiently leave two open facts on distinct sort
  keys; the next supersede heals. Documented, accepted.
- **Projection** is eventually consistent by design; readers see empty results
  during lag rather than falling back mid-flight (traverse's error-fallback is
  the exception).
- **Audit** writes are best-effort; observation success is authoritative.

## Trust & security model

- Identity arrives exclusively via headers (`X-Actor-Type/ID`, `X-Scope`,
  `X-On-Behalf-Of`); bodies cannot carry it (unknown fields rejected).
- Agents inherit the requesting user's scope; per-agent write budgets
  (token bucket, default 5 rps) return 429 + `Retry-After`.
- Humans gate every state transition: promote/retract reject non-human actors
  at service level (`ErrHumanGated` → 403).
- Model output is untrusted data: sanitized structurally, stored as proposals,
  never interpolated into queries; recalled memories returned through MCP are
  fenced (`<memory-context>` + data-not-instructions note).
- Audit records every write attributed to actor + on_behalf_of, content-free.

## Scaling posture (measured, § Phase-1 plan §5)

Brute-force vector scan stays comfortable to ~10⁵–10⁶ rows; the HNSW-class
index and QBit exist but were unnecessary here. The graph exists for traversal
ergonomics, not scan speed. Extraction/projection workers are batched and
idempotent — horizontal scale starts with sharding scopes, not rewriting loops.
