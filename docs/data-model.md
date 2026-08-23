# Data Model Reference

*Authoritative shapes as of Phase 2. Migrations live in `internal/ch/migrations/`; Dgraph schema is installed by `internal/graph.InstallSchema`.*

## ClickHouse — `mem` database

### `entities` — canonical objects

`ReplacingMergeTree(updated_at DateTime64(3)) ORDER BY (scope, entity_type, key)`

| Column | Type | Notes |
|---|---|---|
| entity_id | UUID | stable id; survives last_seen refreshes |
| scope | LowCardinality(String) | team/tenant partition |
| entity_type | Enum8 | ioc_ip, ioc_domain, ioc_hash, asset, user, actor, campaign, malware, technique, tool, case, document |
| key | String | normalized value: `1.2.3.4`, sha256 hex, `T1566`, lowercase domain |
| display_name | String | original raw text at first sight |
| attrs | Map(String,String) | reserved |
| first_seen / last_seen | DateTime | last_seen refreshes on re-resolution (upsert re-insert, same entity_id) |

Dedup contract: `(scope, entity_type, key)` is the identity. Lookups use
FINAL; the resolver documents last-write-wins semantics for concurrent
first-sight races.

### `observations` — episodic layer (append-only)

`MergeTree ORDER BY (scope, ts)` · text index (`splitByNonAlpha`) · conditional TTL: alert rows expire after 365 days (eventual, on merge)

obs_id UUID (UUIDv7 going forward; ClientEventID retries reuse it and create a second physical row — plain MergeTree never dedups) · kind Enum8(alert, triage_decision, investigation_note, hunt_finding, agent_action, human_statement) · actor_type/actor_id/on_behalf_of attribution · case_id Nullable(UUID) · confidentiality Enum8(internal, restricted) — internal = readable org-wide (default), restricted = originating scope ONLY (enforced on every observation read since ADR-010) · content String · content_vec Array(Float32) — empty when embedding unavailable · entity_refs Array(UUID)

### `facts` — distilled semantic layer

`ReplacingMergeTree(updated_at DateTime64(3)) ORDER BY (scope, subject_id, predicate, object_value)` — **object_value is inside the sort key**, so supersede requires an explicit mutation closing priors; FINAL alone cannot collapse different values.

fact_id UUID (new per version) · status Enum8(proposed, active, retracted) DEFAULT 'active' — **always written explicitly** (fail-open column default) · confidence Float32 (clamped [0,1], NaN→0) · source_obs UUID → provenance to the observation that caused it · valid_from / valid_to DateTime (sentinel 2105-12-31; bounded because '9999' silently wraps in UInt32-backed DateTime)

Read path: `FINAL WHERE status='active' AND valid_from <= now AND valid_to > now AND (scope = caller OR visibility = 'org')` — the last clause implements org-wide sharing (ADR-010).

### `edges` — graph source-of-truth rows

`MergeTree ORDER BY src_id` (+ updated_at DateTime64(3), bloom/minmax skip indexes from migration 003)

Written when a fact lands `active` with non-null object_id; closed by mutation when superseded/retracted. The Dgraph projection replays this table.

### `audit` — write trail, NO TTL ever

ts · actor_type Enum8(human, agent, system) · actor_id · operation · target_table · target_id · payload_summary (**counts and keys only — never observation/fact content**). Best-effort on write-path failures by documented decision.

### `projection_watermark` — projection cursors

name ('entities'|'edges') · ts DateTime64(3) · last_id String. Advanced CAS-style; see ADR-007 for why ordering is textual `toString(id)`.

### `extract_log` — extraction coverage

obs_id UUID (ORDER BY) · extracted_at. Presence = "extraction attempted"; written regardless of outcome to prevent poison wedges. Cancellation races deliberately leave rows absent so shutdown-raced observations retry.

## Dgraph — projected graph

Schema (idempotent ALTER):

```
scope: string @index(hash) @upsert .
key: string @index(hash) @upsert .
ch_id: string @index(exact) @upsert .
entity_type: string @index(hash) .
display_name: string .
first_seen: datetime @index(hour) .
last_seen: datetime .
related_to: [uid] @reverse .
type Entity { ... }
```

- One node per mem.entities row; `ch_id` is the immutable join key back to ClickHouse.
- Edges: `<src> related_to <dst>` with facets `(relation, valid_from, valid_to)`.
  - **One relation per ordered pair** — a second relation overwrites facets (ADR-003).
  - Facets read only via `@facets(...)` sub-selection; v25 has no facet schema directives.
  - Closed edges are deleted at projection time.
- `@reverse` supports inbound hops without storing duplicate edges.

## Identifier policy

All new identifiers are **UUIDv7** (`internal/ids.New`, v4 fallback on clock
error): textual order equals creation order, which makes every
`toString(id)` cursor tiebreaker chronological. Legacy v4 rows textually sort
after all v7 ids — monotonic cursors remain correct (ADR-007).

## Invariants worth testing after any change

1. Facts always carry explicit status (column default is fail-open) and explicit visibility.
2. Proposals never close prior active facts; only `active` versions trigger supersede waves.
3. Audit summaries contain counts/keys/statuses — never observation or fact content.
4. Edge closure mutations stamp `updated_at`, or closures become invisible to the projector.
5. Every read query filters visibility: `(scope = caller OR visibility/confidentiality gate)` — restricted content is fully invisible cross-team; by-case timelines stay scope-local.
6. Entity keys are shared knowledge's join currency: normalization changes (e.g., hyphen handling) alter which rows connect across teams.
