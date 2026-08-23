# ADR-007: Keyset cursor with textual UUID ordering; UUIDv7 identifiers

Status: accepted · Date: 2026-08-23

## Context

Projection workers need to page ClickHouse changes into Dgraph without skips
or repeats. Row identity is UUID. Two empirical facts about ClickHouse 26.3
drove this decision:

1. **Native UUID comparison is not RFC-canonical.** Internally the two 64-bit
   halves are compared in swapped order relative to the textual form.
   Verified live: a random v4 UUID sorts *between* two v7 UUIDs under native
   `ORDER BY`.
2. **`toString(uuid)` order equals creation order for UUIDv7** (the leading
   48 bits are unix-milliseconds), and for our mixed population all v7 ids
   textually precede legacy v4 ids.

## Decision

- Projection cursors are composite keyset cursors `(updated_at, last_id)`
  comparing **textual** id order — `(updated_at, toString(id)) > (?, ?)` with
  matching `ORDER BY`. All three comparators (page predicate, ORDER BY,
  cursor CAS) share one total order, pinned by regression test using
  engineered UUIDs where byte-order and text-order disagree.
- Cursor advancement is compare-and-set (`WHERE (ts,last_id) < (?,?)`) so
  overlapping callers cannot rewind the watermark.
- All new identifiers (entities, observations, facts, edges) are **UUIDv7**
  (`internal/ids.New`, v4 fallback on clock error). Consequence: textual id
  order becomes *chronological* — cursor tiebreakers, audit scans and
  debugging gain time-ordering semantics for free.

## Rejected alternatives

- Pure timestamp watermark: same-millisecond write clusters (routine —
  ResolveBatch stamps one `now` per call) livelock a ts-only cursor or skip
  boundary rows depending on comparison strictness.
- Native UUID ordering: broken per fact 1 above.
- Per-statement progress recording: rejected as premature complexity; upsert
  replay is already harmless.

## Consequences

- Mixed v4/v7 populations sort "v7-era first, then legacy tail" textually;
  monotonic cursors remain correct, only "oldest-first" intuition bends.
- Same-millisecond v7 ties resolve randomly within the ms — the composite
  `(ts, id)` cursor plus FINAL/versioned reads absorb this.
- clickhouse-go binds untyped `time.Time` at second precision; all
  millisecond-precision binds use explicitly formatted strings (documented at
  each site after a live-probe bug).
