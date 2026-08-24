# Development Guide

## Setup

```bash
git clone <repo> && cd soc-memory-layer
go build ./...          # Go 1.22+; module name: socmem
task db-up              # ClickHouse 26.3 (:9000) + Dgraph standalone (:9080), waits healthy
task itest              # full suite against live stores (auto-starts them)
```

TEI embedder on `localhost:3000` (started by `task db-up`) is optional — the
service degrades gracefully without it, but similarity's vector leg and the
extraction worker need it (`lms server start`).

## Daily commands (Taskfile)

| Task | Does |
|---|---|
| `task build` | compile everything |
| `task test` | unit tests only (no DBs) |
| `task itest` | integration suite, `-p 1`, both store env vars set |
| `task db-up` / `task db-down` | start stack (health-gated) / stop + wipe volumes |
| `task run` | run memserved on :8080 |
| `task swagger` | regenerate OpenAPI from handler annotations |
| `task seed` / `task eval` | backfill sample data / recall@k gate |

## Testing conventions

These are load-bearing — read before writing tests:

- **Unit tests never touch databases.** Pure logic (normalizers, trust rules,
  extractors, RRF assembly, limiter) lives in table-driven tests.
- **Integration tests skip unless env is set**: `MEM_TEST_CH_ADDR` /
  `MEM_TEST_DGRAPH_ADDR`. Helpers: `itestConn`/`itestScope` (memory),
  `itestGraph` (graph), `buildService`/`testServer` (api).
- **Never TRUNCATE shared tables** — parallel packages share one ClickHouse.
  Isolate via unique per-test scopes (`itestScope()` = nanotime suffix).
- The suite runs with **`-p 1`**: graph and memory packages both wipe/reinstall
  the Dgraph schema in setup, so they must not run concurrently.
- Seed through **real writers** (`RecordObservation`/`AssertFact`) whenever
  possible — raw inserts miss side effects (edges, audit) that later assertions
  depend on. When you must plant raw rows (dual-open healing tests), guard the
  premise first.
- Time-sensitive assertions sleep ~1.1s (DateTime second granularity;
  DateTime64(3) version ties) and compare against DB state, not returned
  structs.

## Code conventions

- Errors: `fmt.Errorf("memory: <stage> %w", err)` style prefixes per package;
  sentinels for API-mappable conditions (`ErrHumanGated` → 403,
  `ErrFactNotFound` → 404, `ErrConflict` → 409, `ErrInvalidInput` → 400).
- Validation errors wrap `ErrInvalidInput`; storage faults are detected via
  `ch.IsStorageError` → 502 at the edge.
- **Content never reaches logs or audit.** Audit summaries carry counts,
  statuses, keys. Tests assert absence of content substrings negatively.
- All SQL user input bound via `?`; only int constants may be `%d`-formatted.
- Timestamps: UTC everywhere; DateTime columns are second-granularity (sleep
  1.1s when a test needs distinct values); cursor binds use explicit ms
  strings (clickhouse-go truncates bare time.Time binds).
- New identifiers use `internal/ids.New()` (UUIDv7).

## How to add things

### An HTTP endpoint
1. Handler + doc-only request/response structs in `internal/api/handlers.go`
2. swaggo annotations (Summary/Params/Responses/@Security) — `task swagger`
3. Route in `server.go Routes()`; write endpoints wrapped `identity(writeLimit(…))`
4. Identity from context only; map errors via `mapServiceError`
5. Integration test over httptest with identity headers

### A service operation
Follow the AssertFact pattern: validate → classify errors as
`ErrInvalidInput`-wrapped → do storage work → best-effort audit → return a
projection struct (not raw rows). Add integration test incl. one negative and
one zero-rows case.

### A migration
Numbered file `internal/ch/migrations/00N_name.sql`; every statement must be
idempotent (IF NOT EXISTS); seed rows guarded by NOT EXISTS. The runner
records filenames; mid-file failure + rerun must converge.

### An MCP tool
Handler in `internal/mcpserver/server.go`: parse args → call service → recall
results fenced via `fencedResult`, writes plain JSON. Tool description written
for LLM consumption; annotations (readOnly/destructive) set honestly.

## Review workflow

Every change passes two independent reviews before merge: **spec compliance**
(does it match what was asked — nothing more/less, verified against live
systems, never trusting the implementer's report) then **code quality**
(correctness, maintainability, test quality). Fix loops repeat until approved.
Direct-to-main commits of unreviewed work are how the '9999 date sentinel bug
class survives.

## Debugging tips

- `EXPLAIN indexes = 1` proves which indexes a query used (vector/text/bloom).
- `docker compose exec clickhouse clickhouse-client --user mem --password memdev`
  for direct probes; same pattern with `curl localhost:18080/query` + DQL for Dgraph.
- Projection stuck? Check `mem.projection_watermark` vs max(updated_at), then
  `system.mutations` backlog.
- Extraction silent? It's disabled unless `MEM_EXTRACT_ENABLED=true`.
