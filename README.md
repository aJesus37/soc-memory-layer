# soc-memory-layer

Shared memory layer for a security operations team: investigations (human +
agent), alert triage context, and company knowledge — stored in ClickHouse,
served over HTTP, consumable by analysts and agents alike.

Knowledge is **org-shared by default**: a fact team B asserts about a domain
is immediately visible (with attribution) when team A enriches that domain.
`restricted` items stay inside their team. See ADR-010.

**Status:** Phases 1–2 complete — memory core, Dgraph projection, dreaming-lite
extraction, MCP server. Phase 3: UUIDv7 identifiers, hyphenated-domain
extraction, full documentation set.

- Design: `docs/plans/2026-08-22-soc-memory-layer-design.md`
- Plans: `docs/plans/` · Docs index below

## Documentation

| Doc | Contents |
|---|---|
| [docs/architecture.md](docs/architecture.md) | how everything works end-to-end |
| [docs/api.md](docs/api.md) + `/swagger/index.html` | HTTP + MCP reference |
| [docs/data-model.md](docs/data-model.md) | every table, predicate, invariant |
| [docs/adr/](docs/adr/README.md) | architecture decision records |
| [docs/development.md](docs/development.md) | setup, conventions, how-to-extend |
| [docs/runbook.md](docs/runbook.md) | ops procedures & troubleshooting |

## Quickstart

```bash
# 1. Dev database
task db-up                       # ClickHouse 26.3 on :9000 (user mem / memdev)

# 2. Tests (unit always; integration needs the DB)
task test                        # unit only
MEM_TEST_CH_ADDR=localhost:9000 go test ./... -count=1   # integration

# 3. Run the service (embedding server optional — degrades gracefully)
lms server start                 # LM Studio with nomic-embed-text-v1.5 loaded
task run                         # listens on :8080

# 4. Write + read
curl -s localhost:8090/v1/observations \
  -H 'Content-Type: application/json' \
  -H 'X-Actor-Type: human' -H 'X-Actor-ID: analyst-j' -H 'X-Scope: team-a' \
  -d '{"kind":"human_statement","content":"Saw 1.2.3.4 beaconing to evil.example.com"}'

curl -s 'localhost:8090/v1/enrich?type=ioc_ip&key=1.2.3.4' \
  -H 'X-Actor-Type: agent' -H 'X-Actor-ID: triage-bot' -H 'X-Scope: team-a'

# 5. Backfill history / evaluate recall
go run ./cmd/memseed -file seeds/smoke.jsonl
go run ./cmd/memeval             # recall@k gate; exits 1 on regression
```

## Identity & scoping

Every request carries identity via headers — never via body:

| Header | Values |
|---|---|
| `X-Actor-Type` | `human` \| `agent` |
| `X-Actor-ID` | analyst id or agent name+version |
| `X-On-Behalf-Of` | requesting user when actor is an agent |
| `X-Scope` | team/tenant scope; all rows are scoped by it |

Humans assert facts (`active`); agents propose them (`proposed`) unless the
predicate is whitelisted with sufficient confidence. Promotion/retraction is
human-gated at service level (agents get 403).

## Rate limiting

Agent WRITE endpoints (`POST /v1/*`) are budgeted per `X-Actor-ID` with a
token bucket (5 req/s, burst 5; over-budget writes get 429 + a `Retry-After`
header). Humans are exempt — they gate themselves. This is unrelated to
`MEM_TRUST_*`, which governs agent-fact auto-activation, not throughput.

## API

| Endpoint | Purpose |
|---|---|
| `POST /v1/observations` | append episodic event (auto entity-linking, embedding, audit) |
| `POST /v1/facts` | assert a versioned fact |
| `POST /v1/facts/{id}/promote` | proposed → active (humans only) |
| `POST /v1/facts/{id}/retract` | withdraw a fact, keeping history |
| `GET /v1/enrich?type=&key=` | what do we know about X: open facts + recent obs + neighbors |
| `GET /v1/similar?q=&k=` | hybrid vector+text recall (RRF) |
| `GET /v1/timeline?case_id=\|entity_id=&limit=&offset=` | chronological reconstruction |
| `GET /healthz` | storage ping |

Errors: 400 invalid input · 403 `human_gated` · 404 `fact_not_found` · 409
`conflict` · 429 rate-limited (agent writes, `Retry-After` header) · 502
`storage_error` ("storage temporarily unavailable" — driver detail never
leaks to clients). Unknown JSON fields are rejected — typos fail loudly.

## Configuration (env)

`MEM_CH_ADDR` (:9000) · `MEM_CH_USER` (mem) · `MEM_CH_PASSWORD` (memdev) ·
`MEM_LISTEN_ADDR` (:8080) · `MEM_EMBED_URL` (http://localhost:1234/v1) ·
`MEM_EMBED_MODEL` (nomic-embed-text-v1.5) · `MEM_TRUST_FLOOR` (0.8) ·
`MEM_TRUST_WHITELIST` (resolved_to) · client timeout for embeddings is 120s
(model cold-start).

## Operations notes

- **Embedding outage:** observations persist with empty vectors and
  `embedded=false`; similarity falls back to text-only. A backfill re-embed is
  future work (re-run memseed with same `client_event_id`s to repair vectors).
- **TTL is eventual:** expired alerts vanish on part merge, not instantly.
  Backups taken pre-merge still contain them — restore + first merge purges.
- **Audit has no TTL**, ever. It is evidence-adjacent.
- **Mutations backlog:** supersede/promote/retract use `ALTER UPDATE`;
  monitor `system.mutations` if write volume grows.
- **Backup:** native ClickHouse backups cover everything (the Phase-2 graph
  projection is rebuildable from `mem.edges`, so it needs none).

## Phase 2 — graph, extraction, MCP

### Dgraph projection (rebuildable)

Entities and open edges project into Dgraph every 5s (watermark cursors in
`mem.projection_watermark`). ClickHouse stays the sole source of truth — the
graph can be dropped and replayed at any time:

```bash
go run ./cmd/graphrebuild            # wipe + replay from mem.entities/mem.edges
```

Edges carry facets (`relation`, validity window). One relation per ordered
entity pair (last write wins) — known modeling constraint, revisit if hunting
needs multi-relational pairs. Traversal filters validity at read time.

**Ops:** monitor Dgraph memory (in-memory indexes); `task db-down` wipes both
stores; projection lag = watermark ts vs now.

### Dreaming-lite fact extraction

Off by default. Enable with:

```bash
MEM_EXTRACT_ENABLED=true MEM_EXTRACT_MODEL=qwen/qwen3-8b task run
```

A worker polls observations lacking fact coverage every 30s, asks the local
chat model for structured candidate facts, and asserts them as
`proposed` (actor `extractor-v1`) — except whitelisted predicates
(`resolved_to` by default) which auto-activate at confidence ≥ floor.
Coverage lives in `mem.extract_log`; a hostile/garbage model response covers
the observation and moves on (no wedge). Review proposals via
`GET /v1/enrich` + human promote/retract as usual.

**Extraction is LLM output: treat proposals as untrusted.** They land behind
the same trust policy, scoping and audit as agent writes.

### MCP server — local stdio or shared remote

**Local (stdio):** each machine builds `memmcp` and points it at the central
stores. opencode config (`opencode.json`):

```json
{
  "$schema": "https://opencode.ai/config.json",
  "mcp": {
    "soc-memory": {
      "type": "local",
      "command": ["/path/to/memmcp"],
      "environment": {
        "MEM_CH_ADDR": "clickhouse.internal:9000",
        "MEM_DGRAPH_ADDR": "dgraph.internal:9080",
        "MEM_MCP_ACTOR_TYPE": "agent",
        "MEM_MCP_ACTOR_ID": "opencode-agent",
        "MEM_MCP_SCOPE": "team-a"
      }
    }
  }
}
```

**Remote (shared deployment):** one memmcp serves the whole team over
Streamable HTTP with per-user bearer tokens:

```bash
# tokens.json: [{"token":"smem_<64hex>","actor_type":"human","actor_id":"analyst-j","scope":"team-a"}, ...]
MEM_MCP_HTTP_ADDR=127.0.0.1:18443 MEM_MCP_TOKENS_FILE=tokens.json ./memmcp
# terminate TLS at your reverse proxy; tokens are bearer credentials
```

opencode config for the remote server:

```json
{
  "mcp": {
    "soc-memory": {
      "type": "remote",
      "url": "https://memory.internal.company/mcp",
      "headers": { "Authorization": "Bearer smem_<your-token>" }
    }
  }
}
```

Token file rules: `smem_` + 64 hex chars, unique, actor `human|agent`.
Missing/invalid token → 401 before anything touches memory. See
[ADR-009](docs/adr/adr-009-remote-mcp-bearer-auth.md).

Tools: `memory_enrich`, `memory_search`, `memory_traverse`,
`memory_record_observation`, `memory_assert_fact`. Recall results are wrapped
in `<memory-context>` fences (data-not-instructions). The MCP connection
inherits ONE identity from env — whoever talks to the pipe IS that identity;
per-user identity arrives with OIDC (future).

## Layout

```
cmd/memserved   HTTP server          internal/memory   core domain logic
cmd/memseed     JSONL backfill       internal/entity   normalization + resolution
cmd/memeval     recall@k evals       internal/embed    LM Studio/OpenAI client
cmd/graphrebuild Dgraph rebuild      internal/extract  LLM fact proposals
cmd/memmcp      MCP stdio server     internal/graph    Dgraph projection + schema
internal/ch     connect + migrations internal/api      HTTP transport
internal/mcpserver  MCP tool layer   evals/*.yaml      eval definitions
```
