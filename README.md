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

## Prerequisites

- **Go** 1.22+ (`go version`)
- **Docker** + Compose (`docker compose version`) — for ClickHouse, Dgraph, TEI
- **[Task](https://taskfile.dev)** (`task --version` or `go install github.com/go-task/task/v3/cmd/task@latest`)
- `curl` + `jq` (optional, for the demo)

No API keys needed — the dev stack runs fully offline via TEI (`BAAI/bge-base-en-v1.5`) on `:3000`.

## Quickstart (30s)

```bash
git clone https://github.com/aJesus37/soc-memory-layer && cd soc-memory-layer

task db-up          # ClickHouse :9000/:8123 + Dgraph :9080 + TEI embedder :3000 (health-gated)
task run            # memserved on :8090 (leave running; open a second shell for the next steps)
```

Write + read:

```bash
curl -s localhost:8090/v1/observations \
  -H 'Content-Type: application/json' \
  -H 'X-Actor-Type: human' -H 'X-Actor-ID: analyst-j' -H 'X-Scope: team-a' \
  -d '{"kind":"human_statement","content":"Saw 1.2.3.4 beaconing to evil.example.com"}'

curl -s 'localhost:8090/v1/enrich?type=ioc_ip&key=1.2.3.4' \
  -H 'X-Actor-Type: agent' -H 'X-Actor-ID: triage-bot' -H 'X-Scope: team-a' | jq .

open http://localhost:8090/swagger/index.html   # interactive API docs
```

Cleanup: `task stop` (stop memserved) / `task db-down` (stop + wipe volumes).

## Demo — PHANTOM PORTAL (2 min, synthetic incident)

A multi-team story (phishing → C2 → loader → attribution) that exercises every feature — cross-team enrichment, `restricted` scoping, graph traversal, extraction, and the trust model. All data is synthetic (RFC 5737 IPs, invented domains). Full walkthrough: [`docs/demo.md`](docs/demo.md).

```bash
# 1. Start stores + seed the story (3 teams + facts + observations)
task db-up
task demo-seed       # seeds/phantom-portal/observations.jsonl + facts.jsonl

# 2. Ensure the service is running
task run             # :8090 — keep it running in this shell
```

In a second shell, try the five checks (copy-paste):

```bash
# 1 — Cross-team enrichment (team-a sees threatresp/hunt knowledge)
curl -s 'localhost:8090/v1/enrich?type=ioc_domain&key=secure-portal.invoice-update.com' \
  -H 'X-Actor-Type: human' -H 'X-Actor-ID: you' -H 'X-Scope: team-a' | jq .
# expect: found:true, facts with origin_scope=team-threatresp

# 2 — Restricted stays home (team-a vs originating scope)
curl -s 'localhost:8090/v1/similar?q=executive%20target%20variant&k=10' \
  -H 'X-Actor-Type: human' -H 'X-Actor-ID: you' -H 'X-Scope: team-a' | jq .
# expect: hits, but NO "VP Finance" restricted note
curl -s 'localhost:8090/v1/similar?q=executive%20target%20variant&k=10' \
  -H 'X-Actor-Type: human' -H 'X-Actor-ID: you' -H 'X-Scope: team-tier1' | jq .
# expect: SAME hits PLUS the restricted VP Finance observation (only team-tier1 sees it)

# 3 — Graph traversal (domain → C2 IP via threatresp edge, hops=2)
curl -s 'localhost:8090/v1/traverse?type=ioc_domain&key=secure-portal.invoice-update.com&hops=2' \
  -H 'X-Actor-Type: human' -H 'X-Actor-ID: you' -H 'X-Scope: team-a' | jq .

# 4 — Dreaming-lite extraction (requires an LLM — any OpenAI-compatible /v1)
task stop
MEM_EXTRACT_ENABLED=true MEM_EXTRACT_BASE_URL=https://api.openai.com/v1 \
  MEM_EXTRACT_MODEL=gpt-4o-mini MEM_EXTRACT_API_KEY=sk-... task run-extract
# then in another shell, post prose and wait ~30s:
curl -s -X POST localhost:8090/v1/observations \
  -H 'Content-Type: application/json' \
  -H 'X-Actor-Type: human' -H 'X-Actor-ID: analyst-x' -H 'X-Scope: team-hunt' \
  -d '{"kind":"hunt_finding","content":"confirmed 198.51.100.23 resolved_to fallback-c2.phantom-infra.net during rotation"}'
# check: curl enrich for fallback-c2.phantom-infra.net — fact proposed by extractor-v1 (auto-active because resolved_to is whitelisted)

# 5 — Trust model (agent propose → human promote)
#   open http://localhost:8090/swagger/index.html
#   POST /v1/facts with X-Actor-Type: agent → status proposed
#   POST /v1/facts/{id}/promote with X-Actor-Type: human → active + audited
```

Recall gate for the demo scenario:

```bash
task eval           # runs evals/demo.yaml — exits 1 on recall regression
```

## Testing (easy)

```bash
task test           # unit only — no DB, <5s
task itest          # full suite vs live ClickHouse + Dgraph (-p 1, auto-starts stores)
# or manually:
MEM_TEST_CH_ADDR=localhost:9000 MEM_TEST_DGRAPH_ADDR=localhost:9080 go test -count=1 -p 1 ./...

# single package / single test
go test ./internal/memory -run TestEnrich -count=1 -v
go test ./internal/api -run TestTimeline -count=1 -v

# lint / build / swagger
go vet ./...
task build
task swagger        # regenerate docs/swagger.{json,yaml} from handler annotations
task seed           # seeds/smoke.jsonl into a running service
task demo-seed      # full PHANTOM PORTAL dataset
task mcp            # build local stdio MCP binary ./memmcp
```

Tests need no secrets. Integration tests skip unless the env vars are set (so `go test ./...` never fails on a laptop without Docker). All packages share one ClickHouse — each test isolates via a unique `X-Scope` and never `TRUNCATE`s. See [`docs/development.md`](docs/development.md#testing-conventions) for the invariants.

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
`MEM_LISTEN_ADDR` (:8080) · `MEM_EMBED_URL` (http://localhost:3000) ·
`MEM_EMBED_MODEL` (nomic-ai/nomic-embed-text-v1.5) · `MEM_EMBED_API_KEY` (none) ·
`MEM_TRUST_FLOOR` (0.8) · `MEM_TRUST_WHITELIST` (resolved_to) · client timeout
for embeddings is 120s (model cold-start).

For extraction: `MEM_EXTRACT_ENABLED` (off) · `MEM_EXTRACT_MODEL`
(qwen/qwen3-8b) · `MEM_EXTRACT_BASE_URL` (empty → extraction disabled) ·
`MEM_EXTRACT_API_KEY` (none). Any OpenAI-compatible provider works — set
the base URL to `https://api.openai.com/v1` (plus an API key) or any other
`/v1` endpoint (Anthropic, Azure, Groq, Ollama, vLLM, …) and set the model
name accordingly.

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
cmd/memeval     recall@k evals       internal/embed    TEI/OpenAI-compatible embedding client
cmd/graphrebuild Dgraph rebuild      internal/extract  LLM fact proposals
cmd/memmcp      MCP stdio server     internal/graph    Dgraph projection + schema
internal/ch     connect + migrations internal/api      HTTP transport
internal/mcpserver  MCP tool layer   evals/*.yaml      eval definitions
```
