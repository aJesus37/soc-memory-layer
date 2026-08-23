# soc-memory-layer

Shared memory layer for a security operations team: investigations (human +
agent), alert triage context, and company knowledge — stored in ClickHouse,
served over HTTP, consumable by analysts and agents alike.

**Status:** Phase 1 complete (memory service core on ClickHouse). Graph
projection, fact-extraction pipeline, MCP tools = later phases.

- Design: `docs/plans/2026-08-22-soc-memory-layer-design.md`
- Plan: `docs/plans/2026-08-22-phase1-memory-service.md`

## Quickstart

```bash
# 1. Dev database
make db-up                       # ClickHouse 26.3 on :9000 (user mem / memdev)

# 2. Tests (unit always; integration needs the DB)
make test                        # unit only
MEM_TEST_CH_ADDR=localhost:9000 go test ./... -count=1   # integration

# 3. Run the service (embedding server optional — degrades gracefully)
lms server start                 # LM Studio with nomic-embed-text-v1.5 loaded
make run                         # listens on :8080

# 4. Write + read
curl -s localhost:8080/v1/observations \
  -H 'Content-Type: application/json' \
  -H 'X-Actor-Type: human' -H 'X-Actor-ID: analyst-j' -H 'X-Scope: team-a' \
  -d '{"kind":"human_statement","content":"Saw 1.2.3.4 beaconing to evil.example.com"}'

curl -s 'localhost:8080/v1/enrich?type=ioc_ip&key=1.2.3.4' \
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
`conflict` · 429 rate-limited (agent writes, `Retry-After` header).
Unknown JSON fields are rejected — typos fail loudly.

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

## Layout

```
cmd/memserved   HTTP server          internal/memory   core domain logic
cmd/memseed     JSONL backfill       internal/entity   normalization + resolution
cmd/memeval     recall@k evals       internal/embed    LM Studio/OpenAI client
internal/ch     connect + migrations internal/api      HTTP transport
evals/*.yaml    eval definitions     seeds/*.jsonl     sample data
```
