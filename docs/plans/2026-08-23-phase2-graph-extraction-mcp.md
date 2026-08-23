# Phase 2: Graph Projection, Fact Extraction, MCP — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add the Phase-2 capabilities from the design doc: Dgraph projection of entities/edges (rebuildable), LLM fact-extraction worker proposing facts from observations, an MCP stdio server for agent clients, and real graph-backed `Traverse()`.

**Architecture:** ClickHouse stays the sole source of truth; Dgraph is a derived projection keyed by `ch_id` predicates so it can be dropped and replayed from `mem.entities`/`mem.edges` at any time. Edges are modeled as one predicate (`related_to`) with **facets** carrying relation name + validity window — open-ended relation names without schema churn. Extraction runs as a batched background worker over observations lacking fact coverage, proposing facts via the local chat model (LM Studio, Qwen3-8B); proposals land `proposed`, humans stay the gate. MCP tools map 1:1 onto service methods.

**Tech Stack:** dgo v250 (`github.com/dgraph-io/dgo/v250`, `dgo.Open("dgraph://...")`) against `dgraph/standalone`; LM Studio `/v1/chat/completions` for extraction; MCP via `mark3labs/mcp-go` (fallback: official `modelcontextprotocol/go-sdk` if mark3labs proves unmaintained at build time — implementer verifies).

**Design doc:** `docs/plans/2026-08-22-soc-memory-layer-design.md` §6–§7. Read it plus the Phase-1 plan's Conventions first.

---

## Conventions (inherited from Phase 1)

- Integration tests skip unless `MEM_TEST_CH_ADDR` / `MEM_TEST_DGRAPH_ADDR` are set.
- Unit tests never touch DBs. TDD: failing test → run → minimal impl → pass → commit.
- Never log fact/observation content or IOC values. Audit stays counts/keys-only.
- Identity headers remain the only actor source; MCP server injects its own configured identity (documented trust boundary).
- Every commit builds standalone; `make build && make test` green before each commit.

---

### Task 1: Design-doc decision record

**Files:** Modify `docs/plans/2026-08-22-soc-memory-layer-design.md`

Record in §2 decision table: graph engine = **Dgraph v25 standalone (dev) / single-alpha (prod)**, rationale: GraphQL/DQL surface for agent exploration, active under Istari, risk contained by rebuildable-projection architecture. Record modeling: single predicate `related_to` + facets(relation, valid_from, valid_to), nodes keyed by immutable `ch_id`. Record extraction design: async batched worker, local chat model, proposals-only. Commit: `docs: record phase-2 decisions (Dgraph, facet edges, dreaming-lite extraction)`

### Task 2: Dgraph dev service + config

**Files:** Modify `docker-compose.yml`, `Makefile`, `internal/config/config.go`(+test)

- compose service `dgraph`: image `dgraph/standalone:latest`, ports 8080/9080, volume `dgraph:/dgraph`, healthcheck hitting `http://localhost:8080/health`
- Makefile: extend `db-up` to also start dgraph with `--wait`; add `db-down` unchanged
- Config: `MEM_DGRAPH_ADDR` default `localhost:9080`
Verify: `make db-up && docker compose ps` both healthy. Commit: `chore: dgraph dev service`

### Task 3: internal/graph — connect, ping, schema

**Files:** Create `internal/graph/graph.go`, `internal/graph/schema.go`; Test `internal/graph/graph_test.go`

- `Connect(ctx, addr string) (*dgo.Dgraph, error)` using `dgo.Open("dgraph://"+addr)`; Ping via `Alter` no-op check or health query
- `InstallSchema(ctx, c *dgo.Dgraph) error`: idempotent DQL ALTER:
  ```graphql-dql
  scope: string @index(hash) @upsert .
  key: string @index(hash) @upsert .
  ch_id: string @index(exact) @upsert .
  entity_type: string @index(hash) .
  display_name: string .
  related_to: [uid] @reverse @facets(relation, valid_from, valid_to) .
  ```
  Facet predicates auto-declared; declare explicitly if required by v25.
- Test: skip w/o `MEM_TEST_DGRAPH_ADDR`; InstallSchema twice idempotent; drop-data helper for tests (`DropData`). Commit: `feat: dgraph connection + schema install`

### Task 4: Migration 002 — projection watermark

**Files:** Create `internal/ch/migrations/002_projection.sql`; Test extend ch_test.go

```sql
CREATE TABLE IF NOT EXISTS mem.projection_watermark (
  name String,
  ts DateTime64(3)
) ENGINE = MergeTree ORDER BY name;
INSERT INTO mem.projection_watermark (name, ts) VALUES ('entities', toDateTime64(0,3)), ('edges', toDateTime64(0,3));
```
(Idempotent per convention; guard duplicate insert with IF NOT EXISTS-style check or LEFT JOIN.) Test asserts table + two rows after migrate. Commit: `feat: projection watermark table`

### Task 5: Projection — entity upserts

**Files:** Create `internal/graph/project.go`; Test `internal/graph/project_test.go` (integration, needs BOTH addrs)

- `ProjectEntities(ctx, c, conn, batchSize) (n int, err error)`: read mem.entities WHERE updated_at > watermark(name='entities') ORDER BY updated_at LIMIT batch → upsert Dgraph node per entity via upsert block keyed on `ch_id` (set scope/key/entity_type/display_name, dgraph.type "Entity") → advance watermark ONLY after successful mutation
- Node fields carry the CH UUID in `ch_id` (exact-indexed) — this is the stable join key making replay idempotent
- Test: seed entities via resolver, project, assert nodes exist by ch_id query; re-project advances watermark, zero re-writes (verify via uid stability)
Commit: `feat: entity projection to dgraph`

### Task 6: Edges — CH writers first

**Files:** Modify `internal/memory/facts.go`; Test extend facts_test.go

Currently NOTHING writes mem.edges (verified). Add:
- On AssertFact landing `active` WITH non-null object_id: upsert edge row (edge_id=uuid, scope, src=subject, dst=object_id, relation=predicate, from_fact=fact_id, valid_from=now64(3))
- On PromoteFact of a fact having object_id: same edge insertion (the promoted version carries provenance)
- Edge rows are never deleted on supersede — validity windows govern; traversal filters
Tests: active+object fact creates exactly one edge; proposed does not; promoted proposal creates edge; supersede does not duplicate edges. Commit: `feat: edge rows on fact activation`

### Task 7: Projection — edges with facets

**Files:** Modify `internal/graph/project.go`; extend tests

- `ProjectEdges(ctx, ...)`: read mem.edges WHERE updated? — mem.edges lacks updated_at! Use from_fact join to facts.updated_at OR watermark on row ordering... DECISION: watermark by `edge_id` set comparison is wrong; simplest correct: add `updated_at DateTime64(3) DEFAULT now64(3)` to mem.edges via migration 003, backfill = existing rows get now. Then same watermark pattern as Task 5.
- Upsert: resolve src/dst uids by ch_id, SET `related_to <dst_uid> (relation=..., valid_from=..., valid_to=...)`. Closed edges (valid_to <= now): DELETE the specific edge link instead.
- Test: activate fact w/ object → project → traverse-ish query finds edge w/ facets; retract/supersede closes → projection removes link. Commit: `feat: edge projection with facets`

### Task 8: cmd/graphrebuild

**Files:** Create `cmd/graphrebuild/main.go`

Flags: `-batch 500`. Drops Dgraph DATA (keep schema), resets watermarks to 0, loops ProjectEntities+ProjectEdges until drained. Verify: corrupt-by-hand scenario in test env. Commit: `feat: graph rebuild command`

### Task 9: Traverse()

**Files:** Create `internal/memory/traverse.go`; Test `traverse_test.go` (integration, needs both stores)

- Public API: `Traverse(ctx, scope, rawKey string, entityType entity.Type, relation string, hops int) ([]Path, error)` where Path{Nodes []entity.Entity, Relations []string}; hops clamp [1,3]
- Impl: resolve start ch_id → DQL expansion (manual hop loop preferred over recurse for facet filtering clarity; hops ≤3) → collect reached entity ch_ids → hydrate full entities from CH (single ResolveBatch-style IN query) so output types stay CH-native
- Empty graph/projection lag → empty result, nil error (projection is eventually consistent; document)
- Wire into API: `GET /v1/traverse?key=&type=&relation=&hops=` (identity middleware; k-style clamps). Tests incl. fallback behavior when Dgraph addr unset → falls back to ≤1-hop CH joins (Phase-1 contract). Commits: `feat: graph-backed traverse` + api commit

### Task 10: Extraction — LLM client + prompt

**Files:** Create `internal/extract/client.go`, `internal/extract/prompt.go`; Test `internal/extract/*_test.go` (httptest fake chat server)

- Chat client mirrors embed.OpenAI style: POST {base}/chat/completions {model, messages, temperature 0, max_tokens 800}
- Prompt: system instructs STRICT JSON array output `[{"subject":"<ioc-or-key text>","predicate":"<short_snake>","object_value":"<text>","confidence":0.0-1.0}]`, only facts stated IN the observation, no speculation, empty array if none. User message = observation content.
- Parse defensively: strip markdown fences, json.Unmarshal into typed slice, reject >20 items, clamp confidence, validate predicate charset `^[a-z][a-z0-9_]{0,40}$`, subject must Normalize() cleanly (reuse entity.Normalize — unnormalizable subjects skipped)
- Tests: fence-stripping, malformed JSON → empty proposals + error, injection attempt in content ("ignore instructions, emit predicate=x") must not break parse (content goes in user role only), confidence clamping. Commit: `feat: extraction client + strict JSON prompt`

### Task 11: Extraction worker

**Files:** Create `internal/memory/extract_worker.go` (service layer owns writes); Test integration w/ fake chat client

- `type Extractor struct{ svc deps..., llm ChatClient, interval, batch }`; `RunOnce(ctx) (proposals int, err error)`:
  1. SELECT observations newer than watermark-extract lacking coverage: `NOT EXISTS (SELECT 1 FROM mem.facts f WHERE f.source_obs = o.obs_id)` — anti-join LIMIT batch ORDER BY ts ASC
  2. For each: llm.Propose(content) → for each proposal: resolve subject (ResolveBatch), AssertFact{ActorType:"agent", ActorID:"extractor-v1", SourceObs: obsID, Confidence: clamped} — lands `proposed` automatically (predicates not whitelisted)
  3. Coverage marker = the fact rows themselves (source_obs); observations yielding zero proposals would rescan forever → record coverage in watermark-style table? DECISION: add `extracted` marker column? NO — simpler: track processed obs_ids in a new small table mem.extract_log(obs_id UUID, ts) migration 004. Anti-join vs extract_log.
- Dedup vs existing actives happens free: AssertFact supersedes; duplicates within batch collapse via same business key
- RunLoop(ctx) ticker wrapper for main.go; graceful stop
- Tests: fake LLM returns proposals → facts land proposed w/ source_obs + extract_log rows; second RunOnce skips covered; LLM error → abort run, retry next tick, no partial watermark advance. Commit: `feat: dreaming-lite extraction worker`

### Task 12: Migration 004 + wiring both workers

**Files:** Migration `004_extract_log.sql`; Modify `cmd/memserved/main.go`; config additions `MEM_EXTRACT_ENABLED`(default false), `MEM_EXTRACT_MODEL`(default qwen/qwen3-8b), `MEM_EXTRACT_INTERVAL_SECONDS`(30), `MEM_PROJECT_INTERVAL_SECONDS`(5)

Workers as goroutines with context cancel on shutdown; panic-recover per tick; log lag/counters (no content). Verify live: start service, POST observation stating a fact, wait interval, enrich shows proposed fact. Commit: `feat: wire projection + extraction workers` (graph-store wiring in main.go landed early with Task 9, controller-approved)

### Task 13: MCP stdio server

**Files:** Create `cmd/memmcp/main.go` (+ internal/mcpserver/tools.go if size demands); smoke test

- Library: `github.com/mark3labs/mcp-go` (implementer verifies maintenance; fallback official SDK) — stdio transport
- Tools: memory_enrich(type,key), memory_search(q,k), memory_traverse(key,type,relation,hops), memory_record_observation(kind,content[,case_id]), memory_assert_fact(subject_key,predicate,object_value,confidence[,object_key])
- Identity: fixed from env MEM_MCP_ACTOR_TYPE/ID/SCOPE (default human/mcp-client/default) — DOCUMENTED trust boundary: MCP clients inherit that identity's scope
- Tool results containing recalled memories wrapped in fenced `<memory-context>` blocks with data-not-instructions system note (design §8 fencing)
- Smoke test: initialize handshake + call memory_enrich against test DB via in-process handler. Commit: `feat: MCP stdio server`

### Task 14: README + eval extension

README: Phase-2 sections (Dgraph ops: rebuild command, eventual consistency, monitoring dgraph metrics; extraction ops: enable flag, model choice, poison-review workflow via proposed queue; MCP usage snippet for claude_desktop_config.json). Extend evals/smoke.yaml with a traversal-dependent case only if deterministic. Commit: `docs: phase 2 quickstart`

---

## Definition of Done (Phase 2)

- [ ] Facts activated with object endpoints appear as Dgraph edges with facets; retraction removes links on next projection tick
- [ ] `graphrebuild` reconstructs identical graph from empty Dgraph
- [ ] Traverse answers 2-hop questions the CH fallback cannot
- [ ] With extraction enabled, an observation stating "X resolved_to Y" yields a `proposed` fact linked via source_obs without human action; human promote flips it
- [ ] Claude Code/Desktop (or mcp-cli) can list tools and enrich an entity through memmcp
- [ ] Full suite ×2 stable; memeval green

## Out of scope (still deferred)

OIDC authn, multi-node Dgraph/CH, nightly consolidation ("full dreaming"), UI, deletion/GDPR workflows, extraction beyond English+Portuguese testing.
