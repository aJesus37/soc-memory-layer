# SOC Memory Layer — Design

*Date: 2026-08-22 · Status: approved · Scope: multi-user, multi-agent memory for security operations*

## 1. Context & goals

A SOC/CSIRT with 5–20 analysts and a fragmented tooling stack needs a shared memory layer that:

1. Captures **investigations** performed by humans *and* agents (notes, findings, triage decisions, hunt results)
2. Captures **alert triage** context and outcomes
3. Accumulates **company context** — facts learned during investigations or stated directly by humans ("this asset is a honeypot", "we don't run software X", "domain Y was concluded benign in case #123")
4. Serves future **triage agents** and **threat hunting agents** as both readers (enrichment, recall, pivots) and writers (observations, fact proposals)

Non-goals: replacing the SIEM/case management; storing raw telemetry at SIEM scale; real-time streaming analytics.

## 2. Decision record

**Chosen: ClickHouse = system of record; graph DB = rebuildable projection; one Memory Service API in front of both.**

| Option | Verdict | Reason |
|---|---|---|
| CH-only, graph-shaped schema | Rejected (for now) | Poor ergonomics for deep/ad-hoc traversal; user wants to enable unpredictable exploratory queries |
| CH + graph projection (**chosen**) | ✅ | Provenance/audit/rebuildability cheap; degrades gracefully both directions; fits platform capacity (2 stateful services) |
| Graph-centric temporal KG spine (Zep-style) | Rejected | Puts LLM-extraction errors into the source of truth; heaviest ops |

Key architectural rule: **the graph is never a second source of truth.** Edges live in ClickHouse (`mem.edges`); the graph holds a rebuildable copy. Engine swap or corruption ⇒ truncate + replay.

## 3. Components

```
                    ┌─────────────────────────┐
 humans & agents ──▶│   Memory Service (Go)   │◀── read queries
                    └───────────┬─────────────┘
              sync writes       │ async projection
                    ┌───────────▼─────────────┐
                    │      ClickHouse          │  ← source of truth
                    └───────────┬─────────────┘
                                │ rebuildable projection
                    ┌───────────▼─────────────┐
                    │    Graph DB (swappable)  │
                    └──────────────────────────┘
```

### Memory Service operations

| Operation | Used by | Behavior |
|---|---|---|
| `record_observation` | humans + agents | note/finding/alert → embed → append `observations` + entity refs |
| `assert_fact` | humans direct; agents → `proposed` | versioned fact into `facts` (supersedes prior) + edge row |
| `enrich(entity_key)` | triage/hunting agents | valid-now facts + neighbors + recent observations |
| `similar(text\|embedding, filters)` | all | hybrid vector+text recall (RRF) over observations |
| `traverse(start, pattern, hops)` | hunting agents | forwarded to graph engine; ≤2-hop fallback to CH joins on outage |
| `timeline(case_id\|entity)` | all | chronological reconstruction from CH |

Nobody talks to CH or the graph except this service.

### ClickHouse schema (source of truth)

```sql
-- canonical objects, deduped by (type, key)
CREATE TABLE mem.entities (
  entity_id   UUID,
  scope       LowCardinality(String),
  entity_type Enum8('ioc_ip','ioc_domain','ioc_hash','asset','user',
                    'actor','campaign','malware','technique','tool',
                    'case','document'),
  key         String,                -- normalized: '1.2.3.4', sha256, 'T1566'
  display_name String,
  attrs       Map(String, String),
  first_seen  DateTime,
  last_seen   DateTime,
  updated_at  DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at) ORDER BY (scope, entity_type, key);

-- append-only episodic layer
CREATE TABLE mem.observations (
  obs_id      UUID,
  scope       LowCardinality(String),
  ts          DateTime,
  kind        Enum8('alert','triage_decision','investigation_note',
                    'hunt_finding','agent_action','human_statement'),
  actor_type  Enum8('human','agent'),
  actor_id    String,               -- analyst id or agent name+version
  on_behalf_of String,              -- requesting user when actor is agent
  case_id     Nullable(UUID),
  confidentiality Enum8('internal','restricted'),
  content     String,
  content_vec Array(Float32),        -- empty array if embedding unavailable
  entity_refs Array(UUID)
) ENGINE = MergeTree ORDER BY (scope, ts);   -- TTL per scope policy

-- distilled semantic layer, version-at-read
CREATE TABLE mem.facts (
  fact_id     UUID,
  scope       LowCardinality(String),
  subject_id  UUID,
  predicate   LowCardinality(String), -- communicates_with | resolved_to |
                                      -- attributed_to | verdict_malicious |
                                      -- is_honeypot | uses_software | ...
  object_value String,
  object_id   Nullable(UUID),
  status      Enum8('proposed','active','retracted') DEFAULT 'active',
  confidence  Float32,
  source_obs  UUID,
  written_by  String,                 -- actor attribution
  valid_from  DateTime,
  valid_to    DateTime DEFAULT toDateTime64('9999-12-31', 0),
  updated_at  DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at) ORDER BY (scope, subject_id, predicate, object_value);

-- edge source-of-truth rows (graph projection replays from here)
CREATE TABLE mem.edges (
  edge_id    UUID,
  scope      LowCardinality(String),
  src_id     UUID,
  dst_id     UUID,
  relation   LowCardinality(String),
  from_fact  UUID,
  valid_from DateTime,
  valid_to   DateTime
) ENGINE = MergeTree ORDER BY src_id;

-- every write, attributed; NO TTL ever (evidence-adjacent)
CREATE TABLE mem.audit (
  ts         DateTime,
  actor_type Enum8('human','agent','system'),
  actor_id   String,
  operation  String,
  target_table String,
  target_id  UUID,
  payload_summary String
) ENGINE = MergeTree ORDER BY ts;
```

### Entity resolution

Every write normalizes keys before lookup: domains lowercased + registrable-domain extraction; IP parsing; hash typing by length; ATT&CK ID matching; assets against CMDB import. Hit ⇒ update `last_seen`; miss ⇒ create entity. This layer is what makes "things seen in investigations" linkable instead of free-text soup.

## 4. Data flows

**Write:** observation → normalize/embed → insert row → async batched LLM proposes candidate facts → approval via human confirm / N-source agreement / whitelisted auto-activation predicates (`resolved_to`, IOC extraction) → `facts` row + `edges` row → projection worker ships to graph.

**Triage read path (<500ms budget excluding LLM):** extract entities from alert → `enrich()` each (point lookups) → `similar()` hybrid recall over past triage decisions → assemble context → act → `record_observation` + fact proposals.

**Hunting read path:** interactive `traverse()` on graph; heavy aggregations stay on CH.

## 5. Trust model

**Humans assert; agents propose.**

- Agent writes enter as `status='proposed'` unless the predicate is on the auto-activation whitelist
- Promotion paths: human confirmation; independent N-source agreement; whitelist
- Every row carries `actor_type, actor_id, on_behalf_of`
- Scoping: team scope + confidentiality level on all rows; agents inherit requesting user's scope
- Per-agent write rate limits; misbehaving bots can't flood the fact base
- `mem.audit` has no TTL

## 6. Consistency & failure handling

| Failure | Behavior |
|---|---|
| Graph DB down | Reads degrade to CH joins (≤2-hop fallback inside `traverse()`); writes unaffected; alert don't block |
| Projection lag | Watermark worker batches every few seconds; lag metric exposed; rebuild = truncate + replay `mem.edges` |
| Embedding service down | Observations persist with empty vec; backfill re-embeds; similarity excludes unembedded |
| Bad LLM extraction | Facts are proposals until confirmed; retraction creates new version; nothing deleted |
| Duplicate entities | Audited merge tool: rewrite refs to canonical, tombstone duplicate |

Backups: native ClickHouse backups only. The graph is cattle.

## 7. Graph engine choice (deferred by design)

Swappable behind the service adapter; decision is a taste call because risk is contained by architecture:

- **Dgraph** — GraphQL surface is nice for agent-facing queries; actively maintained under Istari Digital since Oct 2025 (v25.3.x through May 2026, EE license removed); but two ownership changes in three years
- **Neo4j Community** — safest, Cypher ecosystem; single-node CE fine at this scale
- **FalkorDB/Memgraph** — lighter, smaller ecosystems

Lean: Dgraph acceptable *because* the architecture contains the risk; flip to Neo4j if the team knows Cypher.

## 8. Access surfaces

```
                    Memory Service (gRPC + REST API)
                   /          |            |
             Go SDK      MCP server     Web UI
          (agent code    (Claude Code,  (analysts;
           hot paths)     Desktop, any  later ticketing/
                          MCP client)   SIEM connectors)
```

**Scaffolded calls (hot path):** predictable operations — alert enrichment, timelines, bulk similarity — are deterministic SDK calls in agent code. No LLM discretion, no token cost, latency guaranteed.

**MCP tools (judgment path):** exposed for agentic/interactive use:

| Tool | Purpose |
|---|---|
| `memory_enrich(entity_key)` | ad-hoc "what do we know about X?" |
| `memory_search(query, filters)` | semantic recall of cases/notes |
| `memory_traverse(start, pattern)` | hunting pivots |
| `memory_record_observation(...)` | "remember this finding" — always allowed |
| `memory_assert_fact(...)` | conclusions → `proposed` per trust model |

One MCP server makes memory available to any MCP-capable client without per-agent integration work.

**Identity & fencing:** every surface resolves to an identity before hitting core ops — humans via OIDC/UI session, agent services via service credentials carrying `on_behalf_of`, MCP connections via per-user tokens. Scoping/attribution therefore works identically everywhere. Tool/memory results injected into LLM context are wrapped in fenced blocks (`<memory-context>` + system note marking them data-not-instructions) to blunt stored-content prompt injection.

## 9. Rollout & testing

1. **Seed phase:** backfill from existing fragmented sources (tickets, notes, past cases). First metric: entity-resolution hit rate.
2. **Memory eval harness:** 20–50 past investigations with known outcomes; retrieval questions ("had we seen this hash?", "what did we conclude about this domain?"); measure recall@k; rerun on every schema/extraction change.
3. **Shadow mode:** triage agent memory reads alongside human triage for weeks; compare surfaced vs used before any agent write rights beyond `proposed`.
4. **Progressive write rights:** hunting agents read day 1; write predicates expand config-driven, driven by audit review.

## 10. Open questions

- Final graph engine pick (deliberately deferred to implementation Phase 2)
- CMDB import format for asset entities
- Scope taxonomy: how many teams/confidentiality levels on day 1
- Retention policy per observation kind (TTL values)

## References

Design conversation grounded in: `~/Projects/embeddings-rag-clickhouse/README.md` (Parts 1–6, measured thresholds), Zep bi-temporal pattern, Mem0 consolidation ops, Anthropic memory tool fencing patterns, Hermes memory-manager lifecycle hooks.
