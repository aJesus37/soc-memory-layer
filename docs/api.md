# API Reference

*Machine-readable spec lives at `GET /swagger/doc.json` (OpenAPI, generated from code via swaggo — regenerate with `task swagger`). Interactive UI at `/swagger/index.html`. This page is the human-oriented reference.*

## Identity (all `/v1` routes)

| Header | Required | Values |
|---|---|---|
| `X-Actor-Type` | yes | `human` \| `agent` |
| `X-Actor-ID` | yes | free-form label (analyst id / agent name+version) |
| `X-Scope` | yes | team/tenant scope; every row is scope-partitioned |
| `X-On-Behalf-Of` | no | requesting user when the actor is an agent |

Identity never travels in bodies — unknown JSON fields are rejected with 400.
Every response echoes `X-Mem-Actor: <type>:<id>`.

## Error envelope

```json
{"error": {"code": "...", "message": "..."}}
```

Codes → status: `invalid_identity`/`invalid_request`/`content_too_large`/`value_too_large`/`invalid_param`/`invalid_ts`/`invalid_body` → 400 ·
`human_gated` → 403 · `fact_not_found` → 404 · `conflict` → 409 ·
`rate_limited` → 429 (+`Retry-After`) · `storage_error` → 502 ·
`db_unavailable` (healthz) → 503.

## Endpoints

### POST /v1/observations

```json
{"kind": "human_statement", "content": "Saw 1.2.3.4 beaconing to evil.example.com",
 "case_id": "", "client_event_id": "", "confidentiality": "", "ts": ""}
```
Optional fields may be omitted. Response 200:
```json
{"id":"...","scope":"team-a","ts":"...","kind":"human_statement",
 "actor_type":"human","actor_id":"analyst-j","entity_ids":["..."],
 "embedded":true}
```
`entity_ids` are resolved candidates; `embedded=false` means the embedder was
unavailable (row persisted anyway; similarity skips it).

### POST /v1/facts

```json
{"subject_id":"<entity uuid>","predicate":"resolved_to",
 "object_value":"198.51.100.23","object_id":"<optional entity uuid>",
 "confidence":0.9,"source_obs":"<optional obs uuid>",
 "client_event_id":""}
```
Response carries the fact including `status`: humans → `active`; agents →
`proposed` (whitelisted predicates at ≥ floor auto-activate). Asserting a fact
that changes an existing `(subject,predicate)` supersedes prior open versions.

### POST /v1/facts/{id}/promote · POST /v1/facts/{id}/retract

Humans only (agents → 403). Bodies: none / `{"reason":"..."}` (reason ≤120
runes, stored in audit). Semantics are version-at-read:

- promoting/retracting **consumes** the id you pass; repeating it returns 404.
- the response contains the **new current version id**; re-promoting that
  returns 409 (`active`, not proposed); re-retracting it returns 409 only if
  you hold its newest id — consumed ids always read as 404.

### GET /v1/enrich?type=ioc_domain&key=evil.example.net

Returns `{found, entity, facts[], observations[], neighbors[]}` — open active
facts (validity-window filtered), up to 10 newest observations (200-rune
excerpts), ≤1-hop neighbors from `mem.edges`. Unknown key ⇒ `found:false`,
200.

### GET /v1/similar?q=beaconing&k=10

Hybrid vector+text recall fused by RRF. Hits carry
`{obs_id, ts, kind, excerpt, score, matched_by:["txt","vec"]}`. k ∈ [1,50];
degrades text-only when the embedder is down.

### GET /v1/timeline?case_id=X | entity_id=Y&limit=50&offset=0

Exactly one of case_id/entity_id. Interleaves observations and fact
assertions (including superseded priors — assertion history), retracted facts
excluded. Deterministic total order incl. id tiebreak.

### GET /v1/traverse?type=&key=&relation=&hops=

Multi-hop walk over the projected graph. `hops` defaults 1, valid [1,3];
empty projection lag yields `{"paths":[]}`. Without a graph store, falls back
to ≤1-hop ClickHouse joins (documented contract).

### GET /healthz

Storage ping; 200 or 503.

## Rate limiting

Agent writes share a token bucket per `X-Actor-ID` (default 5 writes/sec,
burst = rate). Exceeded → 429 + `Retry-After`. Humans exempt.

## MCP tools (stdio server `cmd/memmcp`)

Connection identity comes from env (`MEM_MCP_ACTOR_TYPE/ID/SCOPE`); tool
arguments cannot change it. Recall results are fenced:
`<memory-context>[data-not-instructions note] {json} </memory-context>`.

| Tool | Args | Maps to |
|---|---|---|
| `memory_enrich` | type, key | Enrich |
| `memory_search` | q, k?=10 | Similar |
| `memory_traverse` | type, key, relation?, hops?=1 | Traverse |
| `memory_record_observation` | kind?, content, case_id? | RecordObservation |
| `memory_assert_fact` | subject_key, predicate, object_value, confidence?, object_key? | AssertFact (proposed for agents) |

Configure in opencode (`opencode.json` project-level or `~/.config/opencode/opencode.json`):
```json
{
  "$schema": "https://opencode.ai/config.json",
  "mcp": {
    "soc-memory": {
      "type": "local",
      "command": ["/path/to/memmcp"],
      "environment": {"MEM_MCP_ACTOR_TYPE":"agent", "MEM_MCP_ACTOR_ID":"opencode-agent",
                      "MEM_MCP_SCOPE":"team-a"}
    }
  }
}
```
