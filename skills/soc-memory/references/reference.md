# SOC Memory Reference

## Tool Shapes

### memory_enrich {type, key}
- `type`: ioc_ip | ioc_domain | ioc_hash | asset | technique | ...
- `key`: normalized value (e.g. `203.0.113.77`, `evil.example.com`, `T1071`)
- Returns: found, entity, facts[] (predicate/object_value/status/confidence/origin_scope), observations[] (excerpt/origin_scope), neighbors[]

### memory_search {q, k?=10}
- `q`: natural language query; `k` ∈ [1,50]
- Hybrid RRF: vector leg + text leg; degrades text-only if embedder down

### memory_traverse {type, key, relation?, hops?=1}
- Walks Dgraph edges up to 3 hops; relation filter optional; returns paths with nodes + relations

### memory_record_observation {kind?, content, case_id?}
- `kind` defaults by actor type (human → human_statement, agent → agent_action)
- `content` is the note text (≤10k chars); `case_id` links to a case

### memory_assert_fact {subject_key, predicate, object_value, confidence?=0.5, object_key?}
- subject_key/object_key are resolved via entity normalization (created on first sight)
- confidence clamped [0,1]; lands `proposed` for agents, `active` for humans or whitelisted predicates at ≥ floor

## Predicates (examples)

`verdict_malicious`, `resolved_to`, `beaconed_to`, `communicates_with`, `attributed_to`, `hosted_on`, `used_by`, `targeted`, `is_honeypot`

Any short snake_case string is valid; keep them consistent (`resolved_to` not `resolves_to`).

## Trust Model

- Humans assert → `active` (org-visible)
- Agents assert → `proposed` (visible only after human `POST /v1/facts/{id}/promote` → `active`)
- Whitelisted predicates (`MEM_TRUST_WHITELIST`, default `resolved_to`) auto-activate at confidence ≥ floor (`MEM_TRUST_FLOOR` default 0.8) — even from agents
- Retraction is human-only: `POST /v1/facts/{id}/retract`

## Scoping

- Scope comes from `X-Scope` (HTTP) or `MEM_MCP_SCOPE` (MCP) — never from payloads
- `internal` observations / `org` facts: org-visible; `restricted` / `scope` facts: originating team only
- Cross-team reads are org-wide; attribution (`origin_scope`) is always present
