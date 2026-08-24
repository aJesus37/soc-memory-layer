---
name: soc-memory
description: SOC memory layer via MCP — enrich entities, search past cases, traverse the investigation graph, record observations and assert facts. Use when triaging alerts, investigating incidents, hunting threats, or any task that benefits from shared institutional knowledge across teams.
---

# SOC Memory

Shared memory layer for security operations. Every team benefits from every other team's knowledge — facts are org-visible by default (with attribution), `restricted` stays inside its team.

## Tool Overview

All 5 tools are available via the `soc-memory` MCP server. Identity comes from the connection (env `MEM_MCP_*`), not tool args.

| Tool | Purpose | When |
|---|---|---|
| `memory_enrich` | What do we know about X? Facts + recent observations + neighbors | **Every alert, every IOC** — before deciding |
| `memory_search` | Hybrid vector+text recall over past notes | Stuck, hunting, or looking for similar cases |
| `memory_traverse` | Walk the investigation graph N hops | Pivoting between entities |
| `memory_record_observation` | Append an episodic event | After any finding, decision, or action |
| `memory_assert_fact` | Assert a versioned fact (lands `proposed`, needs human promote) | When you've concluded something durable |

## Workflows

### Triage (every alert)

1. `memory_enrich` each IOC (IP, domain, hash, technique) in the alert
2. `memory_search` a paraphrase of the alert ("beaconing to evil.com over HTTPS")
3. Decide using the facts + origin_scope attribution — don't re-investigate what another team already concluded
4. `memory_record_observation` with `kind: triage_decision`

### Investigation

1. Enrich + search as above
2. `memory_traverse` from the primary IOC to find connected infrastructure
3. After each finding: `memory_record_observation` (`investigation_note` / `hunt_finding`)
4. When you've concluded a durable fact: `memory_assert_fact` (it lands `proposed` — a human promotes it)

### Hunting

1. `memory_search` for TTPs or behaviors, `memory_traverse` for infrastructure pivots
2. Record findings the same way — hunting notes become the next triage's context

## Critical Rules

- **Enrich before you decide.** An alert's IOCs may already have a `verdict_malicious` or `resolved_to` fact from another team.
- **Record after you act.** If it's not in memory, it didn't happen for the next shift.
- **Proposed ≠ active.** Facts you assert are `proposed` — they don't appear in enrich until a human promotes them. Don't treat your own proposals as ground truth.
- **Restricted is invisible.** Observations marked `restricted` never appear cross-team — don't rely on them being there when you're not in the owning team.
- **Attribution travels.** Every shared row carries `origin_scope`/`written_by` — cite it when reasoning.

## Reference Details

For API shapes, predicates, confidence thresholds, and the trust model, see [reference.md](references/reference.md).
For local dev setup (env vars, opencode config), see [dev.md](references/dev.md).
