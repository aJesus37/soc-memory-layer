# ADR-004: Humans assert, agents propose

Status: accepted · Date: 2026-08-22

## Context

Both human analysts and LLM agents write to shared memory. LLM extraction
makes errors — plausible-but-wrong facts, stale conclusions, prompt-injection
carried content. A wrong fact that enters the record of truth silently is the
single worst failure mode for an investigation memory (Part-6 research:
staleness and silent corruption outrank every other failure).

## Decision

Fact writes carry a status decided by `ApplyTrust`:

- **Human actors → `active`** unconditionally.
- **Agent actors → `proposed`** unless the predicate is on the operator
  whitelist (`MEM_TRUST_WHITELIST`, default: `resolved_to`) AND confidence ≥
  floor (`MEM_TRUST_FLOOR`, default 0.8; clamped to [0,1], NaN → 0).
- Unknown actor types fail closed to `proposed`.
- Only humans may promote (`proposed` → `active`) or retract. Agents receive
  403 at the API and `ErrHumanGated` at service level.
- Retraction creates a born-closed version; nothing is ever deleted.
- Proposals never close prior active facts (supersede waves run only when the
  new version lands `active`).

## Consequences

- Whitelisted predicates auto-activate from agent extraction at high
  confidence — deliberate ("203.0.113.7 resolved_to X" is mechanical), while
  judgment predicates (`attributed_to`) always wait for a human.
- The audit trail records status transitions with actor attribution, enabling
  post-hoc review of what automation changed.
- Cost: humans are the throughput bottleneck for judgment facts. Accepted;
  promotion-by-agreement (N independent agents) is a future option.
