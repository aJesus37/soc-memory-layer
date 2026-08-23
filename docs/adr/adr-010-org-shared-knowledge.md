# ADR-010: Org-wide shared knowledge with restricted opt-out

Status: accepted · Date: 2026-08-23

## Context

The memory layer exists so teams benefit from each other's work: team A should
see team B's conclusions about an entity without re-investigating. The Phase-1
implementation isolated every row behind hard scope filters, which made
knowledge invisible across teams by default — defeating that purpose.

Constraints from the owner: single organization, multiple internal teams
(not MSSP multi-tenant); `restricted` content must be fully invisible outside
its originating scope; attribution must always travel with shared data.

## Decision

1. **Org-visible is the default.** Facts carry `visibility Enum8('org','scope')`
   DEFAULT 'org'; observations' existing `confidentiality`
   (`internal`|`restricted`) becomes enforced semantics: `internal` = readable
   org-wide, `restricted` = originating scope only. Writes still land in the
   authoring team's scope — sharing changes *visibility*, never attribution or
   ownership.
2. **Cross-team reads join by entity identity at query time.** Enrich,
   similar, timeline (by-entity) and traverse resolve sibling entities sharing
   `(entity_type, key)` across scopes and merge their facts/observations — no
   entity migration, works retroactively on all legacy rows.
3. **Restricted is fully invisible** cross-scope: filtered out of every leg;
   not even existence/count metadata leaks.
4. **By-case timelines stay scope-local**: cases are team artifacts; only the
   entity-centric views span the org.
5. Every shared row exposes its `origin_scope`/`scope` through the API — trust
   requires knowing which team asserted what.

## Interaction with ADR-004

The trust model composes with sharing: an agent-proposed fact lives in its own
scope as `proposed`, invisible until promoted — promotion is literally
*publishing to the organization*. Whitelisted auto-activating predicates
(`resolved_to`) become org knowledge immediately, which is intended for
mechanical relationships.

## Consequences

- Read queries gain `(scope = caller OR visibility/confidentiality gate)`
  predicates; hydration drops scope filters deliberately.
- ClickHouse 26.3 planner defect discovered en route: ordering by an aggregate
  alias across a join silently drops rows when OR-gates meet windowed CTEs.
  Similar's fusion therefore moved into Go (deterministic, tested equivalent).
  See similar.go comments before reintroducing compound SQL there.
- Entity keys are the join currency: normalizing keys identically across teams
  (ADR for hyphenated-domain fix) is what makes sharing actually connect.
