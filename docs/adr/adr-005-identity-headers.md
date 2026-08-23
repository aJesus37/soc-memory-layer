# ADR-005: Identity via headers/env, never bodies or tool args

Status: accepted · Date: 2026-08-22 (extended 2026-08-23 for MCP)

## Context

Every write must be attributable (`actor_type`, `actor_id`, `on_behalf_of`)
and scoped. HTTP clients and MCP clients have different transport realities.

## Decision

- **HTTP:** identity arrives exclusively via `X-Actor-Type`, `X-Actor-ID`,
  `X-On-Behalf-Of`, `X-Scope` headers, injected into request context by
  middleware. Request bodies structurally cannot carry identity — unknown
  JSON fields are rejected, so a body-borne `scope` or `on_behalf_of` fails
  with 400 rather than being ignored.
- **MCP:** the connection inherits ONE identity from environment variables
  (`MEM_MCP_ACTOR_TYPE/ID/SCOPE`), validated fail-closed at startup. Tool
  arguments cannot override it. Whoever can speak to the stdio pipe *is* that
  identity.
- OIDC-backed per-user identity for remote transports is deferred; the header
  contract is designed so middleware is the only thing that changes.

## Consequences

- Attribution and scoping are enforced in exactly one place per transport;
  handlers merge context into service inputs and nothing else.
- Rate limiting keys on `X-Actor-ID` — rotation of that unauthenticated header
  can bypass budgets. Accepted for Phase 1/2 (internal deployment); OIDC
  closes it.
- MCP tool results embedding recalled memories are fenced
  (`<memory-context>` + data-not-instructions note) because recalled content
  is an injection vector; write results stay plain JSON.
