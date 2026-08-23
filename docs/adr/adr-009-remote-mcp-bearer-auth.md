# ADR-009: Remote MCP via Streamable HTTP with bearer-token identity

Status: accepted · Date: 2026-08-23

## Context

stdio MCP servers are per-machine: every analyst needs Go installed and a
local binary. Teams asked for shared deployment — connect from any machine,
no build step — which requires the Streamable HTTP transport (mcp-go v0.58
`NewStreamableHTTPServer`, verified present) and, critically, real
authentication: over the network, self-declared env identity becomes an
unauthenticated API.

## Decision

`memmcp` gains a second transport mode:

- `MEM_MCP_HTTP_ADDR` set → serve `/mcp` over **Streamable HTTP**;
  unset → stdio as before.
- HTTP mode is **fail-closed on credentials**: `MEM_MCP_TOKENS_FILE` must
  exist, parse strictly (`smem_<64hex>` tokens, human|agent actor types,
  unique tokens) and contain ≥1 record, or startup aborts.
- Each request presents `Authorization: Bearer <token>`; the middleware maps
  it to `{actor_type, actor_id, scope}` in constant time over all records and
  rejects anonymous/invalid requests with 401 before any MCP handling.
- Identity flows through request context to tool handlers (per-request, never
  process-global) — two tokens yield two attributions, verified at the DB row
  level.

## Security posture (explicit)

- Tokens are static API-key-style credentials: no expiry, no revocation, no
  hot reload (edit file + restart). Rotation = replace record + restart.
- **TLS terminates at a reverse proxy.** The server binds plain HTTP by
  design; mcp-go's localhost DNS-rebinding guard is consciously disabled
  because bearer auth gates every request before MCP handling.
- No built-in throttling of auth attempts — put proxy-level rate limiting in
  front for internet-exposed deployments.
- Full OIDC remains the end-state; bearer tokens are the pragmatic interim
  that keeps the identity contract (ADR-005) intact.

## Consequences

- opencode/Claude-class clients configure `"type": "remote", "url": ...` with
  the token as header — no local binary.
- Attribution, scoping and trust policy continue to be enforced by the service
  layer exactly as for stdio; the transport changes nothing downstream.
