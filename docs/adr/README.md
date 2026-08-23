# Architecture Decision Records

Decisions are recorded when made and never rewritten — superseding an ADR adds
a new one that links back. Statuses: *accepted*, *superseded by ADR-N*.

| ADR | Title | Status |
|---|---|---|
| [001](adr-001-clickhouse-source-of-truth.md) | ClickHouse as sole source of truth; graph as rebuildable projection | accepted |
| [002](adr-002-dgraph-engine.md) | Dgraph v25 as the graph engine | accepted |
| [003](adr-003-facet-edge-modeling.md) | Facet-based edge modeling (one relation per ordered pair) | accepted |
| [004](adr-004-trust-model.md) | Humans assert, agents propose | accepted |
| [005](adr-005-identity-headers.md) | Identity via headers/env, not bodies or tool args | accepted |
| [006](adr-006-dreaming-lite-extraction.md) | Async batched fact extraction ("dreaming-lite") | accepted |
| [007](adr-007-keyset-cursor-textual-order.md) | Keyset cursor with textual UUID ordering; UUIDv7 identifiers | accepted |
| [008](adr-008-rrf-hybrid-recall.md) | Reciprocal Rank Fusion for hybrid recall; no BM25 dependency | accepted |
| [009](adr-009-remote-mcp-bearer-auth.md) | Remote MCP via Streamable HTTP with bearer-token identity | accepted |
