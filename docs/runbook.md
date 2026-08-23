# Runbook

Operational procedures for the SOC memory layer. Assumes the dev stack
(`task db-up`) or an equivalent production deployment of ClickHouse 26.3 +
Dgraph v25 + memserved.

## Start / stop

```bash
task db-up      # start stack; blocks until both stores report healthy
task run        # foreground service (SIGTERM → 30s graceful drain)
task db-down    # stop AND wipe volumes — destructive
```

Startup order matters only for the workers: they tolerate absent stores by
erroring per tick, but `db-up` health-gates make this moot.

## Backup / restore

**ClickHouse holds everything of record.** Dgraph is disposable (ADR-001).

```bash
# backup (native)
docker compose exec clickhouse clickhouse-client --user mem --password memdev \
  --query "BACKUP DATABASE mem TO Disk('backups', 'mem-$(date +%F)')"
```

Restore: reverse with `RESTORE`; then `go run ./cmd/graphrebuild` to rebuild
Dgraph from restored truth. Old backups re-materialize TTL-expired alerts;
the first merge after restore purges them again (TTL is eventual).

## Graph rebuild

Symptoms calling for it: traversal returns empty while enrich works; Dgraph
disk corruption; engine swap; schema drift after manual ALTERs.

```bash
go build -o graphrebuild ./cmd/graphrebuild && ./graphrebuild -batch 500
```

Safe to run live: it wipes only Dgraph data (schema kept), resets projection
watermarks to epoch, and replays idempotently. Interrupting mid-run is fine —
rerun converges.

## Extraction backlog

Check depth:

```sql
SELECT count() FROM mem.observations o
WHERE NOT EXISTS (SELECT 1 FROM mem.extract_log x WHERE x.obs_id = o.obs_id)
```

Legacy imports can queue hours ahead of fresh observations (worker drains
oldest-first). One-time catch-up: mark pre-import history covered directly,

```sql
INSERT INTO mem.extract_log (obs_id)
SELECT o.obs_id FROM mem.observations o
WHERE o.ts < '<import-cutoff>' AND o.kind = 'alert'
```

or raise `-batch`/interval temporarily. The worker itself needs no restart.

## Trust policy changes

Whitelist/floor are env-driven (`MEM_TRUST_WHITELIST`, `MEM_TRUST_FLOOR`);
changing them affects only NEW proposals — existing `proposed` facts wait for
human promote regardless.

## Troubleshooting

| Symptom | Check | Fix |
|---|---|---|
| `embedded:false` rows appearing | LM Studio up? `curl localhost:1234/v1/models` | restart model server; rows stay usable (text-only recall) |
| Similar returns few/no hits | scope correct? embedder up? | text-only degrade logs a warn per search |
| Traverse empty, enrich works | projection lag or Dgraph down | check watermark vs max(updated_at); `docker compose ps` |
| Cursor frozen (same page re-projected) | see ADR-007; fixed by toString ordering — verify no raw-UUID comparators crept back | run graphrebuild if divergence confirmed |
| 409 on promote | fact already active (version-at-read: consumed ids read 404) | use the id from the transition response |
| 429 bursts | per-agent budget (5 rps default) | expected; humans exempt |
| Enrich misses hyphenated domains | fixed Phase 3 (hyphen-preserving tokenization); confirm binary rebuilt | rebuild + re-ingest affected observations |

## Health endpoints & metrics

- `GET /healthz` — ClickHouse ping.
- Projection lag: `mem.projection_watermark` ts vs `max(updated_at)` per table.
- Extraction backlog: anti-join count query above.
- Mutations backlog (supersede storms): `system.mutations WHERE NOT is_done`.
- Dgraph: `curl localhost:18080/state` and alpha metrics endpoint.

## Data lifecycle quick reference

| Store | Retention |
|---|---|
| observations | alert kind: TTL 365d (eventual on merge); other kinds: indefinite |
| facts | indefinite; superseded versions collapse via ReplacingMergeTree, history lives in audit |
| audit | forever — evidence-adjacent, no TTL permitted |
| extract_log | indefinite (coverage marker; tiny) |
| Dgraph | disposable; rebuilt via graphrebuild |

## MCP remote deployment & tokens

HTTP mode requires a tokens file; startup fails closed without one.

Issue a token:
```bash
echo "{"token":"smem_$(openssl rand -hex 32)","actor_type":"human","actor_id":"analyst-x","scope":"team-a"}" >> /etc/socmem/tokens.json
# then restart memmcp (no hot reload yet)
```

Revoke: remove the record from tokens.json and restart. Audit who did what via
mem.audit (attribution rides the token's identity).

Deploy behind a TLS-terminating reverse proxy; add proxy-level rate limiting
on /mcp if internet-exposed. Bind to loopback on the app host and let the
proxy carry external traffic.
