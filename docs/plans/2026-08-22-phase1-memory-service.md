# Phase 1: Memory Service Core — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build the SOC Memory Service core on ClickHouse: schema, entity resolution, observation/fact write paths with trust rules, hybrid recall, enrichment, timeline, REST API, seed + eval CLIs. (Graph projection, MCP server, Web UI = later phases.)

**Architecture:** Single Go service owning all reads/writes; ClickHouse is the only store in Phase 1. Identity via `X-Actor-*` headers. Facts version-at-read (`ReplacingMergeTree`); agents write `proposed`, humans `active`. Hybrid recall = vector leg (brute-force cosine) fused with text leg (text index) by Reciprocal Rank Fusion. Embeddings from any OpenAI-compatible endpoint (LM Studio locally).

**Tech Stack:** Go 1.22+ (stdlib `net/http` mux), `clickhouse-go/v2`, `golang.org/x/net/publicsuffix`, docker compose for the ClickHouse server, stdlib testing.

**Design doc:** `docs/plans/2026-08-22-soc-memory-layer-design.md` — read §3 (schema), §5 (trust model), §8 (surfaces) before starting.

---

## Conventions for every task

- Integration tests need a real ClickHouse: `make db-up` starts one; tests skip unless `MEM_TEST_CH_ADDR` is set. Unit tests never touch the DB.
- Run integration suite: `make test` (unit) / `make itest` (all).
- Commit after every green step. Messages follow `feat:`/`test:`/`chore:` style.
- All timestamps UTC. All IDs UUIDv4 (`github.com/google/uuid`).
- Never log fact/observation content — audit payloads are summaries only.

---

### Task 1: Repo scaffold + dev database

**Files:**
- Create: `go.mod`, `.gitignore`, `Makefile`, `docker-compose.yml`
- Create: `cmd/memserved/main.go` (placeholder), `internal/config/config.go`

**Step 1: Scaffold**

```bash
cd ~/Projects/soc-memory-layer
go mod init socmem
mkdir -p cmd/memserved cmd/memseed cmd/memeval \
         internal/{config,ch,entity,memory,embed,api} \
         internal/ch/migrations internal/api/static \
         evals seeds
```

**Step 2: `.gitignore`**

```gitignore
/memserved
/memseed
/memeval
*.test
.env
```

**Step 3: `docker-compose.yml`** (dev/test ClickHouse)

```yaml
services:
  clickhouse:
    image: clickhouse/clickhouse-server:26.3
    ports: ["9000:9000", "8123:8123"]
    environment:
      CLICKHOUSE_USER: mem
      CLICKHOUSE_PASSWORD: memdev
      CLICKHOUSE_DB: mem
    volumes:
      - chdata:/var/lib/clickhouse
volumes:
  chdata:
```

**Step 4: `Makefile`**

```makefile
.PHONY: build test itest db-up db-down run
build:
	go build ./...
test:
	go test ./...
itest: db-up
	MEM_TEST_CH_ADDR=localhost:9000 go test -count=1 ./...
db-up:
	docker compose up -d clickhouse
db-down:
	docker compose down -v
run:
	go run ./cmd/memserved
```

**Step 5: Placeholder main** — `cmd/memserved/main.go`

```go
package main

func main() {}
```

**Step 6: Verify + commit**

Run: `make build && make db-up && docker compose ps`
Expected: build clean; container Up.

```bash
git add -A && git commit -m "chore: scaffold Go service + compose dev DB"
```

---

### Task 2: Config package

**Files:**
- Create: `internal/config/config.go`
- Test: `internal/config/config_test.go`

**Step 1: Failing test**

```go
package config

import "testing"

func TestLoadDefaults(t *testing.T) {
	t.Setenv("MEM_CH_ADDR", "")
	t.Setenv("MEM_EMBED_URL", "")
	c := Load()
	if c.ChAddr != "localhost:9000" || c.ListenAddr != ":8080" {
		t.Fatalf("defaults wrong: %+v", c)
	}
	if c.EmbedURL != "http://localhost:1234/v1" || c.EmbedModel == "" {
		t.Fatalf("embed defaults wrong: %+v", c)
	}
}

func TestLoadOverride(t *testing.T) {
	t.Setenv("MEM_CH_ADDR", "db:9000")
	c := Load()
	if c.ChAddr != "db:9000" {
		t.Fatalf("override ignored: %+v", c)
	}
}
```

**Step 2: Run to verify failure**

Run: `go test ./internal/config/`
Expected: FAIL (undefined: Load)

**Step 3: Implement** — `internal/config/config.go`

```go
package config

import "os"

type Config struct {
	ChAddr     string
	ChUser      string
	ChPassword  string
	ListenAddr  string
	EmbedURL    string
	EmbedModel  string
	AgentRateRPS float64 // per-agent writes/sec
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func Load() Config {
	return Config{
		ChAddr:       envOr("MEM_CH_ADDR", "localhost:9000"),
		ChUser:       envOr("MEM_CH_USER", "mem"),
		ChPassword:   envOr("MEM_CH_PASSWORD", "memdev"),
		ListenAddr:   envOr("MEM_LISTEN_ADDR", ":8080"),
		EmbedURL:     envOr("MEM_EMBED_URL", "http://localhost:1234/v1"),
		EmbedModel:   envOr("MEM_EMBED_MODEL", "text-embedding-nomic-embed-text-v1.5"),
		AgentRateRPS: 5,
	}
}
```

**Step 4: Pass, then commit**

Run: `go test ./internal/config/`
Expected: PASS

```bash
git add -A && git commit -m "feat: env-based config"
```

---

### Task 3: ClickHouse connection + migration runner

**Files:**
- Create: `internal/ch/ch.go`
- Test: `internal/ch/ch_test.go`

**Step 1: Failing test** (integration)

```go
package ch

import (
	"context"
	"testing"

	"socmem/internal/config"
)

func TestMigrateAppliesOnce(t *testing.T) {
	addr := os.Getenv("MEM_TEST_CH_ADDR")
	if addr == "" {
		t.Skip("set MEM_TEST_CH_ADDR to run")
	}
	ctx := context.Background()
	conn := Connect(ctx, t, addr, config.Load())
	if err := Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, conn); err != nil { // idempotent
		t.Fatal(err)
	}
	var n int
	if err := conn.QueryRow(ctx,
		"SELECT count() FROM schema_migrations WHERE database='mem'").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n < 1 {
		t.Fatalf("no migrations recorded")
	}
}
```

(Add `"os"` import when writing.)

**Step 2: Run → FAIL (undefined)**

**Step 3: Implement** — `internal/ch/ch.go`: `Connect()` opens `clickhouse-go/v2` conn (Auth user/pass from config, database `default` during migrate); `Migrate()` creates database if missing, then applies embedded `migrations/*.sql` in filename order, recording each in `<db>.schema_migrations(name, applied_at)` and skipping already-applied ones.

Test helper `Connect(ctx, t, addr, cfg)` builds options with test creds (`mem`/`memdev`) and pings.

**Step 4: Pass via `make itest`, commit**

```bash
git add -A && git commit -m "feat: CH connect + idempotent migration runner"
```

---

### Task 4: Migration 001 — five tables

**Files:**
- Create: `internal/ch/migrations/001_init.sql`
- Test: extend `internal/ch/ch_test.go` (verify tables exist after migrate)

**Step 1: Failing assertion**

```go
func TestSchemaTables(t *testing.T) { /* after Migrate: SHOW TABLES FROM mem must contain
entities, observations, facts, edges, audit */ }
```

**Step 2: DDL** — port tables verbatim from design doc §3:

- `mem.entities` ReplacingMergeTree(updated_at) ORDER BY (scope, entity_type, key)
- `mem.observations` MergeTree ORDER BY (scope, ts) + `INDEX ft_idx content TYPE text(tokenizer=splitByNonAlpha)` + `TTL ts + INTERVAL 365 DAY` for kind='alert'
- `mem.facts` ReplacingMergeTree(updated_at) ORDER BY (scope, subject_id, predicate, object_value)
- `mem.edges` MergeTree ORDER BY src_id
- `mem.audit` MergeTree ORDER BY ts (NO TTL)

Enum values exactly as design doc. `valid_to DateTime DEFAULT toDateTime64('9999-12-31 00:00:00',0)`.

**Step 3: `make itest` PASS, commit**

```bash
git add -A && git commit -m "feat: initial schema (entities/observations/facts/edges/audit)"
```

---

### Task 5: Entity normalizers (pure TDD)

**Files:**
- Create: `internal/entity/entity.go`
- Test: `internal/entity/entity_test.go`

**Step 1: Failing table-driven test**

```go
func TestNormalize(t *testing.T) {
	cases := []struct{ in, typ, key string; ok bool }{
		{"Example.COM", "ioc_domain", "example.com", true},
		{"  example.com. ", "ioc_domain", "example.com", true},
		{"1.2.3.4", "ioc_ip", "1.2.3.4", true},
		{"::1", "ioc_ip", "::1", true},
		{"999.1.1.1", "", "", false},
		{"D41D8CD98F00B204E9800998ECF8427E", "ioc_hash", "d41d8cd98f00b204e9800998ecf8427e", true},
		{"notahash", "", "", false},
		{"T1566", "technique", "T1566", true},
		{"t1566.002", "technique", "T1566.002", true},
	}
	for _, c := range cases {
		got, err := Normalize(c.in)
		if c.ok && (err != nil || got.Type != Type(c.typ) || got.Key != c.key) {
			t.Errorf("%q → %+v err=%v want %s/%s", c.in, got, err, c.typ, c.key)
		}
		if !c.ok && err == nil {
			t.Errorf("%q should not normalize", c.in)
		}
	}
}
```

**Step 2: Implement**

`Normalize(raw)` tries in order: technique regex (`^T\d{4}(\.\d{3})?$`, uppercase), hash (hex, len 32→md5/40→sha1/64→sha256/128→sha512, lowercase), IP (`net.ParseIP` + `.String()`), domain (lowercase, trim trailing dot, must contain one dot and valid label chars). Returns `(Normalized{Type,Key}, error)`.

**Step 3: PASS (`go test ./internal/entity/`), commit**

```bash
git add -A && git commit -m "feat: entity normalizers (domain/ip/hash/technique)"
```

---

### Task 6: EntityResolver — lookup or create

**Files:**
- Create: `internal/entity/resolver.go`
- Test: `internal/entity/resolver_test.go` (integration)

**Step 1: Failing test**

```go
func TestResolveOrCreate(t *testing.T) {
	itest(t) // skips w/o MEM_TEST_CH_ADDR; truncates mem.entities
	r := NewResolver(conn(t))
	e1, created, _ := r.Resolve(ctx(), "default", "Example.COM")
	if !created || e1.Key != "example.com" || e1.EntityType != IocDomain {
		t.Fatalf("first resolve wrong: %+v created=%v", e1, created)
	}
	e2, created2, _ := r.Resolve(ctx(), "default", "example.com")
	if created2 || e1.EntityID != e2.EntityID {
		t.Fatalf("dedup failed: %+v vs %+v", e1, e2)
	}
}
```

**Step 2: Implement**

`Resolve(scope, raw)` → Normalize → SELECT by (scope,type,key); miss ⇒ INSERT UUIDv4 row (`display_name=raw`, timestamps now) and return it. Hit ⇒ UPDATE `last_seen=now()` (via lightweight ALTER... no — keep simple: ReplacingMergeTree re-insert same id with new last_seen) and return existing id.

**Step 3: `make itest` PASS, commit**

```bash
git add -A && git commit -m "feat: entity resolution with dedup by (scope,type,key)"
```

---

### Task 7: Embedder — interface, LM Studio client, fake

**Files:**
- Create: `internal/embed/embed.go`, `internal/embed/openai.go`
- Test: `internal/embed/embed_test.go` (unit, uses httptest server)

**Step 1: Interface first**

```go
type Embedder interface {
	Embed(ctx context.Context, kind string, texts []string) ([][]float32, error)
}
// kind ∈ {"document","query"} — nomic task prefixes live inside the impl.
```

**Step 2: Fake for tests**

```go
func NewFake(dim int) *Fake // deterministic vectors: hash(text) seeded, normalized
```

**Step 3: OpenAI-compatible client** — POST `{EmbedURL}/embeddings` with model + prefixed inputs (`search_document: `/`search_query: `), parse `data[].embedding` sorted by index; 30s timeout; returns typed error on non-200.

**Step 4: Unit test against `httptest.Server` asserting prefix + ordering; PASS; commit**

```bash
git add -A && git commit -m "feat: embedder interface + OpenAI-compatible client"
```

---

### Task 8: RecordObservation (+ audit)

**Files:**
- Create: `internal/memory/service.go`
- Test: `internal/memory/service_test.go` (integration)

**Step 1: Service skeleton + failing test**

```go
type Service struct {
	conn     driver.Conn
	resolver *entity.Resolver
	embedder embed.Embedder
	cfg      config.Config
}

func TestRecordObservation(t *testing.T) {
	itest(t); s := testService(t) // fake embedder dim=8
	o, err := s.RecordObservation(ctx(), Input{
		Scope: "default", Kind: "human_statement",
		ActorType: "human", ActorID: "analyst-j",
		Content:   "Saw 1.2.3.4 beaconing to example.com",
	})
	if err != nil { t.Fatal(err) }
	// assert: obs row exists; vec length == 8; two entities resolved+linked in entity_refs;
	// audit row written with operation='record_observation', payload summary has NO content text
}
```

**Step 2: Implement**

Flow: extract entities (run `Normalize` over whitespace/URL-tokenized candidates) → resolve each → embed content (`kind=document`) → INSERT observation (vec empty array if embedder errors — log, don't fail) → INSERT audit (summary = kind+counts only).

Entity candidate extraction: split on non `[A-Za-z0-9.:]`, keep tokens that normalize cleanly; cap 32 per observation.

**Step 3: `make itest` PASS, commit**

```bash
git add -A && git commit -m "feat: RecordObservation with entity linking, embedding fallback, audit"
```

---

### Task 9: Trust rules (pure TDD)

**Files:**
- Create: `internal/memory/trust.go`
- Test: `internal/memory/trust_test.go`

**Step 1: Failing test**

```go
func TestApplyTrust(t *testing.T) {
	wl := map[string]bool{"resolved_to": true, "ioc_extraction": true}
	cases := []struct {
		actorType, predicate string
		conf                 float32
		want                 Status
	}{
		{"human", "attributed_to", 0.1, Active},
		{"agent", "attributed_to", 0.99, Proposed},
		{"agent", "resolved_to", 0.9, Active},
		{"agent", "resolved_to", 0.5, Proposed}, // below confidence floor
	}
	for i, c := range cases {
		if got := ApplyTrust(c.actorType, c.predicate, c.conf, wl); got != c.want {
			t.Errorf("case %d: got %v want %v", i, got, c.want)
		}
	}
}
```

**Step 2: Implement exactly as the table says** (human→Active; agent+whitelisted+conf≥0.8→Active; else Proposed). Confidence floor and whitelist come from config (`MEM_TRUST_FLOOR`, `MEM_TRUST_WHITELIST` comma-separated).

**Step 3: PASS, commit**

```bash
git add -A && git commit -m "feat: trust rules — humans assert, agents propose"
```

---

### Task 10: AssertFact with supersede semantics

**Files:**
- Modify: `internal/memory/service.go`
- Test: extend `internal/memory/service_test.go`

**Step 1: Failing test**

```go
func TestAssertFactSupersedes(t *testing.T) {
	itest(t); s := testService(t)
	subj := mustEntity(t, s, "bad.example.com")
	f1, _ := s.AssertFact(ctx(), FactInput{Scope: "default",
		SubjectID: subj.EntityID, Predicate: "verdict_malicious",
		ObjectValue: "c2", ActorType: "human", ActorID: "analyst-j"})
	f2, _ := s.AssertFact(ctx(), FactInput{ // same key → supersedes f1
		SubjectID: subj.EntityID, Predicate: "verdict_malicious",
		ObjectValue: "benign-parked", ActorType: "human", ActorID: "analyst-k"})
	// assert: FINAL view returns only f2's object_value;
	// f1 row still exists (history preserved), valid_to set to now-ish via superseded write
	// agent variant: same call with ActorType=agent lands status='proposed'
}
```

**Step 2: Implement**

On insert: UPDATE prior active fact(s) with same ORDER BY key setting `valid_to=now()` (mutation or new-version row pattern — use re-insert with same business key, new `fact_id`, `updated_at=now()`; readers use FINAL). Status from `ApplyTrust`. Audit rows for both the new fact and the superseded one.

**Step 3: `make itest` PASS, commit**

```bash
git add -A && git commit -m "feat: AssertFact with version-at-read supersede + trust application"
```

---

### Task 11: Promote / Retract

**Files:** Modify service; extend tests.

Promote: `proposed → active` (records promoting actor in audit). Retract: creates new version with `status='retracted'`, keeps history. Both human-gated in Phase 1 (API returns 403 for agent actors). Unit-test status transitions; integration-test that FINAL excludes retracted.

```bash
git add -A && git commit -m "feat: fact promotion/retraction (human-gated)"
```

---

### Task 12: Enrich()

**Files:** Modify service; extend tests.

**Step 1: Failing test** — seed entity E + 2 facts (one expired `valid_to` in past, one active) + 2 linked observations of different ages. Expect: active fact only, observations newest-first capped 10.

**Step 2: Implement**

```sql
SELECT predicate, object_value, status, confidence FROM mem.facts FINAL
WHERE subject_id = ? AND status != 'retracted'
  AND valid_from <= now() AND valid_to > now();
SELECT obs_id, ts, kind, substring(content,1,200) FROM mem.observations
WHERE hasAny(entity_refs, [?]) ORDER BY ts DESC LIMIT 10;
```

(≤1-hop CH neighbor query ships here too: union edges where src/dst = id, return neighbor ids + relations — this is also the graph-outage fallback.)

**Step 3: PASS, commit**

```bash
git add -A && git commit -m "feat: enrich() — valid-now facts + recent observations"
```

---

### Task 13: Similar() — hybrid RRF

**Files:** Modify service; extend tests.

**Step 1: Failing test** — seed 4 observations; two contain token `beaconing`; one is semantically near the query (fake embedder makes it nearest). Query `"c2 beacon traffic"` must fuse both legs: top result = the row matching BOTH legs.

**Step 2: Implement** (SQL from learning project §2.4, parameterized):

```sql
WITH vec_leg AS (
  SELECT obs_id, row_number() OVER (ORDER BY cosineDistance(content_vec, ?)) AS rnk
  FROM mem.observations WHERE scope=? AND length(content_vec)>0 LIMIT 20),
txt_leg AS (
  SELECT obs_id, row_number() OVER () AS rnk FROM mem.observations
  WHERE scope=? AND hasAnyTokens(content, ?) LIMIT 20)
SELECT obs_id, sum(1.0/(60+rnk)) AS score,
       groupArray(src) AS matched_by
FROM (SELECT obs_id,'vec' AS src,rnk FROM vec_leg
      UNION ALL SELECT obs_id,'txt',rnk FROM txt_leg)
GROUP BY obs_id ORDER BY score DESC LIMIT ?
```

Empty vector leg (embedder down) ⇒ text-only graceful path.

**Step 3: PASS, commit**

```bash
git add -A && git commit -m "feat: hybrid recall via Reciprocal Rank Fusion"
```

---

### Task 14: Timeline()

Union observations (`ts, kind, actor, content`) + facts (`valid_from AS ts, 'fact', written_by, rendered text`) filtered by case_id or subject_id, ordered desc, LIMIT/OFFSET. Integration test asserts interleaved ordering.

```bash
git add -A && git commit -m "feat: timeline reconstruction"
```

---

### Task 15: HTTP API + identity middleware

**Files:**
- Create: `internal/api/server.go`, `internal/api/middleware.go`
- Test: `internal/api/api_test.go` (httptest against service w/ fake embedder)

**Endpoints (Go 1.22 mux patterns):**

```
POST /v1/observations          → RecordObservation
POST /v1/facts                 → AssertFact
POST /v1/facts/{id}/promote    → Promote (403 for agent actors)
POST /v1/facts/{id}/retract    → Retract
GET  /v1/enrich?type=&key=     → Enrich
GET  /v1/similar?q=&scope=&k=  → Similar
GET  /v1/timeline?case_id=|entity_id=&limit=&offset=
GET  /healthz                  → CH ping
```

**Identity middleware:** reads `X-Actor-Type` (`human|agent`), `X-Actor-ID`, `X-On-Behalf-Of`, `X-Scope`; rejects missing/invalid with 400; injects into request context; every response carries `X-Mem-Actor` echo. JSON in/out, consistent error envelope `{"error": {"code","message"}}`.

Tests: happy path per endpoint + 400s + agent-promote 403.

```bash
git add -A && git commit -m "feat: REST API with identity middleware"
```

---

### Task 16: Per-agent write rate limit

Token bucket keyed by `actor_id` where `actor_type=agent`, RPS from config (`AgentRateRPS`); humans exempt. 429 with `Retry-After`. Unit-test the bucket; one httptest case.

```bash
git add -A && git commit -m "feat: per-agent write rate limiting"
```

---

### Task 17: Wire main.go

`cmd/memserved/main.go`: Load config → connect+migrate → build service (real embedder) → mount API → graceful shutdown on SIGTERM. Structured logs (stdlib `log/slog`) — never content text.

Verify end-to-end: `make run` then curl an observation + enrich.

```bash
git add -A && git commit -m "feat: service entrypoint"
```

---

### Task 18: memseed CLI (backfill)

`cmd/memseed`: reads JSONL `{ts?, kind, actor_type, actor_id, scope, content}` lines, calls RecordObservation sequentially, prints summary counts (entities created/resolved, failures). `-dry-run` flag normalizes+resolves without writing. Test with a temp JSONL fixture against integration DB.

```bash
git add -A && git commit -m "feat: seed CLI for historical backfill"
```

---

### Task 19: memeval harness

**Files:**
- Create: `cmd/memeval/main.go`
- Create: `evals/smoke.yaml`

Eval file schema:

```yaml
name: smoke
queries:
  - q: "had we seen this hash before?"
    kind: query
    must_reference_entities: ["<sha256-key>"]
    k: 10
```

Runner: for each query → `Similar(kind=query)` → PASS if any top-k result's `entity_refs` contains a referenced key. Output table: per-query pass/fail + aggregate recall@k. Exit non-zero on regression vs previous run's stored score (`evals/.last_score`). Seed the DB from a fixed fixture before evaluating so runs are comparable.

```bash
git add -A && git commit -m "feat: memory eval harness (recall@k gate)"
```

---

### Task 20: README + quickstart

Document: prerequisites (Docker, Go), `make db-up && make itest`, `make run`, curl examples for all endpoints, LM Studio model note (`nomic-embed-text-v1.5`, document/query prefixes handled server-side), pointer to design doc + this plan. Note Phase 2 roadmap (graph projection, fact extraction pipeline, MCP tools).

```bash
git add -A && git commit -m "docs: phase 1 quickstart"
```

---

## Definition of Done (Phase 1)

- [ ] `make itest` green end-to-end
- [ ] An observation containing an IOC round-trips: seeded via API → enrich returns entity + linked observation → similar() finds it by paraphrase query
- [ ] Agent-written fact lands as `proposed`; human promote flips it; audit shows all three events without content leakage
- [ ] memeval smoke eval passes and gates regressions
- [ ] Embedder down ⇒ writes still succeed (empty vec), similar() degrades to text-only

## Explicitly out of scope (later plans)

Graph projection + traverse(), LLM fact-extraction pipeline, promotion-by-agreement, MCP tool surface, OIDC auth, multi-node ClickHouse.


