# Demo walkthrough: the PHANTOM PORTAL investigation

A synthetic, multi-team incident seeded into the dev stores. One story, three
teams, designed so every memory-layer feature visibly fires. All data is
synthetic (RFC 5737 documentation IPs, invented domains/hashes).

## The story

| When | Team | What happened |
|---|---|---|
| Aug 10 | tier1 | Phish to finance (j.malheiro) — link click on ws-fin-0042.corp.company.com |
| Aug 11–12 | threatresp | Investigated phishing domain; found C2 IP 203.0.113.77 |
| Aug 13 | tier1 | **restricted**: VP Finance also targeted |
| Aug 14 | hunt | Proactive hunt: ws-fin-0042.corp.company.com beaconing to **the same IP** |
| Aug 15–16 | threatresp + hunt | Loader hash; second C2 198.51.100.23; attribution to phantom-panda |
| Aug 18 | tier1 | Portuguese-language phish variant reported |
| Aug 21 | agent | triage-bot auto-closed a duplicate alert |
| Aug 22 | hunt | Containment sweep |

## Seed it

```bash
task db-up        # ClickHouse + Dgraph healthy
task demo-seed    # observations + human fact assertions (3 scopes)
```

## The five things to try

### 1. Cross-team enrichment (org-shared knowledge)

Team-a's connection sees what hunt and threatresp learned:

```bash
curl -s 'localhost:8090/v1/enrich?type=ioc_domain&key=secure-portal.invoice-update.com' \
  -H 'X-Actor-Type: human' -H 'X-Actor-ID: you' -H 'X-Scope: team-a' | jq .
```

Expect: `found:true`, facts asserted by team-threatresp (`origin_scope` labels),
observations from tier1 AND threatresp — even though "you" are team-a.

### 2. Restricted stays home

The Aug-13 restricted note is invisible from any other scope:

```bash
curl -s 'localhost:8090/v1/similar?q=executive target variant&k=10' \
  -H 'X-Actor-Type: human' -H 'X-Actor-ID: you' -H 'X-Scope: team-a'
# → no restricted content
# same query with X-Scope: team-tier1 → present
```

### 3. Graph traversal across teams

```bash
curl -s 'localhost:8090/v1/traverse?type=ioc_domain&key=secure-portal.invoice-update.com&hops=2' \
  -H 'X-Actor-Type: human' -H 'X-Actor-ID: you' -H 'X-Scope: team-a' | jq .
```

Walks domain → C2 IP via team-threatresp's edge.

### 4. Dreaming-lite extraction

Restart the service with `task run-extract`, then POST a *new* note stating a
fact in prose:

```bash
curl -s -X POST localhost:8090/v1/observations \
  -H 'Content-Type: application/json' \
  -H 'X-Actor-Type: human' -H 'X-Actor-ID: analyst-x' -H 'X-Scope: team-hunt' \
  -d '{"kind":"hunt_finding","content":"confirmed 198.51.100.23 resolved_to fallback-c2.phantom-infra.net during rotation"}'
```

Within ~30s the extractor proposes `fallback-c2.phantom-infra.net resolved_to
198.51.100.23` — enrich it and watch attribution say `extractor-v1`.

### 5. Trust model in one flow

Open Swagger (`localhost:8090/swagger/index.html`), authorize nothing, call
`POST /v1/facts` with an agent identity → status `proposed`. Promote it with a
human identity → `active`, org-visible. The audit trail shows both steps.

## Recall gate

```bash
task eval   # uses evals/demo.yaml — cross-team recall assertions
```
