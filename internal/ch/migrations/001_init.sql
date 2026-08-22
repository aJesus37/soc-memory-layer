-- Migration 001: initial schema. All statements idempotent (retry-safe
-- convention: ClickHouse has no DDL transactions, migrations may re-run
-- mid-file after a failure).

CREATE TABLE IF NOT EXISTS mem.entities (
  entity_id    UUID,
  scope        LowCardinality(String),
  entity_type  Enum8('ioc_ip','ioc_domain','ioc_hash','asset','user',
                     'actor','campaign','malware','technique','tool',
                     'case','document'),
  key          String,
  display_name String,
  attrs        Map(String, String),
  first_seen   DateTime,
  last_seen    DateTime,
  updated_at   DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (scope, entity_type, key);

CREATE TABLE IF NOT EXISTS mem.observations (
  obs_id          UUID,
  scope           LowCardinality(String),
  ts              DateTime,
  kind            Enum8('alert','triage_decision','investigation_note',
                        'hunt_finding','agent_action','human_statement'),
  actor_type      Enum8('human','agent'),
  actor_id        String,
  on_behalf_of    String DEFAULT '',
  case_id         Nullable(UUID),
  confidentiality Enum8('internal','restricted') DEFAULT 'internal',
  content         String,
  content_vec     Array(Float32),
  entity_refs     Array(UUID),
  INDEX ft_idx content TYPE text(tokenizer = splitByNonAlpha)
) ENGINE = MergeTree
ORDER BY (scope, ts)
TTL ts + INTERVAL 365 DAY DELETE WHERE kind = 'alert';

CREATE TABLE IF NOT EXISTS mem.facts (
  fact_id      UUID,
  scope        LowCardinality(String),
  subject_id   UUID,
  predicate    LowCardinality(String),
  object_value String,
  object_id    Nullable(UUID),
  status       Enum8('proposed','active','retracted') DEFAULT 'active',
  confidence   Float32 DEFAULT 0.5,
  source_obs   UUID,
  written_by   String DEFAULT '',
  valid_from   DateTime,
  valid_to     DateTime DEFAULT toDateTime64('9999-12-31 00:00:00', 0),
  updated_at   DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (scope, subject_id, predicate, object_value);

CREATE TABLE IF NOT EXISTS mem.edges (
  edge_id    UUID,
  scope      LowCardinality(String),
  src_id     UUID,
  dst_id     UUID,
  relation   LowCardinality(String),
  from_fact  UUID,
  valid_from DateTime,
  valid_to   DateTime DEFAULT toDateTime64('9999-12-31 00:00:00', 0)
) ENGINE = MergeTree
ORDER BY src_id;

CREATE TABLE IF NOT EXISTS mem.audit (
  ts              DateTime DEFAULT now(),
  actor_type      Enum8('human','agent','system'),
  actor_id        String,
  operation       String,
  target_table    String,
  target_id       UUID,
  payload_summary String DEFAULT ''
) ENGINE = MergeTree
ORDER BY ts;
