-- Migration 002: projection watermark table. All statements idempotent
-- (retry-safe convention: ClickHouse has no DDL transactions, migrations may
-- re-run mid-file after a failure).
--
-- The cursor is the composite (ts, last_id): pages are read with strict
-- tuple comparison (updated_at, toString(entity_id)) > (ts, last_id) — the
-- id tiebreaker is cast to String so pagination, ORDER BY and the CAS all
-- share ONE canonical-text total order (ClickHouse's internal UUID byte
-- order disagrees with text) — which makes projection exactly-once per row
-- version: no skips across same-timestamp page boundaries, no re-reads of
-- the boundary row itself. last_id is the entity_id tiebreaker; '' means
-- "start of time".

CREATE TABLE IF NOT EXISTS mem.projection_watermark (
  name String,
  ts DateTime64(3),
  last_id String DEFAULT ''
) ENGINE = MergeTree ORDER BY name;

-- Seed one epoch row per projected source. The WHERE NOT EXISTS guard keeps
-- reruns from doubling rows; plain MergeTree does not collapse duplicates on
-- its own. Projection workers advance these with ALTER TABLE ... UPDATE
-- (mutations_sync=1), never INSERT.
INSERT INTO mem.projection_watermark (name, ts, last_id)
SELECT 'entities', toDateTime64(0, 3), ''
WHERE NOT EXISTS (SELECT 1 FROM mem.projection_watermark WHERE name = 'entities');

INSERT INTO mem.projection_watermark (name, ts, last_id)
SELECT 'edges', toDateTime64(0, 3), ''
WHERE NOT EXISTS (SELECT 1 FROM mem.projection_watermark WHERE name = 'edges');
