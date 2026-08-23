-- Migration 004: extraction coverage log ("dreaming-lite" worker
-- bookkeeping). All statements idempotent (retry-safe convention:
-- ClickHouse has no DDL transactions, migrations may re-run mid-file after
-- a failure).
--
-- WHY a coverage table: the extraction worker turns observations into
-- PROPOSED facts via an LLM, selecting observations that lack coverage.
-- The fact rows themselves cannot serve as the coverage marker
-- (source_obs only exists when something was asserted), so observations
-- yielding ZERO proposals — model found nothing, or its output sanitized
-- away — would rescan forever. Worse, a model that errors on ONE specific
-- content would retry that observation every tick and wedge the queue
-- behind it (the poison-wedge concern from review). Each ATTEMPTED obs_id
-- is therefore logged here regardless of outcome — zero proposals and
-- LLM-error observations included — so worker progress is monotonic.
--
-- Plain MergeTree, no Replacing engine: duplicate rows for one obs_id are
-- possible under reruns/concurrent workers and harmless — the worker's
-- NOT EXISTS anti-join only asks "covered?", never counts. ORDER BY
-- obs_id gives the primary-key index the per-row probe uses; the global
-- scan over mem.observations dominates at SOC volumes either way.

CREATE TABLE IF NOT EXISTS mem.extract_log (
  obs_id UUID,
  extracted_at DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree ORDER BY obs_id;
