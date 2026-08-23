-- Migration 005: fact visibility model for the shared-knowledge deployment.
-- All statements idempotent (retry-safe convention: ClickHouse has no DDL
-- transactions, migrations may re-run mid-file after a failure).
--
-- PRODUCT DECISION (single-org deployment): facts are ORG-VISIBLE by
-- default so every team benefits from every other team's knowledge.
-- visibility='org' (the default, also backfilled onto every pre-existing
-- row) means the fact is readable from any scope; visibility='scope' marks
-- a fact readable only within its originating scope. The Enrich read path
-- enforces this with (scope = caller OR visibility = 'org'); the write
-- path always writes the column explicitly and promote/retract
-- replacements preserve the original value.
--
-- Observations need no schema change: mem.observations already carries
-- confidentiality Enum8('internal','restricted') — this migration is what
-- makes enforcement real on the read side: internal = org-readable,
-- restricted = originating-scope-only.

ALTER TABLE mem.facts ADD COLUMN IF NOT EXISTS visibility Enum8('org','scope') DEFAULT 'org';
