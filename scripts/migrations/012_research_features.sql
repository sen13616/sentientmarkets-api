-- 012: research_features JSONB on sentiment_history (Track B1, 2026-07-22)
--
-- Research-only feature store. Tick-time research features (positioning
-- z-scores, future candidates) live in this single JSONB column keyed by
-- feature name — no schema change per feature, ever. The column is:
--   * written by pipeline/features/positioning.py (flag-gated,
--     ENABLE_POSITIONING_FEATURES) and scripts/eval/backfill_features.py
--   * read ONLY by the eval harness (scripts/eval/data.py), which
--     auto-registers every key it finds
--   * NEVER serialized into any API response — api/response/schemas.py has
--     no such field and tests/test_api_hardening.py guards it explicitly.
--
-- B1 decision, documented here: raw_articles.created_at (000_initial_schema,
-- TIMESTAMPTZ NOT NULL DEFAULT NOW()) already reliably records ingestion time
-- (verified 2026-07-22: 137,607 rows, 0 NULLs, 0 created_at < published_at).
-- A separate ingested_at column would be redundant — created_at IS the
-- ingestion timestamp. No article-side change in this migration.

ALTER TABLE sentiment_history ADD COLUMN IF NOT EXISTS research_features JSONB;
