-- ============================================================
-- Migration 014 — sentiment_history.replay_run
--
-- Tags rows that were (re)computed offline rather than served live.
-- NULL = live row written by the scoring tick (the served value).
-- Non-NULL = run ID of the offline job that wrote or rewrote the row,
-- e.g. 'news-backfill-2026-10' (scripts/backfill/rebuild_narrative.py and
-- scripts/backfill/replay_scores.py, after the 2026-08-10 → 10-02 outage).
--
-- The eval harness excludes non-NULL rows by default (it measures served
-- scores). Nullable with no default → metadata-only ALTER, no table rewrite.
--
-- Apply via psql:
--   psql $DATABASE_URL < scripts/migrations/014_replay_run.sql
-- ============================================================

ALTER TABLE sentiment_history
    ADD COLUMN IF NOT EXISTS replay_run TEXT NULL;
