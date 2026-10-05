-- ============================================================
-- Migration 016 — raw_articles.ingest_run
--
-- NULL = ingested by the live narrative_job. Non-NULL = the offline
-- backfill run that inserted the row (scripts/backfill/news_backfill.py
-- --run-id). Lets the eval harness measure publication→ingestion latency
-- on live ingestion only (backfilled articles arrive weeks late).
--
-- One-off tag for run news-backfill-2026-10 (2026-10-02 09:47 → 16:39 UTC):
-- rows it inserted that were published before 2026-09-29 — the live job's
-- 3-day lookback never reaches that far back, so those are certainly
-- backfill. Rows published 2026-09-29 → 10-02 inserted in that window are
-- ambiguous (live catch-up vs backfill) and stay NULL; their latency is
-- at most ~4 days.
--
-- Apply:  psql $DATABASE_URL < scripts/migrations/016_article_ingest_run.sql
-- ============================================================

ALTER TABLE raw_articles
    ADD COLUMN IF NOT EXISTS ingest_run TEXT NULL;

UPDATE raw_articles
   SET ingest_run = 'news-backfill-2026-10'
 WHERE ingest_run IS NULL
   AND created_at >= '2026-10-02 09:47:00+00'
   AND created_at <  '2026-10-02 16:40:00+00'
   AND published_at < '2026-09-29 00:00:00+00';
