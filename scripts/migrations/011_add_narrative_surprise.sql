-- ============================================================
-- Migration 011 — narrative surprise column (nowcasting plan, Phase 5b)
--
-- Research-only field, written when ENABLE_NARRATIVE_SURPRISE=1: z-score of
-- the last-24h relevance-weighted FinBERT coverage tone vs the ticker's own
-- trailing 14-day baseline, mapped to [0, 100]. NOT served by the API and
-- never enters the composite — promotion requires beating narrative_index
-- on the eval scorecard (scripts/eval/) after weeks of accumulation.
--
-- ALREADY APPLIED to Railway production on 2026-07-21.
-- This file exists for reproducibility only — do NOT re-run.
-- ============================================================

ALTER TABLE sentiment_history
    ADD COLUMN narrative_surprise DOUBLE PRECISION;
