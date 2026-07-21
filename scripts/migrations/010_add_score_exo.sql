-- ============================================================
-- Migration 010 — exogenous sentiment-only composite (nowcasting plan, Phase 3)
--
-- composite_score_exo stores the RAW composite over the non-price layers
-- (narrative/influencer/macro, weights renormalized; market excluded).
-- Nullable: NULL means all three exo layers were missing that tick —
-- never a fabricated neutral 50.
--
-- ALREADY APPLIED to Railway production on 2026-07-21.
-- This file exists for reproducibility only — do NOT re-run.
-- ============================================================

ALTER TABLE sentiment_history
    ADD COLUMN composite_score_exo DOUBLE PRECISION;
