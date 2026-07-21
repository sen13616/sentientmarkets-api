-- ============================================================
-- Migration 009 — per-consumer API key labels (nowcasting plan, Phase 1)
--
-- Adds a human-readable label so each consumer (website, research jobs,
-- backtests) gets its own key and rate-limit bucket. Nullable and unread
-- by the auth path (lookup stays key_hash-only).
--
-- ALREADY APPLIED to Railway production on 2026-07-21.
-- This file exists for reproducibility only — do NOT re-run.
-- ============================================================

ALTER TABLE api_keys
    ADD COLUMN label VARCHAR(100);

-- Backfill the two pre-existing internal keys so `list` output is readable.
UPDATE api_keys
   SET label = 'legacy-internal-' || tier
 WHERE owner_email = 'sentientmarkets@internal' AND label IS NULL;
