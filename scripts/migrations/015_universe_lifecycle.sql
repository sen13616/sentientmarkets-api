-- ============================================================
-- Migration 015 — ticker_universe lifecycle + S&P 500 membership
--
-- delisted_at       last trading day under this symbol (NULL = active).
--                   Retired symbols stay in the table so their history
--                   remains servable/researchable; live jobs skip them.
-- successor_ticker  the symbol holders/coverage moved to (ticker change,
--                   merger), or NULL (cash take-private / no successor).
-- delisted_reason   short human-readable cause (see
--                   scripts/tools/data/universe_changes_*.csv for sources).
-- in_sp500          current S&P 500 membership snapshot (set by
--                   scripts/tools/update_universe.py; membership HISTORY
--                   is not tracked).
-- sp500_added       S&P 500 "date added" for current members.
--
-- Nullable / constant-default columns → metadata-only ALTER, no rewrite.
-- Apply:  psql $DATABASE_URL < scripts/migrations/015_universe_lifecycle.sql
-- ============================================================

ALTER TABLE ticker_universe
    ADD COLUMN IF NOT EXISTS delisted_at      TIMESTAMPTZ NULL,
    ADD COLUMN IF NOT EXISTS successor_ticker VARCHAR(10) NULL,
    ADD COLUMN IF NOT EXISTS delisted_reason  TEXT        NULL,
    ADD COLUMN IF NOT EXISTS in_sp500         BOOLEAN     NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS sp500_added      DATE        NULL;
