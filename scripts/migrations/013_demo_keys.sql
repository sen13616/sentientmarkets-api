-- ============================================================
-- Migration 013 — demo API keys (docs/APIACCESSPAGE.md, Session B)
--
-- Adds key_type ('demo'|'standard') and expires_at so /v1/demo-key can
-- mint self-expiring free-tier sandbox keys for the /api-access page.
-- Backwards compatible: existing rows default to 'standard' with
-- expires_at NULL (never expire — behave exactly as today, including
-- the keys minted by scripts/tools/generate_keys.py).
--
-- NOT yet applied. Owner applies manually via psql:
--   psql $DATABASE_URL < scripts/migrations/013_demo_keys.sql
-- ============================================================

ALTER TABLE api_keys
    ADD COLUMN key_type   VARCHAR(20) NOT NULL DEFAULT 'standard'
        CHECK (key_type IN ('demo', 'standard')),
    ADD COLUMN expires_at TIMESTAMPTZ NULL;

-- Cheap scans for the hourly cleanup job (demo rows only).
CREATE INDEX idx_api_keys_demo_expiry
    ON api_keys (expires_at)
    WHERE key_type = 'demo';
