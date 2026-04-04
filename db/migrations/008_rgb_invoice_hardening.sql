-- NODE-002: Harden rgb_invoices storage for same-node transfers.
-- Store receiver wallet_key + rgb_account_ref + asset_id + asset_amount directly on the invoice row
-- so later wallet reassignment does not break invoice → receiver mapping.

BEGIN;

ALTER TABLE rgb_invoices
  ADD COLUMN IF NOT EXISTS receiver_wallet_key TEXT,
  ADD COLUMN IF NOT EXISTS receiver_rgb_account_ref TEXT,
  ADD COLUMN IF NOT EXISTS asset_id TEXT,
  ADD COLUMN IF NOT EXISTS asset_amount NUMERIC(30, 0);

-- Backfill from joins / metadata where possible.
UPDATE rgb_invoices i
SET
  receiver_wallet_key = COALESCE(i.receiver_wallet_key, w.wallet_key),
  receiver_rgb_account_ref = COALESCE(i.receiver_rgb_account_ref, w.rgb_account_ref),
  asset_id = COALESCE(i.asset_id, wa.asset_id, NULLIF(i.metadata->>'asset_id','')),
  asset_amount = COALESCE(
    i.asset_amount,
    NULLIF(i.metadata->>'asset_amount','')::numeric,
    i.assignment_value
  )
FROM wallets w
LEFT JOIN wallet_assets wa
  ON wa.id = i.wallet_asset_id
WHERE w.id = i.wallet_id;

CREATE INDEX IF NOT EXISTS rgb_invoices_receiver_wallet_key_idx
  ON rgb_invoices(receiver_wallet_key);

CREATE INDEX IF NOT EXISTS rgb_invoices_receiver_account_ref_idx
  ON rgb_invoices(receiver_rgb_account_ref);

CREATE INDEX IF NOT EXISTS rgb_invoices_asset_id_idx
  ON rgb_invoices(asset_id);

COMMIT;

