BEGIN;

-- Enum for UTXO slot lifecycle states.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'utxo_slot_state_enum') THEN
    CREATE TYPE utxo_slot_state_enum AS ENUM (
      'FREE',
      'OCCUPIED',
      'EMPTY',
      'REDEEMED'
    );
  END IF;
END
$$;

-- Per-user UTXO slot accounting.
-- Each row represents one physical UTXO ("txid:vout") in the rgb-lightning-node's
-- shared wallet pool that has been allocated to a specific user wallet.
-- The node does not support per-user UTXO control, so this table is the
-- accounting layer that tracks ownership and lifecycle state.
CREATE TABLE IF NOT EXISTS user_utxo_slots (
  id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  wallet_id        UUID        NOT NULL REFERENCES wallets(id) ON DELETE CASCADE,
  outpoint         TEXT        NOT NULL,       -- "txid:vout" as returned by rgb-lightning-node
  state            utxo_slot_state_enum NOT NULL DEFAULT 'FREE',
  sats_value       BIGINT,                     -- satoshis locked in this UTXO (from createutxos)
  node_account_ref TEXT,                       -- which node this slot lives on (e.g. "owner" / "user")
  rgb_transfer_id  UUID        REFERENCES rgb_transfers(id) ON DELETE SET NULL,
  invoice_id       UUID        REFERENCES rgb_invoices(id)  ON DELETE SET NULL,
  redeemed_txid    TEXT,                       -- Bitcoin txid when sats were paid back to user
  created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  redeemed_at      TIMESTAMPTZ,
  UNIQUE (wallet_id, outpoint)
);

COMMENT ON TABLE user_utxo_slots IS
  'Accounting layer for per-user UTXO slot ownership in the shared rgb-lightning-node pool.
   Physical UTXOs live in the node wallet; this table tracks who owns each slot and its state.
   FREE     – invoice created, receive_utxo assigned, no RGB asset settled yet.
   OCCUPIED – incoming transfer settled and validated into this slot.
   EMPTY    – asset that occupied this slot was sent away and outgoing transfer settled.
   REDEEMED – user cashed out the BTC value of this slot; terminal state.';

COMMENT ON COLUMN user_utxo_slots.outpoint        IS '"txid:vout" string as returned by /rgbinvoice receive_utxo field.';
COMMENT ON COLUMN user_utxo_slots.sats_value       IS 'Satoshis locked in this UTXO. Set from createutxos response or known slot size.';
COMMENT ON COLUMN user_utxo_slots.node_account_ref IS 'rgb_account_ref of the node wallet that owns this UTXO pool slot.';
COMMENT ON COLUMN user_utxo_slots.redeemed_txid    IS 'Bitcoin txid of the faucet hot-wallet payout to the user on redemption.';

-- Fast lookups by wallet + state (used by GET /api/utxo/slots and redeem endpoint)
CREATE INDEX IF NOT EXISTS user_utxo_slots_wallet_state_idx
  ON user_utxo_slots (wallet_id, state);

-- Fast lookup by outpoint alone (used by markSlotOccupied / markSlotEmpty)
CREATE INDEX IF NOT EXISTS user_utxo_slots_outpoint_idx
  ON user_utxo_slots (outpoint);

-- Auto-update updated_at on any row change
CREATE OR REPLACE FUNCTION set_utxo_slot_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS utxo_slots_set_updated_at ON user_utxo_slots;
CREATE TRIGGER utxo_slots_set_updated_at
  BEFORE UPDATE ON user_utxo_slots
  FOR EACH ROW EXECUTE FUNCTION set_utxo_slot_updated_at();

COMMIT;
