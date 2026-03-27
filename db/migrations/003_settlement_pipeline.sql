BEGIN;

-- Enum type for the Lightning/RGB settlement state machine.
-- Only populated on outgoing lightning transfers; NULL means onchain/non-lightning.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'settlement_status_enum') THEN
    CREATE TYPE settlement_status_enum AS ENUM (
      'INITIATED',
      'PAYMENT_SUCCESS',
      'CONSIGNMENT_UPLOADED',
      'SETTLED'
    );
  END IF;
END
$$;

ALTER TABLE rgb_transfers
  ADD COLUMN IF NOT EXISTS settlement_status settlement_status_enum;

CREATE INDEX IF NOT EXISTS rgb_transfers_settlement_status_idx
  ON rgb_transfers (wallet_id, settlement_status)
  WHERE settlement_status IS NOT NULL;

COMMENT ON COLUMN rgb_transfers.settlement_status IS
  'Multi-stage Lightning/RGB settlement state machine.
   INITIATED              – payment request received, pre-flight row created.
   PAYMENT_SUCCESS        – Lightning pre-image received from the node.
   CONSIGNMENT_UPLOADED   – PHO consignment pushed to the RGB Proxy.
   SETTLED                – Receiver validated the asset and updated coloring table.
   NULL                   – On-chain transfer; use the status column instead.';

COMMIT;
