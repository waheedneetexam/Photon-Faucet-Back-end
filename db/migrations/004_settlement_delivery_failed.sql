BEGIN;

-- Extend settlement_status_enum with a terminal failure state so the UI can
-- show exactly where the transfer pipeline stalled.
ALTER TYPE settlement_status_enum ADD VALUE IF NOT EXISTS 'DELIVERY_FAILED';

COMMIT;
