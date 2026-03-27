ALTER TABLE consignment_records
ADD COLUMN IF NOT EXISTS blinding_secret TEXT;

ALTER TABLE consignment_records
ADD COLUMN IF NOT EXISTS blinding_secret_status TEXT NOT NULL DEFAULT 'unavailable';

ALTER TABLE consignment_records
DROP CONSTRAINT IF EXISTS consignment_records_blinding_secret_status_check;

ALTER TABLE consignment_records
ADD CONSTRAINT consignment_records_blinding_secret_status_check
CHECK (blinding_secret_status IN ('unavailable', 'pending', 'active'));

CREATE INDEX IF NOT EXISTS consignment_records_recipient_secret_idx
ON consignment_records (recipient_id, blinding_secret);

CREATE INDEX IF NOT EXISTS consignment_records_secret_status_idx
ON consignment_records (wallet_id, blinding_secret_status);
