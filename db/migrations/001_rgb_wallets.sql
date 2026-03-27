BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS wallets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    wallet_key TEXT NOT NULL UNIQUE,
    owner_principal_id TEXT,
    owner_user_id TEXT,
    network TEXT NOT NULL CHECK (network IN ('mainnet', 'testnet3', 'testnet4', 'regtest')),
    backend_profile_id TEXT,
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'locked', 'disabled')),
    display_name TEXT,
    rgb_account_ref TEXT,
    wallet_state_ref TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ,
    deleted_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS wallets_owner_principal_idx ON wallets (owner_principal_id);
CREATE INDEX IF NOT EXISTS wallets_owner_user_idx ON wallets (owner_user_id);
CREATE INDEX IF NOT EXISTS wallets_network_status_idx ON wallets (network, status);

CREATE TABLE IF NOT EXISTS wallet_auth_tokens (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    wallet_id UUID NOT NULL REFERENCES wallets(id) ON DELETE CASCADE,
    token_hash TEXT NOT NULL UNIQUE,
    label TEXT,
    scope JSONB NOT NULL DEFAULT '[]'::jsonb,
    expires_at TIMESTAMPTZ,
    last_used_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    revoked_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS wallet_auth_tokens_wallet_idx ON wallet_auth_tokens (wallet_id);
CREATE INDEX IF NOT EXISTS wallet_auth_tokens_active_idx ON wallet_auth_tokens (wallet_id, revoked_at, expires_at);

CREATE TABLE IF NOT EXISTS wallet_assets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    wallet_id UUID NOT NULL REFERENCES wallets(id) ON DELETE CASCADE,
    asset_id TEXT NOT NULL,
    asset_schema TEXT NOT NULL,
    ticker TEXT,
    name TEXT NOT NULL,
    precision INTEGER NOT NULL DEFAULT 0,
    contract_id TEXT,
    imported_by_user BOOLEAN NOT NULL DEFAULT TRUE,
    display_color TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (wallet_id, asset_id)
);

CREATE INDEX IF NOT EXISTS wallet_assets_wallet_idx ON wallet_assets (wallet_id);
CREATE INDEX IF NOT EXISTS wallet_assets_contract_idx ON wallet_assets (contract_id);

CREATE TABLE IF NOT EXISTS wallet_asset_balances (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    wallet_asset_id UUID NOT NULL REFERENCES wallet_assets(id) ON DELETE CASCADE,
    settled NUMERIC(30, 0) NOT NULL DEFAULT 0,
    future NUMERIC(30, 0) NOT NULL DEFAULT 0,
    spendable NUMERIC(30, 0) NOT NULL DEFAULT 0,
    offchain_outbound NUMERIC(30, 0) NOT NULL DEFAULT 0,
    offchain_inbound NUMERIC(30, 0) NOT NULL DEFAULT 0,
    source_updated_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (wallet_asset_id)
);

CREATE TABLE IF NOT EXISTS rgb_invoices (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    wallet_id UUID NOT NULL REFERENCES wallets(id) ON DELETE CASCADE,
    wallet_asset_id UUID REFERENCES wallet_assets(id) ON DELETE SET NULL,
    invoice_string TEXT NOT NULL UNIQUE,
    recipient_id TEXT NOT NULL,
    recipient_type TEXT,
    assignment_type TEXT,
    assignment_value NUMERIC(30, 0),
    amount_open BOOLEAN NOT NULL DEFAULT FALSE,
    batch_transfer_idx BIGINT,
    proxy_endpoint TEXT,
    expiration_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'pending_consignment', 'acknowledged', 'settled', 'expired', 'failed')),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS rgb_invoices_wallet_idx ON rgb_invoices (wallet_id, created_at DESC);
CREATE INDEX IF NOT EXISTS rgb_invoices_recipient_idx ON rgb_invoices (recipient_id);
CREATE INDEX IF NOT EXISTS rgb_invoices_status_idx ON rgb_invoices (wallet_id, status);

CREATE TABLE IF NOT EXISTS rgb_transfers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    wallet_id UUID NOT NULL REFERENCES wallets(id) ON DELETE CASCADE,
    wallet_asset_id UUID REFERENCES wallet_assets(id) ON DELETE SET NULL,
    invoice_id UUID REFERENCES rgb_invoices(id) ON DELETE SET NULL,
    direction TEXT NOT NULL CHECK (direction IN ('incoming', 'outgoing')),
    transfer_kind TEXT NOT NULL,
    status TEXT NOT NULL,
    rgb_transfer_idx BIGINT,
    txid TEXT,
    change_utxo TEXT,
    receive_utxo TEXT,
    recipient_id TEXT,
    requested_assignment_type TEXT,
    requested_assignment_value NUMERIC(30, 0),
    settled_amount NUMERIC(30, 0),
    expiration_at TIMESTAMPTZ,
    detected_at TIMESTAMPTZ,
    settled_at TIMESTAMPTZ,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS rgb_transfers_wallet_idx ON rgb_transfers (wallet_id, created_at DESC);
CREATE INDEX IF NOT EXISTS rgb_transfers_wallet_status_idx ON rgb_transfers (wallet_id, status, created_at DESC);
CREATE INDEX IF NOT EXISTS rgb_transfers_txid_idx ON rgb_transfers (txid);
CREATE INDEX IF NOT EXISTS rgb_transfers_recipient_idx ON rgb_transfers (recipient_id);
CREATE UNIQUE INDEX IF NOT EXISTS rgb_transfers_wallet_rgb_idx
    ON rgb_transfers (wallet_id, rgb_transfer_idx)
    WHERE rgb_transfer_idx IS NOT NULL;

CREATE TABLE IF NOT EXISTS consignment_records (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    wallet_id UUID NOT NULL REFERENCES wallets(id) ON DELETE CASCADE,
    transfer_id UUID REFERENCES rgb_transfers(id) ON DELETE CASCADE,
    invoice_id UUID REFERENCES rgb_invoices(id) ON DELETE SET NULL,
    recipient_id TEXT NOT NULL,
    consignment_hash TEXT,
    consignment_object_key TEXT,
    txid TEXT,
    vout INTEGER,
    proxy_endpoint TEXT,
    delivery_status TEXT NOT NULL DEFAULT 'awaiting' CHECK (delivery_status IN ('awaiting', 'posted', 'fetched', 'validated', 'acked', 'nacked', 'failed', 'expired')),
    ack BOOLEAN,
    acked_at TIMESTAMPTZ,
    fetched_at TIMESTAMPTZ,
    validated_at TIMESTAMPTZ,
    error_message TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS consignment_records_wallet_idx ON consignment_records (wallet_id, created_at DESC);
CREATE INDEX IF NOT EXISTS consignment_records_recipient_idx ON consignment_records (recipient_id);
CREATE INDEX IF NOT EXISTS consignment_records_status_idx ON consignment_records (wallet_id, delivery_status);

CREATE TABLE IF NOT EXISTS transfer_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    wallet_id UUID NOT NULL REFERENCES wallets(id) ON DELETE CASCADE,
    transfer_id UUID REFERENCES rgb_transfers(id) ON DELETE CASCADE,
    invoice_id UUID REFERENCES rgb_invoices(id) ON DELETE SET NULL,
    consignment_id UUID REFERENCES consignment_records(id) ON DELETE SET NULL,
    event_type TEXT NOT NULL,
    event_source TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS transfer_events_wallet_idx ON transfer_events (wallet_id, created_at DESC);
CREATE INDEX IF NOT EXISTS transfer_events_transfer_idx ON transfer_events (transfer_id, created_at DESC);
CREATE INDEX IF NOT EXISTS transfer_events_type_idx ON transfer_events (event_type, created_at DESC);

CREATE TABLE IF NOT EXISTS refresh_jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    wallet_id UUID NOT NULL REFERENCES wallets(id) ON DELETE CASCADE,
    transfer_id UUID REFERENCES rgb_transfers(id) ON DELETE CASCADE,
    job_type TEXT NOT NULL CHECK (job_type IN ('refresh_wallet', 'refresh_transfer', 'poll_consignment', 'settlement_reconcile')),
    status TEXT NOT NULL DEFAULT 'queued' CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')),
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 10,
    run_after TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    last_error TEXT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS refresh_jobs_sched_idx ON refresh_jobs (status, run_after);
CREATE INDEX IF NOT EXISTS refresh_jobs_wallet_idx ON refresh_jobs (wallet_id, status, run_after);

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS wallets_set_updated_at ON wallets;
CREATE TRIGGER wallets_set_updated_at
BEFORE UPDATE ON wallets
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS wallet_assets_set_updated_at ON wallet_assets;
CREATE TRIGGER wallet_assets_set_updated_at
BEFORE UPDATE ON wallet_assets
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS wallet_asset_balances_set_updated_at ON wallet_asset_balances;
CREATE TRIGGER wallet_asset_balances_set_updated_at
BEFORE UPDATE ON wallet_asset_balances
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS rgb_invoices_set_updated_at ON rgb_invoices;
CREATE TRIGGER rgb_invoices_set_updated_at
BEFORE UPDATE ON rgb_invoices
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS rgb_transfers_set_updated_at ON rgb_transfers;
CREATE TRIGGER rgb_transfers_set_updated_at
BEFORE UPDATE ON rgb_transfers
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS consignment_records_set_updated_at ON consignment_records;
CREATE TRIGGER consignment_records_set_updated_at
BEFORE UPDATE ON consignment_records
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS refresh_jobs_set_updated_at ON refresh_jobs;
CREATE TRIGGER refresh_jobs_set_updated_at
BEFORE UPDATE ON refresh_jobs
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

COMMIT;
