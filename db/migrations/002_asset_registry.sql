BEGIN;

CREATE TABLE IF NOT EXISTS asset_registry (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    network TEXT NOT NULL CHECK (network IN ('mainnet', 'testnet3', 'testnet4', 'regtest')),
    token_name TEXT NOT NULL,
    ticker TEXT NOT NULL,
    total_supply NUMERIC(30, 0) NOT NULL,
    precision INTEGER NOT NULL DEFAULT 0,
    issuer_ref TEXT,
    creation_date DATE,
    block_height BIGINT,
    contract_id TEXT NOT NULL UNIQUE,
    schema_id TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS asset_registry_network_idx ON asset_registry (network, created_at DESC);
CREATE INDEX IF NOT EXISTS asset_registry_ticker_idx ON asset_registry (ticker);
CREATE INDEX IF NOT EXISTS asset_registry_token_name_idx ON asset_registry (token_name);

DROP TRIGGER IF EXISTS asset_registry_set_updated_at ON asset_registry;
CREATE TRIGGER asset_registry_set_updated_at
BEFORE UPDATE ON asset_registry
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

INSERT INTO asset_registry (
    network,
    token_name,
    ticker,
    total_supply,
    precision,
    issuer_ref,
    creation_date,
    block_height,
    contract_id,
    schema_id,
    metadata
)
VALUES (
    'regtest',
    'Photon Token',
    'PHO',
    1000000,
    8,
    'photon_dev',
    DATE '2026-03-13',
    112,
    'rgb:2Mhfmuc0-BqWCUwP-kkJKF_V-F1~L4j6-A1_W6Yy-hK6Z~rA',
    'RWhwUfTMpuP2Zfx1~j4nswCANGeJrYOqDcKelaMV4zU#remote-digital-pegasus',
    jsonb_build_object(
        'display_color', '#38bdf8',
        'name', 'Photon Token',
        'notes', 'Photon regtest PHO asset registry entry'
    )
)
ON CONFLICT (contract_id)
DO UPDATE SET
    token_name = EXCLUDED.token_name,
    ticker = EXCLUDED.ticker,
    total_supply = EXCLUDED.total_supply,
    precision = EXCLUDED.precision,
    issuer_ref = EXCLUDED.issuer_ref,
    creation_date = EXCLUDED.creation_date,
    block_height = EXCLUDED.block_height,
    schema_id = EXCLUDED.schema_id,
    metadata = EXCLUDED.metadata,
    updated_at = NOW();

COMMIT;
