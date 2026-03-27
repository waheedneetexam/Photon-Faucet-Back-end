const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.PHOTON_RGB_DATABASE_URL || undefined,
  host: process.env.PGHOST || '/var/run/postgresql',
  port: Number(process.env.PGPORT || 5432),
  database: process.env.PGDATABASE || 'photon_rgb_wallets',
  user: process.env.PGUSER || process.env.USER || 'waheed',
  password: process.env.PGPASSWORD || undefined,
  max: Number(process.env.PGPOOL_MAX || 10),
  idleTimeoutMillis: Number(process.env.PG_IDLE_TIMEOUT_MS || 30000),
});

pool.on('error', (error) => {
  console.error('[RGB DB] Unexpected PostgreSQL error:', error);
});

function resolveWalletKey(req) {
  const headerValue = req.headers['x-photon-wallet-key'];
  if (typeof headerValue === 'string' && headerValue.trim()) {
    return headerValue.trim();
  }
  return `dev-${process.env.BITCOIN_RPC_WALLET || 'photon_dev'}-regtest`;
}

async function query(text, params = []) {
  return pool.query(text, params);
}

async function ensureWallet(req, network = 'regtest') {
  const walletKey = resolveWalletKey(req);
  const result = await query(
    `
      INSERT INTO wallets (wallet_key, network, backend_profile_id, display_name)
      VALUES ($1, $2, $3, $4)
      ON CONFLICT (wallet_key)
      DO UPDATE SET
        network = EXCLUDED.network,
        backend_profile_id = EXCLUDED.backend_profile_id,
        last_seen_at = NOW()
      RETURNING id, wallet_key, network, rgb_account_ref
    `,
    [walletKey, network, 'photon-dev-regtest', walletKey]
  );
  return result.rows[0];
}

async function upsertWalletAsset({ walletId, assetId, assetSchema = 'Nia', contractId, name, ticker, precision = 0 }) {
  const result = await query(
    `
      INSERT INTO wallet_assets (
        wallet_id, asset_id, asset_schema, ticker, name, precision, contract_id
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7)
      ON CONFLICT (wallet_id, asset_id)
      DO UPDATE SET
        asset_schema = EXCLUDED.asset_schema,
        ticker = COALESCE(EXCLUDED.ticker, wallet_assets.ticker),
        name = EXCLUDED.name,
        precision = EXCLUDED.precision,
        contract_id = COALESCE(EXCLUDED.contract_id, wallet_assets.contract_id)
      RETURNING id, asset_id, contract_id, ticker, name, precision
    `,
    [walletId, assetId, assetSchema, ticker, name, precision, contractId || null]
  );
  return result.rows[0];
}

async function upsertWalletAssetBalance(walletAssetId, balance) {
  await query(
    `
      INSERT INTO wallet_asset_balances (
        wallet_asset_id, settled, future, spendable, offchain_outbound, offchain_inbound, source_updated_at
      )
      VALUES ($1, $2, $3, $4, $5, $6, NOW())
      ON CONFLICT (wallet_asset_id)
      DO UPDATE SET
        settled = EXCLUDED.settled,
        future = EXCLUDED.future,
        spendable = EXCLUDED.spendable,
        offchain_outbound = EXCLUDED.offchain_outbound,
        offchain_inbound = EXCLUDED.offchain_inbound,
        source_updated_at = NOW()
    `,
    [
      walletAssetId,
      String(balance.settled || 0),
      String(balance.future || 0),
      String(balance.spendable || 0),
      String(balance.offchain_outbound || 0),
      String(balance.offchain_inbound || 0),
    ]
  );
}

module.exports = {
  pool,
  query,
  ensureWallet,
  resolveWalletKey,
  upsertWalletAsset,
  upsertWalletAssetBalance,
};
