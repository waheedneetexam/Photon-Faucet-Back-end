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

async function ensureWallet(req, network = 'regtest', { generateFundingAddress = null } = {}) {
  const walletKey = resolveWalletKey(req);

  // Upsert the wallet row
  const result = await query(
    `INSERT INTO wallets (wallet_key, network, backend_profile_id, display_name)
     VALUES ($1, $2, $3, $4)
     ON CONFLICT (wallet_key)
     DO UPDATE SET
       network            = EXCLUDED.network,
       backend_profile_id = EXCLUDED.backend_profile_id,
       last_seen_at       = NOW()
     RETURNING id, wallet_key, network, rgb_account_ref,
               utxo_funding_address, main_btc_address`,
    [walletKey, network, 'photon-dev-regtest', walletKey]
  );
  const wallet = result.rows[0];

  // Generate and store a UTXO funding address the first time this wallet is seen
  // generateFundingAddress is an async fn (apiBase) => address string, supplied by server.js
  if (!wallet.utxo_funding_address && typeof generateFundingAddress === 'function') {
    try {
      const address = await generateFundingAddress(wallet);
      if (address) {
        await query(
          `UPDATE wallets
           SET utxo_funding_address = $2, updated_at = NOW()
           WHERE id = $1`,
          [wallet.id, address]
        );
        wallet.utxo_funding_address = address;
      }
    } catch (err) {
      // Non-fatal — wallet still works, funding address can be generated on next call
      console.warn(`[DB] Failed to generate utxo_funding_address for wallet ${wallet.wallet_key}:`, err.message);
    }
  }

  return wallet;
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

// ─── Wallet BTC address helpers ───────────────────────────────────────────────

async function setWalletFundingAddress({ walletId, utxoFundingAddress }) {
  const result = await query(
    `UPDATE wallets
     SET utxo_funding_address = $2
     WHERE id = $1
     RETURNING id, utxo_funding_address`,
    [walletId, utxoFundingAddress]
  );
  return result.rows[0];
}

async function setWalletMainBtcAddress({ walletId, mainBtcAddress }) {
  const result = await query(
    `UPDATE wallets
     SET main_btc_address = $2
     WHERE id = $1
     RETURNING id, main_btc_address`,
    [walletId, mainBtcAddress]
  );
  return result.rows[0];
}

async function getWalletAddresses(walletId) {
  const result = await query(
    `SELECT id, main_btc_address, utxo_funding_address
     FROM wallets
     WHERE id = $1`,
    [walletId]
  );
  return result.rows[0] || null;
}

// ─── UTXO slot helpers ─────────────────────────────────────────────────────────

async function upsertUtxoSlot({ walletId, outpoint, state = 'FREE', satsValue = null, nodeAccountRef = null, transferId = null, invoiceId = null }) {
  const result = await query(
    `INSERT INTO user_utxo_slots (
       wallet_id, outpoint, state, sats_value, node_account_ref,
       rgb_transfer_id, invoice_id
     )
     VALUES ($1, $2, $3, $4, $5, $6, $7)
     ON CONFLICT (wallet_id, outpoint)
     DO UPDATE SET
       state            = EXCLUDED.state,
       sats_value       = COALESCE(EXCLUDED.sats_value, user_utxo_slots.sats_value),
       node_account_ref = COALESCE(EXCLUDED.node_account_ref, user_utxo_slots.node_account_ref),
       rgb_transfer_id  = COALESCE(EXCLUDED.rgb_transfer_id, user_utxo_slots.rgb_transfer_id),
       invoice_id       = COALESCE(EXCLUDED.invoice_id, user_utxo_slots.invoice_id),
       updated_at       = NOW()
     RETURNING *`,
    [walletId, outpoint, state, satsValue, nodeAccountRef, transferId || null, invoiceId || null]
  );
  return result.rows[0];
}

async function getWalletUtxoSlots(walletId) {
  const result = await query(
    `SELECT
       s.id, s.outpoint, s.state, s.sats_value, s.node_account_ref,
       s.rgb_transfer_id, s.invoice_id, s.redeemed_txid,
       s.created_at, s.updated_at, s.redeemed_at
     FROM user_utxo_slots s
     WHERE s.wallet_id = $1
     ORDER BY s.created_at DESC`,
    [walletId]
  );
  return result.rows;
}

async function markSlotOccupied(outpoint, transferId = null) {
  const result = await query(
    `UPDATE user_utxo_slots
     SET state           = 'OCCUPIED',
         rgb_transfer_id = COALESCE($2, rgb_transfer_id),
         updated_at      = NOW()
     WHERE outpoint = $1
       AND state IN ('FREE', 'EMPTY')
     RETURNING *`,
    [outpoint, transferId || null]
  );
  return result.rows[0] || null;
}

async function markSlotEmpty(outpoint) {
  const result = await query(
    `UPDATE user_utxo_slots
     SET state      = 'EMPTY',
         updated_at = NOW()
     WHERE outpoint = $1
       AND state = 'OCCUPIED'
     RETURNING *`,
    [outpoint]
  );
  return result.rows[0] || null;
}

async function markSlotRedeemed(slotId, redeemedTxid = null) {
  const result = await query(
    `UPDATE user_utxo_slots
     SET state         = 'REDEEMED',
         redeemed_txid = $2,
         redeemed_at   = NOW(),
         updated_at    = NOW()
     WHERE id = $1
       AND state IN ('FREE', 'EMPTY')
     RETURNING *`,
    [slotId, redeemedTxid || null]
  );
  return result.rows[0] || null;
}

// ─── UTXO deposit request helpers ─────────────────────────────────────────────

async function createDepositRequest({ walletId, depositAddress, expectedSats = 33000, requiredConfirmations = 1, nodeAccountRef }) {
  const result = await query(
    `INSERT INTO utxo_deposit_requests (
       wallet_id, deposit_address, expected_sats,
       required_confirmations, node_account_ref
     )
     VALUES ($1, $2, $3, $4, $5)
     RETURNING *`,
    [walletId, depositAddress, expectedSats, requiredConfirmations, nodeAccountRef]
  );
  return result.rows[0];
}

async function getWalletDepositRequests(walletId) {
  const result = await query(
    `SELECT *
     FROM utxo_deposit_requests
     WHERE wallet_id = $1
     ORDER BY created_at DESC`,
    [walletId]
  );
  return result.rows;
}

// Returns the active (pending/confirming) deposit request for a wallet, if any
async function getActiveDepositRequestForWallet(walletId) {
  const result = await query(
    `SELECT *
     FROM utxo_deposit_requests
     WHERE wallet_id = $1
       AND status IN ('pending', 'confirming')
       AND expires_at > NOW()
     ORDER BY created_at DESC
     LIMIT 1`,
    [walletId]
  );
  return result.rows[0] || null;
}

// Returns all pending/confirming requests — used by the deposit watcher
async function getActiveDepositRequests() {
  const result = await query(
    `SELECT r.*, w.utxo_funding_address, w.wallet_key
     FROM utxo_deposit_requests r
     JOIN wallets w ON w.id = r.wallet_id
     WHERE r.status IN ('pending', 'confirming')
       AND r.expires_at > NOW()
     ORDER BY r.created_at ASC`
  );
  return result.rows;
}

async function updateDepositDetected({ id, depositTxid, receivedSats, confirmations = 0 }) {
  const result = await query(
    `UPDATE utxo_deposit_requests
     SET status       = 'confirming',
         deposit_txid = $2,
         received_sats = $3,
         confirmations = $4,
         detected_at  = COALESCE(detected_at, NOW()),
         updated_at   = NOW()
     WHERE id = $1
     RETURNING *`,
    [id, depositTxid, receivedSats, confirmations]
  );
  return result.rows[0] || null;
}

async function updateDepositConfirmations({ id, confirmations }) {
  const result = await query(
    `UPDATE utxo_deposit_requests
     SET confirmations = $2,
         updated_at    = NOW()
     WHERE id = $1
     RETURNING *`,
    [id, confirmations]
  );
  return result.rows[0] || null;
}

async function markDepositConfirmed({ id, utxoSlotId }) {
  const result = await query(
    `UPDATE utxo_deposit_requests
     SET status       = 'confirmed',
         utxo_slot_id = $2,
         confirmed_at = NOW(),
         updated_at   = NOW()
     WHERE id = $1
     RETURNING *`,
    [id, utxoSlotId || null]
  );
  return result.rows[0] || null;
}

async function markDepositFailed({ id, errorMessage }) {
  const result = await query(
    `UPDATE utxo_deposit_requests
     SET status        = 'failed',
         error_message = $2,
         updated_at    = NOW()
     WHERE id = $1
     RETURNING *`,
    [id, errorMessage]
  );
  return result.rows[0] || null;
}

async function expireStaleDeposits() {
  const result = await query(
    `UPDATE utxo_deposit_requests
     SET status     = 'expired',
         updated_at = NOW()
     WHERE status IN ('pending', 'confirming')
       AND expires_at <= NOW()
     RETURNING id, wallet_id`
  );
  return result.rows;
}

let _channelApplicationsTableReady = false;

async function ensureChannelApplicationsTable() {
  if (_channelApplicationsTableReady) {
    return;
  }

  await query(`
    CREATE TABLE IF NOT EXISTS channel_applications (
      id TEXT PRIMARY KEY,
      wallet_id UUID REFERENCES wallets(id) ON DELETE SET NULL,
      owner_wallet_address TEXT NOT NULL,
      owner_wallet_key TEXT,
      account_ref TEXT NOT NULL,
      peer_pubkey TEXT NOT NULL,
      btc_deposit_address TEXT NOT NULL,
      rgb_invoice_id TEXT,
      rgb_invoice TEXT,
      status TEXT NOT NULL DEFAULT 'pending_funding',
      btc_amount_sats BIGINT NOT NULL,
      rgb_asset_id TEXT NOT NULL,
      rgb_asset_amount BIGINT NOT NULL,
      tx_counter INTEGER NOT NULL DEFAULT 0,
      tx_threshold INTEGER NOT NULL DEFAULT 100,
      commission_rate_sats BIGINT NOT NULL DEFAULT 0,
      earned_fees_sats BIGINT NOT NULL DEFAULT 0,
      deposit_request_id UUID REFERENCES utxo_deposit_requests(id) ON DELETE SET NULL,
      channel_id TEXT,
      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await query(`CREATE INDEX IF NOT EXISTS idx_channel_applications_status ON channel_applications(status)`);
  await query(`CREATE INDEX IF NOT EXISTS idx_channel_applications_deposit_request_id ON channel_applications(deposit_request_id)`);
  _channelApplicationsTableReady = true;
}

async function createChannelApplication({
  id,
  walletId = null,
  ownerWalletAddress,
  ownerWalletKey = null,
  accountRef,
  peerPubkey,
  btcDepositAddress,
  rgbInvoiceId = null,
  rgbInvoice = null,
  status = 'pending_funding',
  btcAmountSats,
  rgbAssetId,
  rgbAssetAmount,
  txCounter = 0,
  txThreshold = 100,
  commissionRateSats = 0,
  earnedFeesSats = 0,
  depositRequestId = null,
  channelId = null,
  metadata = {},
}) {
  await ensureChannelApplicationsTable();
  const result = await query(
    `INSERT INTO channel_applications (
       id, wallet_id, owner_wallet_address, owner_wallet_key, account_ref, peer_pubkey,
       btc_deposit_address, rgb_invoice_id, rgb_invoice, status, btc_amount_sats,
       rgb_asset_id, rgb_asset_amount, tx_counter, tx_threshold, commission_rate_sats,
       earned_fees_sats, deposit_request_id, channel_id, metadata
     )
     VALUES (
       $1, $2, $3, $4, $5, $6,
       $7, $8, $9, $10, $11,
       $12, $13, $14, $15, $16,
       $17, $18, $19, $20::jsonb
     )
     RETURNING *`,
    [
      id,
      walletId,
      ownerWalletAddress,
      ownerWalletKey,
      accountRef,
      peerPubkey,
      btcDepositAddress,
      rgbInvoiceId,
      rgbInvoice,
      status,
      btcAmountSats,
      rgbAssetId,
      rgbAssetAmount,
      txCounter,
      txThreshold,
      commissionRateSats,
      earnedFeesSats,
      depositRequestId,
      channelId,
      JSON.stringify(metadata || {}),
    ]
  );
  return result.rows[0];
}

async function getChannelApplicationById(id) {
  await ensureChannelApplicationsTable();
  const result = await query(
    `SELECT a.*, r.status AS btc_deposit_status, r.deposit_txid, r.received_sats, r.confirmations, r.confirmed_at
     FROM channel_applications a
     LEFT JOIN utxo_deposit_requests r ON r.id = a.deposit_request_id
     WHERE a.id = $1
     LIMIT 1`,
    [id]
  );
  return result.rows[0] || null;
}

async function getChannelApplicationByInvoice(invoice) {
  await ensureChannelApplicationsTable();
  const result = await query(
    `SELECT a.*, r.status AS btc_deposit_status, r.deposit_txid, r.received_sats, r.confirmations, r.confirmed_at
     FROM channel_applications a
     LEFT JOIN utxo_deposit_requests r ON r.id = a.deposit_request_id
     WHERE a.rgb_invoice = $1
     LIMIT 1`,
    [invoice]
  );
  return result.rows[0] || null;
}

async function markChannelApplicationRgbFunded({
  id,
  paymentHash = null,
  paymentStatus = null,
  metadata = {},
}) {
  await ensureChannelApplicationsTable();
  const result = await query(
    `UPDATE channel_applications
     SET status = CASE WHEN status = 'channel_active' THEN status ELSE 'rgb_funded' END,
         metadata = COALESCE(metadata, '{}'::jsonb) || $2::jsonb,
         updated_at = NOW()
     WHERE id = $1
     RETURNING *`,
    [
      id,
      JSON.stringify({
        rgbPaymentHash: paymentHash,
        rgbPaymentStatus: paymentStatus,
        rgbFundedAt: new Date().toISOString(),
        ...(metadata || {}),
      }),
    ]
  );
  return result.rows[0] || null;
}

async function markChannelApplicationActive({
  id,
  channelId = null,
  metadata = {},
}) {
  await ensureChannelApplicationsTable();
  const result = await query(
    `UPDATE channel_applications
     SET status = 'channel_active',
         channel_id = COALESCE($2, channel_id),
         metadata = COALESCE(metadata, '{}'::jsonb) || $3::jsonb,
         updated_at = NOW()
     WHERE id = $1
     RETURNING *`,
    [
      id,
      channelId,
      JSON.stringify({
        activatedAt: new Date().toISOString(),
        ...(metadata || {}),
      }),
    ]
  );
  return result.rows[0] || null;
}

module.exports = {
  pool,
  query,
  ensureWallet,
  resolveWalletKey,
  upsertWalletAsset,
  upsertWalletAssetBalance,
  // wallet address helpers
  setWalletFundingAddress,
  setWalletMainBtcAddress,
  getWalletAddresses,
  // utxo slot helpers
  upsertUtxoSlot,
  getWalletUtxoSlots,
  markSlotOccupied,
  markSlotEmpty,
  markSlotRedeemed,
  // utxo deposit helpers
  createDepositRequest,
  getWalletDepositRequests,
  getActiveDepositRequestForWallet,
  getActiveDepositRequests,
  updateDepositDetected,
  updateDepositConfirmations,
  markDepositConfirmed,
  markDepositFailed,
  expireStaleDeposits,
  ensureChannelApplicationsTable,
  createChannelApplication,
  getChannelApplicationById,
  getChannelApplicationByInvoice,
  markChannelApplicationRgbFunded,
  markChannelApplicationActive,
};
