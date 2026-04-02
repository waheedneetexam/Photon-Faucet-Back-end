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

async function withTransaction(callback) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const result = await callback(client);
    await client.query('COMMIT');
    return result;
  } catch (error) {
    try {
      await client.query('ROLLBACK');
    } catch (rollbackError) {
      console.error('[RGB DB] Failed to rollback transaction:', rollbackError);
    }
    throw error;
  } finally {
    client.release();
  }
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
let _rgbNodesTableReady = false;
let _rgbAssetIssuancesTableReady = false;

async function ensureRgbNodesTable() {
  if (_rgbNodesTableReady) {
    return;
  }

  await query(`
    CREATE TABLE IF NOT EXISTS rgb_nodes (
      account_ref TEXT PRIMARY KEY,
      label TEXT NOT NULL,
      api_base TEXT NOT NULL,
      role TEXT NOT NULL,
      enabled BOOLEAN NOT NULL DEFAULT TRUE,
      sort_order INTEGER NOT NULL DEFAULT 0,
      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await query(`CREATE INDEX IF NOT EXISTS idx_rgb_nodes_enabled_sort ON rgb_nodes(enabled, sort_order, account_ref)`);
  _rgbNodesTableReady = true;
}

async function upsertRgbNode({
  accountRef,
  label,
  apiBase,
  role = 'user',
  enabled = true,
  sortOrder = 0,
  metadata = {},
}) {
  await ensureRgbNodesTable();
  const result = await query(
    `INSERT INTO rgb_nodes (
       account_ref, label, api_base, role, enabled, sort_order, metadata
     )
     VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
     ON CONFLICT (account_ref)
     DO UPDATE SET
       label = EXCLUDED.label,
       api_base = EXCLUDED.api_base,
       role = EXCLUDED.role,
       enabled = EXCLUDED.enabled,
       sort_order = EXCLUDED.sort_order,
       metadata = EXCLUDED.metadata,
       updated_at = NOW()
     RETURNING *`,
    [
      accountRef,
      label,
      apiBase,
      role,
      enabled,
      sortOrder,
      JSON.stringify(metadata || {}),
    ]
  );
  return result.rows[0] || null;
}

async function listRgbNodes({ enabledOnly = false } = {}) {
  await ensureRgbNodesTable();
  const result = await query(
    `SELECT *
     FROM rgb_nodes
     WHERE ($1::boolean = false OR enabled = true)
     ORDER BY sort_order ASC, account_ref ASC`,
    [enabledOnly]
  );
  return result.rows;
}

async function getRgbNodeByAccountRef(accountRef) {
  await ensureRgbNodesTable();
  const result = await query(
    `SELECT *
     FROM rgb_nodes
     WHERE account_ref = $1
     LIMIT 1`,
    [accountRef]
  );
  return result.rows[0] || null;
}

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

async function ensureRgbAssetIssuancesTable() {
  if (_rgbAssetIssuancesTableReady) {
    return;
  }

  await query(`
    CREATE TABLE IF NOT EXISTS rgb_asset_issuances (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      wallet_id UUID REFERENCES wallets(id) ON DELETE SET NULL,
      node_account_ref TEXT NOT NULL,
      network TEXT NOT NULL CHECK (network IN ('mainnet', 'testnet3', 'testnet4', 'regtest')),
      schema TEXT NOT NULL DEFAULT 'NIA',
      token_name TEXT NOT NULL,
      ticker TEXT,
      precision INTEGER NOT NULL DEFAULT 0,
      total_supply NUMERIC(30, 0) NOT NULL,
      contract_id TEXT,
      status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'issuing', 'issued', 'failed')),
      liquidity_percentage NUMERIC(5, 2),
      reserved_asset_amount NUMERIC(30, 0),
      requested_channel_btc_sats BIGINT,
      channel_bootstrap_mode TEXT,
      lifecycle_status TEXT NOT NULL DEFAULT 'issued_registry_only',
      primary_channel_id TEXT,
      last_error TEXT,
      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await query(`ALTER TABLE rgb_asset_issuances ADD COLUMN IF NOT EXISTS liquidity_percentage NUMERIC(5, 2)`);
  await query(`ALTER TABLE rgb_asset_issuances ADD COLUMN IF NOT EXISTS reserved_asset_amount NUMERIC(30, 0)`);
  await query(`ALTER TABLE rgb_asset_issuances ADD COLUMN IF NOT EXISTS requested_channel_btc_sats BIGINT`);
  await query(`ALTER TABLE rgb_asset_issuances ADD COLUMN IF NOT EXISTS channel_bootstrap_mode TEXT`);
  await query(`ALTER TABLE rgb_asset_issuances ADD COLUMN IF NOT EXISTS lifecycle_status TEXT NOT NULL DEFAULT 'issued_registry_only'`);
  await query(`ALTER TABLE rgb_asset_issuances ADD COLUMN IF NOT EXISTS primary_channel_id TEXT`);
  await query(`ALTER TABLE rgb_asset_issuances ADD COLUMN IF NOT EXISTS last_error TEXT`);

  await query(`CREATE INDEX IF NOT EXISTS idx_rgb_asset_issuances_wallet_created ON rgb_asset_issuances(wallet_id, created_at DESC)`);
  await query(`CREATE INDEX IF NOT EXISTS idx_rgb_asset_issuances_contract ON rgb_asset_issuances(contract_id)`);
  _rgbAssetIssuancesTableReady = true;
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

async function ensureBoardTicketStatusesTable() {
  await query(`
    CREATE TABLE IF NOT EXISTS board_ticket_statuses (
      ticket_id TEXT PRIMARY KEY,
      status TEXT NOT NULL CHECK (status IN ('todo', 'progress', 'review', 'done')),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
}

async function listBoardTicketStatuses() {
  await ensureBoardTicketStatusesTable();
  const result = await query(
    `SELECT ticket_id, status, updated_at
     FROM board_ticket_statuses
     ORDER BY ticket_id ASC`
  );
  return result.rows;
}

async function upsertBoardTicketStatus({ ticketId, status }) {
  await ensureBoardTicketStatusesTable();
  const result = await query(
    `INSERT INTO board_ticket_statuses (ticket_id, status, updated_at)
     VALUES ($1, $2, NOW())
     ON CONFLICT (ticket_id)
     DO UPDATE SET
       status = EXCLUDED.status,
       updated_at = NOW()
     RETURNING ticket_id, status, updated_at`,
    [ticketId, status]
  );
  return result.rows[0] || null;
}

async function deleteBoardTicketStatus(ticketId) {
  await ensureBoardTicketStatusesTable();
  const result = await query(
    `DELETE FROM board_ticket_statuses
     WHERE ticket_id = $1
     RETURNING ticket_id`,
    [ticketId]
  );
  return result.rows[0] || null;
}

async function ensureBoardTicketsTable() {
  await query(`
    CREATE TABLE IF NOT EXISTS board_tickets (
      ticket_id TEXT PRIMARY KEY,
      title TEXT NOT NULL,
      status TEXT NOT NULL CHECK (status IN ('todo', 'progress', 'review', 'done')),
      priority TEXT NOT NULL CHECK (priority IN ('high', 'medium', 'low')),
      category TEXT NOT NULL CHECK (category IN ('ui', 'android', 'node', 'token', 'research', 'backend', 'infra')),
      estimate TEXT NOT NULL DEFAULT '1d',
      assignee TEXT NOT NULL DEFAULT '—',
      desc_html TEXT NOT NULL,
      links JSONB NOT NULL DEFAULT '[]'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
}

async function listBoardTickets() {
  await ensureBoardTicketStatusesTable();
  await ensureBoardTicketsTable();
  const result = await query(
    `SELECT
       bt.ticket_id,
       bt.title,
       COALESCE(bts.status, bt.status) AS status,
       bt.priority,
       bt.category,
       bt.estimate,
       bt.assignee,
       bt.desc_html,
       bt.links,
       bt.created_at,
       GREATEST(bt.updated_at, COALESCE(bts.updated_at, bt.updated_at)) AS updated_at
     FROM board_tickets bt
     LEFT JOIN board_ticket_statuses bts
       ON bts.ticket_id = bt.ticket_id
     ORDER BY bt.ticket_id ASC`
  );
  return result.rows;
}

async function createBoardTicket({
  ticketId,
  title,
  status,
  priority,
  category,
  estimate,
  assignee,
  descHtml,
  links = [],
}) {
  await ensureBoardTicketsTable();
  const result = await query(
    `INSERT INTO board_tickets (
       ticket_id, title, status, priority, category, estimate, assignee, desc_html, links, created_at, updated_at
     )
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, NOW(), NOW())
     RETURNING
       ticket_id, title, status, priority, category, estimate, assignee, desc_html, links, created_at, updated_at`,
    [
      ticketId,
      title,
      status,
      priority,
      category,
      estimate,
      assignee,
      descHtml,
      JSON.stringify(Array.isArray(links) ? links : []),
    ]
  );
  return result.rows[0] || null;
}

async function updateBoardTicket({
  ticketId,
  title,
  status,
  priority,
  category,
  estimate,
  assignee,
  descHtml,
  links = [],
}) {
  await ensureBoardTicketsTable();
  const result = await query(
    `UPDATE board_tickets
     SET
       title = $2,
       status = $3,
       priority = $4,
       category = $5,
       estimate = $6,
       assignee = $7,
       desc_html = $8,
       links = $9::jsonb,
       updated_at = NOW()
     WHERE ticket_id = $1
     RETURNING
       ticket_id, title, status, priority, category, estimate, assignee, desc_html, links, created_at, updated_at`,
    [
      ticketId,
      title,
      status,
      priority,
      category,
      estimate,
      assignee,
      descHtml,
      JSON.stringify(Array.isArray(links) ? links : []),
    ]
  );
  return result.rows[0] || null;
}

async function deleteBoardTicket(ticketId) {
  await ensureBoardTicketsTable();
  const result = await query(
    `DELETE FROM board_tickets
     WHERE ticket_id = $1
     RETURNING ticket_id`,
    [ticketId]
  );
  return result.rows[0] || null;
}

module.exports = {
  pool,
  query,
  withTransaction,
  ensureWallet,
  resolveWalletKey,
  ensureRgbNodesTable,
  upsertRgbNode,
  listRgbNodes,
  getRgbNodeByAccountRef,
  ensureRgbAssetIssuancesTable,
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
  ensureBoardTicketStatusesTable,
  listBoardTicketStatuses,
  upsertBoardTicketStatus,
  deleteBoardTicketStatus,
  ensureBoardTicketsTable,
  listBoardTickets,
  createBoardTicket,
  updateBoardTicket,
  deleteBoardTicket,
};
