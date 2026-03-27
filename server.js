const http = require('http');
const fs = require('fs');
const fsp = require('fs/promises');
const path = require('path');
const {
  query,
  ensureWallet,
  upsertWalletAsset,
  upsertWalletAssetBalance,
} = require('./db');

const ROOT_DIR = __dirname;
const PUBLIC_DIR = path.join(ROOT_DIR, 'public');
const DATA_DIR = path.join(ROOT_DIR, 'data');
const CLAIMS_PATH = path.join(DATA_DIR, 'claims.json');
const ENV_PATH = path.join(ROOT_DIR, '.env');

function loadEnvFile(filePath) {
  if (!fs.existsSync(filePath)) {
    return;
  }

  const contents = fs.readFileSync(filePath, 'utf8');
  for (const line of contents.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) {
      continue;
    }

    const separatorIndex = trimmed.indexOf('=');
    if (separatorIndex === -1) {
      continue;
    }

    const key = trimmed.slice(0, separatorIndex).trim();
    if (!key || process.env[key] !== undefined) {
      continue;
    }

    let value = trimmed.slice(separatorIndex + 1).trim();
    if (
      (value.startsWith('"') && value.endsWith('"')) ||
      (value.startsWith("'") && value.endsWith("'"))
    ) {
      value = value.slice(1, -1);
    }

    process.env[key] = value;
  }
}

loadEnvFile(ENV_PATH);

const PORT = Number(process.env.PORT || 8788);
const RPC_HOST = process.env.BITCOIN_RPC_HOST || '127.0.0.1';
const RPC_PORT = Number(process.env.BITCOIN_RPC_PORT || 18443);
const RPC_PROTOCOL = process.env.BITCOIN_RPC_PROTOCOL || 'http';
const RPC_USER = process.env.BITCOIN_RPC_USER || 'user';
const RPC_PASSWORD = process.env.BITCOIN_RPC_PASSWORD || 'password';
const RPC_WALLET = process.env.BITCOIN_RPC_WALLET || 'photon_dev';
const RGB_NODE_API_BASE = process.env.RGB_NODE_API_BASE || 'http://127.0.0.1:3001';
const RGB_LIGHTNING_NODE_API_BASE = process.env.RGB_LIGHTNING_NODE_API_BASE || 'http://127.0.0.1:3002';
const RGB_PUBLIC_PROXY_ENDPOINT =
  process.env.RGB_PUBLIC_PROXY_ENDPOINT || 'rpcs://dev-proxy.photonbolt.xyz/json-rpc';
// Internal JSON-RPC base used by the backend for proxy queries (consignment.get / ack.get).
// Defaults to the local container; override in .env if the proxy is remote-only.
const RGB_PROXY_RPC_BASE =
  process.env.RGB_PROXY_RPC_BASE || 'http://127.0.0.1:3000/json-rpc';
const CONSIGNMENT_POLL_ATTEMPTS = Number(process.env.CONSIGNMENT_POLL_ATTEMPTS || 3);
const CONSIGNMENT_POLL_DELAY_MS = Number(process.env.CONSIGNMENT_POLL_DELAY_MS || 2000);
const RECEIVER_WATCHDOG_INTERVAL_MS = Number(process.env.RECEIVER_WATCHDOG_INTERVAL_MS || 30000);
const BINANCE_API_BASE = process.env.BINANCE_API_BASE || 'https://api.binance.com';
const BINANCE_API_KEY = process.env.BINANCE_API_KEY || '';
const DEFAULT_FAUCET_AMOUNT_BTC = process.env.FAUCET_AMOUNT_BTC || '0.5';
const ALLOWED_FAUCET_AMOUNTS_BTC = ['0.5', '1', '2'];
const COOLDOWN_MINUTES = Number(process.env.FAUCET_COOLDOWN_MINUTES || 15);
const AUTO_MINE_BLOCKS = Number(process.env.FAUCET_AUTO_MINE_BLOCKS || 1);
const MINING_ADDRESS_TYPE = process.env.FAUCET_MINING_ADDRESS_TYPE || 'bech32';

const CLAIMS_TTL_MS = COOLDOWN_MINUTES * 60 * 1000;
const MAX_BODY_BYTES = 8 * 1024;
const SATS_PER_BTC = 100000000;
const RGB_OWNER_WALLET_KEY = `dev-${RPC_WALLET}-regtest`;
const RGB_OWNER_ACCOUNT_REF = 'photon-rln-issuer';
const RGB_USER_ACCOUNT_REF = 'photon-rln-user';

const MIME_TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.ico': 'image/x-icon',
};

let claimState = { claims: [] };
let writeQueue = Promise.resolve();
let scanQueue = Promise.resolve();

function baseHeaders(extra = {}) {
  return {
    'Cache-Control': 'no-store',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, x-photon-wallet-key',
    'Referrer-Policy': 'no-referrer',
    'X-Content-Type-Options': 'nosniff',
    'X-Frame-Options': 'DENY',
    ...extra,
  };
}

function sendJson(res, statusCode, payload) {
  const body = JSON.stringify(payload);
  res.writeHead(statusCode, baseHeaders({ 'Content-Type': 'application/json; charset=utf-8' }));
  res.end(body);
}

function sendText(res, statusCode, text) {
  res.writeHead(statusCode, baseHeaders({ 'Content-Type': 'text/plain; charset=utf-8' }));
  res.end(text);
}

function nowIso() {
  return new Date().toISOString();
}

function getRemoteIp(req) {
  const forwarded = req.headers['x-forwarded-for'];
  if (typeof forwarded === 'string' && forwarded.trim()) {
    return forwarded.split(',')[0].trim();
  }
  return req.socket.remoteAddress || 'unknown';
}

function sanitizeAddress(value) {
  return typeof value === 'string' ? value.trim() : '';
}

function normalizeFaucetAmount(value) {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return String(value);
  }
  if (typeof value === 'string') {
    return value.trim();
  }
  return '';
}

function btcToSats(value) {
  return Math.round(Number(value || 0) * SATS_PER_BTC);
}

function decodeAddressFromPath(value) {
  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

function rewriteRgbInvoiceTransportEndpoint(invoice) {
  if (typeof invoice !== 'string' || !invoice.includes('endpoints=')) {
    return invoice;
  }

  const [base, query = ''] = invoice.split('?');
  const params = new URLSearchParams(query);
  if (!params.has('endpoints')) {
    return invoice;
  }

  params.set('endpoints', RGB_PUBLIC_PROXY_ENDPOINT);
  return `${base}?${params.toString()}`;
}

async function rgbNodeRequest(endpoint, payload = {}) {
  return rgbNodeRequestWithBase(RGB_NODE_API_BASE, endpoint, payload);
}

async function rgbNodeRequestWithBase(apiBase, endpoint, payload = {}, method = 'POST') {
  console.log(`[${nowIso()}] [RGB API] Forwarding request to RGB node`, {
    apiBase,
    endpoint,
    payload,
  });
  const headers = {};
  const options = { method, headers };

  if (method !== 'GET') {
    headers['Content-Type'] = 'application/json';
    options.body = JSON.stringify(payload);
  }

  const response = await fetch(`${apiBase}${endpoint}`, options);

  const raw = await response.text();
  let parsed = null;
  if (raw) {
    try {
      parsed = JSON.parse(raw);
    } catch {
      parsed = raw;
    }
  }

  if (!response.ok) {
    const message =
      (parsed && typeof parsed === 'object' && parsed.error) ||
      (parsed && typeof parsed === 'object' && parsed.message) ||
      raw ||
      `RGB node request failed with status ${response.status}`;
    throw new Error(String(message));
  }

  console.log(`[${nowIso()}] [RGB API] RGB node response received`, {
    apiBase,
    endpoint,
    ok: response.ok,
  });
  return parsed;
}

function getDefaultAccountRefForWallet(wallet) {
  return wallet.wallet_key === RGB_OWNER_WALLET_KEY ? RGB_OWNER_ACCOUNT_REF : RGB_USER_ACCOUNT_REF;
}

function resolveRgbNodeApiBaseForAccountRef(accountRef) {
  if (accountRef === RGB_USER_ACCOUNT_REF) {
    return RGB_LIGHTNING_NODE_API_BASE;
  }
  return RGB_NODE_API_BASE;
}

function resolveWalletNodeContext(wallet) {
  const accountRef = wallet.rgb_account_ref || getDefaultAccountRefForWallet(wallet);
  return {
    accountRef,
    apiBase: resolveRgbNodeApiBaseForAccountRef(accountRef),
  };
}

async function setWalletRgbAccountRef(walletId, accountRef) {
  await query(
    `
      UPDATE wallets
      SET
        rgb_account_ref = $2,
        updated_at = NOW()
      WHERE id = $1
    `,
    [walletId, accountRef]
  );
}

async function fetchWalletLightningAssetBalance(wallet, assetId) {
  if (!wallet.rgb_account_ref) {
    return null;
  }

  try {
    const apiBase = resolveRgbNodeApiBaseForAccountRef(wallet.rgb_account_ref);
    const balance = await rgbNodeRequestWithBase(apiBase, '/assetbalance', {
      asset_id: assetId,
    });

    if (!balance || typeof balance !== 'object') {
      return null;
    }

    return {
      settled: String(balance.settled ?? 0),
      future: String(balance.future ?? 0),
      spendable: String(balance.spendable ?? 0),
      offchain_outbound: String(balance.offchain_outbound ?? 0),
      offchain_inbound: String(balance.offchain_inbound ?? 0),
      locked_missing_secret: '0',
      locked_unconfirmed: '0',
      spendability_status: Number(balance.offchain_outbound || 0) > 0 ? 'lightning_ready' : 'spendable',
    };
  } catch (error) {
    console.warn(`[${nowIso()}] [RGB API] Live lightning balance lookup failed:`, error.message);
    return null;
  }
}

async function listNodePayments(apiBase) {
  const response = await rgbNodeRequestWithBase(apiBase, '/listpayments', {}, 'GET');
  return Array.isArray(response?.payments) ? response.payments : [];
}

async function upsertLightningPaymentTransfer({
  wallet,
  walletAssetId,
  payment,
  direction,
  invoice,
  settlementStatus = null,
}) {
  const paymentHash = payment?.payment_hash || null;
  const assetAmount =
    payment?.asset_amount !== undefined && payment?.asset_amount !== null
      ? String(payment.asset_amount)
      : null;
  const metadata = {
    payment_hash: paymentHash,
    payment_secret: payment?.payment_secret || null,
    payee_pubkey: payment?.payee_pubkey || null,
    route: 'lightning',
    invoice,
    amt_msat: payment?.amt_msat ?? null,
    inbound: payment?.inbound ?? null,
  };

  const existing = paymentHash
    ? await query(
      `
        SELECT id
        FROM rgb_transfers
        WHERE wallet_id = $1
          AND metadata->>'payment_hash' = $2
        LIMIT 1
      `,
      [wallet.id, paymentHash]
    )
    : { rows: [] };

  const params = [
    wallet.id,
    walletAssetId,
    direction,
    direction === 'incoming' ? 'LightningReceive' : 'LightningSend',
    payment?.status === 'Succeeded' ? 'Settled' : String(payment?.status || 'Pending'),
    null,
    null,
    null,
    null,
    paymentHash,
    'Fungible',
    assetAmount,
    null,
    payment?.status === 'Succeeded' ? assetAmount : null,
    payment?.status === 'Succeeded' ? new Date() : null,
    JSON.stringify(metadata),
    settlementStatus || null,
  ];

  if (existing.rows.length > 0) {
    await query(
      `
        UPDATE rgb_transfers
        SET
          wallet_asset_id = $2,
          direction = $3,
          transfer_kind = $4,
          status = $5,
          rgb_transfer_idx = $6,
          txid = $7,
          change_utxo = $8,
          receive_utxo = $9,
          recipient_id = $10,
          requested_assignment_type = $11,
          requested_assignment_value = $12,
          expiration_at = $13,
          settled_amount = $14,
          settled_at = $15,
          metadata = $16,
          settlement_status = $17::settlement_status_enum
        WHERE id = $1
      `,
      [existing.rows[0].id, ...params.slice(1)]
    );
    return existing.rows[0].id;
  }

  const inserted = await query(
    `
      INSERT INTO rgb_transfers (
        wallet_id,
        wallet_asset_id,
        direction,
        transfer_kind,
        status,
        rgb_transfer_idx,
        txid,
        change_utxo,
        receive_utxo,
        recipient_id,
        requested_assignment_type,
        requested_assignment_value,
        expiration_at,
        settled_amount,
        settled_at,
        metadata,
        settlement_status
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17::settlement_status_enum)
      RETURNING id
    `,
    params
  );

  return inserted.rows[0].id;
}

async function updateLightningTransferSettlementStatus(transferId, settlementStatus, payment = null) {
  if (payment) {
    const assetAmount =
      payment.asset_amount !== undefined && payment.asset_amount !== null
        ? String(payment.asset_amount)
        : null;
    const isSucceeded = payment.status === 'Succeeded';
    await query(
      `
        UPDATE rgb_transfers
        SET
          settlement_status = $1::settlement_status_enum,
          status            = $2,
          settled_amount    = $3,
          settled_at        = $4
        WHERE id = $5
      `,
      [
        settlementStatus,
        isSucceeded ? 'Settled' : String(payment.status || 'Pending'),
        isSucceeded ? assetAmount : null,
        isSucceeded ? new Date() : null,
        transferId,
      ]
    );
    return;
  }

  await query(
    `UPDATE rgb_transfers SET settlement_status = $1::settlement_status_enum WHERE id = $2`,
    [settlementStatus, transferId]
  );
}

// ---------------------------------------------------------------------------
// RGB Proxy JSON-RPC helpers
// ---------------------------------------------------------------------------

/**
 * Call a JSON-RPC 2.0 method on the RGB Proxy server.
 * Returns the `result` field on success; throws on JSON-RPC or HTTP error.
 */
async function proxyJsonRpc(method, params = {}) {
  const body = JSON.stringify({ jsonrpc: '2.0', id: '1', method, params });
  const response = await fetch(RGB_PROXY_RPC_BASE, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body,
  });

  const raw = await response.text();
  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new Error(`Proxy returned non-JSON response (${response.status}): ${raw.slice(0, 200)}`);
  }

  if (!response.ok || parsed.error) {
    const msg = parsed?.error?.message || parsed?.error || `Proxy HTTP ${response.status}`;
    const err = new Error(String(msg));
    err.statusCode = response.status;
    throw err;
  }

  return parsed.result;
}

/**
 * Ask the proxy whether a consignment for `recipientId` is present.
 * Returns `{ consignment: <base64>, txid }` on success, `null` if not found.
 */
async function proxyGetConsignment(recipientId) {
  try {
    const result = await proxyJsonRpc('consignment.get', { recipient_id: recipientId });
    return result || null;
  } catch (err) {
    // -400 == "Consignment file not found" in the proxy source
    if (err.message && err.message.includes('not found')) {
      return null;
    }
    throw err;
  }
}

/**
 * Ask the proxy for the ACK status of a consignment.
 * Returns `true` (ACK), `false` (NACK), or `undefined` (pending).
 */
async function proxyGetAck(recipientId) {
  try {
    return await proxyJsonRpc('ack.get', { recipient_id: recipientId });
  } catch (err) {
    if (err.message && err.message.includes('not found')) {
      return undefined;
    }
    throw err;
  }
}

/**
 * Upsert a consignment_records row to POSTED state for an outgoing lightning transfer.
 * Asset-agnostic — uses the assetId from the transfer context.
 */
async function recordLightningConsignmentPosted({
  walletId,
  transferId,
  recipientId,
  assetId,
  proxyData,
}) {
  const existing = await query(
    `SELECT id FROM consignment_records WHERE wallet_id = $1 AND recipient_id = $2 LIMIT 1`,
    [walletId, recipientId]
  );

  if (existing.rows.length > 0) {
    await query(
      `
        UPDATE consignment_records
        SET
          transfer_id    = COALESCE(transfer_id, $2),
          delivery_status = CASE
            WHEN delivery_status IN ('awaiting', 'posted') THEN 'posted'
            ELSE delivery_status
          END,
          proxy_endpoint = $3,
          metadata       = metadata || $4::jsonb,
          updated_at     = NOW()
        WHERE id = $1
      `,
      [
        existing.rows[0].id,
        transferId,
        RGB_PUBLIC_PROXY_ENDPOINT,
        JSON.stringify({ assetId, proxyTxid: proxyData?.txid || null, autoUploaded: true }),
      ]
    );
    return existing.rows[0].id;
  }

  const inserted = await query(
    `
      INSERT INTO consignment_records (
        wallet_id, transfer_id, recipient_id, proxy_endpoint,
        delivery_status, metadata
      )
      VALUES ($1, $2, $3, $4, 'posted', $5)
      RETURNING id
    `,
    [
      walletId,
      transferId,
      recipientId,
      RGB_PUBLIC_PROXY_ENDPOINT,
      JSON.stringify({ assetId, proxyTxid: proxyData?.txid || null, autoUploaded: true }),
    ]
  );
  return inserted.rows[0].id;
}

// ---------------------------------------------------------------------------
// Settlement orchestrator
// ---------------------------------------------------------------------------

/**
 * Background pipeline: runs after PAYMENT_SUCCESS.
 *
 * Stage 1 – Verify consignment was auto-uploaded by the node to the proxy.
 *           The rgb-lightning-node pushes the consignment to the transport
 *           endpoint embedded in the invoice during sendpayment.  We poll
 *           consignment.get to confirm arrival (CONSIGNMENT_POLL_ATTEMPTS
 *           retries, CONSIGNMENT_POLL_DELAY_MS apart).
 *
 * Stage 2 – Poll ack.get once; record result.
 *           Full ACK polling (SETTLED transition) is intentionally left as
 *           a recurring job — set up a cron/refresh endpoint to call
 *           pollPendingConsignmentAcks() on a schedule.
 *
 * On permanent failure → settlement_status = DELIVERY_FAILED.
 *
 * Asset-agnostic: uses assetId from the call site, not hard-coded.
 *
 * @param {{ transferId: string, wallet: object, assetId: string, payment: object, decoded: object|null, invoice: string|null }} ctx
 */
async function orchestrateConsignmentUpload({ transferId, wallet, assetId, payment, decoded = null, invoice = null }) {
  const tag = `[RGB Settlement] transferId=${transferId} assetId=${assetId}`;

  // Derive the blinded seal / recipient_id from the decoded invoice.
  // The rgb-lightning-node embeds this in the LN invoice as the RGB recipient.
  const recipientId =
    decoded?.recipient_id ||
    decoded?.blinded_utxo ||
    decoded?.blinded_seal ||
    payment?.recipient_id ||
    null;

  if (!recipientId) {
    console.warn(`${tag} — no recipient_id available; cannot verify proxy consignment. Marking DELIVERY_FAILED.`);
    await updateLightningTransferSettlementStatus(transferId, 'DELIVERY_FAILED');
    await recordTransferEvent({
      walletId: wallet.id,
      transferId,
      eventType: 'rgb_consignment_failed',
      eventSource: 'settlement_orchestrator',
      payload: { reason: 'missing_recipient_id', assetId },
    });
    return;
  }

  console.log(`${tag} — polling proxy for consignment (recipient_id=${recipientId})`);

  // --- Stage 1: verify consignment is present at the proxy ---
  let proxyData = null;
  let lastError = null;

  for (let attempt = 1; attempt <= CONSIGNMENT_POLL_ATTEMPTS; attempt++) {
    try {
      proxyData = await proxyGetConsignment(recipientId);
      if (proxyData) {
        console.log(`${tag} — consignment confirmed at proxy (attempt ${attempt})`);
        break;
      }
      console.log(`${tag} — consignment not yet at proxy (attempt ${attempt}/${CONSIGNMENT_POLL_ATTEMPTS})`);
    } catch (err) {
      lastError = err;
      console.warn(`${tag} — proxy query error (attempt ${attempt}): ${err.message}`);
    }

    if (attempt < CONSIGNMENT_POLL_ATTEMPTS) {
      await new Promise((resolve) => setTimeout(resolve, CONSIGNMENT_POLL_DELAY_MS * attempt));
    }
  }

  if (!proxyData) {
    const reason = lastError ? lastError.message : 'consignment_not_found_after_retries';
    console.error(`${tag} — consignment never arrived at proxy. Marking DELIVERY_FAILED. reason=${reason}`);
    await updateLightningTransferSettlementStatus(transferId, 'DELIVERY_FAILED');
    await recordTransferEvent({
      walletId: wallet.id,
      transferId,
      eventType: 'rgb_consignment_failed',
      eventSource: 'settlement_orchestrator',
      payload: { reason, recipientId, assetId, attempts: CONSIGNMENT_POLL_ATTEMPTS },
    });
    return;
  }

  // Consignment arrived — record it and advance to CONSIGNMENT_UPLOADED.
  const consignmentId = await recordLightningConsignmentPosted({
    walletId: wallet.id,
    transferId,
    recipientId,
    assetId,
    proxyData,
  });

  await updateLightningTransferSettlementStatus(transferId, 'CONSIGNMENT_UPLOADED');

  await recordTransferEvent({
    walletId: wallet.id,
    transferId,
    consignmentId,
    eventType: 'rgb_consignment_uploaded',
    eventSource: 'settlement_orchestrator',
    payload: { recipientId, assetId, txid: proxyData?.txid || null },
  });

  console.log(`${tag} — advanced to CONSIGNMENT_UPLOADED. Checking for immediate ACK...`);

  // --- Stage 2: check for immediate ACK from the receiver ---
  try {
    const ack = await proxyGetAck(recipientId);
    if (ack === true) {
      await updateLightningTransferSettlementStatus(transferId, 'SETTLED');
      await query(
        `UPDATE consignment_records SET delivery_status = 'acked', ack = TRUE, acked_at = NOW() WHERE id = $1`,
        [consignmentId]
      );
      await recordTransferEvent({
        walletId: wallet.id,
        transferId,
        consignmentId,
        eventType: 'rgb_transfer_settled',
        eventSource: 'settlement_orchestrator',
        payload: { recipientId, assetId },
      });
      console.log(`${tag} — receiver ACKed immediately. Advanced to SETTLED.`);
    } else if (ack === false) {
      // Receiver NACKed — treat as delivery failure.
      await updateLightningTransferSettlementStatus(transferId, 'DELIVERY_FAILED');
      await query(
        `UPDATE consignment_records SET delivery_status = 'nacked', ack = FALSE, acked_at = NOW() WHERE id = $1`,
        [consignmentId]
      );
      await recordTransferEvent({
        walletId: wallet.id,
        transferId,
        consignmentId,
        eventType: 'rgb_consignment_nacked',
        eventSource: 'settlement_orchestrator',
        payload: { recipientId, assetId },
      });
      console.warn(`${tag} — receiver NACKed consignment. Marked DELIVERY_FAILED.`);
    } else {
      // ack === undefined: receiver hasn't validated yet.
      // The transfer stays at CONSIGNMENT_UPLOADED until a refresh job confirms ACK.
      console.log(`${tag} — ACK pending; transfer remains at CONSIGNMENT_UPLOADED for refresh polling.`);
    }
  } catch (ackErr) {
    // Non-fatal: ACK check failed — transfer stays at CONSIGNMENT_UPLOADED.
    console.warn(`${tag} — ACK probe failed (non-fatal): ${ackErr.message}`);
  }
}

async function ensureRgbUtxos(apiBase = null) {
  const payload = {
    up_to: false,
    num: 4,
    size: 32500,
    fee_rate: 5,
    skip_sync: false,
  };
  console.log(`[${nowIso()}] [RGB API] Creating RGB UTXOs`, payload);
  return apiBase
    ? await rgbNodeRequestWithBase(apiBase, '/createutxos', payload)
    : await rgbNodeRequest('/createutxos', payload);
}

function extractInvoiceAssignment(invoice) {
  if (!invoice || typeof invoice !== 'object') {
    return { assignmentType: null, assignmentValue: null };
  }

  const assignment = invoice.assignment;
  if (!assignment || typeof assignment !== 'object') {
    return { assignmentType: null, assignmentValue: null };
  }

  return {
    assignmentType: assignment.type || null,
    assignmentValue:
      assignment.value !== undefined && assignment.value !== null ? String(assignment.value) : null,
  };
}

function getRequestNetwork(body) {
  const network = typeof body?.network === 'string' && body.network.trim() ? body.network.trim() : 'regtest';
  return network;
}

function normalizeTransferAmount(value) {
  if (value === undefined || value === null || value === '') {
    return '0';
  }

  return String(value);
}

async function getWalletInvoiceOwnership(walletId, assetId) {
  const [invoiceResult, outgoingSendResult] = await Promise.all([
    query(
      `
        SELECT
          recipient_id,
          batch_transfer_idx
        FROM rgb_invoices
        WHERE wallet_id = $1
          AND wallet_asset_id IN (
            SELECT id
            FROM wallet_assets
            WHERE wallet_id = $1 AND asset_id = $2
          )
      `,
      [walletId, assetId]
    ),
    query(
      `
        SELECT payload
        FROM transfer_events
        WHERE wallet_id = $1
          AND event_type = 'rgb_send_requested'
          AND (
            payload->>'assetId' = $2
            OR payload->>'contractId' = $2
          )
      `,
      [walletId, assetId]
    ),
  ]);

  return {
    recipientIds: new Set(invoiceResult.rows.map((row) => row.recipient_id).filter(Boolean)),
    batchTransferIdxs: new Set(
      invoiceResult.rows
        .map((row) => (row.batch_transfer_idx !== null ? Number(row.batch_transfer_idx) : null))
        .filter((value) => Number.isFinite(value))
    ),
    sentRecipientIds: new Set(
      outgoingSendResult.rows
        .map((row) => row.payload?.recipientId || null)
        .filter(Boolean)
    ),
  };
}

async function findWalletInvoiceByRecipient(walletId, recipientId, walletAssetId = null) {
  const params = [walletId, recipientId];
  let whereClause = 'wallet_id = $1 AND recipient_id = $2';

  if (walletAssetId) {
    params.push(walletAssetId);
    whereClause += ' AND wallet_asset_id = $3';
  }

  const result = await query(
    `
      SELECT id, wallet_asset_id, invoice_string, recipient_id, batch_transfer_idx, metadata, status
      FROM rgb_invoices
      WHERE ${whereClause}
      ORDER BY created_at DESC
      LIMIT 1
    `,
    params
  );

  return result.rows[0] || null;
}

async function findRgbTransferByIdx(assetId, transferIdx) {
  if (!assetId || !Number.isFinite(Number(transferIdx))) {
    return null;
  }

  const transferList = await rgbNodeRequest('/listtransfers', { asset_id: assetId });
  const transfers = Array.isArray(transferList?.transfers) ? transferList.transfers : [];
  return transfers.find((entry) => Number(entry.idx) === Number(transferIdx)) || null;
}

async function upsertPendingConsignmentSecret({
  walletId,
  walletAssetId,
  invoiceId = null,
  recipientId,
  blindingSecret = null,
  invoiceString = null,
  assetId = null,
  amount = null,
  network = 'regtest',
  source = 'wallet',
}) {
  const existing = await query(
    `
      SELECT id, metadata, delivery_status, ack
      FROM consignment_records
      WHERE wallet_id = $1 AND recipient_id = $2
      ORDER BY created_at DESC
      LIMIT 1
    `,
    [walletId, recipientId]
  );

  const secretStatus = blindingSecret ? 'active' : 'pending';
  const metadata = {
    ...(existing.rows[0]?.metadata || {}),
    assetId,
    amount: amount !== null ? String(amount) : null,
    network,
    invoiceString,
    registeredFrom: source,
    blindingSecretStatus: secretStatus,
  };

  if (existing.rows.length > 0) {
    await query(
      `
        UPDATE consignment_records
        SET
          wallet_id = $1,
          transfer_id = COALESCE(transfer_id, NULL),
          invoice_id = COALESCE($2, invoice_id),
          blinding_secret = COALESCE($3, blinding_secret),
          blinding_secret_status = CASE
            WHEN COALESCE($3, blinding_secret) IS NOT NULL AND COALESCE($3, blinding_secret) <> '' THEN 'active'
            ELSE $4
          END,
          delivery_status = CASE
            WHEN delivery_status = 'awaiting' THEN 'posted'
            ELSE delivery_status
          END,
          metadata = $5
        WHERE id = $6
      `,
      [walletId, invoiceId, blindingSecret, secretStatus, JSON.stringify(metadata), existing.rows[0].id]
    );
    return existing.rows[0].id;
  }

  const inserted = await query(
    `
      INSERT INTO consignment_records (
        wallet_id,
        invoice_id,
        recipient_id,
        proxy_endpoint,
        delivery_status,
        blinding_secret,
        blinding_secret_status,
        metadata
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
      RETURNING id
    `,
    [
      walletId,
      invoiceId,
      recipientId,
      RGB_PUBLIC_PROXY_ENDPOINT,
      blindingSecret ? 'posted' : 'awaiting',
      blindingSecret,
      secretStatus,
      JSON.stringify(metadata),
    ]
  );

  return inserted.rows[0].id;
}

async function registerWalletInvoiceSecret({
  req,
  network,
  assetId,
  amount,
  invoiceString,
  recipientId,
  blindingSecret,
  source,
}) {
  const wallet = await ensureWallet(req, network);
  let walletAsset = null;

  if (assetId) {
    const synced = await syncWalletAssetFromRgbNode({ walletId: wallet.id, assetId });
    walletAsset = synced.walletAsset;
  }

  await recordRgbInvoice({
    walletId: wallet.id,
    walletAssetId: walletAsset?.id || null,
    invoice: {
      invoice: invoiceString,
      recipient_id: recipientId,
      recipient_type: 'BlindSeal',
      assignment: amount !== null ? { type: 'Fungible', value: amount } : null,
      batch_transfer_idx: null,
      expiration_timestamp: null,
    },
    openAmount: amount === null,
    proxyEndpoint: RGB_PUBLIC_PROXY_ENDPOINT,
  });

  const invoice = await findWalletInvoiceByRecipient(wallet.id, recipientId, walletAsset?.id || null);
  const consignmentId = await upsertPendingConsignmentSecret({
    walletId: wallet.id,
    walletAssetId: walletAsset?.id || null,
    invoiceId: invoice?.id || null,
    recipientId,
    blindingSecret,
    invoiceString,
    assetId,
    amount,
    network,
    source,
  });

  return {
    wallet,
    walletAsset,
    invoiceId: invoice?.id || null,
    consignmentId,
  };
}

async function reconcileWalletConsignmentSecrets(wallet, walletAssetId, transfers) {
  for (const transfer of transfers) {
    if (!(transfer.kind && String(transfer.kind).startsWith('Receive')) || !transfer.recipient_id) {
      continue;
    }

    const invoice = await findWalletInvoiceByRecipient(wallet.id, transfer.recipient_id, walletAssetId);
    const existing = await query(
      `
        SELECT id, ack, delivery_status, metadata, blinding_secret
        FROM consignment_records
        WHERE wallet_id = $1 AND recipient_id = $2
        ORDER BY created_at DESC
        LIMIT 1
      `,
      [wallet.id, transfer.recipient_id]
    );

    const secret = existing.rows[0]?.blinding_secret || null;
    const secretStatus = secret ? 'active' : 'unavailable';
    const nextStatus =
      transfer.status === 'Settled'
        ? (existing.rows[0]?.ack ? 'acked' : 'validated')
        : (existing.rows[0]?.delivery_status || 'posted');

    const metadata = {
      ...(existing.rows[0]?.metadata || {}),
      transferIdx: transfer.idx || null,
      txid: transfer.txid || null,
      blindingSecretStatus: secretStatus,
    };

    if (existing.rows.length > 0) {
      await query(
        `
          UPDATE consignment_records
          SET
            transfer_id = $1,
            invoice_id = COALESCE($2, invoice_id),
            txid = COALESCE($3, txid),
            delivery_status = $4,
            validated_at = CASE WHEN $4 IN ('validated', 'acked') THEN COALESCE(validated_at, NOW()) ELSE validated_at END,
            blinding_secret_status = $5,
            metadata = $6
          WHERE id = $7
        `,
        [
          transfer._dbId || null,
          invoice?.id || null,
          transfer.txid || null,
          nextStatus,
          secretStatus,
          JSON.stringify(metadata),
          existing.rows[0].id,
        ]
      );
    } else {
      await query(
        `
          INSERT INTO consignment_records (
            wallet_id,
            transfer_id,
            invoice_id,
            recipient_id,
            txid,
            proxy_endpoint,
            delivery_status,
            validated_at,
            blinding_secret,
            blinding_secret_status,
            metadata
          )
          VALUES ($1, $2, $3, $4, $5, $6, $7, CASE WHEN $7 IN ('validated', 'acked') THEN NOW() ELSE NULL END, $8, $9, $10)
        `,
        [
          wallet.id,
          transfer._dbId || null,
          invoice?.id || null,
          transfer.recipient_id,
          transfer.txid || null,
          RGB_PUBLIC_PROXY_ENDPOINT,
          nextStatus,
          secret,
          secretStatus,
          JSON.stringify(metadata),
        ]
      );
    }

    if (invoice?.id && transfer.status === 'Settled') {
      await query(
        `
          UPDATE rgb_invoices
          SET
            status = CASE WHEN $2 = 'active' THEN 'settled' ELSE 'acknowledged' END,
            metadata = COALESCE(metadata, '{}'::jsonb) || $3::jsonb
          WHERE id = $1
        `,
        [
          invoice.id,
          secretStatus,
          JSON.stringify({ blindingSecretStatus: secretStatus, settledTxid: transfer.txid || null }),
        ]
      );
    }
  }
}

async function recordTransferEvent({
  walletId,
  transferId = null,
  invoiceId = null,
  consignmentId = null,
  eventType,
  eventSource,
  payload = {},
}) {
  await query(
    `
      INSERT INTO transfer_events (
        wallet_id,
        transfer_id,
        invoice_id,
        consignment_id,
        event_type,
        event_source,
        payload
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7)
    `,
    [
      walletId,
      transferId,
      invoiceId,
      consignmentId,
      eventType,
      eventSource,
      JSON.stringify(payload),
    ]
  );
}

async function getAnchorConfirmations(txid) {
  if (!txid) {
    return 0;
  }

  try {
    const transaction = await rpcRequest('getrawtransaction', [txid, true]);
    return Number(transaction?.confirmations || 0);
  } catch {
    return 0;
  }
}

function isTransferRelevantToWallet(transfer, wallet, ownership) {
  const kind = typeof transfer.kind === 'string' ? transfer.kind : '';
  const transferIdx = Number(transfer.idx);
  const recipientId = transfer.recipient_id || null;

  if (kind === 'Issuance') {
    return wallet.wallet_key === RGB_OWNER_WALLET_KEY;
  }

  if (kind.startsWith('Receive')) {
    return (
      (recipientId && ownership.recipientIds.has(recipientId)) ||
      (Number.isFinite(transferIdx) && ownership.batchTransferIdxs.has(transferIdx))
    );
  }

  if (kind === 'Send') {
    return (
      wallet.wallet_key === RGB_OWNER_WALLET_KEY ||
      (recipientId && ownership.sentRecipientIds.has(recipientId))
    );
  }

  return false;
}

async function deriveWalletScopedBalance(walletAssetId) {
  const result = await query(
    `
      SELECT
        t.id,
        t.direction,
        t.status,
        t.transfer_kind,
        t.txid,
        t.settlement_status,
        COALESCE(settled_amount, 0) AS settled_amount,
        COALESCE(requested_assignment_value, 0) AS requested_amount,
        EXISTS (
          SELECT 1
          FROM consignment_records c
          WHERE c.transfer_id = t.id
            AND c.blinding_secret IS NOT NULL
            AND c.blinding_secret <> ''
        ) AS has_blinding_secret
      FROM rgb_transfers t
      WHERE wallet_asset_id = $1
    `,
    [walletAssetId]
  );

  let settled = 0n;
  let offchainInbound = 0n;
  let offchainOutbound = 0n;
  let spendable = 0n;
  let lockedMissingSecret = 0n;
  let lockedUnconfirmed = 0n;
  const confirmationCache = new Map();

  for (const row of result.rows) {
    const status = row.status || 'Unknown';
    const direction = row.direction || 'incoming';
    const transferKind = row.transfer_kind || 'Unknown';
    const settlementStatus = row.settlement_status || null;
    const settledAmount = BigInt(normalizeTransferAmount(row.settled_amount));
    const requestedAmount = BigInt(normalizeTransferAmount(row.requested_amount));

    // Outgoing transfers in the active settlement pipeline (pre-final-handshake)
    // are counted as offchain_outbound and must NOT reduce the settled balance.
    // DELIVERY_FAILED is included so the UI can surface the stuck amount.
    if (
      direction === 'outgoing' &&
      (settlementStatus === 'INITIATED' ||
        settlementStatus === 'PAYMENT_SUCCESS' ||
        settlementStatus === 'CONSIGNMENT_UPLOADED' ||
        settlementStatus === 'DELIVERY_FAILED')
    ) {
      offchainOutbound += requestedAmount > 0n ? requestedAmount : settledAmount;
      continue;
    }

    // A transfer is only "truly settled" when the runtime reports Settled AND
    // either settlement_status is NULL (on-chain) or 'SETTLED' (lightning final handshake done).
    const isTrulySettled =
      status === 'Settled' &&
      (settlementStatus === null || settlementStatus === 'SETTLED');

    if (isTrulySettled) {
      if (direction === 'incoming') {
        settled += settledAmount;
        if (transferKind === 'Issuance') {
          spendable += settledAmount;
        } else {
          const txid = row.txid || null;
          const confirmations = txid
            ? (confirmationCache.has(txid)
              ? confirmationCache.get(txid)
              : await getAnchorConfirmations(txid))
            : 0;

          if (txid && !confirmationCache.has(txid)) {
            confirmationCache.set(txid, confirmations);
          }

          if (!row.has_blinding_secret) {
            lockedMissingSecret += settledAmount;
          } else if (confirmations < 6) {
            lockedUnconfirmed += settledAmount;
          } else {
            spendable += settledAmount;
          }
        }
      } else if (direction === 'outgoing') {
        const debit = requestedAmount > 0n ? requestedAmount : settledAmount;
        settled -= debit;
        spendable -= debit;
      }
      continue;
    }

    if (direction === 'incoming' && transferKind !== 'Issuance') {
      offchainInbound += requestedAmount;
    } else if (direction === 'outgoing') {
      offchainOutbound += requestedAmount;
    }
  }

  if (settled < 0n) {
    settled = 0n;
  }

  if (spendable < 0n) {
    spendable = 0n;
  }

  const future = settled + offchainInbound;
  const spendabilityStatus =
    lockedMissingSecret > 0n
      ? (spendable > 0n ? 'partially_locked_missing_secret' : 'locked_missing_secret')
      : lockedUnconfirmed > 0n
        ? (spendable > 0n ? 'partially_locked_unconfirmed' : 'locked_unconfirmed')
        : 'spendable';

  return {
    settled: settled.toString(),
    future: future.toString(),
    spendable: spendable.toString(),
    offchain_outbound: offchainOutbound.toString(),
    offchain_inbound: offchainInbound.toString(),
    locked_missing_secret: lockedMissingSecret.toString(),
    locked_unconfirmed: lockedUnconfirmed.toString(),
    spendability_status: spendabilityStatus,
  };
}

async function recordRgbInvoice({ walletId, walletAssetId, invoice, openAmount, proxyEndpoint }) {
  const { assignmentType, assignmentValue } = extractInvoiceAssignment(invoice);
  const expirationAt = invoice?.expiration_timestamp
    ? new Date(Number(invoice.expiration_timestamp) * 1000)
    : null;

  await query(
    `
      INSERT INTO rgb_invoices (
        wallet_id,
        wallet_asset_id,
        invoice_string,
        recipient_id,
        recipient_type,
        assignment_type,
        assignment_value,
        amount_open,
        batch_transfer_idx,
        proxy_endpoint,
        expiration_at,
        status,
        metadata
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'open', $12)
      ON CONFLICT (invoice_string)
      DO UPDATE SET
        wallet_id = EXCLUDED.wallet_id,
        wallet_asset_id = EXCLUDED.wallet_asset_id,
        recipient_id = EXCLUDED.recipient_id,
        recipient_type = EXCLUDED.recipient_type,
        assignment_type = EXCLUDED.assignment_type,
        assignment_value = EXCLUDED.assignment_value,
        amount_open = EXCLUDED.amount_open,
        batch_transfer_idx = EXCLUDED.batch_transfer_idx,
        proxy_endpoint = EXCLUDED.proxy_endpoint,
        expiration_at = EXCLUDED.expiration_at,
        metadata = EXCLUDED.metadata
    `,
    [
      walletId,
      walletAssetId || null,
      invoice.invoice,
      invoice.recipient_id,
      invoice.recipient_type || null,
      assignmentType,
      assignmentValue,
      openAmount,
      invoice.batch_transfer_idx || null,
      proxyEndpoint || null,
      expirationAt,
      JSON.stringify(invoice),
    ]
  );
}

async function syncWalletAssetFromRgbNode({ walletId, assetId }) {
  const assetList = await rgbNodeRequest('/listassets', { filter_asset_schemas: ['Nia', 'Uda', 'Cfa'] });

  const asset =
    assetList?.nia?.find((entry) => entry.asset_id === assetId) ||
    assetList?.uda?.find((entry) => entry.asset_id === assetId) ||
    assetList?.cfa?.find((entry) => entry.asset_id === assetId);

  if (!asset) {
    throw new Error(`Asset ${assetId} was not found in the RGB wallet`);
  }

  const walletAsset = await upsertWalletAsset({
    walletId,
    assetId: asset.asset_id,
    assetSchema: asset.asset_schema || 'Nia',
    contractId: asset.asset_id,
    name: asset.name,
    ticker: asset.ticker || null,
    precision: Number(asset.precision || 0),
  });

  return {
    walletAsset,
    asset,
  };
}

async function syncWalletTransferRows(wallet, assetId, walletAssetId) {
  const { apiBase: walletApiBase } = resolveWalletNodeContext(wallet);
  const transferList = await rgbNodeRequestWithBase(walletApiBase, '/listtransfers', { asset_id: assetId });
  const transfers = Array.isArray(transferList?.transfers) ? transferList.transfers : [];
  const ownership = await getWalletInvoiceOwnership(wallet.id, assetId);
  const relevantTransfers = transfers.filter((transfer) => isTransferRelevantToWallet(transfer, wallet, ownership));
  const relevantTransferIdxs = relevantTransfers
    .map((transfer) => Number(transfer.idx))
    .filter((value) => Number.isFinite(value));

  for (const transfer of relevantTransfers) {
    const requestedAssignment = transfer.requested_assignment || {};
    const expirationAt = transfer.expiration ? new Date(Number(transfer.expiration) * 1000) : null;
    const settledAt = transfer.status === 'Settled' ? new Date() : null;

    const params = [
      wallet.id,
      walletAssetId || null,
      transfer.kind === 'Issuance' || (transfer.kind && String(transfer.kind).startsWith('Receive'))
        ? 'incoming'
        : 'outgoing',
      transfer.kind || 'Unknown',
      transfer.status || 'Unknown',
      transfer.idx || null,
      transfer.txid || null,
      transfer.change_utxo || null,
      transfer.receive_utxo || null,
      transfer.recipient_id || null,
      requestedAssignment.type || null,
      requestedAssignment.value !== undefined && requestedAssignment.value !== null
        ? String(requestedAssignment.value)
        : null,
      expirationAt,
      Array.isArray(transfer.assignments) && transfer.assignments.length > 0 && transfer.assignments[0].value !== undefined
        ? String(transfer.assignments[0].value)
        : null,
      settledAt,
      JSON.stringify(transfer),
    ];

    const existing = transfer.idx
      ? await query(
        `SELECT id FROM rgb_transfers WHERE wallet_id = $1 AND rgb_transfer_idx = $2 LIMIT 1`,
        [wallet.id, transfer.idx]
      )
      : { rows: [] };

    if (existing.rows.length > 0) {
      await query(
        `
          UPDATE rgb_transfers
          SET
            wallet_asset_id = $1,
            direction = $2,
            transfer_kind = $3,
            status = $4,
            rgb_transfer_idx = $5,
            txid = $6,
            change_utxo = $7,
            receive_utxo = $8,
            recipient_id = $9,
            requested_assignment_type = $10,
            requested_assignment_value = $11,
            expiration_at = $12,
            settled_amount = $13,
            settled_at = $14,
            metadata = $15
          WHERE id = $16
        `,
        [...params.slice(1), existing.rows[0].id]
      );
      transfer._dbId = existing.rows[0].id;
    } else {
      const inserted = await query(
        `
          INSERT INTO rgb_transfers (
            wallet_id,
            wallet_asset_id,
            direction,
            transfer_kind,
            status,
            rgb_transfer_idx,
            txid,
            change_utxo,
            receive_utxo,
            recipient_id,
            requested_assignment_type,
            requested_assignment_value,
            expiration_at,
            settled_amount,
            settled_at,
            metadata
          )
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
          RETURNING id
        `,
        params
      );
      transfer._dbId = inserted.rows[0].id;
    }
  }

  if (relevantTransferIdxs.length > 0) {
    await query(
      `
        DELETE FROM rgb_transfers
        WHERE wallet_id = $1
          AND wallet_asset_id = $2
          AND rgb_transfer_idx IS NOT NULL
          AND NOT (rgb_transfer_idx = ANY($3::bigint[]))
      `,
      [wallet.id, walletAssetId, relevantTransferIdxs]
    );
  } else {
    await query(
      `
        DELETE FROM rgb_transfers
        WHERE wallet_id = $1
          AND wallet_asset_id = $2
      `,
      [wallet.id, walletAssetId]
    );
  }

  return relevantTransfers;
}

async function getStoredWalletTransfers(walletId, walletAssetId) {
  const result = await query(
    `
      SELECT
        id,
        direction,
        transfer_kind,
        status,
        created_at,
        updated_at,
        settled_at,
        rgb_transfer_idx,
        txid,
        change_utxo,
        receive_utxo,
        recipient_id,
        requested_assignment_type,
        requested_assignment_value,
        settled_amount,
        metadata
      FROM rgb_transfers
      WHERE wallet_id = $1
        AND wallet_asset_id = $2
      ORDER BY COALESCE(settled_at, updated_at, created_at) DESC, created_at DESC
    `,
    [walletId, walletAssetId]
  );

  return result.rows.map((row) => {
    const assignments =
      row.settled_amount !== null && row.settled_amount !== undefined
        ? [{ type: row.requested_assignment_type || 'Fungible', value: String(row.settled_amount) }]
        : [];

    const requestedAssignment =
      row.requested_assignment_value !== null && row.requested_assignment_value !== undefined
        ? {
          type: row.requested_assignment_type || 'Fungible',
          value: String(row.requested_assignment_value),
        }
        : null;

    return {
      idx: row.rgb_transfer_idx !== null ? Number(row.rgb_transfer_idx) : null,
      status: row.status,
      kind: row.transfer_kind,
      created_at: row.created_at ? row.created_at.toISOString() : null,
      updated_at: row.updated_at ? row.updated_at.toISOString() : null,
      settled_at: row.settled_at ? row.settled_at.toISOString() : null,
      txid: row.txid,
      recipient_id: row.recipient_id,
      receive_utxo: row.receive_utxo,
      change_utxo: row.change_utxo,
      assignments,
      requested_assignment: requestedAssignment,
      direction: row.direction,
      metadata: row.metadata || {},
      _dbId: row.id,
    };
  });
}

async function handleRgbBalance(req, res) {
  let body;
  try {
    body = await readRequestJson(req);
  } catch (error) {
    sendJson(res, 400, { ok: false, error: error.message });
    return;
  }

  const assetId = typeof body.assetId === 'string' && body.assetId.trim() ? body.assetId.trim() : null;
  if (!assetId) {
    sendJson(res, 400, { ok: false, error: 'assetId is required.' });
    return;
  }

  try {
    const wallet = await ensureWallet(req, 'regtest');
    const synced = await syncWalletAssetFromRgbNode({ walletId: wallet.id, assetId });
    const transfers = await syncWalletTransferRows(wallet, assetId, synced.walletAsset.id);
    await reconcileWalletConsignmentSecrets(wallet, synced.walletAsset.id, transfers);
    const derivedBalance = await deriveWalletScopedBalance(synced.walletAsset.id);
    const balance = derivedBalance;
    await upsertWalletAssetBalance(synced.walletAsset.id, balance);
    sendJson(res, 200, {
      ok: true,
      walletKey: wallet.wallet_key,
      asset: {
        assetId: synced.asset.asset_id,
        ticker: synced.asset.ticker || null,
        name: synced.asset.name,
        precision: synced.asset.precision,
      },
      balance,
    });
  } catch (error) {
    console.error(`[${nowIso()}] [RGB API] Balance lookup failed:`, error.message);
    sendJson(res, 502, { ok: false, error: error.message });
  }
}

async function handleRgbDecodeInvoice(req, res) {
  let body;
  try {
    body = await readRequestJson(req);
  } catch (error) {
    sendJson(res, 400, { ok: false, error: error.message });
    return;
  }

  const invoice = typeof body.invoice === 'string' && body.invoice.trim() ? body.invoice.trim() : null;
  if (!invoice) {
    sendJson(res, 400, { ok: false, error: 'invoice is required.' });
    return;
  }

  try {
    const decoded = await rgbNodeRequest('/decodergbinvoice', { invoice });
    sendJson(res, 200, {
      ok: true,
      decoded,
    });
  } catch (error) {
    console.error(`[${nowIso()}] [RGB API] Decode invoice failed:`, error.message);
    sendJson(res, 502, { ok: false, error: error.message });
  }
}

async function handleRgbDecodeLightningInvoice(req, res) {
  let body;
  try {
    body = await readRequestJson(req);
  } catch (error) {
    sendJson(res, 400, { ok: false, error: error.message });
    return;
  }

  const invoice = typeof body.invoice === 'string' && body.invoice.trim() ? body.invoice.trim() : null;
  if (!invoice) {
    sendJson(res, 400, { ok: false, error: 'invoice is required.' });
    return;
  }

  try {
    const wallet = await ensureWallet(req, 'regtest');
    const { apiBase } = resolveWalletNodeContext(wallet);
    const decoded = await rgbNodeRequestWithBase(apiBase, '/decodelninvoice', { invoice });
    sendJson(res, 200, {
      ok: true,
      walletKey: wallet.wallet_key,
      decoded,
    });
  } catch (error) {
    console.error(`[${nowIso()}] [RGB API] Decode lightning invoice failed:`, error.message);
    sendJson(res, 502, { ok: false, error: error.message });
  }
}

async function handleRgbLightningInvoice(req, res) {
  let body;
  try {
    body = await readRequestJson(req);
  } catch (error) {
    sendJson(res, 400, { ok: false, error: error.message });
    return;
  }

  const assetId = typeof body.assetId === 'string' && body.assetId.trim() ? body.assetId.trim() : null;
  const assetAmount = Number.isFinite(Number(body.amount)) ? Math.max(1, Math.trunc(Number(body.amount))) : 0;
  const expirySec = Number.isFinite(Number(body.expirySec)) ? Math.max(60, Math.trunc(Number(body.expirySec))) : 420;
  const amtMsat = Number.isFinite(Number(body.amtMsat)) ? Math.max(3000000, Math.trunc(Number(body.amtMsat))) : 3000000;

  if (!assetId) {
    sendJson(res, 400, { ok: false, error: 'assetId is required.' });
    return;
  }

  if (!assetAmount) {
    sendJson(res, 400, { ok: false, error: 'amount is required.' });
    return;
  }

  try {
    const wallet = await ensureWallet(req, 'regtest');
    await setWalletRgbAccountRef(wallet.id, RGB_USER_ACCOUNT_REF);
    wallet.rgb_account_ref = RGB_USER_ACCOUNT_REF;

    const { apiBase } = resolveWalletNodeContext(wallet);
    const invoiceResponse = await rgbNodeRequestWithBase(apiBase, '/lninvoice', {
      expiry_sec: expirySec,
      amt_msat: amtMsat,
      asset_id: assetId,
      asset_amount: assetAmount,
    });

    const decoded = await rgbNodeRequestWithBase(apiBase, '/decodelninvoice', {
      invoice: invoiceResponse.invoice,
    });

    sendJson(res, 200, {
      ok: true,
      walletKey: wallet.wallet_key,
      invoice: invoiceResponse.invoice,
      decoded,
    });
  } catch (error) {
    console.error(`[${nowIso()}] [RGB API] Lightning invoice generation failed:`, error.message);
    sendJson(res, 502, { ok: false, error: error.message });
  }
}

async function handleRgbTransfers(req, res) {
  let body;
  try {
    body = await readRequestJson(req);
  } catch (error) {
    sendJson(res, 400, { ok: false, error: error.message });
    return;
  }

  const assetId = typeof body.assetId === 'string' && body.assetId.trim() ? body.assetId.trim() : null;
  if (!assetId) {
    sendJson(res, 400, { ok: false, error: 'assetId is required.' });
    return;
  }

  try {
    const wallet = await ensureWallet(req, 'regtest');
    const synced = await syncWalletAssetFromRgbNode({ walletId: wallet.id, assetId });
    const runtimeTransfers = await syncWalletTransferRows(wallet, assetId, synced.walletAsset.id);
    await reconcileWalletConsignmentSecrets(wallet, synced.walletAsset.id, runtimeTransfers);
    const derivedBalance = await deriveWalletScopedBalance(synced.walletAsset.id);
    const balance = derivedBalance;
    await upsertWalletAssetBalance(synced.walletAsset.id, balance);
    const transfers = await getStoredWalletTransfers(wallet.id, synced.walletAsset.id);
    sendJson(res, 200, {
      ok: true,
      walletKey: wallet.wallet_key,
      assetId,
      balance,
      transfers,
    });
  } catch (error) {
    console.error(`[${nowIso()}] [RGB API] Transfer lookup failed:`, error.message);
    sendJson(res, 502, { ok: false, error: error.message });
  }
}

async function handleRgbSend(req, res) {
  let body;
  try {
    body = await readRequestJson(req);
  } catch (error) {
    sendJson(res, 400, { ok: false, error: error.message });
    return;
  }

  const invoice = typeof body.invoice === 'string' && body.invoice.trim() ? body.invoice.trim() : null;
  if (!invoice) {
    sendJson(res, 400, { ok: false, error: 'invoice is required.' });
    return;
  }

  const feeRate = Number.isFinite(Number(body.feeRate)) ? Math.max(1, Math.trunc(Number(body.feeRate))) : 5;
  const minConfirmations = Number.isFinite(Number(body.minConfirmations))
    ? Math.max(1, Math.trunc(Number(body.minConfirmations)))
    : 1;

  try {
    const wallet = await ensureWallet(req, 'regtest');
    const { apiBase: walletApiBase } = resolveWalletNodeContext(wallet);
    const decoded = await rgbNodeRequestWithBase(walletApiBase, '/decodergbinvoice', { invoice });
    const assetId = typeof decoded?.asset_id === 'string' ? decoded.asset_id : null;
    const recipientId = typeof decoded?.recipient_id === 'string' ? decoded.recipient_id : null;
    const amount = decoded?.assignment?.value !== undefined && decoded?.assignment?.value !== null
      ? Number(decoded.assignment.value)
      : null;
    const endpoint = Array.isArray(decoded?.transport_endpoints) ? decoded.transport_endpoints[0] : null;

    if (!assetId || !recipientId || !amount || !endpoint) {
      throw new Error('Decoded RGB invoice is missing required send fields.');
    }

    const synced = await syncWalletAssetFromRgbNode({ walletId: wallet.id, assetId });

    await recordTransferEvent({
      walletId: wallet.id,
      eventType: 'rgb_send_requested',
      eventSource: 'wallet_api',
      payload: {
        assetId,
        contractId: assetId,
        recipientId,
        amount: String(amount),
        invoice,
        feeRate,
        minConfirmations,
      },
    });

    const sendResult = await rgbNodeRequestWithBase(walletApiBase, '/sendrgb', {
      donation: false,
      fee_rate: feeRate,
      min_confirmations: minConfirmations,
      recipient_map: {
        [assetId]: [
          {
            recipient_id: recipientId,
            assignment: {
              type: 'Fungible',
              value: amount,
            },
            transport_endpoints: [endpoint],
          },
        ],
      },
      skip_sync: false,
    });

    const txid = sendResult?.txid || null;
    const transfers = await syncWalletTransferRows(wallet, assetId, synced.walletAsset.id);
    await reconcileWalletConsignmentSecrets(wallet, synced.walletAsset.id, transfers);
    const derivedBalance = await deriveWalletScopedBalance(synced.walletAsset.id);
    const balance = derivedBalance;
    await upsertWalletAssetBalance(synced.walletAsset.id, balance);

    const transferRow = transfers.find((transfer) =>
      transfer.kind === 'Send' &&
      transfer.recipient_id === recipientId &&
      (!txid || transfer.txid === txid || transfer.txid === null)
    ) || null;

    await recordTransferEvent({
      walletId: wallet.id,
      transferId: transferRow?._dbId || null,
      eventType: 'rgb_send_broadcast',
      eventSource: 'wallet_api',
      payload: {
        assetId,
        recipientId,
        amount: String(amount),
        txid,
        invoice,
      },
    });

    sendJson(res, 200, {
      ok: true,
      walletKey: wallet.wallet_key,
      assetId,
      txid,
      decoded,
      balance,
      transfer: transferRow || null,
    });
  } catch (error) {
    console.error(`[${nowIso()}] [RGB API] Send failed:`, error.message);
    sendJson(res, 502, { ok: false, error: error.message });
  }
}

async function handleRgbRefresh(req, res) {
  let body;
  try {
    body = await readRequestJson(req);
  } catch (error) {
    sendJson(res, 400, { ok: false, error: error.message });
    return;
  }

  const assetId = typeof body.assetId === 'string' && body.assetId.trim() ? body.assetId.trim() : null;
  if (!assetId) {
    sendJson(res, 400, { ok: false, error: 'assetId is required.' });
    return;
  }

  try {
    const wallet = await ensureWallet(req, 'regtest');
    const { apiBase: walletApiBase } = resolveWalletNodeContext(wallet);
    await rgbNodeRequestWithBase(walletApiBase, '/refreshtransfers', { skip_sync: false });
    const synced = await syncWalletAssetFromRgbNode({ walletId: wallet.id, assetId });
    const runtimeTransfers = await syncWalletTransferRows(wallet, assetId, synced.walletAsset.id);
    await reconcileWalletConsignmentSecrets(wallet, synced.walletAsset.id, runtimeTransfers);
    const derivedBalance = await deriveWalletScopedBalance(synced.walletAsset.id);
    const balance = derivedBalance;
    await upsertWalletAssetBalance(synced.walletAsset.id, balance);
    const transfers = await getStoredWalletTransfers(wallet.id, synced.walletAsset.id);
    sendJson(res, 200, {
      ok: true,
      walletKey: wallet.wallet_key,
      assetId,
      balance,
      transfers,
    });
  } catch (error) {
    console.error(`[${nowIso()}] [RGB API] Refresh failed:`, error.message);
    sendJson(res, 502, { ok: false, error: error.message });
  }
}

async function handleRgbPayLightning(req, res) {
  let body;
  try {
    body = await readRequestJson(req);
  } catch (error) {
    sendJson(res, 400, { ok: false, error: error.message });
    return;
  }

  const invoice = typeof body.invoice === 'string' && body.invoice.trim() ? body.invoice.trim() : null;
  if (!invoice) {
    sendJson(res, 400, { ok: false, error: 'invoice is required.' });
    return;
  }

  try {
    const wallet = await ensureWallet(req, 'regtest');
    const { accountRef, apiBase } = resolveWalletNodeContext(wallet);
    const decoded = await rgbNodeRequestWithBase(apiBase, '/decodelninvoice', { invoice });
    const assetId = typeof decoded?.asset_id === 'string' ? decoded.asset_id : null;

    if (!assetId) {
      throw new Error('Lightning invoice is missing an RGB asset id.');
    }

    const synced = await syncWalletAssetFromRgbNode({ walletId: wallet.id, assetId });

    // Stage 1: INITIATED — create transfer row before touching the node
    const transferId = await upsertLightningPaymentTransfer({
      wallet,
      walletAssetId: synced.walletAsset.id,
      payment: { ...decoded, status: 'Pending' },
      direction: 'outgoing',
      invoice,
      settlementStatus: 'INITIATED',
    });

    // Stage 2: execute Lightning payment at the node
    const paymentResult = await rgbNodeRequestWithBase(apiBase, '/sendpayment', { invoice });
    const payments = await listNodePayments(apiBase);
    const payment = payments.find((entry) => entry.payment_hash === paymentResult?.payment_hash) || {
      ...decoded,
      ...paymentResult,
      status: paymentResult?.status || 'Pending',
      inbound: false,
    };

    // Stage 3: PAYMENT_SUCCESS — pre-image received; update row status and settlement_status
    await updateLightningTransferSettlementStatus(transferId, 'PAYMENT_SUCCESS', payment);

    await recordTransferEvent({
      walletId: wallet.id,
      transferId,
      eventType: 'rgb_lightning_payment',
      eventSource: 'wallet_api',
      payload: {
        assetId,
        paymentHash: payment.payment_hash || null,
        amtMsat: payment.amt_msat ?? null,
        assetAmount: payment.asset_amount ?? null,
      },
    });

    // Stage 4: trigger async consignment verification/ACK pipeline (non-blocking)
    setImmediate(() => {
      orchestrateConsignmentUpload({ transferId, wallet, assetId, payment, decoded, invoice }).catch((err) =>
        console.error(`[${nowIso()}] [RGB Settlement] Consignment orchestration error:`, err.message)
      );
    });

    await setWalletRgbAccountRef(wallet.id, accountRef);
    wallet.rgb_account_ref = accountRef;

    const derivedBalance = await deriveWalletScopedBalance(synced.walletAsset.id);
    const balance = derivedBalance;
    await upsertWalletAssetBalance(synced.walletAsset.id, balance);

    sendJson(res, 200, {
      ok: true,
      walletKey: wallet.wallet_key,
      assetId,
      balance,
      payment: {
        payment_hash: payment.payment_hash || paymentResult?.payment_hash || null,
        status: payment.status || paymentResult?.status || 'Pending',
        asset_amount: payment.asset_amount ?? decoded?.asset_amount ?? null,
        amt_msat: payment.amt_msat ?? decoded?.amt_msat ?? null,
      },
      decoded,
    });
  } catch (error) {
    console.error(`[${nowIso()}] [RGB API] Lightning pay failed:`, error.message);
    sendJson(res, 502, { ok: false, error: error.message });
  }
}

/**
 * GET /api/rgb/settlement-status?assetId=<id>
 *
 * Returns all transfers for the calling wallet that are in a non-terminal
 * settlement state so the UI can show real-time settlement progress without
 * WebSockets.  The wallet polls this endpoint on a short interval and
 * transitions from "Waiting for Proof" → "Payment Received" when a row
 * reaches settlement_status = 'SETTLED'.
 */
async function handleRgbSettlementStatus(req, res, parsedUrl) {
  try {
    const wallet = await ensureWallet(req, 'regtest');
    const assetId = parsedUrl.searchParams.get('assetId') || null;

    const params = [wallet.id];
    let assetFilter = '';
    if (assetId) {
      params.push(assetId);
      assetFilter = `AND wa.asset_id = $${params.length}`;
    }

    const result = await query(
      `
        SELECT
          t.id,
          t.direction,
          t.transfer_kind,
          t.status,
          t.settlement_status,
          t.requested_assignment_value,
          t.settled_amount,
          t.settled_at,
          t.updated_at,
          wa.asset_id,
          wa.ticker,
          wa.name
        FROM rgb_transfers t
        JOIN wallet_assets wa ON wa.id = t.wallet_asset_id
        WHERE t.wallet_id = $1
          ${assetFilter}
          AND (
            t.settlement_status IS NOT NULL
            OR t.status IN ('WaitingCounterparty', 'WaitingConfirmations', 'Pending')
          )
        ORDER BY t.updated_at DESC
        LIMIT 100
      `,
      params
    );

    sendJson(res, 200, {
      ok: true,
      walletKey: wallet.wallet_key,
      transfers: result.rows.map((row) => ({
        id: row.id,
        direction: row.direction,
        transferKind: row.transfer_kind,
        nodeStatus: row.status,
        settlementStatus: row.settlement_status,
        assetId: row.asset_id,
        ticker: row.ticker,
        assetName: row.name,
        requestedAmount: row.requested_assignment_value,
        settledAmount: row.settled_amount,
        settledAt: row.settled_at,
        updatedAt: row.updated_at,
      })),
    });
  } catch (error) {
    console.error(`[${nowIso()}] [RGB API] Settlement status lookup failed:`, error.message);
    sendJson(res, 502, { ok: false, error: error.message });
  }
}

async function handleRgbRegistry(res) {
  try {
    const result = await query(
      `
        SELECT
          token_name,
          ticker,
          total_supply,
          precision,
          issuer_ref,
          creation_date,
          block_height,
          contract_id,
          schema_id
        FROM asset_registry
        ORDER BY created_at DESC, token_name ASC
      `
    );

    sendJson(res, 200, {
      ok: true,
      assets: result.rows,
    });
  } catch (error) {
    console.error(`[${nowIso()}] [RGB API] Registry lookup failed:`, error.message);
    sendJson(res, 502, { ok: false, error: error.message });
  }
}

async function handleRgbRegistryTransfers(res, parsedUrl) {
  const assetId = parsedUrl.searchParams.get('assetId');
  if (!assetId) {
    sendJson(res, 400, { ok: false, error: 'assetId is required.' });
    return;
  }

  try {
    const [registryResult, transferResult] = await Promise.all([
      query(
        `
          SELECT
            token_name,
            ticker,
            total_supply,
            precision,
            issuer_ref,
            creation_date,
            block_height,
            contract_id,
            schema_id
          FROM asset_registry
          WHERE contract_id = $1
          LIMIT 1
        `,
        [assetId]
      ),
      rgbNodeRequest('/listtransfers', { asset_id: assetId }),
    ]);

    sendJson(res, 200, {
      ok: true,
      asset: registryResult.rows[0] || null,
      transfers: Array.isArray(transferResult?.transfers) ? transferResult.transfers : [],
    });
  } catch (error) {
    console.error(`[${nowIso()}] [RGB API] Registry transfer lookup failed:`, error.message);
    sendJson(res, 502, { ok: false, error: error.message });
  }
}

async function ensureState() {
  await fsp.mkdir(DATA_DIR, { recursive: true });
  try {
    const raw = await fsp.readFile(CLAIMS_PATH, 'utf8');
    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed.claims)) {
      claimState = { claims: parsed.claims };
    }
  } catch (error) {
    if (error.code !== 'ENOENT') {
      console.error(`[${nowIso()}] Failed to load claims store:`, error);
    }
    await persistState();
  }
  pruneClaims();
}

function pruneClaims() {
  const threshold = Date.now() - CLAIMS_TTL_MS;
  claimState.claims = claimState.claims.filter((entry) => entry.timestamp >= threshold);
}

function persistState() {
  pruneClaims();
  const payload = JSON.stringify(claimState, null, 2);
  writeQueue = writeQueue.then(() => fsp.writeFile(CLAIMS_PATH, payload));
  return writeQueue;
}

function readRequestJson(req) {
  return new Promise((resolve, reject) => {
    let raw = '';
    req.on('data', (chunk) => {
      raw += chunk;
      if (Buffer.byteLength(raw) > MAX_BODY_BYTES) {
        reject(new Error('Request body is too large.'));
        req.destroy();
      }
    });
    req.on('end', () => {
      if (!raw) {
        resolve({});
        return;
      }
      try {
        resolve(JSON.parse(raw));
      } catch (error) {
        reject(new Error('Request body must be valid JSON.'));
      }
    });
    req.on('error', reject);
  });
}

function rpcRequest(method, params = [], walletScoped = false) {
  const auth = Buffer.from(`${RPC_USER}:${RPC_PASSWORD}`).toString('base64');
  const targetPath = walletScoped ? `/wallet/${encodeURIComponent(RPC_WALLET)}` : '/';

  const payload = JSON.stringify({
    jsonrpc: '1.0',
    id: `faucet-${method}`,
    method,
    params,
  });

  return new Promise((resolve, reject) => {
    const request = http.request(
      {
        host: RPC_HOST,
        port: RPC_PORT,
        path: targetPath,
        method: 'POST',
        headers: {
          Authorization: `Basic ${auth}`,
          'Content-Type': 'text/plain',
          'Content-Length': Buffer.byteLength(payload),
        },
      },
      (response) => {
        let raw = '';
        response.setEncoding('utf8');
        response.on('data', (chunk) => {
          raw += chunk;
        });
        response.on('end', () => {
          if (!raw) {
            reject(new Error(`RPC ${method} returned an empty response.`));
            return;
          }

          let parsed;
          try {
            parsed = JSON.parse(raw);
          } catch (error) {
            reject(new Error(`RPC ${method} returned invalid JSON.`));
            return;
          }

          if (response.statusCode && response.statusCode >= 400) {
            const rpcMessage = parsed?.error?.message || response.statusMessage || 'RPC request failed.';
            reject(new Error(`RPC ${method} failed: ${rpcMessage}`));
            return;
          }

          if (parsed.error) {
            reject(new Error(parsed.error.message || `RPC ${method} failed.`));
            return;
          }

          resolve(parsed.result);
        });
      }
    );

    request.on('error', reject);
    request.write(payload);
    request.end();
  });
}

function buildCooldownMessage(entry) {
  const waitMs = Math.max(0, entry.timestamp + CLAIMS_TTL_MS - Date.now());
  const waitMinutes = Math.ceil(waitMs / 60000);
  return {
    error: `Faucet cooldown active. Please wait about ${waitMinutes} minute(s) before requesting again.`,
    retryAfterMinutes: waitMinutes,
    lastClaimAt: new Date(entry.timestamp).toISOString(),
  };
}

function getRecentClaim(ip, address) {
  pruneClaims();
  return claimState.claims.find((entry) => entry.ip === ip || entry.address === address);
}

async function scanAddress(address) {
  const runScan = async () => rpcRequest('scantxoutset', ['start', [`addr(${address})`]]);
  const queued = scanQueue.then(runScan, runScan);
  scanQueue = queued.catch(() => undefined);
  return queued;
}

async function buildAddressStats(address) {
  const scan = await scanAddress(address);
  const fundedCount = Array.isArray(scan?.unspents) ? scan.unspents.length : 0;
  const fundedSum = Array.isArray(scan?.unspents)
    ? scan.unspents.reduce((sum, item) => sum + btcToSats(item.amount), 0)
    : 0;

  return {
    address,
    chain_stats: {
      funded_txo_count: fundedCount,
      funded_txo_sum: fundedSum,
      spent_txo_count: 0,
      spent_txo_sum: 0,
      tx_count: fundedCount,
    },
    mempool_stats: {
      funded_txo_count: 0,
      funded_txo_sum: 0,
      spent_txo_count: 0,
      spent_txo_sum: 0,
      tx_count: 0,
    },
    unspents: scan?.unspents || [],
  };
}

async function getMempoolAddressUtxos(address) {
  const mempool = await rpcRequest('getrawmempool', [true]);
  const transactions = Object.keys(mempool || {});
  if (transactions.length === 0) {
    return [];
  }

  const spentOutpoints = new Set();
  const receivedUtxos = [];

  for (const txid of transactions) {
    const raw = await rpcRequest('getrawtransaction', [txid, true]);

    for (const input of raw.vin || []) {
      if (input.txid !== undefined && input.vout !== undefined) {
        spentOutpoints.add(`${input.txid}:${input.vout}`);
      }
    }

    (raw.vout || []).forEach((output) => {
      const outputAddress =
        output.scriptPubKey?.address || output.scriptPubKey?.addresses?.[0] || null;
      if (outputAddress === address) {
        receivedUtxos.push({
          txid: raw.txid,
          vout: output.n,
          value: btcToSats(output.value),
          status: {
            confirmed: false,
            block_height: 0,
          },
        });
      }
    });
  }

  return receivedUtxos.filter((utxo) => !spentOutpoints.has(`${utxo.txid}:${utxo.vout}`));
}

async function handleFees(res) {
  try {
    const [fast, average, slow] = await Promise.all([
      rpcRequest('estimatesmartfee', [1]),
      rpcRequest('estimatesmartfee', [3]),
      rpcRequest('estimatesmartfee', [6]),
    ]);

    const fastestFee = Math.max(1, Math.ceil(((fast?.feerate || 0.0002) * SATS_PER_BTC) / 1000));
    const halfHourFee = Math.max(1, Math.ceil(((average?.feerate || fast?.feerate || 0.0002) * SATS_PER_BTC) / 1000));
    const hourFee = Math.max(1, Math.ceil(((slow?.feerate || average?.feerate || 0.0002) * SATS_PER_BTC) / 1000));

    sendJson(res, 200, {
      fastestFee,
      halfHourFee,
      hourFee,
      minimumFee: 1,
    });
  } catch (error) {
    sendJson(res, 200, {
      fastestFee: 2,
      halfHourFee: 2,
      hourFee: 1,
      minimumFee: 1,
    });
  }
}

async function handleAddressDetails(res, address) {
  try {
    const details = await buildAddressStats(address);
    sendJson(res, 200, details);
  } catch (error) {
    sendJson(res, 502, { error: error.message });
  }
}

async function handleAddressUtxos(res, address, parsedUrl) {
  try {
    const details = await buildAddressStats(address);
    const confirmedUtxos = details.unspents.map((item) => ({
      txid: item.txid,
      vout: item.vout,
      value: btcToSats(item.amount),
      status: {
        confirmed: true,
        block_height: item.height,
      },
    }));

    const includeMempool = parsedUrl?.searchParams?.get('include_mempool') === '1';
    const mempoolUtxos = includeMempool ? await getMempoolAddressUtxos(address) : [];
    const seen = new Set();
    const utxos = [...confirmedUtxos, ...mempoolUtxos].filter((utxo) => {
      const key = `${utxo.txid}:${utxo.vout}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });

    sendJson(res, 200, utxos);
  } catch (error) {
    sendJson(res, 502, { error: error.message });
  }
}

async function handleAddressTxs(res, address) {
  try {
    const details = await buildAddressStats(address);
    const txs = await Promise.all(
      details.unspents.map(async (item) => {
        const raw = await rpcRequest('getrawtransaction', [item.txid, true]);
        const block = raw.blockhash ? await rpcRequest('getblockheader', [raw.blockhash]) : null;
        return {
          txid: raw.txid,
          fee: 0,
          vin: raw.vin || [],
          vout: (raw.vout || []).map((output) => ({
            value: btcToSats(output.value),
            scriptpubkey_address: output.scriptPubKey?.address || output.scriptPubKey?.addresses?.[0] || null,
          })),
          status: {
            confirmed: true,
            block_height: block?.height || item.height || 0,
            block_time: block?.time || Math.floor(Date.now() / 1000),
          },
        };
      })
    );

    sendJson(res, 200, txs);
  } catch (error) {
    sendJson(res, 502, { error: error.message });
  }
}

async function handleBroadcast(req, res) {
  try {
    let raw = '';
    req.on('data', (chunk) => {
      raw += chunk;
      if (Buffer.byteLength(raw) > 1024 * 1024) {
        req.destroy();
      }
    });
    await new Promise((resolve, reject) => {
      req.on('end', resolve);
      req.on('error', reject);
    });

    const txHex = raw.trim();
    if (!txHex) {
      sendText(res, 400, 'Transaction hex is required');
      return;
    }

    const txid = await rpcRequest('sendrawtransaction', [txHex]);
    sendText(res, 200, txid);
  } catch (error) {
    sendText(res, 502, error.message);
  }
}

async function handleMineBlocks(req, res) {
  let body = {};
  try {
    if (req.method === 'POST') {
      body = await readRequestJson(req);
    }
  } catch (error) {
    sendJson(res, 400, { ok: false, error: error.message });
    return;
  }

  try {
    const blocks = Math.max(1, Number(body.blocks || 1));
    const miningAddress = await rpcRequest('getnewaddress', ['wallet-auto-mine', MINING_ADDRESS_TYPE], true);
    const minedBlocks = await rpcRequest('generatetoaddress', [blocks, miningAddress]);
    sendJson(res, 200, {
      ok: true,
      blocks,
      minedBlocks,
      miningAddress,
    });
  } catch (error) {
    sendJson(res, 502, { ok: false, error: error.message });
  }
}

async function handleStatus(req, res) {
  try {
    const [chain, walletInfo, balance] = await Promise.all([
      rpcRequest('getblockchaininfo'),
      rpcRequest('getwalletinfo', [], true),
      rpcRequest('getbalance', ['*', 0, false], true),
    ]);

    sendJson(res, 200, {
      ok: true,
      network: chain.chain,
      blocks: chain.blocks,
      wallet: RPC_WALLET,
      balance,
      amountBtc: DEFAULT_FAUCET_AMOUNT_BTC,
      allowedAmountsBtc: ALLOWED_FAUCET_AMOUNTS_BTC,
      cooldownMinutes: COOLDOWN_MINUTES,
      autoMineBlocks: AUTO_MINE_BLOCKS,
      walletTxCount: walletInfo.txcount,
    });
  } catch (error) {
    sendJson(res, 503, {
      ok: false,
      error: error.message,
      wallet: RPC_WALLET,
      rpcHost: `${RPC_PROTOCOL}://${RPC_HOST}:${RPC_PORT}`,
    });
  }
}

async function handleBtcUsdPrice(res) {
  try {
    const headers = {};
    if (BINANCE_API_KEY) {
      headers['X-MBX-APIKEY'] = BINANCE_API_KEY;
    }

    const response = await fetch(`${BINANCE_API_BASE}/api/v3/ticker/price?symbol=BTCUSDT`, {
      headers,
    });

    if (!response.ok) {
      throw new Error(`Binance price endpoint responded with status ${response.status}`);
    }

    const payload = await response.json();
    const price = Number(payload?.price);

    if (!Number.isFinite(price) || price <= 0) {
      throw new Error('Binance returned an invalid BTC price.');
    }

    sendJson(res, 200, {
      ok: true,
      symbol: 'BTCUSD',
      source: 'binance',
      priceUsd: price,
    });
  } catch (error) {
    sendJson(res, 502, { ok: false, error: error.message });
  }
}

async function handleRgbHealth(res) {
  try {
    console.log(`[${nowIso()}] [RGB API] Health check requested`);
    const info = await fetch(`${RGB_NODE_API_BASE}/networkinfo`);
    if (!info.ok) {
      throw new Error(`RGB node responded with status ${info.status}`);
    }
    const payload = await info.json();
    console.log(`[${nowIso()}] [RGB API] Health check succeeded`, payload);
    sendJson(res, 200, {
      ok: true,
      rgbNodeApiBase: RGB_NODE_API_BASE,
      ...payload,
    });
  } catch (error) {
    console.error(`[${nowIso()}] [RGB API] Health check failed:`, error.message);
    sendJson(res, 503, {
      ok: false,
      error: error.message,
      rgbNodeApiBase: RGB_NODE_API_BASE,
    });
  }
}

async function handleRgbInvoice(req, res) {
  let body;
  try {
    body = await readRequestJson(req);
  } catch (error) {
    console.error(`[${nowIso()}] [RGB API] Invalid invoice request body:`, error.message);
    sendJson(res, 400, { ok: false, error: error.message });
    return;
  }

  const assetId =
    typeof body.assetId === 'string' && body.assetId.trim() ? body.assetId.trim() : null;
  const openAmount = Boolean(body.openAmount);
  const amount = Number(body.amount || 0);
  console.log(`[${nowIso()}] [RGB API] Invoice request received`, {
    assetId,
    openAmount,
    amount,
  });

  if (!openAmount && (!Number.isFinite(amount) || amount <= 0)) {
    console.warn(`[${nowIso()}] [RGB API] Rejecting invoice request due to invalid amount`, {
      assetId,
      openAmount,
      amount,
    });
    sendJson(res, 400, { ok: false, error: 'A positive RGB amount is required unless openAmount is enabled.' });
    return;
  }

  try {
    const wallet = await ensureWallet(req, getRequestNetwork(body));
    const { apiBase: walletApiBase } = resolveWalletNodeContext(wallet);
    const payload = {
      min_confirmations: 1,
      asset_id: assetId,
      assignment: openAmount ? null : { type: 'Fungible', value: amount },
      duration_seconds: 86400,
      witness: false,
    };

    let invoice;
    try {
      invoice = await rgbNodeRequestWithBase(walletApiBase, '/rgbinvoice', payload);
    } catch (error) {
      const message = error?.message || '';
      if (message.includes('No uncolored UTXOs are available')) {
        console.warn(`[${nowIso()}] [RGB API] RGB issuer is out of uncolored UTXOs, replenishing and retrying once`);
        await ensureRgbUtxos(walletApiBase);
        invoice = await rgbNodeRequestWithBase(walletApiBase, '/rgbinvoice', payload);
      } else {
        throw error;
      }
    }
    const rewrittenInvoice = rewriteRgbInvoiceTransportEndpoint(invoice?.invoice);
    let walletAsset = null;
    if (assetId) {
      const synced = await syncWalletAssetFromRgbNode({ walletId: wallet.id, assetId });
      walletAsset = synced.walletAsset;
    }
    await recordRgbInvoice({
      walletId: wallet.id,
      walletAssetId: walletAsset?.id || null,
      invoice: {
        ...invoice,
        invoice: rewrittenInvoice,
      },
      openAmount,
      proxyEndpoint: RGB_PUBLIC_PROXY_ENDPOINT,
    });
    const storedInvoice = await findWalletInvoiceByRecipient(wallet.id, invoice?.recipient_id || null, walletAsset?.id || null);
    let managedSecret = null;
    let managedSecretSource = 'backend-invoice';

    if (getRequestNetwork(body) === 'regtest' && Number.isFinite(Number(invoice?.batch_transfer_idx))) {
      try {
        const runtimeTransfer = await findRgbTransferByIdx(assetId, Number(invoice.batch_transfer_idx));
        if (runtimeTransfer?.receive_utxo) {
          managedSecret = `backend-managed:${runtimeTransfer.receive_utxo}`;
          managedSecretSource = 'backend-runtime';
        } else if (invoice?.recipient_id) {
          managedSecret = `backend-managed:${invoice.recipient_id}`;
          managedSecretSource = 'backend-runtime';
        }
      } catch (runtimeError) {
        console.warn(`[${nowIso()}] [RGB API] Unable to derive backend-managed secret token for invoice`, runtimeError?.message || runtimeError);
      }
    }

    await upsertPendingConsignmentSecret({
      walletId: wallet.id,
      walletAssetId: walletAsset?.id || null,
      invoiceId: storedInvoice?.id || null,
      recipientId: invoice?.recipient_id || null,
      blindingSecret: managedSecret,
      invoiceString: rewrittenInvoice,
      assetId,
      amount: openAmount ? null : amount,
      network: getRequestNetwork(body),
      source: managedSecretSource,
    });
    console.log(`[${nowIso()}] [RGB API] Invoice created successfully`, {
      walletKey: wallet.wallet_key,
      assetId,
      recipientId: invoice?.recipient_id || null,
      batchTransferIdx: invoice?.batch_transfer_idx || null,
      expirationTimestamp: invoice?.expiration_timestamp || null,
      publicProxyEndpoint: RGB_PUBLIC_PROXY_ENDPOINT,
    });
    sendJson(res, 200, {
      ok: true,
      ...invoice,
      invoice: rewrittenInvoice,
    });
  } catch (error) {
    console.error(`[${nowIso()}] RGB invoice generation failed:`, error.message);
    sendJson(res, 502, { ok: false, error: error.message });
  }
}

async function handleRgbInvoiceRegister(req, res) {
  let body;
  try {
    body = await readRequestJson(req);
  } catch (error) {
    sendJson(res, 400, { ok: false, error: error.message });
    return;
  }

  const recipientId = typeof body.recipientId === 'string' && body.recipientId.trim() ? body.recipientId.trim() : null;
  const blindingSecret = typeof body.blindingSecret === 'string' && body.blindingSecret.trim() ? body.blindingSecret.trim() : null;
  const invoiceString = typeof body.invoice === 'string' && body.invoice.trim() ? body.invoice.trim() : null;
  const assetId = typeof body.assetId === 'string' && body.assetId.trim() ? body.assetId.trim() : null;
  const amount =
    body.amount !== undefined && body.amount !== null && Number.isFinite(Number(body.amount))
      ? Math.trunc(Number(body.amount))
      : null;

  if (!recipientId) {
    sendJson(res, 400, { ok: false, error: 'recipientId is required.' });
    return;
  }

  if (!blindingSecret) {
    sendJson(res, 400, { ok: false, error: 'blindingSecret is required.' });
    return;
  }

  try {
    const registered = await registerWalletInvoiceSecret({
      req,
      network: getRequestNetwork(body),
      assetId,
      amount,
      invoiceString,
      recipientId,
      blindingSecret,
      source: 'browser',
    });

    sendJson(res, 200, {
      ok: true,
      walletKey: registered.wallet.wallet_key,
      invoiceId: registered.invoiceId,
      consignmentId: registered.consignmentId,
      recipientId,
      blindingSecretStatus: 'active',
    });
  } catch (error) {
    console.error(`[${nowIso()}] [RGB API] Invoice secret registration failed:`, error.message);
    sendJson(res, 502, { ok: false, error: error.message });
  }
}

async function handleClaim(req, res) {
  let body;
  try {
    body = await readRequestJson(req);
  } catch (error) {
    sendJson(res, 400, { ok: false, error: error.message });
    return;
  }

  const address = sanitizeAddress(body.address);
  const requestedAmountBtc = normalizeFaucetAmount(body.amountBtc);
  const ip = getRemoteIp(req);

  if (!address) {
    sendJson(res, 400, { ok: false, error: 'A regtest Bitcoin address is required.' });
    return;
  }

  if (!ALLOWED_FAUCET_AMOUNTS_BTC.includes(requestedAmountBtc)) {
    sendJson(res, 400, {
      ok: false,
      error: `Amount must be one of: ${ALLOWED_FAUCET_AMOUNTS_BTC.join(', ')} BTC.`,
      allowedAmountsBtc: ALLOWED_FAUCET_AMOUNTS_BTC,
    });
    return;
  }

  const recent = getRecentClaim(ip, address);
  if (recent) {
    sendJson(res, 429, { ok: false, ...buildCooldownMessage(recent) });
    return;
  }

  try {
    const chainInfo = await rpcRequest('getblockchaininfo');
    if (chainInfo.chain !== 'regtest') {
      throw new Error(`Faucet is configured for regtest, but bitcoind reports ${chainInfo.chain}.`);
    }

    const validation = await rpcRequest('validateaddress', [address]);
    if (!validation?.isvalid) {
      sendJson(res, 400, { ok: false, error: 'The supplied address is not a valid regtest Bitcoin address.' });
      return;
    }

    const txid = await rpcRequest(
      'sendtoaddress',
      [address, Number(requestedAmountBtc), 'PhotonBolt regtest faucet', 'PhotonBolt faucet claim'],
      true
    );

    let minedBlocks = [];
    if (AUTO_MINE_BLOCKS > 0) {
      const miningAddress = await rpcRequest('getnewaddress', ['faucet-mining', MINING_ADDRESS_TYPE], true);
      minedBlocks = await rpcRequest('generatetoaddress', [AUTO_MINE_BLOCKS, miningAddress]);
    }

    const record = {
      ip,
      address,
      timestamp: Date.now(),
      txid,
    };
    claimState.claims.push(record);
    await persistState();

    sendJson(res, 200, {
      ok: true,
      txid,
      amountBtc: requestedAmountBtc,
      network: 'regtest',
      wallet: RPC_WALLET,
      cooldownMinutes: COOLDOWN_MINUTES,
      minedBlocks,
    });
  } catch (error) {
    console.error(`[${nowIso()}] Claim failed for ${address} from ${ip}:`, error.message);
    sendJson(res, 502, { ok: false, error: error.message });
  }
}

async function serveStatic(req, res) {
  const parsedPath = new URL(req.url, `http://${req.headers.host || 'localhost'}`).pathname;
  const normalizedPath = parsedPath === '/' ? '/index.html' : parsedPath;
  const resolvedPath = path.join(PUBLIC_DIR, path.normalize(normalizedPath));

  if (!resolvedPath.startsWith(PUBLIC_DIR)) {
    sendText(res, 403, 'Forbidden');
    return;
  }

  try {
    const stat = await fsp.stat(resolvedPath);
    const targetPath = stat.isDirectory() ? path.join(resolvedPath, 'index.html') : resolvedPath;
    const ext = path.extname(targetPath).toLowerCase();
    const content = await fsp.readFile(targetPath);
    res.writeHead(
      200,
      baseHeaders({
        'Content-Type': MIME_TYPES[ext] || 'application/octet-stream',
        'Cache-Control': ext === '.html' ? 'no-store' : 'public, max-age=3600',
      })
    );
    res.end(content);
  } catch (error) {
    if (error.code === 'ENOENT') {
      sendText(res, 404, 'Not found');
      return;
    }
    console.error(`[${nowIso()}] Static file error:`, error);
    sendText(res, 500, 'Internal server error');
  }
}

async function requestHandler(req, res) {
  if (!req.url) {
    sendText(res, 400, 'Bad request');
    return;
  }

  const parsedUrl = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  const pathname = parsedUrl.pathname;

  if (req.method === 'OPTIONS') {
    res.writeHead(204, baseHeaders());
    res.end();
    return;
  }

  if (req.method === 'GET' && pathname === '/api/status') {
    await handleStatus(req, res);
    return;
  }

  if (req.method === 'GET' && pathname === '/api/market/btc-usd') {
    await handleBtcUsdPrice(res);
    return;
  }

  if (req.method === 'GET' && pathname === '/api/rgb/health') {
    await handleRgbHealth(res);
    return;
  }

  if (req.method === 'POST' && pathname === '/api/rgb/watchdog/trigger') {
    setImmediate(() => runReceiverWatchdogCycle().catch((e) =>
      console.error(`[${nowIso()}] [Watchdog] Manual trigger error:`, e.message)
    ));
    sendJson(res, 200, { ok: true, message: 'Watchdog cycle triggered.' });
    return;
  }

  if (req.method === 'POST' && pathname === '/api/rgb/invoice') {
    await handleRgbInvoice(req, res);
    return;
  }

  if (req.method === 'POST' && pathname === '/api/rgb/ln-invoice') {
    await handleRgbLightningInvoice(req, res);
    return;
  }

  if (req.method === 'POST' && pathname === '/api/rgb/invoice/register') {
    await handleRgbInvoiceRegister(req, res);
    return;
  }

  if (req.method === 'POST' && pathname === '/api/rgb/balance') {
    await handleRgbBalance(req, res);
    return;
  }

  if (req.method === 'POST' && pathname === '/api/rgb/decode-invoice') {
    await handleRgbDecodeInvoice(req, res);
    return;
  }

  if (req.method === 'POST' && pathname === '/api/rgb/decode-lightning-invoice') {
    await handleRgbDecodeLightningInvoice(req, res);
    return;
  }

  if (req.method === 'POST' && pathname === '/api/rgb/transfers') {
    await handleRgbTransfers(req, res);
    return;
  }

  if (req.method === 'POST' && pathname === '/api/rgb/send') {
    await handleRgbSend(req, res);
    return;
  }

  if (req.method === 'POST' && pathname === '/api/rgb/pay-lightning') {
    await handleRgbPayLightning(req, res);
    return;
  }

  if (req.method === 'POST' && pathname === '/api/rgb/refresh') {
    await handleRgbRefresh(req, res);
    return;
  }

  if (req.method === 'GET' && pathname === '/api/rgb/settlement-status') {
    await handleRgbSettlementStatus(req, res, parsedUrl);
    return;
  }

  if (req.method === 'GET' && pathname === '/api/rgb/registry') {
    await handleRgbRegistry(res);
    return;
  }

  if (req.method === 'GET' && pathname === '/api/rgb/registry/transfers') {
    await handleRgbRegistryTransfers(res, parsedUrl);
    return;
  }

  if (req.method === 'POST' && pathname === '/api/claim') {
    await handleClaim(req, res);
    return;
  }

  if (req.method === 'GET' && pathname === '/api/v1/fees/recommended') {
    await handleFees(res);
    return;
  }

  if (req.method === 'POST' && pathname === '/api/tx') {
    await handleBroadcast(req, res);
    return;
  }

  if (req.method === 'POST' && pathname === '/api/regtest/mine') {
    await handleMineBlocks(req, res);
    return;
  }

  const addressUtxoMatch = pathname.match(/^\/api\/address\/([^/]+)\/utxo$/);
  if (req.method === 'GET' && addressUtxoMatch) {
    await handleAddressUtxos(res, decodeAddressFromPath(addressUtxoMatch[1]), parsedUrl);
    return;
  }

  const addressTxsMatch = pathname.match(/^\/api\/address\/([^/]+)\/txs$/);
  if (req.method === 'GET' && addressTxsMatch) {
    await handleAddressTxs(res, decodeAddressFromPath(addressTxsMatch[1]));
    return;
  }

  const addressMatch = pathname.match(/^\/api\/address\/([^/]+)$/);
  if (req.method === 'GET' && addressMatch) {
    await handleAddressDetails(res, decodeAddressFromPath(addressMatch[1]));
    return;
  }

  if (req.method === 'GET' || req.method === 'HEAD') {
    await serveStatic(req, res);
    return;
  }

  sendText(res, 405, 'Method not allowed');
}

// ---------------------------------------------------------------------------
// Receiver-Side Settlement Watchdog
// ---------------------------------------------------------------------------

let _watchdogRunning = false;

/**
 * Normalise node transfer status strings to our settlement_status vocabulary.
 * Returns 'SETTLED', 'VALIDATION_FAILED', or null (still pending).
 */
function classifyNodeTransferStatus(nodeStatus) {
  if (!nodeStatus) return null;
  const s = String(nodeStatus);
  if (s === 'Settled') return 'SETTLED';
  if (s === 'Failed' || s === 'NotOwned') return 'VALIDATION_FAILED';
  return null; // WaitingCounterparty, WaitingConfirmations, Pending, etc.
}

/**
 * Part A — Confirm ACK on outgoing CONSIGNMENT_UPLOADED transfers.
 *
 * For each outgoing transfer the sender advanced to CONSIGNMENT_UPLOADED we
 * poll the proxy ack.get endpoint using the blinded-seal stored in
 * consignment_records.  On ACK → SETTLED; on NACK → DELIVERY_FAILED.
 */
async function watchdogConfirmSenderAcks() {
  const result = await query(
    `
      SELECT
        t.id            AS transfer_id,
        t.wallet_id,
        wa.asset_id,
        w.wallet_key,
        w.rgb_account_ref,
        cr.id           AS consignment_record_id,
        cr.recipient_id AS proxy_recipient_id
      FROM rgb_transfers t
      JOIN wallets w        ON w.id  = t.wallet_id
      JOIN wallet_assets wa ON wa.id = t.wallet_asset_id
      JOIN consignment_records cr ON cr.transfer_id = t.id
      WHERE t.direction         = 'outgoing'
        AND t.settlement_status = 'CONSIGNMENT_UPLOADED'
        AND t.wallet_asset_id  IS NOT NULL
      ORDER BY t.updated_at ASC
    `
  );

  for (const row of result.rows) {
    const tag = `[Watchdog/SenderACK] transferId=${row.transfer_id} asset=${row.asset_id}`;
    try {
      const ack = await proxyGetAck(row.proxy_recipient_id);
      if (ack === true) {
        await updateLightningTransferSettlementStatus(row.transfer_id, 'SETTLED');
        await query(
          `UPDATE consignment_records
           SET delivery_status = 'acked', ack = TRUE, acked_at = NOW()
           WHERE id = $1`,
          [row.consignment_record_id]
        );
        await recordTransferEvent({
          walletId: row.wallet_id,
          transferId: row.transfer_id,
          consignmentId: row.consignment_record_id,
          eventType: 'rgb_transfer_settled',
          eventSource: 'receiver_watchdog',
          payload: { assetId: row.asset_id, walletKey: row.wallet_key },
        });
        console.log(`${tag} — ACKed by receiver → SETTLED`);
      } else if (ack === false) {
        await updateLightningTransferSettlementStatus(row.transfer_id, 'DELIVERY_FAILED');
        await query(
          `UPDATE consignment_records
           SET delivery_status = 'nacked', ack = FALSE, acked_at = NOW()
           WHERE id = $1`,
          [row.consignment_record_id]
        );
        await recordTransferEvent({
          walletId: row.wallet_id,
          transferId: row.transfer_id,
          consignmentId: row.consignment_record_id,
          eventType: 'rgb_consignment_nacked',
          eventSource: 'receiver_watchdog',
          payload: { assetId: row.asset_id, walletKey: row.wallet_key },
        });
        console.warn(`${tag} — NACKed → DELIVERY_FAILED`);
      }
      // ack === undefined: still pending, try again next cycle
    } catch (err) {
      console.warn(`${tag} — ack.get error (will retry): ${err.message}`);
    }
  }
}

/**
 * Part B — Drive the receiver side to settlement for managed wallets.
 *
 * Finds all incoming transfers that are not yet Settled (any non-terminal
 * node status: WaitingCounterparty, WaitingConfirmations, Pending) for
 * wallets with a known rgb_account_ref.  Groups them by (apiBase, assetId)
 * to issue one /refreshtransfers call per unique node+asset combination,
 * then checks /listtransfers to see if the node has accepted the consignment.
 *
 * On success → settlement_status = 'SETTLED', consignment validated.
 * On node rejection → settlement_status = 'VALIDATION_FAILED'.
 *
 * Asset-agnostic: uses asset_id from the database row, not hard-coded.
 * Multi-tenant: routes each refresh through the wallet's own rgb_account_ref.
 */
async function watchdogDriveReceiverSettlement() {
  const result = await query(
    `
      SELECT
        t.id              AS transfer_id,
        t.wallet_id,
        t.transfer_kind,
        t.status          AS node_status,
        t.settlement_status,
        t.recipient_id,
        t.rgb_transfer_idx,
        t.requested_assignment_value,
        wa.asset_id,
        w.wallet_key,
        w.rgb_account_ref
      FROM rgb_transfers t
      JOIN wallets w        ON w.id  = t.wallet_id
      JOIN wallet_assets wa ON wa.id = t.wallet_asset_id
      WHERE t.direction        = 'incoming'
        AND t.transfer_kind   != 'Issuance'
        AND t.status          != 'Settled'
        AND t.wallet_asset_id IS NOT NULL
        AND w.rgb_account_ref IS NOT NULL
        AND (
          t.settlement_status IS NULL
          OR t.settlement_status NOT IN ('SETTLED', 'VALIDATION_FAILED')
        )
      ORDER BY t.updated_at ASC
    `
  );

  if (result.rows.length === 0) return;

  // Group by (apiBase, assetId) so we call refreshtransfers once per pair.
  const refreshKeys = new Map(); // key → { apiBase, assetId, walletId, walletKey }
  const rowsByKey = new Map();   // key → rows[]

  for (const row of result.rows) {
    const apiBase = resolveRgbNodeApiBaseForAccountRef(row.rgb_account_ref);
    const key = `${apiBase}::${row.asset_id}`;
    if (!refreshKeys.has(key)) {
      refreshKeys.set(key, { apiBase, assetId: row.asset_id, walletId: row.wallet_id, walletKey: row.wallet_key });
      rowsByKey.set(key, []);
    }
    rowsByKey.get(key).push(row);
  }

  for (const [key, ctx] of refreshKeys) {
    const tag = `[Watchdog/ReceiverSettle] node=${ctx.apiBase} asset=${ctx.asset_id}`;
    try {
      // Step 1: force the node to check the proxy for new consignments
      await rgbNodeRequestWithBase(ctx.apiBase, '/refreshtransfers', { skip_sync: false });

      // Step 2: read updated transfer list from the node (asset-scoped)
      const transferList = await rgbNodeRequestWithBase(
        ctx.apiBase, '/listtransfers', { asset_id: ctx.assetId }
      );
      const nodeTransfers = Array.isArray(transferList?.transfers) ? transferList.transfers : [];

      // Step 3: reconcile each pending DB row against the node response
      for (const row of rowsByKey.get(key)) {
        const rtag = `${tag} transferId=${row.transfer_id}`;
        const matched = nodeTransfers.find((nt) => {
          if (row.rgb_transfer_idx != null) {
            return Number(nt.idx) === Number(row.rgb_transfer_idx);
          }
          return nt.recipient_id && nt.recipient_id === row.recipient_id;
        });

        if (!matched) {
          // Node doesn't know about this transfer yet — next cycle will retry
          continue;
        }

        const newSettlementStatus = classifyNodeTransferStatus(matched.status);
        if (!newSettlementStatus) continue; // still pending

        if (newSettlementStatus === 'SETTLED') {
          const settledAmount = matched.assignments?.[0]?.value ?? matched.requested_assignment?.value ?? null;

          // Update transfer row
          await query(
            `
              UPDATE rgb_transfers
              SET
                status             = 'Settled',
                settlement_status  = 'SETTLED'::settlement_status_enum,
                settled_amount     = COALESCE($2, settled_amount),
                settled_at         = COALESCE(settled_at, NOW()),
                txid               = COALESCE($3, txid),
                receive_utxo       = COALESCE($4, receive_utxo)
              WHERE id = $1
            `,
            [
              row.transfer_id,
              settledAmount !== null ? String(settledAmount) : null,
              matched.txid || null,
              matched.receive_utxo || null,
            ]
          );

          // Mark consignment_records as validated + acked
          await query(
            `
              UPDATE consignment_records
              SET
                delivery_status = 'acked',
                ack             = TRUE,
                validated_at    = COALESCE(validated_at, NOW()),
                acked_at        = COALESCE(acked_at, NOW())
              WHERE transfer_id = $1
            `,
            [row.transfer_id]
          );

          // Post ACK to proxy if we have a blinded-seal recipient_id
          if (row.recipient_id) {
            try {
              await proxyJsonRpc('ack.post', { recipient_id: row.recipient_id, ack: true });
            } catch (ackErr) {
              console.warn(`${rtag} — proxy ack.post failed (non-fatal): ${ackErr.message}`);
            }
          }

          await recordTransferEvent({
            walletId: row.wallet_id,
            transferId: row.transfer_id,
            eventType: 'rgb_transfer_settled',
            eventSource: 'receiver_watchdog',
            payload: {
              assetId: row.asset_id,
              walletKey: row.wallet_key,
              settledAmount,
              txid: matched.txid || null,
            },
          });
          console.log(`${rtag} — SETTLED (amount=${settledAmount} txid=${matched.txid || 'n/a'})`);

        } else if (newSettlementStatus === 'VALIDATION_FAILED') {
          await query(
            `
              UPDATE rgb_transfers
              SET
                status            = $2,
                settlement_status = 'VALIDATION_FAILED'::settlement_status_enum
              WHERE id = $1
            `,
            [row.transfer_id, matched.status]
          );

          await query(
            `
              UPDATE consignment_records
              SET delivery_status = 'nacked', ack = FALSE, acked_at = NOW()
              WHERE transfer_id = $1
            `,
            [row.transfer_id]
          );

          if (row.recipient_id) {
            try {
              await proxyJsonRpc('ack.post', { recipient_id: row.recipient_id, ack: false });
            } catch (ackErr) {
              console.warn(`${rtag} — proxy nack.post failed (non-fatal): ${ackErr.message}`);
            }
          }

          await recordTransferEvent({
            walletId: row.wallet_id,
            transferId: row.transfer_id,
            eventType: 'rgb_consignment_nacked',
            eventSource: 'receiver_watchdog',
            payload: { assetId: row.asset_id, walletKey: row.wallet_key, nodeStatus: matched.status },
          });
          console.warn(`${rtag} — node status=${matched.status} → VALIDATION_FAILED`);
        }
      }
    } catch (err) {
      console.warn(`${tag} — refresh cycle error (will retry): ${err.message}`);
    }
  }
}

/**
 * Single watchdog tick: runs both settlement sub-tasks sequentially.
 * A guard flag prevents overlapping cycles if a tick takes longer than the interval.
 */
async function runReceiverWatchdogCycle() {
  if (_watchdogRunning) return;
  _watchdogRunning = true;
  try {
    await watchdogConfirmSenderAcks();
    await watchdogDriveReceiverSettlement();
  } catch (err) {
    console.error(`[${nowIso()}] [Watchdog] Unhandled error in watchdog cycle:`, err.message);
  } finally {
    _watchdogRunning = false;
  }
}

function startReceiverWatchdog() {
  console.log(`[${nowIso()}] [Watchdog] Receiver settlement watchdog started (interval=${RECEIVER_WATCHDOG_INTERVAL_MS}ms)`);
  setInterval(runReceiverWatchdogCycle, RECEIVER_WATCHDOG_INTERVAL_MS);
  // Run one cycle immediately so any pre-existing CONSIGNMENT_UPLOADED rows are
  // picked up without waiting for the first interval to elapse.
  setImmediate(runReceiverWatchdogCycle);
}

async function start() {
  await ensureState();
  const server = http.createServer((req, res) => {
    Promise.resolve(requestHandler(req, res)).catch((error) => {
      console.error(`[${nowIso()}] Unhandled request error:`, error);
      sendJson(res, 500, { ok: false, error: 'Internal server error.' });
    });
  });

  server.on('error', (error) => {
    console.error(`[${nowIso()}] Faucet server failed:`, error);
  });

  server.listen(PORT, '127.0.0.1', () => {
    console.log(
      `[${nowIso()}] PhotonBolt faucet listening on http://127.0.0.1:${PORT} using wallet ${RPC_WALLET}`
    );
    startReceiverWatchdog();
  });
}

start().catch((error) => {
  console.error(`[${nowIso()}] Failed to start faucet server:`, error);
  process.exitCode = 1;
});
