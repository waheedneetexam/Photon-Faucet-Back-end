const http = require('http');
const { randomUUID } = require('crypto');
const crypto = require('crypto');
const { execFile } = require('child_process');
const fs = require('fs');
const fsp = require('fs/promises');
const path = require('path');
const { promisify } = require('util');
const bitcoin = require('/home/waheed/PhotonBoltXYZ/photon-web-wallet/node_modules/bitcoinjs-lib');
const ecc = require('/home/waheed/PhotonBoltXYZ/photon-web-wallet/node_modules/tiny-secp256k1');
const {
  query,
  withTransaction,
  ensureWallet,
  ensureRgbNodesTable,
  ensureRgbAssetIssuancesTable,
  upsertRgbNode,
  listRgbNodes,
  upsertWalletAsset,
  upsertWalletAssetBalance,
  setWalletFundingAddress,
  setWalletMainBtcAddress,
  getWalletAddresses,
  upsertUtxoSlot,
  getWalletUtxoSlots,
  markSlotOccupied,
  markSlotEmpty,
  markSlotRedeemed,
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

const execFileAsync = promisify(execFile);

const PORT = Number(process.env.PORT || 8788);
const RPC_HOST = process.env.BITCOIN_RPC_HOST || '127.0.0.1';
const RPC_PORT = Number(process.env.BITCOIN_RPC_PORT || 18443);
const RPC_PROTOCOL = process.env.BITCOIN_RPC_PROTOCOL || 'http';
const RPC_USER = process.env.BITCOIN_RPC_USER || 'user';
const RPC_PASSWORD = process.env.BITCOIN_RPC_PASSWORD || 'password';
const RPC_WALLET = process.env.BITCOIN_RPC_WALLET || 'photon_dev';
const RGB_OWNER_API_BASE_DEFAULT = process.env.RGB_NODE_API_BASE || 'http://127.0.0.1:3001';
const RGB_USER_API_BASE_DEFAULT = process.env.RGB_LIGHTNING_NODE_API_BASE || 'http://127.0.0.1:3002';
const RGB_USER_B_API_BASE_DEFAULT = process.env.RGB_LIGHTNING_NODE_B_API_BASE || 'http://127.0.0.1:3003';
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
const RGB_FAUCET_MAX_AMOUNT = Number(process.env.RGB_FAUCET_MAX_AMOUNT || 1000);
const RGB_FAUCET_COOLDOWN_PAUSED = true;
const AUTO_MINE_BLOCKS = Number(process.env.FAUCET_AUTO_MINE_BLOCKS || 1);
const MINING_ADDRESS_TYPE = process.env.FAUCET_MINING_ADDRESS_TYPE || 'bech32';

const CLAIMS_TTL_MS = COOLDOWN_MINUTES * 60 * 1000;
const MAX_BODY_BYTES = 8 * 1024;
const SATS_PER_BTC = 100000000;
const RGB_ISSUANCE_MIN_FUNDING_SATS = Number(process.env.RGB_ISSUANCE_MIN_FUNDING_SATS || 50000);
const RGB_OWNER_WALLET_KEY = `dev-${RPC_WALLET}-regtest`;
const RGB_OWNER_ACCOUNT_REF = 'photon-rln-issuer';
const RGB_USER_ACCOUNT_REF = 'photon-rln-user';
const RGB_USER_B_ACCOUNT_REF = 'photon-rln-user-b';
const DEFAULT_RGB_NODES = [
  {
    accountRef: RGB_OWNER_ACCOUNT_REF,
    label: 'Issuer Node',
    apiBase: RGB_OWNER_API_BASE_DEFAULT,
    role: 'issuer',
    enabled: true,
    sortOrder: 0,
  },
  {
    accountRef: RGB_USER_ACCOUNT_REF,
    label: 'User Node',
    apiBase: RGB_USER_API_BASE_DEFAULT,
    role: 'user',
    enabled: true,
    sortOrder: 10,
  },
  {
    accountRef: RGB_USER_B_ACCOUNT_REF,
    label: 'User Node B',
    apiBase: RGB_USER_B_API_BASE_DEFAULT,
    role: 'user',
    enabled: true,
    sortOrder: 20,
  },
];
const RGB_NODE_CONTROL_TARGETS = {
  [RGB_OWNER_ACCOUNT_REF]: {
    target: RGB_OWNER_ACCOUNT_REF,
    label: 'Issuer Node',
    type: 'rgb-node',
    container: 'photon-rln-issuer',
    accountRef: RGB_OWNER_ACCOUNT_REF,
  },
  [RGB_USER_ACCOUNT_REF]: {
    target: RGB_USER_ACCOUNT_REF,
    label: 'User Node',
    type: 'rgb-node',
    container: 'photon-rln-user',
    accountRef: RGB_USER_ACCOUNT_REF,
  },
  [RGB_USER_B_ACCOUNT_REF]: {
    target: RGB_USER_B_ACCOUNT_REF,
    label: 'User Node B',
    type: 'rgb-node',
    container: 'photon-rln-user-b',
    accountRef: RGB_USER_B_ACCOUNT_REF,
  },
  'photon-electrs': {
    target: 'photon-electrs',
    label: 'Electrs',
    type: 'infra',
    container: 'photon-electrs',
  },
};
const RGB_NODE_UNLOCK_DEFAULTS = {
  [RGB_OWNER_ACCOUNT_REF]: {
    password: process.env.RGB_OWNER_NODE_PASSWORD || '',
    announceAlias: 'photon-rln-issuer',
  },
  [RGB_USER_ACCOUNT_REF]: {
    password: process.env.RGB_USER_NODE_PASSWORD || '',
    announceAlias: 'photon-rln-user',
  },
  [RGB_USER_B_ACCOUNT_REF]: {
    password: process.env.RGB_USER_B_NODE_PASSWORD || '',
    announceAlias: 'photon-rln-user-b',
  },
};
const ADMIN_WALLET_ADDRESS = typeof process.env.PHOTON_ADMIN_WALLET_ADDRESS === 'string'
  ? process.env.PHOTON_ADMIN_WALLET_ADDRESS.trim()
  : '';
const ADMIN_AUTH_HEADER = 'x-photon-admin-token';
const BOARD_AUTH_HEADER = 'x-photon-board-token';
const BOARD_SHARED_TOKEN = process.env.PHOTON_BOARD_SHARED_TOKEN || 'photon-board-auth-v1';
const ADMIN_CHALLENGE_TTL_MS = 5 * 60 * 1000;
const ADMIN_SESSION_TTL_MS = 12 * 60 * 60 * 1000;
const adminChallenges = new Map();
const adminSessions = new Map();

const MIME_TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.ico': 'image/x-icon',
};

let claimState = { claims: [], rgbClaims: [] };
let writeQueue = Promise.resolve();
let scanQueue = Promise.resolve();
let rgbNodeRegistry = DEFAULT_RGB_NODES.map((node) => ({ ...node }));
let rgbNodeRegistryByRef = new Map(rgbNodeRegistry.map((node) => [node.accountRef, node]));

function baseHeaders(extra = {}) {
  return {
    'Cache-Control': 'no-store',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, x-photon-wallet-key, x-photon-admin-token, x-photon-board-token',
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

function getKnownRgbAccountRefs() {
  return rgbNodeRegistry.map((node) => node.accountRef);
}

function isKnownRgbAccountRef(accountRef) {
  return rgbNodeRegistryByRef.has(accountRef);
}

function getRgbAccountRefError() {
  const refs = getKnownRgbAccountRefs();
  if (refs.length === 0) {
    return 'No RGB nodes are registered.';
  }
  return `accountRef must be one of: ${refs.join(', ')}.`;
}

async function refreshRgbNodeRegistry() {
  try {
    const rows = await listRgbNodes({ enabledOnly: true });
    if (Array.isArray(rows) && rows.length > 0) {
      rgbNodeRegistry = rows.map((row) => ({
        accountRef: row.account_ref,
        label: row.label,
        apiBase: row.api_base,
        role: row.role,
        enabled: row.enabled !== false,
        sortOrder: Number(row.sort_order || 0),
        metadata: row.metadata || {},
      }));
      rgbNodeRegistryByRef = new Map(rgbNodeRegistry.map((node) => [node.accountRef, node]));
      return rgbNodeRegistry;
    }
  } catch (error) {
    console.error(`[${nowIso()}] Failed to refresh RGB node registry:`, error.message);
  }

  rgbNodeRegistry = DEFAULT_RGB_NODES.map((node) => ({ ...node }));
  rgbNodeRegistryByRef = new Map(rgbNodeRegistry.map((node) => [node.accountRef, node]));
  return rgbNodeRegistry;
}

async function ensureRgbNodeRegistrySeeded() {
  await ensureRgbNodesTable();
  for (const node of DEFAULT_RGB_NODES) {
    await upsertRgbNode(node);
  }
  await refreshRgbNodeRegistry();
}

async function ensureBoardSupportSeeded() {
  await ensureBoardTicketStatusesTable();
  await ensureBoardTicketsTable();
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

function detectRgbInvoiceKind(invoice) {
  const normalized = String(invoice || '').trim().toLowerCase();
  if (!normalized) {
    return null;
  }
  if (normalized.startsWith('ln')) {
    return 'lightning';
  }
  return 'rgb';
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

function hasExplicitWalletKeyHeader(req) {
  const headerValue = req?.headers?.['x-photon-wallet-key'];
  return typeof headerValue === 'string' && headerValue.trim().length > 0;
}

function pruneAdminAuthState() {
  const now = Date.now();

  for (const [challengeId, challenge] of adminChallenges.entries()) {
    if ((challenge?.expiresAt || 0) <= now) {
      adminChallenges.delete(challengeId);
    }
  }

  for (const [token, session] of adminSessions.entries()) {
    if ((session?.expiresAt || 0) <= now) {
      adminSessions.delete(token);
    }
  }
}

function buildAdminChallengeMessage(address, nonce) {
  return [
    'PhotonBolt Admin Authentication',
    `address:${address}`,
    `nonce:${nonce}`,
  ].join('\n');
}

function buildPhotonMessageDigest(message) {
  return crypto
    .createHash('sha256')
    .update(Buffer.from(`Photon Signed Message:\n${message}`, 'utf8'))
    .digest();
}

function verifyPhotonAdminSignature(address, message, signatureHex) {
  const decoded = bitcoin.address.fromBech32(address);
  const outputKey = Buffer.from(decoded.data);
  const signature = Buffer.from(signatureHex, 'hex');

  if (outputKey.length !== 32) {
    throw new Error('Admin address is not a taproot address.');
  }

  if (signature.length !== 64) {
    throw new Error('Admin signature must be a 64-byte Schnorr signature.');
  }

  const digest = buildPhotonMessageDigest(message);
  return ecc.verifySchnorr(digest, outputKey, signature);
}

function getAdminSession(req) {
  pruneAdminAuthState();
  const headerValue = req.headers[ADMIN_AUTH_HEADER];
  const token = typeof headerValue === 'string' ? headerValue.trim() : '';
  if (!token) {
    return null;
  }
  const session = adminSessions.get(token);
  if (!session || session.expiresAt <= Date.now()) {
    adminSessions.delete(token);
    return null;
  }
  return { token, ...session };
}

function requireAdminSession(req, res) {
  const session = getAdminSession(req);
  if (!session) {
    sendJson(res, 403, { ok: false, error: 'Admin authentication required.' });
    return null;
  }
  return session;
}

function requireBoardSession(req, res) {
  const headerValue = req.headers[BOARD_AUTH_HEADER];
  const token = typeof headerValue === 'string' ? headerValue.trim() : '';
  if (token === BOARD_SHARED_TOKEN) {
    return { token, kind: 'board' };
  }

  const adminSession = getAdminSession(req);
  if (adminSession) {
    return { ...adminSession, kind: 'admin' };
  }

  sendJson(res, 403, { ok: false, error: 'Board login required.' });
  return null;
}

async function rgbNodeRequest(endpoint, payload = {}) {
  return rgbNodeRequestWithBase(resolveRgbNodeApiBaseForAccountRef(RGB_OWNER_ACCOUNT_REF), endpoint, payload);
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
  const node = rgbNodeRegistryByRef.get(accountRef);
  if (node?.apiBase) {
    return node.apiBase;
  }
  const fallback = rgbNodeRegistryByRef.get(RGB_OWNER_ACCOUNT_REF);
  return fallback?.apiBase || RGB_OWNER_API_BASE_DEFAULT;
}

function walletHasDedicatedRgbAccount(wallet) {
  return Boolean(wallet?.rgb_account_ref) || wallet?.wallet_key === RGB_OWNER_WALLET_KEY;
}

// Calls POST /address on the wallet's assigned node to get a fresh Bitcoin address.
// Used as the generateFundingAddress callback passed to ensureWallet() on first wallet creation.
async function generateFundingAddressForWallet(wallet) {
  const accountRef = wallet.rgb_account_ref || getDefaultAccountRefForWallet(wallet);
  const apiBase = resolveRgbNodeApiBaseForAccountRef(accountRef);
  const response = await rgbNodeRequestWithBase(apiBase, '/address', {}, 'POST');
  return response?.address || null;
}

// Drop-in replacement for ensureWallet that always wires the funding address generator.
// Use this everywhere in server.js instead of calling ensureWallet directly.
function ensureWalletWithFunding(req, network = 'regtest') {
  return ensureWallet(req, network, { generateFundingAddress: generateFundingAddressForWallet });
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

function normalizeNumericString(value) {
  const numeric = Number(value || 0);
  return Number.isFinite(numeric) ? numeric : 0;
}

function normalizeLiveRgbBalance(balance = {}) {
  return {
    settled: String(balance.settled || 0),
    future: String(balance.future || 0),
    spendable: String(balance.spendable || 0),
    offchain_outbound: String(balance.offchain_outbound || 0),
    offchain_inbound: String(balance.offchain_inbound || 0),
  };
}

function isArchivedRegistryMetadata(metadata) {
  if (!metadata || typeof metadata !== 'object') {
    return false;
  }
  const value = metadata.archived;
  if (typeof value === 'boolean') {
    return value;
  }
  if (typeof value === 'string') {
    return value.trim().toLowerCase() === 'true';
  }
  return false;
}

async function getRegistryAssetState(contractId) {
  if (typeof contractId !== 'string' || !contractId.trim()) {
    return null;
  }

  const result = await query(
    `
      SELECT
        token_name,
        ticker,
        contract_id,
        metadata
      FROM asset_registry
      WHERE contract_id = $1
      LIMIT 1
    `,
    [contractId.trim()]
  );

  if (result.rows.length === 0) {
    return null;
  }

  const row = result.rows[0];
  return {
    tokenName: row.token_name,
    ticker: row.ticker,
    contractId: row.contract_id,
    archived: isArchivedRegistryMetadata(row.metadata),
    metadata: row.metadata || {},
  };
}

async function assertAssetNotArchived(contractId) {
  const asset = await getRegistryAssetState(contractId);
  if (asset?.archived) {
    const label = asset.ticker || asset.tokenName || asset.contractId;
    const error = new Error(`RGB asset ${label} is archived and hidden from wallets.`);
    error.statusCode = 410;
    throw error;
  }
  return asset;
}

async function inspectWalletReassignmentSafety(wallet) {
  const effectiveAccountRef = wallet.rgb_account_ref || getDefaultAccountRefForWallet(wallet);

  const pendingTransfersResult = await query(
    `
      SELECT
        t.id,
        t.transfer_kind,
        t.status,
        t.settlement_status,
        t.requested_assignment_value,
        t.settled_amount,
        wa.asset_id,
        wa.ticker,
        wa.name
      FROM rgb_transfers t
      LEFT JOIN wallet_assets wa ON wa.id = t.wallet_asset_id
      WHERE t.wallet_id = $1
        AND (
          t.status IN ('WaitingCounterparty', 'WaitingConfirmations', 'Pending')
          OR (
            t.settlement_status IS NOT NULL
            AND t.settlement_status NOT IN ('SETTLED', 'VALIDATION_FAILED', 'DELIVERY_FAILED')
          )
        )
      ORDER BY t.updated_at DESC
      LIMIT 10
    `,
    [wallet.id]
  );

  const assetRows = await query(
    `
      SELECT DISTINCT
        wa.asset_id,
        wa.ticker,
        wa.name
      FROM wallet_assets wa
      WHERE wa.wallet_id = $1
        AND wa.asset_id IS NOT NULL
      ORDER BY wa.ticker NULLS LAST, wa.asset_id ASC
    `,
    [wallet.id]
  );

  const liveBalances = [];
  for (const asset of assetRows.rows) {
    const balance = await fetchWalletLightningAssetBalance(
      {
        ...wallet,
        rgb_account_ref: effectiveAccountRef,
      },
      asset.asset_id
    );

    if (!balance) {
      continue;
    }

    const parts = {
      settled: normalizeNumericString(balance.settled),
      future: normalizeNumericString(balance.future),
      spendable: normalizeNumericString(balance.spendable),
      outbound: normalizeNumericString(balance.offchain_outbound),
      inbound: normalizeNumericString(balance.offchain_inbound),
    };
    const totalExposure = parts.settled + parts.future + parts.spendable + parts.outbound + parts.inbound;
    if (totalExposure <= 0) {
      continue;
    }

    liveBalances.push({
      assetId: asset.asset_id,
      ticker: asset.ticker || asset.name || asset.asset_id,
      ...parts,
    });
  }

  const reasons = [];
  if (pendingTransfersResult.rows.length > 0) {
    const summary = pendingTransfersResult.rows
      .slice(0, 3)
      .map((row) => {
        const ticker = row.ticker || row.name || row.asset_id || 'asset';
        const amount = row.requested_assignment_value || row.settled_amount || '0';
        const settlement = row.settlement_status ? `/${row.settlement_status}` : '';
        return `${ticker} ${row.transfer_kind} ${amount} (${row.status}${settlement})`;
      })
      .join('; ');
    reasons.push(`Pending or unfinished RGB transfers exist on ${effectiveAccountRef}: ${summary}`);
  }

  if (liveBalances.length > 0) {
    const summary = liveBalances
      .slice(0, 3)
      .map((row) => {
        const parts = [];
        if (row.settled > 0) parts.push(`settled ${row.settled}`);
        if (row.future > 0) parts.push(`future ${row.future}`);
        if (row.inbound > 0) parts.push(`inbound ${row.inbound}`);
        if (row.outbound > 0) parts.push(`outbound ${row.outbound}`);
        return `${row.ticker}: ${parts.join(', ')}`;
      })
      .join('; ');
    reasons.push(`Live RGB balances still exist on ${effectiveAccountRef}: ${summary}`);
  }

  return {
    safe: reasons.length === 0,
    effectiveAccountRef,
    pendingTransfers: pendingTransfersResult.rows,
    liveBalances,
    reasons,
  };
}

function toWalletAssignmentRow(row) {
  const effectiveAccountRef =
    row.rgb_account_ref || (row.wallet_key === RGB_OWNER_WALLET_KEY ? RGB_OWNER_ACCOUNT_REF : null);

  return {
    walletKey: row.wallet_key,
    displayName: row.display_name || row.wallet_key,
    network: row.network,
    rgbAccountRef: row.rgb_account_ref || null,
    effectiveAccountRef,
    mainBtcAddress: row.main_btc_address || null,
    utxoFundingAddress: row.utxo_funding_address || null,
    lastSeenAt: row.last_seen_at || null,
    updatedAt: row.updated_at || null,
    isOwnerWallet: row.wallet_key === RGB_OWNER_WALLET_KEY,
    canAssign: row.wallet_key !== RGB_OWNER_WALLET_KEY,
  };
}

async function handleRgbWalletAssignments(res) {
  try {
    const result = await query(
      `
        SELECT
          wallet_key,
          display_name,
          network,
          rgb_account_ref,
          main_btc_address,
          utxo_funding_address,
          last_seen_at,
          updated_at
        FROM wallets
        WHERE network = 'regtest'
        ORDER BY
          CASE WHEN wallet_key = $1 THEN 0 ELSE 1 END,
          last_seen_at DESC NULLS LAST,
          updated_at DESC NULLS LAST,
          wallet_key ASC
      `,
      [RGB_OWNER_WALLET_KEY]
    );

    sendJson(res, 200, {
      ok: true,
      wallets: result.rows.map(toWalletAssignmentRow),
      accountRefs: getKnownRgbAccountRefs(),
      nodes: rgbNodeRegistry.map((node) => ({
        accountRef: node.accountRef,
        label: node.label,
        role: node.role,
        apiBase: node.apiBase,
      })),
    });
  } catch (error) {
    sendJson(res, 502, { ok: false, error: error.message });
  }
}

async function handleAdminAuthChallenge(res, parsedUrl) {
  pruneAdminAuthState();
  const address = typeof parsedUrl.searchParams.get('address') === 'string'
    ? parsedUrl.searchParams.get('address').trim()
    : '';

  if (!ADMIN_WALLET_ADDRESS) {
    sendJson(res, 503, { ok: false, error: 'Admin wallet is not configured on the server.' });
    return;
  }

  if (!address) {
    sendJson(res, 400, { ok: false, error: 'address is required.' });
    return;
  }

  if (address !== ADMIN_WALLET_ADDRESS) {
    sendJson(res, 403, { ok: false, error: 'Only the configured admin wallet can request an admin challenge.' });
    return;
  }

  const challengeId = randomUUID();
  const nonce = randomUUID();
  const message = buildAdminChallengeMessage(address, nonce);
  const expiresAt = Date.now() + ADMIN_CHALLENGE_TTL_MS;
  adminChallenges.set(challengeId, { address, message, expiresAt });

  sendJson(res, 200, {
    ok: true,
    challengeId,
    message,
    address,
    expiresAt: new Date(expiresAt).toISOString(),
  });
}

async function handleAdminAuthVerify(req, res) {
  let body;
  try {
    body = await readRequestJson(req);
  } catch (error) {
    sendJson(res, 400, { ok: false, error: error.message });
    return;
  }

  pruneAdminAuthState();

  const challengeId = typeof body.challengeId === 'string' ? body.challengeId.trim() : '';
  const address = typeof body.address === 'string' ? body.address.trim() : '';
  const signature = typeof body.signature === 'string' ? body.signature.trim() : '';

  if (!challengeId || !address || !signature) {
    sendJson(res, 400, { ok: false, error: 'challengeId, address, and signature are required.' });
    return;
  }

  if (!ADMIN_WALLET_ADDRESS) {
    sendJson(res, 503, { ok: false, error: 'Admin wallet is not configured on the server.' });
    return;
  }

  if (address !== ADMIN_WALLET_ADDRESS) {
    sendJson(res, 403, { ok: false, error: 'Only the configured admin wallet can authenticate.' });
    return;
  }

  const challenge = adminChallenges.get(challengeId);
  if (!challenge || challenge.expiresAt <= Date.now()) {
    adminChallenges.delete(challengeId);
    sendJson(res, 400, { ok: false, error: 'Admin challenge expired. Request a new one.' });
    return;
  }

  if (challenge.address !== address) {
    sendJson(res, 403, { ok: false, error: 'Admin challenge address mismatch.' });
    return;
  }

  let verified = false;
  try {
    verified = verifyPhotonAdminSignature(address, challenge.message, signature);
  } catch (error) {
    sendJson(res, 400, { ok: false, error: error.message });
    return;
  }

  if (!verified) {
    sendJson(res, 403, { ok: false, error: 'Admin signature verification failed.' });
    return;
  }

  adminChallenges.delete(challengeId);
  const token = randomUUID();
  const expiresAt = Date.now() + ADMIN_SESSION_TTL_MS;
  adminSessions.set(token, {
    address,
    expiresAt,
  });

  sendJson(res, 200, {
    ok: true,
    token,
    address,
    expiresAt: new Date(expiresAt).toISOString(),
  });
}

async function handleAdminAuthLogout(req, res) {
  const session = getAdminSession(req);
  if (session?.token) {
    adminSessions.delete(session.token);
  }
  sendJson(res, 200, { ok: true });
}

async function handleAdminAuthConfig(res) {
  sendJson(res, 200, {
    ok: true,
    adminWalletAddress: ADMIN_WALLET_ADDRESS || null,
    adminConfigured: Boolean(ADMIN_WALLET_ADDRESS),
  });
}

async function handleRgbWalletAssignmentUpdate(req, res) {
  let body;
  try {
    body = await readRequestJson(req);
  } catch (error) {
    sendJson(res, 400, { ok: false, error: error.message });
    return;
  }

  const walletKey = typeof body.walletKey === 'string' ? body.walletKey.trim() : '';
  const accountRef = typeof body.accountRef === 'string' ? body.accountRef.trim() : '';
  const forceAssign = Boolean(body.forceAssign);

  if (!walletKey) {
    sendJson(res, 400, { ok: false, error: 'walletKey is required.' });
    return;
  }

  if (!isKnownRgbAccountRef(accountRef)) {
    sendJson(res, 400, { ok: false, error: getRgbAccountRefError() });
    return;
  }

  try {
    const existing = await query(
      `
        SELECT
          id,
          wallet_key,
          display_name,
          network,
          rgb_account_ref,
          main_btc_address,
          utxo_funding_address,
          last_seen_at,
          updated_at
        FROM wallets
        WHERE wallet_key = $1
          AND network = 'regtest'
        LIMIT 1
      `,
      [walletKey]
    );

    const wallet = existing.rows[0];
    if (!wallet) {
      sendJson(res, 404, { ok: false, error: 'Wallet not found.' });
      return;
    }

    if (wallet.wallet_key === RGB_OWNER_WALLET_KEY) {
      sendJson(res, 400, { ok: false, error: 'The issuer owner wallet stays pinned to photon-rln-issuer.' });
      return;
    }

    const currentEffectiveAccountRef =
      wallet.rgb_account_ref || getDefaultAccountRefForWallet(wallet);

    if (currentEffectiveAccountRef === accountRef) {
      sendJson(res, 200, {
        ok: true,
        wallet: toWalletAssignmentRow(wallet),
        message: `${walletKey} is already assigned to ${accountRef}.`,
      });
      return;
    }

    const safety = await inspectWalletReassignmentSafety(wallet);
    if (!safety.safe && !forceAssign) {
      sendJson(res, 409, {
        ok: false,
        error: `Wallet reassignment blocked. This wallet is not clean to move from ${safety.effectiveAccountRef} to ${accountRef}. ${safety.reasons.join(' ')}`,
        details: {
          fromAccountRef: safety.effectiveAccountRef,
          toAccountRef: accountRef,
          forceAssignable: true,
          pendingTransfers: safety.pendingTransfers.map((row) => ({
            id: row.id,
            assetId: row.asset_id || null,
            ticker: row.ticker || row.name || row.asset_id || 'asset',
            transferKind: row.transfer_kind,
            status: row.status,
            settlementStatus: row.settlement_status || null,
            amount: row.requested_assignment_value || row.settled_amount || '0',
          })),
          liveBalances: safety.liveBalances,
        },
      });
      return;
    }

    await setWalletRgbAccountRef(wallet.id, accountRef);

    const updated = await query(
      `
        SELECT
          wallet_key,
          display_name,
          network,
          rgb_account_ref,
          main_btc_address,
          utxo_funding_address,
          last_seen_at,
          updated_at
        FROM wallets
        WHERE id = $1
      `,
      [wallet.id]
    );

    sendJson(res, 200, {
      ok: true,
      wallet: toWalletAssignmentRow(updated.rows[0]),
      message: `${walletKey} is now assigned to ${accountRef}.${forceAssign ? ' Forced reassignment was used.' : ''}`,
    });
  } catch (error) {
    sendJson(res, 502, { ok: false, error: error.message });
  }
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

async function listNodeChannels(apiBase) {
  const response = await rgbNodeRequestWithBase(apiBase, '/listchannels', {}, 'GET');
  return Array.isArray(response?.channels) ? response.channels : [];
}

function getChannelDashboardNodeSources() {
  return rgbNodeRegistry.map((node) => ({
    label: node.label,
    accountRef: node.accountRef,
    apiBase: node.apiBase,
  }));
}

function normalizeDashboardTimestamp(value) {
  const seconds = Number(value || 0);
  return Number.isFinite(seconds) && seconds > 0 ? new Date(seconds * 1000).toISOString() : null;
}

function deriveDashboardPaymentStatus(statuses) {
  const values = new Set((statuses || []).map((status) => String(status || '').toLowerCase()));
  if (values.has('succeeded')) return 'Succeeded';
  if (values.has('pending')) return 'Pending';
  if (values.has('failed')) return 'Failed';
  return 'Unknown';
}

async function buildChannelDashboardSnapshot() {
  const sources = getChannelDashboardNodeSources();
  const [walletRows, sourceSnapshots] = await Promise.all([
    query(
      `
        SELECT wallet_key, rgb_account_ref
        FROM wallets
        WHERE rgb_account_ref IS NOT NULL
        ORDER BY wallet_key ASC
      `
    ),
    Promise.all(
      sources.map(async (source) => {
        const [channels, payments] = await Promise.all([
          listNodeChannels(source.apiBase),
          listNodePayments(source.apiBase),
        ]);

        return { ...source, channels, payments };
      })
    ),
  ]);

  const walletKeysByAccountRef = new Map();
  for (const row of walletRows.rows) {
    const key = row.rgb_account_ref || 'unknown';
    if (!walletKeysByAccountRef.has(key)) {
      walletKeysByAccountRef.set(key, []);
    }
    walletKeysByAccountRef.get(key).push(row.wallet_key);
  }

  const assetIds = new Set();
  for (const source of sourceSnapshots) {
    for (const channel of source.channels) {
      if (channel?.asset_id) assetIds.add(channel.asset_id);
    }
    for (const payment of source.payments) {
      if (payment?.asset_id) assetIds.add(payment.asset_id);
    }
  }

  const assetRows = assetIds.size > 0
    ? await query(
      `
        SELECT DISTINCT ON (contract_id)
          contract_id,
          token_name,
          ticker,
          precision
        FROM asset_registry
        WHERE contract_id = ANY($1::text[])
        ORDER BY contract_id, created_at DESC
      `,
      [[...assetIds]]
    )
    : { rows: [] };

  const assetMetaById = new Map(
    assetRows.rows.map((row) => [
      row.contract_id,
      {
        tokenName: row.token_name || null,
        ticker: row.ticker || null,
        precision: row.precision ?? null,
      },
    ])
  );

  const channelsById = new Map();
  for (const source of sourceSnapshots) {
    for (const channel of source.channels) {
      const channelId = channel?.channel_id;
      if (!channelId) continue;

      const assetMeta = assetMetaById.get(channel.asset_id) || null;
      if (!channelsById.has(channelId)) {
        channelsById.set(channelId, {
          channelId,
          shortChannelId: channel.short_channel_id || null,
          fundingTxid: channel.funding_txid || null,
          assetId: channel.asset_id || null,
          assetName: assetMeta?.tokenName || null,
          assetTicker: assetMeta?.ticker || null,
          assetPrecision: assetMeta?.precision ?? null,
          status: channel.status || 'Unknown',
          ready: Boolean(channel.ready),
          isUsable: Boolean(channel.is_usable),
          public: Boolean(channel.public),
          capacitySat: Number(channel.capacity_sat || 0),
          nodes: [],
          payments: [],
          unmatchedPayments: [],
        });
      }

      channelsById.get(channelId).nodes.push({
        nodeLabel: source.label,
        accountRef: source.accountRef,
        apiBase: source.apiBase,
        walletKeys: walletKeysByAccountRef.get(source.accountRef) || [],
        peerPubkey: channel.peer_pubkey || null,
        localBalanceSat: Number(channel.local_balance_sat || 0),
        outboundBalanceMsat: Number(channel.outbound_balance_msat || 0),
        inboundBalanceMsat: Number(channel.inbound_balance_msat || 0),
        nextOutboundHtlcLimitMsat: Number(channel.next_outbound_htlc_limit_msat || 0),
        nextOutboundHtlcMinimumMsat: Number(channel.next_outbound_htlc_minimum_msat || 0),
        assetLocalAmount: Number(channel.asset_local_amount || 0),
        assetRemoteAmount: Number(channel.asset_remote_amount || 0),
        ready: Boolean(channel.ready),
        isUsable: Boolean(channel.is_usable),
        status: channel.status || 'Unknown',
      });
    }
  }

  for (const source of sourceSnapshots) {
    const sourceChannels = source.channels;
    for (const payment of source.payments) {
      const paymentHash = payment?.payment_hash;
      if (!paymentHash) continue;

      let matchedChannels = sourceChannels.filter((channel) => {
        if (!channel?.channel_id) return false;
        if (channel.peer_pubkey !== payment.payee_pubkey) return false;
        if (payment.asset_id) return channel.asset_id === payment.asset_id;
        return true;
      });

      if (!payment.asset_id && matchedChannels.length > 1) {
        matchedChannels = [];
      }

      if (matchedChannels.length === 0) {
        continue;
      }

      const matchedChannel = channelsById.get(matchedChannels[0].channel_id);
      if (!matchedChannel) continue;

      let paymentEntry = matchedChannel.payments.find((entry) => entry.paymentHash === paymentHash);
      if (!paymentEntry) {
        paymentEntry = {
          paymentHash,
          assetId: payment.asset_id || matchedChannel.assetId || null,
          assetName: matchedChannel.assetName,
          assetTicker: matchedChannel.assetTicker,
          assetAmount: payment.asset_amount ?? null,
          amtMsat: payment.amt_msat ?? null,
          createdAt: normalizeDashboardTimestamp(payment.created_at),
          updatedAt: normalizeDashboardTimestamp(payment.updated_at),
          overallStatus: payment.status || 'Unknown',
          nodeStatuses: [],
        };
        matchedChannel.payments.push(paymentEntry);
      }

      paymentEntry.assetAmount = paymentEntry.assetAmount ?? payment.asset_amount ?? null;
      paymentEntry.amtMsat = paymentEntry.amtMsat ?? payment.amt_msat ?? null;
      paymentEntry.createdAt = paymentEntry.createdAt || normalizeDashboardTimestamp(payment.created_at);
      paymentEntry.updatedAt = normalizeDashboardTimestamp(payment.updated_at) || paymentEntry.updatedAt;
      paymentEntry.nodeStatuses.push({
        nodeLabel: source.label,
        accountRef: source.accountRef,
        direction: payment.inbound ? 'Inbound' : 'Outbound',
        status: payment.status || 'Unknown',
        payeePubkey: payment.payee_pubkey || null,
      });
      paymentEntry.overallStatus = deriveDashboardPaymentStatus(
        paymentEntry.nodeStatuses.map((status) => status.status)
      );
    }
  }

  const channels = [...channelsById.values()]
    .map((channel) => {
      const maxLocalAssetAmount = channel.nodes.reduce(
        (max, node) => Math.max(max, Number(node.assetLocalAmount || 0)),
        0
      );
      const maxRemoteAssetAmount = channel.nodes.reduce(
        (max, node) => Math.max(max, Number(node.assetRemoteAmount || 0)),
        0
      );
      const maxOutboundBalanceMsat = channel.nodes.reduce(
        (max, node) => Math.max(max, Number(node.outboundBalanceMsat || 0)),
        0
      );
      const maxInboundBalanceMsat = channel.nodes.reduce(
        (max, node) => Math.max(max, Number(node.inboundBalanceMsat || 0)),
        0
      );
      const nextOutboundHtlcLimitMsat = channel.nodes.reduce(
        (max, node) => Math.max(max, Number(node.nextOutboundHtlcLimitMsat || 0)),
        0
      );

      channel.payments.sort((left, right) => {
        const a = Date.parse(right.updatedAt || right.createdAt || 0);
        const b = Date.parse(left.updatedAt || left.createdAt || 0);
        return a - b;
      });

      return {
        ...channel,
        totalAssetLiquidity: Math.max(
          maxLocalAssetAmount + maxRemoteAssetAmount,
          Number(channel.nodes[0]?.assetLocalAmount || 0) + Number(channel.nodes[0]?.assetRemoteAmount || 0)
        ),
        maxLocalAssetAmount,
        maxRemoteAssetAmount,
        maxOutboundBalanceMsat,
        maxInboundBalanceMsat,
        nextOutboundHtlcLimitMsat,
        totalPaymentCount: channel.payments.length,
        settledPaymentCount: channel.payments.filter((payment) => payment.overallStatus === 'Succeeded').length,
        failedPaymentCount: channel.payments.filter((payment) => payment.overallStatus === 'Failed').length,
        pendingPaymentCount: channel.payments.filter((payment) => payment.overallStatus === 'Pending').length,
      };
    })
    .sort((left, right) => {
      const leftUpdated = left.payments[0]?.updatedAt || '';
      const rightUpdated = right.payments[0]?.updatedAt || '';
      return rightUpdated.localeCompare(leftUpdated);
    });

  return {
    ok: true,
    refreshedAt: nowIso(),
    nodeSources: sources.map((source) => ({
      label: source.label,
      accountRef: source.accountRef,
      apiBase: source.apiBase,
      walletKeys: walletKeysByAccountRef.get(source.accountRef) || [],
    })),
    channels,
  };
}

async function handleRgbChannelDashboard(res) {
  try {
    const snapshot = await buildChannelDashboardSnapshot();
    sendJson(res, 200, snapshot);
  } catch (error) {
    console.error(`[${nowIso()}] [RGB API] Channel dashboard lookup failed:`, error.message);
    sendJson(res, 502, { ok: false, error: error.message });
  }
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

function deriveLightningSettlementStatus(direction, paymentStatus) {
  const status = paymentStatus ? String(paymentStatus) : null;
  if (status === 'Succeeded') {
    return 'SETTLED';
  }
  if (status === 'Failed') {
    return direction === 'incoming' ? 'VALIDATION_FAILED' : 'DELIVERY_FAILED';
  }
  return null;
}

async function syncWalletLightningPayments(wallet, assetId = null, walletAssetId = null) {
  if (!walletHasDedicatedRgbAccount(wallet)) {
    return new Set();
  }

  const { apiBase } = resolveWalletNodeContext(wallet);
  const payments = await listNodePayments(apiBase);
  const filtered = payments.filter((payment) => {
    if (!payment || typeof payment !== 'object') return false;
    if (!payment.asset_id) return false;
    return assetId ? payment.asset_id === assetId : true;
  });

  if (filtered.length === 0) {
    return new Set();
  }

  const assetCache = new Map();
  const touchedWalletAssetIds = new Set();

  for (const payment of filtered) {
    const paymentAssetId = String(payment.asset_id);
    let resolvedWalletAssetId = walletAssetId && (!assetId || assetId === paymentAssetId) ? walletAssetId : null;

    if (!resolvedWalletAssetId) {
      if (!assetCache.has(paymentAssetId)) {
        assetCache.set(
          paymentAssetId,
          syncWalletAssetFromRgbNode({ walletId: wallet.id, assetId: paymentAssetId })
            .then((synced) => synced.walletAsset.id)
        );
      }
      resolvedWalletAssetId = await assetCache.get(paymentAssetId);
    }

    const direction = payment.inbound ? 'incoming' : 'outgoing';
    await upsertLightningPaymentTransfer({
      wallet,
      walletAssetId: resolvedWalletAssetId,
      payment,
      direction,
      invoice: null,
      settlementStatus: deriveLightningSettlementStatus(direction, payment.status),
    });
    touchedWalletAssetIds.add(resolvedWalletAssetId);
  }

  return touchedWalletAssetIds;
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
    console.warn(`${tag} — no recipient_id available from Lightning metadata; skipping proxy verification and waiting for payment sync.`);
    await recordTransferEvent({
      walletId: wallet.id,
      transferId,
      eventType: 'rgb_consignment_pending',
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

async function findStoredInvoiceByString(invoiceString) {
  if (!invoiceString) {
    return null;
  }

  const result = await query(
    `
      SELECT
        i.id,
        i.wallet_id,
        i.wallet_asset_id,
        i.invoice_string,
        i.recipient_id,
        i.status,
        i.metadata,
        w.wallet_key,
        w.rgb_account_ref,
        wa.asset_id,
        wa.contract_id,
        wa.ticker,
        wa.name,
        wa.precision
      FROM rgb_invoices i
      JOIN wallets w
        ON w.id = i.wallet_id
      LEFT JOIN wallet_assets wa
        ON wa.id = i.wallet_asset_id
      WHERE i.invoice_string = $1
      LIMIT 1
    `,
    [invoiceString]
  );

  return result.rows[0] || null;
}

async function findExistingSameNodeTransfer(walletId, invoiceString) {
  if (!walletId || !invoiceString) {
    return null;
  }

  const result = await query(
    `
      SELECT
        id,
        wallet_asset_id,
        status,
        settled_amount,
        settled_at,
        metadata
      FROM rgb_transfers
      WHERE wallet_id = $1
        AND metadata->>'route' = 'internal_same_node'
        AND metadata->>'invoice' = $2
      ORDER BY created_at DESC
      LIMIT 1
    `,
    [walletId, invoiceString]
  );

  return result.rows[0] || null;
}

async function findRgbTransferByIdx(assetId, transferIdx, apiBase = null) {
  if (!assetId || !Number.isFinite(Number(transferIdx))) {
    return null;
  }

  const transferList = apiBase
    ? await rgbNodeRequestWithBase(apiBase, '/listtransfers', { asset_id: assetId })
    : await rgbNodeRequest('/listtransfers', { asset_id: assetId });
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
  const wallet = await ensureWalletWithFunding(req, network);
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

    const secret =
      existing.rows[0]?.blinding_secret ||
      (transfer.status === 'Settled' && transfer.recipient_id
        ? `backend-managed:${transfer.recipient_id}`
        : null);
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
            blinding_secret = COALESCE(blinding_secret, $5),
            blinding_secret_status = $6,
            metadata = $7
          WHERE id = $8
        `,
        [
          transfer._dbId || null,
          invoice?.id || null,
          transfer.txid || null,
          nextStatus,
          secret,
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
    // Do not include DELIVERY_FAILED here: failed deliveries are no longer
    // actively "sending" and should not inflate the outbound badge.
    if (
      direction === 'outgoing' &&
      (settlementStatus === 'INITIATED' ||
        settlementStatus === 'PAYMENT_SUCCESS' ||
        settlementStatus === 'CONSIGNMENT_UPLOADED')
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
        if (transferKind === 'Issuance' || transferKind === 'LightningReceive') {
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

    if (
      direction === 'incoming' &&
      transferKind !== 'Issuance' &&
      status !== 'WaitingCounterparty' &&
      settlementStatus !== 'VALIDATION_FAILED'
    ) {
      offchainInbound += requestedAmount;
    } else if (direction === 'outgoing' && settlementStatus !== 'DELIVERY_FAILED') {
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
  const walletResult = await query(
    `
      SELECT id, wallet_key, rgb_account_ref
      FROM wallets
      WHERE id = $1
      LIMIT 1
    `,
    [walletId]
  );
  const walletRow = walletResult.rows[0];
  if (!walletRow) {
    throw new Error(`Wallet ${walletId} was not found while syncing asset ${assetId}`);
  }

  const assetApiBase = resolveRgbNodeApiBaseForAccountRef(
    walletRow.rgb_account_ref || getDefaultAccountRefForWallet(walletRow)
  );
  const assetList = await rgbNodeRequestWithBase(assetApiBase, '/listassets', {
    filter_asset_schemas: ['Nia', 'Uda', 'Cfa'],
  });

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
  if (!walletHasDedicatedRgbAccount(wallet)) {
    return [];
  }

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

    // ── UTXO slot state transitions ──────────────────────────────────────────
    // params[2] = direction, params[4] = status, params[8] = receive_utxo
    const slotDirection = params[2];
    const slotStatus    = params[4];
    const slotUtxo      = params[8];
    if (slotStatus === 'Settled' && slotUtxo) {
      try {
        if (slotDirection === 'incoming') {
          const updated = await markSlotOccupied(slotUtxo, transfer._dbId);
          if (updated) console.log(`[${nowIso()}] [UTXO] slot OCCUPIED outpoint=${slotUtxo} transfer=${transfer._dbId}`);
        } else if (slotDirection === 'outgoing') {
          const updated = await markSlotEmpty(slotUtxo);
          if (updated) console.log(`[${nowIso()}] [UTXO] slot EMPTY outpoint=${slotUtxo}`);
        }
      } catch (slotErr) {
        console.warn(`[${nowIso()}] [UTXO] slot transition failed outpoint=${slotUtxo}:`, slotErr.message);
      }
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
    await assertAssetNotArchived(assetId);
    const wallet = await ensureWalletWithFunding(req, 'regtest');
    if (!walletHasDedicatedRgbAccount(wallet)) {
      sendJson(res, 200, {
        ok: true,
        walletKey: wallet.wallet_key,
        asset: {
          assetId,
          ticker: null,
          name: assetId,
          precision: 0,
        },
        balance: {
          settled: '0',
          future: '0',
          spendable: '0',
          offchain_outbound: '0',
          offchain_inbound: '0',
          locked_missing_secret: '0',
          locked_unconfirmed: '0',
          spendability_status: 'spendable',
        },
      });
      return;
    }

    const synced = await syncWalletAssetFromRgbNode({ walletId: wallet.id, assetId });
    const transfers = await syncWalletTransferRows(wallet, assetId, synced.walletAsset.id);
    await syncWalletLightningPayments(wallet, assetId, synced.walletAsset.id);
    await reconcileWalletConsignmentSecrets(wallet, synced.walletAsset.id, transfers);
    const derivedBalance = await deriveWalletScopedBalance(synced.walletAsset.id);
    const balance = synced.asset?.balance
      ? {
        ...normalizeLiveRgbBalance(synced.asset.balance),
        locked_missing_secret: '0',
        locked_unconfirmed: '0',
        spendability_status: 'spendable',
      }
      : derivedBalance;
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
    sendJson(res, error.statusCode || 502, { ok: false, error: error.message });
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
    const wallet = await ensureWalletWithFunding(req, 'regtest');
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

  if (!hasExplicitWalletKeyHeader(req)) {
    sendJson(res, 400, {
      ok: false,
      error: 'x-photon-wallet-key header is required for RGB Lightning invoice generation to avoid creating the invoice on the wrong node.',
    });
    return;
  }

  try {
    await assertAssetNotArchived(assetId);
    const wallet = await ensureWalletWithFunding(req, 'regtest');
    const synced = await syncWalletAssetFromRgbNode({ walletId: wallet.id, assetId });
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

    const expirationTimestamp =
      Number.isFinite(Number(decoded?.timestamp)) && Number.isFinite(Number(decoded?.expiry_sec))
        ? Number(decoded.timestamp) + Number(decoded.expiry_sec)
        : null;

    await recordRgbInvoice({
      walletId: wallet.id,
      walletAssetId: synced.walletAsset.id,
      invoice: {
        invoice: invoiceResponse.invoice,
        recipient_id:
          (typeof decoded?.payment_hash === 'string' && decoded.payment_hash.trim()) ||
          (typeof invoiceResponse?.invoice_id === 'string' && invoiceResponse.invoice_id.trim()) ||
          `ln:${randomUUID()}`,
        recipient_type: 'LightningPaymentHash',
        assignment: { type: 'Fungible', value: assetAmount },
        expiration_timestamp: expirationTimestamp,
        payment_hash: decoded?.payment_hash || null,
        payee_pubkey: decoded?.payee_pubkey || null,
        asset_id: decoded?.asset_id || assetId,
        asset_amount: decoded?.asset_amount ?? assetAmount,
        amt_msat: decoded?.amt_msat ?? amtMsat,
        expiry_sec: decoded?.expiry_sec ?? expirySec,
        timestamp: decoded?.timestamp || null,
        account_ref: resolveWalletNodeContext(wallet).accountRef,
        invoice_kind: 'lightning',
      },
      openAmount: false,
      proxyEndpoint: null,
    });

    sendJson(res, 200, {
      ok: true,
      walletKey: wallet.wallet_key,
      invoice: invoiceResponse.invoice,
      decoded,
    });
  } catch (error) {
    console.error(`[${nowIso()}] [RGB API] Lightning invoice generation failed:`, error.message);
    sendJson(res, error.statusCode || 502, { ok: false, error: error.message });
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
    const wallet = await ensureWalletWithFunding(req, 'regtest');
    if (!walletHasDedicatedRgbAccount(wallet)) {
      sendJson(res, 200, {
        ok: true,
        walletKey: wallet.wallet_key,
        assetId,
        balance: {
          settled: '0',
          future: '0',
          spendable: '0',
          offchain_outbound: '0',
          offchain_inbound: '0',
          locked_missing_secret: '0',
          locked_unconfirmed: '0',
          spendability_status: 'spendable',
        },
        transfers: [],
      });
      return;
    }

    const synced = await syncWalletAssetFromRgbNode({ walletId: wallet.id, assetId });
    const runtimeTransfers = await syncWalletTransferRows(wallet, assetId, synced.walletAsset.id);
    await syncWalletLightningPayments(wallet, assetId, synced.walletAsset.id);
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
    const wallet = await ensureWalletWithFunding(req, 'regtest');
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

    await assertAssetNotArchived(assetId);

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
    sendJson(res, error.statusCode || 502, { ok: false, error: error.message });
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
    const wallet = await ensureWalletWithFunding(req, 'regtest');
    const { apiBase: walletApiBase } = resolveWalletNodeContext(wallet);
    await rgbNodeRequestWithBase(walletApiBase, '/refreshtransfers', { skip_sync: false });
    const synced = await syncWalletAssetFromRgbNode({ walletId: wallet.id, assetId });
    const runtimeTransfers = await syncWalletTransferRows(wallet, assetId, synced.walletAsset.id);
    await syncWalletLightningPayments(wallet, assetId, synced.walletAsset.id);
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

async function executeSameNodeWalletTransfer({
  senderWallet,
  senderAccountRef,
  receiverInvoice,
  decoded,
  invoice,
  eventSource,
}) {
  if (!receiverInvoice?.id) {
    throw new Error('Stored receiver invoice is required for same-node transfer.');
  }

  if (receiverInvoice.wallet_id === senderWallet.id) {
    throw new Error('Cannot pay your own invoice from the same wallet.');
  }

  if (!['open', 'pending_consignment', 'acknowledged'].includes(receiverInvoice.status)) {
    const existingTransfer = await findExistingSameNodeTransfer(senderWallet.id, invoice);
    if (existingTransfer) {
      const assetId = receiverInvoice.asset_id || decoded?.asset_id || null;
      if (!assetId) {
        throw new Error('Existing same-node transfer is missing an asset id.');
      }
      const senderBalance = await deriveWalletScopedBalance(existingTransfer.wallet_asset_id);
      await upsertWalletAssetBalance(existingTransfer.wallet_asset_id, senderBalance);
      return {
        wallet: senderWallet,
        assetId,
        balance: senderBalance,
        payment: {
          ...decoded,
          status: existingTransfer.status === 'Settled' ? 'Succeeded' : existingTransfer.status,
          inbound: false,
        },
        paymentResult: {
          payment_hash: decoded?.payment_hash || existingTransfer.metadata?.payment_hash || null,
          status: existingTransfer.status === 'Settled' ? 'Succeeded' : existingTransfer.status,
        },
        decoded,
      };
    }
    throw new Error('Lightning invoice is no longer open.');
  }

  const assetId = receiverInvoice.asset_id || decoded?.asset_id || null;
  const assetAmountValue = decoded?.asset_amount;
  if (!assetId) {
    throw new Error('Stored Lightning invoice is missing an RGB asset id.');
  }
  if (assetAmountValue === undefined || assetAmountValue === null || Number(assetAmountValue) <= 0) {
    throw new Error('Stored Lightning invoice is missing a valid RGB asset amount.');
  }

  const senderSynced = await syncWalletAssetFromRgbNode({ walletId: senderWallet.id, assetId });
  const receiverWallet = {
    id: receiverInvoice.wallet_id,
    wallet_key: receiverInvoice.wallet_key,
    rgb_account_ref: receiverInvoice.rgb_account_ref,
  };
  const receiverWalletAsset =
    receiverInvoice.wallet_asset_id
      ? { id: receiverInvoice.wallet_asset_id }
      : (await syncWalletAssetFromRgbNode({ walletId: receiverWallet.id, assetId })).walletAsset;

  const senderBalanceBefore = await deriveWalletScopedBalance(senderSynced.walletAsset.id);
  const requestedAmount = BigInt(normalizeTransferAmount(assetAmountValue));
  const availableAmount =
    BigInt(normalizeTransferAmount(senderBalanceBefore.spendable)) +
    BigInt(normalizeTransferAmount(senderBalanceBefore.offchain_outbound));
  if (availableAmount < requestedAmount) {
    throw new Error(
      `Insufficient spendable balance for same-node transfer. Required ${requestedAmount.toString()}, available ${availableAmount.toString()}.`
    );
  }

  const sameNodeTransferId = randomUUID();
  const paymentHash =
    (typeof decoded?.payment_hash === 'string' && decoded.payment_hash.trim()) ||
    (typeof receiverInvoice.recipient_id === 'string' && receiverInvoice.recipient_id.trim()) ||
    null;
  const settledAt = new Date();
  const outgoingMetadata = {
    payment_hash: paymentHash,
    payment_secret: decoded?.payment_secret || null,
    payee_pubkey: decoded?.payee_pubkey || null,
    route: 'internal_same_node',
    invoice,
    amt_msat: decoded?.amt_msat ?? null,
    inbound: false,
    same_node_transfer_id: sameNodeTransferId,
    node_account_ref: senderAccountRef,
    receiver_wallet_id: receiverWallet.id,
    receiver_wallet_key: receiverWallet.wallet_key,
  };
  const incomingMetadata = {
    payment_hash: paymentHash,
    payment_secret: decoded?.payment_secret || null,
    payee_pubkey: decoded?.payee_pubkey || null,
    route: 'internal_same_node',
    invoice,
    amt_msat: decoded?.amt_msat ?? null,
    inbound: true,
    same_node_transfer_id: sameNodeTransferId,
    node_account_ref: senderAccountRef,
    sender_wallet_id: senderWallet.id,
    sender_wallet_key: senderWallet.wallet_key,
  };

  const txResult = await withTransaction(async (client) => {
    const invoiceUpdate = await client.query(
      `
        UPDATE rgb_invoices
        SET
          status = 'settled',
          metadata = COALESCE(metadata, '{}'::jsonb) || $2::jsonb
        WHERE id = $1
          AND status IN ('open', 'pending_consignment', 'acknowledged')
        RETURNING id
      `,
      [
        receiverInvoice.id,
        JSON.stringify({
          route: 'internal_same_node',
          sameNodeTransferId,
          settledAt: settledAt.toISOString(),
          settledByWalletId: senderWallet.id,
          settledByWalletKey: senderWallet.wallet_key,
        }),
      ]
    );

    if (invoiceUpdate.rows.length === 0) {
      throw new Error('Lightning invoice is no longer open.');
    }

    const outgoingTransfer = await client.query(
      `
        INSERT INTO rgb_transfers (
          wallet_id,
          wallet_asset_id,
          invoice_id,
          direction,
          transfer_kind,
          status,
          recipient_id,
          requested_assignment_type,
          requested_assignment_value,
          settled_amount,
          settled_at,
          metadata,
          settlement_status
        )
        VALUES ($1, $2, $3, 'outgoing', 'LightningSend', 'Settled', $4, 'Fungible', $5, $5, $6, $7, 'SETTLED'::settlement_status_enum)
        RETURNING id
      `,
      [
        senderWallet.id,
        senderSynced.walletAsset.id,
        receiverInvoice.id,
        receiverInvoice.recipient_id,
        requestedAmount.toString(),
        settledAt,
        JSON.stringify(outgoingMetadata),
      ]
    );

    const incomingTransfer = await client.query(
      `
        INSERT INTO rgb_transfers (
          wallet_id,
          wallet_asset_id,
          invoice_id,
          direction,
          transfer_kind,
          status,
          recipient_id,
          requested_assignment_type,
          requested_assignment_value,
          settled_amount,
          settled_at,
          metadata,
          settlement_status
        )
        VALUES ($1, $2, $3, 'incoming', 'LightningReceive', 'Settled', $4, 'Fungible', $5, $5, $6, $7, 'SETTLED'::settlement_status_enum)
        RETURNING id
      `,
      [
        receiverWallet.id,
        receiverWalletAsset.id,
        receiverInvoice.id,
        receiverInvoice.recipient_id,
        requestedAmount.toString(),
        settledAt,
        JSON.stringify(incomingMetadata),
      ]
    );

    await client.query(
      `
        INSERT INTO transfer_events (
          wallet_id,
          transfer_id,
          invoice_id,
          event_type,
          event_source,
          payload
        )
        VALUES
          ($1, $2, $3, 'same_node_transfer_sent', $4, $5),
          ($6, $7, $3, 'same_node_transfer_received', $4, $8)
      `,
      [
        senderWallet.id,
        outgoingTransfer.rows[0].id,
        receiverInvoice.id,
        eventSource,
        JSON.stringify({
          sameNodeTransferId,
          invoice,
          assetId,
          assetAmount: requestedAmount.toString(),
          counterpartyWalletId: receiverWallet.id,
          counterpartyWalletKey: receiverWallet.wallet_key,
          nodeAccountRef: senderAccountRef,
        }),
        receiverWallet.id,
        incomingTransfer.rows[0].id,
        JSON.stringify({
          sameNodeTransferId,
          invoice,
          assetId,
          assetAmount: requestedAmount.toString(),
          counterpartyWalletId: senderWallet.id,
          counterpartyWalletKey: senderWallet.wallet_key,
          nodeAccountRef: senderAccountRef,
        }),
      ]
    );

    return {
      outgoingTransferId: outgoingTransfer.rows[0].id,
      incomingTransferId: incomingTransfer.rows[0].id,
    };
  });

  const senderBalance = await deriveWalletScopedBalance(senderSynced.walletAsset.id);
  const receiverBalance = await deriveWalletScopedBalance(receiverWalletAsset.id);
  await upsertWalletAssetBalance(senderSynced.walletAsset.id, senderBalance);
  await upsertWalletAssetBalance(receiverWalletAsset.id, receiverBalance);

  const payment = {
    ...decoded,
    payment_hash: paymentHash,
    asset_id: assetId,
    asset_amount: requestedAmount.toString(),
    status: 'Succeeded',
    inbound: false,
  };

  return {
    wallet: senderWallet,
    assetId,
    balance: senderBalance,
    payment,
    paymentResult: {
      payment_hash: paymentHash,
      status: 'Succeeded',
      transfer_id: txResult.outgoingTransferId,
    },
    decoded,
  };
}

async function executeRgbLightningPayment({ req, invoice, eventSource = 'wallet_api' }) {
  const wallet = await ensureWalletWithFunding(req, 'regtest');
  const { accountRef, apiBase } = resolveWalletNodeContext(wallet);
  const decoded = await rgbNodeRequestWithBase(apiBase, '/decodelninvoice', { invoice });
  const assetId = typeof decoded?.asset_id === 'string' ? decoded.asset_id : null;
  const payeePubkey = typeof decoded?.payee_pubkey === 'string' ? decoded.payee_pubkey : null;
  const storedInvoice = await findStoredInvoiceByString(invoice);

  if (!assetId) {
    throw new Error('Lightning invoice is missing an RGB asset id.');
  }

  if (storedInvoice?.wallet_id === wallet.id) {
    throw new Error('Cannot pay your own invoice from the same wallet.');
  }

  if (storedInvoice) {
    const receiverWallet = {
      wallet_key: storedInvoice.wallet_key,
      rgb_account_ref: storedInvoice.rgb_account_ref,
    };
    const receiverAccountRef = storedInvoice.rgb_account_ref || getDefaultAccountRefForWallet(receiverWallet);
    if (receiverAccountRef === accountRef) {
      return executeSameNodeWalletTransfer({
        senderWallet: wallet,
        senderAccountRef: accountRef,
        receiverInvoice: storedInvoice,
        decoded,
        invoice,
        eventSource,
      });
    }
  }

  if (payeePubkey) {
    const nodeInfo = await rgbNodeRequestWithBase(apiBase, '/nodeinfo', {}, 'GET');
    const senderPubkey = typeof nodeInfo?.pubkey === 'string' ? nodeInfo.pubkey : null;
    if (senderPubkey && senderPubkey === payeePubkey) {
      throw new Error(
        `RGB Lightning invoice belongs to the sender node. Generate the invoice from a different wallet/node first (${senderPubkey}).`
      );
    }
  }

  const synced = await syncWalletAssetFromRgbNode({ walletId: wallet.id, assetId });

  const transferId = await upsertLightningPaymentTransfer({
    wallet,
    walletAssetId: synced.walletAsset.id,
    payment: { ...decoded, status: 'Pending' },
    direction: 'outgoing',
    invoice,
    settlementStatus: 'INITIATED',
  });

  const paymentResult = await rgbNodeRequestWithBase(apiBase, '/sendpayment', { invoice });
  const paymentHash = paymentResult?.payment_hash || decoded?.payment_hash || null;
  let payment = {
    ...decoded,
    ...paymentResult,
    status: paymentResult?.status || 'Pending',
    inbound: false,
  };
  const shouldRetryPending = Boolean(paymentHash);

  if (paymentHash) {
    for (let attempt = 0; attempt < 15; attempt += 1) {
      const payments = await listNodePayments(apiBase);
      const matched = payments.find((entry) => entry.payment_hash === paymentHash);
      if (matched) {
        payment = matched;
        if (matched.status === 'Succeeded' || matched.status === 'Failed') {
          break;
        }
      }

      if (!shouldRetryPending || attempt === 14) {
        break;
      }

      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
  }

  console.log(`[${nowIso()}] [RGB API] sendpayment result`, {
    eventSource,
    accountRef,
    assetId,
    paymentHash,
    paymentResult,
    matchedPayment: payment,
  });

  if (payment.status !== 'Succeeded') {
    const failedSettlement = deriveLightningSettlementStatus('outgoing', payment.status) || 'DELIVERY_FAILED';
    await updateLightningTransferSettlementStatus(transferId, failedSettlement, payment);
    console.warn(`[${nowIso()}] [RGB API] sendpayment did not succeed`, {
      eventSource,
      accountRef,
      assetId,
      paymentHash: payment.payment_hash || paymentResult?.payment_hash || decoded?.payment_hash || null,
      paymentStatus: payment.status || paymentResult?.status || 'unknown',
      paymentResult,
      matchedPayment: payment,
    });
    throw new Error(
      `RGB Lightning payment failed: ${payment.status || paymentResult?.status || 'unknown'}${payment.payment_hash ? ` (${payment.payment_hash})` : ''}`
    );
  }

  await updateLightningTransferSettlementStatus(transferId, 'PAYMENT_SUCCESS', payment);

  await recordTransferEvent({
    walletId: wallet.id,
    transferId,
    eventType: 'rgb_lightning_payment',
    eventSource,
    payload: {
      assetId,
      paymentHash: payment.payment_hash || null,
      amtMsat: payment.amt_msat ?? null,
      assetAmount: payment.asset_amount ?? null,
    },
  });

  setImmediate(() => {
    orchestrateConsignmentUpload({ transferId, wallet, assetId, payment, decoded, invoice }).catch((err) =>
      console.error(`[${nowIso()}] [RGB Settlement] Consignment orchestration error:`, err.message)
    );
  });

  await setWalletRgbAccountRef(wallet.id, accountRef);
  wallet.rgb_account_ref = accountRef;

  await syncWalletLightningPayments(wallet, assetId, synced.walletAsset.id);

  const derivedBalance = await deriveWalletScopedBalance(synced.walletAsset.id);
  const balance = derivedBalance;
  await upsertWalletAssetBalance(synced.walletAsset.id, balance);

  return {
    wallet,
    assetId,
    balance,
    decoded,
    payment,
    paymentResult,
  };
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
    const { wallet, assetId, balance, payment, paymentResult, decoded } = await executeRgbLightningPayment({
      req,
      invoice,
      eventSource: 'wallet_api',
    });

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
    const wallet = await ensureWalletWithFunding(req, 'regtest');
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

// ─────────────────────────────────────────────────────────────────────────────
// UTXO Slot Management API
// ─────────────────────────────────────────────────────────────────────────────

const UTXO_SLOT_SATS = Number(process.env.UTXO_SLOT_SATS || 33000); // 0.00033 BTC

/*
 * GET /api/utxo/funding-address
 *
 * Returns the wallet's permanent node-generated UTXO funding address and the
 * exact amount the user must send to create one UTXO slot. The extension shows
 * this address + QR code when the user clicks "Create UTXO".
 *
 * If the wallet somehow has no funding address yet (e.g. created before this
 * feature was deployed) we generate one on-demand and store it.
 */
async function handleGetFundingAddress(req, res) {
  try {
    const wallet = await ensureWalletWithFunding(req, 'regtest');

    // Fallback: if still missing after ensureWallet, generate now
    let fundingAddress = wallet.utxo_funding_address;
    if (!fundingAddress) {
      fundingAddress = await generateFundingAddressForWallet(wallet);
      if (fundingAddress) {
        await setWalletFundingAddress({ walletId: wallet.id, utxoFundingAddress: fundingAddress });
      }
    }

    if (!fundingAddress) {
      sendJson(res, 503, { ok: false, error: 'Could not generate a UTXO funding address. Node may be offline.' });
      return;
    }

    // Ensure a pending deposit request exists so the watcher starts monitoring.
    // Skip if one is already active (pending or confirming).
    const existing = await getActiveDepositRequestForWallet(wallet.id);
    if (!existing) {
      const { accountRef } = resolveWalletNodeContext(wallet);
      await createDepositRequest({
        walletId: wallet.id,
        depositAddress: fundingAddress,
        expectedSats: UTXO_SLOT_SATS,
        requiredConfirmations: DEPOSIT_REQUIRED_CONFIRMATIONS,
        nodeAccountRef: accountRef,
      });
      console.log(`[${nowIso()}] [UTXO] Created pending deposit request for wallet=${wallet.wallet_key}`);
    }

    sendJson(res, 200, {
      ok: true,
      fundingAddress,
      expectedSats: UTXO_SLOT_SATS,
      expectedBtc: (UTXO_SLOT_SATS / 1e8).toFixed(8),
      label: `PhotonBolt UTXO slot — send exactly ${(UTXO_SLOT_SATS / 1e8).toFixed(8)} BTC`,
      walletKey: wallet.wallet_key,
    });
  } catch (error) {
    console.error(`[${nowIso()}] [UTXO] funding-address failed:`, error.message);
    sendJson(res, 502, { ok: false, error: error.message });
  }
}

function formatChannelApplicationResponse(application) {
  if (!application) {
    return null;
  }

  const btcDepositStatus = application.btc_deposit_status || 'pending';
  const btcFunded = btcDepositStatus === 'confirmed';
  const rgbFunded = application.status === 'funded' || application.status === 'channel_active' || application.status === 'rgb_funded';
  let status = application.status || 'pending_funding';

  if (status === 'pending_funding' && btcFunded) {
    status = 'btc_funded';
  }

  return {
    id: application.id,
    ownerWalletAddress: application.owner_wallet_address,
    ownerWalletKey: application.owner_wallet_key || null,
    accountRef: application.account_ref,
    peerPubkey: application.peer_pubkey,
    btcDepositAddress: application.btc_deposit_address,
    btcAmountSats: Number(application.btc_amount_sats || 0),
    btcDepositStatus,
    btcDepositTxid: application.deposit_txid || null,
    btcReceivedSats: Number(application.received_sats || 0),
    btcConfirmations: Number(application.confirmations || 0),
    btcFunded,
    rgbInvoiceId: application.rgb_invoice_id || null,
    rgbInvoice: application.rgb_invoice || null,
    rgbAssetId: application.rgb_asset_id,
    rgbAssetAmount: Number(application.rgb_asset_amount || 0),
    rgbFunded,
    status,
    txCounter: Number(application.tx_counter || 0),
    txThreshold: Number(application.tx_threshold || 100),
    commissionRateSats: Number(application.commission_rate_sats || 0),
    earnedFeesSats: Number(application.earned_fees_sats || 0),
    channelId: application.channel_id || null,
    createdAt: application.created_at,
    updatedAt: application.updated_at,
    fundingProgress: {
      completed: (btcFunded ? 1 : 0) + (rgbFunded ? 1 : 0),
      total: 2,
    },
    metadata: application.metadata || {},
  };
}

async function handleChannelApplicationCreate(req, res) {
  let body;
  try {
    body = await readRequestJson(req);
  } catch (error) {
    sendJson(res, 400, { ok: false, error: error.message });
    return;
  }

  const ownerWalletAddress = typeof body.ownerWalletAddress === 'string' ? body.ownerWalletAddress.trim() : '';
  const accountRef = typeof body.accountRef === 'string' ? body.accountRef.trim() : '';
  const peerPubkey = typeof body.peerPubkey === 'string' ? body.peerPubkey.trim() : '';
  const rgbAssetId = typeof body.rgbAssetId === 'string' ? body.rgbAssetId.trim() : '';
  const btcAmountSats = Math.trunc(Number(body.btcAmountSats || 0));
  const rgbAssetAmount = Math.trunc(Number(body.rgbAssetAmount || 0));
  const commissionRateSats = Math.max(0, Math.trunc(Number(body.commissionRateSats || 0)));
  const txThreshold = Math.max(100, Math.trunc(Number(body.txThreshold || 100)));

  if (!ownerWalletAddress) {
    sendJson(res, 400, { ok: false, error: 'ownerWalletAddress is required.' });
    return;
  }
  if (!isKnownRgbAccountRef(accountRef)) {
    sendJson(res, 400, { ok: false, error: getRgbAccountRefError() });
    return;
  }
  if (!/^[0-9a-fA-F]{66}$/.test(peerPubkey)) {
    sendJson(res, 400, { ok: false, error: 'peerPubkey must be a 33-byte compressed hex pubkey.' });
    return;
  }
  if (!rgbAssetId) {
    sendJson(res, 400, { ok: false, error: 'rgbAssetId is required.' });
    return;
  }
  if (!Number.isFinite(btcAmountSats) || btcAmountSats <= 0) {
    sendJson(res, 400, { ok: false, error: 'btcAmountSats must be a positive integer.' });
    return;
  }
  if (!Number.isFinite(rgbAssetAmount) || rgbAssetAmount <= 0) {
    sendJson(res, 400, { ok: false, error: 'rgbAssetAmount must be a positive integer.' });
    return;
  }

  try {
    await ensureChannelApplicationsTable();
    const wallet = await ensureWalletWithFunding(req, 'regtest');
    const apiBase = resolveRgbNodeApiBaseForAccountRef(accountRef);
    const btcFunding = await rgbNodeRequestWithBase(apiBase, '/address', {}, 'POST');
    const btcDepositAddress = typeof btcFunding?.address === 'string' ? btcFunding.address.trim() : '';
    if (!btcDepositAddress) {
      throw new Error('Unable to generate a BTC deposit address for the requested node.')
    }

    const invoiceResponse = await rgbNodeRequestWithBase(apiBase, '/lninvoice', {
      expiry_sec: 3600,
      amt_msat: 3000000,
      asset_id: rgbAssetId,
      asset_amount: rgbAssetAmount,
    });
    const rgbInvoice = typeof invoiceResponse?.invoice === 'string' ? invoiceResponse.invoice.trim() : null;

    const depositRequest = await createDepositRequest({
      walletId: wallet.id,
      depositAddress: btcDepositAddress,
      expectedSats: btcAmountSats,
      requiredConfirmations: DEPOSIT_REQUIRED_CONFIRMATIONS,
      nodeAccountRef: accountRef,
    });

    const application = await createChannelApplication({
      id: randomUUID(),
      walletId: wallet.id,
      ownerWalletAddress,
      ownerWalletKey: wallet.wallet_key,
      accountRef,
      peerPubkey,
      btcDepositAddress,
      rgbInvoiceId: invoiceResponse?.invoice_id || null,
      rgbInvoice,
      btcAmountSats,
      rgbAssetId,
      rgbAssetAmount,
      txThreshold,
      commissionRateSats,
      depositRequestId: depositRequest.id,
      metadata: {
        network: 'regtest',
        applicationSource: 'dev-dashboard',
      },
    });

    const hydrated = await getChannelApplicationById(application.id);
    sendJson(res, 200, {
      ok: true,
      application: formatChannelApplicationResponse(hydrated || application),
    });
  } catch (error) {
    console.error(`[${nowIso()}] [PLM] create application failed:`, error.message);
    sendJson(res, 502, { ok: false, error: error.message });
  }
}

async function handleChannelApplicationStatus(_req, res, parsedUrl) {
  const id = parsedUrl.searchParams.get('id') || '';
  if (!id.trim()) {
    sendJson(res, 400, { ok: false, error: 'id is required.' });
    return;
  }

  try {
    await ensureChannelApplicationsTable();
    const application = await maybeAutoOpenChannelApplication({ id: id.trim() });
    if (!application) {
      sendJson(res, 404, { ok: false, error: 'Channel application not found.' });
      return;
    }

    sendJson(res, 200, {
      ok: true,
      application: formatChannelApplicationResponse(application),
    });
  } catch (error) {
    console.error(`[${nowIso()}] [PLM] application status failed:`, error.message);
    sendJson(res, 502, { ok: false, error: error.message });
  }
}

/**
 * POST /api/utxo/redeem
 *
 * Redeems a FREE or EMPTY UTXO slot: sends the slot's BTC back to the wallet's
 * main_btc_address via the rgb-lightning-node /sendbtc endpoint, then marks
 * the slot REDEEMED.
 *
 * Body: { slotId: "<uuid>", mainBtcAddress?: "<address>" }
 *   mainBtcAddress is optional if already stored in wallets.main_btc_address.
 *   If supplied it is saved for future use.
 */
async function handleRedeemUtxoSlot(req, res) {
  try {
    const wallet = await ensureWalletWithFunding(req, 'regtest');
    const body = await readRequestJson(req);

    const { slotId, mainBtcAddress: bodyAddress } = body || {};
    if (!slotId) {
      sendJson(res, 400, { ok: false, error: 'slotId is required.' });
      return;
    }

    // Resolve and optionally persist the user's main BTC address
    let returnAddress = bodyAddress || wallet.main_btc_address;
    if (bodyAddress && bodyAddress !== wallet.main_btc_address) {
      await setWalletMainBtcAddress({ walletId: wallet.id, mainBtcAddress: bodyAddress });
      returnAddress = bodyAddress;
    }
    if (!returnAddress) {
      sendJson(res, 400, { ok: false, error: 'mainBtcAddress is required (first call must supply it).' });
      return;
    }

    // Load slot and verify ownership + state
    const slots = await getWalletUtxoSlots(wallet.id);
    const slot = slots.find((s) => s.id === slotId);
    if (!slot) {
      sendJson(res, 404, { ok: false, error: 'Slot not found for this wallet.' });
      return;
    }
    if (!['FREE', 'EMPTY'].includes(slot.state)) {
      sendJson(res, 409, { ok: false, error: `Cannot redeem slot in state ${slot.state}. Only FREE or EMPTY slots can be redeemed.` });
      return;
    }

    // Determine which node holds this wallet's funds
    const { apiBase: walletApiBase } = resolveWalletNodeContext(wallet);

    // Amount to send: use stored sats_value, fall back to UTXO_SLOT_SATS minus a miner fee buffer
    const REDEEM_FEE_BUFFER_SATS = 500;
    const sendSats = slot.sats_value
      ? Math.max(0, Number(slot.sats_value) - REDEEM_FEE_BUFFER_SATS)
      : Math.max(0, UTXO_SLOT_SATS - REDEEM_FEE_BUFFER_SATS);

    if (sendSats <= 546) {
      sendJson(res, 400, { ok: false, error: 'Slot value too low to redeem (dust limit).' });
      return;
    }

    // Call /sendbtc on the node
    const sendResult = await rgbNodeRequestWithBase(walletApiBase, '/sendbtc', {
      address: returnAddress,
      amount: sendSats,
      fee_rate: 1,
      skip_sync: false,
    });

    const txid = sendResult?.txid || null;
    if (!txid) {
      console.error(`[${nowIso()}] [UTXO] /sendbtc returned no txid:`, JSON.stringify(sendResult));
      sendJson(res, 502, { ok: false, error: 'Node did not return a txid. Redeem may have failed.' });
      return;
    }

    // Mark slot as redeemed
    await markSlotRedeemed(slotId, txid);

    console.log(`[${nowIso()}] [UTXO] slot REDEEMED slotId=${slotId} txid=${txid} to=${returnAddress} sats=${sendSats}`);

    sendJson(res, 200, {
      ok: true,
      txid,
      slotId,
      returnAddress,
      sentSats: sendSats,
    });
  } catch (error) {
    console.error(`[${nowIso()}] [UTXO] /redeem failed:`, error.message);
    sendJson(res, 502, { ok: false, error: error.message });
  }
}

/**
 * GET /api/utxo/slots
 *
 * Returns all UTXO slots for the calling wallet with their current state
 * (FREE / OCCUPIED / EMPTY / REDEEMED).
 */
async function handleGetUtxoSlots(req, res) {
  try {
    const wallet = await ensureWalletWithFunding(req, 'regtest');
    const slots = await getWalletUtxoSlots(wallet.id);

    sendJson(res, 200, {
      ok: true,
      walletKey: wallet.wallet_key,
      slots: slots.map((s) => ({
        id: s.id,
        outpoint: s.outpoint,
        state: s.state,
        satsValue: s.sats_value ? Number(s.sats_value) : null,
        nodeAccountRef: s.node_account_ref,
        transferId: s.rgb_transfer_id,
        invoiceId: s.invoice_id,
        redeemedTxid: s.redeemed_txid,
        createdAt: s.created_at,
        updatedAt: s.updated_at,
        redeemedAt: s.redeemed_at,
      })),
    });
  } catch (error) {
    console.error(`[${nowIso()}] [UTXO] /slots failed:`, error.message);
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
        WHERE COALESCE((metadata->>'archived')::boolean, FALSE) = FALSE
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

async function buildRgbIssueAssetReadiness(wallet, options = {}) {
  const requestedChannelFundingSats = Number.isFinite(Number(options.requestedChannelFundingSats))
    ? Math.max(0, Math.trunc(Number(options.requestedChannelFundingSats)))
    : 0;
  const channelFundingTiming = typeof options.channelFundingTiming === 'string'
    ? options.channelFundingTiming
    : 'after_issuance';
  const { accountRef } = resolveWalletNodeContext(wallet);
  const addresses = await getWalletAddresses(wallet.id);
  const utxoFundingAddress = addresses?.utxo_funding_address || wallet.utxo_funding_address || null;
  const addressStats = utxoFundingAddress ? await buildAddressStats(utxoFundingAddress) : null;
  const confirmedFundingSats = Number(addressStats?.chain_stats?.funded_txo_sum || 0);
  const confirmedUtxoCount = Number(addressStats?.chain_stats?.funded_txo_count || 0);
  const slots = await getWalletUtxoSlots(wallet.id);
  const freeSlotCount = slots.filter((slot) => slot.state === 'FREE').length;
  const issuanceFundingReady = confirmedFundingSats >= RGB_ISSUANCE_MIN_FUNDING_SATS && confirmedUtxoCount > 0;
  const requiredFundingSats =
    channelFundingTiming === 'during_issuance'
      ? RGB_ISSUANCE_MIN_FUNDING_SATS + requestedChannelFundingSats
      : RGB_ISSUANCE_MIN_FUNDING_SATS;
  const channelFundingReady =
    channelFundingTiming === 'during_issuance'
      ? confirmedFundingSats >= requiredFundingSats && confirmedUtxoCount > 0
      : requestedChannelFundingSats > 0
        ? false
        : issuanceFundingReady;
  const channelFundingShortfallSats =
    channelFundingTiming === 'during_issuance'
      ? Math.max(0, requiredFundingSats - confirmedFundingSats)
      : requestedChannelFundingSats;

  return {
    walletKey: wallet.wallet_key,
    network: 'regtest',
    nodeAccountRef: accountRef,
    utxoFundingAddress,
    confirmedFundingSats,
    confirmedUtxoCount,
    freeSlotCount,
    minimumFundingSats: RGB_ISSUANCE_MIN_FUNDING_SATS,
    issuanceFundingReady,
    channelFundingTiming,
    requestedChannelFundingSats,
    requiredFundingSats,
    channelFundingReady,
    channelFundingShortfallSats,
    isReady: issuanceFundingReady,
  };
}

function getPrimaryBootstrapPeerAccountRef(accountRef) {
  if (accountRef === RGB_USER_ACCOUNT_REF) {
    return RGB_USER_B_ACCOUNT_REF;
  }
  if (accountRef === RGB_USER_B_ACCOUNT_REF) {
    return RGB_USER_ACCOUNT_REF;
  }
  return RGB_USER_ACCOUNT_REF;
}

async function fetchNodePubkeyForAccountRef(accountRef) {
  const apiBase = resolveRgbNodeApiBaseForAccountRef(accountRef);
  const nodeInfo = await rgbNodeRequestWithBase(apiBase, '/nodeinfo', {}, 'GET');
  const pubkey = typeof nodeInfo?.pubkey === 'string' ? nodeInfo.pubkey.trim() : '';
  if (!pubkey) {
    throw new Error(`Unable to resolve peer pubkey for account ref ${accountRef}.`);
  }
  return pubkey;
}

async function createPrimaryBootstrapChannelApplication({
  wallet,
  accountRef,
  assetId,
  assetAmount,
  issuanceId,
  issuedAt,
  requestedChannelBtcSats,
  channelFundingTiming,
  readiness,
}) {
  await ensureChannelApplicationsTable();
  const peerAccountRef = getPrimaryBootstrapPeerAccountRef(accountRef);
  const peerPubkey = await fetchNodePubkeyForAccountRef(peerAccountRef);
  const apiBase = resolveRgbNodeApiBaseForAccountRef(accountRef);
  const addresses = await getWalletAddresses(wallet.id);
  const ownerWalletAddress = addresses?.main_btc_address || wallet.main_btc_address || readiness.utxoFundingAddress || '';
  if (!ownerWalletAddress) {
    throw new Error('Cannot create primary channel bootstrap application without an owner wallet address.');
  }

  let btcDepositAddress = readiness.utxoFundingAddress || '';
  let depositRequest = null;

  if (channelFundingTiming === 'during_issuance') {
    if (!btcDepositAddress) {
      throw new Error('The wallet does not have a funding address for immediate channel bootstrap.');
    }
    depositRequest = await createDepositRequest({
      walletId: wallet.id,
      depositAddress: btcDepositAddress,
      expectedSats: requestedChannelBtcSats,
      requiredConfirmations: DEPOSIT_REQUIRED_CONFIRMATIONS,
      nodeAccountRef: accountRef,
    });
    await markDepositConfirmed({ id: depositRequest.id, utxoSlotId: null });
  } else {
    const btcFunding = await rgbNodeRequestWithBase(apiBase, '/address', {}, 'POST');
    btcDepositAddress = typeof btcFunding?.address === 'string' ? btcFunding.address.trim() : '';
    if (!btcDepositAddress) {
      throw new Error('Unable to generate a BTC deposit address for primary channel bootstrap.');
    }
    depositRequest = await createDepositRequest({
      walletId: wallet.id,
      depositAddress: btcDepositAddress,
      expectedSats: requestedChannelBtcSats,
      requiredConfirmations: DEPOSIT_REQUIRED_CONFIRMATIONS,
      nodeAccountRef: accountRef,
    });
  }

  const application = await createChannelApplication({
    id: randomUUID(),
    walletId: wallet.id,
    ownerWalletAddress,
    ownerWalletKey: wallet.wallet_key,
    accountRef,
    peerPubkey,
    btcDepositAddress,
    rgbInvoiceId: null,
    rgbInvoice: null,
    status: 'rgb_funded',
    btcAmountSats: requestedChannelBtcSats,
    rgbAssetId: assetId,
    rgbAssetAmount: assetAmount,
    depositRequestId: depositRequest?.id || null,
    metadata: {
      network: 'regtest',
      applicationSource: 'issuance_bootstrap',
      issuanceId,
      createdFromIssuanceAt: issuedAt.toISOString(),
      peerAccountRef,
      channelFundingTiming,
      btcFundingSatisfiedAtIssuance: channelFundingTiming === 'during_issuance',
    },
  });

  const hydrated = await maybeAutoOpenChannelApplication(application);
  return hydrated || application;
}

function pickIssuedAssetColor(ticker = '') {
  if (ticker.toUpperCase() === 'PHO') {
    return '#38bdf8';
  }
  return '#f8fafc';
}

async function handleRgbIssueAssetReadiness(req, res, parsedUrl) {
  try {
    const requestedChannelFundingSats = parsedUrl?.searchParams?.get('channelFundingSats');
    const channelFundingTiming = parsedUrl?.searchParams?.get('channelFundingTiming');
    const wallet = await ensureWalletWithFunding(req, 'regtest');
    const readiness = await buildRgbIssueAssetReadiness(wallet, {
      requestedChannelFundingSats,
      channelFundingTiming,
    });
    sendJson(res, 200, {
      ok: true,
      ...readiness,
    });
  } catch (error) {
    console.error(`[${nowIso()}] [RGB API] Issue asset readiness failed:`, error.message);
    sendJson(res, 502, { ok: false, error: error.message });
  }
}

async function handleRgbIssueAsset(req, res) {
  let body;
  try {
    body = await readRequestJson(req);
  } catch (error) {
    sendJson(res, 400, { ok: false, error: error.message });
    return;
  }

  const network = getRequestNetwork(body);
  if (network !== 'regtest') {
    sendJson(res, 400, { ok: false, error: 'RGB asset issuance is currently available only on regtest.' });
    return;
  }

  const schema = typeof body.schema === 'string' && body.schema.trim() ? body.schema.trim().toUpperCase() : 'NIA';
  const tokenName = typeof body.name === 'string' ? body.name.trim() : '';
  const ticker = typeof body.ticker === 'string' ? body.ticker.trim().toUpperCase() : '';
  const description = typeof body.description === 'string' ? body.description.trim() : '';
  const precision = Number(body.precision);
  const totalSupply = Number(body.totalSupply);
  const publicRegistry = true;
  const bootstrapLightning = Boolean(body.bootstrapLightning);
  const liquidityPercentageRaw =
    body.liquidityPercentage === null || body.liquidityPercentage === undefined || body.liquidityPercentage === ''
      ? null
      : Number(body.liquidityPercentage);
  const requestedChannelBtcSatsRaw =
    body.channelFundingSats === null || body.channelFundingSats === undefined || body.channelFundingSats === ''
      ? null
      : Number(body.channelFundingSats);
  const requestedChannelFundingTiming =
    typeof body.channelFundingTiming === 'string' && body.channelFundingTiming.trim()
      ? body.channelFundingTiming.trim().toLowerCase()
      : 'after_issuance';

  if (schema !== 'NIA') {
    sendJson(res, 400, { ok: false, error: 'Phase 1 supports only NIA asset issuance.' });
    return;
  }
  if (!tokenName) {
    sendJson(res, 400, { ok: false, error: 'name is required.' });
    return;
  }
  if (!/^[A-Z0-9]{3,8}$/.test(ticker)) {
    sendJson(res, 400, { ok: false, error: 'ticker must be 3-8 uppercase letters or numbers.' });
    return;
  }
  if (!Number.isInteger(precision) || precision < 0 || precision > 18) {
    sendJson(res, 400, { ok: false, error: 'precision must be an integer between 0 and 18.' });
    return;
  }
  if (!Number.isFinite(totalSupply) || totalSupply <= 0 || !Number.isInteger(totalSupply)) {
    sendJson(res, 400, { ok: false, error: 'totalSupply must be a positive integer.' });
    return;
  }
  if (!['during_issuance', 'after_issuance'].includes(requestedChannelFundingTiming)) {
    sendJson(res, 400, {
      ok: false,
      error: 'channelFundingTiming must be either during_issuance or after_issuance.',
    });
    return;
  }
  if (liquidityPercentageRaw !== null && (!Number.isFinite(liquidityPercentageRaw) || liquidityPercentageRaw < 0 || liquidityPercentageRaw > 100)) {
    sendJson(res, 400, {
      ok: false,
      error: 'liquidityPercentage must be between 0 and 100.',
    });
    return;
  }
  if (requestedChannelBtcSatsRaw !== null && (!Number.isFinite(requestedChannelBtcSatsRaw) || requestedChannelBtcSatsRaw <= 0 || !Number.isInteger(requestedChannelBtcSatsRaw))) {
    sendJson(res, 400, {
      ok: false,
      error: 'channelFundingSats must be a positive integer amount of sats.',
    });
    return;
  }
  if (!hasExplicitWalletKeyHeader(req)) {
    sendJson(res, 400, {
      ok: false,
      error: 'x-photon-wallet-key header is required for RGB asset issuance so the full supply is minted into the requesting wallet.',
    });
    return;
  }

  const normalizedLiquidityPercentage = bootstrapLightning ? Number((liquidityPercentageRaw ?? 0).toFixed(2)) : null;
  const reservedAssetAmount =
    bootstrapLightning && normalizedLiquidityPercentage !== null && normalizedLiquidityPercentage > 0
      ? Math.floor((totalSupply * normalizedLiquidityPercentage) / 100)
      : 0;
  if (bootstrapLightning && normalizedLiquidityPercentage !== null && normalizedLiquidityPercentage > 0 && reservedAssetAmount <= 0) {
    sendJson(res, 400, {
      ok: false,
      error: 'Selected liquidity percentage does not reserve any RGB supply. Increase the percentage or supply.',
    });
    return;
  }
  const requestedChannelBtcSats = bootstrapLightning ? requestedChannelBtcSatsRaw : null;
  const channelBootstrapMode = bootstrapLightning ? requestedChannelFundingTiming : null;
  const initialLifecycleStatus = bootstrapLightning
    ? requestedChannelBtcSats && requestedChannelBtcSats > 0
      ? 'waiting_primary_channel'
      : 'waiting_btc_channel_funding'
    : 'issued_registry_only';

  try {
    await ensureRgbAssetIssuancesTable();
    const wallet = await ensureWalletWithFunding(req, 'regtest');
    const readiness = await buildRgbIssueAssetReadiness(wallet, {
      requestedChannelFundingSats,
      channelFundingTiming: requestedChannelFundingTiming,
    });
    if (!readiness.isReady) {
      sendJson(res, 409, {
        ok: false,
        error: `Asset issuance requires BTC funding and confirmed UTXOs. Fund ${readiness.utxoFundingAddress || 'the wallet'} with at least ${RGB_ISSUANCE_MIN_FUNDING_SATS} sats first.`,
        readiness,
      });
      return;
    }
    if (channelBootstrapMode === 'during_issuance' && requestedChannelBtcSats && !readiness.channelFundingReady) {
      sendJson(res, 409, {
        ok: false,
        error: `Primary channel bootstrap during issuance requires ${readiness.requiredFundingSats.toLocaleString()} sats in the funding wallet. Current shortfall: ${readiness.channelFundingShortfallSats.toLocaleString()} sats.`,
        readiness,
      });
      return;
    }

    const duplicate = await query(
      `
        SELECT contract_id
        FROM asset_registry
        WHERE network = 'regtest'
          AND lower(ticker) = lower($1)
        LIMIT 1
      `,
      [ticker]
    );
    if (duplicate.rows.length > 0) {
      sendJson(res, 409, { ok: false, error: `Ticker ${ticker} is already present in the asset registry.` });
      return;
    }

    const { accountRef, apiBase } = resolveWalletNodeContext(wallet);
    const issuedAt = new Date();
    const issuanceInsert = await query(
      `
        INSERT INTO rgb_asset_issuances (
          wallet_id,
          node_account_ref,
          network,
          schema,
          token_name,
          ticker,
          precision,
          total_supply,
          liquidity_percentage,
          reserved_asset_amount,
          requested_channel_btc_sats,
          channel_bootstrap_mode,
          lifecycle_status,
          status,
          metadata
        )
        VALUES ($1, $2, 'regtest', $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 'issuing', $13::jsonb)
        RETURNING id
      `,
      [
        wallet.id,
        accountRef,
        schema,
        tokenName,
        ticker,
        precision,
        String(totalSupply),
        normalizedLiquidityPercentage,
        reservedAssetAmount > 0 ? String(reservedAssetAmount) : null,
        requestedChannelBtcSats,
        channelBootstrapMode,
        initialLifecycleStatus,
        JSON.stringify({
          requestedAt: issuedAt.toISOString(),
          description: description || null,
          publicRegistry,
          bootstrapPlan: {
            enabled: bootstrapLightning,
            liquidityPercentage: normalizedLiquidityPercentage,
            reservedAssetAmount,
            requestedChannelBtcSats,
            channelFundingTiming: channelBootstrapMode,
            lifecycleStatus: initialLifecycleStatus,
          },
        }),
      ]
    );
    const issuanceId = issuanceInsert.rows[0].id;

    try {
      await ensureRgbUtxos(apiBase);
      const issueResult = await rgbNodeRequestWithBase(apiBase, '/issueassetnia', {
        ticker,
        name: tokenName,
        amounts: [Math.trunc(totalSupply)],
        precision,
        ...(description ? { details: description } : {}),
      });

      const asset = issueResult?.asset || issueResult || {};
      const contractId =
        (typeof asset.asset_id === 'string' && asset.asset_id.trim()) ||
        (typeof asset.assetId === 'string' && asset.assetId.trim()) ||
        null;
      if (!contractId) {
        throw new Error('RGB node did not return an asset id for the issued asset.');
      }

      const issuedSupply =
        asset.issued_supply !== undefined && asset.issued_supply !== null
          ? String(asset.issued_supply)
          : String(totalSupply);
      const schemaId = typeof asset.schema_id === 'string' ? asset.schema_id : null;
      let bootstrapApplication = null;
      let effectiveLifecycleStatus = initialLifecycleStatus;
      let bootstrapErrorMessage = null;

      if (bootstrapLightning && reservedAssetAmount > 0 && requestedChannelBtcSats) {
        try {
          bootstrapApplication = await createPrimaryBootstrapChannelApplication({
            wallet,
            accountRef,
            assetId: contractId,
            assetAmount: reservedAssetAmount,
            issuanceId,
            issuedAt,
            requestedChannelBtcSats,
            channelFundingTiming: channelBootstrapMode,
            readiness,
          });

          if (bootstrapApplication?.channel_id || bootstrapApplication?.status === 'channel_active') {
            effectiveLifecycleStatus = 'lightning_ready';
          } else if ((bootstrapApplication?.btc_deposit_status || '') === 'confirmed') {
            effectiveLifecycleStatus = 'waiting_primary_channel';
          } else {
            effectiveLifecycleStatus = 'waiting_btc_channel_funding';
          }
        } catch (bootstrapError) {
          bootstrapErrorMessage = bootstrapError.message;
          effectiveLifecycleStatus = 'bootstrap_failed';
          console.error(`[${nowIso()}] [RGB API] Primary bootstrap setup failed:`, bootstrapError.message);
        }
      }

      const walletAssetId = await withTransaction(async (client) => {
        await client.query(
          `
            UPDATE rgb_asset_issuances
            SET
              contract_id = $2,
              status = 'issued',
              lifecycle_status = $4,
              primary_channel_id = $5,
              last_error = $6,
              metadata = COALESCE(metadata, '{}'::jsonb) || $3::jsonb,
              updated_at = NOW()
            WHERE id = $1
          `,
          [
            issuanceId,
            contractId,
            JSON.stringify({
              issuedAt: issuedAt.toISOString(),
              nodeResponse: issueResult || {},
              bootstrapChannelApplicationId: bootstrapApplication?.id || null,
              bootstrapChannelStatus: bootstrapApplication?.status || null,
              bootstrapChannelId: bootstrapApplication?.channel_id || null,
              bootstrapError: bootstrapErrorMessage,
            }),
            effectiveLifecycleStatus,
            bootstrapApplication?.channel_id || null,
            bootstrapErrorMessage,
          ]
        );

        if (publicRegistry) {
          await client.query(
            `
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
              VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb)
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
                updated_at = NOW()
            `,
            [
              'regtest',
              tokenName,
              ticker,
              issuedSupply,
              precision,
              wallet.wallet_key,
              issuedAt.toISOString().slice(0, 10),
              await rpcRequest('getblockcount'),
              contractId,
              schemaId,
              JSON.stringify({
                description: description || null,
                display_color: pickIssuedAssetColor(ticker),
                issuance_id: issuanceId,
                issued_by_wallet_key: wallet.wallet_key,
              }),
            ]
          );
        }

        const walletAssetResult = await client.query(
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
            RETURNING id
          `,
          [wallet.id, contractId, 'Nia', ticker, tokenName, precision, contractId]
        );
        const walletAssetId = walletAssetResult.rows[0].id;

        const existingIssuanceTransfer = await client.query(
          `
            SELECT id
            FROM rgb_transfers
            WHERE wallet_id = $1
              AND wallet_asset_id = $2
              AND transfer_kind = 'Issuance'
              AND metadata->>'contract_id' = $3
            LIMIT 1
          `,
          [wallet.id, walletAssetId, contractId]
        );

        if (existingIssuanceTransfer.rows.length === 0) {
          await client.query(
            `
              INSERT INTO rgb_transfers (
                wallet_id,
                wallet_asset_id,
                direction,
                transfer_kind,
                status,
                requested_assignment_type,
                requested_assignment_value,
                settled_amount,
                settled_at,
                metadata
              )
              VALUES ($1, $2, 'incoming', 'Issuance', 'Settled', 'Fungible', $3, $3, $4, $5::jsonb)
            `,
            [
              wallet.id,
              walletAssetId,
              issuedSupply,
              issuedAt,
              JSON.stringify({
                route: 'issuance',
                contract_id: contractId,
                issuance_id: issuanceId,
                schema: 'NIA',
                ticker,
                bootstrap_lifecycle_status: effectiveLifecycleStatus,
              }),
            ]
          );
        }

        await client.query(
          `
            INSERT INTO transfer_events (
              wallet_id,
              invoice_id,
              event_type,
              event_source,
              payload
            )
            VALUES ($1, NULL, 'rgb_asset_issued', 'wallet_api', $2::jsonb)
          `,
          [
            wallet.id,
            JSON.stringify({
              issuanceId,
              contractId,
              ticker,
              totalSupply: issuedSupply,
              nodeAccountRef: accountRef,
              publicRegistry,
              bootstrapPlan: {
                enabled: bootstrapLightning,
                liquidityPercentage: normalizedLiquidityPercentage,
                reservedAssetAmount,
                requestedChannelBtcSats,
                channelFundingTiming: channelBootstrapMode,
                lifecycleStatus: effectiveLifecycleStatus,
                channelApplicationId: bootstrapApplication?.id || null,
                channelId: bootstrapApplication?.channel_id || null,
                error: bootstrapErrorMessage,
              },
            }),
          ]
        );

        return walletAssetId;
      });

      const synced = await syncWalletAssetFromRgbNode({ walletId: wallet.id, assetId: contractId });
      const transfers = await syncWalletTransferRows(wallet, contractId, synced.walletAsset.id);
      await syncWalletLightningPayments(wallet, contractId, synced.walletAsset.id);
      await reconcileWalletConsignmentSecrets(wallet, synced.walletAsset.id, transfers);

      const balance = synced.asset?.balance
        ? normalizeLiveRgbBalance(synced.asset.balance)
        : await deriveWalletScopedBalance(synced.walletAsset.id);
      await upsertWalletAssetBalance(synced.walletAsset.id, balance);

      sendJson(res, 200, {
        ok: true,
        walletKey: wallet.wallet_key,
        ownerWalletKey: wallet.wallet_key,
        ownerNodeAccountRef: accountRef,
        issuanceId,
        registryListed: publicRegistry,
        asset: {
          token_name: tokenName,
          ticker,
          total_supply: issuedSupply,
          precision,
          issuer_ref: wallet.wallet_key,
          creation_date: issuedAt.toISOString().slice(0, 10),
          block_height: null,
          contract_id: contractId,
          schema_id: schemaId,
        },
        ownership: {
          walletKey: wallet.wallet_key,
          nodeAccountRef: accountRef,
          initialSupplyAssigned: issuedSupply,
        },
        bootstrapPlan: {
          enabled: bootstrapLightning,
          liquidityPercentage: normalizedLiquidityPercentage,
          reservedAssetAmount,
          requestedChannelBtcSats,
          channelFundingTiming: channelBootstrapMode,
          lifecycleStatus: effectiveLifecycleStatus,
          channelApplicationId: bootstrapApplication?.id || null,
          channelId: bootstrapApplication?.channel_id || null,
          error: bootstrapErrorMessage,
        },
      });
    } catch (issueError) {
      await query(
        `
          UPDATE rgb_asset_issuances
          SET
            status = 'failed',
            last_error = $3,
            metadata = COALESCE(metadata, '{}'::jsonb) || $2::jsonb,
            updated_at = NOW()
          WHERE id = $1
        `,
        [
          issuanceId,
          JSON.stringify({
            failedAt: new Date().toISOString(),
            error: issueError.message,
          }),
          issueError.message,
        ]
      );
      throw issueError;
    }
  } catch (error) {
    console.error(`[${nowIso()}] [RGB API] Asset issuance failed:`, error.message);
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
    const registryState = await getRegistryAssetState(assetId);
    if (registryState?.archived) {
      sendJson(res, 410, { ok: false, error: `RGB asset ${registryState.ticker || registryState.tokenName || assetId} is archived.` });
      return;
    }

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

async function handleRgbArchiveAsset(req, res) {
  if (!requireAdminSession(req, res)) {
    return;
  }

  let body;
  try {
    body = await readRequestJson(req);
  } catch (error) {
    sendJson(res, 400, { ok: false, error: error.message });
    return;
  }

  const assetId = typeof body.assetId === 'string' && body.assetId.trim() ? body.assetId.trim() : '';
  if (!assetId) {
    sendJson(res, 400, { ok: false, error: 'assetId is required.' });
    return;
  }

  const archived = body.archived !== undefined ? Boolean(body.archived) : true;
  const reason = typeof body.reason === 'string' && body.reason.trim() ? body.reason.trim() : null;

  try {
    const result = await query(
      `
        UPDATE asset_registry
        SET
          metadata = COALESCE(metadata, '{}'::jsonb) || $2::jsonb,
          updated_at = NOW()
        WHERE contract_id = $1
        RETURNING token_name, ticker, contract_id, metadata
      `,
      [
        assetId,
        JSON.stringify({
          archived,
          archived_at: new Date().toISOString(),
          archive_reason: reason,
        }),
      ]
    );

    if (result.rows.length === 0) {
      sendJson(res, 404, { ok: false, error: 'Asset not found in registry.' });
      return;
    }

    const row = result.rows[0];
    sendJson(res, 200, {
      ok: true,
      asset: {
        token_name: row.token_name,
        ticker: row.ticker,
        contract_id: row.contract_id,
        archived: isArchivedRegistryMetadata(row.metadata),
      },
    });
  } catch (error) {
    console.error(`[${nowIso()}] [RGB API] Archive asset failed:`, error.message);
    sendJson(res, 502, { ok: false, error: error.message });
  }
}

async function ensureState() {
  await fsp.mkdir(DATA_DIR, { recursive: true });
  await ensureRgbNodeRegistrySeeded();
  await ensureBoardSupportSeeded();
  try {
    const raw = await fsp.readFile(CLAIMS_PATH, 'utf8');
    const parsed = JSON.parse(raw);
    claimState = {
      claims: Array.isArray(parsed.claims) ? parsed.claims : [],
      rgbClaims: Array.isArray(parsed.rgbClaims) ? parsed.rgbClaims : [],
    };
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
  claimState.rgbClaims = claimState.rgbClaims.filter((entry) => entry.timestamp >= threshold);
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

function getRecentRgbClaim(ip, invoice) {
  pruneClaims();
  return claimState.rgbClaims.find((entry) => entry.ip === ip || entry.invoice === invoice);
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
      rgbFaucetMaxAmount: RGB_FAUCET_MAX_AMOUNT,
      rgbFaucetCooldownPaused: RGB_FAUCET_COOLDOWN_PAUSED,
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
  const issuerApiBase = resolveRgbNodeApiBaseForAccountRef(RGB_OWNER_ACCOUNT_REF);
  try {
    console.log(`[${nowIso()}] [RGB API] Health check requested`);
    const info = await fetch(`${issuerApiBase}/networkinfo`);
    if (!info.ok) {
      throw new Error(`RGB node responded with status ${info.status}`);
    }
    const payload = await info.json();
    console.log(`[${nowIso()}] [RGB API] Health check succeeded`, payload);
    sendJson(res, 200, {
      ok: true,
      rgbNodeApiBase: issuerApiBase,
      ...payload,
    });
  } catch (error) {
    console.error(`[${nowIso()}] [RGB API] Health check failed:`, error.message);
    sendJson(res, 503, {
      ok: false,
      error: error.message,
      rgbNodeApiBase: issuerApiBase,
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
    if (assetId) {
      await assertAssetNotArchived(assetId);
    }
    const wallet = await ensureWalletWithFunding(req, getRequestNetwork(body));
    const { apiBase: walletApiBase } = resolveWalletNodeContext(wallet);
    const payload = {
      min_confirmations: 1,
      asset_id: assetId,
      assignment: openAmount ? null : { type: 'Fungible', value: amount },
      duration_seconds: 86400,
      witness: false,
    };

    let invoice;
    let invoiceUsedOpenAsset = false;
    try {
      invoice = await rgbNodeRequestWithBase(walletApiBase, '/rgbinvoice', payload);
    } catch (error) {
      const message = error?.message || '';
      if (message.includes('No uncolored UTXOs are available')) {
        console.warn(`[${nowIso()}] [RGB API] RGB issuer is out of uncolored UTXOs, replenishing and retrying once`);
        await ensureRgbUtxos(walletApiBase);
        invoice = await rgbNodeRequestWithBase(walletApiBase, '/rgbinvoice', payload);
      } else if (message.includes('Unknown RGB contract ID') && assetId) {
        // Node has never seen this asset — first-time bootstrap.
        // Generate an open invoice (no asset_id). The consignment from the sender
        // will carry the full contract data and teach this node about the asset.
        console.warn(`[${nowIso()}] [RGB API] Node doesn't know contract for ${assetId}, falling back to open invoice for bootstrap`);
        const openPayload = { ...payload, asset_id: null, assignment: null };
        invoice = await rgbNodeRequestWithBase(walletApiBase, '/rgbinvoice', openPayload);
        invoiceUsedOpenAsset = true;
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
    let receiveUtxo = invoice?.receive_utxo || null;

    if (getRequestNetwork(body) === 'regtest' && Number.isFinite(Number(invoice?.batch_transfer_idx))) {
      try {
        const runtimeTransfer = await findRgbTransferByIdx(assetId, Number(invoice.batch_transfer_idx), walletApiBase);
        if (runtimeTransfer?.receive_utxo) {
          receiveUtxo = runtimeTransfer.receive_utxo;
          managedSecret = `backend-managed:${runtimeTransfer.receive_utxo}`;
          managedSecretSource = 'backend-runtime';
        } else if (invoice?.recipient_id) {
          managedSecret = `backend-managed:${invoice.recipient_id}`;
          managedSecretSource = 'backend-runtime';
        }
      } catch (runtimeError) {
        // Node may not know the asset yet (bootstrap case). Fall back to recipient_id-based secret.
        console.warn(`[${nowIso()}] [RGB API] Unable to derive backend-managed secret token for invoice`, runtimeError?.message || runtimeError);
        if (invoice?.recipient_id) {
          managedSecret = `backend-managed:${invoice.recipient_id}`;
          managedSecretSource = 'backend-runtime';
        }
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

    // Record the receive_utxo as a FREE slot — this is the UTXO locker the node
    // has assigned for this invoice. It will transition to OCCUPIED when the
    // incoming transfer settles (via syncWalletTransferRows or the watchdog).
    if (receiveUtxo) {
      try {
        const { accountRef } = resolveWalletNodeContext(wallet);
        await upsertUtxoSlot({
          walletId: wallet.id,
          outpoint: receiveUtxo,
          state: 'FREE',
          nodeAccountRef: accountRef,
          invoiceId: storedInvoice?.id || null,
          transferId: null,
        });
        console.log(`[${nowIso()}] [RGB API] UTXO slot recorded FREE outpoint=${receiveUtxo}`);
      } catch (slotErr) {
        // Non-fatal — invoice still valid, slot can be reconciled later
        console.warn(`[${nowIso()}] [RGB API] Failed to record UTXO slot for invoice:`, slotErr.message);
      }
    }

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
      ...(invoiceUsedOpenAsset ? {
        bootstrapInvoice: true,
        bootstrapAssetId: assetId,
        bootstrapNote: 'Your node has not received this asset before. This is an open invoice — ask the sender to send the correct asset to it. After your first receipt the node will know the contract and future invoices will work normally.',
      } : {}),
    });
  } catch (error) {
    console.error(`[${nowIso()}] RGB invoice generation failed:`, error.message);
    sendJson(res, error.statusCode || 502, { ok: false, error: error.message });
  }
}

async function handleRgbOpenChannel(req, res) {
  if (!requireAdminSession(req, res)) {
    return;
  }

  let body;
  try {
    body = await readRequestJson(req);
  } catch (error) {
    sendJson(res, 400, { ok: false, error: error.message });
    return;
  }

  const accountRef =
    typeof body.accountRef === 'string' && body.accountRef.trim() ? body.accountRef.trim() : RGB_USER_ACCOUNT_REF;
  const peerPubkey =
    typeof body.peerPubkey === 'string' && body.peerPubkey.trim() ? body.peerPubkey.trim() : '';
  const assetId =
    typeof body.assetId === 'string' && body.assetId.trim() ? body.assetId.trim() : '';
  const capacitySat = Number(body.capacitySat);
  const assetAmount = Number(body.assetAmount);
  const pushMsat = body.pushMsat === undefined ? 0 : Number(body.pushMsat);
  const temporaryChannelId =
    typeof body.temporaryChannelId === 'string' && body.temporaryChannelId.trim()
      ? body.temporaryChannelId.trim()
      : null;
  const publicFlag = Boolean(body.public);
  const withAnchors = body.withAnchors === undefined ? true : Boolean(body.withAnchors);
  const feeBaseMsat = body.feeBaseMsat === undefined ? 0 : Number(body.feeBaseMsat);
  const feeProportionalMillionths =
    body.feeProportionalMillionths === undefined ? 0 : Number(body.feeProportionalMillionths);

  if (!isKnownRgbAccountRef(accountRef)) {
    sendJson(res, 400, { ok: false, error: getRgbAccountRefError() });
    return;
  }

  if (!/^[0-9a-f]{66}$/i.test(peerPubkey)) {
    sendJson(res, 400, { ok: false, error: 'peerPubkey must be a 33-byte compressed hex pubkey.' });
    return;
  }

  if (!assetId) {
    sendJson(res, 400, { ok: false, error: 'assetId is required.' });
    return;
  }

  if (!Number.isFinite(capacitySat) || capacitySat <= 0) {
    sendJson(res, 400, { ok: false, error: 'capacitySat must be a positive number.' });
    return;
  }

  if (!Number.isFinite(assetAmount) || assetAmount <= 0) {
    sendJson(res, 400, { ok: false, error: 'assetAmount must be a positive number.' });
    return;
  }

  if (!Number.isFinite(pushMsat) || pushMsat < 0) {
    sendJson(res, 400, { ok: false, error: 'pushMsat must be zero or greater.' });
    return;
  }

  if (!Number.isFinite(feeBaseMsat) || feeBaseMsat < 0) {
    sendJson(res, 400, { ok: false, error: 'feeBaseMsat must be zero or greater.' });
    return;
  }

  if (!Number.isFinite(feeProportionalMillionths) || feeProportionalMillionths < 0) {
    sendJson(res, 400, { ok: false, error: 'feeProportionalMillionths must be zero or greater.' });
    return;
  }

  try {
    const apiBase = resolveRgbNodeApiBaseForAccountRef(accountRef);
    const payload = {
      peer_pubkey_and_opt_addr: peerPubkey,
      capacity_sat: Math.trunc(capacitySat),
      push_msat: Math.trunc(pushMsat),
      public: publicFlag,
      with_anchors: withAnchors,
      fee_base_msat: Math.trunc(feeBaseMsat),
      fee_proportional_millionths: Math.trunc(feeProportionalMillionths),
      temporary_channel_id: temporaryChannelId,
      asset_id: assetId,
      asset_amount: Math.trunc(assetAmount),
    };

    const result = await rgbNodeRequestWithBase(apiBase, '/openchannel', payload);

    sendJson(res, 200, {
      ok: true,
      accountRef,
      apiBase,
      request: payload,
      result,
    });
  } catch (error) {
    console.error(`[${nowIso()}] [RGB API] Open channel failed:`, error.message);
    sendJson(res, 502, { ok: false, error: error.message });
  }
}

async function openRgbChannelInternal({
  accountRef,
  peerPubkey,
  assetId,
  capacitySat,
  assetAmount,
  pushMsat = 0,
  public: publicFlag = false,
  withAnchors = true,
  feeBaseMsat = 0,
  feeProportionalMillionths = 0,
  temporaryChannelId = null,
}) {
  const apiBase = resolveRgbNodeApiBaseForAccountRef(accountRef);
  const payload = {
    peer_pubkey_and_opt_addr: peerPubkey,
    capacity_sat: Math.trunc(capacitySat),
    push_msat: Math.trunc(pushMsat),
    public: Boolean(publicFlag),
    with_anchors: Boolean(withAnchors),
    fee_base_msat: Math.trunc(feeBaseMsat),
    fee_proportional_millionths: Math.trunc(feeProportionalMillionths),
    temporary_channel_id: temporaryChannelId,
    asset_id: assetId,
    asset_amount: Math.trunc(assetAmount),
  };

  const result = await rgbNodeRequestWithBase(apiBase, '/openchannel', payload);
  return { apiBase, payload, result };
}

async function reconcileChannelApplicationRgbFunding(applicationLike) {
  if (!applicationLike?.id) {
    return null;
  }

  const application = await getChannelApplicationById(applicationLike.id);
  if (!application) {
    return null;
  }

  if (!application.rgb_invoice) {
    return application;
  }

  if (['rgb_funded', 'funded', 'channel_active'].includes(String(application.status || ''))) {
    return application;
  }

  try {
    const apiBase = resolveRgbNodeApiBaseForAccountRef(application.account_ref);
    const decoded = await rgbNodeRequestWithBase(apiBase, '/decodelninvoice', {
      invoice: application.rgb_invoice,
    });
    const paymentHash = typeof decoded?.payment_hash === 'string' ? decoded.payment_hash : '';
    if (!paymentHash) {
      return application;
    }

    const payments = await listNodePayments(apiBase);
    const matched = payments.find((entry) => entry?.payment_hash === paymentHash);
    const status = String(matched?.status || '').toLowerCase();

    if (status === 'succeeded') {
      await markChannelApplicationRgbFunded({
        id: application.id,
        paymentHash,
        paymentStatus: matched?.status || 'Succeeded',
        metadata: {
          rgbFundingReconciledAt: new Date().toISOString(),
        },
      });
      return await getChannelApplicationById(application.id);
    }
  } catch (error) {
    console.warn(`[${nowIso()}] [PLM] RGB funding reconcile skipped for ${application.id}:`, error.message);
  }

  return application;
}

async function maybeAutoOpenChannelApplication(applicationLike) {
  if (!applicationLike?.id) {
    return null;
  }

  let application = await getChannelApplicationById(applicationLike.id);
  if (!application) {
    return null;
  }

  application = await reconcileChannelApplicationRgbFunding(application);
  if (!application) {
    return null;
  }

  if (application.channel_id || application.status === 'channel_active') {
    return application;
  }

  const btcConfirmed = (application.btc_deposit_status || '') === 'confirmed';
  const rgbFunded = ['rgb_funded', 'funded', 'channel_active'].includes(String(application.status || ''));

  if (!btcConfirmed || !rgbFunded) {
    return application;
  }

  try {
    const opened = await openRgbChannelInternal({
      accountRef: application.account_ref,
      peerPubkey: application.peer_pubkey,
      assetId: application.rgb_asset_id,
      capacitySat: Number(application.btc_amount_sats || 0),
      assetAmount: Number(application.rgb_asset_amount || 0),
    });

    const rawChannelId =
      opened?.result?.channel_id ||
      opened?.result?.channelId ||
      opened?.result?.temporary_channel_id ||
      opened?.result?.temporaryChannelId ||
      null;

    await markChannelApplicationActive({
      id: application.id,
      channelId: rawChannelId,
      metadata: {
        autoOpenResult: opened.result || null,
        autoOpenRequest: opened.payload || null,
      },
    });

    return await getChannelApplicationById(application.id);
  } catch (error) {
    console.error(`[${nowIso()}] [PLM] auto-open failed for application ${application.id}:`, error.message);
    await query(
      `UPDATE channel_applications
       SET metadata = COALESCE(metadata, '{}'::jsonb) || $2::jsonb,
           updated_at = NOW()
       WHERE id = $1`,
      [
        application.id,
        JSON.stringify({
          autoOpenError: error.message,
          autoOpenFailedAt: new Date().toISOString(),
        }),
      ]
    );
    return await getChannelApplicationById(application.id);
  }
}

async function handleRgbOpenChannelCheck(req, res, parsedUrl) {
  if (!requireAdminSession(req, res)) {
    return;
  }

  const accountRef =
    typeof parsedUrl.searchParams.get('accountRef') === 'string' && parsedUrl.searchParams.get('accountRef').trim()
      ? parsedUrl.searchParams.get('accountRef').trim()
      : RGB_USER_ACCOUNT_REF;
  const assetId =
    typeof parsedUrl.searchParams.get('assetId') === 'string' && parsedUrl.searchParams.get('assetId').trim()
      ? parsedUrl.searchParams.get('assetId').trim()
      : '';
  const capacitySat = Number(parsedUrl.searchParams.get('capacitySat') || 0);
  const assetAmount = Number(parsedUrl.searchParams.get('assetAmount') || 0);
  const feeReserveSat = 2500;

  if (!isKnownRgbAccountRef(accountRef)) {
    sendJson(res, 400, { ok: false, error: getRgbAccountRefError() });
    return;
  }

  if (!assetId) {
    sendJson(res, 400, { ok: false, error: 'assetId is required.' });
    return;
  }

  if (!Number.isFinite(capacitySat) || capacitySat <= 0) {
    sendJson(res, 400, { ok: false, error: 'capacitySat must be a positive number.' });
    return;
  }

  if (!Number.isFinite(assetAmount) || assetAmount <= 0) {
    sendJson(res, 400, { ok: false, error: 'assetAmount must be a positive number.' });
    return;
  }

  try {
    const apiBase = resolveRgbNodeApiBaseForAccountRef(accountRef);
    const [btcBalance, assetBalance] = await Promise.all([
      rgbNodeRequestWithBase(apiBase, '/btcbalance', { skip_sync: false }),
      rgbNodeRequestWithBase(apiBase, '/assetbalance', { asset_id: assetId }),
    ]);

    const vanillaSpendableSat = Number(btcBalance?.vanilla?.spendable || 0);
    const assetSpendable = Number(assetBalance?.spendable || 0);
    const requiredBtcSat = Math.trunc(capacitySat) + feeReserveSat;
    const missingBtcSat = Math.max(0, requiredBtcSat - vanillaSpendableSat);
    const missingAssetAmount = Math.max(0, Math.trunc(assetAmount) - assetSpendable);

    sendJson(res, 200, {
      ok: true,
      accountRef,
      apiBase,
      feeReserveSat,
      required: {
        capacitySat: Math.trunc(capacitySat),
        assetAmount: Math.trunc(assetAmount),
        btcWithReserveSat: requiredBtcSat,
      },
      available: {
        vanillaSpendableSat,
        assetSpendable,
      },
      checks: {
        btcSufficient: missingBtcSat === 0,
        assetSufficient: missingAssetAmount === 0,
        canOpenChannel: missingBtcSat === 0 && missingAssetAmount === 0,
      },
      missing: {
        btcSat: missingBtcSat,
        assetAmount: missingAssetAmount,
      },
      note: 'BTC check includes a small 2500 sat fee reserve estimate. Actual channel-open fees can vary.',
    });
  } catch (error) {
    console.error(`[${nowIso()}] [RGB API] Open channel check failed:`, error.message);
    sendJson(res, 502, { ok: false, error: error.message });
  }
}

function waitMs(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function restartDockerContainer(container) {
  const { stdout, stderr } = await execFileAsync('docker', ['restart', container], {
    timeout: 30000,
  });
  return {
    stdout: typeof stdout === 'string' ? stdout.trim() : '',
    stderr: typeof stderr === 'string' ? stderr.trim() : '',
  };
}

function buildRgbNodeUnlockPayload(accountRef, password) {
  const defaults = RGB_NODE_UNLOCK_DEFAULTS[accountRef] || {};
  const resolvedPassword =
    typeof password === 'string' && password.trim() ? password.trim() : defaults.password;

  if (!resolvedPassword) {
    return null;
  }

  return {
    password: resolvedPassword,
    bitcoind_rpc_username: RPC_USER,
    bitcoind_rpc_password: RPC_PASSWORD,
    bitcoind_rpc_host: 'photon-bitcoind',
    bitcoind_rpc_port: RPC_PORT,
    indexer_url: 'tcp://photon-electrs:50001',
    proxy_endpoint: 'rpc://photon-rgb-proxy:3000/json-rpc',
    announce_addresses: [],
    announce_alias: defaults.announceAlias || accountRef,
  };
}

async function unlockRgbNode(accountRef, password = '') {
  const payload = buildRgbNodeUnlockPayload(accountRef, password);
  if (!payload) {
    return {
      attempted: false,
      ok: false,
      skipped: true,
      message:
        'No unlock password configured for this node. Set the matching RGB_*_NODE_PASSWORD environment variable before restarting or unlocking it.',
    };
  }

  const apiBase = resolveRgbNodeApiBaseForAccountRef(accountRef);
  try {
    const response = await rgbNodeRequestWithBase(apiBase, '/unlock', payload);
    return {
      attempted: true,
      ok: true,
      response,
      message: 'Node unlocked.',
    };
  } catch (error) {
    if (String(error.message || '').includes('already been unlocked')) {
      return {
        attempted: true,
        ok: true,
        alreadyUnlocked: true,
        message: 'Node was already unlocked.',
      };
    }
    throw error;
  }
}

async function handleAdminRgbNodeControl(req, res) {
  if (!requireAdminSession(req, res)) {
    return;
  }

  let body;
  try {
    body = await readRequestJson(req);
  } catch (error) {
    sendJson(res, 400, { ok: false, error: error.message });
    return;
  }

  const target = typeof body.target === 'string' && body.target.trim() ? body.target.trim() : '';
  const action = typeof body.action === 'string' && body.action.trim() ? body.action.trim() : '';
  const unlockPassword =
    typeof body.unlockPassword === 'string' && body.unlockPassword.trim()
      ? body.unlockPassword.trim()
      : '';
  const targetConfig = RGB_NODE_CONTROL_TARGETS[target];

  if (!targetConfig) {
    sendJson(res, 400, {
      ok: false,
      error: `target must be one of: ${Object.keys(RGB_NODE_CONTROL_TARGETS).join(', ')}`,
    });
    return;
  }

  if (!['restart', 'refresh', 'restart-refresh'].includes(action)) {
    sendJson(res, 400, {
      ok: false,
      error: 'action must be one of: restart, refresh, restart-refresh.',
    });
    return;
  }

  if (targetConfig.type !== 'rgb-node' && action !== 'restart') {
    sendJson(res, 400, { ok: false, error: `${targetConfig.label} supports restart only.` });
    return;
  }

  try {
    let restart = null;
    let refresh = null;
    let unlock = null;

    if (action === 'restart' || action === 'restart-refresh') {
      restart = await restartDockerContainer(targetConfig.container);
      if (targetConfig.type === 'rgb-node') {
        await waitMs(1500);
        unlock = await unlockRgbNode(targetConfig.accountRef, unlockPassword);
      }
    }

    if (action === 'refresh' || action === 'restart-refresh') {
      refresh = await rgbNodeRequestWithBase(
        resolveRgbNodeApiBaseForAccountRef(targetConfig.accountRef),
        '/refreshtransfers',
        { skip_sync: false }
      );
    }

    sendJson(res, 200, {
      ok: true,
      target: targetConfig.target,
      label: targetConfig.label,
      action,
      container: targetConfig.container,
      restart,
      unlock,
      refresh,
      message:
        action === 'restart-refresh'
          ? `${targetConfig.label} restarted, unlocked, and transfer refresh triggered.`
          : action === 'refresh'
            ? `${targetConfig.label} transfer refresh triggered.`
            : unlock?.skipped
              ? `${targetConfig.label} restarted. Unlock still required.`
              : `${targetConfig.label} restarted and unlocked.`,
    });
  } catch (error) {
    console.error(`[${nowIso()}] [RGB API] Node control failed for ${target}:`, error.message);
    sendJson(res, 502, { ok: false, error: error.message });
  }
}

async function handleRgbCloseChannel(req, res) {
  let body;
  try {
    body = await readRequestJson(req);
  } catch (error) {
    sendJson(res, 400, { ok: false, error: error.message });
    return;
  }

  const accountRef =
    typeof body.accountRef === 'string' && body.accountRef.trim() ? body.accountRef.trim() : '';
  const channelId =
    typeof body.channelId === 'string' && body.channelId.trim() ? body.channelId.trim() : '';
  const peerPubkey =
    typeof body.peerPubkey === 'string' && body.peerPubkey.trim() ? body.peerPubkey.trim() : '';
  const force = Boolean(body.force);

  if (!isKnownRgbAccountRef(accountRef)) {
    sendJson(res, 400, { ok: false, error: getRgbAccountRefError() });
    return;
  }

  if (!/^[0-9a-f]{64}$/i.test(channelId)) {
    sendJson(res, 400, { ok: false, error: 'channelId must be a 32-byte hex channel id.' });
    return;
  }

  if (!/^[0-9a-f]{66}$/i.test(peerPubkey)) {
    sendJson(res, 400, { ok: false, error: 'peerPubkey must be a 33-byte compressed hex pubkey.' });
    return;
  }

  try {
    const apiBase = resolveRgbNodeApiBaseForAccountRef(accountRef);
    const result = await rgbNodeRequestWithBase(apiBase, '/closechannel', {
      channel_id: channelId,
      peer_pubkey: peerPubkey,
      force,
    });

    sendJson(res, 200, {
      ok: true,
      accountRef,
      apiBase,
      request: {
        channel_id: channelId,
        peer_pubkey: peerPubkey,
        force,
      },
      result,
    });
  } catch (error) {
    console.error(`[${nowIso()}] [RGB API] Close channel failed:`, error.message);
    sendJson(res, 502, { ok: false, error: error.message });
  }
}

async function handleBoardStatusesGet(res) {
  try {
    const rows = await listBoardTicketStatuses();
    sendJson(res, 200, {
      ok: true,
      statuses: rows.map((row) => ({
        ticketId: row.ticket_id,
        status: row.status,
        updatedAt: row.updated_at,
      })),
    });
  } catch (error) {
    console.error(`[${nowIso()}] [BOARD API] Failed to load ticket statuses:`, error.message);
    sendJson(res, 502, { ok: false, error: error.message });
  }
}

async function handleBoardStatusUpsert(req, res) {
  let body;
  try {
    body = await readRequestJson(req);
  } catch (error) {
    sendJson(res, 400, { ok: false, error: error.message });
    return;
  }

  const ticketId = typeof body.ticketId === 'string' ? body.ticketId.trim() : '';
  const status = typeof body.status === 'string' ? body.status.trim() : '';

  if (!ticketId) {
    sendJson(res, 400, { ok: false, error: 'ticketId is required.' });
    return;
  }

  if (!['todo', 'progress', 'review', 'done'].includes(status)) {
    sendJson(res, 400, { ok: false, error: 'status must be one of: todo, progress, review, done.' });
    return;
  }

  try {
    const row = await upsertBoardTicketStatus({ ticketId, status });
    sendJson(res, 200, {
      ok: true,
      ticketId: row.ticket_id,
      status: row.status,
      updatedAt: row.updated_at,
    });
  } catch (error) {
    console.error(`[${nowIso()}] [BOARD API] Failed to update ticket status:`, error.message);
    sendJson(res, 502, { ok: false, error: error.message });
  }
}

function normalizeBoardTicketRow(row) {
  return {
    id: row.ticket_id,
    title: row.title,
    status: row.status,
    priority: row.priority,
    category: row.category,
    estimate: row.estimate,
    assignee: row.assignee,
    desc: row.desc_html,
    links: Array.isArray(row.links) ? row.links : [],
    isCustom: true,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

function escapeHtml(value) {
  return String(value || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function plainTextToHtmlParagraphs(value) {
  const blocks = String(value || '')
    .split(/\n{2,}/)
    .map((block) => block.trim())
    .filter(Boolean);

  if (blocks.length === 0) {
    return '<p>No description provided.</p>';
  }

  return blocks
    .map((block) => `<p>${escapeHtml(block).replace(/\n/g, '<br>')}</p>`)
    .join('');
}

async function handleBoardTicketsGet(res) {
  try {
    const rows = await listBoardTickets();
    sendJson(res, 200, {
      ok: true,
      tickets: rows.map(normalizeBoardTicketRow),
    });
  } catch (error) {
    console.error(`[${nowIso()}] [BOARD API] Failed to load custom tickets:`, error.message);
    sendJson(res, 502, { ok: false, error: error.message });
  }
}

async function handleBoardTicketCreate(req, res) {
  if (!requireBoardSession(req, res)) {
    return;
  }

  let body;
  try {
    body = await readRequestJson(req);
  } catch (error) {
    sendJson(res, 400, { ok: false, error: error.message });
    return;
  }

  const title = typeof body.title === 'string' ? body.title.trim() : '';
  const status = typeof body.status === 'string' ? body.status.trim() : 'todo';
  const priority = typeof body.priority === 'string' ? body.priority.trim() : 'medium';
  const category = typeof body.category === 'string' ? body.category.trim() : 'backend';
  const estimate = typeof body.estimate === 'string' && body.estimate.trim() ? body.estimate.trim() : '1d';
  const assignee = typeof body.assignee === 'string' && body.assignee.trim() ? body.assignee.trim() : '—';
  const description = typeof body.description === 'string' ? body.description.trim() : '';
  const links = Array.isArray(body.links)
    ? body.links
        .filter((entry) => entry && typeof entry.label === 'string' && typeof entry.href === 'string')
        .map((entry) => ({ label: entry.label.trim(), href: entry.href.trim() }))
        .filter((entry) => entry.label && entry.href)
    : [];

  if (!title) {
    sendJson(res, 400, { ok: false, error: 'title is required.' });
    return;
  }

  if (!['todo', 'progress', 'review', 'done'].includes(status)) {
    sendJson(res, 400, { ok: false, error: 'status must be one of: todo, progress, review, done.' });
    return;
  }

  if (!['high', 'medium', 'low'].includes(priority)) {
    sendJson(res, 400, { ok: false, error: 'priority must be one of: high, medium, low.' });
    return;
  }

  if (!['ui', 'android', 'node', 'token', 'research', 'backend', 'infra'].includes(category)) {
    sendJson(res, 400, { ok: false, error: 'category is invalid.' });
    return;
  }

  const numericIds = await listBoardTickets();
  const highestExisting = numericIds
    .map((row) => {
      const match = String(row.ticket_id || '').match(/^PHO-(\d+)$/);
      return match ? Number(match[1]) : 0;
    })
    .reduce((max, value) => Math.max(max, value), 20);
  const ticketId = `PHO-${String(highestExisting + 1).padStart(3, '0')}`;

  try {
    const row = await createBoardTicket({
      ticketId,
      title,
      status,
      priority,
      category,
      estimate,
      assignee,
      descHtml: plainTextToHtmlParagraphs(description),
      links,
    });
    await upsertBoardTicketStatus({ ticketId, status });
    sendJson(res, 200, {
      ok: true,
      ticket: normalizeBoardTicketRow(row),
    });
  } catch (error) {
    console.error(`[${nowIso()}] [BOARD API] Failed to create ticket:`, error.message);
    sendJson(res, 502, { ok: false, error: error.message });
  }
}

async function handleBoardTicketUpdate(req, res, parsedUrl) {
  if (!requireBoardSession(req, res)) {
    return;
  }

  const ticketId =
    typeof parsedUrl.searchParams.get('id') === 'string' ? parsedUrl.searchParams.get('id').trim() : '';
  if (!ticketId) {
    sendJson(res, 400, { ok: false, error: 'id is required.' });
    return;
  }

  let body;
  try {
    body = await readRequestJson(req);
  } catch (error) {
    sendJson(res, 400, { ok: false, error: error.message });
    return;
  }

  const title = typeof body.title === 'string' ? body.title.trim() : '';
  const status = typeof body.status === 'string' ? body.status.trim() : 'todo';
  const priority = typeof body.priority === 'string' ? body.priority.trim() : 'medium';
  const category = typeof body.category === 'string' ? body.category.trim() : 'backend';
  const estimate = typeof body.estimate === 'string' && body.estimate.trim() ? body.estimate.trim() : '1d';
  const assignee = typeof body.assignee === 'string' && body.assignee.trim() ? body.assignee.trim() : '—';
  const description = typeof body.description === 'string' ? body.description.trim() : '';
  const links = Array.isArray(body.links)
    ? body.links
        .filter((entry) => entry && typeof entry.label === 'string' && typeof entry.href === 'string')
        .map((entry) => ({ label: entry.label.trim(), href: entry.href.trim() }))
        .filter((entry) => entry.label && entry.href)
    : [];

  if (!title) {
    sendJson(res, 400, { ok: false, error: 'title is required.' });
    return;
  }

  if (!['todo', 'progress', 'review', 'done'].includes(status)) {
    sendJson(res, 400, { ok: false, error: 'status must be one of: todo, progress, review, done.' });
    return;
  }

  if (!['high', 'medium', 'low'].includes(priority)) {
    sendJson(res, 400, { ok: false, error: 'priority must be one of: high, medium, low.' });
    return;
  }

  if (!['ui', 'android', 'node', 'token', 'research', 'backend', 'infra'].includes(category)) {
    sendJson(res, 400, { ok: false, error: 'category is invalid.' });
    return;
  }

  try {
    const row = await updateBoardTicket({
      ticketId,
      title,
      status,
      priority,
      category,
      estimate,
      assignee,
      descHtml: plainTextToHtmlParagraphs(description),
      links,
    });
    if (!row) {
      sendJson(res, 404, { ok: false, error: 'Ticket not found.' });
      return;
    }
    await upsertBoardTicketStatus({ ticketId, status });
    sendJson(res, 200, {
      ok: true,
      ticket: normalizeBoardTicketRow(row),
    });
  } catch (error) {
    console.error(`[${nowIso()}] [BOARD API] Failed to update ticket:`, error.message);
    sendJson(res, 502, { ok: false, error: error.message });
  }
}

async function handleBoardTicketDelete(req, res, parsedUrl) {
  if (!requireBoardSession(req, res)) {
    return;
  }

  const ticketId =
    typeof parsedUrl.searchParams.get('id') === 'string' ? parsedUrl.searchParams.get('id').trim() : '';
  if (!ticketId) {
    sendJson(res, 400, { ok: false, error: 'id is required.' });
    return;
  }

  try {
    const deleted = await deleteBoardTicket(ticketId);
    if (!deleted) {
      sendJson(res, 404, { ok: false, error: 'Ticket not found.' });
      return;
    }
    await deleteBoardTicketStatus(ticketId);
    sendJson(res, 200, { ok: true, ticketId });
  } catch (error) {
    console.error(`[${nowIso()}] [BOARD API] Failed to delete ticket:`, error.message);
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

async function handleRgbFaucetClaim(req, res) {
  let body;
  try {
    body = await readRequestJson(req);
  } catch (error) {
    sendJson(res, 400, { ok: false, error: error.message });
    return;
  }

  const invoice = typeof body.invoice === 'string' && body.invoice.trim() ? body.invoice.trim() : null;
  const ip = getRemoteIp(req);

  if (!invoice) {
    sendJson(res, 400, { ok: false, error: 'An RGB invoice is required.' });
    return;
  }

  const recent = RGB_FAUCET_COOLDOWN_PAUSED ? null : getRecentRgbClaim(ip, invoice);
  if (recent) {
    sendJson(res, 429, { ok: false, ...buildCooldownMessage(recent) });
    return;
  }

  try {
    const wallet = await ensureWalletWithFunding(req, 'regtest');
    const { apiBase } = resolveWalletNodeContext(wallet);
    const invoiceKind = detectRgbInvoiceKind(invoice);

    if (!invoiceKind) {
      throw new Error('Invoice format is not supported.');
    }

    let decoded;
    let assetId = null;
    let assetAmount = 0;
    let transferTxid = null;
    let paymentHash = null;
    let minedBlocks = [];

    if (invoiceKind === 'lightning') {
      decoded = await rgbNodeRequestWithBase(apiBase, '/decodelninvoice', { invoice });
      assetId = typeof decoded?.asset_id === 'string' ? decoded.asset_id : null;
      assetAmount = Number(decoded?.asset_amount || 0);

      if (!assetId || !Number.isFinite(assetAmount) || assetAmount <= 0) {
        throw new Error('Decoded RGB Lightning invoice is missing asset details.');
      }

      if (assetAmount > RGB_FAUCET_MAX_AMOUNT) {
        sendJson(res, 400, {
          ok: false,
          error: `RGB faucet limit exceeded. Maximum claim is ${RGB_FAUCET_MAX_AMOUNT} units per invoice.`,
          maxAmount: RGB_FAUCET_MAX_AMOUNT,
        });
        return;
      }
    } else {
      decoded = await rgbNodeRequestWithBase(apiBase, '/decodergbinvoice', { invoice });
      assetId = typeof decoded?.asset_id === 'string' ? decoded.asset_id : null;
      assetAmount = Number(decoded?.assignment?.value || 0);
      const recipientId = typeof decoded?.recipient_id === 'string' ? decoded.recipient_id : null;
      const endpoint = Array.isArray(decoded?.transport_endpoints) ? decoded.transport_endpoints[0] : null;

      if (!assetId || !recipientId || !Number.isFinite(assetAmount) || assetAmount <= 0 || !endpoint) {
        throw new Error('Decoded RGB invoice is missing required fields.');
      }

      if (assetAmount > RGB_FAUCET_MAX_AMOUNT) {
        sendJson(res, 400, {
          ok: false,
          error: `RGB faucet limit exceeded. Maximum claim is ${RGB_FAUCET_MAX_AMOUNT} units per invoice.`,
          maxAmount: RGB_FAUCET_MAX_AMOUNT,
        });
        return;
      }
    }

    const registryResult = await query(
      `
        SELECT token_name, ticker, precision, contract_id
        FROM asset_registry
        WHERE contract_id = $1
          AND COALESCE((metadata->>'archived')::boolean, FALSE) = FALSE
        LIMIT 1
      `,
      [assetId]
    );
    const asset = registryResult.rows[0] || null;

    if (!asset) {
      sendJson(res, 400, {
        ok: false,
        error: 'This RGB asset is not available in the Photon faucet registry.',
        maxAmount: RGB_FAUCET_MAX_AMOUNT,
      });
      return;
    }

    if (invoiceKind === 'lightning') {
      const lightningResult = await executeRgbLightningPayment({
        req,
        invoice,
        eventSource: 'faucet_api',
      });
      decoded = lightningResult.decoded;
      paymentHash = lightningResult.payment?.payment_hash || lightningResult.paymentResult?.payment_hash || decoded?.payment_hash || null;
    } else {
      const recipientId = typeof decoded?.recipient_id === 'string' ? decoded.recipient_id : null;
      const endpoint = Array.isArray(decoded?.transport_endpoints) ? decoded.transport_endpoints[0] : null;

      const sendResult = await rgbNodeRequestWithBase(apiBase, '/sendrgb', {
        donation: false,
        fee_rate: 5,
        min_confirmations: 1,
        recipient_map: {
          [assetId]: [
            {
              recipient_id: recipientId,
              assignment: {
                type: 'Fungible',
                value: assetAmount,
              },
              transport_endpoints: [endpoint],
            },
          ],
        },
        skip_sync: false,
      });
      transferTxid = sendResult?.txid || null;

      if (AUTO_MINE_BLOCKS > 0 && transferTxid) {
        const miningAddress = await rpcRequest('getnewaddress', ['rgb-faucet-mining', MINING_ADDRESS_TYPE], true);
        minedBlocks = await rpcRequest('generatetoaddress', [AUTO_MINE_BLOCKS, miningAddress]);
      }
    }

    claimState.rgbClaims.push({
      ip,
      invoice,
      invoiceKind,
      assetId,
      assetAmount,
      timestamp: Date.now(),
      txid: transferTxid,
      paymentHash,
    });
    await persistState();

    sendJson(res, 200, {
      ok: true,
      invoiceKind,
      network: 'regtest',
      cooldownMinutes: RGB_FAUCET_COOLDOWN_PAUSED ? 0 : COOLDOWN_MINUTES,
      cooldownPaused: RGB_FAUCET_COOLDOWN_PAUSED,
      maxAmount: RGB_FAUCET_MAX_AMOUNT,
      asset: asset || { contract_id: assetId },
      amount: assetAmount,
      txid: transferTxid,
      paymentHash,
      minedBlocks,
      decoded,
    });
  } catch (error) {
    console.error(`[${nowIso()}] RGB faucet claim failed for ${ip}:`, error.message);
    sendJson(res, 502, { ok: false, error: error.message, maxAmount: RGB_FAUCET_MAX_AMOUNT });
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

  if (req.method === 'GET' && pathname === '/api/rgb/issue-asset-readiness') {
    await handleRgbIssueAssetReadiness(req, res, parsedUrl);
    return;
  }

  if (req.method === 'POST' && pathname === '/api/rgb/issue-asset') {
    await handleRgbIssueAsset(req, res);
    return;
  }

  if (req.method === 'POST' && pathname === '/api/rgb/archive-asset') {
    await handleRgbArchiveAsset(req, res);
    return;
  }

  if (req.method === 'POST' && pathname === '/api/rgb/ln-invoice') {
    await handleRgbLightningInvoice(req, res);
    return;
  }

  if (req.method === 'POST' && pathname === '/api/channel/apply') {
    await handleChannelApplicationCreate(req, res);
    return;
  }

  if (req.method === 'GET' && pathname === '/api/channel/application') {
    await handleChannelApplicationStatus(req, res, parsedUrl);
    return;
  }

  if (req.method === 'POST' && pathname === '/api/rgb/open-channel') {
    await handleRgbOpenChannel(req, res);
    return;
  }

  if (req.method === 'GET' && pathname === '/api/rgb/open-channel-check') {
    await handleRgbOpenChannelCheck(req, res, parsedUrl);
    return;
  }

  if (req.method === 'POST' && pathname === '/api/rgb/close-channel') {
    await handleRgbCloseChannel(req, res);
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

  if (req.method === 'POST' && pathname === '/api/rgb/faucet/claim') {
    await handleRgbFaucetClaim(req, res);
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

  if (req.method === 'GET' && pathname === '/api/rgb/channel-dashboard') {
    await handleRgbChannelDashboard(res);
    return;
  }

  if (req.method === 'GET' && pathname === '/api/board/statuses') {
    await handleBoardStatusesGet(res);
    return;
  }

  if (req.method === 'POST' && pathname === '/api/board/status') {
    await handleBoardStatusUpsert(req, res);
    return;
  }

  if (req.method === 'GET' && pathname === '/api/board/tickets') {
    await handleBoardTicketsGet(res);
    return;
  }

  if (req.method === 'POST' && pathname === '/api/board/tickets') {
    await handleBoardTicketCreate(req, res);
    return;
  }

  if (req.method === 'PUT' && pathname === '/api/board/tickets') {
    await handleBoardTicketUpdate(req, res, parsedUrl);
    return;
  }

  if (req.method === 'DELETE' && pathname === '/api/board/tickets') {
    await handleBoardTicketDelete(req, res, parsedUrl);
    return;
  }

  if (req.method === 'GET' && pathname === '/api/admin/auth/challenge') {
    await handleAdminAuthChallenge(res, parsedUrl);
    return;
  }

  if (req.method === 'GET' && pathname === '/api/admin/auth/config') {
    await handleAdminAuthConfig(res);
    return;
  }

  if (req.method === 'POST' && pathname === '/api/admin/auth/verify') {
    await handleAdminAuthVerify(req, res);
    return;
  }

  if (req.method === 'POST' && pathname === '/api/admin/auth/logout') {
    await handleAdminAuthLogout(req, res);
    return;
  }

  if (req.method === 'GET' && pathname === '/api/rgb/wallet-assignments') {
    if (!requireAdminSession(req, res)) return;
    await handleRgbWalletAssignments(res);
    return;
  }

  if (req.method === 'POST' && pathname === '/api/rgb/wallet-assign') {
    if (!requireAdminSession(req, res)) return;
    await handleRgbWalletAssignmentUpdate(req, res);
    return;
  }

  if (req.method === 'POST' && pathname === '/api/admin/rgb-node-control') {
    if (!requireAdminSession(req, res)) return;
    await handleAdminRgbNodeControl(req, res);
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

  if (req.method === 'GET' && pathname === '/api/utxo/funding-address') {
    await handleGetFundingAddress(req, res);
    return;
  }

  if (req.method === 'GET' && pathname === '/api/utxo/slots') {
    await handleGetUtxoSlots(req, res);
    return;
  }

  if (req.method === 'POST' && pathname === '/api/utxo/redeem') {
    await handleRedeemUtxoSlot(req, res);
    return;
  }

  if (req.method === 'POST' && pathname === '/api/utxo/deposit-watcher/trigger') {
    runDepositWatcherCycle();
    sendJson(res, 200, { ok: true, message: 'Deposit watcher cycle triggered.' });
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

          // Belt-and-suspenders: ensure the UTXO slot is marked OCCUPIED
          const watchdogReceiveUtxo = matched.receive_utxo || null;
          if (watchdogReceiveUtxo) {
            try {
              const updated = await markSlotOccupied(watchdogReceiveUtxo, row.transfer_id);
              if (updated) console.log(`[${nowIso()}] [UTXO] watchdog slot OCCUPIED outpoint=${watchdogReceiveUtxo}`);
            } catch (slotErr) {
              console.warn(`[${nowIso()}] [UTXO] watchdog slot transition failed outpoint=${watchdogReceiveUtxo}:`, slotErr.message);
            }
          }

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

// ─────────────────────────────────────────────────────────────────────────────
// UTXO Deposit Watcher
//
// Polls bitcoind for incoming transactions on all active UTXO funding addresses.
// On sufficient confirmations calls /createutxos on the wallet's node, records
// the new slot as FREE, and marks the deposit request as confirmed.
// ─────────────────────────────────────────────────────────────────────────────

const DEPOSIT_WATCHER_INTERVAL_MS = Number(process.env.DEPOSIT_WATCHER_INTERVAL_MS || 15000);
const DEPOSIT_REQUIRED_CONFIRMATIONS = Number(process.env.DEPOSIT_REQUIRED_CONFIRMATIONS || 1);

let _depositWatcherRunning = false;

async function depositWatcherCycle() {
  // Expire stale requests first
  const expired = await expireStaleDeposits();
  if (expired.length > 0) {
    console.log(`[${nowIso()}] [DepositWatcher] Expired ${expired.length} stale deposit request(s)`);
  }

  // Fetch all pending/confirming requests
  const active = await getActiveDepositRequests();
  if (active.length === 0) return;

  console.log(`[${nowIso()}] [DepositWatcher] Checking ${active.length} active deposit request(s)`);

  for (const req of active) {
    const tag = `[DepositWatcher] wallet=${req.wallet_key} addr=${req.deposit_address.slice(0, 16)}…`;
    try {
      // Scan the UTXO set for this address
      const scan = await rpcRequest('scantxoutset', ['start', [`addr(${req.deposit_address})`]]);
      const unspents = Array.isArray(scan?.unspents) ? scan.unspents : [];

      if (unspents.length === 0) {
        // Nothing received yet — check mempool for unconfirmed tx
        const mempool = await rpcRequest('getrawmempool', [true]);
        const mempoolTxids = Object.keys(mempool || {});
        let mempoolHit = null;

        for (const txid of mempoolTxids) {
          try {
            const raw = await rpcRequest('getrawtransaction', [txid, true]);
            const matchingVout = (raw.vout || []).find((v) =>
              v.scriptPubKey?.address === req.deposit_address
            );
            if (matchingVout) {
              mempoolHit = { txid, sats: btcToSats(matchingVout.value) };
              break;
            }
          } catch (_) { /* skip */ }
        }

        if (mempoolHit && req.status === 'pending') {
          await updateDepositDetected({
            id: req.id,
            depositTxid: mempoolHit.txid,
            receivedSats: mempoolHit.sats,
            confirmations: 0,
          });
          console.log(`${tag} — tx detected in mempool txid=${mempoolHit.txid} sats=${mempoolHit.sats}`);
        }
        continue;
      }

      // UTXO found in confirmed UTXO set
      const utxo = unspents[0];
      const receivedSats = btcToSats(utxo.amount);

      // Get confirmation count from the tx
      let confirmations = 0;
      let txid = req.deposit_txid;
      try {
        const raw = await rpcRequest('getrawtransaction', [utxo.txid, true]);
        confirmations = raw.confirmations || 0;
        txid = utxo.txid;
      } catch (_) { /* use existing txid */ }

      // Update detected if not already
      if (req.status === 'pending') {
        await updateDepositDetected({
          id: req.id,
          depositTxid: txid,
          receivedSats,
          confirmations,
        });
        console.log(`${tag} — tx confirmed on-chain txid=${txid} sats=${receivedSats} confirmations=${confirmations}`);
      } else {
        await updateDepositConfirmations({ id: req.id, confirmations });
      }

      // Check if we have enough confirmations
      const required = req.required_confirmations || DEPOSIT_REQUIRED_CONFIRMATIONS;
      if (confirmations < required) {
        console.log(`${tag} — ${confirmations}/${required} confirmations, waiting…`);
        continue;
      }

      // Validate amount (allow up to 10% variance for fee fluctuations)
      if (receivedSats < req.expected_sats * 0.9) {
        await markDepositFailed({
          id: req.id,
          errorMessage: `Received ${receivedSats} sats but expected at least ${Math.floor(req.expected_sats * 0.9)} sats`,
        });
        console.warn(`${tag} — insufficient amount received (${receivedSats} sats), marking failed`);
        continue;
      }

      // Confirmed and valid — call /createutxos on the wallet's node
      console.log(`${tag} — ${confirmations} confirmations reached, calling /createutxos…`);
      const apiBase = resolveRgbNodeApiBaseForAccountRef(req.node_account_ref);

      let createResult;
      try {
        createResult = await rgbNodeRequestWithBase(apiBase, '/createutxos', {
          up_to: false,
          num: 1,
          size: 32500,
          fee_rate: 5,
          skip_sync: false,
        });
      } catch (nodeErr) {
        await markDepositFailed({ id: req.id, errorMessage: nodeErr.message });
        console.error(`${tag} — /createutxos failed: ${nodeErr.message}`);
        continue;
      }

      // Record the new slot as FREE
      const slot = await upsertUtxoSlot({
        walletId: req.wallet_id,
        outpoint: `${txid}:0`,         // placeholder — actual outpoint from node if exposed
        state: 'FREE',
        satsValue: receivedSats,
        nodeAccountRef: req.node_account_ref,
        invoiceId: null,
        transferId: null,
      });

      // Mark deposit confirmed
      await markDepositConfirmed({ id: req.id, utxoSlotId: slot.id });

      console.log(`${tag} — slot created FREE outpoint=${slot.outpoint} slotId=${slot.id} ✓`);
    } catch (err) {
      console.error(`${tag} — unexpected error: ${err.message}`);
    }
  }
}

async function runDepositWatcherCycle() {
  if (_depositWatcherRunning) return;
  _depositWatcherRunning = true;
  try {
    await depositWatcherCycle();
  } catch (err) {
    console.error(`[${nowIso()}] [DepositWatcher] Unhandled error:`, err.message);
  } finally {
    _depositWatcherRunning = false;
  }
}

function startDepositWatcher() {
  console.log(`[${nowIso()}] [DepositWatcher] Started (interval=${DEPOSIT_WATCHER_INTERVAL_MS}ms, required_confirmations=${DEPOSIT_REQUIRED_CONFIRMATIONS})`);
  setInterval(runDepositWatcherCycle, DEPOSIT_WATCHER_INTERVAL_MS);
  setImmediate(runDepositWatcherCycle);
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
    startDepositWatcher();
  });
}

start().catch((error) => {
  console.error(`[${nowIso()}] Failed to start faucet server:`, error);
  process.exitCode = 1;
});
