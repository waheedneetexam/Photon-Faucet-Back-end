const channelSelect = document.getElementById('channel-select');
const channelSearch = document.getElementById('channel-search');
const refreshButton = document.getElementById('refresh-button');
const dashboardStatus = document.getElementById('dashboard-status');
const channelTableStatus = document.getElementById('channel-table-status');
const dashboardSubtitle = document.getElementById('dashboard-subtitle');
const channelsBody = document.getElementById('channels-body');
const detailTitle = document.getElementById('detail-title');
const detailMeta = document.getElementById('detail-meta');
const detailFunding = document.getElementById('detail-funding');
const detailShortId = document.getElementById('detail-short-id');
const detailAssetCapacity = document.getElementById('detail-asset-capacity');
const detailBtcCapacity = document.getElementById('detail-btc-capacity');
const detailOwner = document.getElementById('detail-owner');
const detailNodesBody = document.getElementById('detail-nodes-body');
const paymentsBody = document.getElementById('payments-body');
const paymentsMeta = document.getElementById('payments-meta');
const openChannelForm = document.getElementById('open-channel-form');
const openChannelStatus = document.getElementById('open-channel-status');
const prefillSelectedButton = document.getElementById('prefill-selected-button');
const openAccountRef = document.getElementById('open-account-ref');
const openPeerPubkey = document.getElementById('open-peer-pubkey');
const openAssetId = document.getElementById('open-asset-id');
const openCapacitySat = document.getElementById('open-capacity-sat');
const openAssetAmount = document.getElementById('open-asset-amount');
const openPushMsat = document.getElementById('open-push-msat');
const openFeeBaseMsat = document.getElementById('open-fee-base-msat');
const openFeePpm = document.getElementById('open-fee-ppm');
const openWithAnchors = document.getElementById('open-with-anchors');
const openPublic = document.getElementById('open-public');
const openChannelButton = document.getElementById('open-channel-button');
const checkBtcStatus = document.getElementById('check-btc-status');
const checkBtcDetail = document.getElementById('check-btc-detail');
const checkAssetStatus = document.getElementById('check-asset-status');
const checkAssetDetail = document.getElementById('check-asset-detail');
const checkOpenStatus = document.getElementById('check-open-status');
const checkOpenDetail = document.getElementById('check-open-detail');
const assetPickerButton = document.getElementById('asset-picker-button');
const assetPickerModal = document.getElementById('asset-picker-modal');
const assetPickerClose = document.getElementById('asset-picker-close');
const assetPickerSearch = document.getElementById('asset-picker-search');
const assetPickerStatus = document.getElementById('asset-picker-status');
const assetPickerBody = document.getElementById('asset-picker-body');
const assetSummaryBody = document.getElementById('asset-summary-body');
const detailCloseForce = document.getElementById('detail-close-force');
const detailCloseButton = document.getElementById('detail-close-button');
const detailCloseStatus = document.getElementById('detail-close-status');
const walletMenuTrigger = document.getElementById('wallet-menu-trigger');
const walletMenuPanel = document.getElementById('wallet-menu-panel');
const walletMenuTitle = document.getElementById('wallet-menu-title');
const walletMenuNetwork = document.getElementById('wallet-menu-network');
const walletMenuAddress = document.getElementById('wallet-menu-address');
const walletAddressCopyButton = document.getElementById('wallet-address-copy');
const walletMenuStatus = document.getElementById('wallet-menu-status');
const walletBtcBalance = document.getElementById('wallet-btc-balance');
const walletPhoBalance = document.getElementById('wallet-pho-balance');
const walletLightBalance = document.getElementById('wallet-light-balance');
const walletConnectButton = document.getElementById('wallet-connect-button');
const walletDisconnectButton = document.getElementById('wallet-disconnect-button');
const plmApplicationForm = document.getElementById('plm-application-form');
const plmOwnerWallet = document.getElementById('plm-owner-wallet');
const plmAccountRef = document.getElementById('plm-account-ref');
const plmPeerPubkey = document.getElementById('plm-peer-pubkey');
const plmAssetId = document.getElementById('plm-asset-id');
const plmAssetPickerButton = document.getElementById('plm-asset-picker-button');
const plmBtcAmount = document.getElementById('plm-btc-amount');
const plmRgbAmount = document.getElementById('plm-rgb-amount');
const plmCommissionRate = document.getElementById('plm-commission-rate');
const plmTxThreshold = document.getElementById('plm-tx-threshold');
const plmApplyButton = document.getElementById('plm-apply-button');
const plmUseSelectedButton = document.getElementById('plm-use-selected');
const plmResetButton = document.getElementById('plm-reset-button');
const plmRefreshButton = document.getElementById('plm-refresh-button');
const plmStatusLabel = document.getElementById('plm-status-label');
const plmStatusDetail = document.getElementById('plm-status-detail');
const plmFundingProgress = document.getElementById('plm-funding-progress');
const plmFundingDetail = document.getElementById('plm-funding-detail');
const plmEarnings = document.getElementById('plm-earnings');
const plmBtcAddress = document.getElementById('plm-btc-address');
const plmBtcSummary = document.getElementById('plm-btc-summary');
const plmRgbInvoice = document.getElementById('plm-rgb-invoice');
const plmRgbSummary = document.getElementById('plm-rgb-summary');
const plmPayBtcButton = document.getElementById('plm-pay-btc-button');
const plmPayRgbButton = document.getElementById('plm-pay-rgb-button');
const plmProgressCount = document.getElementById('plm-progress-count');
const plmProgressFill = document.getElementById('plm-progress-fill');
const plmApplicationStatus = document.getElementById('plm-application-status');
const plmStepper = document.getElementById('plm-stepper');
const dashboardTabButtons = Array.from(document.querySelectorAll('[data-dashboard-tab]'));
const dashboardTabPanels = Array.from(document.querySelectorAll('[data-tab-panel]'));
const walletAssignmentRefreshButton = document.getElementById('wallet-assignment-refresh-button');
const walletAssignmentStatus = document.getElementById('wallet-assignment-status');
const walletAssignmentBody = document.getElementById('wallet-assignment-body');
const walletAssignmentFilterInput = document.getElementById('wallet-assignment-filter-input');
const walletAssignmentFilterType = document.getElementById('wallet-assignment-filter-type');
const walletAssignmentForceToggle = document.getElementById('wallet-assignment-force-toggle');
const adminWalletCard = document.getElementById('admin-wallet-card');
const walletAssignmentCard = document.getElementById('wallet-assignment-card');
const adminOperatorTab = document.getElementById('admin-operator-tab');
const adminAuthButton = document.getElementById('admin-auth-button');
const adminLogoutButton = document.getElementById('admin-logout-button');
const adminAuthStatus = document.getElementById('admin-auth-status');
const openChannelCard = document.getElementById('open-channel-card');

const metricChannels = document.getElementById('metric-channels');
const metricAsset = document.getElementById('metric-asset');
const metricSendable = document.getElementById('metric-sendable');
const metricPayments = document.getElementById('metric-payments');

let dashboardData = [];
let filteredChannels = [];
let selectedChannelId = '';
let refreshTimer = null;
let openChannelCheckTimer = null;
let lastOpenChannelCheckPassed = false;
let registryAssets = [];
let filteredRegistryAssets = [];
let walletMenuOpen = false;
let walletMenuRefreshTimer = null;
let plmState = null;
let assetPickerTarget = 'open';
let walletAssignments = [];
let registeredRgbNodes = [];
let adminSessionToken = null;
let activeDashboardTab = 'overview';
let pendingWalletAssignments = new Map();

const PHO_ASSET_CANDIDATES = [
  'pho',
  'PHO',
  'Photon Token',
  'rgb:2Mhfmuc0-BqWCUwP-kkJKF_V-F1~L4j6-A1_W6Yy-hK6Z~rA',
];
const LIGHT_ASSET_CANDIDATES = [
  'light',
  'LIGHT',
  'lightning-btc',
  'Lightning BTC',
  'lbtc',
  'ckBTC',
];

const PLM_STATE_KEY = 'photonbolt-dev-dashboard-plm-state-v1';
const ADMIN_SESSION_STORAGE_KEY = 'photonbolt-dev-admin-session-v1';
let adminWalletAddress = '';

function shorten(value, start = 10, end = 8) {
  if (!value || value.length <= start + end + 3) return value || '-';
  return `${value.slice(0, start)}...${value.slice(-end)}`;
}

function shortenWalletAddress(value) {
  if (!value) return '---';
  if (value.length <= 9) return value;
  return `${value.slice(0, 3)}...${value.slice(-3)}`;
}

function formatNumber(value, suffix = '') {
  const number = Number(value || 0);
  return `${new Intl.NumberFormat('en-US').format(number)}${suffix}`;
}

function formatDateTime(value) {
  if (!value) return '-';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
  }).format(date);
}

function statusClass(status) {
  const normalized = String(status || '').toLowerCase();
  if (normalized.includes('success') || normalized.includes('ready') || normalized.includes('open')) return 'success';
  if (normalized.includes('fail')) return 'failed';
  if (normalized.includes('pending')) return 'pending';
  return '';
}

function accountRefLabel(accountRef) {
  const registeredNode = registeredRgbNodes.find((node) => node.accountRef === accountRef);
  if (registeredNode?.label) return registeredNode.label;
  if (accountRef === 'photon-rln-user') return 'User Node';
  if (accountRef === 'photon-rln-user-b') return 'User Node B';
  if (accountRef === 'photon-rln-issuer') return 'Issuer Node';
  return accountRef || '-';
}

function renderWalletAssignmentSelectOptions(selectedAccountRef) {
  const nodes = registeredRgbNodes.length
    ? registeredRgbNodes
    : [
        { accountRef: 'photon-rln-user', label: 'User Node' },
        { accountRef: 'photon-rln-user-b', label: 'User Node B' },
        { accountRef: 'photon-rln-issuer', label: 'Issuer Node' },
      ];

  return nodes
    .map((node) => `<option value="${node.accountRef}" ${selectedAccountRef === node.accountRef ? 'selected' : ''}>${node.label}</option>`)
    .join('');
}

function setActiveDashboardTab(nextTab) {
  const resolvedTab = nextTab || 'overview';
  const adminVisible = adminOperatorTab && !adminOperatorTab.classList.contains('hidden');
  activeDashboardTab = resolvedTab === 'admin' && !adminVisible ? 'overview' : resolvedTab;

  dashboardTabButtons.forEach((button) => {
    button.classList.toggle('is-active', button.dataset.dashboardTab === activeDashboardTab);
  });

  dashboardTabPanels.forEach((panel) => {
    panel.classList.toggle('tab-panel-hidden', panel.dataset.tabPanel !== activeDashboardTab);
  });
}

function loadStoredAdminSessionToken() {
  try {
    return window.localStorage?.getItem(ADMIN_SESSION_STORAGE_KEY) || null;
  } catch {
    return null;
  }
}

function persistAdminSessionToken(token) {
  adminSessionToken = token || null;
  try {
    if (!window.localStorage) return;
    if (token) {
      window.localStorage.setItem(ADMIN_SESSION_STORAGE_KEY, token);
    } else {
      window.localStorage.removeItem(ADMIN_SESSION_STORAGE_KEY);
    }
  } catch {
    // ignore storage errors
  }
}

function getConnectedWalletAddress() {
  return walletMenuAddress?.textContent?.trim() || '';
}

function isAdminWalletAddress(address) {
  return Boolean(adminWalletAddress) && typeof address === 'string' && address.trim() === adminWalletAddress;
}

async function loadAdminConfig() {
  try {
    const response = await fetch('/api/admin/auth/config', {
      method: 'GET',
      headers: { 'Content-Type': 'application/json' },
    });
    const payload = await response.json();
    if (!response.ok || !payload.ok) {
      throw new Error(payload.error || 'Failed to load admin configuration.');
    }
    adminWalletAddress = typeof payload.adminWalletAddress === 'string' ? payload.adminWalletAddress.trim() : '';
  } catch (error) {
    console.error('Admin config load failed:', error);
    adminWalletAddress = '';
  }

  updateAdminUi();
}

function setAdminAuthStatus(message) {
  if (adminAuthStatus) {
    adminAuthStatus.textContent = message;
  }
}

function updateAdminUi() {
  const connectedAddress = getConnectedWalletAddress();
  const matchesAdminWallet = isAdminWalletAddress(connectedAddress);
  const isAuthenticated = Boolean(adminSessionToken) && matchesAdminWallet;
  const showAdminTab = matchesAdminWallet || isAuthenticated;

  if (adminOperatorTab) {
    adminOperatorTab.classList.toggle('hidden', !showAdminTab);
  }

  if (adminWalletCard) {
    adminWalletCard.classList.toggle('hidden', !matchesAdminWallet);
  }

  if (walletAssignmentCard) {
    walletAssignmentCard.classList.toggle('hidden', !isAuthenticated);
  }

  if (openChannelCard) {
    openChannelCard.classList.toggle('hidden', !isAuthenticated);
  }

  if (openChannelButton) {
    openChannelButton.disabled = !isAuthenticated;
  }

  if (adminAuthButton) {
    adminAuthButton.disabled = !matchesAdminWallet;
  }

  if (adminLogoutButton) {
    adminLogoutButton.disabled = !isAuthenticated;
  }

  if (!adminWalletAddress) {
    setAdminAuthStatus('Admin wallet is not configured on the server.');
    if (activeDashboardTab === 'admin') {
      setActiveDashboardTab('overview');
    } else {
      setActiveDashboardTab(activeDashboardTab);
    }
    return;
  }

  if (!matchesAdminWallet) {
    setAdminAuthStatus('Connect the configured admin wallet to unlock sensitive controls.');
    if (activeDashboardTab === 'admin') {
      setActiveDashboardTab('overview');
    } else {
      setActiveDashboardTab(activeDashboardTab);
    }
    return;
  }

  if (isAuthenticated) {
    setAdminAuthStatus('Admin wallet authenticated. Sensitive controls are unlocked.');
    setActiveDashboardTab(activeDashboardTab);
    return;
  }

  setAdminAuthStatus('Admin wallet detected. Sign the challenge to unlock wallet assignment controls.');
  setActiveDashboardTab(activeDashboardTab);
}

function buildAdminHeaders(extraHeaders = {}) {
  return adminSessionToken
    ? { ...extraHeaders, 'x-photon-admin-token': adminSessionToken }
    : extraHeaders;
}

function setWalletAssignmentStatus(message) {
  if (walletAssignmentStatus) {
    walletAssignmentStatus.textContent = message;
  }
}

function formatWalletAssignmentDate(value) {
  if (!value) return 'Never';
  return formatDateTime(value);
}

function getFilteredWalletAssignments() {
  const query = walletAssignmentFilterInput?.value?.trim().toLowerCase() || '';
  const type = walletAssignmentFilterType?.value || 'all';

  return walletAssignments.filter((wallet) => {
    const isExtension = wallet.walletKey.startsWith('extension-');
    const isAssigned = Boolean(wallet.rgbAccountRef);
    const isDevTest = !isExtension;

    if (type === 'extension' && !isExtension) return false;
    if (type === 'assigned' && !isAssigned) return false;
    if (type === 'unassigned' && isAssigned) return false;
    if (type === 'devtest' && !isDevTest) return false;

    if (!query) return true;

    const haystack = [
      wallet.walletKey,
      wallet.displayName,
      wallet.mainBtcAddress,
      wallet.utxoFundingAddress,
      wallet.rgbAccountRef,
      wallet.effectiveAccountRef,
    ]
      .filter(Boolean)
      .join(' ')
      .toLowerCase();

    return haystack.includes(query);
  });
}

function renderWalletAssignments() {
  if (!walletAssignmentBody) return;

  const visibleWallets = getFilteredWalletAssignments();

  if (!visibleWallets.length) {
    walletAssignmentBody.innerHTML = '<tr class="empty-row"><td colspan="5">No regtest wallets found yet.</td></tr>';
    return;
  }

  walletAssignmentBody.innerHTML = visibleWallets
    .map((wallet) => {
      const pendingAccountRef = pendingWalletAssignments.get(wallet.walletKey) || wallet.effectiveAccountRef;
      const isDirty = pendingAccountRef !== wallet.effectiveAccountRef;
      return `
      <tr data-wallet-key="${wallet.walletKey}">
        <td class="wallet-key-cell">
          <code title="${wallet.walletKey}">${wallet.walletKey}</code>
          ${wallet.isOwnerWallet ? '<div class="tiny-muted">Issuer owner wallet</div>' : ''}
        </td>
        <td class="wallet-address-cell">
          ${wallet.mainBtcAddress ? `<code title="${wallet.mainBtcAddress}">${shorten(wallet.mainBtcAddress, 8, 4)}</code>` : '<span class="tiny-muted">Not synced yet</span>'}
        </td>
        <td>
          <strong>${accountRefLabel(wallet.effectiveAccountRef)}</strong>
          <div class="tiny-muted">${isDirty ? `Pending change to ${accountRefLabel(pendingAccountRef)}` : (wallet.rgbAccountRef || 'No explicit assignment yet')}</div>
        </td>
        <td>${formatWalletAssignmentDate(wallet.lastSeenAt)}</td>
        <td>
          <div class="wallet-assignment-actions">
            <select class="wallet-assignment-select" data-wallet-key="${wallet.walletKey}" ${wallet.canAssign ? '' : 'disabled'}>
              ${renderWalletAssignmentSelectOptions(pendingAccountRef)}
            </select>
            <button class="page-btn action-btn wallet-assignment-apply ${isDirty ? 'wallet-assignment-apply-dirty' : ''}" type="button" data-wallet-key="${wallet.walletKey}" ${wallet.canAssign ? '' : 'disabled'}>
              Assign
            </button>
          </div>
        </td>
      </tr>
    `;
    })
    .join('');
}

async function loadWalletAssignments({ silent = false } = {}) {
  if (!adminSessionToken) {
    walletAssignments = [];
    registeredRgbNodes = [];
    pendingWalletAssignments = new Map();
    renderWalletAssignments();
    if (!silent) {
      setWalletAssignmentStatus('Admin authentication required.');
    }
    return;
  }

  if (!silent) {
    setWalletAssignmentStatus('Loading wallet assignments...');
  }

  try {
    const response = await fetch('/api/rgb/wallet-assignments', {
      headers: buildAdminHeaders(),
    });
    const payload = await response.json();
    if (!response.ok || !payload.ok) {
      if (response.status === 403) {
        persistAdminSessionToken(null);
        updateAdminUi();
      }
      throw new Error(payload.error || 'Failed to load wallet assignments.');
    }

    walletAssignments = Array.isArray(payload.wallets) ? payload.wallets : [];
    registeredRgbNodes = Array.isArray(payload.nodes) ? payload.nodes : [];
    pendingWalletAssignments = new Map();
    renderWalletAssignments();
    const visibleCount = getFilteredWalletAssignments().length;
    setWalletAssignmentStatus(`Loaded ${walletAssignments.length} wallet assignment${walletAssignments.length === 1 ? '' : 's'}${visibleCount !== walletAssignments.length ? ` • showing ${visibleCount}` : ''}.`);
  } catch (error) {
    walletAssignments = [];
    registeredRgbNodes = [];
    pendingWalletAssignments = new Map();
    renderWalletAssignments();
    setWalletAssignmentStatus(error.message || 'Failed to load wallet assignments.');
  }
}

async function assignWalletToNode(walletKey, accountRef) {
  if (!adminSessionToken) {
    throw new Error('Admin authentication required.');
  }

  const forceAssign = Boolean(walletAssignmentForceToggle?.checked);
  setWalletAssignmentStatus(`${forceAssign ? 'Force assigning' : 'Assigning'} ${walletKey} to ${accountRefLabel(accountRef)}...`);

  const response = await fetch('/api/rgb/wallet-assign', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      ...buildAdminHeaders(),
    },
    body: JSON.stringify({ walletKey, accountRef, forceAssign }),
  });
  const payload = await response.json();
  if (!response.ok || !payload.ok) {
    if (response.status === 403) {
      persistAdminSessionToken(null);
      updateAdminUi();
    }
    if (response.status === 409) {
      throw new Error(`${payload.error || 'Wallet reassignment blocked.'} Tick Force Assign to override if you accept the loss risk.`);
    }
    throw new Error(payload.error || 'Failed to assign wallet.');
  }

  const updatedWallet = payload.wallet || null;
  if (updatedWallet) {
    walletAssignments = walletAssignments.map((wallet) =>
      wallet.walletKey === updatedWallet.walletKey ? updatedWallet : wallet
    );
  }

  renderWalletAssignments();
  setWalletAssignmentStatus(payload.message || `${walletKey} assigned to ${accountRefLabel(accountRef)}.`);
  await loadDashboard();
}

async function unlockAdminControls() {
  const photon = getPhotonProvider();
  if (!photon) {
    throw new Error('Photon Wallet extension is not installed in this browser.');
  }

  const address = getConnectedWalletAddress();
  if (!isAdminWalletAddress(address)) {
    throw new Error('Connect the configured admin wallet first.');
  }

  setAdminAuthStatus('Requesting admin challenge...');
  const challengeResponse = await fetch(`/api/admin/auth/challenge?address=${encodeURIComponent(address)}`);
  const challengePayload = await challengeResponse.json();
  if (!challengeResponse.ok || !challengePayload.ok) {
    throw new Error(challengePayload.error || 'Failed to request admin challenge.');
  }

  setAdminAuthStatus('Sign the admin challenge in Photon Wallet...');
  const signResult = typeof photon._sendRequest === 'function'
    ? await photon._sendRequest('signMessage', { message: challengePayload.message })
    : { signature: await photon.signMessage(challengePayload.message), address };
  const signedAddress = typeof signResult?.address === 'string' ? signResult.address.trim() : address;
  const signature = typeof signResult?.signature === 'string' ? signResult.signature.trim() : '';

  if (!signature) {
    throw new Error('Photon Wallet did not return an admin signature.');
  }

  if (!isAdminWalletAddress(signedAddress)) {
    throw new Error(`Photon Wallet signed with ${signedAddress || 'an unknown address'}, not the configured admin wallet.`);
  }

  setAdminAuthStatus('Verifying admin signature...');
  const verifyResponse = await fetch('/api/admin/auth/verify', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      challengeId: challengePayload.challengeId,
      address: signedAddress,
      signature,
    }),
  });
  const verifyPayload = await verifyResponse.json();
  if (!verifyResponse.ok || !verifyPayload.ok) {
    throw new Error(verifyPayload.error || 'Failed to verify admin signature.');
  }

  persistAdminSessionToken(verifyPayload.token);
  updateAdminUi();
  await loadWalletAssignments();
}

async function lockAdminControls() {
  if (!adminSessionToken) {
    updateAdminUi();
    return;
  }

  try {
    await fetch('/api/admin/auth/logout', {
      method: 'POST',
      headers: buildAdminHeaders(),
    });
  } catch {
    // ignore logout failures
  }

  persistAdminSessionToken(null);
  walletAssignments = [];
  renderWalletAssignments();
  updateAdminUi();
  setWalletAssignmentStatus('Admin controls locked.');
}

function getPhotonProvider() {
  if (typeof window === 'undefined') return null;
  if (!window.photonbolt || !window.photonbolt.isPhotonBolt) return null;
  return window.photonbolt;
}

function setWalletMenuOpen(nextOpen) {
  walletMenuOpen = Boolean(nextOpen);
  if (walletMenuPanel) {
    walletMenuPanel.classList.toggle('hidden', !walletMenuOpen);
  }
  if (walletMenuTrigger) {
    walletMenuTrigger.setAttribute('aria-expanded', String(walletMenuOpen));
  }
}

function setWalletStatus(message) {
  if (walletMenuStatus) {
    walletMenuStatus.textContent = message;
  }
}

function setWalletValues({ address = '---', network = 'Not connected', btc = '--', pho = '--', light = '--' }) {
  if (walletMenuAddress) walletMenuAddress.textContent = address;
  if (walletMenuNetwork) walletMenuNetwork.textContent = network;
  if (walletBtcBalance) walletBtcBalance.textContent = btc;
  if (walletPhoBalance) walletPhoBalance.textContent = pho;
  if (walletLightBalance) walletLightBalance.textContent = light;
}

async function copyWalletAddress() {
  const address = walletMenuAddress?.textContent?.trim();
  if (!address || address === '---') return;

  try {
    await navigator.clipboard.writeText(address);
    if (walletAddressCopyButton) {
      walletAddressCopyButton.textContent = '✓';
      window.setTimeout(() => {
        walletAddressCopyButton.textContent = '⎘';
      }, 1200);
    }
  } catch {
    setWalletStatus('Unable to copy BTC address.');
  }
}

function updateWalletTrigger(address = '') {
  if (!walletMenuTrigger) return;
  walletMenuTrigger.textContent = address ? shortenWalletAddress(address) : 'Connect Wallet';
}

function setWalletButtonState(isConnected) {
  if (walletConnectButton) {
    walletConnectButton.classList.toggle('wallet-hidden', Boolean(isConnected));
    walletConnectButton.disabled = false;
    walletConnectButton.textContent = 'Connect';
  }
  if (walletDisconnectButton) {
    walletDisconnectButton.classList.toggle('wallet-hidden', !isConnected);
    walletDisconnectButton.disabled = !isConnected;
    walletDisconnectButton.textContent = 'Disconnect';
  }
}

function scheduleWalletMenuRefresh() {
  if (walletMenuRefreshTimer) {
    window.clearTimeout(walletMenuRefreshTimer);
  }

  walletMenuRefreshTimer = window.setTimeout(() => {
    refreshWalletMenu();
  }, 80);
}

async function tryCall(fn, ...args) {
  try {
    return await fn(...args);
  } catch {
    return null;
  }
}

function matchAssetEntry(entry, candidates) {
  const values = [
    entry?.id,
    entry?.ticker,
    entry?.unit,
    entry?.name,
    entry?.assetId,
    entry?.contractId,
  ].filter(Boolean).map((value) => String(value).toLowerCase());

  return candidates.some((candidate) => values.includes(String(candidate).toLowerCase()));
}

async function readAssetBalanceFromPhoton(photon, candidates) {
  if (!photon) return null;

  if (typeof photon.getAssetBalance === 'function') {
    for (const candidate of candidates) {
      const direct = await tryCall(() => photon.getAssetBalance({ assetId: candidate }));
      if (direct != null) return String(direct);
    }
  }

  if (typeof photon.getAssets === 'function') {
    const assets = await tryCall(() => photon.getAssets());
    if (Array.isArray(assets)) {
      const matchedAsset = assets.find((entry) => matchAssetEntry(entry, candidates));
      if (matchedAsset && matchedAsset.amount != null) {
        const unit = matchedAsset.unit ? ` ${matchedAsset.unit}` : '';
        return `${matchedAsset.amount}${unit}`;
      }
    }
  }

  if (typeof photon.getWalletState === 'function') {
    const state = await tryCall(() => photon.getWalletState());
    const assets = Array.isArray(state?.assets) ? state.assets : [];
    const matchedAsset = assets.find((entry) => matchAssetEntry(entry, candidates));
    if (matchedAsset && matchedAsset.amount != null) {
      const unit = matchedAsset.unit ? ` ${matchedAsset.unit}` : '';
      return `${matchedAsset.amount}${unit}`;
    }
  }

  if (typeof photon.getBalances === 'function') {
    const balances = await tryCall(() => photon.getBalances());
    if (balances && typeof balances === 'object') {
      const entry = Object.entries(balances).find(([key]) =>
        candidates.some((candidate) => key.toLowerCase() === String(candidate).toLowerCase())
      );
      if (entry) {
        return String(entry[1]);
      }
    }
  }
  return null;
}

async function refreshWalletMenu({ announce = false } = {}) {
  const photon = getPhotonProvider();

  if (!photon) {
    persistAdminSessionToken(null);
    updateWalletTrigger('');
    setWalletValues({});
    syncPlmOwnerWallet('');
    setWalletStatus('Photon Wallet extension is not installed in this browser.');
    if (walletMenuTitle) walletMenuTitle.textContent = 'Photon Wallet';
    setWalletButtonState(false);
    updateAdminUi();
    updatePlmWalletRequirementUi();
    return;
  }

  try {
    const accounts = await photon.getAccounts();
    const address = Array.isArray(accounts) ? (accounts[0] || '') : '';

    if (!address) {
      persistAdminSessionToken(null);
      updateWalletTrigger('');
      setWalletValues({});
      syncPlmOwnerWallet('');
      setWalletStatus('Connect Photon Wallet to view balances.');
      if (walletMenuTitle) walletMenuTitle.textContent = 'Photon Wallet';
      setWalletButtonState(false);
      updateAdminUi();
      updatePlmWalletRequirementUi();
      return;
    }

    const [network, btcBalance, phoBalance, lightBalance] = await Promise.all([
      photon.getNetwork(),
      photon.getBalance(),
      readAssetBalanceFromPhoton(photon, PHO_ASSET_CANDIDATES),
      readAssetBalanceFromPhoton(photon, LIGHT_ASSET_CANDIDATES),
    ]);

    updateWalletTrigger(address);
    setWalletValues({
      address,
      network: network || 'Unknown network',
      btc: btcBalance != null ? `${btcBalance} BTC` : '--',
      pho: phoBalance || 'Unavailable',
      light: lightBalance || 'Unavailable',
    });
    syncPlmOwnerWallet(address);
    setWalletStatus(
      phoBalance || lightBalance
        ? 'Connected through Photon Wallet.'
        : 'Connected. BTC is available through the public wallet API. PHO/LIGHT depend on optional asset-balance hooks.'
    );
    if (walletMenuTitle) walletMenuTitle.textContent = 'Photon Wallet Connected';
    setWalletButtonState(true);
    if (!isAdminWalletAddress(address)) {
      persistAdminSessionToken(null);
    }
    updateAdminUi();
    updatePlmWalletRequirementUi();
    if (announce) {
      console.log('Photon wallet connected:', address, network);
    }
  } catch (error) {
    persistAdminSessionToken(null);
    updateWalletTrigger('');
    setWalletValues({});
    syncPlmOwnerWallet('');
    setWalletStatus(error.message || 'Failed to read Photon Wallet state.');
    setWalletButtonState(false);
    updateAdminUi();
    updatePlmWalletRequirementUi();
  }
}

async function connectWalletMenu() {
  const photon = getPhotonProvider();
  if (!photon) {
    setWalletStatus('Photon Wallet extension is not installed in this browser.');
    return;
  }

  if (walletConnectButton) {
    walletConnectButton.disabled = true;
    walletConnectButton.textContent = 'Connecting...';
  }
  setWalletStatus('Requesting connection from Photon Wallet. Approve the connection in the extension popup if prompted.');
  try {
    setWalletStatus('Waiting for Photon Wallet to return your account and network details...');
    await photon.connect();
    await refreshWalletMenu({ announce: true });
    setWalletStatus('Photon Wallet connected successfully. Balances and the owner wallet field are now synced.');
  } catch (error) {
    setWalletStatus(error.message || 'Connection failed.');
  } finally {
    if (walletConnectButton) {
      walletConnectButton.disabled = false;
      walletConnectButton.textContent = 'Connect';
    }
  }
}

async function disconnectWalletMenu() {
  const photon = getPhotonProvider();
  if (!photon) {
    setWalletStatus('Photon Wallet extension is not installed in this browser.');
    return;
  }

  setWalletStatus('Disconnecting...');
  if (walletDisconnectButton) {
    walletDisconnectButton.disabled = true;
    walletDisconnectButton.textContent = 'Disconnecting...';
  }
  try {
    await photon.disconnect();
    persistAdminSessionToken(null);
    updateWalletTrigger('');
    setWalletValues({});
    syncPlmOwnerWallet('');
    setWalletStatus('Disconnected from Photon Wallet.');
    if (walletMenuTitle) walletMenuTitle.textContent = 'Photon Wallet';
    setWalletButtonState(false);
    updateAdminUi();
    updatePlmWalletRequirementUi();
  } catch (error) {
    setWalletStatus(error.message || 'Disconnect failed.');
    if (walletDisconnectButton) {
      walletDisconnectButton.disabled = false;
      walletDisconnectButton.textContent = 'Disconnect';
    }
  }
}

function setPlmMessage(message) {
  if (plmApplicationStatus) {
    plmApplicationStatus.textContent = message;
  }
}

function syncPlmOwnerWallet(address = '') {
  if (!plmOwnerWallet) return;
  plmOwnerWallet.value = address || '';
  plmOwnerWallet.placeholder = address
    ? 'Loaded from connected Photon Wallet'
    : 'Connect Photon Wallet to autofill';
}

function updatePlmWalletRequirementUi() {
  const hasConnectedWallet = Boolean(currentWalletAddress());
  if (plmApplyButton) {
    plmApplyButton.disabled = !hasConnectedWallet;
  }
  if (!hasConnectedWallet && !plmState) {
    setPlmMessage('Connect Photon Wallet first. Owner wallet address is always taken from the connected wallet.');
  }
}

function safeJsonParse(value) {
  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
}

function isLegacyPlmDraft(state) {
  if (!state || typeof state !== 'object') return false;
  const depositAddress = typeof state.btcDepositAddress === 'string' ? state.btcDepositAddress : '';
  return depositAddress.includes('plm') && depositAddress.endsWith('funding');
}

function readPlmState() {
  if (typeof window === 'undefined' || !window.localStorage) return null;
  const state = safeJsonParse(window.localStorage.getItem(PLM_STATE_KEY));
  if (isLegacyPlmDraft(state)) {
    window.localStorage.removeItem(PLM_STATE_KEY);
    return null;
  }
  return state;
}

function persistPlmState(nextState) {
  plmState = nextState;
  if (typeof window !== 'undefined' && window.localStorage) {
    if (nextState) {
      window.localStorage.setItem(PLM_STATE_KEY, JSON.stringify(nextState));
    } else {
      window.localStorage.removeItem(PLM_STATE_KEY);
    }
  }
}

function currentWalletAddress() {
  const address = walletMenuAddress?.textContent?.trim() || '';
  return address && address !== '---' ? address : '';
}

function plmStepKeyForStatus(status) {
  if (status === 'channel_active') return 'open';
  if (status === 'rgb_funded') return 'open';
  if (status === 'btc_funded') return 'rgb';
  if (status === 'pending_funding') return 'btc';
  return 'apply';
}

function updatePlmStepper(status) {
  if (!plmStepper) return;
  const activeStep = plmStepKeyForStatus(status);
  const order = ['apply', 'btc', 'rgb', 'open'];
  const activeIndex = order.indexOf(activeStep);

  plmStepper.querySelectorAll('.plm-step').forEach((element) => {
    const step = element.getAttribute('data-step');
    const stepIndex = order.indexOf(step);
    element.classList.toggle('is-active', step === activeStep);
    element.classList.toggle('is-complete', stepIndex > -1 && stepIndex < activeIndex);
  });
}

function renderPlmState() {
  const state = plmState;
  const status = state?.status || 'draft';
  const fundedCount = (state?.btcFunded ? 1 : 0) + (state?.rgbFunded ? 1 : 0);
  const txThreshold = Number(state?.txThreshold || plmTxThreshold?.value || 100);
  const txCounter = Number(state?.txCounter || 0);
  const progressPercent = Math.max(0, Math.min(100, (txCounter / Math.max(txThreshold, 1)) * 100));

  if (plmStatusLabel) plmStatusLabel.textContent = status.replaceAll('_', ' ');
  if (plmStatusDetail) {
    plmStatusDetail.textContent = state
      ? `Application ${state.id} is staged in dev mode. Backend endpoints are the next implementation step.`
      : 'Create a funding application to generate deposit targets.';
  }
  if (plmFundingProgress) plmFundingProgress.textContent = `${fundedCount} / 2 complete`;
  if (plmFundingDetail) {
    plmFundingDetail.textContent = state
      ? `BTC ${state.btcFunded ? 'funded' : 'pending'} • RGB ${state.rgbFunded ? 'funded' : 'pending'}`
      : 'BTC and RGB deposits both need to settle before the channel opens.';
  }
  if (plmEarnings) {
    plmEarnings.textContent = `${formatNumber(Number(state?.earnedFeesSats || 0))} sats`;
  }
  if (plmBtcAddress) {
    plmBtcAddress.textContent = state?.btcDepositAddress || 'Not generated';
  }
  if (plmBtcSummary) {
    plmBtcSummary.textContent = state
      ? `${formatNumber(state.btcAmountSats)} sats required from the owner wallet.`
      : 'Application required before BTC funding can start.';
  }
  if (plmRgbInvoice) {
    plmRgbInvoice.textContent = state?.rgbInvoice || 'Not generated';
  }
  if (plmRgbSummary) {
    plmRgbSummary.textContent = state
      ? `${formatNumber(state.rgbAssetAmount)} units of ${state.rgbAssetId} required for liquidity.`
      : 'Application required before RGB liquidity can start.';
  }
  if (plmProgressCount) {
    plmProgressCount.textContent = `${formatNumber(txCounter)} / ${formatNumber(txThreshold)}`;
  }
  if (plmProgressFill) {
    plmProgressFill.style.width = `${progressPercent}%`;
  }
  if (plmPayBtcButton) {
    plmPayBtcButton.disabled = !state || state.btcFunded || status === 'channel_active';
  }
  if (plmPayRgbButton) {
    plmPayRgbButton.disabled = !state || !state.btcFunded || state.rgbFunded || status === 'channel_active';
  }
  updatePlmStepper(status);
}

function useSelectedAssetForPlm() {
  const selected = getSelectedDashboardChannel();
  if (!selected || !plmAssetId) {
    setPlmMessage('Select a channel first or use Asset List to fill the PLM asset.');
    return;
  }
  plmAssetId.value = selected.assetId || '';
  autoFillPlmPeerPubkey(false);
  setPlmMessage(`PLM asset prefilled from selected channel ${shorten(selected.channelId, 12, 8)}.`);
}

function useSelectedChannelForPlm() {
  const selected = getSelectedDashboardChannel();
  if (!selected) {
    setPlmMessage('Select a channel first to prefill the PLM form.');
    return;
  }
  const selectedAssetId = selected.assetId || selected.assetTicker || selected.assetName || '';

  const preferredNode = selected.nodes.reduce((best, node) => {
    if (!best) return node;
    return Number(node.assetLocalAmount || 0) > Number(best.assetLocalAmount || 0) ? node : best;
  }, null);

  if (plmAccountRef) {
    plmAccountRef.value = preferredNode?.accountRef || selected.nodes[0]?.accountRef || 'photon-rln-user';
  }
  if (plmAssetId) {
    plmAssetId.value = selectedAssetId;
  }
  if (plmBtcAmount) {
    plmBtcAmount.value = String(selected.capacitySat || 32000);
  }
  if (plmRgbAmount) {
    plmRgbAmount.value = String(Math.max(selected.maxLocalAssetAmount || 0, 1));
  }
  autoFillPlmPeerPubkey(true);
  setPlmMessage(`PLM form prefilled from selected channel ${shorten(selected.channelId, 12, 8)}.`);
}

function buildPlmApplicationPayload() {
  return {
    ownerWalletAddress: currentWalletAddress(),
    accountRef: plmAccountRef?.value || 'photon-rln-user',
    peerPubkey: String(plmPeerPubkey?.value || '').trim(),
    rgbAssetId: String(plmAssetId?.value || '').trim(),
    btcAmountSats: Number(plmBtcAmount?.value || 0),
    rgbAssetAmount: Number(plmRgbAmount?.value || 0),
    commissionRateSats: Number(plmCommissionRate?.value || 0),
    txThreshold: Number(plmTxThreshold?.value || 100),
  };
}

async function fetchPlmApplicationStatus(id) {
  const response = await fetch(`/api/channel/application?id=${encodeURIComponent(id)}`);
  const payload = await response.json();
  if (!response.ok || !payload.ok) {
    throw new Error(payload.error || 'Unable to load channel application status.');
  }
  return payload.application;
}

async function submitPlmApplication(event) {
  event.preventDefault();
  if (!currentWalletAddress()) {
    setPlmMessage('Connect Photon Wallet first. Owner wallet address comes from the connected wallet.');
    walletMenuTrigger?.focus();
    return;
  }
  if (plmApplyButton) plmApplyButton.disabled = true;
  setPlmMessage('Creating PLM channel application...');

  try {
    const response = await fetch('/api/channel/apply', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(buildPlmApplicationPayload()),
    });
    const payload = await response.json();
    if (!response.ok || !payload.ok) {
      throw new Error(payload.error || 'Unable to create PLM application.');
    }

    const application = payload.application;
    persistPlmState(application);
    renderPlmState();
    setPlmMessage(`PLM application ${application.id} created. BTC funding can now be approved from the connected wallet.`);
  } catch (error) {
    setPlmMessage(error.message || 'Unable to create PLM application.');
  } finally {
    updatePlmWalletRequirementUi();
  }
}

function openChannelReadyState(state) {
  if (!state) return state;
  if (state.btcFunded && state.rgbFunded) {
    return {
      ...state,
      status: 'channel_active',
    };
  }
  if (state.rgbFunded) {
    return {
      ...state,
      status: 'rgb_funded',
    };
  }
  if (state.btcFunded) {
    return {
      ...state,
      status: 'btc_funded',
    };
  }
  return {
    ...state,
    status: 'pending_funding',
  };
}

async function payPlmBtcFunding() {
  const photon = getPhotonProvider();
  if (!plmState) {
    setPlmMessage('Create a PLM application first.');
    return;
  }
  if (isLegacyPlmDraft(plmState)) {
    persistPlmState(null);
    renderPlmState();
    setPlmMessage('This PLM draft was created with the old mock address flow. Create a new funding application first.');
    return;
  }
  if (!photon || typeof photon.sendBtcFunding !== 'function') {
    setPlmMessage('Photon Wallet does not expose sendBtcFunding() yet. Backend + wallet bridge implementation is still pending.');
    return;
  }

  setPlmMessage('Requesting BTC funding approval from Photon Wallet...');
  try {
    const result = await photon.sendBtcFunding({
      address: plmState.btcDepositAddress,
      amountSats: plmState.btcAmountSats,
    });
    persistPlmState({
      ...plmState,
      btcDepositTxid: result?.txId || null,
      lastFundingAttemptAt: new Date().toISOString(),
    });
    renderPlmState();
    setPlmMessage(`BTC funding broadcast${result?.txId ? `: ${shorten(result.txId, 12, 8)}` : ''}. Waiting for backend confirmation.`);
  } catch (error) {
    setPlmMessage(error.message || 'BTC funding approval failed.');
  }
}

async function payPlmRgbFunding() {
  const photon = getPhotonProvider();
  if (!plmState) {
    setPlmMessage('Create a PLM application first.');
    return;
  }
  if (!plmState.btcFunded) {
    setPlmMessage('BTC funding must be completed before RGB funding in this staged flow.');
    return;
  }
  if (!photon || typeof photon.payRgbInvoice !== 'function') {
    setPlmMessage('Photon Wallet does not expose payRgbInvoice() yet. RGB funding remains a planned bridge step.');
    return;
  }

  setPlmMessage('Requesting RGB funding approval from Photon Wallet...');
  try {
    await photon.payRgbInvoice({
      invoice: plmState.rgbInvoice,
    });
    const refreshed = plmState?.id ? await fetchPlmApplicationStatus(plmState.id) : null;
    persistPlmState({
      ...plmState,
      ...(refreshed || {}),
      fundedRgbAt: new Date().toISOString(),
    });
    renderPlmState();
    setPlmMessage(
      refreshed?.status === 'channel_active'
        ? 'RGB funding confirmed and channel auto-open request has been triggered.'
        : 'RGB funding approved. Waiting for backend reconciliation and auto-open.'
    );
  } catch (error) {
    setPlmMessage(error.message || 'RGB funding approval failed.');
  }
}

function resetPlmDraft() {
  persistPlmState(null);
  if (plmApplicationForm) {
    plmApplicationForm.reset();
  }
  if (plmTxThreshold) {
    plmTxThreshold.value = '100';
  }
  renderPlmState();
  setPlmMessage('PLM draft reset.');
}

function setOpenChannelCheckState({ btcLabel, btcDetail, assetLabel, assetDetail, openLabel, openDetail, canSubmit }) {
  if (checkBtcStatus) checkBtcStatus.textContent = btcLabel;
  if (checkBtcDetail) checkBtcDetail.textContent = btcDetail;
  if (checkAssetStatus) checkAssetStatus.textContent = assetLabel;
  if (checkAssetDetail) checkAssetDetail.textContent = assetDetail;
  if (checkOpenStatus) checkOpenStatus.textContent = openLabel;
  if (checkOpenDetail) checkOpenDetail.textContent = openDetail;
  if (openChannelButton) openChannelButton.disabled = !canSubmit;
  lastOpenChannelCheckPassed = Boolean(canSubmit);
}

function formatAssetLabel(channel) {
  const primary = channel.assetTicker || channel.assetName || 'Unknown asset';
  const secondary = channel.assetTicker && channel.assetName ? channel.assetName : channel.assetId;
  return secondary ? `${primary} • ${shorten(secondary, 18, 10)}` : primary;
}

function renderAssetSummaries(channels) {
  if (!assetSummaryBody) return;

  const byAsset = new Map();
  channels.forEach((channel) => {
    const key = channel.assetId || channel.channelId;
    if (!byAsset.has(key)) {
      byAsset.set(key, {
        assetId: channel.assetId || null,
        assetTicker: channel.assetTicker || null,
        assetName: channel.assetName || null,
        readyCount: 0,
        openingCount: 0,
        issuerLocalTotal: 0,
        userLocalTotal: 0,
        issuerMaxSingle: 0,
        userMaxSingle: 0,
      });
    }

    const row = byAsset.get(key);
    const isReady = Boolean(channel.ready && channel.isUsable && String(channel.status).toLowerCase() === 'opened');
    if (isReady) {
      row.readyCount += 1;
    } else {
      row.openingCount += 1;
    }

    channel.nodes.forEach((node) => {
      const amount = Number(node.assetLocalAmount || 0);
      if (node.accountRef === 'photon-rln-issuer') {
        if (isReady) {
          row.issuerLocalTotal += amount;
          row.issuerMaxSingle = Math.max(row.issuerMaxSingle, amount);
        }
      }
      if (node.accountRef === 'photon-rln-user' || node.accountRef === 'photon-rln-user-b') {
        if (isReady) {
          row.userLocalTotal += amount;
          row.userMaxSingle = Math.max(row.userMaxSingle, amount);
        }
      }
    });
  });

  const rows = [...byAsset.values()].sort((left, right) => {
    const leftName = `${left.assetTicker || ''}${left.assetName || ''}`;
    const rightName = `${right.assetTicker || ''}${right.assetName || ''}`;
    return leftName.localeCompare(rightName);
  });

  if (rows.length === 0) {
    assetSummaryBody.innerHTML = `
      <tr class="empty-row">
        <td colspan="7">No asset channels found.</td>
      </tr>
    `;
    return;
  }

  assetSummaryBody.innerHTML = rows
    .map((row) => `
      <tr>
        <td>
          <strong>${row.assetTicker || row.assetName || 'Unknown'}</strong>
          <div class="tiny-muted">${shorten(row.assetId || '-', 16, 10)}</div>
        </td>
        <td>${row.readyCount}</td>
        <td>${row.openingCount}</td>
        <td>${formatNumber(row.issuerLocalTotal)}</td>
        <td>${formatNumber(row.userLocalTotal)}</td>
        <td>${formatNumber(row.issuerMaxSingle)}</td>
        <td>${formatNumber(row.userMaxSingle)}</td>
      </tr>
    `)
    .join('');
}

function renderAssetPickerRows() {
  if (!assetPickerBody) return;

  if (filteredRegistryAssets.length === 0) {
    assetPickerBody.innerHTML = `
      <tr class="empty-row">
        <td colspan="6">No assets match the current filter.</td>
      </tr>
    `;
    return;
  }

  assetPickerBody.innerHTML = filteredRegistryAssets
    .map((asset, index) => `
      <tr>
        <td>${index + 1}</td>
        <td><strong>${asset.token_name || '-'}</strong></td>
        <td>${asset.ticker || '-'}</td>
        <td>${asset.total_supply || '-'}</td>
        <td><code title="${asset.contract_id}">${shorten(asset.contract_id, 14, 10)}</code></td>
        <td><button class="copy-btn select-asset-btn" data-contract-id="${asset.contract_id}" type="button">Use</button></td>
      </tr>
    `)
    .join('');

  assetPickerBody.querySelectorAll('.select-asset-btn').forEach((button) => {
    button.addEventListener('click', () => {
      const contractId = button.getAttribute('data-contract-id') || '';
      if (assetPickerTarget === 'plm' && plmAssetId) {
        plmAssetId.value = contractId;
        autoFillPlmPeerPubkey(false);
        setPlmMessage(`PLM asset selected from Asset List: ${shorten(contractId, 14, 10)}.`);
      } else if (openAssetId) {
        openAssetId.value = contractId;
      }
      closeAssetPicker();
      scheduleOpenChannelCheck();
    });
  });
}

function filterAssetPickerRows() {
  const query = String(assetPickerSearch?.value || '').trim().toLowerCase();
  filteredRegistryAssets = registryAssets.filter((asset) => {
    if (!query) return true;
    return [
      asset.token_name,
      asset.ticker,
      asset.contract_id,
    ].join(' ').toLowerCase().includes(query);
  });
  renderAssetPickerRows();
}

async function loadAssetRegistry() {
  if (!assetPickerStatus || !assetPickerBody) return;
  assetPickerStatus.textContent = 'Loading registry...';
  assetPickerBody.innerHTML = `
    <tr class="loading-row">
      <td colspan="6">Loading asset registry...</td>
    </tr>
  `;

  try {
    const response = await fetch('/api/rgb/registry');
    const payload = await response.json();
    if (!response.ok || !payload.ok) {
      throw new Error(payload.error || 'Failed to load asset registry.');
    }

    registryAssets = Array.isArray(payload.assets) ? payload.assets : [];
    filteredRegistryAssets = registryAssets;
    assetPickerStatus.textContent = `${registryAssets.length} asset(s) available. Click Use to fill the Asset ID field.`;
    renderAssetPickerRows();
  } catch (error) {
    assetPickerStatus.textContent = error.message;
    assetPickerBody.innerHTML = `
      <tr class="empty-row">
        <td colspan="6">${error.message}</td>
      </tr>
    `;
  }
}

function openAssetPicker(target = 'open') {
  if (!assetPickerModal) return;
  assetPickerTarget = target;
  assetPickerModal.classList.remove('hidden');
  if (assetPickerSearch) {
    assetPickerSearch.value = '';
  }
  loadAssetRegistry().then(() => {
    if (assetPickerSearch) {
      assetPickerSearch.focus();
    }
  });
}

function openPlmAssetPicker() {
  openAssetPicker('plm');
}

function closeAssetPicker() {
  if (!assetPickerModal) return;
  assetPickerModal.classList.add('hidden');
}

function updateSelectOptions(rows) {
  if (!channelSelect) return;
  const current = selectedChannelId;
  channelSelect.innerHTML = rows
    .map((channel) => `<option value="${channel.channelId}">${formatAssetLabel(channel)} • ${shorten(channel.channelId, 12, 8)}</option>`)
    .join('');

  if (rows.length === 0) {
    channelSelect.innerHTML = '<option value="">No channels</option>';
    selectedChannelId = '';
    return;
  }

  if (rows.some((row) => row.channelId === current)) {
    channelSelect.value = current;
    selectedChannelId = current;
    return;
  }

  selectedChannelId = rows[0].channelId;
  channelSelect.value = selectedChannelId;
}

function renderChannelRows(rows) {
  if (!channelsBody) return;

  if (rows.length === 0) {
    channelsBody.innerHTML = `
      <tr class="empty-row">
        <td colspan="11">No channels match the current filter.</td>
      </tr>
    `;
    return;
  }

  channelsBody.innerHTML = rows
    .map((channel, index) => {
      const peerValue = channel.nodes[0]?.peerPubkey || '-';
      const badges = `
        <div class="channel-badges">
          <span class="badge ${statusClass(channel.status)}">${channel.status}</span>
          <span class="badge ${channel.ready ? 'success' : 'pending'}">${channel.ready ? 'Ready' : 'Not Ready'}</span>
          <span class="badge ${channel.isUsable ? 'success' : 'failed'}">${channel.isUsable ? 'Usable' : 'Blocked'}</span>
        </div>
      `;
      return `
        <tr data-channel-id="${channel.channelId}">
          <td>${index + 1}</td>
          <td><strong>${channel.assetTicker || channel.assetName || 'Unknown'}</strong><div class="tiny-muted">${shorten(channel.assetId || '-', 16, 10)}</div></td>
          <td class="compact-id-cell"><code title="${channel.channelId}">${shorten(channel.channelId, 8, 6)}</code></td>
          <td class="compact-peer-cell"><code title="${peerValue}">${shorten(peerValue, 7, 5)}</code></td>
          <td>${formatNumber(channel.totalAssetLiquidity)}</td>
          <td>${formatNumber(channel.capacitySat, ' sats')}</td>
          <td>${formatNumber(channel.nextOutboundHtlcLimitMsat, ' msat')}</td>
          <td>${channel.totalPaymentCount}</td>
          <td>${badges}</td>
          <td><button class="copy-btn inspect-btn" data-channel-id="${channel.channelId}" type="button">Inspect</button></td>
          <td><button class="copy-btn close-row-btn" data-channel-id="${channel.channelId}" type="button">Close</button></td>
        </tr>
      `;
    })
    .join('');

  channelsBody.querySelectorAll('.inspect-btn').forEach((button) => {
    button.addEventListener('click', () => {
      selectedChannelId = button.getAttribute('data-channel-id') || '';
      if (channelSelect) {
        channelSelect.value = selectedChannelId;
      }
      renderSelectedChannel();
    });
  });

  channelsBody.querySelectorAll('.close-row-btn').forEach((button) => {
    button.addEventListener('click', async () => {
      const channelId = button.getAttribute('data-channel-id') || '';
      selectedChannelId = channelId;
      if (channelSelect) {
        channelSelect.value = selectedChannelId;
      }
      renderSelectedChannel();
      await closeSelectedChannel(false);
    });
  });
}

function renderSelectedChannel() {
  const selected = filteredChannels.find((channel) => channel.channelId === selectedChannelId)
    || dashboardData.find((channel) => channel.channelId === selectedChannelId);

  if (!selected) {
    detailTitle.textContent = 'Channel details';
    detailMeta.textContent = 'No channel selected.';
    detailFunding.textContent = '-';
    detailShortId.textContent = '-';
    detailAssetCapacity.textContent = '0';
    detailBtcCapacity.textContent = '0 sats';
    if (detailOwner) detailOwner.textContent = '-';
    metricAsset.textContent = '-';
    metricSendable.textContent = '0';
    metricPayments.textContent = '0';
    detailNodesBody.innerHTML = '<tr class="empty-row"><td colspan="8">Select a channel to inspect liquidity.</td></tr>';
    paymentsBody.innerHTML = '<tr class="empty-row"><td colspan="7">Select a channel to inspect transfer history.</td></tr>';
    paymentsMeta.textContent = '';
    if (detailCloseStatus) {
      detailCloseStatus.textContent = 'Select a channel to enable close actions.';
    }
    if (detailCloseButton) {
      detailCloseButton.disabled = true;
    }
    if (openChannelStatus) {
      openChannelStatus.textContent = 'Pick a channel or enter values manually to open a new parallel channel.';
    }
    setOpenChannelCheckState({
      btcLabel: 'Waiting',
      btcDetail: 'Enter channel values to verify BTC capacity.',
      assetLabel: 'Waiting',
      assetDetail: 'Enter channel values to verify asset balance.',
      openLabel: 'Waiting',
      openDetail: 'The form will block submit if the preflight check fails.',
      canSubmit: false,
    });
    return;
  }

  detailTitle.textContent = `${selected.assetTicker || selected.assetName || 'Asset'} channel`;
  detailMeta.textContent = `${selected.nodes.length} node view(s) • ${selected.status} • ${selected.ready ? 'ready' : 'not ready'}`;
  detailFunding.textContent = shorten(selected.fundingTxid || '-', 18, 14);
  detailFunding.title = selected.fundingTxid || '';
  detailShortId.textContent = String(selected.shortChannelId || '-');
  detailAssetCapacity.textContent = formatNumber(selected.totalAssetLiquidity);
  detailBtcCapacity.textContent = formatNumber(selected.capacitySat, ' sats');
  const preferredNode = selected.nodes.reduce((best, node) => {
    if (!best) return node;
    return Number(node.assetLocalAmount || 0) > Number(best.assetLocalAmount || 0) ? node : best;
  }, null);
  if (detailOwner) {
    detailOwner.textContent = accountRefLabel(preferredNode?.accountRef);
  }

  metricAsset.textContent = selected.assetTicker || selected.assetName || shorten(selected.assetId || '-', 8, 6);
  metricSendable.textContent = formatNumber(selected.maxLocalAssetAmount);
  metricPayments.textContent = String(selected.totalPaymentCount);
  if (detailCloseStatus) {
    detailCloseStatus.textContent = selected.ready
      ? 'Cooperative close is available. Use force close only if the peer is unresponsive.'
      : `Channel is ${selected.status}. Close may still be possible, but force close is riskier.`;
  }
  if (detailCloseButton) {
    detailCloseButton.disabled = false;
  }

  detailNodesBody.innerHTML = selected.nodes
    .map((node) => `
      <tr>
        <td><strong>${node.nodeLabel}</strong><div class="tiny-muted">${node.accountRef}</div></td>
        <td>${node.walletKeys.length ? node.walletKeys.map((walletKey) => `<div class="tiny-muted">${walletKey}</div>`).join('') : '<span class="tiny-muted">No bound wallets</span>'}</td>
        <td>${formatNumber(node.assetLocalAmount)}</td>
        <td>${formatNumber(node.assetRemoteAmount)}</td>
        <td>${formatNumber(node.outboundBalanceMsat, ' msat')}</td>
        <td>${formatNumber(node.inboundBalanceMsat, ' msat')}</td>
        <td>${formatNumber(node.nextOutboundHtlcLimitMsat, ' msat')}</td>
        <td>
          <div class="status-stack">
            <span class="badge ${node.ready ? 'success' : 'pending'}">${node.ready ? 'Ready' : 'Pending'}</span>
            <span class="badge ${node.isUsable ? 'success' : 'failed'}">${node.isUsable ? 'Usable' : 'Blocked'}</span>
          </div>
        </td>
      </tr>
    `)
    .join('');

  if (selected.payments.length === 0) {
    paymentsBody.innerHTML = '<tr class="empty-row"><td colspan="7">No matched payments for this channel yet.</td></tr>';
    paymentsMeta.textContent = 'No matched transfer history yet.';
    return;
  }

  paymentsMeta.textContent = `${selected.settledPaymentCount} settled • ${selected.pendingPaymentCount} pending • ${selected.failedPaymentCount} failed`;
  paymentsBody.innerHTML = selected.payments
    .map((payment, index) => `
      <tr>
        <td>${index + 1}</td>
        <td>${formatDateTime(payment.updatedAt || payment.createdAt)}</td>
        <td><code title="${payment.paymentHash}">${shorten(payment.paymentHash, 12, 10)}</code></td>
        <td>${payment.assetAmount == null ? '-' : formatNumber(payment.assetAmount)}</td>
        <td>${payment.amtMsat == null ? '-' : formatNumber(payment.amtMsat, ' msat')}</td>
        <td><span class="badge ${statusClass(payment.overallStatus)}">${payment.overallStatus}</span></td>
        <td>
          <div class="node-lines">
            ${payment.nodeStatuses.map((nodeStatus) => `
              <div class="node-line">
                <strong>${nodeStatus.nodeLabel}</strong>
                <span class="tiny-muted">${nodeStatus.direction}</span>
                <span class="badge ${statusClass(nodeStatus.status)}">${nodeStatus.status}</span>
              </div>
            `).join('')}
          </div>
        </td>
      </tr>
    `)
    .join('');

  autoFillPeerPubkey(false);
  autoFillPlmPeerPubkey(false);
}

async function closeSelectedChannel(force = false) {
  const selected = getSelectedDashboardChannel();
  if (!selected) {
    if (detailCloseStatus) {
      detailCloseStatus.textContent = 'Select a channel first.';
    }
    return;
  }

  const requestedAccountRef = openAccountRef?.value || 'photon-rln-user';
  const sourceNode = selected.nodes.find((node) => node.accountRef === requestedAccountRef) || selected.nodes[0];
  const confirmText = force
    ? `Force close channel ${shorten(selected.channelId, 12, 8)} for ${selected.assetTicker || selected.assetName || 'this asset'}?`
    : `Close channel ${shorten(selected.channelId, 12, 8)} for ${selected.assetTicker || selected.assetName || 'this asset'}?`;

  if (!window.confirm(confirmText)) {
    return;
  }

  if (detailCloseStatus) {
    detailCloseStatus.textContent = force ? 'Force closing channel...' : 'Closing channel...';
  }

  try {
    const response = await fetch('/api/rgb/close-channel', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        accountRef: sourceNode.accountRef,
        channelId: selected.channelId,
        peerPubkey: sourceNode.peerPubkey,
        force,
      }),
    });
    const payload = await response.json();
    if (!response.ok || !payload.ok) {
      throw new Error(payload.error || 'Close channel failed.');
    }

    if (detailCloseStatus) {
      detailCloseStatus.textContent = force
        ? 'Force close request accepted. Refreshing dashboard...'
        : 'Close request accepted. Refreshing dashboard...';
    }
    await loadDashboard();
  } catch (error) {
    if (detailCloseStatus) {
      detailCloseStatus.textContent = error.message;
    }
  }
}

function getSelectedDashboardChannel() {
  return filteredChannels.find((channel) => channel.channelId === selectedChannelId)
    || dashboardData.find((channel) => channel.channelId === selectedChannelId)
    || null;
}

function autoFillPeerPubkey(force = false) {
  if (!openPeerPubkey) return;

  const selected = getSelectedDashboardChannel();
  if (!selected) return;

  const currentValue = (openPeerPubkey.value || '').trim();
  if (!force && currentValue) return;

  const requestedAccountRef = openAccountRef?.value || '';
  const sourceNode = selected.nodes.find((node) => node.accountRef === requestedAccountRef);
  const fallbackNode = selected.nodes[0] || null;
  const peerPubkey = sourceNode?.peerPubkey || fallbackNode?.peerPubkey || '';

  if (peerPubkey) {
    openPeerPubkey.value = peerPubkey;
  }
}

function autoFillPlmPeerPubkey(force = false) {
  if (!plmPeerPubkey) return;

  const selected = getSelectedDashboardChannel();
  if (!selected) return;

  const currentValue = (plmPeerPubkey.value || '').trim();
  if (!force && currentValue) return;

  const requestedAccountRef = plmAccountRef?.value || '';
  const sourceNode = selected.nodes.find((node) => node.accountRef === requestedAccountRef);
  const fallbackNode = selected.nodes[0] || null;
  const peerPubkey = sourceNode?.peerPubkey || fallbackNode?.peerPubkey || '';

  if (peerPubkey) {
    plmPeerPubkey.value = peerPubkey;
  }
}

async function runOpenChannelCheck() {
  const accountRef = openAccountRef?.value || '';
  const assetId = openAssetId?.value?.trim() || '';
  const capacitySat = Number(openCapacitySat?.value || 0);
  const assetAmount = Number(openAssetAmount?.value || 0);

  if (!accountRef || !assetId || !Number.isFinite(capacitySat) || capacitySat <= 0 || !Number.isFinite(assetAmount) || assetAmount <= 0) {
    setOpenChannelCheckState({
      btcLabel: 'Waiting',
      btcDetail: 'Fill account, asset, BTC capacity, and asset amount.',
      assetLabel: 'Waiting',
      assetDetail: 'Fill account, asset, BTC capacity, and asset amount.',
      openLabel: 'Waiting',
      openDetail: 'Complete the required fields to run the balance check.',
      canSubmit: false,
    });
    return;
  }

  setOpenChannelCheckState({
    btcLabel: 'Checking',
    btcDetail: 'Reading node BTC balance...',
    assetLabel: 'Checking',
    assetDetail: 'Reading node asset balance...',
    openLabel: 'Checking',
    openDetail: 'Running channel-open preflight...',
    canSubmit: false,
  });

  try {
    const params = new URLSearchParams({
      accountRef,
      assetId,
      capacitySat: String(capacitySat),
      assetAmount: String(assetAmount),
    });
    const response = await fetch(`/api/rgb/open-channel-check?${params.toString()}`, {
      headers: buildAdminHeaders(),
    });
    const payload = await response.json();
    if (!response.ok || !payload.ok) {
      throw new Error(payload.error || 'Open channel check failed.');
    }

    const btcOk = Boolean(payload.checks?.btcSufficient);
    const assetOk = Boolean(payload.checks?.assetSufficient);
    const canOpen = Boolean(payload.checks?.canOpenChannel);
    const btcAvailable = formatNumber(payload.available?.vanillaSpendableSat || 0, ' sats');
    const btcRequired = formatNumber(payload.required?.btcWithReserveSat || 0, ' sats');
    const assetAvailable = formatNumber(payload.available?.assetSpendable || 0);
    const assetRequired = formatNumber(payload.required?.assetAmount || 0);

    setOpenChannelCheckState({
      btcLabel: btcOk ? 'Enough BTC' : 'Need More BTC',
      btcDetail: btcOk
        ? `Available ${btcAvailable}. Required about ${btcRequired}.`
        : `Available ${btcAvailable}. Missing ${formatNumber(payload.missing?.btcSat || 0, ' sats')}.`,
      assetLabel: assetOk ? 'Enough Asset' : 'Need More Asset',
      assetDetail: assetOk
        ? `Available ${assetAvailable}. Required ${assetRequired}.`
        : `Available ${assetAvailable}. Missing ${formatNumber(payload.missing?.assetAmount || 0)}.`,
      openLabel: canOpen ? 'Ready to Open' : 'Blocked',
      openDetail: payload.note || 'Preflight complete.',
      canSubmit: canOpen,
    });
  } catch (error) {
    setOpenChannelCheckState({
      btcLabel: 'Check Failed',
      btcDetail: error.message,
      assetLabel: 'Check Failed',
      assetDetail: error.message,
      openLabel: 'Blocked',
      openDetail: error.message,
      canSubmit: false,
    });
  }
}

function scheduleOpenChannelCheck() {
  if (openChannelCheckTimer) {
    window.clearTimeout(openChannelCheckTimer);
  }
  openChannelCheckTimer = window.setTimeout(runOpenChannelCheck, 250);
}

function prefillOpenChannelForm() {
  const selected = getSelectedDashboardChannel();

  if (!selected) {
    if (openChannelStatus) {
      openChannelStatus.textContent = 'Select a channel first to prefill the open-channel form.';
    }
    return;
  }

  const preferredNode = selected.nodes.reduce((best, node) => {
    if (!best) return node;
    return Number(node.assetLocalAmount || 0) > Number(best.assetLocalAmount || 0) ? node : best;
  }, null);
  const peerNode = selected.nodes.find((node) => node.accountRef !== preferredNode?.accountRef) || selected.nodes[0];

  if (openAccountRef) openAccountRef.value = preferredNode?.accountRef || 'photon-rln-user';
  if (openAssetId) openAssetId.value = selected.assetId || '';
  if (openCapacitySat) openCapacitySat.value = String(selected.capacitySat || 32000);
  if (openAssetAmount) openAssetAmount.value = String(Math.max(selected.maxLocalAssetAmount || 0, 1));
  if (openPushMsat) openPushMsat.value = '0';
  if (openFeeBaseMsat) openFeeBaseMsat.value = '0';
  if (openFeePpm) openFeePpm.value = '0';
  if (openWithAnchors) openWithAnchors.checked = true;
  if (openPublic) openPublic.checked = false;
  autoFillPeerPubkey(true);

  if (openChannelStatus) {
    openChannelStatus.textContent = `Form prefilled from ${selected.assetTicker || selected.assetName || 'selected asset'} channel ${shorten(selected.channelId, 12, 8)}. Opening a new parallel channel is how you add effective liquidity here.`;
  }
  scheduleOpenChannelCheck();
}

async function submitOpenChannel(event) {
  event.preventDefault();
  if (!openChannelStatus) return;

  if (!adminSessionToken) {
    openChannelStatus.textContent = 'Admin authentication is required to open a parallel asset channel.';
    setActiveDashboardTab('admin');
    return;
  }

  if (!lastOpenChannelCheckPassed) {
    openChannelStatus.textContent = 'Balance check failed or has not completed yet. Fix the form inputs before opening the channel.';
    return;
  }

  openChannelStatus.textContent = 'Opening channel...';

  try {
    const response = await fetch('/api/rgb/open-channel', {
      method: 'POST',
      headers: buildAdminHeaders({ 'Content-Type': 'application/json' }),
      body: JSON.stringify({
        accountRef: openAccountRef?.value,
        peerPubkey: openPeerPubkey?.value,
        assetId: openAssetId?.value,
        capacitySat: Number(openCapacitySat?.value || 0),
        assetAmount: Number(openAssetAmount?.value || 0),
        pushMsat: Number(openPushMsat?.value || 0),
        feeBaseMsat: Number(openFeeBaseMsat?.value || 0),
        feeProportionalMillionths: Number(openFeePpm?.value || 0),
        withAnchors: Boolean(openWithAnchors?.checked),
        public: Boolean(openPublic?.checked),
      }),
    });

    const payload = await response.json();
    if (!response.ok || !payload.ok) {
      throw new Error(payload.error || 'Open channel failed.');
    }

    openChannelStatus.textContent = `Channel open request accepted on ${payload.accountRef}. Refreshing dashboard...`;
    await loadDashboard();
    scheduleOpenChannelCheck();
  } catch (error) {
    openChannelStatus.textContent = error.message;
  }
}

function applyFilter() {
  const query = String(channelSearch?.value || '').trim().toLowerCase();
  filteredChannels = dashboardData.filter((channel) => {
    if (!query) return true;
    return [
      channel.channelId,
      channel.assetId,
      channel.assetName,
      channel.assetTicker,
      channel.nodes.map((node) => node.peerPubkey).join(' '),
      channel.nodes.map((node) => node.walletKeys.join(' ')).join(' '),
    ].join(' ').toLowerCase().includes(query);
  });

  renderChannelRows(filteredChannels);
  updateSelectOptions(filteredChannels);
  channelTableStatus.textContent = `${filteredChannels.length} channel(s) shown`;
  renderSelectedChannel();
}

async function loadDashboard() {
  dashboardStatus.textContent = 'Loading live channel data...';
  try {
    const response = await fetch('/api/rgb/channel-dashboard');
    const payload = await response.json();
    if (!response.ok || !payload.ok) {
      throw new Error(payload.error || 'Failed to load channel dashboard.');
    }

    dashboardData = Array.isArray(payload.channels) ? payload.channels : [];
    metricChannels.textContent = String(dashboardData.length);
    dashboardSubtitle.textContent = `Last refreshed ${formatDateTime(payload.refreshedAt)} • ${payload.nodeSources.length} node source(s)`;
    dashboardStatus.textContent = `Live snapshot loaded from ${payload.nodeSources.map((source) => source.label).join(' and ')}.`;
    renderAssetSummaries(dashboardData);
    applyFilter();
    void loadWalletAssignments({ silent: true });
  } catch (error) {
    dashboardStatus.textContent = error.message;
    channelsBody.innerHTML = `<tr class="empty-row"><td colspan="10">${error.message}</td></tr>`;
    if (assetSummaryBody) {
      assetSummaryBody.innerHTML = `<tr class="empty-row"><td colspan="7">${error.message}</td></tr>`;
    }
  }
}

if (channelSelect) {
  channelSelect.addEventListener('change', () => {
    selectedChannelId = channelSelect.value;
    renderSelectedChannel();
  });
}

if (channelSearch) {
  channelSearch.addEventListener('input', applyFilter);
}

if (refreshButton) {
  refreshButton.addEventListener('click', loadDashboard);
}

if (walletAssignmentRefreshButton) {
  walletAssignmentRefreshButton.addEventListener('click', async () => {
    await loadWalletAssignments();
  });
}

if (walletAssignmentFilterInput) {
  walletAssignmentFilterInput.addEventListener('input', () => {
    renderWalletAssignments();
    const visibleCount = getFilteredWalletAssignments().length;
    setWalletAssignmentStatus(`Loaded ${walletAssignments.length} wallet assignment${walletAssignments.length === 1 ? '' : 's'}${visibleCount !== walletAssignments.length ? ` • showing ${visibleCount}` : ''}.`);
  });
}

if (walletAssignmentFilterType) {
  walletAssignmentFilterType.addEventListener('change', () => {
    renderWalletAssignments();
    const visibleCount = getFilteredWalletAssignments().length;
    setWalletAssignmentStatus(`Loaded ${walletAssignments.length} wallet assignment${walletAssignments.length === 1 ? '' : 's'}${visibleCount !== walletAssignments.length ? ` • showing ${visibleCount}` : ''}.`);
  });
}

if (prefillSelectedButton) {
  prefillSelectedButton.addEventListener('click', prefillOpenChannelForm);
}

if (openChannelForm) {
  openChannelForm.addEventListener('submit', submitOpenChannel);
}

[openAccountRef, openPeerPubkey, openAssetId, openCapacitySat, openAssetAmount].forEach((element) => {
  if (element) {
    element.addEventListener('input', scheduleOpenChannelCheck);
    element.addEventListener('change', scheduleOpenChannelCheck);
  }
});

if (openAccountRef) {
  openAccountRef.addEventListener('change', () => {
    autoFillPeerPubkey(true);
  });
}

if (plmAccountRef) {
  plmAccountRef.addEventListener('change', () => {
    autoFillPlmPeerPubkey(true);
  });
}

if (assetPickerButton) {
  assetPickerButton.addEventListener('click', openAssetPicker);
}

if (plmAssetPickerButton) {
  plmAssetPickerButton.addEventListener('click', openPlmAssetPicker);
}

if (assetPickerClose) {
  assetPickerClose.addEventListener('click', closeAssetPicker);
}

if (assetPickerSearch) {
  assetPickerSearch.addEventListener('input', filterAssetPickerRows);
}

if (assetPickerModal) {
  assetPickerModal.addEventListener('click', (event) => {
    if (event.target === assetPickerModal) {
      closeAssetPicker();
    }
  });
}

if (detailCloseButton) {
  detailCloseButton.addEventListener('click', async () => {
    await closeSelectedChannel(Boolean(detailCloseForce?.checked));
  });
}

if (walletMenuTrigger) {
  walletMenuTrigger.addEventListener('click', (event) => {
    event.stopPropagation();
    const nextOpen = !walletMenuOpen;
    setWalletMenuOpen(nextOpen);
    if (nextOpen) {
      scheduleWalletMenuRefresh();
    }
  });

  walletMenuTrigger.addEventListener('focus', () => {
    scheduleWalletMenuRefresh();
  });
}

if (walletMenuPanel) {
  walletMenuPanel.addEventListener('click', (event) => {
    event.stopPropagation();
  });

  walletMenuPanel.addEventListener('focusin', () => {
    scheduleWalletMenuRefresh();
  });
}

if (walletConnectButton) {
  walletConnectButton.addEventListener('click', async () => {
    await connectWalletMenu();
  });
}

if (walletDisconnectButton) {
  walletDisconnectButton.addEventListener('click', async () => {
    await disconnectWalletMenu();
  });
}

if (adminAuthButton) {
  adminAuthButton.addEventListener('click', async () => {
    try {
      await unlockAdminControls();
    } catch (error) {
      setAdminAuthStatus(error.message || 'Unable to unlock admin controls.');
    }
  });
}

if (adminLogoutButton) {
  adminLogoutButton.addEventListener('click', async () => {
    await lockAdminControls();
  });
}

if (plmApplicationForm) {
  plmApplicationForm.addEventListener('submit', submitPlmApplication);
}

if (plmUseSelectedButton) {
  plmUseSelectedButton.addEventListener('click', () => {
    useSelectedChannelForPlm();
  });
}

if (plmPayBtcButton) {
  plmPayBtcButton.addEventListener('click', async () => {
    await payPlmBtcFunding();
  });
}

if (plmPayRgbButton) {
  plmPayRgbButton.addEventListener('click', async () => {
    await payPlmRgbFunding();
  });
}

if (plmResetButton) {
  plmResetButton.addEventListener('click', () => {
    resetPlmDraft();
  });
}

if (plmRefreshButton) {
  plmRefreshButton.addEventListener('click', async () => {
    try {
      const localState = readPlmState();
      if (!localState?.id) {
        plmState = localState;
        renderPlmState();
        setPlmMessage('No persisted PLM application found.');
        return;
      }
      const refreshed = await fetchPlmApplicationStatus(localState.id);
      persistPlmState({
        ...localState,
        ...refreshed,
      });
      renderPlmState();
      setPlmMessage('PLM application refreshed from backend status.');
    } catch (error) {
      setPlmMessage(error.message || 'Unable to refresh PLM application status.');
    }
  });
}

if (walletAssignmentBody) {
  walletAssignmentBody.addEventListener('change', (event) => {
    const select = event.target.closest('.wallet-assignment-select');
    if (!select) return;
    const walletKey = select.getAttribute('data-wallet-key') || '';
    if (!walletKey) return;
    const wallet = walletAssignments.find((entry) => entry.walletKey === walletKey);
    if (!wallet) return;

    const selectedAccountRef = select.value || '';
    if (!selectedAccountRef || selectedAccountRef === wallet.effectiveAccountRef) {
      pendingWalletAssignments.delete(walletKey);
    } else {
      pendingWalletAssignments.set(walletKey, selectedAccountRef);
    }
    renderWalletAssignments();
  });

  walletAssignmentBody.addEventListener('click', async (event) => {
    const button = event.target.closest('.wallet-assignment-apply');
    if (!button) return;

    const walletKey = button.getAttribute('data-wallet-key') || '';
    if (!walletKey) return;

    const row = button.closest('tr');
    const select = row?.querySelector('.wallet-assignment-select');
    const accountRef = select?.value || '';
    if (!accountRef) return;

    try {
      button.disabled = true;
      await assignWalletToNode(walletKey, accountRef);
      pendingWalletAssignments.delete(walletKey);
    } catch (error) {
      setWalletAssignmentStatus(error.message || 'Unable to assign wallet.');
    } finally {
      button.disabled = false;
    }
  });
}

if (walletAddressCopyButton) {
  walletAddressCopyButton.addEventListener('click', async () => {
    await copyWalletAddress();
  });
}

dashboardTabButtons.forEach((button) => {
  button.addEventListener('click', () => {
    setActiveDashboardTab(button.dataset.dashboardTab || 'overview');
  });
});

window.addEventListener('keydown', (event) => {
  if (event.key === 'Escape') {
    closeAssetPicker();
    setWalletMenuOpen(false);
  }
});

document.addEventListener('click', () => {
  setWalletMenuOpen(false);
});

const photonProvider = getPhotonProvider();
if (photonProvider) {
  if (typeof photonProvider.on === 'function') {
    photonProvider.on('accountsChanged', () => {
      refreshWalletMenu();
    });
    photonProvider.on('networkChanged', () => {
      refreshWalletMenu();
    });
    photonProvider.on('disconnect', () => {
      refreshWalletMenu();
    });
  }
}

plmState = null;
adminSessionToken = loadStoredAdminSessionToken();
renderPlmState();
loadAdminConfig().catch(() => {});
updateAdminUi();
setActiveDashboardTab('overview');
loadDashboard();
refreshWalletMenu();
refreshTimer = window.setInterval(loadDashboard, 15000);
scheduleOpenChannelCheck();

window.addEventListener('beforeunload', () => {
  if (refreshTimer) {
    window.clearInterval(refreshTimer);
  }
});
