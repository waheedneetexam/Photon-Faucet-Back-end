const ADMIN_SESSION_STORAGE_KEY = 'photonbolt-dev-admin-session-v1';
const ADMIN_WALLET_ADDRESS = 'bcrt1pyzsrsnu84dmrtvthpvxfjd88pk60h3q394ulaq2q5dqun3wrj2eq4h4q95';

const registryBody = document.getElementById('registry-body');
const registrySearch = document.getElementById('registry-search');
const registryStatus = document.getElementById('registry-status');
const prevPageButton = document.getElementById('prev-page-btn');
const nextPageButton = document.getElementById('next-page-btn');
const paginationInfo = document.getElementById('pagination-info');
const walletConnectButton = document.getElementById('registry-wallet-connect');
const adminLoginButton = document.getElementById('registry-admin-login');
const adminLogoutButton = document.getElementById('registry-admin-logout');
const adminStatus = document.getElementById('registry-admin-status');

let registryRows = [];
let filteredRows = [];
let currentPage = 1;
let connectedAddress = '';
let adminSessionToken = loadStoredAdminSessionToken();
const pageSize = 10;
const dateFormatter = new Intl.DateTimeFormat('en-US', {
  month: 'long',
  day: 'numeric',
  year: 'numeric',
});

function shorten(value, start = 10, end = 8) {
  if (!value || value.length <= start + end + 3) return value || '';
  return `${value.slice(0, start)}...${value.slice(-end)}`;
}

function formatCreationDate(value) {
  if (!value) return '-';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return dateFormatter.format(date);
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

function getPhotonProvider() {
  if (typeof window === 'undefined') return null;
  if (!window.photonbolt || !window.photonbolt.isPhotonBolt) return null;
  return window.photonbolt;
}

function isAdminWalletAddress(address) {
  return typeof address === 'string' && address.trim() === ADMIN_WALLET_ADDRESS;
}

function isAdminAuthenticated() {
  return Boolean(adminSessionToken) && isAdminWalletAddress(connectedAddress);
}

function buildAdminHeaders(extraHeaders = {}) {
  return adminSessionToken
    ? { ...extraHeaders, 'x-photon-admin-token': adminSessionToken }
    : extraHeaders;
}

function setStatus(text) {
  if (registryStatus) {
    registryStatus.textContent = text;
  }
}

function setAdminStatus(text) {
  if (adminStatus) {
    adminStatus.textContent = text;
  }
}

function updateAdminUi() {
  const isAdminAddress = isAdminWalletAddress(connectedAddress);
  const isAuthenticated = isAdminAuthenticated();

  if (walletConnectButton) {
    walletConnectButton.textContent = connectedAddress ? `Wallet ${shorten(connectedAddress, 8, 6)}` : 'Connect Wallet';
  }
  if (adminLoginButton) {
    adminLoginButton.disabled = !isAdminAddress;
  }
  if (adminLogoutButton) {
    adminLogoutButton.disabled = !adminSessionToken;
  }

  if (!connectedAddress) {
    setAdminStatus('Connect Photon Wallet to continue.');
  } else if (!isAdminAddress) {
    setAdminStatus(`Connected wallet ${shorten(connectedAddress, 8, 6)} is not the configured admin wallet.`);
  } else if (isAuthenticated) {
    setAdminStatus(`Admin unlocked for ${shorten(connectedAddress, 8, 6)}. Archive controls are enabled.`);
  } else {
    setAdminStatus('Configured admin wallet detected. Sign the Photon challenge to unlock archive controls.');
  }
}

async function copyText(value) {
  try {
    await navigator.clipboard.writeText(value);
  } catch (error) {
    console.error('Clipboard write failed:', error);
  }
}

async function refreshConnectedWalletState() {
  const photon = getPhotonProvider();
  if (!photon) {
    connectedAddress = '';
    persistAdminSessionToken(null);
    updateAdminUi();
    return;
  }

  try {
    const accounts = await photon.getAccounts();
    connectedAddress = Array.isArray(accounts) ? (accounts[0] || '') : '';
    if (!isAdminWalletAddress(connectedAddress)) {
      persistAdminSessionToken(null);
    }
  } catch {
    connectedAddress = '';
    persistAdminSessionToken(null);
  }

  updateAdminUi();
}

function updatePagination(totalRows) {
  const totalPages = Math.max(1, Math.ceil(totalRows / pageSize));
  if (paginationInfo) {
    paginationInfo.textContent = `Page ${currentPage} of ${totalPages}`;
  }
  if (prevPageButton) {
    prevPageButton.disabled = currentPage <= 1 || totalRows === 0;
  }
  if (nextPageButton) {
    nextPageButton.disabled = currentPage >= totalPages || totalRows === 0;
  }
}

async function archiveAsset(assetId, archived) {
  const response = await fetch('/api/rgb/archive-asset', {
    method: 'POST',
    headers: buildAdminHeaders({
      'Content-Type': 'application/json',
    }),
    body: JSON.stringify({
      assetId,
      archived,
      reason: archived ? 'Archived from asset registry by admin wallet' : 'Restored in asset registry by admin wallet',
    }),
  });
  const payload = await response.json();
  if (!response.ok || !payload.ok) {
    if (response.status === 403) {
      persistAdminSessionToken(null);
      updateAdminUi();
    }
    throw new Error(payload.error || 'Archive request failed.');
  }
  return payload.asset;
}

function renderRows(rows) {
  if (!registryBody) return;

  if (rows.length === 0) {
    registryBody.innerHTML = `
      <tr class="empty-row">
        <td colspan="10">No assets match the current filter.</td>
      </tr>
    `;
    updatePagination(0);
    return;
  }

  const totalPages = Math.max(1, Math.ceil(rows.length / pageSize));
  if (currentPage > totalPages) currentPage = totalPages;

  const pageRows = rows.slice((currentPage - 1) * pageSize, currentPage * pageSize);
  const showAdminActions = isAdminAuthenticated();

  registryBody.innerHTML = pageRows
    .map((row, index) => `
      <tr>
        <td>${(currentPage - 1) * pageSize + index + 1}</td>
        <td><strong>${row.token_name}</strong></td>
        <td>${row.total_supply}</td>
        <td>${row.precision}</td>
        <td>${row.issuer_ref || '-'}</td>
        <td>${formatCreationDate(row.creation_date)}</td>
        <td>${row.block_height || '-'}</td>
        <td>
          <code title="${row.contract_id}">${shorten(row.contract_id, 12, 10)}</code>
        </td>
        <td>
          <a class="copy-btn page-link-btn" href="/asset-transactions.html?assetId=${encodeURIComponent(row.contract_id)}" title="View token transactions">View</a>
        </td>
        <td>
          <div class="action-buttons">
            <button class="copy-btn" data-copy-contract-id="${row.contract_id}" title="Copy contract ID">Copy ID</button>
            <button class="import-btn" data-import-contract-id="${row.contract_id}" title="Copy contract ID for wallet import">Import</button>
            ${showAdminActions ? `<button class="archive-btn archive" data-archive-contract-id="${row.contract_id}" title="Archive asset">Archive</button>` : ''}
          </div>
        </td>
      </tr>
    `)
    .join('');

  registryBody.querySelectorAll('button[data-copy-contract-id]').forEach((button) => {
    button.addEventListener('click', async () => {
      const contractId = button.getAttribute('data-copy-contract-id') || '';
      await copyText(contractId);
      button.classList.add('copied');
      button.textContent = 'Copied';
      setStatus(`Contract ID copied: ${shorten(contractId, 18, 14)}`);
      setTimeout(() => {
        button.classList.remove('copied');
        button.textContent = 'Copy ID';
      }, 1400);
    });
  });

  registryBody.querySelectorAll('button[data-import-contract-id]').forEach((button) => {
    button.addEventListener('click', async () => {
      const contractId = button.getAttribute('data-import-contract-id') || '';
      const photon = getPhotonProvider();
      if (!photon || typeof photon.importAsset !== 'function') {
        await copyText(contractId);
        button.classList.add('copied');
        button.textContent = 'Ready';
        setStatus(`Photon Wallet does not expose importAsset() yet. Contract ID copied for manual import: ${shorten(contractId, 18, 14)}.`);
        setTimeout(() => {
          button.classList.remove('copied');
          button.textContent = 'Import';
        }, 1800);
        return;
      }

      try {
        const accounts = typeof photon.getAccounts === 'function' ? await photon.getAccounts() : [];
        if (!Array.isArray(accounts) || accounts.length === 0) {
          await photon.connect();
          await refreshConnectedWalletState();
        }

        button.disabled = true;
        button.textContent = 'Importing...';
        const result = await photon.importAsset({ contractId });
        button.classList.add('copied');
        button.textContent = result?.alreadyImported ? 'Imported' : 'Added';
        setStatus(
          result?.alreadyImported
            ? `Asset already existed in Photon Wallet and was refreshed.`
            : `Asset added to Photon Wallet successfully.`
        );
      } catch (error) {
        console.error('Asset import failed:', error);
        await copyText(contractId);
        setStatus(`${error.message || 'Asset import failed.'} Contract ID copied for manual import.`);
        button.textContent = 'Copy Fallback';
      } finally {
        setTimeout(() => {
          button.disabled = false;
          button.classList.remove('copied');
          button.textContent = 'Import';
        }, 1800);
      }
    });
  });

  registryBody.querySelectorAll('button[data-archive-contract-id]').forEach((button) => {
    button.addEventListener('click', async () => {
      const contractId = button.getAttribute('data-archive-contract-id') || '';
      const confirmed = window.confirm(`Archive asset ${contractId}? It will stop showing in wallets and registry views.`);
      if (!confirmed) return;
      button.disabled = true;
      button.textContent = 'Archiving...';
      try {
        const archivedAsset = await archiveAsset(contractId, true);
        setStatus(`Archived ${archivedAsset.ticker || archivedAsset.token_name || shorten(contractId, 12, 10)}.`);
        await loadRegistry();
      } catch (error) {
        console.error('Archive failed:', error);
        setStatus(error.message || 'Archive failed.');
        button.disabled = false;
        button.textContent = 'Archive';
      }
    });
  });

  updatePagination(rows.length);
}

function filterRows() {
  const query = (registrySearch?.value || '').trim().toLowerCase();
  if (!query) {
    filteredRows = registryRows;
    currentPage = 1;
    renderRows(filteredRows);
    return;
  }

  filteredRows = registryRows.filter((row) => (
    String(row.token_name || '').toLowerCase().includes(query) ||
    String(row.ticker || '').toLowerCase().includes(query) ||
    String(row.contract_id || '').toLowerCase().includes(query)
  ));

  currentPage = 1;
  renderRows(filteredRows);
}

async function loadRegistry() {
  try {
    const response = await fetch('/api/rgb/registry');
    const payload = await response.json();

    if (!response.ok || !payload.ok) {
      throw new Error(payload.error || 'Failed to load asset registry.');
    }

    registryRows = Array.isArray(payload.assets) ? payload.assets : [];
    filteredRows = registryRows;
    setStatus(`${registryRows.length} asset(s) available in the Photon registry.`);
    renderRows(filteredRows);
  } catch (error) {
    registryBody.innerHTML = `
      <tr class="empty-row">
        <td colspan="10">${error.message}</td>
      </tr>
    `;
    setStatus(error.message);
  }
}

async function connectWallet() {
  const photon = getPhotonProvider();
  if (!photon) {
    setAdminStatus('Photon Wallet extension is not installed in this browser.');
    return;
  }

  try {
    setAdminStatus('Requesting connection from Photon Wallet...');
    await photon.connect();
    await refreshConnectedWalletState();
  } catch (error) {
    setAdminStatus(error.message || 'Photon Wallet connection failed.');
  }
}

async function unlockAdminControls() {
  const photon = getPhotonProvider();
  if (!photon) {
    setAdminStatus('Photon Wallet extension is not installed in this browser.');
    return;
  }

  if (!isAdminWalletAddress(connectedAddress)) {
    setAdminStatus('Connect the configured admin wallet first.');
    return;
  }

  try {
    setAdminStatus('Requesting admin challenge...');
    const challengeResponse = await fetch(`/api/admin/auth/challenge?address=${encodeURIComponent(connectedAddress)}`);
    const challengePayload = await challengeResponse.json();
    if (!challengeResponse.ok || !challengePayload.ok) {
      throw new Error(challengePayload.error || 'Failed to request admin challenge.');
    }

    setAdminStatus('Sign the admin challenge in Photon Wallet...');
    const signResult = typeof photon._sendRequest === 'function'
      ? await photon._sendRequest('signMessage', { message: challengePayload.message })
      : { signature: await photon.signMessage(challengePayload.message), address: connectedAddress };

    const signedAddress = typeof signResult?.address === 'string' ? signResult.address.trim() : connectedAddress;
    const signature = typeof signResult?.signature === 'string' ? signResult.signature.trim() : '';

    if (!signature) {
      throw new Error('Photon Wallet did not return a signature.');
    }

    if (!isAdminWalletAddress(signedAddress)) {
      throw new Error(`Photon Wallet signed with ${signedAddress || 'an unknown address'}, not the configured admin wallet.`);
    }

    setAdminStatus('Verifying admin signature...');
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
    renderRows(filteredRows);
  } catch (error) {
    setAdminStatus(error.message || 'Admin login failed.');
  }
}

async function logoutAdmin() {
  try {
    if (adminSessionToken) {
      await fetch('/api/admin/auth/logout', {
        method: 'POST',
        headers: buildAdminHeaders(),
      });
    }
  } catch {
    // ignore logout failures
  }

  persistAdminSessionToken(null);
  updateAdminUi();
  renderRows(filteredRows);
}

if (registrySearch) {
  registrySearch.addEventListener('input', filterRows);
}

if (prevPageButton) {
  prevPageButton.addEventListener('click', () => {
    if (currentPage > 1) {
      currentPage -= 1;
      renderRows(filteredRows);
    }
  });
}

if (nextPageButton) {
  nextPageButton.addEventListener('click', () => {
    const totalPages = Math.max(1, Math.ceil(filteredRows.length / pageSize));
    if (currentPage < totalPages) {
      currentPage += 1;
      renderRows(filteredRows);
    }
  });
}

if (walletConnectButton) {
  walletConnectButton.addEventListener('click', async () => {
    await connectWallet();
  });
}

if (adminLoginButton) {
  adminLoginButton.addEventListener('click', async () => {
    await unlockAdminControls();
  });
}

if (adminLogoutButton) {
  adminLogoutButton.addEventListener('click', async () => {
    await logoutAdmin();
  });
}

window.addEventListener('photonbolt#initialized', () => {
  refreshConnectedWalletState().catch(() => {});
});

(async () => {
  await refreshConnectedWalletState();

  const photon = getPhotonProvider();
  if (photon && typeof photon.on === 'function') {
    photon.on('accountsChanged', async () => {
      await refreshConnectedWalletState();
      renderRows(filteredRows);
    });
    photon.on('disconnect', async () => {
      await refreshConnectedWalletState();
      renderRows(filteredRows);
    });
  }

  await loadRegistry();
})();
