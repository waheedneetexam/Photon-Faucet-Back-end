const elements = {
  network: document.getElementById('networkValue'),
  cooldown: document.getElementById('cooldownValue'),
  wallet: document.getElementById('walletValue'),
  maxAmount: document.getElementById('maxAmountValue'),
  form: document.getElementById('claimForm'),
  invoice: document.getElementById('invoiceInput'),
  pasteButton: document.getElementById('pasteButton'),
  decodeButton: document.getElementById('decodeButton'),
  claimButton: document.getElementById('claimButton'),
  message: document.getElementById('messageBox'),
  invoiceKind: document.getElementById('invoiceKindValue'),
  assetId: document.getElementById('assetIdValue'),
  amount: document.getElementById('amountValue'),
  recipient: document.getElementById('recipientValue'),
  decodedStatus: document.getElementById('decodedStatusValue'),
  decodeState: document.getElementById('decodeState'),
  activityBody: document.getElementById('activityBody'),
  registryBody: document.getElementById('registryBody'),
  registrySearch: document.getElementById('registrySearch'),
  registryStatus: document.getElementById('registryStatus'),
  prevPageButton: document.getElementById('prevPageButton'),
  nextPageButton: document.getElementById('nextPageButton'),
  paginationInfo: document.getElementById('paginationInfo'),
};

const ACTIVITY_KEY = 'photonbolt_rgb_faucet_recent_activity';
const pageSize = 10;

let decodedInvoice = null;
let registryRows = [];
let filteredRows = [];
let currentPage = 1;

function setText(element, value) {
  if (element) {
    element.textContent = value;
  }
}

function shorten(value, start = 12, end = 10) {
  if (!value || value.length <= start + end + 3) return value || '';
  return `${value.slice(0, start)}...${value.slice(-end)}`;
}

function detectInvoiceKind(invoice) {
  const normalized = String(invoice || '').trim().toLowerCase();
  if (!normalized) return null;
  return normalized.startsWith('ln') ? 'lightning' : 'rgb';
}

function setMessage(type, text) {
  if (!elements.message) return;
  elements.message.className = `message ${type}`;
  elements.message.textContent = text;
}

function clearMessage() {
  if (!elements.message) return;
  elements.message.className = 'message hidden';
  elements.message.textContent = '';
}

function resetDecodedInvoice() {
  decodedInvoice = null;
  setText(elements.invoiceKind, '-');
  setText(elements.assetId, '-');
  setText(elements.amount, '-');
  setText(elements.recipient, '-');
  setText(elements.decodedStatus, '-');
  setText(elements.decodeState, 'Awaiting invoice');
}

function renderDecodedInvoice(payload) {
  decodedInvoice = payload;
  const decoded = payload?.decoded || {};
  const invoiceKind = payload?.invoiceKind || detectInvoiceKind(elements.invoice?.value || '');
  const assetId = decoded.asset_id || '-';
  const amount = invoiceKind === 'lightning'
    ? decoded.asset_amount || '-'
    : decoded?.assignment?.value || '-';
  const recipient = decoded.recipient_id || decoded.payee_pubkey || decoded.payment_hash || '-';
  const status = decoded.status || 'Ready for faucet claim';

  setText(elements.invoiceKind, invoiceKind || '-');
  setText(elements.assetId, assetId);
  setText(elements.amount, String(amount));
  setText(elements.recipient, recipient);
  setText(elements.decodedStatus, status);
  setText(elements.decodeState, 'Invoice decoded');
}

function readActivity() {
  try {
    const raw = localStorage.getItem(ACTIVITY_KEY);
    const parsed = raw ? JSON.parse(raw) : [];
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function writeActivity(entries) {
  localStorage.setItem(ACTIVITY_KEY, JSON.stringify(entries.slice(0, 8)));
}

function renderActivity() {
  const entries = readActivity();
  if (!elements.activityBody) return;

  if (entries.length === 0) {
    elements.activityBody.innerHTML = `
      <tr class="empty-row">
        <td colspan="4">No local RGB claims yet.</td>
      </tr>
    `;
    return;
  }

  elements.activityBody.innerHTML = entries
    .map((entry) => `
      <tr>
        <td>${entry.assetLabel}</td>
        <td>${entry.amount}</td>
        <td>${shorten(entry.reference, 14, 10)}</td>
        <td>${entry.invoiceKind}</td>
      </tr>
    `)
    .join('');
}

function pushActivity(entry) {
  const entries = readActivity();
  entries.unshift(entry);
  writeActivity(entries);
  renderActivity();
}

async function copyText(value) {
  try {
    await navigator.clipboard.writeText(value);
  } catch (error) {
    console.error('Clipboard write failed:', error);
  }
}

function updatePagination(totalRows) {
  const totalPages = Math.max(1, Math.ceil(totalRows / pageSize));
  if (elements.paginationInfo) {
    elements.paginationInfo.textContent = `Page ${currentPage} of ${totalPages}`;
  }
  if (elements.prevPageButton) {
    elements.prevPageButton.disabled = currentPage <= 1 || totalRows === 0;
  }
  if (elements.nextPageButton) {
    elements.nextPageButton.disabled = currentPage >= totalPages || totalRows === 0;
  }
}

function setRegistryStatus(text) {
  if (elements.registryStatus) {
    elements.registryStatus.textContent = text;
  }
}

function renderRegistry(rows) {
  if (!elements.registryBody) return;

  if (!rows.length) {
    elements.registryBody.innerHTML = `
      <tr class="empty-row">
        <td colspan="7">No RGB faucet assets match the current filter.</td>
      </tr>
    `;
    updatePagination(0);
    return;
  }

  const totalPages = Math.max(1, Math.ceil(rows.length / pageSize));
  if (currentPage > totalPages) currentPage = totalPages;
  const pageRows = rows.slice((currentPage - 1) * pageSize, currentPage * pageSize);

  elements.registryBody.innerHTML = pageRows.map((row, index) => `
    <tr>
      <td>${(currentPage - 1) * pageSize + index + 1}</td>
      <td><strong>${row.token_name}</strong></td>
      <td>${row.ticker || '-'}</td>
      <td>${row.precision}</td>
      <td><code title="${row.contract_id}">${shorten(row.contract_id)}</code></td>
      <td>${row.total_supply}</td>
      <td>
        <div class="action-buttons">
          <button class="copy-btn" data-contract-id="${row.contract_id}" type="button">Copy ID</button>
          <a class="copy-btn" href="/asset-transactions.html?assetId=${encodeURIComponent(row.contract_id)}">View Tx</a>
        </div>
      </td>
    </tr>
  `).join('');

  elements.registryBody.querySelectorAll('button[data-contract-id]').forEach((button) => {
    button.addEventListener('click', async () => {
      const contractId = button.getAttribute('data-contract-id') || '';
      await copyText(contractId);
      button.classList.add('copied');
      button.textContent = 'Copied';
      setRegistryStatus(`Contract ID copied: ${shorten(contractId, 16, 14)}`);
      setTimeout(() => {
        button.classList.remove('copied');
        button.textContent = 'Copy ID';
      }, 1200);
    });
  });

  updatePagination(rows.length);
}

function filterRegistry() {
  const query = (elements.registrySearch?.value || '').trim().toLowerCase();
  if (!query) {
    filteredRows = registryRows;
  } else {
    filteredRows = registryRows.filter((row) =>
      String(row.token_name || '').toLowerCase().includes(query) ||
      String(row.ticker || '').toLowerCase().includes(query) ||
      String(row.contract_id || '').toLowerCase().includes(query)
    );
  }
  currentPage = 1;
  renderRegistry(filteredRows);
}

async function loadStatus() {
  try {
    const response = await fetch('/api/status');
    const payload = await response.json();
    if (!response.ok || !payload.ok) {
      throw new Error(payload.error || 'Unable to load faucet status.');
    }
    setText(elements.network, `Connected (${payload.network})`);
    setText(
      elements.cooldown,
      payload.rgbFaucetCooldownPaused ? 'Paused' : `${payload.cooldownMinutes} min`
    );
    setText(elements.wallet, payload.wallet || '-');
    setText(elements.maxAmount, `${payload.rgbFaucetMaxAmount || '-'} units`);
  } catch (error) {
    setText(elements.network, 'Disconnected');
    setText(elements.cooldown, 'Unavailable');
    setText(elements.wallet, 'Unavailable');
    setText(elements.maxAmount, 'Unavailable');
    setMessage('error', error.message);
  }
}

async function loadRegistry() {
  try {
    const response = await fetch('/api/rgb/registry');
    const payload = await response.json();
    if (!response.ok || !payload.ok) {
      throw new Error(payload.error || 'Failed to load RGB faucet registry.');
    }
    registryRows = Array.isArray(payload.assets) ? payload.assets : [];
    filteredRows = registryRows;
    setRegistryStatus(`${registryRows.length} faucet-supported RGB assets available.`);
    renderRegistry(filteredRows);
  } catch (error) {
    setRegistryStatus(error.message);
    if (elements.registryBody) {
      elements.registryBody.innerHTML = `
        <tr class="empty-row">
          <td colspan="7">${error.message}</td>
        </tr>
      `;
    }
  }
}

async function decodeInvoice() {
  clearMessage();
  const invoice = elements.invoice?.value.trim() || '';
  if (!invoice) {
    setMessage('error', 'Paste an RGB invoice first.');
    return;
  }

  setText(elements.decodeState, 'Decoding...');
  const invoiceKind = detectInvoiceKind(invoice);
  const endpoint = invoiceKind === 'lightning'
    ? '/api/rgb/decode-lightning-invoice'
    : '/api/rgb/decode-invoice';

  try {
    const response = await fetch(endpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ invoice }),
    });
    const payload = await response.json();
    if (!response.ok || !payload.ok) {
      throw new Error(payload.error || 'Invoice decode failed.');
    }
    renderDecodedInvoice({
      invoiceKind,
      decoded: payload.decoded,
    });
    setMessage('info', 'Invoice decoded. Review asset and amount, then claim from the faucet.');
  } catch (error) {
    resetDecodedInvoice();
    setMessage('error', error.message);
  }
}

async function pasteInvoice() {
  if (!navigator.clipboard?.readText) {
    setMessage('error', 'Clipboard paste is not available in this browser.');
    return;
  }

  try {
    const value = (await navigator.clipboard.readText()).trim();
    if (!value) {
      setMessage('error', 'Clipboard is empty.');
      return;
    }
    if (elements.invoice) {
      elements.invoice.value = value;
      elements.invoice.focus();
    }
    clearMessage();
  } catch (error) {
    setMessage('error', 'Clipboard access was blocked.');
  }
}

async function submitClaim(event) {
  event.preventDefault();
  clearMessage();

  const invoice = elements.invoice?.value.trim() || '';
  if (!invoice) {
    setMessage('error', 'Paste an RGB invoice first.');
    return;
  }

  if (!decodedInvoice) {
    await decodeInvoice();
    if (!decodedInvoice) {
      return;
    }
  }

  if (elements.claimButton) {
    elements.claimButton.disabled = true;
    elements.claimButton.textContent = 'Funding invoice...';
  }

  try {
    const response = await fetch('/api/rgb/faucet/claim', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ invoice }),
    });
    const payload = await response.json();
    if (!response.ok || !payload.ok) {
      throw new Error(payload.error || 'RGB faucet claim failed.');
    }

    const assetLabel = payload.asset?.ticker || payload.asset?.token_name || shorten(payload.asset?.contract_id || 'RGB');
    const reference = payload.paymentHash || payload.txid || invoice;
    pushActivity({
      assetLabel,
      amount: `${payload.amount}`,
      reference,
      invoiceKind: payload.invoiceKind,
    });

    setMessage(
      'success',
      payload.paymentHash
        ? `RGB Lightning invoice funded. Payment hash: ${payload.paymentHash}.`
        : `RGB faucet transfer broadcast. Txid: ${payload.txid}.`
    );
    await loadStatus();
  } catch (error) {
    setMessage('error', error.message);
  } finally {
    if (elements.claimButton) {
      elements.claimButton.disabled = false;
      elements.claimButton.textContent = 'Claim From Faucet';
    }
  }
}

if (elements.form) {
  elements.form.addEventListener('submit', submitClaim);
}
if (elements.pasteButton) {
  elements.pasteButton.addEventListener('click', pasteInvoice);
}
if (elements.decodeButton) {
  elements.decodeButton.addEventListener('click', decodeInvoice);
}
if (elements.invoice) {
  elements.invoice.addEventListener('input', () => {
    resetDecodedInvoice();
    clearMessage();
  });
}
if (elements.registrySearch) {
  elements.registrySearch.addEventListener('input', filterRegistry);
}
if (elements.prevPageButton) {
  elements.prevPageButton.addEventListener('click', () => {
    if (currentPage > 1) {
      currentPage -= 1;
      renderRegistry(filteredRows);
    }
  });
}
if (elements.nextPageButton) {
  elements.nextPageButton.addEventListener('click', () => {
    const totalPages = Math.max(1, Math.ceil(filteredRows.length / pageSize));
    if (currentPage < totalPages) {
      currentPage += 1;
      renderRegistry(filteredRows);
    }
  });
}

resetDecodedInvoice();
renderActivity();
loadStatus();
loadRegistry();
