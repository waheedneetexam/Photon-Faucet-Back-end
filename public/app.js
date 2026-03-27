const elements = {
  network: document.getElementById('networkValue'),
  walletBalance: document.getElementById('walletBalanceValue'),
  amount: document.getElementById('amountValue'),
  cooldown: document.getElementById('cooldownValue'),
  statusMode: document.getElementById('statusMode'),
  statusSummary: document.getElementById('statusSummary'),
  form: document.getElementById('claimForm'),
  address: document.getElementById('address'),
  button: document.getElementById('claimButton'),
  pasteButton: document.getElementById('pasteButton'),
  message: document.getElementById('messageBox'),
  activityBody: document.getElementById('activityBody'),
};
const amountChips = Array.from(document.querySelectorAll('[data-amount-btc]'));

const ACTIVITY_KEY = 'photonbolt_faucet_recent_activity';
const urlParams = new URLSearchParams(window.location.search);
let selectedAmountBtc = '0.5';

function setText(element, value) {
  if (element) {
    element.textContent = value;
  }
}

function shortenMiddle(value, prefix = 10, suffix = 8) {
  if (!value || value.length <= prefix + suffix + 3) {
    return value || '';
  }
  return `${value.slice(0, prefix)}...${value.slice(-suffix)}`;
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
  localStorage.setItem(ACTIVITY_KEY, JSON.stringify(entries.slice(0, 6)));
}

function renderActivity() {
  const entries = readActivity();
  if (!elements.activityBody) {
    return;
  }
  if (entries.length === 0) {
    elements.activityBody.innerHTML = `
      <tr class="empty-row">
        <td colspan="4">No recent local requests yet.</td>
      </tr>
    `;
    return;
  }

  elements.activityBody.innerHTML = entries
    .map(
      (entry) => `
        <tr>
          <td>${shortenMiddle(entry.txid, 10, 6)}</td>
          <td>${shortenMiddle(entry.address, 12, 6)}</td>
          <td>${entry.amount}</td>
          <td class="status-confirmed">${entry.status}</td>
        </tr>
      `
    )
    .join('');
}

function pushActivity(entry) {
  const entries = readActivity();
  entries.unshift(entry);
  writeActivity(entries);
  renderActivity();
}

function setMessage(type, text) {
  if (!elements.message) {
    return;
  }
  elements.message.className = `message ${type}`;
  elements.message.textContent = text;
}

function clearMessage() {
  if (!elements.message) {
    return;
  }
  elements.message.className = 'message hidden';
  elements.message.textContent = '';
}

function updateAmountSelection(nextAmountBtc) {
  selectedAmountBtc = nextAmountBtc;
  amountChips.forEach((chip) => {
    const isActive = chip.dataset.amountBtc === nextAmountBtc;
    chip.classList.toggle('amount-chip-active', isActive);
    chip.setAttribute('aria-pressed', String(isActive));
  });
}

async function loadStatus() {
  try {
    const response = await fetch('/api/status');
    const payload = await response.json();

    if (!response.ok || !payload.ok) {
      throw new Error(payload.error || 'Unable to reach the faucet backend.');
    }

    setText(elements.network, `Connected (${payload.network})`);
    setText(elements.walletBalance, `${payload.balance} BTC`);
    const allowedAmounts = Array.isArray(payload.allowedAmountsBtc) ? payload.allowedAmountsBtc : [];
    const defaultAmount = typeof payload.amountBtc === 'string' ? payload.amountBtc : '0.5';
    const nextAmount = allowedAmounts.includes(selectedAmountBtc) ? selectedAmountBtc : defaultAmount;
    updateAmountSelection(nextAmount);
    setText(elements.amount, `${allowedAmounts.join(' / ')} BTC`);
    setText(elements.cooldown, `${payload.cooldownMinutes} min`);
    setText(elements.statusMode, String(payload.blocks));
    setText(
      elements.statusSummary,
      `Node connected on ${payload.network}. Wallet ${payload.wallet} is funded and ready.`
    );
  } catch (error) {
    setText(elements.network, 'Disconnected');
    setText(elements.walletBalance, 'Unavailable');
    setText(elements.amount, 'Unavailable');
    setText(elements.cooldown, 'Unavailable');
    setText(elements.statusMode, 'Offline');
    setText(elements.statusSummary, error.message);
    setMessage('error', `Backend status check failed: ${error.message}`);
  }
}

async function submitClaim(event) {
  event.preventDefault();
  clearMessage();

  const address = elements.address?.value.trim() || '';
  if (!address) {
    setMessage('error', 'Enter a regtest Bitcoin address first.');
    return;
  }

  if (elements.button) {
    elements.button.disabled = true;
    elements.button.textContent = 'Request in progress...';
  }

  try {
    const response = await fetch('/api/claim', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ address, amountBtc: selectedAmountBtc }),
    });

    const payload = await response.json();
    if (!response.ok || !payload.ok) {
      throw new Error(payload.error || 'Faucet request failed.');
    }

    const mined = Array.isArray(payload.minedBlocks) && payload.minedBlocks.length > 0;
    const minedSuffix = mined ? ` Block mined: ${payload.minedBlocks[0]}.` : '';
    setMessage(
      'success',
      `Sent ${payload.amountBtc} BTC on ${payload.network}. Txid: ${payload.txid}.${minedSuffix}`
    );
    pushActivity({
      txid: payload.txid,
      address,
      amount: `${payload.amountBtc} BTC`,
      status: 'Confirmed',
    });
    if (elements.address) {
      elements.address.value = '';
    }
    await loadStatus();
  } catch (error) {
    setMessage('error', error.message);
  } finally {
    if (elements.button) {
      elements.button.disabled = false;
      elements.button.textContent = 'Request bitcoin';
    }
  }
}

async function pasteAddress() {
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
    if (elements.address) {
      elements.address.value = value;
      elements.address.focus();
    }
    clearMessage();
  } catch (error) {
    setMessage('error', 'Clipboard access was blocked.');
  }
}

function handleAddressKeys(event) {
  if (event.ctrlKey && event.key === 'Enter') {
    event.preventDefault();
    elements.form.requestSubmit();
  }
}

function hydrateAddressFromQuery() {
  const queryAddress = (urlParams.get('address') || '').trim();
  if (!queryAddress || !elements.address) {
    return;
  }
  elements.address.value = queryAddress;
}

if (elements.form) {
  elements.form.addEventListener('submit', submitClaim);
}
if (elements.address) {
  elements.address.addEventListener('keydown', handleAddressKeys);
}
if (elements.pasteButton) {
  elements.pasteButton.addEventListener('click', pasteAddress);
}
amountChips.forEach((chip) => {
  chip.addEventListener('click', () => {
    if (chip.dataset.amountBtc) {
      updateAmountSelection(chip.dataset.amountBtc);
    }
  });
});
updateAmountSelection(selectedAmountBtc);
hydrateAddressFromQuery();
renderActivity();
loadStatus();
