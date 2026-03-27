const transactionsBody = document.getElementById('transactions-body');
const transactionsStatus = document.getElementById('transactions-status');
const assetTitle = document.getElementById('asset-title');
const assetSubtitle = document.getElementById('asset-subtitle');
const params = new URLSearchParams(window.location.search);
const assetId = (params.get('assetId') || '').trim();

function shorten(value, start = 12, end = 10) {
  if (!value || value.length <= start + end + 3) return value || '-';
  return `${value.slice(0, start)}...${value.slice(-end)}`;
}

function formatTimestamp(value) {
  if (!value) return '-';
  const seconds = Number(value);
  if (Number.isNaN(seconds)) return String(value);
  return new Date(seconds * 1000).toLocaleString('en-US', {
    year: 'numeric',
    month: 'long',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
  });
}

function stringifyAssignment(assignment) {
  if (!assignment) return '-';
  if (assignment.type && assignment.value !== undefined) {
    return `${assignment.type}: ${assignment.value}`;
  }
  return JSON.stringify(assignment);
}

function stringifyAssignments(assignments) {
  if (!Array.isArray(assignments) || assignments.length === 0) return '-';
  return assignments.map((entry) => stringifyAssignment(entry)).join(', ');
}

function renderTransfers(transfers) {
  if (!transactionsBody) return;

  if (!Array.isArray(transfers) || transfers.length === 0) {
    transactionsBody.innerHTML = `
      <tr class="empty-row">
        <td colspan="9">No transfers found for this asset.</td>
      </tr>
    `;
    return;
  }

  transactionsBody.innerHTML = transfers.map((transfer, index) => `
    <tr>
      <td>${index + 1}</td>
      <td>${transfer.status || '-'}</td>
      <td>${transfer.kind || '-'}</td>
      <td>${stringifyAssignment(transfer.requested_assignment)}</td>
      <td>${stringifyAssignments(transfer.assignments)}</td>
      <td><code title="${transfer.txid || ''}">${shorten(transfer.txid || '-')}</code></td>
      <td><code title="${transfer.recipient_id || ''}">${shorten(transfer.recipient_id || '-')}</code></td>
      <td>${formatTimestamp(transfer.created_at)}</td>
      <td>${formatTimestamp(transfer.updated_at)}</td>
    </tr>
  `).join('');
}

async function loadTransfers() {
  if (!assetId) {
    transactionsStatus.textContent = 'Missing assetId in query string.';
    transactionsBody.innerHTML = `
      <tr class="empty-row">
        <td colspan="9">Missing assetId.</td>
      </tr>
    `;
    return;
  }

  try {
    const response = await fetch(`/api/rgb/registry/transfers?assetId=${encodeURIComponent(assetId)}`);
    const payload = await response.json();

    if (!response.ok || !payload.ok) {
      throw new Error(payload.error || 'Failed to load transfers.');
    }

    if (payload.asset) {
      assetTitle.textContent = `${payload.asset.token_name} transactions`;
      assetSubtitle.textContent = `${payload.asset.ticker} • ${payload.asset.contract_id}`;
    } else {
      assetTitle.textContent = 'Asset transactions';
      assetSubtitle.textContent = assetId;
    }

    transactionsStatus.textContent = `${Array.isArray(payload.transfers) ? payload.transfers.length : 0} transfer(s) found.`;
    renderTransfers(payload.transfers);
  } catch (error) {
    transactionsStatus.textContent = error.message;
    transactionsBody.innerHTML = `
      <tr class="empty-row">
        <td colspan="9">${error.message}</td>
      </tr>
    `;
  }
}

loadTransfers();
