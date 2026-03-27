const registryBody = document.getElementById('registry-body');
const registrySearch = document.getElementById('registry-search');
const registryStatus = document.getElementById('registry-status');
const prevPageButton = document.getElementById('prev-page-btn');
const nextPageButton = document.getElementById('next-page-btn');
const paginationInfo = document.getElementById('pagination-info');

let registryRows = [];
let filteredRows = [];
let currentPage = 1;
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

async function copyText(value) {
  try {
    await navigator.clipboard.writeText(value);
  } catch (error) {
    console.error('Clipboard write failed:', error);
  }
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
          <a class="copy-btn" href="/asset-transactions.html?assetId=${encodeURIComponent(row.contract_id)}" title="View token transactions">View</a>
        </td>
        <td>
          <div class="action-buttons">
            <button class="copy-btn" data-contract-id="${row.contract_id}" title="Copy contract ID">Copy ID</button>
            <button class="import-btn" data-contract-id="${row.contract_id}" title="Copy contract ID for wallet import">Import</button>
          </div>
        </td>
      </tr>
    `)
    .join('');

  registryBody.querySelectorAll('.copy-btn').forEach((button) => {
    button.addEventListener('click', async () => {
      const contractId = button.getAttribute('data-contract-id') || '';
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

  registryBody.querySelectorAll('.import-btn').forEach((button) => {
    button.addEventListener('click', async () => {
      const contractId = button.getAttribute('data-contract-id') || '';
      await copyText(contractId);
      button.classList.add('copied');
      button.textContent = 'Ready';
      setStatus(`Import ready. Contract ID copied. Open Photon wallet > Add Assets and paste ${shorten(contractId, 18, 14)}.`);
      setTimeout(() => {
        button.classList.remove('copied');
        button.textContent = 'Import';
      }, 1800);
    });
  });

  updatePagination(rows.length);
}

function setStatus(text) {
  if (registryStatus) {
    registryStatus.textContent = text;
  }
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

function filterRows() {
  const query = (registrySearch?.value || '').trim().toLowerCase();
  if (!query) {
    filteredRows = registryRows;
    currentPage = 1;
    renderRows(filteredRows);
    return;
  }

  filteredRows = registryRows.filter((row) => {
    return (
      String(row.token_name || '').toLowerCase().includes(query) ||
      String(row.ticker || '').toLowerCase().includes(query) ||
      String(row.contract_id || '').toLowerCase().includes(query)
    );
  });

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

loadRegistry();
