const ADMIN_SESSION_STORAGE_KEY = 'photonbolt-dev-admin-session-v1';

const authStatus = document.getElementById('auth-status');
const controlsGrid = document.getElementById('controls-grid');
const reloadSessionButton = document.getElementById('reload-session-button');

const NODE_CONTROLS = [
  {
    target: 'photon-rln-issuer',
    label: 'Issuer Node',
    meta: 'Container: photon-rln-issuer · API: 127.0.0.1:3001',
    actions: ['restart', 'refresh', 'restart-refresh'],
  },
  {
    target: 'photon-rln-user',
    label: 'User Node',
    meta: 'Container: photon-rln-user · API: 127.0.0.1:3002',
    actions: ['restart', 'refresh', 'restart-refresh'],
  },
  {
    target: 'photon-rln-user-b',
    label: 'User Node B',
    meta: 'Container: photon-rln-user-b · API: 127.0.0.1:3003',
    actions: ['restart', 'refresh', 'restart-refresh'],
  },
  {
    target: 'photon-electrs',
    label: 'Electrs',
    meta: 'Container: photon-electrs · Internal: 127.0.0.1:50001',
    actions: ['restart'],
  },
];

let adminSessionToken = null;

function setAuthStatus(message, tone = '') {
  if (!authStatus) return;
  authStatus.textContent = message;
  authStatus.className = `registry-status${tone ? ` ${tone}` : ''}`;
}

function loadStoredAdminSessionToken() {
  try {
    return window.localStorage?.getItem(ADMIN_SESSION_STORAGE_KEY) || null;
  } catch {
    return null;
  }
}

function buildAdminHeaders(extraHeaders = {}) {
  return adminSessionToken
    ? { ...extraHeaders, 'x-photon-admin-token': adminSessionToken }
    : extraHeaders;
}

function actionLabel(action) {
  if (action === 'restart-refresh') return 'Restart + Refresh';
  if (action === 'refresh') return 'Refresh Transfers';
  return 'Restart';
}

function renderControls() {
  if (!controlsGrid) return;

  controlsGrid.innerHTML = NODE_CONTROLS.map((control) => `
    <article class="node-card">
      <div class="node-card-head">
        <h3 class="node-card-title">${control.label}</h3>
        <p class="node-card-meta">${control.meta}</p>
      </div>
      <div class="node-card-actions">
        ${control.actions.map((action) => `
          <button
            class="page-btn action-btn"
            type="button"
            data-target="${control.target}"
            data-action="${action}"
            ${adminSessionToken ? '' : 'disabled'}
          >
            ${actionLabel(action)}
          </button>
        `).join('')}
      </div>
      ${control.target === 'photon-electrs' ? '' : `
        <label class="form-label" for="unlock-password-${control.target}">Unlock password</label>
        <input
          id="unlock-password-${control.target}"
          class="token-input"
          type="password"
          placeholder="Optional override for restart actions"
          autocomplete="off"
        />
      `}
      <div id="status-${control.target}" class="node-card-status">No action run yet.</div>
    </article>
  `).join('');

  controlsGrid.querySelectorAll('button[data-target]').forEach((button) => {
    button.addEventListener('click', () => {
      const target = button.getAttribute('data-target') || '';
      const action = button.getAttribute('data-action') || '';
      runNodeAction(target, action, button);
    });
  });
}

function setNodeStatus(target, message, tone = '') {
  const nodeStatus = document.getElementById(`status-${target}`);
  if (!nodeStatus) return;
  nodeStatus.textContent = message;
  nodeStatus.className = `node-card-status${tone ? ` ${tone}` : ''}`;
}

async function runNodeAction(target, action, button) {
  if (!adminSessionToken) {
    setAuthStatus('Admin session missing. Authenticate in Dev Dashboard first.', 'warning');
    renderControls();
    return;
  }

  const originalText = button.textContent;
  const unlockPasswordInput = document.getElementById(`unlock-password-${target}`);
  const unlockPassword =
    unlockPasswordInput && typeof unlockPasswordInput.value === 'string'
      ? unlockPasswordInput.value.trim()
      : '';
  button.disabled = true;
  button.textContent = 'Running...';
  setNodeStatus(target, `${actionLabel(action)} in progress...`);

  try {
    const response = await fetch('/api/admin/rgb-node-control', {
      method: 'POST',
      headers: buildAdminHeaders({ 'Content-Type': 'application/json' }),
      body: JSON.stringify({ target, action, unlockPassword }),
    });
    const payload = await response.json();
    if (!response.ok || !payload.ok) {
      if (response.status === 403) {
        adminSessionToken = null;
        try {
          window.localStorage.removeItem(ADMIN_SESSION_STORAGE_KEY);
        } catch {
          // ignore storage errors
        }
        renderControls();
      }
      throw new Error(payload.error || 'Node action failed.');
    }

    setNodeStatus(
      target,
      payload.message || `${actionLabel(action)} completed for ${payload.label || target}.`,
      'is-success'
    );
    setAuthStatus('Admin session active. Node controls unlocked.');
  } catch (error) {
    setNodeStatus(target, error.message || 'Node action failed.', 'is-error');
    setAuthStatus('Node action failed. See card status for details.', 'warning');
  } finally {
    button.disabled = !adminSessionToken;
    button.textContent = originalText;
  }
}

function reloadSession() {
  adminSessionToken = loadStoredAdminSessionToken();
  if (adminSessionToken) {
    setAuthStatus('Admin session token detected from Dev Dashboard.');
  } else {
    setAuthStatus('No admin session token found. Authenticate in Dev Dashboard first.', 'warning');
  }
  renderControls();
}

if (reloadSessionButton) {
  reloadSessionButton.addEventListener('click', reloadSession);
}

reloadSession();
