const JSON_HEADERS = { "Content-Type": "application/json" };
const MIN_PASSWORD_LENGTH = 8;

async function _readJson(res) {
  try {
    return await res.json();
  } catch {
    return null;
  }
}

async function _getJson(url) {
  const res = await fetch(url, { cache: "no-store", credentials: "same-origin" });
  const data = await _readJson(res);
  if (!res.ok) {
    throw new Error((data && (data.error || data.message)) || `HTTP ${res.status}`);
  }
  return data || {};
}

async function _postJson(url, body) {
  const res = await fetch(url, {
    method: "POST",
    headers: JSON_HEADERS,
    credentials: "same-origin",
    cache: "no-store",
    body: JSON.stringify(body || {}),
  });
  const data = await _readJson(res);
  if (!res.ok || (data && data.ok === false)) {
    throw new Error((data && (data.error || data.message)) || `HTTP ${res.status}`);
  }
  return data || {};
}

function _markAuthSetupPending(flag) {
  try {
    window.__cwAuthSetupPending = !!flag;
    window.dispatchEvent(new CustomEvent("cw-auth-setup-pending", { detail: { pending: !!flag } }));
  } catch {}
}

export function escapeHtml(s) {
  return String(s || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

export async function fetchAppAuthStatus() {
  try {
    return await _getJson("/api/app-auth/status");
  } catch {
    return {};
  }
}

export function hasEnabledAppAuth(status) {
  return !!(status && status.enabled && status.configured);
}

export function syncAppAuthState(root, state) {
  if (!root || !state) return state;
  const usernameEl = root.querySelector('[data-field="username"]');
  const passwordEl = root.querySelector('[data-field="password"]');
  const password2El = root.querySelector('[data-field="password2"]');
  if (usernameEl) state.username = usernameEl.value;
  if (passwordEl) state.password = passwordEl.value;
  if (password2El) state.password2 = password2El.value;
  return state;
}

export function validateAppAuthState(state) {
  if (!String(state?.username || "").trim()) return "Username is required.";
  if (!String(state?.password || "")) return "Password is required.";
  if (String(state?.password || "").length < MIN_PASSWORD_LENGTH) return `Password must be at least ${MIN_PASSWORD_LENGTH} characters.`;
  if (state?.password !== state?.password2) return "Passwords do not match.";
  return "";
}

function _liveAppAuthError(state) {
  const password = String(state?.password || "");
  const password2 = String(state?.password2 || "");
  if (!password && !password2) return "";
  if (!password && password2) return "Password is required.";
  if (password.length < MIN_PASSWORD_LENGTH) return `Password must be at least ${MIN_PASSWORD_LENGTH} characters.`;
  if (password2 && password !== password2) return "Passwords do not match.";
  return "";
}

export function wireLiveAppAuthValidation(root, state, errorId = "", saveBtn = null) {
  if (!root || !state) return;
  const errEl = errorId ? root.querySelector(`#${errorId}`) : root.querySelector(".err");
  const update = () => {
    syncAppAuthState(root, state);
    const next = _liveAppAuthError(state);
    state.error = next;
    if (saveBtn) saveBtn.disabled = !!state.saving || !!validateAppAuthState(state);
    if (!errEl) return;
    errEl.textContent = next;
    errEl.classList.toggle("show", !!next);
  };

  for (const sel of ['[data-field="username"]', '[data-field="password"]', '[data-field="password2"]']) {
    const el = root.querySelector(sel);
    if (!el || el.dataset.liveAuthWired === "1") continue;
    el.addEventListener("input", update);
    el.dataset.liveAuthWired = "1";
  }
  update();
}

export function setModalShellInline(shell) {
  if (!shell) return;
  shell.style.width = "auto";
  shell.style.maxWidth = "none";
  shell.style.height = "auto";
  shell.style.maxHeight = "none";
  shell.style.display = "inline-block";
}

export function setModalDismissible(flag) {
  try { window.cxSetModalDismissible?.(flag !== false); } catch {}
}

export function appAuthFormCss(scope) {
  return `
${scope} .authPanel{display:grid;gap:12px;margin-top:12px}
${scope} .authCard{padding:14px;border-radius:16px;background:rgba(255,255,255,.03);border:1px solid rgba(255,255,255,.08);box-shadow:0 10px 30px rgba(0,0,0,.30)}
${scope} .authGrid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));column-gap:16px;row-gap:18px}
${scope} .field.full{grid-column:1 / -1}
@media (max-width:780px){${scope} .authGrid{grid-template-columns:1fr}}
${scope} label{display:block;font-weight:800;font-size:12px;opacity:.86;margin-bottom:6px}
${scope} .field input{width:100%;min-height:42px;border-radius:12px;border:1px solid rgba(255,255,255,.12);background:rgba(8,10,16,.88);color:#eaf0ff;padding:10px 12px;font:inherit}
${scope} .field input:focus{outline:2px solid rgba(150,70,255,.35);outline-offset:1px}
${scope} .field .subtxt{opacity:.68;font-size:12px;margin-top:6px;line-height:1.4}
${scope} .err{display:none;margin-top:10px;padding:10px 12px;border-radius:12px;border:1px solid rgba(255,120,120,.22);background:linear-gradient(180deg,rgba(255,77,79,.12),rgba(255,77,79,.04));color:#ffd8d8;font-size:12.5px}
${scope} .err.show{display:block}
  `;
}

export function renderAppAuthFields({
  idPrefix,
  state,
  errorId = "",
  wrap = true,
}) {
  const errCls = state?.error ? "err show" : "err";
  const errAttr = errorId ? ` id="${escapeHtml(errorId)}"` : "";
  const body = `
    <div class="authGrid">
      <div class="field full">
        <label for="${escapeHtml(idPrefix)}-user">Username</label>
        <input id="${escapeHtml(idPrefix)}-user" data-field="username" type="text" autocomplete="username" placeholder="admin" value="${escapeHtml(state?.username)}">
      </div>
      <div class="field">
        <label for="${escapeHtml(idPrefix)}-pass">Password</label>
        <input id="${escapeHtml(idPrefix)}-pass" data-field="password" type="password" autocomplete="new-password" placeholder="Enter password" value="${escapeHtml(state?.password)}">
        <div class="subtxt">Minimum ${MIN_PASSWORD_LENGTH} characters.</div>
      </div>
      <div class="field">
        <label for="${escapeHtml(idPrefix)}-pass2">Confirm password</label>
        <input id="${escapeHtml(idPrefix)}-pass2" data-field="password2" type="password" autocomplete="new-password" placeholder="Repeat password" value="${escapeHtml(state?.password2)}">
      </div>
    </div>
    <div class="${errCls}"${errAttr}>${escapeHtml(state?.error)}</div>
  `;
  if (wrap === false) return body;
  return `
    <div class="authPanel">
      <div class="authCard">${body}</div>
    </div>
  `;
}

export async function saveRequiredAppAuth({ username, password, keepPending = false }) {
  const user = String(username || "").trim();
  const pass = String(password || "");
  if (!user) throw new Error("Username is required");
  if (!pass) throw new Error("Password is required");
  if (pass.length < MIN_PASSWORD_LENGTH) throw new Error(`Password must be at least ${MIN_PASSWORD_LENGTH} characters`);

  const data = await _postJson("/api/app-auth/credentials", {
    enabled: true,
    username: user,
    password: pass,
  });
  _markAuthSetupPending(keepPending);
  try { window.dispatchEvent(new Event("auth-changed")); } catch {}
  return data;
}
