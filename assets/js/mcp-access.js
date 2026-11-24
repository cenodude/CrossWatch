/* assets/js/mcp-access.js */
/* CrossWatch - AI access tokens and connect snippets */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function (w, d) {
  const T = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.t?.(key, vars) ?? key;
  const TH = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.html?.(key, vars) ?? key;
  if (w.cwMcpAccess) return;

  const PLACEHOLDER = "cwt_YOUR_TOKEN";
  const TOKENS_URL = "/api/app-auth/tokens";
  const $ = (id) => d.getElementById(id);
  const esc = (v) => String(v ?? "").replace(/[&<>"']/g, (m) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[m]));
  const json = (value) => JSON.stringify(value, null, 2);
  const revokeTimers = new Map();
  let rawToken = "";
  let tokens = [];
  let ready = false;
  let loaded = false;
  let client = "claude-code";
  let where = "remote";

  function toast(message, ok = false) {
    try { w._cwShowToast?.(String(message || ""), !!ok); } catch {}
  }

  async function api(url, opts) {
    const res = await fetch(url, { cache: "no-store", credentials: "same-origin", ...(opts || {}) });
    const data = await res.json().catch(() => null);
    if (!res.ok || data?.ok === false) throw new Error(String(data?.error || `HTTP ${res.status}`));
    return data;
  }

  function endpoint() {
    return `${w.location.origin}/api/mcp`;
  }

  const CLIENTS = {
    "claude-code": {
      note: T("assistant.mcp.terminal"),
      snippet: (url, token) => `claude mcp add --transport http crosswatch ${url} --header "Authorization: Bearer ${token}"`,
    },
    codex: {
      note: T("assistant.mcp.codex"),
      snippet: (url, token) => `[mcp_servers.crosswatch]\nurl = "${url}"\nhttp_headers = { "Authorization" = "Bearer ${token}" }`,
    },
    cursor: {
      note: T("assistant.mcp.cursor"),
      snippet: (url, token) => json({ mcpServers: { crosswatch: { url, headers: { Authorization: `Bearer ${token}` } } } }),
    },
    "gemini-cli": {
      note: T("assistant.mcp.terminal"),
      snippet: (url, token) => `gemini mcp add --transport http --header "Authorization: Bearer ${token}" crosswatch ${url}`,
    },
    "lm-studio": {
      note: T("assistant.mcp.lmstudio"),
      snippet: (url, token) => json({ mcpServers: { crosswatch: { url, headers: { Authorization: `Bearer ${token}` } } } }),
    },
    vscode: {
      note: T("assistant.mcp.copilot"),
      snippet: (url, token) => json({ servers: { crosswatch: { type: "http", url, headers: { Authorization: `Bearer ${token}` } } } }),
    },
    "claude-desktop-docker": {
      note: T("assistant.mcp.desktop"),
      snippet: (url, token) => json({ mcpServers: { crosswatch: { command: "docker", args: ["exec", "-i", "-e", `CW_TOKEN=${token}`, "crosswatch", "cw", "mcp", "serve"] } } }),
    },
    "claude-desktop": {
      note: T("assistant.mcp.desktopNode"),
      snippet: (url, token) => {
        const args = ["-y", "mcp-remote", url, "--header", "Authorization:${AUTH_HEADER}"];
        if (url.startsWith("http://")) args.push("--allow-http");
        return json({ mcpServers: { crosswatch: { command: "npx", args, env: { AUTH_HEADER: `Bearer ${token}` } } } });
      },
    },
    other: {
      note: T("assistant.mcp.generic"),
      snippet: (url, token) => `${url}\nAuthorization: Bearer ${token}`,
    },
  };

  function selected() {
    if (client === "claude-desktop" && where === "local") return "claude-desktop-docker";
    return CLIENTS[client] ? client : "claude-code";
  }

  function when(ts) {
    const value = Number(ts || 0);
    if (value <= 0) return T("assistant.mcp.never");
    return new Date(value * 1000).toLocaleDateString((w.CW?.I18n || w.__cwI18n)?.locale, { day: "numeric", month: "short", year: "numeric" });
  }

  function stateText() {
    if (!loaded) return "";
    if (!ready) return T("assistant.mcp.signInFirst");
    if (rawToken) return T("assistant.mcp.newToken");
    if (tokens.length) return T("assistant.mcp.reuse", {placeholder: PLACEHOLDER});
    return T("assistant.mcp.createOne");
  }

  function renderTokens() {
    const state = $("mcp_token_state");
    if (state) {
      state.textContent = stateText();
      state.dataset.tone = !loaded ? "" : !ready ? "error" : rawToken ? "good" : "";
    }
    const create = $("btn-mcp-token-create");
    if (create) create.disabled = !ready;
    const list = $("mcp_tokens");
    if (!list) return;
    list.innerHTML = tokens.map((row) => `
      <div class="cw-ai-token" data-mcp-token="${esc(row.id)}">
        <span class="material-symbols-rounded" aria-hidden="true">key</span>
        <div>
          <strong>${esc(row.name || row.id)}</strong>
          <small>${TH("assistant.mcp.createdUsed", {created: when(row.created_at), used: when(row.last_used_at)})}</small>
        </div>
        <button class="cw-ai-revoke" type="button" data-mcp-revoke>${TH("assistant.mcp.revoke")}</button>
      </div>`).join("");
  }

  function render() {
    for (const btn of d.querySelectorAll("[data-mcp-client]")) {
      btn.setAttribute("aria-checked", btn.dataset.mcpClient === client ? "true" : "false");
    }
    for (const btn of d.querySelectorAll("[data-mcp-where]")) {
      btn.setAttribute("aria-checked", btn.dataset.mcpWhere === where ? "true" : "false");
    }
    $("mcp_desktop_where")?.classList.toggle("hidden", client !== "claude-desktop");
    const key = selected();
    const out = $("mcp_snippet");
    if (out) out.textContent = CLIENTS[key].snippet(endpoint(), rawToken || PLACEHOLDER);
    const note = $("mcp_client_note");
    if (note) note.textContent = CLIENTS[key].note;
    renderTokens();
  }

  async function refresh() {
    if (!$("mcp_access")) return;
    try {
      const data = await api(TOKENS_URL);
      tokens = (Array.isArray(data?.tokens) ? data.tokens : []).filter((row) => row?.access === "monitor");
      ready = true;
    } catch {
      tokens = [];
      ready = false;
    }
    loaded = true;
    render();
  }

  async function createToken() {
    const label = String(d.querySelector(`[data-mcp-client="${client}"] strong`)?.textContent || "app").trim();
    try {
      const data = await api(TOKENS_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name: `AI ${label}`.slice(0, 64), access: "monitor" }),
      });
      rawToken = String(data?.token || "");
      toast(T("assistant.mcp.created"), true);
      await refresh();
    } catch (err) {
      toast(err?.message || T("assistant.mcp.createFailed"));
    }
  }

  async function revokeToken(row) {
    const id = String(row?.dataset?.mcpToken || "");
    const btn = row?.querySelector?.("[data-mcp-revoke]");
    if (!id || !btn) return;
    if (btn.dataset.cwConfirm !== "1") {
      btn.dataset.cwConfirm = "1";
      btn.classList.add("is-confirming");
      btn.textContent = T("assistant.mcp.clickAgain");
      clearTimeout(revokeTimers.get(id));
      revokeTimers.set(id, setTimeout(() => {
        btn.dataset.cwConfirm = "";
        btn.classList.remove("is-confirming");
        btn.textContent = T("assistant.mcp.revoke");
        revokeTimers.delete(id);
      }, 4200));
      return;
    }
    clearTimeout(revokeTimers.get(id));
    revokeTimers.delete(id);
    try {
      await api(`${TOKENS_URL}/${encodeURIComponent(id)}`, { method: "DELETE" });
      if (rawToken.startsWith(`cwt_${id}.`)) rawToken = "";
      toast(T("assistant.mcp.revoked"), true);
      await refresh();
    } catch (err) {
      toast(err?.message || T("assistant.mcp.revokeFailed"));
    }
  }

  async function copy() {
    const text = String($("mcp_snippet")?.textContent || "");
    if (!text) return;
    const ok = (await w.cwCopyText?.(text)) === true;
    toast(ok ? T("assistant.mcp.copied") : T("assistant.mcp.copyFailed"), ok);
  }

  d.addEventListener("click", (event) => {
    const target = event.target;
    const pick = target?.closest?.("[data-mcp-client]");
    if (pick) {
      client = pick.dataset.mcpClient;
      render();
    }
    const place = target?.closest?.("[data-mcp-where]");
    if (place) {
      where = place.dataset.mcpWhere;
      render();
    }
    if (target?.closest?.("#btn-mcp-copy")) copy();
    if (target?.closest?.("#btn-mcp-token-create")) createToken();
    const revoke = target?.closest?.("[data-mcp-revoke]");
    if (revoke) revokeToken(revoke.closest("[data-mcp-token]"));
  });

  w.cwMcpAccess = { render, refresh, clients: CLIENTS };
  d.addEventListener("DOMContentLoaded", () => {
    render();
    refresh();
  });
  d.addEventListener("cw-settings-pane-changed", (event) => {
    if (String(event?.detail?.pane || "").trim().toLowerCase() === "security") refresh();
  });
})(window, document);
