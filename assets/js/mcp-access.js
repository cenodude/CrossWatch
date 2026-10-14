/* assets/js/mcp-access.js */
/* CrossWatch - AI access tokens and connect snippets */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function (w, d) {
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
      note: "Run this once in a terminal",
      snippet: (url, token) => `claude mcp add --transport http crosswatch ${url} --header "Authorization: Bearer ${token}"`,
    },
    codex: {
      note: "Add this to ~/.codex/config.toml",
      snippet: (url, token) => `[mcp_servers.crosswatch]\nurl = "${url}"\nhttp_headers = { "Authorization" = "Bearer ${token}" }`,
    },
    cursor: {
      note: "Add this to .cursor/mcp.json",
      snippet: (url, token) => json({ mcpServers: { crosswatch: { url, headers: { Authorization: `Bearer ${token}` } } } }),
    },
    "gemini-cli": {
      note: "Run this once in a terminal",
      snippet: (url, token) => `gemini mcp add --transport http --header "Authorization: Bearer ${token}" crosswatch ${url}`,
    },
    "lm-studio": {
      note: "In LM Studio open Program, Install, Edit mcp.json and add this",
      snippet: (url, token) => json({ mcpServers: { crosswatch: { url, headers: { Authorization: `Bearer ${token}` } } } }),
    },
    vscode: {
      note: "For GitHub Copilot, add this to .vscode/mcp.json",
      snippet: (url, token) => json({ servers: { crosswatch: { type: "http", url, headers: { Authorization: `Bearer ${token}` } } } }),
    },
    "claude-desktop-docker": {
      note: "Add this to claude_desktop_config.json",
      snippet: (url, token) => json({ mcpServers: { crosswatch: { command: "docker", args: ["exec", "-i", "-e", `CW_TOKEN=${token}`, "crosswatch", "cw", "mcp", "serve"] } } }),
    },
    "claude-desktop": {
      note: "Add this to claude_desktop_config.json. It needs Node.js on this computer",
      snippet: (url, token) => {
        const args = ["-y", "mcp-remote", url, "--header", "Authorization:${AUTH_HEADER}"];
        if (url.startsWith("http://")) args.push("--allow-http");
        return json({ mcpServers: { crosswatch: { command: "npx", args, env: { AUTH_HEADER: `Bearer ${token}` } } } });
      },
    },
    other: {
      note: "Give your app this address and header",
      snippet: (url, token) => `${url}\nAuthorization: Bearer ${token}`,
    },
  };

  function selected() {
    if (client === "claude-desktop" && where === "local") return "claude-desktop-docker";
    return CLIENTS[client] ? client : "claude-code";
  }

  function when(ts) {
    const value = Number(ts || 0);
    if (value <= 0) return "never";
    return new Date(value * 1000).toLocaleDateString(undefined, { day: "numeric", month: "short", year: "numeric" });
  }

  function stateText() {
    if (!loaded) return "";
    if (!ready) return "Turn on sign-in under Authentication first. AI apps can only connect with a token.";
    if (rawToken) return "New token filled in below. It is shown only once, so copy the text now.";
    if (tokens.length) return `Reuse a token you saved earlier, or create a new one. Until then the text below shows ${PLACEHOLDER}.`;
    return "Create one and it is filled in below.";
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
          <small>Created ${esc(when(row.created_at))}, last used ${esc(when(row.last_used_at))}</small>
        </div>
        <button class="cw-ai-revoke" type="button" data-mcp-revoke>Revoke</button>
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
      toast("Token created", true);
      await refresh();
    } catch (err) {
      toast(err?.message || "Could not create the token");
    }
  }

  async function revokeToken(row) {
    const id = String(row?.dataset?.mcpToken || "");
    const btn = row?.querySelector?.("[data-mcp-revoke]");
    if (!id || !btn) return;
    if (btn.dataset.cwConfirm !== "1") {
      btn.dataset.cwConfirm = "1";
      btn.classList.add("is-confirming");
      btn.textContent = "Click again to revoke";
      clearTimeout(revokeTimers.get(id));
      revokeTimers.set(id, setTimeout(() => {
        btn.dataset.cwConfirm = "";
        btn.classList.remove("is-confirming");
        btn.textContent = "Revoke";
        revokeTimers.delete(id);
      }, 4200));
      return;
    }
    clearTimeout(revokeTimers.get(id));
    revokeTimers.delete(id);
    try {
      await api(`${TOKENS_URL}/${encodeURIComponent(id)}`, { method: "DELETE" });
      if (rawToken.startsWith(`cwt_${id}.`)) rawToken = "";
      toast("Token revoked", true);
      await refresh();
    } catch (err) {
      toast(err?.message || "Could not revoke the token");
    }
  }

  async function copy() {
    const text = String($("mcp_snippet")?.textContent || "");
    if (!text) return;
    let ok = false;
    try {
      if (navigator.clipboard && w.isSecureContext) {
        await navigator.clipboard.writeText(text);
        ok = true;
      } else {
        const area = d.createElement("textarea");
        area.value = text;
        area.setAttribute("readonly", "");
        area.style.position = "fixed";
        area.style.top = "-9999px";
        d.body.appendChild(area);
        area.select();
        ok = d.execCommand("copy");
        area.remove();
      }
    } catch {}
    toast(ok ? "Copied" : "Copy failed, select the text and copy it by hand", ok);
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
