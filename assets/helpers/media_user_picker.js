/* CrossWatch - shared media user picker (Emby/Jellyfin/Plex) */
(() => {
  const w = window;
  const d = document;

  const HOST_ID = "cw_media_user_picker";
  const OVERLAY_ID = "cw_media_user_picker_overlay";
  const STYLE_ID = "cw_media_user_picker_style";

  function el(tag, props) {
    const n = d.createElement(tag);
    if (props) {
      for (const [k, v] of Object.entries(props)) {
        if (k === "className") n.className = String(v || "");
        else if (k === "textContent") n.textContent = String(v || "");
        else if (k === "innerHTML") n.innerHTML = String(v || "");
        else if (k.startsWith("on") && typeof v === "function") n.addEventListener(k.slice(2), v);
        else if (v !== undefined) n.setAttribute(k, String(v));
      }
    }
    return n;
  }

  function ensureStyle() {
    if (d.getElementById(STYLE_ID)) return;
    const s = el("style", { id: STYLE_ID });
    s.textContent = `
#${OVERLAY_ID}{position:fixed;inset:0;z-index:9998;background:rgba(0,0,0,.45)}
#${OVERLAY_ID}.hidden{display:none}
#${HOST_ID}{position:fixed;z-index:9999;width:min(380px,calc(100vw - 24px));max-height:min(460px,calc(100vh - 24px));border-radius:14px;background:var(--panel,#111);box-shadow:0 0 0 1px rgba(255,255,255,.08) inset,0 18px 50px rgba(0,0,0,.55);border:1px solid rgba(255,255,255,.10);overflow:hidden}
#${HOST_ID}.hidden{display:none}
#${HOST_ID} .head{display:flex;justify-content:space-between;align-items:center;gap:10px;padding:10px 12px;border-bottom:1px solid rgba(255,255,255,.06)}
#${HOST_ID} .title{font-weight:800}
#${HOST_ID} .body{padding:10px 12px;display:grid;gap:10px}
#${HOST_ID} .list{overflow:auto;border:1px solid rgba(255,255,255,.08);border-radius:12px;max-height:300px}
#${HOST_ID} .userrow{width:100%;text-align:left;background:transparent;border:0;color:inherit;padding:10px 10px;cursor:pointer}
#${HOST_ID} .userrow:hover{background:rgba(255,255,255,.05)}
#${HOST_ID} .row1{display:flex;justify-content:space-between;align-items:center;gap:8px}
#${HOST_ID} .sub{font-size:12px;opacity:.7;padding:10px}
#${HOST_ID} .tag{font-size:11px;padding:2px 8px;border-radius:999px;background:rgba(255,255,255,.08);border:1px solid rgba(255,255,255,.10);opacity:.85;white-space:nowrap}
#${HOST_ID} input{width:100%;padding:10px 12px;border-radius:10px;border:1px solid rgba(255,255,255,.10);background:rgba(255,255,255,.04);color:inherit;outline:none}
#${HOST_ID} .xbtn{background:transparent;border:0;color:inherit;opacity:.7;cursor:pointer;font-size:18px;line-height:1}
#${HOST_ID} .xbtn:hover{opacity:1}
`;
    d.head.appendChild(s);
  }

  function ensureHost() {
    ensureStyle();
    if (d.getElementById(HOST_ID)) return;

    const overlay = el("div", { id: OVERLAY_ID, className: "hidden" });
    overlay.addEventListener("click", () => api.close());

    const host = el("div", { id: HOST_ID, className: "hidden", role: "dialog", "aria-modal": "true" });
    host.appendChild(
      el("div", { className: "head" })
    );

    const head = host.querySelector(".head");
    head.appendChild(el("div", { className: "title", id: `${HOST_ID}_title`, textContent: "Pick user" }));
    head.appendChild(el("button", { className: "xbtn", type: "button", "aria-label": "Close", textContent: "×", onclick: () => api.close() }));

    const body = el("div", { className: "body" });
    body.appendChild(el("input", { id: `${HOST_ID}_filter`, placeholder: "Filter…", type: "text" }));
    body.appendChild(el("div", { className: "list", id: `${HOST_ID}_list` }));

    host.appendChild(body);
    d.body.appendChild(overlay);
    d.body.appendChild(host);

    d.addEventListener(
      "keydown",
      (e) => {
        if (e.key === "Escape") api.close();
      },
      true
    );

    const filter = d.getElementById(`${HOST_ID}_filter`);
    filter.addEventListener("input", () => render());
  }

  function canonicalInstanceId(v) {
    const s = String(v || "default").trim() || "default";
    const m = s.match(/^(.*?):(\d+)$/);
    return m ? (m[1] || "default") : s;
  }

  function nameFrom(u) {
    return String(u?.username || u?.Name || u?.name || u?.title || "").trim();
  }

  function idFrom(u) {
    return String(u?.id || u?.Id || u?.uuid || "").trim();
  }

  function tagFrom(u, provider) {
    const p = String(provider || "").toLowerCase();
    if (p === "plex") {
      const t = String(u?.type || "").toLowerCase().trim();
      if (t === "owner" || u?.owned === true) return "Owner";
      if (t === "managed" || u?.isHomeUser === true) return "Managed";
      return "";
    }
    const isAdmin = u?.IsAdministrator === true || u?.Policy?.IsAdministrator === true || u?.is_admin === true || u?.admin === true;
    return isAdmin ? "Admin" : "";
  }

  async function fetchUsers(provider, instance) {
    const p = String(provider || "").toLowerCase();
    const inst = canonicalInstanceId(instance);
    if (!p) return [];

    const url = `/api/${encodeURIComponent(p)}/users?instance=${encodeURIComponent(inst)}`;
    let r = null;
    let payload = null;
    let rawText = "";

    try {
      r = await fetch(url, { headers: { Accept: "application/json" }, cache: "no-store" });
    } catch (e) {
      throw new Error("Could not reach CrossWatch API. Is the app running?");
    }

    try {
      rawText = await r.text();
      payload = rawText ? JSON.parse(rawText) : null;
    } catch {
      payload = null;
    }

    if (!r.ok || payload?.ok === false) {
      const status = r.status || 0;
      let msg = String(payload?.error || payload?.detail || payload?.message || payload?.msg || "");

      if (!msg) {
        if (status === 401 || status === 403) msg = "Unauthorized. Please sign in first.";
        else if (status === 404) msg = "Endpoint not found.";
        else if (status === 502 || status === 503 || status === 504) msg = "Media server unreachable. Check Server URL, token, and network.";
        else if (status) msg = `HTTP ${status}`;
        else msg = "Request failed.";
      }

      throw new Error(msg);
    }

    const list = Array.isArray(payload) ? payload : Array.isArray(payload?.users) ? payload.users : [];
    return Array.isArray(list) ? list : [];
  }

  const STATE = {
    open: false,
    provider: "",
    instance: "default",
    anchorEl: null,
    title: "Pick user",
    onPick: null,
    all: [],
  };

  function place() {
    const host = d.getElementById(HOST_ID);
    if (!host) return;

    const pad = 12;
    const vw = w.innerWidth;
    const vh = w.innerHeight;

    let left = Math.max(pad, (vw - host.offsetWidth) / 2);
    let top = Math.max(pad, (vh - host.offsetHeight) / 2);

    const a = STATE.anchorEl;
    if (a && a.getBoundingClientRect) {
      const r = a.getBoundingClientRect();
      left = Math.min(Math.max(pad, r.left), vw - host.offsetWidth - pad);
      top = Math.min(Math.max(pad, r.bottom + 8), vh - host.offsetHeight - pad);
      if (top < pad) top = pad;
    }

    host.style.left = `${Math.round(left)}px`;
    host.style.top = `${Math.round(top)}px`;
  }

  function render() {
    const listEl = d.getElementById(`${HOST_ID}_list`);
    const filter = d.getElementById(`${HOST_ID}_filter`);
    if (!listEl || !filter) return;

    const q = String(filter.value || "").toLowerCase().trim();
    listEl.innerHTML = "";

    const items = Array.isArray(STATE.all) ? STATE.all : [];
    const mapped = items
      .map((raw) => ({ raw, name: nameFrom(raw), id: idFrom(raw), tag: tagFrom(raw, STATE.provider) }))
      .filter((x) => x.name && x.id)
      .filter((x) => !q || x.name.toLowerCase().includes(q) || x.id.toLowerCase().includes(q))
      .sort((a, b) => a.name.localeCompare(b.name, undefined, { sensitivity: "base" }));

    if (!mapped.length) {
      listEl.appendChild(el("div", { className: "sub", textContent: "No users found." }));
      return;
    }

    for (const u of mapped) {
      const btn = el("button", { className: "userrow", type: "button" });
      const row = el("div", { className: "row1" });
      row.appendChild(el("strong", { textContent: u.name }));
      if (u.tag) row.appendChild(el("span", { className: "tag", textContent: u.tag }));
      btn.appendChild(row);
      btn.addEventListener("click", () => {
        try {
          if (typeof STATE.onPick === "function") STATE.onPick({ id: u.id, name: u.name, raw: u.raw, provider: STATE.provider, instance: STATE.instance });
        } finally {
          api.close();
        }
      });
      listEl.appendChild(btn);
    }
  }

  async function open(opts) {
    ensureHost();

    STATE.provider = String(opts?.provider || "").toLowerCase();
    STATE.instance = canonicalInstanceId(opts?.instance || "default");
    STATE.anchorEl = opts?.anchorEl || null;
    STATE.title = String(opts?.title || "Pick user");
    STATE.onPick = typeof opts?.onPick === "function" ? opts.onPick : null;

    const overlay = d.getElementById(OVERLAY_ID);
    const host = d.getElementById(HOST_ID);
    const titleEl = d.getElementById(`${HOST_ID}_title`);
    const listEl = d.getElementById(`${HOST_ID}_list`);
    const filter = d.getElementById(`${HOST_ID}_filter`);

    if (!overlay || !host || !titleEl || !listEl || !filter) return;

    titleEl.textContent = STATE.title;
    filter.value = "";
    listEl.innerHTML = "";
    listEl.appendChild(el("div", { className: "sub", textContent: "Loading…" }));

    overlay.classList.remove("hidden");
    host.classList.remove("hidden");
    STATE.open = true;

    try {
      place();
      filter.focus();
    } catch {}

    try {
      STATE.all = await fetchUsers(STATE.provider, STATE.instance);
      render();
      try { place(); } catch {}
    } catch (e) {
      STATE.all = [];
      listEl.innerHTML = "";
      const msg = String(e?.message || e || "Couldn’t load users.");
      listEl.appendChild(el("div", { className: "sub", textContent: msg }));
      console.warn("[media_user_picker] users fetch failed:", e);
    }
  }

  function close() {
    const overlay = d.getElementById(OVERLAY_ID);
    const host = d.getElementById(HOST_ID);
    if (overlay) overlay.classList.add("hidden");
    if (host) host.classList.add("hidden");
    STATE.open = false;
    STATE.all = [];
    STATE.onPick = null;
  }

  const api = { open, close, canonicalInstanceId };
  w.cwMediaUserPicker = api;
})();
