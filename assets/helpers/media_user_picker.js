/* CrossWatch - shared media user picker (Emby/Jellyfin/Plex) */
(() => {
  const w = window;
  const d = document;

  const HOST_ID = "cw_media_user_picker";
  const OVERLAY_ID = "cw_media_user_picker_overlay";

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

  function ensureHost() {
    if (d.getElementById(HOST_ID)) return;

    const overlay = el("div", { id: OVERLAY_ID, className: "hidden" });
    overlay.addEventListener("click", () => api.close());

    const host = el("div", { id: HOST_ID, className: "hidden", role: "dialog", "aria-modal": "true" });
    host.appendChild(
      el("div", { className: "head" })
    );

    const head = host.querySelector(".head");
    head.appendChild(el("div", { className: "title", id: `${HOST_ID}_title`, textContent: "Pick user" }));

    const body = el("div", { className: "body" });
    const pophead = el("div", { className: "pophead" });
    pophead.appendChild(el("input", { id: `${HOST_ID}_filter`, placeholder: "Filter users...", type: "text" }));
    pophead.appendChild(el("button", { className: "xbtn", type: "button", textContent: "Close", onclick: () => api.close() }));
    body.appendChild(pophead);
    body.appendChild(el("div", { className: "list", id: `${HOST_ID}_list` }));

    host.appendChild(body);
    d.body.appendChild(overlay);
    d.body.appendChild(host);

    d.addEventListener(
      "keydown",
      (e) => {
        if (e.key === "Escape" && STATE.open) {
          e.preventDefault();
          e.stopPropagation();
          e.stopImmediatePropagation?.();
          api.close();
        }
      },
      true
    );

    const filter = d.getElementById(`${HOST_ID}_filter`);
    filter.addEventListener("input", () => render());

    d.addEventListener(
      "click",
      (e) => {
        if (!STATE.open || STATE.overlay) return;
        const currentHost = d.getElementById(HOST_ID);
        if (currentHost?.contains(e.target)) return;
        if (STATE.anchorEl?.contains?.(e.target)) return;
        api.close();
      },
      true
    );
  }

  function canonicalInstanceId(v) {
    const s = String(v || "default").trim() || "default";
    const m = s.match(/^(.*?):(\d+)$/);
    return m ? (m[1] || "default") : s;
  }

  function nameFrom(u) {
    return String(u?.username || u?.Name || u?.name || u?.title || u?.UserName || u?.user?.Name || "").trim();
  }

  function idFrom(u) {
    return String(u?.id || u?.Id || u?.uuid || u?.account_id || u?.cloud_account_id || u?.pms_account_id || u?.user_id || u?.UserId || u?.user?.Id || nameFrom(u)).trim();
  }

  function tagFrom(u, provider) {
    const p = String(provider || "").toLowerCase();
    if (p === "plex") {
      const label = String(u?.label || "").trim();
      if (label) return label;
      const t = String(u?.type || "").toLowerCase().trim();
      if (t === "owner" || t === "self" || u?.owned === true) return "Owner";
      if (t === "managed" || u?.isHomeUser === true) return "Home";
      if (t === "friend") return "Friend";
      return "";
    }
    const isAdmin = u?.IsAdministrator === true || u?.Policy?.IsAdministrator === true || u?.is_admin === true || u?.admin === true;
    return isAdmin ? "Admin" : "";
  }

  function overrideQS() {
    let qs = "";
    if (STATE.server) qs += `&server=${encodeURIComponent(STATE.server)}`;
    if (STATE.verifySsl != null) qs += `&verify_ssl=${STATE.verifySsl ? 1 : 0}`;
    return qs;
  }

  async function fetchUsers(provider, instance) {
    const p = String(provider || "").toLowerCase();
    const inst = canonicalInstanceId(instance);
    if (!p) return [];

    const extra = overrideQS();
    const urls = p === "plex"
      ? [
          `/api/plex/pickusers?instance=${encodeURIComponent(inst)}${extra}`,
          `/api/plex/users?instance=${encodeURIComponent(inst)}${extra}`,
        ]
      : [`/api/${encodeURIComponent(p)}/users?instance=${encodeURIComponent(inst)}${extra}`];

    let lastError = null;
    for (const url of urls) {
      try {
        const users = await fetchUsersUrl(url);
        if (users.length || p !== "plex" || url.includes("/pickusers")) return users;
      } catch (e) {
        lastError = e;
      }
    }

    if (lastError) throw lastError;
    return [];
  }

  async function fetchUsersUrl(url) {
    let r = null;
    let payload = null;
    let rawText = "";

    try {
      r = await fetch(url, { headers: { Accept: "application/json" }, cache: "no-store", credentials: "same-origin" });
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
    overlay: true,
    seq: 0,
    server: "",
    verifySsl: null,
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
      left = Math.min(Math.max(pad, r.right - host.offsetWidth), vw - host.offsetWidth - pad);
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
      if (u.tag) row.appendChild(el("span", { className: `tag ${String(u.tag || "").toLowerCase()}`, textContent: u.tag }));
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
    STATE.overlay = opts?.overlay !== false;
    STATE.server = String(opts?.server || "").trim();
    STATE.verifySsl = (opts && "verifySsl" in opts) ? opts.verifySsl : null;
    const seq = ++STATE.seq;

    const overlay = d.getElementById(OVERLAY_ID);
    const host = d.getElementById(HOST_ID);
    const titleEl = d.getElementById(`${HOST_ID}_title`);
    const listEl = d.getElementById(`${HOST_ID}_list`);
    const filter = d.getElementById(`${HOST_ID}_filter`);

    if (!overlay || !host || !titleEl || !listEl || !filter) return;

    titleEl.textContent = STATE.title;
    filter.value = "";
    listEl.innerHTML = "";
    listEl.appendChild(el("div", { className: "sub", textContent: "Loading..." }));

    overlay.classList.toggle("hidden", !STATE.overlay);
    host.classList.remove("hidden");
    STATE.open = true;

    try {
      place();
      filter.focus();
    } catch {}

    try {
      STATE.all = await fetchUsers(STATE.provider, STATE.instance);
      if (seq !== STATE.seq || !STATE.open) return;
      render();
      try { place(); } catch {}
    } catch (e) {
      STATE.all = [];
      if (seq !== STATE.seq || !STATE.open) return;
      listEl.innerHTML = "";
      const msg = String(e?.message || e || "Couldn't load users.");
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
    STATE.anchorEl = null;
    STATE.seq++;
  }

  const api = { open, close, canonicalInstanceId };
  w.cwMediaUserPicker = api;
})();
