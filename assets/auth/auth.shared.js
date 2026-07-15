// assets/auth/auth.shared.js
// Shared frontend helpers for auth provider panels.
(function (w, d) {
  if (w.CW?.AuthShared) return;

  const el = (id) => d.getElementById(id);
  const txt = (v) => (typeof v === "string" ? v : (v == null ? "" : String(v))).trim();
  const notify = (m) => { try { if (typeof w.notify === "function") w.notify(m); } catch (_) {} };

  function isMaskedSecret(v) {
    const value = txt(v);
    return !!value && (
      value === "********" ||
      value === "**********" ||
      /^[*]{3,}$/.test(value) ||
      (value.length >= 3 && !/[A-Za-z0-9]/.test(value))
    );
  }

  function readSecretField(input) {
    const raw = txt(input && input.value);
    const masked = !!(input && (input.dataset.masked === "1" || isMaskedSecret(raw)));
    if (!raw && !masked) return { hasValue: false, masked: false, value: "" };
    if (masked) return { hasValue: true, masked: true, value: "" };
    return { hasValue: true, masked: false, value: raw };
  }

  function maskSecret(input, hasValue, opts) {
    if (!input) return;
    const mask = txt(opts?.mask) || "********";
    input.value = hasValue ? mask : "";
    input.dataset.masked = hasValue ? "1" : "0";
    input.dataset.loaded = "1";
    input.dataset.touched = "";
    input.dataset.clear = "";
    input.dataset.hasKey = hasValue ? "1" : "";
  }

  function markSecretField(input, value) {
    if (!input) return;
    const text = txt(value);
    input.value = text;
    input.dataset.masked = isMaskedSecret(text) ? "1" : "0";
    input.dataset.loaded = "1";
    if (!input.dataset.touched) input.dataset.touched = "";
  }

  function wireSecretInput(input, opts) {
    if (!input || input.__cwAuthSharedSecret) return;
    const clearOnFocus = opts?.clearOnFocus !== false;
    const clearMask = () => {
      if (input.dataset.masked === "1") {
        input.value = "";
        input.dataset.masked = "0";
        input.dataset.touched = "1";
        input.dataset.hasKey = "";
      }
    };
    if (clearOnFocus) input.addEventListener("focus", clearMask);
    input.addEventListener("beforeinput", clearMask);
    input.addEventListener("input", () => {
      input.dataset.masked = isMaskedSecret(input.value) ? "1" : "0";
      input.dataset.touched = "1";
      if (input.dataset.masked !== "1") input.dataset.hasKey = "";
      try { opts?.onInput?.(input); } catch (_) {}
    });
    input.__cwAuthSharedSecret = true;
  }

  async function fetchJSON(url, opts) {
    const r = await fetch(url, opts || {});
    let data = null;
    try { data = await r.json(); } catch (_) {}
    return { ok: r.ok, data, status: r.status };
  }

  const PROVIDER_LABELS = {
    plex: "Plex", emby: "Emby", jellyfin: "Jellyfin",
    trakt: "Trakt", simkl: "SIMKL", mdblist: "MDBList",
  };

  function providerLabel(provider) {
    const key = txt(provider).toLowerCase();
    return PROVIDER_LABELS[key] || (key ? key.toUpperCase() : "provider");
  }

  function describeUsage(usage) {
    const feature = txt(usage?.feature).toLowerCase();
    const role = txt(usage?.role).toLowerCase();
    if (feature === "watcher") {
      const routeId = txt(usage?.route_id);
      const where = routeId ? `Watcher route ${routeId}` : "a Watcher route";
      const side = role === "provider" ? "source" : "sink";
      return `${where} (${side})${usage?.enabled ? "" : " (disabled)"}`;
    }
    if (feature === "webhook") {
      const inst = txt(usage?.instance) || "default";
      const name = providerLabel(usage?.provider);
      const source = inst === "default" ? name : `${name} ${inst}`;
      return `${source} webhook (${role === "sink" ? "destination" : "source"})`;
    }
    return txt(usage?.label) || "an unknown configuration";
  }

  function isProviderInUse(res) {
    const data = res?.data ?? res;
    return res?.status === 409 || txt(data?.error) === "provider_in_use";
  }

  function providerUsageMessage(res) {
    const data = res?.data ?? res;
    if (!isProviderInUse(res)) return "";
    const usages = Array.isArray(data?.usages) ? data.usages : [];
    if (!usages.length) return txt(data?.message) || "This connection is still in use.";
    const inst = txt(data?.instance) || "default";
    const name = providerLabel(data?.provider);
    const subject = inst === "default" ? `${name} connection` : `${name} profile ${inst}`;
    const details = usages.map(describeUsage).join(", ");
    return `Cannot delete ${subject} because it is used by ${details}. Remove this profile from Watcher or Webhooks first.`;
  }

  const CONNECTION_WARN_VISIBLE_MS = 10000;
  const CONNECTION_WARN_FADE_MS = 700;

  function connectionWarnNodes() {
    const all = [...d.querySelectorAll(".cw-connection-footer-warn")];
    const visible = all.filter((node) => node.offsetParent !== null);
    return visible.length ? visible : all;
  }

  function clearConnectionWarning(node) {
    if (!node) return;
    clearTimeout(node.__cwWarnFade);
    clearTimeout(node.__cwWarnHide);
    node.__cwWarnFade = 0;
    node.__cwWarnHide = 0;
    node.classList.add("hidden");
    node.classList.remove("is-fading");
    node.textContent = "";
  }

  function clearConnectionWarnings() {
    d.querySelectorAll(".cw-connection-footer-warn").forEach(clearConnectionWarning);
  }

  function showConnectionWarning(message) {
    const text = txt(message);
    if (!text) return false;
    const nodes = connectionWarnNodes();
    if (!nodes.length) return false;
    nodes.forEach((node) => {
      clearTimeout(node.__cwWarnFade);
      clearTimeout(node.__cwWarnHide);
      node.innerHTML = `<span class="material-symbols-rounded" aria-hidden="true">warning</span><span class="cw-connection-footer-warn-text"></span>`;
      node.lastElementChild.textContent = text;
      node.classList.remove("hidden", "is-fading");
      node.__cwWarnFade = setTimeout(() => {
        node.classList.add("is-fading");
        node.__cwWarnHide = setTimeout(() => clearConnectionWarning(node), CONNECTION_WARN_FADE_MS);
      }, CONNECTION_WARN_VISIBLE_MS);
    });
    return true;
  }

  function reportProviderUsage(res) {
    const message = providerUsageMessage(res);
    if (!message) return false;
    if (!showConnectionWarning(message)) notify(message);
    return true;
  }

  async function getConfig() {
    if (w._cfgCache) return w._cfgCache;
    const r = await fetchJSON("/api/config?ts=" + Date.now(), { cache: "no-store" });
    return r.ok ? (r.data || {}) : {};
  }

  async function saveMergedConfig(mergeFn) {
    const fresh = await fetchJSON("/api/config?ts=" + Date.now(), { cache: "no-store" });
    const cfg = fresh.ok && fresh.data && typeof fresh.data === "object" ? fresh.data : {};
    try { mergeFn?.(cfg); } catch (_) {}
    const saved = await fetchJSON("/api/config", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(cfg),
      cache: "no-store",
    });
    if (!saved.ok) throw new Error(`POST /api/config ${saved.status || ""}`.trim());
    try { w._cfgCache = JSON.parse(JSON.stringify(cfg)); } catch (_) { w._cfgCache = cfg; }
    try { w.CW?.Cache?.setCfg?.(cfg); } catch (_) {}
    return cfg;
  }

  function setStatusPill(target, state, msg) {
    const node = typeof target === "string" ? el(target) : target;
    if (!node) return;
    node.classList.remove("hidden", "ok", "warn");

    const value = typeof state === "boolean" ? (state ? "ok" : "warn") : txt(state).toLowerCase();
    if (!value || value === "hidden" || value === "none" || value === "clear") {
      node.classList.add("hidden");
      node.textContent = "";
      return;
    }

    const ok = value === "ok" || value === "connected" || value === "success";
    node.classList.add(ok ? "ok" : "warn");
    node.textContent = msg || (ok ? "Connected" : "Not connected");
  }

  function setStatus(msgId, ok, msg) {
    setStatusPill(msgId, ok ? "ok" : "warn", msg);
  }

  function setConnectLocked(targets, locked, message) {
    const list = Array.isArray(targets) ? targets : [targets];
    const title = txt(message) || "Already connected, delete existing connection first.";
    list.forEach((target) => {
      const node = typeof target === "string" ? el(target) : target;
      if (!node) return;
      if (locked) {
        if (!Object.prototype.hasOwnProperty.call(node.dataset, "cwConnectLockPrevTitle")) {
          node.dataset.cwConnectLockPrevTitle = node.getAttribute("title") || "";
        }
        node.disabled = true;
        node.setAttribute("aria-disabled", "true");
        node.setAttribute("title", title);
        node.classList.add("cw-auth-connect-locked");
      } else {
        node.disabled = false;
        node.removeAttribute("aria-disabled");
        node.classList.remove("cw-auth-connect-locked");
        if (node.dataset.cwConnectLockPrevTitle !== undefined) {
          const previous = node.dataset.cwConnectLockPrevTitle;
          if (previous) node.setAttribute("title", previous);
          else node.removeAttribute("title");
          delete node.dataset.cwConnectLockPrevTitle;
        }
      }
    });
  }

  function mediaAuthGuide(root, opts) {
    if (!root) return null;
    let node = root.querySelector(".cw-auth-journey");
    if (!node) {
      node = d.createElement("div");
      node.className = "cw-auth-journey";
      const authPanel = root.querySelector('.cw-subpanel[data-sub="auth"]');
      if (authPanel) authPanel.insertBefore(node, authPanel.firstChild || null);
    }
    const label = txt(opts?.label) || "media server";
    const kind = txt(opts?.kind).toLowerCase();
    const title = txt(opts?.title) || `Connect ${label}`;
    const copy = txt(opts?.copy) || (
      kind === "plex"
        ? "Click Connect Plex, then use the link code shown here in the browser window. Once the account is connected, CrossWatch will move you to Settings to validate the server."
        : `Enter your ${label} server URL and sign in. After the connection succeeds, CrossWatch will move you to Settings so you can validate the server and pick the right user.`
    );
    const brand = kind === "plex"
      ? { c1: "229,160,13", c2: "229,160,13", logo: "PLEX", help: w.CW.HelpLinks.url("plex") }
      : kind === "emby"
        ? { c1: "82,181,75", c2: "82,181,75", logo: "EMBY", help: w.CW.HelpLinks.url("emby") }
        : { c1: "0,164,220", c2: "170,92,195", logo: "JELLYFIN", help: w.CW.HelpLinks.url("jellyfin") };
    node.style.setProperty("--cw-auth-c1", brand.c1);
    node.style.setProperty("--cw-auth-c2", brand.c2);
    node.style.setProperty("--cw-auth-logo", `url("/assets/img/${brand.logo}.svg")`);
    node.innerHTML = `
      <span class="material-symbols-rounded cw-auth-journey-icon" aria-hidden="true">link</span>
      <div class="cw-auth-journey-text">
        <div class="cw-auth-journey-title">${title}</div>
        <div class="cw-auth-journey-copy">${copy}</div>
      </div>
      <a class="cw-auth-journey-help" href="${brand.help}" target="_blank" rel="noopener noreferrer" aria-label="Open ${label} guide" title="Open guide"><span class="material-symbols-rounded" aria-hidden="true">help</span></a>`;
    return node;
  }

  function setMediaAuthStep(root, step) {
    if (!root) return;
    const order = ["auth", "settings", "whitelist"];
    const active = Math.max(0, order.indexOf(txt(step).toLowerCase()));
    root.querySelectorAll(".cw-auth-step").forEach((node, index) => {
      node.classList.toggle("active", index <= active);
    });
  }

  function applyMediaTabState(root, state) {
    if (!root) return;
    const settingsEnabled = !!state?.settingsEnabled;
    const whitelistEnabled = !!state?.whitelistEnabled;
    root.querySelectorAll(".cw-subtile[data-sub]").forEach((btn) => {
      const sub = txt(btn.dataset.sub).toLowerCase();
      const disabled = (sub === "settings" && !settingsEnabled) || (sub === "whitelist" && !whitelistEnabled);
      btn.disabled = disabled;
      btn.classList.toggle("is-disabled", disabled);
      if (disabled) btn.setAttribute("aria-disabled", "true");
      else btn.removeAttribute("aria-disabled");
      if (disabled) {
        btn.title = sub === "settings" ? "Connect or configure this profile first." : "Connect this profile before whitelisting libraries.";
      } else {
        btn.removeAttribute("title");
      }
    });
    root.querySelectorAll(".cw-subpanel[data-sub]").forEach((panel) => {
      const sub = txt(panel.dataset.sub).toLowerCase();
      const locked = (sub === "settings" && !settingsEnabled) || (sub === "whitelist" && !whitelistEnabled);
      panel.classList.toggle("is-locked", locked);
    });
  }

  function flashCopyButton(btn, text) {
    if (!btn) return;
    const old = btn.textContent;
    btn.textContent = text || "Copied";
    btn.classList.add("copied");
    clearTimeout(btn.__cwAuthCopyTimer);
    btn.__cwAuthCopyTimer = setTimeout(() => {
      btn.textContent = old || "Copy";
      btn.classList.remove("copied");
    }, 900);
  }

  async function copyText(value, btn, opts) {
    const text = txt(value);
    if (!text) {
      if (opts?.emptyMessage) notify(opts.emptyMessage);
      return false;
    }
    try {
      if (navigator.clipboard && w.isSecureContext) {
        await navigator.clipboard.writeText(text);
      } else {
        const ta = d.createElement("textarea");
        ta.value = text;
        ta.setAttribute("readonly", "");
        ta.style.position = "fixed";
        ta.style.top = "-9999px";
        d.body.appendChild(ta);
        ta.select();
        d.execCommand("copy");
        ta.remove();
      }
      flashCopyButton(btn, opts?.copiedText);
      if (opts?.successMessage) notify(opts.successMessage);
      return true;
    } catch (e) {
      if (opts?.failureMessage !== false) notify(opts?.failureMessage || "Copy failed");
      try { console.warn("Copy failed", e); } catch (_) {}
      return false;
    }
  }

  async function copyField(field, btn, opts) {
    const node = typeof field === "string" ? el(field) : field;
    const value = txt(node && ("value" in node ? node.value : node.textContent));
    if (!opts?.allowMasked && isMaskedSecret(value)) {
      if (opts?.emptyMessage) notify(opts.emptyMessage);
      return false;
    }
    return copyText(value, btn, opts);
  }

  function wireCopyButton(btn, field, opts) {
    const button = typeof btn === "string" ? el(btn) : btn;
    if (!button || button.__cwAuthCopyWired) return;
    button.__cwAuthCopyWired = true;
    button.addEventListener("click", (ev) => {
      ev.preventDefault();
      void copyField(field, button, opts);
    });
  }

  function createProfileAdapter(opts) {
    const provider = txt(opts.provider);
    const configKey = txt(opts.configKey || provider);
    const label = txt(opts.label || provider);
    const selectId = txt(opts.selectId || `${provider}_instance`);
    const storageKey = txt(opts.storageKey || `cw.ui.${provider}.auth.instance.v1`);
    const sectionId = txt(opts.sectionId || `sec-${provider}`);
    const panelSelector = opts.panelSelector || `#${sectionId} .cw-meta-provider-panel`;
    const apiProvider = txt(opts.instanceProvider || provider);
    const title = txt(opts.title || `Select which ${label} profile this config applies to.`);
    let uiObserver = null;

    function addDefaultOption(sel) {
      if (!sel || sel.options.length) return;
      const option = d.createElement("option");
      option.value = "default";
      option.textContent = "Default";
      sel.appendChild(option);
      sel.value = "default";
    }

    function getInstance() {
      const sel = el(selectId);
      let value = sel ? txt(sel.value) : "";
      if (!value || (value === "default" && sel && sel.options && sel.options.length <= 1)) {
        try { value = localStorage.getItem(storageKey) || value || ""; } catch (_) {}
      }
      value = txt(value) || "default";
      return value.toLowerCase() === "default" ? "default" : value;
    }

    function setInstance(value) {
      const id = txt(value) || "default";
      try { localStorage.setItem(storageKey, id); } catch (_) {}
      const sel = el(selectId);
      if (sel) sel.value = id;
    }

    function api(path) {
      const p = String(path || "");
      const sep = p.indexOf("?") >= 0 ? "&" : "?";
      return p + sep + "instance=" + encodeURIComponent(getInstance()) + "&ts=" + Date.now();
    }

    function cfgBlock(cfg, create) {
      cfg = cfg || {};
      let base = (cfg[configKey] && typeof cfg[configKey] === "object") ? cfg[configKey] : null;
      if (!base && create) base = cfg[configKey] = {};
      if (!base) return {};
      const inst = getInstance();
      if (inst === "default") return base;
      if (!base.instances || typeof base.instances !== "object") {
        if (!create) return {};
        base.instances = {};
      }
      if (!base.instances[inst] || typeof base.instances[inst] !== "object") {
        if (!create) return {};
        base.instances[inst] = {};
      }
      return base.instances[inst];
    }

    async function refreshOptions(preserve) {
      const sel = el(selectId);
      if (!sel) return;
      addDefaultOption(sel);
      let want = preserve === false ? "default" : getInstance();
      try {
        const r = await fetch(`/api/provider-instances/${encodeURIComponent(apiProvider)}?ts=${Date.now()}`, { cache: "no-store" });
        const arr = await r.json().catch(() => []);
        const opts = Array.isArray(arr) ? arr : [];
        sel.innerHTML = "";
        const addOpt = (id, text) => {
          const option = d.createElement("option");
          option.value = String(id);
          option.textContent = String(text || id);
          sel.appendChild(option);
        };
        addOpt("default", "Default");
        opts.forEach((item) => { if (item && item.id && item.id !== "default") addOpt(item.id, item.label || item.id); });
        if (!Array.from(sel.options).some((option) => option.value === want)) want = "default";
        sel.value = want;
        setInstance(want);
      } catch (_) {}
    }

    function ensureUI(onChange) {
      const panel = d.querySelector(panelSelector) || d.querySelector(`#${sectionId}`);
      const head = panel ? panel.querySelector(".cw-panel-head") : null;
      if (!head) {
        if (!uiObserver) {
          try {
            uiObserver = new MutationObserver(() => {
              const retryPanel = d.querySelector(panelSelector) || d.querySelector(`#${sectionId}`);
              const retryHead = retryPanel ? retryPanel.querySelector(".cw-panel-head") : null;
              if (!retryHead) return;
              try { uiObserver.disconnect(); } catch (_) {}
              uiObserver = null;
              ensureUI(onChange);
            });
            uiObserver.observe(d.documentElement || d.body, { childList: true, subtree: true });
          } catch (_) {}
        }
        return;
      }
      if (uiObserver) {
        try { uiObserver.disconnect(); } catch (_) {}
        uiObserver = null;
      }
      if (head.__cwProfileAdapter) return;
      head.__cwProfileAdapter = true;

      const wrap = d.createElement("div");
      wrap.className = "inline cw-profile-switcher";
      wrap.dataset.cwProfileProvider = apiProvider;
      wrap.dataset.cwProfileSelect = selectId;
      wrap.style.display = "flex";
      wrap.style.gap = "8px";
      wrap.style.alignItems = "center";
      wrap.style.marginLeft = "auto";
      wrap.style.flexWrap = "nowrap";
      wrap.title = title;

      const lab = d.createElement("span");
      lab.className = "muted";
      lab.dataset.cwProfileLabel = "true";
      lab.textContent = "Profile";

      const sel = d.createElement("select");
      sel.id = selectId;
      sel.name = selectId;
      sel.className = "input";
      sel.style.minWidth = "160px";
      sel.style.width = "auto";
      sel.style.maxWidth = "220px";
      sel.style.flex = "0 0 auto";
      addDefaultOption(sel);
      setInstance(getInstance());

      const btnNew = d.createElement("button");
      btnNew.type = "button";
      btnNew.className = "btn secondary cw-profile-new";
      btnNew.id = `${selectId}_new`;
      btnNew.textContent = "New";

      const btnDel = d.createElement("button");
      btnDel.type = "button";
      btnDel.className = "btn secondary cw-profile-delete";
      btnDel.id = `${selectId}_del`;
      btnDel.textContent = "Delete";

      wrap.append(lab, sel, btnNew, btnDel);
      head.appendChild(wrap);
      refreshOptions(true);

      sel.addEventListener("change", () => {
        setInstance(sel.value);
        try { Promise.resolve(onChange?.()).catch(() => {}); } catch (_) {}
      });
      btnNew.addEventListener("click", async () => {
        try {
          const r = await fetch(`/api/provider-instances/${encodeURIComponent(apiProvider)}/next?ts=${Date.now()}`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: "{}",
            cache: "no-store",
          });
          const j = await r.json().catch(() => ({}));
          const id = txt(j && j.id);
          if (!r.ok || (j && j.ok === false) || !id) throw new Error(String((j && j.error) || "create_failed"));
          setInstance(id);
          await refreshOptions(true);
          try { panel.dispatchEvent(new CustomEvent("cw-auth-profile-created", { bubbles: true, detail: { provider: apiProvider, instance: id } })); } catch (_) {}
          try { Promise.resolve(onChange?.()).catch(() => {}); } catch (_) {}
        } catch (e) {
          const message = e && e.message === "profile_limit_reached" ? "Maximum 10 profiles reached." : (e && e.message ? e.message : e);
          notify("Could not create profile: " + message);
        }
      });
      btnDel.addEventListener("click", async () => {
        const id = getInstance();
        if (id === "default") return notify("Default profile cannot be deleted.");
        if (!confirm(`Delete ${label} profile "${id}"?`)) return;
        try {
          const r = await fetchJSON(`/api/provider-instances/${encodeURIComponent(apiProvider)}/${encodeURIComponent(id)}`, { method: "DELETE", cache: "no-store" });
          if (reportProviderUsage(r)) return;
          const j = r.data || {};
          if (!r.ok || j.ok === false) throw new Error(String(j.error || "delete_failed"));
          setInstance("default");
          await refreshOptions(false);
          try { Promise.resolve(onChange?.()).catch(() => {}); } catch (_) {}
        } catch (e) {
          notify("Could not delete profile: " + (e && e.message ? e.message : e));
        }
      });
    }

    return { getInstance, setInstance, api, cfgBlock, refreshOptions, ensureUI };
  }

  (function wireQcAutoScroll() {
    const TOP_PAD = 42;
    const scrollContainer = (node) => {
      const panel = node?.closest?.(".cw-connection-modal-panel");
      const direct = panel?.querySelector(":scope > .auth-card, :scope > .cw-subpanels");
      if (direct) return direct;
      let p = node?.parentElement;
      while (p && p !== d.body) {
        const oy = w.getComputedStyle(p).overflowY;
        if ((oy === "auto" || oy === "scroll") && p.scrollHeight > p.clientHeight) return p;
        p = p.parentElement;
      }
      return null;
    };
    const revealScroll = (node, oldClass) => {
      if (!node || !node.id || !/_qc_state$/.test(node.id)) return;
      const wasHidden = /(^|\s)hidden(\s|$)/.test(oldClass || "");
      const isHidden = node.classList.contains("hidden");
      if (wasHidden === isHidden) return;
      const container = scrollContainer(node);
      if (!container) return;
      requestAnimationFrame(() => {
        try {
          if (!isHidden) {
            const delta = node.getBoundingClientRect().top - container.getBoundingClientRect().top - TOP_PAD;
            container.scrollTo({ top: Math.max(0, container.scrollTop + delta), behavior: "smooth" });
          } else {
            container.scrollTo({ top: 0, behavior: "smooth" });
          }
        } catch (_) {}
      });
    };
    const obs = new MutationObserver((records) => {
      for (const m of records) {
        if (m.type === "attributes" && m.attributeName === "class") revealScroll(m.target, m.oldValue);
      }
    });
    const attach = () => {
      const root = d.getElementById("auth-providers") || d.body;
      if (!root) return;
      try { obs.observe(root, { subtree: true, attributes: true, attributeFilter: ["class"], attributeOldValue: true }); } catch (_) {}
    };
    if (d.readyState === "loading") d.addEventListener("DOMContentLoaded", attach, { once: true });
    else attach();
  })();

  function createDevicePoll(opts) {
    opts = opts || {};
    const minIntervalMs = Math.max(1000, Number(opts.minIntervalMs) || 5000);
    const maxTotalMs = Math.max(minIntervalMs, Number(opts.maxTotalMs) || 300000);
    const backoffCapMs = Math.max(minIntervalMs, Number(opts.backoffCapMs) || 60000);
    const method = String(opts.method || "POST").toUpperCase();

    let gen = 0;
    let timer = null;
    let controller = null;
    let deadline = 0;
    let baseIntervalMs = minIntervalMs;
    let backoffStep = 0;
    let running = false;

    const clearTimer = () => { if (timer) { clearTimeout(timer); timer = null; } };
    const abort = () => { if (controller) { try { controller.abort(); } catch (_) {} controller = null; } };

    function stop() {
      gen++;
      running = false;
      clearTimer();
      abort();
      backoffStep = 0;
    }

    function parseRetryAfter(value) {
      if (!value) return 0;
      const s = String(value).trim();
      if (/^\d+$/.test(s)) return Math.max(0, parseInt(s, 10) * 1000);
      const when = Date.parse(s);
      return isNaN(when) ? 0 : Math.max(0, when - Date.now());
    }

    function backoffDelay() {
      const grown = baseIntervalMs * Math.pow(2, Math.max(0, backoffStep));
      return Math.min(backoffCapMs, grown) + Math.floor(Math.random() * 500);
    }

    function schedule(myGen, delay) {
      if (myGen !== gen) return;
      clearTimer();
      timer = setTimeout(() => tick(myGen), Math.max(minIntervalMs, Number(delay) || minIntervalMs));
    }

    async function tick(myGen) {
      if (myGen !== gen) return;
      if (Date.now() >= deadline) {
        stop();
        try { opts.onTimeout?.(); } catch (_) {}
        return;
      }
      if (opts.shouldPause && opts.shouldPause()) {
        schedule(myGen, minIntervalMs);
        return;
      }

      controller = new AbortController();
      let status = 0, data = null, retryAfterMs = 0, failed = false;
      try {
        const target = typeof opts.url === "function" ? opts.url() : String(opts.url || "");
        const init = { method, cache: "no-store", signal: controller.signal };
        if (method !== "GET" && method !== "HEAD") {
          init.headers = opts.headers || { "Content-Type": "application/json" };
          init.body = opts.body != null ? opts.body : "{}";
        } else if (opts.headers) {
          init.headers = opts.headers;
        }
        const r = await fetch(target, init);
        status = r.status;
        if (status === 429) retryAfterMs = parseRetryAfter(r.headers.get("Retry-After"));
        try { data = await r.json(); } catch (_) { data = null; }
      } catch (_) {
        failed = true;
      } finally {
        controller = null;
      }

      if (myGen !== gen) return;

      if (failed) { backoffStep++; schedule(myGen, backoffDelay()); return; }
      if (status === 429) { backoffStep++; schedule(myGen, Math.max(retryAfterMs, backoffDelay())); return; }
      if (status >= 500) { backoffStep++; schedule(myGen, backoffDelay()); return; }

      const verdict = (opts.classify && opts.classify(status, data || {})) || {};
      const state = verdict.state || "pending";
      if (verdict.intervalMs) baseIntervalMs = Math.max(minIntervalMs, Number(verdict.intervalMs));

      if (state === "authorized") { stop(); try { opts.onAuthorized?.(data || {}); } catch (_) {} return; }
      if (state === "expired") { stop(); try { opts.onExpired?.(data || {}); } catch (_) {} return; }
      if (state === "terminal") { stop(); try { opts.onTerminal?.(verdict, data || {}); } catch (_) {} return; }
      if (state === "slow_down" || state === "network" || state === "server") {
        backoffStep++;
        schedule(myGen, Math.max(retryAfterMs, backoffDelay()));
        return;
      }

      backoffStep = 0;
      try { opts.onPending?.(data || {}); } catch (_) {}
      schedule(myGen, baseIntervalMs);
    }

    function start(startOpts) {
      stop();
      const myGen = ++gen;
      running = true;
      backoffStep = 0;
      baseIntervalMs = Math.max(minIntervalMs, Number(startOpts?.intervalMs) || minIntervalMs);
      const now = Date.now();
      const cap = now + maxTotalMs;
      const provided = Number(startOpts?.deadlineMs) || 0;
      deadline = provided > now ? Math.min(cap, provided) : cap;
      schedule(myGen, baseIntervalMs);
    }

    return { start, stop, isRunning: () => running };
  }

  w.CW = w.CW || {};
  w.CW.AuthShared = {
    el,
    txt,
    notify,
    createDevicePoll,
    isMaskedSecret,
    readSecretField,
    maskSecret,
    markSecretField,
    wireSecretInput,
    fetchJSON,
    isProviderInUse,
    providerUsageMessage,
    reportProviderUsage,
    showConnectionWarning,
    clearConnectionWarnings,
    getConfig,
    saveMergedConfig,
    setStatusPill,
    setStatus,
    setConnectLocked,
    mediaAuthGuide,
    setMediaAuthStep,
    applyMediaTabState,
    copyText,
    copyField,
    wireCopyButton,
    createProfileAdapter,
  };
})(window, document);
