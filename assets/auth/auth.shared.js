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

  async function getConfig() {
    if (w._cfgCache) return w._cfgCache;
    const r = await fetchJSON("/api/config?ts=" + Date.now(), { cache: "no-store" });
    return r.ok ? (r.data || {}) : {};
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
      wrap.className = "inline";
      wrap.style.display = "flex";
      wrap.style.gap = "8px";
      wrap.style.alignItems = "center";
      wrap.style.marginLeft = "auto";
      wrap.style.flexWrap = "nowrap";
      wrap.title = title;

      const lab = d.createElement("span");
      lab.className = "muted";
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
      btnNew.className = "btn secondary";
      btnNew.id = `${selectId}_new`;
      btnNew.textContent = "New";

      const btnDel = d.createElement("button");
      btnDel.type = "button";
      btnDel.className = "btn secondary";
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
          try { Promise.resolve(onChange?.()).catch(() => {}); } catch (_) {}
        } catch (e) {
          notify("Could not create profile: " + (e && e.message ? e.message : e));
        }
      });
      btnDel.addEventListener("click", async () => {
        const id = getInstance();
        if (id === "default") return notify("Default profile cannot be deleted.");
        if (!confirm(`Delete ${label} profile "${id}"?`)) return;
        try {
          const r = await fetch(`/api/provider-instances/${encodeURIComponent(apiProvider)}/${encodeURIComponent(id)}`, { method: "DELETE", cache: "no-store" });
          const j = await r.json().catch(() => ({}));
          if (!r.ok || (j && j.ok === false)) throw new Error(String((j && j.error) || "delete_failed"));
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

  w.CW = w.CW || {};
  w.CW.AuthShared = {
    el,
    txt,
    notify,
    isMaskedSecret,
    readSecretField,
    maskSecret,
    markSecretField,
    wireSecretInput,
    fetchJSON,
    getConfig,
    setStatusPill,
    setStatus,
    copyText,
    copyField,
    wireCopyButton,
    createProfileAdapter,
  };
})(window, document);
