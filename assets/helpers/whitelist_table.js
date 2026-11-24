// assets/helpers/whitelist_table.js
// Shared whitelist table for media-server connection modals (Plex / Emby / Jellyfin).
(function (w, d) {
  const T = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.t?.(key, vars) ?? key;
  const TH = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.html?.(key, vars) ?? key;
  if (w.cwWhitelistTable) return;

  const esc = (s) => String(s == null ? "" : s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));

  function relTime(ts) {
    if (!ts) return T("pairs.wl.never");
    const rel = (window.CW?.I18n || window.__cwI18n)?.relative;
    if (typeof rel === "function") return rel(ts);
    const s = Math.max(0, Math.floor((Date.now() - ts) / 1000));
    if (s < 5) return T("pairs.wl.justNow");
    if (s < 60) return `${s}s ago`;
    const m = Math.floor(s / 60);
    if (m < 60) return `${m} min ago`;
    const h = Math.floor(m / 60);
    if (h < 24) return `${h}h ago`;
    return `${Math.floor(h / 24)}d ago`;
  }

  const LIB_ICON = `<span class="material-symbols-rounded" aria-hidden="true">movie</span>`;
  const FEATURE_ICON = { hist: "history", rate: "star", prog: "progress_activity", coll: "inventory_2", scr: "graphic_eq" };

  function mount(opts) {
    const host = opts && opts.host;
    if (!host) return null;
    const owner = {};
    host.__cwWlOwner = owner;
    const active = () => host.__cwWlOwner === owner;

    const features = opts.features || [];
    const getLibs = opts.getLibs || (() => []);
    const isOn = opts.isOn || (() => false);
    const setOn = opts.setOn || (() => {});
    const selectedIds = opts.selectedIds || (() => []);
    const commit = opts.commit || (() => {});
    const load = opts.load || (async () => {});

    const libId = (l) => String(l.id != null ? l.id : l.key);
    const libTitle = (l) => String(l.title || l.name || libId(l));
    const missingTitle = (id) => T("pairs.libs.missing", {id: String(id)});

    function withMissing(libs) {
      if (!libs.length) return libs;
      const known = new Set(libs.map(libId));
      const missing = [];
      for (const raw of selectedIds() || []) {
        const id = String(raw == null ? "" : raw).trim();
        if (!id || known.has(id)) continue;
        known.add(id);
        missing.push({ id, title: missingTitle(id), missing: true });
      }
      return libs.concat(missing);
    }
    const featureCount = Math.max(1, features.length);

    if (host.__cwWlTimer) { clearInterval(host.__cwWlTimer); host.__cwWlTimer = 0; }

    function toggleColumn(fkey) {
      const libs = getLibs();
      if (!libs.length) return;
      const allOn = libs.every((l) => isOn(fkey, libId(l)));
      libs.forEach((l) => setOn(fkey, libId(l), !allOn));
      commit();
      render();
    }

    function colHeadHTML() {
      return features.map((f) => `<button type="button" class="cw-wl-colhead cw-wl-${f.key}" data-col="${f.key}"${f.title ? ` title="${esc(f.title)}"` : ""}><span class="material-symbols-rounded cw-wl-cico" aria-hidden="true">${f.icon || FEATURE_ICON[f.key] || "check_circle"}</span><span>${esc(f.label)}</span></button>`).join("");
    }

    function rowsHTML(libs) {
      if (!libs.length) return `<div class="cw-wl-empty">${TH("pairs.wl.noneLoaded")}</div>`;
      return libs.map((l) => {
        const id = libId(l);
        const checks = features.map((f) => {
          const on = isOn(f.key, id);
          return `<label class="cw-wl-check cw-wl-${f.key}"><input type="checkbox" data-feat="${f.key}" ${on ? "checked" : ""} aria-label="${esc(f.label)} — ${esc(libTitle(l))}"><span class="cw-wl-box"></span><span class="cw-wl-feature-label" aria-hidden="true">${esc(f.label)}</span></label>`;
        }).join("");
        const tip = l.missing ? `${libTitle(l)} · ${T("pairs.wl.gone")}` : `${libTitle(l)} · #${id}`;
        return `<div class="cw-wl-row" data-id="${esc(id)}"${l.missing ? ' data-missing="1"' : ""} title="${esc(tip)}">
          <span class="cw-wl-handle" aria-hidden="true"></span>
          <span class="cw-wl-ic">${LIB_ICON}</span>
          <span class="cw-wl-name">${esc(libTitle(l))}</span>
          <span class="cw-wl-checks">${checks}</span>
        </div>`;
      }).join("");
    }

    function stampText() {
      const ts = host.__cwWlLoadedAt || 0;
      return ts ? TH("pairs.wl.lastLoaded", {when: relTime(ts)}) : TH("pairs.wl.notLoaded");
    }

    function render() {
      if (!active()) return;
      const libs = getLibs();
      host.innerHTML = `
        <div class="cw-wl" style="--wl-feature-count:${featureCount}">
          <div class="cw-wl-scroll">
            <div class="cw-wl-colrow">
              <div class="cw-wl-collib"><span class="cw-wl-desktop-label">${TH("pairs.wl.library")}</span><span class="cw-wl-mobile-label">${TH("pairs.wl.toggleAll")}</span></div>
              <div class="cw-wl-cols">${colHeadHTML()}</div>
            </div>
            <div class="cw-wl-rows">${rowsHTML(withMissing(libs))}</div>
          </div>
          <div class="cw-wl-foot">
            <div class="cw-wl-note">${esc(opts.note || T("pairs.wl.empty"))}</div>
            <div class="cw-wl-foot-r">
              <span class="cw-wl-stamp">${stampText()}</span>
              <button type="button" class="cw-wl-load" data-act="load"><span class="material-symbols-rounded" aria-hidden="true">sync</span>${TH("pairs.cfg.loadLibraries")}</button>
            </div>
          </div>
        </div>`;
      const stampEl = host.querySelector(".cw-wl-stamp");
      host.__cwWlTimer = setInterval(() => {
        const el = host.querySelector(".cw-wl-stamp");
        if (!el) { clearInterval(host.__cwWlTimer); host.__cwWlTimer = 0; return; }
        el.textContent = stampText();
      }, 30000);
      void stampEl;
    }

    async function doLoad(force, flash) {
      const btn = host.querySelector(".cw-wl-load");
      if (btn) { btn.disabled = true; btn.classList.add("busy"); }
      let ok = true;
      try {
        await load(force);
        host.__cwWlLoadedAt = Date.now();
      } catch (_) { ok = false; }
      if (!active()) return;
      render();
      if (flash) {
        const nb = host.querySelector(".cw-wl-load");
        const ic = nb && nb.querySelector(".material-symbols-rounded");
        if (nb) {
          nb.classList.add(ok ? "is-done" : "is-fail");
          if (ic) ic.textContent = ok ? "check" : "error";
          setTimeout(() => {
            nb.classList.remove("is-done", "is-fail");
            const ic2 = nb.querySelector(".material-symbols-rounded");
            if (ic2) ic2.textContent = "sync";
          }, 1500);
        }
      }
    }

    if (host.__cwWlClickHandler) host.removeEventListener("click", host.__cwWlClickHandler);
    if (host.__cwWlChangeHandler) host.removeEventListener("change", host.__cwWlChangeHandler);
    host.__cwWlClickHandler = (ev) => {
      const act = ev.target.closest?.("[data-act]");
      if (act && act.dataset.act === "load") return void doLoad(true, true);
      const col = ev.target.closest?.(".cw-wl-colhead[data-col]");
      if (col) return toggleColumn(col.dataset.col);
    };
    host.__cwWlChangeHandler = (ev) => {
      const cb = ev.target.closest?.('input[type="checkbox"][data-feat]');
      if (!cb) return;
      const row = ev.target.closest?.(".cw-wl-row");
      const id = row && row.dataset.id;
      if (!id) return;
      setOn(cb.dataset.feat, id, cb.checked);
      commit();
      if (row.dataset.missing === "1" && !features.some((f) => isOn(f.key, id))) render();
    };
    host.addEventListener("click", host.__cwWlClickHandler);
    host.addEventListener("change", host.__cwWlChangeHandler);

    render();
    (async () => {
      if (!getLibs().length) await doLoad(false);
    })();

    return { render, load: doLoad };
  }

  w.cwWhitelistTable = { mount };
})(window, document);
