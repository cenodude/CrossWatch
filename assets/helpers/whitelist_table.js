// assets/helpers/whitelist_table.js
// Shared whitelist table for media-server connection modals (Plex / Emby / Jellyfin).
(function (w, d) {
  if (w.cwWhitelistTable) return;

  const esc = (s) => String(s == null ? "" : s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));

  function relTime(ts) {
    if (!ts) return "never";
    const s = Math.max(0, Math.floor((Date.now() - ts) / 1000));
    if (s < 5) return "just now";
    if (s < 60) return `${s}s ago`;
    const m = Math.floor(s / 60);
    if (m < 60) return `${m} min ago`;
    const h = Math.floor(m / 60);
    if (h < 24) return `${h}h ago`;
    return `${Math.floor(h / 24)}d ago`;
  }

  const LIB_ICON = `<span class="material-symbols-rounded" aria-hidden="true">movie</span>`;
  const FEATURE_ICON = { hist: "history", rate: "star", prog: "progress_activity", scr: "graphic_eq" };

  function mount(opts) {
    const host = opts && opts.host;
    if (!host) return null;

    const features = opts.features || [];
    const getLibs = opts.getLibs || (() => []);
    const isOn = opts.isOn || (() => false);
    const setOn = opts.setOn || (() => {});
    const commit = opts.commit || (() => {});
    const load = opts.load || (async () => {});

    const libId = (l) => String(l.id != null ? l.id : l.key);
    const libTitle = (l) => String(l.title || l.name || libId(l));

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
      return features.map((f) => `<button type="button" class="cw-wl-colhead cw-wl-${f.key}" data-col="${f.key}"><span class="material-symbols-rounded cw-wl-cico" aria-hidden="true">${f.icon || FEATURE_ICON[f.key] || "check_circle"}</span><span>${esc(f.label)}</span></button>`).join("");
    }

    function rowsHTML(libs) {
      if (!libs.length) return `<div class="cw-wl-empty">No libraries loaded. Click “Load libraries”.</div>`;
      return libs.map((l) => {
        const id = libId(l);
        const checks = features.map((f) => {
          const on = isOn(f.key, id);
          return `<label class="cw-wl-check cw-wl-${f.key}"><input type="checkbox" data-feat="${f.key}" ${on ? "checked" : ""} aria-label="${esc(f.label)} — ${esc(libTitle(l))}"><span class="cw-wl-box"></span></label>`;
        }).join("");
        return `<div class="cw-wl-row" data-id="${esc(id)}" title="${esc(libTitle(l))} · #${esc(id)}">
          <span class="cw-wl-handle" aria-hidden="true"></span>
          <span class="cw-wl-ic">${LIB_ICON}</span>
          <span class="cw-wl-name">${esc(libTitle(l))}</span>
          <span class="cw-wl-checks">${checks}</span>
        </div>`;
      }).join("");
    }

    function stampText() {
      const ts = host.__cwWlLoadedAt || 0;
      return ts ? `Last loaded: ${relTime(ts)}` : "Not loaded yet";
    }

    function render() {
      const libs = getLibs();
      host.innerHTML = `
        <div class="cw-wl">
          <div class="cw-wl-scroll">
            <div class="cw-wl-colrow">
              <div class="cw-wl-collib">Library</div>
              <div class="cw-wl-cols">${colHeadHTML()}</div>
            </div>
            <div class="cw-wl-rows">${rowsHTML(libs)}</div>
          </div>
          <div class="cw-wl-foot">
            <div class="cw-wl-note">Empty = all libraries.</div>
            <div class="cw-wl-foot-r">
              <span class="cw-wl-stamp">${stampText()}</span>
              <button type="button" class="cw-wl-load" data-act="load"><span class="material-symbols-rounded" aria-hidden="true">sync</span>Load libraries</button>
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

    if (!host.__cwWlBound) {
      host.__cwWlBound = true;
      host.addEventListener("click", (ev) => {
        const act = ev.target.closest?.("[data-act]");
        if (act && act.dataset.act === "load") return void doLoad(true, true);
        const col = ev.target.closest?.(".cw-wl-colhead[data-col]");
        if (col) return toggleColumn(col.dataset.col);
      });
      host.addEventListener("change", (ev) => {
        const cb = ev.target.closest?.('input[type="checkbox"][data-feat]');
        if (!cb) return;
        const row = ev.target.closest?.(".cw-wl-row");
        const id = row && row.dataset.id;
        if (!id) return;
        setOn(cb.dataset.feat, id, cb.checked);
        commit();
      });
    }

    render();
    (async () => {
      if (!getLibs().length) await doLoad(false);
    })();

    return { render, load: doLoad };
  }

  w.cwWhitelistTable = { mount };
})(window, document);
