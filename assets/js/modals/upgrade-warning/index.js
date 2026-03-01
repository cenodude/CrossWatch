// assets/js/modals/upgrade-warning/index.js
const NOTES_ENDPOINT = "/api/update";
function _norm(v) {
  return String(v || "").replace(/^v/i, "").trim();
}

function _cmp(a, b) {
  const pa = _norm(a).split(".").map((n) => parseInt(n, 10) || 0);
  const pb = _norm(b).split(".").map((n) => parseInt(n, 10) || 0);
  for (let i = 0; i < Math.max(pa.length, pb.length); i++) {
    const da = pa[i] || 0;
    const db = pb[i] || 0;
    if (da !== db) return da > db ? 1 : -1;
  }
  return 0;
}

function _escapeHtml(s) {
  return String(s || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function _sanitizeHtml(html) {
  try {
    const tpl = document.createElement("template");
    tpl.innerHTML = String(html || "");
    const blocked = new Set(["SCRIPT", "STYLE", "IFRAME", "OBJECT", "EMBED", "LINK", "META"]);
    const kill = [];

    const walker = document.createTreeWalker(tpl.content, NodeFilter.SHOW_ELEMENT);
    while (walker.nextNode()) {
      const el = walker.currentNode;
      if (blocked.has(el.tagName)) kill.push(el);

      // Strip unsafe attributes
      for (const attr of Array.from(el.attributes || [])) {
        const k = String(attr.name || "").toLowerCase();
        const v = String(attr.value || "");
        if (k.startsWith("on")) el.removeAttribute(attr.name);
        if ((k === "href" || k === "src") && /^\s*javascript:/i.test(v)) el.removeAttribute(attr.name);
      }

      // Force safe link behavior
      if (el.tagName === "A") {
        el.setAttribute("target", "_blank");
        el.setAttribute("rel", "noopener noreferrer");
      }
    }

    for (const el of kill) el.remove();
    return tpl.innerHTML;
  } catch {
    return String(html || "");
  }
}

function _mdInline(s) {
  let x = String(s || "");

  // Code spans, links, bold, italics
  x = x.replace(/`([^`]+)`/g, (_, c) => `<code>${_escapeHtml(c)}</code>`);
  x = x.replace(/\[([^\]]+)\]\(([^)]+)\)/g, (_, t, href) => `<a href="${href}">${t}</a>`);
  x = x.replace(/\*\*([^*]+)\*\*/g, "<b>$1</b>");
  x = x.replace(/(^|[^*])\*([^*\n]+)\*(?!\*)/g, "$1<i>$2</i>");

  return x;
}

function _mdToHtml(md) {
  const src = String(md || "").replace(/\r\n/g, "\n");
  const blocks = [];
  let tmp = src.replace(/```([\s\S]*?)```/g, (_, inner) => {
    let code = String(inner || "");
    let lang = "";
    const lines = code.split("\n");
    const first = (lines[0] || "").trim();
    if (first && /^[a-z0-9_-]+$/i.test(first) && lines.length > 1) {
      lang = first;
      lines.shift();
      code = lines.join("\n");
    }
    const html = `<pre><code${lang ? ` class="lang-${lang}"` : ""}>${_escapeHtml(code.replace(/\n$/, ""))}</code></pre>`;
    const id = blocks.length;
    blocks.push(html);
    return `@@CW_CODE_${id}@@`;
  });

  const out = [];
  const lines = tmp.split("\n");
  let i = 0;
  let inUl = false;
  let inOl = false;

  const closeLists = () => {
    if (inUl) out.push("</ul>");
    if (inOl) out.push("</ol>");
    inUl = false;
    inOl = false;
  };

  while (i < lines.length) {
    const line = lines[i];
    const t = line.trim();

    if (!t) {
      closeLists();
      i += 1;
      continue;
    }

    // Headings
    const hm = line.match(/^(#{1,6})\s+(.*)$/);
    if (hm) {
      closeLists();
      const lvl = hm[1].length;
      out.push(`<h${lvl}>${_mdInline(hm[2])}</h${lvl}>`);
      i += 1;
      continue;
    }

    // Unordered list
    const ulm = line.match(/^\s*[-*]\s+(.*)$/);
    if (ulm) {
      if (!inUl) {
        closeLists();
        out.push("<ul>");
        inUl = true;
      }
      out.push(`<li>${_mdInline(ulm[1])}</li>`);
      i += 1;
      continue;
    }

    // Ordered list
    const olm = line.match(/^\s*\d+\.\s+(.*)$/);
    if (olm) {
      if (!inOl) {
        closeLists();
        out.push("<ol>");
        inOl = true;
      }
      out.push(`<li>${_mdInline(olm[1])}</li>`);
      i += 1;
      continue;
    }

    // Paragraph
    closeLists();
    const buf = [t];
    i += 1;
    while (i < lines.length) {
      const n = lines[i];
      const nt = n.trim();
      if (!nt) break;
      if (/^(#{1,6})\s+/.test(n)) break;
      if (/^\s*[-*]\s+/.test(n)) break;
      if (/^\s*\d+\.\s+/.test(n)) break;
      buf.push(nt);
      i += 1;
    }
    out.push(`<p>${_mdInline(buf.join(" "))}</p>`);
  }

  closeLists();

  let html = out.join("\n");
  html = html.replace(/@@CW_CODE_(\d+)@@/g, (_, n) => blocks[Number(n)] || "");
  return html;
}


async function _getJson(url, opts = {}) {
  const res = await fetch(url, { method: "GET", ...opts });
  let data = null;
  try {
    data = await res.json();
  } catch {}
  if (!res.ok) throw new Error(`${url}: HTTP ${res.status} ${res.statusText}`);
  return data || {};
}

async function _postJson(url, opts = {}) {
  const res = await fetch(url, { method: "POST", ...opts });
  let data = null;
  try {
    data = await res.json();
  } catch {}
  if (!res.ok || (data && data.ok === false)) {
    const msg = (data && (data.error || data.message)) || `HTTP ${res.status} ${res.statusText}`;
    throw new Error(`${url}: ${msg}`);
  }
  return data;
}

async function _saveConfigNoUi() {
  return _postJson("/api/config", {
    headers: { "Content-Type": "application/json" },
    body: "{}"
  });
}


async function _pauseSchedulerOnce() {
  // Stop scheduler once when migration is required (<0.9.11) to avoid running on mixed ID systems.
  const notify = window.notify || ((m) => console.log("[notify]", m));
  const KEY = "cw_stop_scheduler_pre_0911";

  try {
    if (window.__CW_STOP_SCHED_0911_DONE__) return;
    if (window.__CW_STOP_SCHED_0911_INFLIGHT__) return;
  } catch {}

  try {
    if (localStorage.getItem(KEY) === "1") {
      try { window.__CW_STOP_SCHED_0911_DONE__ = true; } catch {}
      return;
    }
  } catch {}

  try { window.__CW_STOP_SCHED_0911_INFLIGHT__ = true; } catch {}

  try {
    await _postJson("/api/scheduling/stop");
    notify("Scheduler stopped until you complete migration.");
    try {
      localStorage.setItem(KEY, "1");
      window.__CW_STOP_SCHED_0911_DONE__ = true;
    } catch {}
  } catch (e) {
    console.warn("[upgrade-warning] scheduler stop failed", e);
  } finally {
    try { window.__CW_STOP_SCHED_0911_INFLIGHT__ = false; } catch {}
  }
}


async function saveNow(btn) {
  const notify = window.notify || ((m) => console.log("[notify]", m));
  try {
    if (btn && btn.dataset && btn.dataset.done === "1") return;
  } catch {}
  try {
    if (btn) {
      btn.disabled = true;
      btn.classList.add("busy");
      btn.textContent = "Migrating...";
    }
  } catch {}

  try {
    await _saveConfigNoUi();
    notify("Migrated. After updates: hard refresh (Ctrl+F5) so the UI loads the new assets.");

    try {
      if (btn) {
        btn.classList.remove("busy");
        btn.textContent = "MIGRATED";
        btn.disabled = true;
        btn.dataset.done = "1";
      }
    } catch {}

    try {
      window.cxCloseModal?.();
    } catch {}
  } catch (e) {
    console.warn("[upgrade-warning] save failed", e);
    notify("Save failed. Check logs.");
  } finally {
    try {
      if (btn) {
        if (!btn.dataset || btn.dataset.done !== "1") {
          btn.disabled = false;
          btn.classList.remove("busy");
          btn.textContent = "MIGRATE";
        }
      }
    } catch {}
  }
}

async function migrateNow(btn, fullClean = false) {
  const notify = window.notify || ((m) => console.log("[notify]", m));
  try {
    if (btn) {
      btn.disabled = true;
      btn.classList.add("busy");
      btn.textContent = "Migrating...";
    }
  } catch {}

  try {
    const ops = [
      { url: "/api/maintenance/clear-state", opts: {} },
      { url: "/api/maintenance/clear-cache", opts: {} },
      { url: "/api/maintenance/clear-metadata-cache", opts: {} },
    ];

    if (fullClean) {
      ops.push(
        {
          url: "/api/maintenance/crosswatch-tracker/clear",
          opts: {
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              clear_state: true,
              clear_snapshots: true,
            }),
          },
        },
        {
          url: "/api/maintenance/reset-stats",
          opts: {
            headers: { "Content-Type": "application/json" },
            body: "{}",
          },
        },
        { url: "/api/maintenance/reset-currently-watching", opts: {} },
      );
    }

    for (const op of ops) {
      await _postJson(op.url, op.opts);
    }

    await _saveConfigNoUi();

    notify(fullClean
      ? "Migration completed. Legacy state/cache cleared and config saved."
      : "Migration completed. State/cache cleared and config saved.");

    try {
      if (btn) {
        btn.classList.remove("busy");
        btn.textContent = "MIGRATED";
        btn.disabled = true;
        btn.dataset.done = "1";
      }
    } catch {}
  } catch (e) {
    console.warn("[upgrade-warning] migrate failed", e);
    notify("Migration failed. Check logs.");

    try {
      if (btn) {
        btn.disabled = false;
        btn.classList.remove("busy");
        btn.textContent = "MIGRATE";
      }
    } catch {}
  }
}



export default {
  async mount(hostEl, props = {}) {
    if (!hostEl) return;

    const cur = _norm(props.current_version || window.__CW_VERSION__ || "0.0.0");

    const rawCfgVer = props.config_version;
    const hasCfgVer = rawCfgVer != null && String(rawCfgVer).trim() !== "";
    const cfg = hasCfgVer ? _norm(rawCfgVer) : "";

    // Legacy if config has no version, or version < 0.7.0
    const legacy = !hasCfgVer || _cmp(cfg, "0.7.0") < 0;

    // v0.9.11 introduced IMDb -> TMDb primary ID change. Anything before that needs a full cleanup.
    const needs0911Cleanup = !hasCfgVer || _cmp(cfg, "0.9.11") < 0;

    if (needs0911Cleanup) {
      // Stop scheduler early for safety until migration is completed.
      _pauseSchedulerOnce();
    }

    hostEl.innerHTML = `
        <style>
      #upg-host{--w:820px;position:relative;overflow:hidden;min-width:min(var(--w),94vw);max-width:94vw;color:#eaf0ff;border-radius:18px;
        border:1px solid rgba(255,255,255,.08);
        background:
          radial-gradient(900px circle at 18% 18%, rgba(150,70,255,.22), transparent 55%),
          radial-gradient(900px circle at 92% 10%, rgba(60,140,255,.18), transparent 55%),
          radial-gradient(800px circle at 55% 110%, rgba(60,255,215,.08), transparent 60%),
          rgba(7,8,11,.92);
        box-shadow:0 30px 90px rgba(0,0,0,.70), inset 0 1px 0 rgba(255,255,255,.04);
        backdrop-filter:saturate(135%) blur(10px)
      }
      #upg-host:before{content:"";position:absolute;inset:-120px;pointer-events:none;
        background:conic-gradient(from 180deg at 50% 50%, rgba(150,70,255,.0), rgba(150,70,255,.30), rgba(60,140,255,.24), rgba(60,255,215,.10), rgba(150,70,255,.0));
        filter:blur(90px);opacity:.35;transform:translate3d(0,0,0);
        animation:upgGlow 16s ease-in-out infinite alternate
      }
      @keyframes upgGlow{from{transform:translate(-16px,-10px) scale(1)}to{transform:translate(16px,12px) scale(1.03)}}
      @media (prefers-reduced-motion: reduce){#upg-host:before{animation:none}}

      #upg-host .head{position:relative;display:flex;align-items:center;gap:12px;padding:14px 16px;border-bottom:1px solid rgba(255,255,255,.08);
        background:linear-gradient(180deg,rgba(255,255,255,.03),rgba(255,255,255,.01))
      }
      #upg-host .icon{width:44px;height:44px;border-radius:14px;display:grid;place-items:center;
        background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.08);
        box-shadow:0 12px 30px rgba(0,0,0,.40), inset 0 1px 0 rgba(255,255,255,.04)
      }
      #upg-host .icon span{font-size:26px;opacity:.95;filter:drop-shadow(0 10px 16px rgba(0,0,0,.45))}
      #upg-host .t{font-weight:950;letter-spacing:.2px;font-size:15px;line-height:1.1;text-transform:uppercase;opacity:.90}
      #upg-host .sub{opacity:.72;font-size:12px;margin-top:2px}
      #upg-host .pill{margin-left:auto;display:flex;gap:8px;align-items:center;font-weight:900;font-size:12px;opacity:.85}
      #upg-host .pill .b{padding:6px 10px;border-radius:999px;background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.08)}

      #upg-host .body{position:relative;padding:16px 16px 8px 16px;max-height:72vh;overflow:auto}
      #upg-host .card{display:block;padding:12px 12px;border-radius:14px;
        background:rgba(255,255,255,.03);
        border:1px solid rgba(255,255,255,.08);
        box-shadow:0 10px 30px rgba(0,0,0,.32);
        margin-bottom:10px
      }
      #upg-host .card .h{font-weight:950}
      #upg-host .card .p{opacity:.84;margin-top:6px;line-height:1.45}
      #upg-host .warn{border-color:rgba(255,120,120,.22);background:linear-gradient(180deg,rgba(255,77,79,.12),rgba(255,77,79,.05))}
      #upg-host ul{margin:.6em 0 0 1.15em}
      #upg-host code{opacity:.95}
      #upg-host .notes{margin-top:8px;overflow:auto;max-height:340px;
        padding:12px 12px;border-radius:12px;background:rgba(255,255,255,.03);border:1px solid rgba(255,255,255,.08);opacity:.92}
      #upg-host .notes.md{white-space:normal;font:13px/1.55 system-ui,-apple-system,Segoe UI,Roboto,Ubuntu,Cantarell,"Helvetica Neue",Arial}
      #upg-host .notes.md h1{font-size:16px;margin:0 0 10px 0}
      #upg-host .notes.md h2{font-size:14px;margin:14px 0 8px 0}
      #upg-host .notes.md h3{font-size:13px;margin:12px 0 6px 0}
      #upg-host .notes.md p{margin:0 0 10px 0}
      #upg-host .notes.md ul,#upg-host .notes.md ol{margin:0 0 10px 1.25em}
      #upg-host .notes.md li{margin:4px 0}
      #upg-host .notes.md code{font:12px/1.45 ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,"Liberation Mono","Courier New",monospace;
        background:rgba(255,255,255,.05);border:1px solid rgba(255,255,255,.08);padding:1px 6px;border-radius:8px}
      #upg-host .notes.md pre{margin:10px 0;padding:10px 10px;border-radius:12px;overflow:auto;white-space:pre;
        background:rgba(0,0,0,.28);border:1px solid rgba(255,255,255,.10)}
      #upg-host .notes.md pre code{background:transparent;border:0;padding:0}
      #upg-host .notes.md a{color:inherit;text-decoration:underline;opacity:.9}
      #upg-host .notes.md img{max-width:100%;height:auto;border-radius:12px;display:block;margin:10px 0;opacity:.96}
      #upg-host .btn{appearance:none;border:1px solid rgba(255,255,255,.12);border-radius:14px;padding:10px 14px;font-weight:950;cursor:pointer;
        background:rgba(255,255,255,.04);color:#eaf0ff
      }
      #upg-host .btn:hover{filter:brightness(1.06)}
      #upg-host .btn.primary{border-color:rgba(150,70,255,.35);
        background:linear-gradient(135deg,rgba(150,70,255,.92),rgba(60,140,255,.82));
        box-shadow:0 16px 50px rgba(0,0,0,.48)
      }
      #upg-host .btn.primary:active{transform:translateY(1px)}
      #upg-host .btn.ghost{background:rgba(255,255,255,.04);border-color:rgba(255,255,255,.10);box-shadow:none}
      #upg-host .btn.danger{border-color:rgba(255,120,120,.28);background:linear-gradient(135deg,rgba(255,77,79,.92),rgba(255,122,122,.82));color:#fff;box-shadow:0 16px 50px rgba(0,0,0,.48)}
      #upg-host .btn.busy{opacity:.82;cursor:progress}

      #upg-host .foot{position:relative;display:flex;justify-content:flex-end;gap:10px;padding:12px 16px;border-top:1px solid rgba(255,255,255,.08);
        background:linear-gradient(180deg,rgba(255,255,255,.02),rgba(255,255,255,.01))
      }
    </style>

    <div id="upg-host">
      <div class="head">
        <div class="icon" aria-hidden="true"><span class="material-symbols-rounded">system_update</span></div>
        <div>
          <div class="t">${needs0911Cleanup ? "Migration required" : (legacy ? "Legacy config detected" : "Config version notice")}</div>
          <div class="sub">${needs0911Cleanup ? "Pre-v0.9.11 data cleanup" : (legacy ? "This release introduced config versioning (0.7.0+)." : "Migrate to new save format.")}</div>
        </div>
        <div class="pill">
          <span class="b">Engine v${cur}</span>
          ${legacy ? `<span class="b">Config: Legacy</span>` : `<span class="b">Config v${cfg}</span>`}
        </div>
      </div>

      <div class="body">
        ${needs0911Cleanup ? `
        <div class="card warn">
          <div class="h">IMPORTANT</div>
          <div class="p">Starting with <b>v0.9.11</b>, we switched the primary ID from <b>IMDb</b> to <b>TMDb</b>. This change affects all existing states and caches created before <b>v0.9.11</b>. Click <b>MIGRATE</b> to remove the old IMDb-based state/cache data.</div>
        </div>
        ${legacy ? `
        <div class="card warn">
          <div class="h">IMPORTANT</div>
          <div class="p">CrossWatch now clearly separates <b>global orchestration state</b> from <b>pair-specific provider caches</b>.</div>
          <ul>
            <li>Multiple pairs can run without overwriting each other’s cached snapshots/watermarks.</li>
            <li>Providers can safely reuse cached “present” indexes (when activities timestamps match) without risking cross-pair contamination.</li>
          </ul>
          <div class="p" style="margin-top:8px">For a smooth transition, the current caches need to be removed/migrated.</div>
        </div>

        <div class="card">
          <div class="h">What to do</div>
          <div class="p">Click <b>MIGRATE</b> below. It clears state/cache, then saves your config so it gets the new <code>version</code> field.</div>
        </div>

        <div class="card">
          <div class="h">Tip</div>
          <div class="p">After each CrossWatch update, hard refresh your browser (Ctrl+F5) so the UI loads the new assets.</div>
        </div>
        ` : ``}

        <div class="card">
          <div class="h">What to do</div>
          <div class="p">Click <b>MIGRATE</b> below. It runs <b>Clean Everything</b> (state, caches, tracker, stats, currently watching) and saves your config.</div>
        </div>

        <div class="card">
          <div class="h">Tip</div>
          <div class="p">After each CrossWatch update, hard refresh your browser (Ctrl+F5) so the UI loads the new assets.</div>
        </div>
        ` : (legacy ? `
        <div class="card warn">
          <div class="h">IMPORTANT</div>
          <div class="p">CrossWatch now clearly separates <b>global orchestration state</b> from <b>pair-specific provider caches</b>.</div>
          <ul>
            <li>Multiple pairs can run without overwriting each other’s cached snapshots/watermarks.</li>
            <li>Providers can safely reuse cached “present” indexes (when activities timestamps match) without risking cross-pair contamination.</li>
          </ul>
          <div class="p" style="margin-top:8px">For a smooth transition, the current caches need to be removed/migrated.</div>
        </div>

        <div class="card">
          <div class="h">What to do</div>
          <div class="p">Click <b>MIGRATE</b> below. It clears state/cache, then saves your config so it gets the new <code>version</code> field.</div>
        </div>

        <div class="card">
          <div class="h">Tip</div>
          <div class="p">After each CrossWatch update, hard refresh your browser (Ctrl+F5) so the UI loads the new assets.</div>
        </div>
        ` : `
        <div class="card">
          <div class="h">What this means</div>
          <div class="p">Nothing is broken. Click <b>MIGRATE</b> once so CrossWatch can apply the updated config structure.</div>
        </div>

        <div class="card">
          <div class="h">Tip</div>
          <div class="p">After each CrossWatch update, hard refresh your browser (Ctrl+F5) so the UI loads the new assets.</div>
        </div>
        `)}

        <div class="card" id="upg-release-notes" style="display:none">
          <div class="h">Release notes</div>
          <div class="p" id="upg-release-notes-meta" style="opacity:.72">&nbsp;</div>
          <div class="notes md" id="upg-release-notes-body"></div>
        </div>
      </div>

      <div class="foot">
        <button class="btn ghost" type="button" data-x="close">Close</button>
        ${needs0911Cleanup || legacy
          ? `<button class="btn danger" type="button" data-x="migrate">MIGRATE</button>`
          : `<button class="btn primary" type="button" data-x="save">MIGRATE</button>`
        }
      </div>
    </div>
    `;

    const shell = hostEl.closest(".cx-modal-shell");
    if (shell) {
      shell.style.width = "auto";
      shell.style.maxWidth = "none";
      shell.style.height = "auto";
      shell.style.maxHeight = "none";
      shell.style.display = "inline-block";
    }

    hostEl.querySelector('[data-x="close"]')?.addEventListener("click", () => {
      try {
        window.cxCloseModal?.();
      } catch {}
    });

    if (needs0911Cleanup || legacy) {
      hostEl.querySelector('[data-x="migrate"]')?.addEventListener("click", (e) => migrateNow(e.currentTarget, needs0911Cleanup));
    } else {
      hostEl.querySelector('[data-x="save"]')?.addEventListener("click", (e) => saveNow(e.currentTarget));
    }

    try {
      const j = await _getJson(NOTES_ENDPOINT, { cache: "no-store" });
      const body = String(j.body || "").trim();
      if (!body) return;
      const card = hostEl.querySelector("#upg-release-notes");
      const pre = hostEl.querySelector("#upg-release-notes-body");
      if (!card || !pre) return;

      pre.innerHTML = _sanitizeHtml(_mdToHtml(body));
      const lat = _norm(j.latest_version || j.latest || "");
      const pub = String(j.published_at || "").trim();
      const meta = hostEl.querySelector("#upg-release-notes-meta");
      if (meta) meta.textContent = `Latest${lat ? ` v${lat}` : ""}${pub ? ` • ${pub}` : ""}`;

      card.style.display = "block";
    } catch {
    }
  },

  unmount() {}
};
