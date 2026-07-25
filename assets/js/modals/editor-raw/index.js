/* assets/js/modals/editor-raw/index.js */
/* CrossWatch - editor raw item fields modal */

const _cwV = (() => {
  try { return new URL(import.meta.url).searchParams.get("v") || window.__CW_VERSION__ || Date.now(); }
  catch { return window.__CW_VERSION__ || Date.now(); }
})();

const _cwVer = (u) => u + (u.includes("?") ? "&" : "?") + "v=" + encodeURIComponent(String(_cwV));

const { escapeHtml, setModalShellInline } = await import(_cwVer("../core/app-auth-setup.js"));

function pretty(value) {
  if (value == null) return "";
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  try { return JSON.stringify(value); }
  catch { return String(value); }
}

function flatten(value, prefix = "") {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return prefix ? [[prefix, value]] : [];
  }
  const rows = [];
  for (const [key, child] of Object.entries(value)) {
    const path = prefix ? `${prefix}.${key}` : key;
    if (child && typeof child === "object" && !Array.isArray(child)) {
      rows.push(...flatten(child, path));
    } else {
      rows.push([path, child]);
    }
  }
  return rows;
}

function labelForSource(source) {
  const s = String(source || "").toLowerCase();
  if (s === "tracker") return "Local Tracker";
  if (s === "state") return "Current State";
  return "Editor";
}

function view(props = {}) {
  const item = props.item && typeof props.item === "object" ? props.item : {};
  const rows = flatten(item);
  const json = JSON.stringify(item, null, 2);
  const title = String(props.title || item.title || item.series_title || props.key || "Stored item");
  const source = labelForSource(props.source);
  const kind = String(props.kind || "").trim();
  const key = String(props.key || "").trim();
  const origin = String(props.origin || "").trim();

  const fieldRows = rows.length
    ? rows.map(([path, value]) => `
      <div class="raw-row">
        <code class="raw-path" title="${escapeHtml(path)}">${escapeHtml(path)}</code>
        <span class="raw-value">${escapeHtml(pretty(value))}</span>
      </div>
    `).join("")
    : '<div class="raw-empty">No stored fields found.</div>';

  return `
    <div id="cx-modal" class="cx-card editor-raw-modal">
      <style>
        .editor-raw-modal{width:min(960px,calc(100vw - 34px));height:min(620px,calc(100vh - 28px));background:#171a22;border:1px solid rgba(255,255,255,.13);border-radius:16px;overflow:hidden;color:#eef1f6}
        .editor-raw-modal *{box-sizing:border-box}
        .editor-raw-modal .cx-head{gap:10px;padding:12px 16px;border-bottom:1px solid rgba(255,255,255,.10)}
        .editor-raw-modal .cx-body{display:grid;grid-template-rows:auto minmax(0,1fr);gap:10px;min-height:0;padding:12px 16px 14px}
        .editor-raw-modal .raw-head-icon{width:32px;height:32px;border-radius:10px;display:grid;place-items:center;background:transparent;border:1px solid rgba(157,183,255,.22);color:#9db7ff}
        .editor-raw-modal .raw-head-icon .material-symbols-rounded{font-size:20px}
        .editor-raw-modal .raw-title{display:grid;gap:2px;min-width:0}
        .editor-raw-modal .raw-title strong{font-size:16px;line-height:1.1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
        .editor-raw-modal .raw-title span{font-size:12px;color:#a9b0bd;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
        .editor-raw-modal .raw-close{margin-left:auto;width:36px;height:36px;padding:0;border-radius:11px}
        .editor-raw-modal .raw-meta{display:flex;gap:6px;flex-wrap:wrap;min-width:0}
        .editor-raw-modal .raw-chip{display:inline-flex;align-items:center;min-height:24px;max-width:100%;padding:0 9px;border-radius:999px;border:1px solid rgba(255,255,255,.11);background:#20242d;color:#d8deea;font-size:10px;font-weight:850;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
        .editor-raw-modal .raw-chip.key{max-width:min(380px,100%)}
        .editor-raw-modal .raw-grid{display:grid;grid-template-columns:minmax(0,1.06fr) minmax(0,.94fr);gap:10px;min-height:0}
        .editor-raw-modal .raw-panel{display:grid;grid-template-rows:auto minmax(0,1fr);min-height:0;border:1px solid rgba(255,255,255,.10);background:#20242d;border-radius:13px;overflow:hidden}
        .editor-raw-modal .raw-panel-head{display:flex;align-items:center;justify-content:space-between;gap:10px;min-height:40px;padding:8px 11px;border-bottom:1px solid rgba(255,255,255,.09);font-size:10px;font-weight:900;letter-spacing:.10em;text-transform:uppercase;color:#a9b0bd}
        .editor-raw-modal .raw-panel-head .cx-btn{width:32px;height:32px;min-height:32px;padding:0;border-radius:10px}
        .editor-raw-modal .raw-panel-head .material-symbols-rounded{font-size:18px}
        .editor-raw-modal .raw-rows{display:grid;align-content:start;min-height:0;overflow:auto}
        .editor-raw-modal .raw-row{display:grid;grid-template-columns:minmax(126px,190px) minmax(0,1fr);gap:10px;align-items:start;min-height:33px;padding:7px 10px;border-top:1px solid rgba(255,255,255,.065)}
        .editor-raw-modal .raw-row:first-child{border-top:0}
        .editor-raw-modal .raw-row:nth-child(odd){background:rgba(255,255,255,.018)}
        .editor-raw-modal .raw-path{min-width:0;height:22px;padding:3px 8px;border-radius:8px;background:#0d0c18;color:#b8c9ff;font:12px/16px ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
        .editor-raw-modal .raw-value{min-width:0;color:#eef1f6;font:12px/1.45 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;white-space:pre-wrap;word-break:break-word}
        .editor-raw-modal .raw-json{margin:0;min-height:0;overflow:auto;padding:10px 12px;color:#e9eefb;background:#141821;font:12px/1.42 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;white-space:pre;tab-size:2}
        .editor-raw-modal .raw-empty{padding:14px;color:#a9b0bd}
        @media(max-width:820px){.editor-raw-modal{height:min(720px,calc(100vh - 22px))}.editor-raw-modal .raw-grid{grid-template-columns:1fr}.editor-raw-modal .raw-row{grid-template-columns:1fr}.editor-raw-modal .raw-title strong{white-space:normal}}
      </style>
      <div class="cx-head">
        <div class="raw-head-icon"><span class="material-symbols-rounded" aria-hidden="true">data_object</span></div>
        <div class="raw-title">
          <strong>${escapeHtml(title)}</strong>
          <span>${escapeHtml(source)}${kind ? ` - ${escapeHtml(kind)}` : ""}</span>
        </div>
        <button class="cx-btn raw-close" type="button" data-close aria-label="Close"><span class="material-symbols-rounded" aria-hidden="true">close</span></button>
      </div>
      <div class="cx-body">
        <div class="raw-meta">
          ${key ? `<span class="raw-chip key">${escapeHtml(key)}</span>` : ""}
          ${origin ? `<span class="raw-chip">${escapeHtml(origin)}</span>` : ""}
          <span class="raw-chip">${rows.length} field${rows.length === 1 ? "" : "s"}</span>
        </div>
        <div class="raw-grid">
          <section class="raw-panel">
            <div class="raw-panel-head">Fields</div>
            <div class="raw-rows">${fieldRows}</div>
          </section>
          <section class="raw-panel">
            <div class="raw-panel-head">
              <span>JSON</span>
              <button class="cx-btn" type="button" data-copy title="Copy JSON" aria-label="Copy JSON"><span class="material-symbols-rounded" aria-hidden="true">content_copy</span></button>
            </div>
            <pre class="raw-json">${escapeHtml(json)}</pre>
          </section>
        </div>
      </div>
    </div>
  `;
}

export async function mount(shell, props = {}) {
  setModalShellInline(shell);
  shell.innerHTML = view(props);
  const root = shell.querySelector(".editor-raw-modal");
  root?.querySelectorAll("[data-close]").forEach((btn) => {
    btn.addEventListener("click", () => window.cxCloseModal?.());
  });
  root?.querySelector("[data-copy]")?.addEventListener("click", async (ev) => {
    const btn = ev.currentTarget;
    const txt = root?.querySelector(".raw-json")?.textContent || "";
    try {
      await navigator.clipboard.writeText(txt);
      btn.innerHTML = '<span class="material-symbols-rounded" aria-hidden="true">check</span>';
      btn.title = "Copied";
      btn.setAttribute("aria-label", "Copied");
      setTimeout(() => {
        btn.innerHTML = '<span class="material-symbols-rounded" aria-hidden="true">content_copy</span>';
        btn.title = "Copy JSON";
        btn.setAttribute("aria-label", "Copy JSON");
      }, 1400);
    } catch {
      btn.innerHTML = '<span class="material-symbols-rounded" aria-hidden="true">error</span>';
      btn.title = "Copy failed";
      btn.setAttribute("aria-label", "Copy failed");
      setTimeout(() => {
        btn.innerHTML = '<span class="material-symbols-rounded" aria-hidden="true">content_copy</span>';
        btn.title = "Copy JSON";
        btn.setAttribute("aria-label", "Copy JSON");
      }, 1600);
    }
  });
}

export function unmount() {}

export default { mount, unmount };
