/* assets/js/modals/editor-raw/index.js */
/* CrossWatch - editor record fields modal */

const I18N = () => globalThis.window?.CW?.I18n || globalThis.window?.__cwI18n;
const T = (key, vars) => I18N()?.t?.(key, vars) ?? key;
const TH = (key, vars) => I18N()?.html?.(key, vars) ?? key;
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
  if (s === "tracker") return T("editor.raw.tracker");
  if (s === "state") return T("editor.raw.state");
  if (s === "manual") return T("editor.raw.manual");
  if (s === "playlist") return T("editor.raw.playlist");
  return T("editor.raw.editor");
}

function view(props = {}) {
  const item = props.item && typeof props.item === "object" ? props.item : {};
  const title = String(props.title || item.title || item.series_title || props.key || T("editor.raw.stored"));
  const source = labelForSource(props.source);
  const kind = String(props.kind || "").trim();
  const key = String(props.key || "").trim();
  const origin = String(props.origin || "").trim();
  const itemRows = flatten(item);
  const existingPaths = new Set(itemRows.map(([path]) => String(path || "")));
  const recordRows = [];
  if (key && !existingPaths.has("key")) recordRows.push(["key", key]);
  if (origin && !existingPaths.has("origin") && !existingPaths.has("_origin")) recordRows.push(["origin", origin]);
  const rows = [...recordRows, ...itemRows];
  const copyText = rows.map(([path, value]) => `${path}: ${pretty(value)}`).join("\n");
  const sizeClass = rows.length <= 8 ? "is-compact" : rows.length <= 16 ? "is-medium" : "is-large";

  const fieldRows = rows.length
    ? rows.map(([path, value]) => `
      <div class="raw-row">
        <code class="raw-path" title="${escapeHtml(path)}">${escapeHtml(path)}</code>
        <span class="raw-value">${escapeHtml(pretty(value))}</span>
      </div>
    `).join("")
    : '<div class="raw-empty">' + TH("editor.raw.noFields") + '</div>';

  return `
    <div id="cx-modal" class="cx-card editor-raw-modal ${sizeClass}">

      <div class="cx-head">
        <div class="raw-head-icon"><span class="material-symbols-rounded" aria-hidden="true">database</span></div>
        <div class="raw-title">
          <strong>${escapeHtml(title)}</strong>
          <span>${escapeHtml(source)}${kind ? ` - ${escapeHtml(kind)}` : ""}</span>
        </div>
        <button class="cx-btn raw-close" type="button" data-close aria-label="${TH("editor.raw.close")}"><span class="material-symbols-rounded" aria-hidden="true">close</span></button>
      </div>
      <div class="cx-body">
        <div class="raw-grid">
          <section class="raw-panel">
            <div class="raw-panel-head">
              <span>${TH("editor.raw.fields")}</span>
              <button class="cx-btn" type="button" data-copy title="${TH("editor.raw.copy")}" aria-label="${TH("editor.raw.copy")}"><span class="material-symbols-rounded" aria-hidden="true">content_copy</span></button>
            </div>
            <div class="raw-rows">${fieldRows}</div>
            <div class="raw-copy-text" hidden>${escapeHtml(copyText)}</div>
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
    const txt = root?.querySelector(".raw-copy-text")?.textContent || "";
    try {
      if (!(await window.cwCopyText?.(txt))) throw new Error("copy failed");
      btn.innerHTML = '<span class="material-symbols-rounded" aria-hidden="true">check</span>';
      btn.title = T("editor.raw.copied");
      btn.setAttribute("aria-label", T("editor.raw.copied"));
      setTimeout(() => {
        btn.innerHTML = '<span class="material-symbols-rounded" aria-hidden="true">content_copy</span>';
        btn.title = T("editor.raw.copy");
        btn.setAttribute("aria-label", T("editor.raw.copy"));
      }, 1400);
    } catch {
      btn.innerHTML = '<span class="material-symbols-rounded" aria-hidden="true">error</span>';
      btn.title = T("editor.raw.copyFailed");
      btn.setAttribute("aria-label", T("editor.raw.copyFailed"));
      setTimeout(() => {
        btn.innerHTML = '<span class="material-symbols-rounded" aria-hidden="true">content_copy</span>';
        btn.title = T("editor.raw.copy");
        btn.setAttribute("aria-label", T("editor.raw.copy"));
      }, 1600);
    }
  });
}

export function unmount() {}

export default { mount, unmount };
