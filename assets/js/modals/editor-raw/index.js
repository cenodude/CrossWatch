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
