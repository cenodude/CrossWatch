// assets/js/modals/tls/index.js

const I18N = () => globalThis.window?.CW?.I18n || globalThis.window?.__cwI18n;
const T = (key, vars) => I18N()?.t?.(key, vars) ?? key;
const TH = (key, vars) => I18N()?.html?.(key, vars) ?? key;
const fjson = async (url, opts = {}) => {
  const r = await fetch(url, { cache: "no-store", ...opts });
  if (!r.ok) {
    const msg = `${r.status} ${r.statusText || ""}`.trim();
    throw new Error(msg || T("modals.tls.requestFailed"));
  }
  if (r.status === 204) return {};
  try { return await r.json(); } catch { return {}; }
};

const $ = (sel, root = document) => root.querySelector(sel);

function closeModal() {
  if (window.cxCloseModal) { window.cxCloseModal(); return; }
  document.querySelector(".cx-modal-shell")?.dispatchEvent(new CustomEvent("cw-modal-close", { bubbles: true }));
}

function splitCSV(s) {
  return String(s || "")
    .split(",")
    .map(x => x.trim())
    .filter(Boolean);
}

async function downloadBlob(url, filename) {
  const r = await fetch(url, { cache: "no-store" });
  if (!r.ok) throw new Error(T("modals.tls.downloadFailed", {error: String(r.status)}));
  const blob = await r.blob();
  const href = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = href;
  a.download = filename || "crosswatch.crt";
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(href), 1000);
}

function setText(root, id, v) {
  const el = root.querySelector(id);
  if (el) el.textContent = (v == null || v === "") ? "—" : String(v);
}

function setPill(root, id, ok, text) {
  const el = root.querySelector(id);
  if (!el) return;
  el.classList.remove("ok", "bad");
  el.classList.add(ok ? "ok" : "bad");
  el.textContent = text;
}

function setNotice(root, show, msg) {
  const el = root.querySelector("#tls-restart");
  if (!el) return;
  el.classList.toggle("show", !!show);
  if (msg) el.textContent = msg;
}

function renderStatus(root, st) {
  const tls = st?.tls || {};
  const cert = tls?.cert || {};
  const exists = !!cert?.exists;

  setText(root, "#st-proto", st?.protocol || "—");
  setText(root, "#st-cert-path", tls?.resolved_cert_path || cert?.path || "—");
  setText(root, "#st-key-path",  tls?.resolved_key_path || "—");
  setPill(root, "#st-cert", exists, exists ? "OK" : T("modals.tls.notFound"));
  setText(root, "#st-exp", cert?.not_after || "—");
  setText(root, "#st-sha", cert?.sha256 || "—");
  setText(root, "#st-sans", (Array.isArray(cert?.sans) && cert.sans.length) ? cert.sans.join(", ") : "—");

  // Prefill configuration fields from current config/status
  const modeSel = root.querySelector("#tls-mode");
  const selfSigned = !!tls?.self_signed;
  if (modeSel) modeSel.value = selfSigned ? "self" : "custom";

  const host = root.querySelector("#tls-hostname");
  if (host) host.value = tls?.hostname || "localhost";

  const days = root.querySelector("#tls-days");
  if (days) days.value = String(tls?.valid_days ?? 825);

  const certFile = root.querySelector("#tls-certfile");
  if (certFile) certFile.value = tls?.cert_file || "";

  const keyFile = root.querySelector("#tls-keyfile");
  if (keyFile) keyFile.value = tls?.key_file || "";

  // Button enable
  const dl = root.querySelector("#tls-download");
  if (dl) dl.disabled = !exists;

  if (certFile && !certFile.value && tls?.resolved_cert_path) certFile.placeholder = tls.resolved_cert_path;
  if (keyFile && !keyFile.value && tls?.resolved_key_path) keyFile.placeholder = tls.resolved_key_path;
}

async function saveTlsConfig(root) {
  const cfg = await fjson("/api/config");
  cfg.ui = (cfg.ui && typeof cfg.ui === "object") ? cfg.ui : {};
  cfg.ui.tls = (cfg.ui.tls && typeof cfg.ui.tls === "object") ? cfg.ui.tls : {};

  const mode = String($("#tls-mode", root)?.value || "self");
  cfg.ui.tls.self_signed = (mode === "self");

  cfg.ui.tls.hostname = String($("#tls-hostname", root)?.value || "localhost").trim() || "localhost";

  const daysRaw = parseInt(String($("#tls-days", root)?.value || "825"), 10);
  cfg.ui.tls.valid_days = Number.isFinite(daysRaw) && daysRaw > 0 ? daysRaw : 825;

  cfg.ui.tls.alt_dns = splitCSV($("#tls-altdns", root)?.value || "");
  cfg.ui.tls.alt_ips = splitCSV($("#tls-altips", root)?.value || "");

  cfg.ui.tls.cert_file = String($("#tls-certfile", root)?.value || "").trim();
  cfg.ui.tls.key_file = String($("#tls-keyfile", root)?.value || "").trim();

  const r = await fetch("/api/config", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(cfg),
  });
  if (!r.ok) throw new Error(`POST /api/config ${r.status}`);

  setNotice(root, true, T("modals.tls.saved"));
}

async function regenerateSelfSigned(root) {
  const payload = {
    hostname: String($("#tls-hostname", root)?.value || "localhost").trim() || "localhost",
    valid_days: parseInt(String($("#tls-days", root)?.value || "825"), 10) || 825,
    alt_dns: splitCSV($("#tls-altdns", root)?.value || ""),
    alt_ips: splitCSV($("#tls-altips", root)?.value || ""),
  };

  await fjson("/api/ui/tls/regenerate", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  setNotice(root, true, T("modals.tls.regenerated"));
}

export default {
  async mount(root) {

    const shell = root.closest(".cx-modal-shell");
    if (shell) {
      shell.classList.add("tls-modal-shell");
      shell.style.setProperty("--cxModalMaxW", "980px");
      shell.style.setProperty("--cxModalMaxH", "92vh");
    }

    root.innerHTML = `
      <div class="cw-tls">
        <div class="cx-head">
          <div class="head-left">
            <div class="icon"><span class="material-symbols-rounded">lock</span></div>
            <div class="titles">
              <div class="title">TLS / HTTPS</div>
              <div class="sub">${TH("modals.tls.sub")}</div>
            </div>
          </div>
          <div class="head-actions">
            <button class="btn" id="tls-close" type="button">${TH("modals.tls.close")}</button>
          </div>
        </div>

        <div class="cx-body">
          <div class="section">
            <h3>${TH("modals.tls.status")}</h3>
            <div class="kv">
              <div class="k">${TH("modals.tls.protocol")}</div><div class="v" id="st-proto">—</div>
              <div class="k">${TH("modals.tls.certPath")}</div><div class="v" id="st-cert-path">—</div>
              <div class="k">${TH("modals.tls.keyPath")}</div><div class="v" id="st-key-path">—</div>
              <div class="k">${TH("modals.tls.certificate")}</div><div class="v"><span class="pill bad" id="st-cert">—</span></div>
              <div class="k">${TH("modals.tls.expires")}</div><div class="v" id="st-exp">—</div>
              <div class="k">SHA-256</div><div class="v" id="st-sha">—</div>
              <div class="k">SANs</div><div class="v" id="st-sans">—</div>
            </div>
            <div style="display:flex;gap:10px;flex-wrap:wrap;margin-top:10px">
              <button class="btn" id="tls-refresh" type="button">${TH("modals.tls.refresh")}</button>
              <button class="btn primary" id="tls-download" type="button" disabled>${TH("modals.tls.download")}</button>
            </div>
            <div class="hint">${TH("modals.tls.tip")}</div>
          </div>

          <div class="section">
            <h3>${TH("modals.tls.config")}</h3>
            <div class="grid2">
              <div class="field">
                <label for="tls-mode">${TH("modals.tls.mode")}</label>
                <select id="tls-mode">
                  <option value="self">${TH("modals.tls.self")}</option>
                  <option value="custom">${TH("modals.tls.custom")}</option>
                </select>
                <div class="hint">${TH("modals.tls.customHint")}</div>
              </div>

              <div class="field">
                <label for="tls-hostname">${TH("modals.tls.hostname")}</label>
                <input id="tls-hostname" type="text" value="localhost" />
                <div class="hint">${TH("modals.tls.hostnameHint")}</div>
              </div>

              <div class="field">
                <label for="tls-days">${TH("modals.tls.days")}</label>
                <input id="tls-days" type="number" min="1" max="3650" value="825" />
              </div>

              <div class="field">
                <label for="tls-altdns">${TH("modals.tls.altDns")}</label>
                <input id="tls-altdns" type="text" placeholder="mybox.local, crosswatch.local" />
              </div>

              <div class="field">
                <label for="tls-altips">${TH("modals.tls.altIps")}</label>
                <input id="tls-altips" type="text" placeholder="192.168.1.10" />
              </div>

              <div class="field">
                <label for="tls-certfile">${TH("modals.tls.certFile")}</label>
                <input id="tls-certfile" type="text" placeholder="/config/tls/crosswatch.crt" />
              </div>

              <div class="field">
                <label for="tls-keyfile">${TH("modals.tls.keyFile")}</label>
                <input id="tls-keyfile" type="text" placeholder="/config/tls/crosswatch.key" />
              </div>
            </div>

            <div class="notice" id="tls-restart">${TH("modals.tls.restart")}</div>
          </div>
        </div>

        <div class="foot">
          <button class="btn" id="tls-validate" type="button">${TH("modals.tls.validate")}</button>
          <button class="btn danger" id="tls-regenerate" type="button">${TH("modals.tls.regenerate")}</button>
          <button class="btn primary" id="tls-save" type="button">${TH("modals.tls.save")}</button>
        </div>
      </div>
    `;

    const on = (id, ev, fn) => {
      const el = root.querySelector(id);
      if (el) el.addEventListener(ev, fn);
    };

    on("#tls-close", "click", () => closeModal());

    const refresh = async () => {
      try {
        const st = await fjson("/api/ui/tls/status");
        renderStatus(root, st);
      } catch (e) {
        setNotice(root, true, T("modals.tls.loadFailed", {error: String(e?.message || e)}));
      }
    };

    on("#tls-refresh", "click", refresh);

    on("#tls-download", "click", async () => {
      try { await downloadBlob("/api/ui/tls/cert", "crosswatch.crt"); }
      catch (e) { setNotice(root, true, T("modals.tls.downloadFailed", {error: String(e?.message || e)})); }
    });

    on("#tls-save", "click", async () => {
      try {
        await saveTlsConfig(root);
        await refresh();
      } catch (e) {
        setNotice(root, true, T("modals.tls.saveFailed", {error: String(e?.message || e)}));
      }
    });

    on("#tls-validate", "click", async () => {
      try {
        await saveTlsConfig(root);
        await refresh();
      } catch (e) {
        setNotice(root, true, T("modals.tls.validateFailed", {error: String(e?.message || e)}));
      }
    });

    on("#tls-regenerate", "click", async () => {
      try {
        await regenerateSelfSigned(root);
        await refresh();
      } catch (e) {
        setNotice(root, true, T("modals.tls.regenerateFailed", {error: String(e?.message || e)}));
      }
    });

    await refresh();
  }
};
