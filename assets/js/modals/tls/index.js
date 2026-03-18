// assets/js/modals/tls/index.js

const fjson = async (url, opts = {}) => {
  const r = await fetch(url, { cache: "no-store", ...opts });
  if (!r.ok) {
    const msg = `${r.status} ${r.statusText || ""}`.trim();
    throw new Error(msg || "Request failed");
  }
  if (r.status === 204) return {};
  try { return await r.json(); } catch { return {}; }
};

const $ = (sel, root = document) => root.querySelector(sel);

function closeModal() {
  if (window.cxCloseModal) { window.cxCloseModal(); return; }
  document.querySelector(".cx-modal-shell")?.dispatchEvent(new CustomEvent("cw-modal-close", { bubbles: true }));
}

function injectCSS() {
  if (document.getElementById("cw-tls-css")) return;
  const el = document.createElement("style");
  el.id = "cw-tls-css";
  el.textContent = `
  .cx-modal-shell.tls-modal-shell{width:min(var(--cxModalMaxW,980px),calc(100vw - 64px))!important;max-width:min(var(--cxModalMaxW,980px),calc(100vw - 64px))!important;height:min(var(--cxModalMaxH,92vh),calc(100vh - 56px))!important;background:linear-gradient(180deg,rgba(7,10,18,.96),rgba(5,8,15,.94))!important;border:1px solid rgba(103,128,255,.16)!important;box-shadow:0 34px 90px rgba(0,0,0,.58),0 0 0 1px rgba(255,255,255,.03) inset!important}
  .cw-tls{position:relative;display:flex;flex-direction:column;height:100%;background:radial-gradient(120% 120% at 0% 0%,rgba(102,88,255,.06),transparent 32%),radial-gradient(110% 140% at 100% 100%,rgba(0,208,255,.05),transparent 30%),linear-gradient(180deg,rgba(6,9,16,.985),rgba(4,6,12,.985));color:#eaf1ff}
  .cw-tls::before{content:"";position:absolute;inset:0;pointer-events:none;background:linear-gradient(90deg,rgba(255,255,255,.022),transparent 30%,transparent 70%,rgba(255,255,255,.018));opacity:.52}
  .cw-tls .cx-head{
    position:relative;z-index:1;display:flex;align-items:center;justify-content:space-between;gap:12px;
    padding:12px 14px 10px;border-bottom:1px solid rgba(255,255,255,.08);
    background:linear-gradient(180deg,rgba(255,255,255,.045),rgba(255,255,255,.01));backdrop-filter:blur(10px)
  }
  .cw-tls .head-left{display:flex;align-items:center;gap:12px;min-width:0}
  .cw-tls .icon{
    width:36px;height:36px;border-radius:12px;display:grid;place-items:center;flex-shrink:0;
    background:linear-gradient(135deg,rgba(94,226,172,.18),rgba(56,189,248,.12));border:1px solid rgba(79,209,156,.22);
    box-shadow:inset 0 0 0 1px rgba(255,255,255,.03)
  }
  .cw-tls .icon .material-symbols-rounded{font-variation-settings:"FILL" 0,"wght" 500,"GRAD" 0,"opsz" 24;font-size:18px;line-height:1;color:#f3f6ff}
  .cw-tls .titles{display:flex;flex-direction:column;gap:2px;min-width:0}
  .cw-tls .title{font-weight:900;font-size:18px;letter-spacing:.08em;text-transform:uppercase;color:#f3f6ff;text-shadow:0 0 18px rgba(104,122,255,.16)}
  .cw-tls .sub{color:rgba(205,215,235,.74);font-size:12px;line-height:1.45;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
  .cw-tls .head-actions{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
  .cw-tls .btn{
    appearance:none;border:1px solid rgba(255,255,255,.12);background:linear-gradient(180deg,rgba(255,255,255,.055),rgba(255,255,255,.02));color:#edf3ff;border-radius:14px;padding:8px 12px;font-size:12px;font-weight:800;letter-spacing:.05em;text-transform:uppercase;display:inline-flex;align-items:center;justify-content:center;gap:6px;white-space:nowrap;box-shadow:0 10px 24px rgba(0,0,0,.16),inset 0 1px 0 rgba(255,255,255,.04);transition:transform .14s ease,box-shadow .14s ease,border-color .14s ease,background .14s ease
  }
  .cw-tls .btn:hover{transform:translateY(-1px);border-color:rgba(123,112,255,.4);box-shadow:0 14px 30px rgba(0,0,0,.24),0 0 0 1px rgba(123,112,255,.14) inset}
  .cw-tls .btn:active{transform:none}
  .cw-tls .btn.primary{background:linear-gradient(135deg,rgba(112,92,255,.92),rgba(72,144,255,.88));border-color:rgba(143,165,255,.38);box-shadow:0 16px 34px rgba(45,96,255,.26),0 0 18px rgba(116,97,255,.18)}
  .cw-tls .btn.danger{background:linear-gradient(135deg,#ff5d76,#ff9f5d);border-color:rgba(255,93,118,.35);box-shadow:0 16px 34px rgba(255,93,118,.18),0 0 18px rgba(255,159,93,.14)}
  .cw-tls .btn:disabled{opacity:.55;cursor:not-allowed}

  .cw-tls .cx-body{position:relative;z-index:1;padding:12px 14px;overflow:auto;flex:1;min-height:0}
  .cw-tls .section{
    border:1px solid rgba(255,255,255,.10);background:rgba(13,17,23,.35);
    border-radius:14px;box-shadow:inset 0 1px 0 rgba(255,255,255,.04);padding:12px 12px;margin-bottom:12px
  }
  .cw-tls .section h3{margin:0 0 10px 0;font-size:14px;font-weight:800;letter-spacing:.01em;color:#f4f7ff}
  .cw-tls .hint{color:rgba(205,215,235,.74);font-size:12px;line-height:1.45;margin-top:8px}
  .cw-tls .grid2{display:grid;grid-template-columns:1fr 1fr;gap:12px 14px}
  .cw-tls .field{display:flex;flex-direction:column;gap:6px;min-width:0}
  .cw-tls label{font-size:12px;font-weight:800;letter-spacing:.01em;color:rgba(230,237,250,.72)}
  .cw-tls input,.cw-tls select,.cw-tls textarea{
    width:100%;box-sizing:border-box;
    border-radius:14px;border:1px solid rgba(255,255,255,.12);
    background:rgba(12,16,30,.82);color:#e6eeff;padding:12px 13px;outline:none;box-shadow:inset 0 0 0 1px rgba(255,255,255,.02),0 8px 24px rgba(0,0,0,.12)
  }
  .cw-tls input:focus,.cw-tls select:focus,.cw-tls textarea:focus{border-color:rgba(120,136,255,.52);box-shadow:0 0 0 3px rgba(115,97,255,.14),inset 0 0 0 1px rgba(255,255,255,.02)}
  .cw-tls .kv{display:grid;grid-template-columns:160px 1fr;gap:8px 12px;align-items:center}
  .cw-tls .k{color:rgba(205,215,235,.74);font-size:12px}
  .cw-tls .v{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
  .cw-tls .pill{display:inline-flex;gap:6px;align-items:center;padding:4px 9px;border-radius:999px;
    border:1px solid rgba(255,255,255,.14);background:rgba(255,255,255,.06);font-weight:750;font-size:12px
  }
  .cw-tls .pill.ok{border-color:rgba(58,205,132,.35);background:rgba(58,205,132,.12)}
  .cw-tls .pill.bad{border-color:rgba(255,93,118,.35);background:rgba(255,93,118,.12)}
  .cw-tls .notice{
    display:none;margin-top:10px;padding:10px 11px;border-radius:12px;
    border:1px solid rgba(255,200,87,.30);background:rgba(255,200,87,.10);font-size:12px;opacity:.95
  }
  .cw-tls .notice.show{display:block}
  .cw-tls .foot{
    position:relative;z-index:1;padding:10px 14px;border-top:1px solid rgba(255,255,255,.10);
    background:linear-gradient(180deg,rgba(255,255,255,.02),rgba(255,255,255,.04));
    display:flex;gap:10px;justify-content:flex-end;flex-wrap:wrap;flex-shrink:0
  }

  @media (max-width: 860px){
    .cx-modal-shell.tls-modal-shell{width:min(var(--cxModalMaxW,980px),calc(100vw - 24px))!important;max-width:min(var(--cxModalMaxW,980px),calc(100vw - 24px))!important;height:min(var(--cxModalMaxH,92vh),calc(100vh - 24px))!important}
    .cw-tls .grid2{grid-template-columns:1fr}
    .cw-tls .kv{grid-template-columns:120px 1fr}
    .cw-tls .cx-head{align-items:flex-start;flex-direction:column}
    .cw-tls .head-actions{width:100%;justify-content:flex-start}
  }
  `;
  document.head.appendChild(el);
}

function splitCSV(s) {
  return String(s || "")
    .split(",")
    .map(x => x.trim())
    .filter(Boolean);
}

async function downloadBlob(url, filename) {
  const r = await fetch(url, { cache: "no-store" });
  if (!r.ok) throw new Error(`Download failed: ${r.status}`);
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
  setPill(root, "#st-cert", exists, exists ? "OK" : "Not found");
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

  setNotice(root, true, "Saved. Restart required if you changed protocol, certs, or key paths.");
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

  setNotice(root, true, "Certificate regenerated. Restart required.");
}

export default {
  async mount(root) {
    injectCSS();

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
              <div class="sub">Self-signed certificate or custom cert/key paths</div>
            </div>
          </div>
          <div class="head-actions">
            <button class="btn" id="tls-close" type="button">Close</button>
          </div>
        </div>

        <div class="cx-body">
          <div class="section">
            <h3>Current status</h3>
            <div class="kv">
              <div class="k">Protocol</div><div class="v" id="st-proto">—</div>
              <div class="k">Cert path</div><div class="v" id="st-cert-path">—</div>
              <div class="k">Key path</div><div class="v" id="st-key-path">—</div>
              <div class="k">Certificate</div><div class="v"><span class="pill bad" id="st-cert">—</span></div>
              <div class="k">Expires</div><div class="v" id="st-exp">—</div>
              <div class="k">SHA-256</div><div class="v" id="st-sha">—</div>
              <div class="k">SANs</div><div class="v" id="st-sans">—</div>
            </div>
            <div style="display:flex;gap:10px;flex-wrap:wrap;margin-top:10px">
              <button class="btn" id="tls-refresh" type="button">Refresh</button>
              <button class="btn primary" id="tls-download" type="button" disabled>Download .crt</button>
            </div>
            <div class="hint">Tip: for a real certificate, terminate TLS in a reverse proxy (Caddy/Nginx/Traefik) and run CrossWatch on HTTP behind it.</div>
          </div>

          <div class="section">
            <h3>Certificate configuration</h3>
            <div class="grid2">
              <div class="field">
                <label for="tls-mode">Mode</label>
                <select id="tls-mode">
                  <option value="self">Self-signed (generated)</option>
                  <option value="custom">Custom cert/key paths</option>
                </select>
                <div class="hint">Custom mode expects you to mount files into the container.</div>
              </div>

              <div class="field">
                <label for="tls-hostname">Hostname (CN)</label>
                <input id="tls-hostname" type="text" value="localhost" />
                <div class="hint">Used for the self-signed certificate CN and included in SAN.</div>
              </div>

              <div class="field">
                <label for="tls-days">Valid days</label>
                <input id="tls-days" type="number" min="1" max="3650" value="825" />
              </div>

              <div class="field">
                <label for="tls-altdns">Additional DNS names (comma separated)</label>
                <input id="tls-altdns" type="text" placeholder="mybox.local, crosswatch.local" />
              </div>

              <div class="field">
                <label for="tls-altips">Additional IPs (comma separated)</label>
                <input id="tls-altips" type="text" placeholder="192.168.1.10" />
              </div>

              <div class="field">
                <label for="tls-certfile">Cert file path</label>
                <input id="tls-certfile" type="text" placeholder="/config/tls/crosswatch.crt" />
              </div>

              <div class="field">
                <label for="tls-keyfile">Key file path</label>
                <input id="tls-keyfile" type="text" placeholder="/config/tls/crosswatch.key" />
              </div>
            </div>

            <div class="notice" id="tls-restart">Restart required.</div>
          </div>
        </div>

        <div class="foot">
          <button class="btn" id="tls-validate" type="button">Validate paths</button>
          <button class="btn danger" id="tls-regenerate" type="button">Regenerate self-signed</button>
          <button class="btn primary" id="tls-save" type="button">Save</button>
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
        setNotice(root, true, `Failed to load status: ${e?.message || e}`);
      }
    };

    on("#tls-refresh", "click", refresh);

    on("#tls-download", "click", async () => {
      try { await downloadBlob("/api/ui/tls/cert", "crosswatch.crt"); }
      catch (e) { setNotice(root, true, `Download failed: ${e?.message || e}`); }
    });

    on("#tls-save", "click", async () => {
      try {
        await saveTlsConfig(root);
        await refresh();
      } catch (e) {
        setNotice(root, true, `Save failed: ${e?.message || e}`);
      }
    });

    on("#tls-validate", "click", async () => {
      try {
        await saveTlsConfig(root);
        await refresh();
      } catch (e) {
        setNotice(root, true, `Validate failed: ${e?.message || e}`);
      }
    });

    on("#tls-regenerate", "click", async () => {
      try {
        await regenerateSelfSigned(root);
        await refresh();
      } catch (e) {
        setNotice(root, true, `Regenerate failed: ${e?.message || e}`);
      }
    });

    await refresh();
  }
};
