/* restart_apply.js - Protocol apply/restart UI helpers */
(() => {
  "use strict";

  const CW_PROTO_PENDING_KEY = "cw_pending_proto_change_v1";
  const OVERLAY_ID = "cw-apply-overlay";
  const OVERLAY_CSS_ID = "cw-apply-overlay-css";
  const BANNER_ID = "cw-restart-banner";
  const BANNER_CSS_ID = "cw-restart-banner-css";

  const DEFAULT_RESTART_SECONDS = 15;
  const CW_PROTO_PENDING_TTL_MS = 1 * 60 * 1000; // 1 minute fallback


  function cwBuildProtoUrl(proto) {
    try {
      const p = String(proto || "").toLowerCase() === "https" ? "https" : "http";
      const u = new URL(window.location.href);
      u.protocol = p + ":";
      return u.toString();
    } catch (_) {
      return null;
    }
  }

  function _lsGet(key) {
    try { return localStorage.getItem(key); } catch (_) { return null; }
  }

  function _lsSet(key, val) {
    try { localStorage.setItem(key, val); } catch (_) {}
  }

  function _lsDel(key) {
    try { localStorage.removeItem(key); } catch (_) {}
  }

  function cwGetPendingProto() {
    try {
      const raw = _lsGet(CW_PROTO_PENDING_KEY);
      if (!raw) return null;
      const j = JSON.parse(raw);
      if (!j || typeof j !== "object") return null;
      if (!j.proto) return null;

      const ts = Number(j.ts || 0);
      if (!Number.isFinite(ts) || ts <= 0) {
        cwClearPendingProto();
        return null;
      }
      if (Date.now() - ts > CW_PROTO_PENDING_TTL_MS) {
        cwClearPendingProto();
        return null;
      }

      return j;
    } catch (_) {
      return null;
    }
  }

  function cwSetPendingProto(p) {
    _lsSet(CW_PROTO_PENDING_KEY, JSON.stringify(p || {}));
  }

  function cwClearPendingProto() {
    _lsDel(CW_PROTO_PENDING_KEY);
  }

  function cwEnsureApplyOverlay() {
    if (document.getElementById(OVERLAY_ID)) return;

    if (!document.getElementById(OVERLAY_CSS_ID)) {
      const st = document.createElement("style");
      st.id = OVERLAY_CSS_ID;
      st.textContent = `#${OVERLAY_ID}{
  position:fixed; inset:0; z-index:10001;
  display:none; align-items:center; justify-content:center;
  padding:18px;
  background:rgba(10,10,14,.72);
  backdrop-filter:blur(12px);
  -webkit-backdrop-filter:blur(12px);
}
#${OVERLAY_ID}.show{display:flex}
#${OVERLAY_ID} .cw-ao-card{
  width:min(560px, calc(100vw - 36px));
  border:1px solid rgba(255,255,255,.14);
  border-radius:18px;
  background:rgba(20,20,24,.92);
  box-shadow:0 12px 40px rgba(0,0,0,.45);
  padding:18px 18px 16px;
}
#${OVERLAY_ID} .cw-ao-top{display:flex; align-items:center; gap:12px}
#${OVERLAY_ID} .cw-ao-spinner{
  width:18px; height:18px; border-radius:999px;
  border:2px solid rgba(255,255,255,.25);
  border-top-color:rgba(255,255,255,.85);
  animation:cw-spin .9s linear infinite;
}
#${OVERLAY_ID} .cw-ao-title{font-weight:900; font-size:16px}
#${OVERLAY_ID} .cw-ao-sub{margin-top:8px; opacity:.85; font-size:13px; line-height:1.35}

#${OVERLAY_ID} .cw-ao-timer{
  margin-top:16px;
  display:flex;
  flex-direction:column;
  align-items:center;
  gap:12px;
  --p:0;
}
#${OVERLAY_ID} .cw-ao-timer.hidden{display:none}

#${OVERLAY_ID} .cw-ao-ring{
  width:148px; height:148px;
  border-radius:999px;
  position:relative;
  display:grid;
  place-items:center;
  isolation:isolate;
  background:conic-gradient(from -90deg,
    rgba(90,200,160,.95) calc(var(--p,0) * 1turn),
    rgba(255,255,255,.10) 0
  );
  box-shadow:0 18px 44px rgba(0,0,0,.45);
  transition:background .25s ease;
}
#${OVERLAY_ID} .cw-ao-ring::before{
  content:"";
  position:absolute;
  inset:-10px;
  border-radius:999px;
  z-index:0;
  pointer-events:none;
  background:radial-gradient(circle, rgba(90,200,160,.22), rgba(90,160,255,.12) 45%, rgba(0,0,0,0) 70%);
  filter:blur(12px);
  opacity:.70;
}
#${OVERLAY_ID} .cw-ao-ring::after{
  content:"";
  position:absolute;
  inset:10px;
  border-radius:999px;
  z-index:0;
  pointer-events:none;
  background:
    radial-gradient(140px 140px at 30% 25%, rgba(255,255,255,.14), rgba(0,0,0,0) 58%),
    rgba(18,18,22,.94);
  border:1px solid rgba(255,255,255,.12);
  box-shadow: inset 0 1px 0 rgba(255,255,255,.10);
}
#${OVERLAY_ID} .cw-ao-count{
  position:relative; z-index:1;
  font-size:48px;
  font-weight:950;
  letter-spacing:-1px;
  line-height:1;
  text-shadow:0 10px 28px rgba(0,0,0,.55);
}
#${OVERLAY_ID} .cw-ao-unit{
  position:relative; z-index:1;
  margin-top:-6px;
  font-size:11px;
  opacity:.72;
  letter-spacing:.28em;
  text-transform:uppercase;
}
#${OVERLAY_ID} .cw-ao-ring.tick .cw-ao-count{animation:cw-pop .18s ease}

#${OVERLAY_ID} .cw-ao-bar{
  width:min(360px, 100%);
  height:10px;
  border-radius:999px;
  background:rgba(255,255,255,.10);
  overflow:hidden;
  border:1px solid rgba(255,255,255,.12);
}
#${OVERLAY_ID} .cw-ao-bar-fill{
  height:100%;
  width:calc(var(--p,0) * 100%);
  background:linear-gradient(90deg, rgba(90,200,160,.95), rgba(90,160,255,.92));
  box-shadow:0 10px 26px rgba(90,200,160,.18);
  transition:width .25s ease;
}

#${OVERLAY_ID} .cw-ao-foot{margin-top:14px; font-size:12px; opacity:.75}

@keyframes cw-spin{to{transform:rotate(360deg)}}
@keyframes cw-pop{0%{transform:scale(1)}45%{transform:scale(1.07)}100%{transform:scale(1)}}

@media (prefers-reduced-motion: reduce){
  #${OVERLAY_ID} .cw-ao-spinner{animation:none}
  #${OVERLAY_ID} .cw-ao-ring.tick .cw-ao-count{animation:none}
  #${OVERLAY_ID} .cw-ao-bar-fill{transition:none}
}`;
      document.head.appendChild(st);
    }

    const o = document.createElement("div");
    o.id = OVERLAY_ID;
    o.innerHTML = `  <div class="cw-ao-card" role="dialog" aria-modal="true" aria-live="polite">
    <div class="cw-ao-top">
      <div class="cw-ao-spinner" aria-hidden="true"></div>
      <div class="cw-ao-title" id="cw-ao-title">Applying changes</div>
    </div>
    <div class="cw-ao-sub" id="cw-ao-sub">Restarting container / service…</div>

    <div class="cw-ao-timer" id="cw-ao-timer" style="--p:0">
      <div class="cw-ao-ring" id="cw-ao-ring" aria-hidden="true">
        <div class="cw-ao-count" id="cw-ao-count">${DEFAULT_RESTART_SECONDS}</div>
        <div class="cw-ao-unit">sec</div>
      </div>
      <div class="cw-ao-bar" aria-hidden="true"><div class="cw-ao-bar-fill" id="cw-ao-bar-fill"></div></div>
    </div>

    <div class="cw-ao-foot" id="cw-ao-foot">You’ll be redirected automatically.</div>
  </div>`;
    document.body.appendChild(o);
  }

function _cwUpdateOverlayTimer(o, left, total) {
  if (!o) return;
  const timer = o.querySelector("#cw-ao-timer");
  const ring = o.querySelector("#cw-ao-ring");
  const c = o.querySelector("#cw-ao-count");

  const l = Number(left);
  const t = Number(total);

  const leftClamped = Number.isFinite(l) ? Math.max(0, l) : 0;
  const totalClamped = Number.isFinite(t) ? Math.max(0, t) : 0;

  if (c) c.textContent = String(Math.ceil(leftClamped));

  const p = totalClamped > 0 ? Math.min(1, Math.max(0, (totalClamped - leftClamped) / totalClamped)) : 0;
  if (timer) timer.style.setProperty("--p", String(p));

  if (ring) {
    try {
      ring.classList.remove("tick");
      void ring.offsetWidth;
      ring.classList.add("tick");
      setTimeout(() => { try { ring.classList.remove("tick"); } catch (_) {} }, 200);
    } catch (_) {}
  }
}

function cwShowApplyOverlay(title, subtitle, seconds) {
  cwEnsureApplyOverlay();
  const o = document.getElementById(OVERLAY_ID);
  if (!o) return;

  const t = o.querySelector("#cw-ao-title");
  const s = o.querySelector("#cw-ao-sub");
  const timer = o.querySelector("#cw-ao-timer");

  if (t) t.textContent = String(title || "Applying changes");
  if (s) s.textContent = String(subtitle || "Restarting container / service…");

  const sec = Number.isFinite(Number(seconds)) ? Number(seconds) : DEFAULT_RESTART_SECONDS;
  if (timer) {
    if (sec > 0) {
      timer.classList.remove("hidden");
      _cwUpdateOverlayTimer(o, sec, sec);
    } else {
      timer.classList.add("hidden");
    }
  }

  o.classList.add("show");
}

  function cwHideApplyOverlay() {
    const o = document.getElementById(OVERLAY_ID);
    if (o) o.classList.remove("show");
  }

  function cwEnsureRestartBanner() {
    if (document.getElementById(BANNER_ID)) return;

    if (!document.getElementById(BANNER_CSS_ID)) {
      const st = document.createElement("style");
      st.id = BANNER_CSS_ID;
      st.textContent = `
#${BANNER_ID}{
  position:fixed; left:0; right:0; bottom:14px; z-index:10000;
  display:flex; justify-content:center; pointer-events:none;
}
#${BANNER_ID}.hidden{display:none}
#${BANNER_ID} .cw-rb-card{
  pointer-events:auto;
  width:min(860px, calc(100vw - 28px));
  display:flex; align-items:center; gap:14px;
  border:1px solid rgba(255,255,255,.14);
  border-radius:16px;
  background:rgba(11,11,15,.92);
  backdrop-filter:blur(10px) saturate(140%);
  -webkit-backdrop-filter:blur(10px) saturate(140%);
  box-shadow:0 18px 40px rgba(0,0,0,.45);
  padding:12px 12px;
}
#${BANNER_ID} .cw-rb-title{font-weight:900}
#${BANNER_ID} .cw-rb-sub{opacity:.85; font-size:13px}
#${BANNER_ID} .cw-rb-actions{margin-left:auto; display:flex; gap:8px}
#${BANNER_ID} .cw-rb-actions button{
  appearance:none; border:1px solid rgba(255,255,255,.16);
  background:rgba(255,255,255,.06);
  color:inherit; border-radius:12px;
  padding:10px 12px;
  cursor:pointer; font-weight:800;
  transition:transform .12s ease, background .12s ease, opacity .12s ease;
}
#${BANNER_ID} .cw-rb-actions button:hover{background:rgba(255,255,255,.10); transform:translateY(-1px); opacity:.98}
#${BANNER_ID} .cw-rb-actions button:active{transform:translateY(0)}
#${BANNER_ID} .cw-rb-actions button.primary{
  border-color:rgba(90,200,160,.35);
  background:rgba(90,200,160,.10);
}
#${BANNER_ID} .cw-rb-actions button.primary:hover{background:rgba(90,200,160,.16)}
#${BANNER_ID} .cw-rb-actions button.danger{
  border-color:rgba(255,80,80,.35);
}
#${BANNER_ID} .cw-rb-actions button:disabled{
  opacity:.6; cursor:not-allowed; transform:none;
}
`;
      document.head.appendChild(st);
    }

    const b = document.createElement("div");
    b.id = BANNER_ID;
    b.className = "hidden";
    b.innerHTML = `
  <div class="cw-rb-card" role="status" aria-live="polite">
    <div>
      <div class="cw-rb-title">Restart required</div>
      <div class="cw-rb-sub" id="cw-rb-sub">A restart is required to apply changes.</div>
    </div>
    <div class="cw-rb-actions">
      <button type="button" class="primary" id="cw-rb-apply" style="display:none">Apply NOW</button>
      <button type="button" class="danger" id="cw-rb-dismiss">Dismiss</button>
    </div>
  </div>
`;
    document.body.appendChild(b);

    const dismiss = b.querySelector("#cw-rb-dismiss");
    dismiss?.addEventListener("click", () => {
      try { cwClearPendingProto(); } catch (_) {}
      try { b.classList.add("hidden"); } catch (_) {}
    });

    const apply = b.querySelector("#cw-rb-apply");
    apply?.addEventListener("click", () => {
      try { cwApplyPendingRestart(); } catch (_) {}
    });
  }

  function cwShowRestartBanner(message, hrefOrOpts, maybeOpts) {
    cwEnsureRestartBanner();
    const b = document.getElementById(BANNER_ID);
    if (!b) return;

    const sub = b.querySelector("#cw-rb-sub");
    if (sub) sub.textContent = String(message || "Restart required");

    let opts = maybeOpts || {};
    if (hrefOrOpts && typeof hrefOrOpts === "object" && !Array.isArray(hrefOrOpts)) opts = hrefOrOpts;

    const apply = b.querySelector("#cw-rb-apply");
    if (apply) {
      const show = !!opts.showApply;
      apply.textContent = String(opts.applyText || "Apply NOW");
      apply.style.display = show ? "" : "none";
      apply.disabled = false;
    }

    b.classList.remove("hidden");
  }

  function cwHideRestartBanner() {
    const b = document.getElementById(BANNER_ID);
    if (b) b.classList.add("hidden");
  }

  function cwInitPendingProtoBanner() {
    const p = cwGetPendingProto();
    if (!p) return;

    const cur = String(window.location.protocol || "http:").replace(":", "");
    const want = String(p.proto || "").toLowerCase() === "https" ? "https" : "http";

    if (want === cur) {
      cwClearPendingProto();
      return;
    }

    cwShowRestartBanner("Protocol changed: restart required", { showApply: true, applyText: "Apply NOW" });
  }

  function cwQueueProtocolApply(proto, url) {
    const want = String(proto || "").toLowerCase() === "https" ? "https" : "http";
    const p = { kind: "protocol", proto: want, url: url || cwBuildProtoUrl(want), ts: Date.now() };
    cwSetPendingProto(p);
    cwShowRestartBanner("Protocol changed: restart required", { showApply: true, applyText: "Apply NOW" });
  }

  async function _postJson(url, body) {
    const r = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "same-origin",
      cache: "no-store",
      body: body ? JSON.stringify(body) : undefined
    });
    let j = null;
    try { j = await r.json(); } catch (_) {}
    return { ok: r.ok && (!j || j.ok !== false), status: r.status, json: j };
  }

  async function cwApplyPendingRestart() {
    const p = cwGetPendingProto();
    if (!p || p.kind !== "protocol") return;

    const targetUrl = p.url || cwBuildProtoUrl(p.proto) || window.location.href;

    cwHideRestartBanner();
    cwShowApplyOverlay("Protocol change: applying", "Logging out, restarting CrossWatch…", DEFAULT_RESTART_SECONDS);

    const btn = document.getElementById("cw-rb-apply");
    if (btn) btn.disabled = true;

    let ok = false;

    try {
      const res = await _postJson("/api/app-auth/apply-now", { kind: "protocol" });
      ok = !!res.ok;
    } catch (_) {}

    if (!ok) {
      try { await fetch("/api/app-auth/logout", { method: "POST", credentials: "same-origin", cache: "no-store" }); } catch (_) {}
      try { await fetch("/api/maintenance/restart", { method: "POST", cache: "no-store" }); ok = true; } catch (_) {}
    }

    if (!ok) {
      cwShowApplyOverlay("Apply failed", "Restart request failed", 0);
      setTimeout(cwHideApplyOverlay, 2400);
      return;
    }

    const total = DEFAULT_RESTART_SECONDS;
    let left = total;
    const o = document.getElementById(OVERLAY_ID);
    _cwUpdateOverlayTimer(o, left, total);

    const tmr = setInterval(() => {
      left -= 1;
      _cwUpdateOverlayTimer(o, left, total);
      if (left <= 0) {
        clearInterval(tmr);
        window.location.href = targetUrl;
      }
    }, 1000);
  }

  async function cwRestartCrossWatchWithOverlay() {
    const seconds = DEFAULT_RESTART_SECONDS;
    cwShowApplyOverlay("Restarting CrossWatch", "Restarting container / service…", seconds);

    let ok = false;
    let err = "";

    try {
      const r = await fetch("/api/maintenance/restart", { method: "POST", cache: "no-store" });
      let j = {};
      try { j = await r.json(); } catch (_) {}
      ok = !!(j && j.ok) || r.ok;
      err = (j && j.error) ? String(j.error) : "";
    } catch (_) {
      err = "network";
    }

    if (!ok) {
      cwShowApplyOverlay("Restart failed", err ? `Restart request failed (${err})` : "Restart request failed", 0);
      setTimeout(cwHideApplyOverlay, 2400);
      return;
    }

    const total = seconds;
    let left = total;
    const o = document.getElementById(OVERLAY_ID);
    _cwUpdateOverlayTimer(o, left, total);

    const tmr = setInterval(() => {
      left -= 1;
      _cwUpdateOverlayTimer(o, left, total);
      if (left <= 0) {
        clearInterval(tmr);
        window.location.reload();
      }
    }, 1000);
  }

  try {
    Object.assign(window, {
      cwBuildProtoUrl,
      cwShowRestartBanner,
      cwHideRestartBanner,
      cwInitPendingProtoBanner,
      cwQueueProtocolApply,
      cwApplyPendingRestart,
      cwShowApplyOverlay,
      cwRestartCrossWatchWithOverlay
    });
  } catch (_) {}
})();
