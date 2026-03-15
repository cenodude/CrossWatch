/* assets/js/scrobbler.js */
/* refactored */
/* Scrobbler configuration UI and logic. */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function (w, d) {
  const authSetupPending = () => w.cwIsAuthSetupPending?.() === true;
  const $ = (s, r) => (r || d).querySelector(s);
  const $all = (s, r) => [...(r || d).querySelectorAll(s)];
  const el = (t, a) => Object.assign(d.createElement(t), a || {});
  const on = (n, e, f) => n && n.addEventListener(e, f);

  const fieldKey = (value, fallback = "field") => String(value || fallback).replace(/[^a-z0-9_-]+/gi, "_");
  const bindFieldIdentity = (node, base, rid, fallback = "field") => {
    if (!node) return node;
    const key = fieldKey(rid, fallback);
    const safeBase = fieldKey(base, "field");
    node.id = `${safeBase}_${key}`;
    node.name = `${safeBase}_${key}`;
    return node;
  };

  const j = async (u, o) => {
    if (authSetupPending()) throw new Error("auth setup pending");
    const r = await fetch(u, { cache: "no-store", ...(o || {}) });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    return r.json();
  };
  const STICKY_NOTES = {};


  function setNote(id, msg, kind) {
    const n = d.getElementById(id);
    if (!n) return;
    const sid = String(id || "");
    if (!msg && STICKY_NOTES[sid]) {
      const s = STICKY_NOTES[sid];
      n.textContent = s.msg || "";
      n.style.display = s.msg ? "" : "none";
      n.classList.remove("err", "warn", "ok");
      if (s.kind) n.classList.add(s.kind);
      return;
    }
    if (!msg) {
      n.textContent = "";
      n.style.display = "none";
      n.classList.remove("err", "warn", "ok");
      return;
    }
    n.textContent = msg || "";
    n.style.display = "";
    const color = kind === "err" ? "#ff6b6b" : kind === "warn" ? "#f59e0b" : "var(--muted,#a7a7a7)";
    n.style.cssText =
      "margin:6px 0 2px;font-size:12px;opacity:.92;color:" +
      color +
      (kind === "warn" ? ";font-weight:700" : "");
  }


function setStickyNote(id, msg, kind) {
  const sid = String(id || "");
  STICKY_NOTES[sid] = { msg: String(msg || ""), kind: kind || "" };
  setNote(sid, msg, kind);
}

function clearStickyNote(id) {
  const sid = String(id || "");
  delete STICKY_NOTES[sid];
  setNote(sid, "");
}

  const HELP_TEXT = {
    "sc-help-auto-remove":
      "When you finish a movie, CW will automatically remove that title from your configured Watchlists. It’s currently movies-only. It honors your filters (username/server). If the movie isn’t on your Watchlist, nothing happens, your libraries and other services remain untouched.",
    "sc-help-webhook-plex-ratings":
      "When enabled, we’ll send ratings to Trakt. Movies, shows, seasons, and episodes are supported.",
    "sc-help-watch-plex-ratings":
      "When enabled, we’ll send ratings to Trakt and/or SIMKL.\nTrakt: Movies, shows, seasons, and episodes are supported.\nMDBList: Movies, shows, seasons, and episodes are supported.\nSIMKL: Movies and shows are supported.\nAdd the below webhook to your Plex instance to enable ratings.",

    "sc-help-adv-pause":
      "Pause debounce (sec) (default 5) - Ignore rapid, duplicate pause events.",
    "sc-help-adv-suppress":
      "Suppress start @ (%) (default 99) - If play/resume is at or above this %, don’t send /scrobble/start.",
    "sc-help-adv-regress":
      "Regress tol % (default 5) - Block progress rollbacks bigger than this %.",
    "sc-help-adv-stop-pause":
      "Stop pause ≥ (%) (default 80) - If STOP arrives below this %, treat it as PAUSE.",
    "sc-help-adv-force-stop":
      "Force stop @ (%) (default 80) - If STOP is at or above this %, send /scrobble/stop.",
    "sc-help-adv-progress-step":
      "Progress updates in percentages, which can significantly reduce or increase the number of API calls required. When in doubt, default to 25% increments.",
    "sc-help-watch-filters":
      "Don't skip the filtering step! While optional for solo media server users, it becomes essential the moment you share your server with other users. Without filters, the system will scrobble everything",
    "sc-help-watch-username-whitelist":
      "Only scrobble activity for the usernames listed here. Leave it empty only if you want all detected users on the selected server or route to be included.",
    "sc-help-watch-server-uuid":
      "Limit watcher scrobbles to one specific media server or user identity. Plex uses the server UUID. Emby and Jellyfin use the user ID.",
        "sc-help-watch-advanced":
      "Do not alter the Advanced settings unless you fully understand their impact. When in doubt, leave them untouched.",
  };

  const helpBtn = (tipId) =>
    `<button type="button" class="cx-help material-symbols-rounded" data-tip-id="${tipId}" aria-label="Help">help</button>`;
  const wrapTooltipText = (text, maxLen = 64) => {
    const raw = String(text || "").replace(/\r/g, "");
    if (!raw) return "";
    return raw
      .split("\n")
      .map((line) => {
        const words = line.trim().split(/\s+/).filter(Boolean);
        if (!words.length) return "";
        const rows = [];
        let current = words.shift();
        words.forEach((word) => {
          if ((current + " " + word).length > maxLen) {
            rows.push(current);
            current = word;
          } else {
            current += " " + word;
          }
        });
        rows.push(current);
        return rows.join("\n");
      })
      .join("\n");
  };
  const scUi = w.CW?.ScrobblerUI || {};
  const helpBtnNode = scUi.helpBtnNode || (() => null);
  const ensureInlineHelp = scUi.ensureInlineHelp || (() => {});
  const enhanceWatcherFiltersUI = scUi.enhanceWatcherFiltersUI || (() => {});
  const enhanceWatcherAdvancedUI = scUi.enhanceWatcherAdvancedUI || (() => {});
  const enhanceWebhookFiltersUI = scUi.enhanceWebhookFiltersUI || (() => {});

  function bindHelpTips(root) {
    const scope = root || d;
    $all(".cx-help[data-tip-id]", scope).forEach((btn) => {
      const id = btn.getAttribute("data-tip-id") || "";
      const text = HELP_TEXT[id] || (id === "sc-help-watch-routes" ? "Routes control which provider sends activity to which sink. You can create separate paths for different services or profiles. Do not forget to set Filters for each route, otherwise playback from the wrong users or server may be scrobbled." : "");
      if (text && !btn.title) btn.title = wrapTooltipText(text);

      if (btn.dataset.cxBound === "1") return;
      btn.dataset.cxBound = "1";
      btn.addEventListener("click", (e) => {
        e.preventDefault();
        e.stopPropagation();
      });
    });
  }

  function injectStyles() {
    if (d.getElementById("sc-styles")) return;
    const s = d.createElement("style");
    s.id = "sc-styles";
    s.textContent = `.row{display:flex;gap:14px;align-items:center;flex-wrap:wrap}.codepair{display:flex;gap:8px;align-items:center}.codepair.right{justify-content:flex-end}.codepair code{padding:6px 8px;border-radius:8px;background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.08)}#card-scrobbler .badge,#sec-scrobbler .badge{padding:4px 10px;border-radius:999px;font-weight:600;opacity:.9}#card-scrobbler .badge.is-on,#sec-scrobbler .badge.is-on{background:#0a3;color:#fff}#card-scrobbler .badge.is-off,#sec-scrobbler .badge.is-off{background:#333;color:#bbb;border:1px solid #444}#card-scrobbler .status-dot,#sec-scrobbler .status-dot{width:10px;height:10px;border-radius:50%}#card-scrobbler .status-dot.on,#sec-scrobbler .status-dot.on{background:#22c55e}#card-scrobbler .status-dot.off,#sec-scrobbler .status-dot.off{background:#ef4444}#card-scrobbler .chips,#sec-scrobbler .chips{display:flex;flex-wrap:wrap;gap:6px}#card-scrobbler .chip,#sec-scrobbler .chip{display:inline-flex;align-items:center;gap:6px;padding:4px 8px;border-radius:10px;background:rgba(255,255,255,.05);border:1px solid rgba(255,255,255,.08)}#card-scrobbler .chip .rm,#sec-scrobbler .chip .rm{cursor:pointer;opacity:.7}.sc-filter-grid{display:grid;grid-template-columns:1fr 1fr;gap:14px;align-items:start}.sc-filter-grid>div{display:grid;gap:10px;min-width:0}.sc-adv-grid{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:14px}.sc-adv-grid .field{display:grid;grid-template-columns:minmax(0,1fr) auto 92px;align-items:center;gap:10px}.sc-adv-grid .field label{min-width:0;font-size:12px;opacity:.8;letter-spacing:.04em;text-transform:uppercase}.sc-adv-grid .field .cx-help{flex:0 0 auto}.sc-adv-grid .field input{width:92px;max-width:100%;justify-self:end}@media (max-width:1380px){.sc-adv-grid{grid-template-columns:repeat(4,minmax(0,1fr));}}@media (max-width:980px){.sc-adv-grid{grid-template-columns:repeat(2,minmax(0,1fr));}}@media (max-width:640px){.sc-adv-grid,.sc-filter-grid{grid-template-columns:1fr;}}.sc-subbox{margin-top:12px;border-radius:12px;background:rgba(255,255,255,.04);box-shadow:0 0 0 1px rgba(255,255,255,.06) inset}.sc-subbox .head{padding:12px 14px;font-weight:700;opacity:.92}.sc-subbox .body{padding:12px 14px;border-top:1px solid rgba(255,255,255,.06)}.sc-toggle{display:inline-flex;align-items:center;gap:8px;font-size:12px;opacity:.9;white-space:nowrap}.wh-logo{width:var(--wh-logo,24px);height:var(--wh-logo,24px);aspect-ratio:1/1;object-fit:contain;display:block;transform-origin:center}.wh-logo[alt="Plex"]{transform:scale(1.15)}.wh-logo[alt="Jellyfin"]{transform:scale(1)}.wh-logo[alt="Emby"]{transform:scale(1.15)}.sc-opt-col{display:flex;flex-direction:column;gap:10px}.sc-opt-row{display:flex;align-items:center;gap:10px;flex-wrap:wrap}.sc-pillbar{display:flex;align-items:center;gap:8px;flex-wrap:wrap}.sc-pill{display:inline-flex;align-items:center;justify-content:center;padding:7px 10px;border-radius:999px;border:1px solid rgba(255,255,255,.12);background:rgba(255,255,255,.05);color:rgba(255,255,255,.92);font-size:12px;line-height:1;cursor:pointer;user-select:none;transition:background .15s ease,border-color .15s ease,opacity .15s ease}.sc-pill.off{opacity:.78}.sc-pill.on{background:rgba(34,197,94,.18);border-color:rgba(34,197,94,.45);opacity:1}.sc-pill:hover{border-color:rgba(255,255,255,.22)}.sc-pill:focus-visible{outline:0;box-shadow:0 0 0 2px rgba(255,255,255,.14),0 0 0 6px rgba(34,197,94,.15)}.sc-pill:disabled{cursor:default;opacity:.45}.sc-user-pop{position:fixed;z-index:9999;width:min(360px,calc(100vw - 24px));max-height:min(420px,calc(100vh - 24px));border-radius:14px;background:var(--panel,#111);box-shadow:0 0 0 1px rgba(255,255,255,.08) inset,0 18px 50px rgba(0,0,0,.55);border:1px solid rgba(255,255,255,.10);overflow:hidden}.sc-user-pop.hidden{display:none}.sc-user-pop .head{display:flex;justify-content:space-between;align-items:center;gap:10px;padding:10px 12px;border-bottom:1px solid rgba(255,255,255,.06)}.sc-user-pop .title{font-weight:800}.sc-user-pop .body{padding:10px 12px;display:grid;gap:10px}.sc-user-pop .list{overflow:auto;border:1px solid rgba(255,255,255,.08);border-radius:12px;max-height:280px}.sc-user-pop .userrow{width:100%;text-align:left;background:transparent;border:0;color:inherit;padding:10px 10px;cursor:pointer}.sc-user-pop .userrow:hover{background:rgba(255,255,255,.05)}.sc-user-pop .row1{display:flex;justify-content:space-between;align-items:center;gap:8px}.sc-user-pop .sub{font-size:12px;opacity:.7;padding:10px}.sc-user-pop .tag{font-size:11px;padding:2px 8px;border-radius:999px;background:rgba(255,255,255,.08);border:1px solid rgba(255,255,255,.10);opacity:.85}#card-scrobbler .input,#sec-scrobbler .input{background:#0a0a17;border:1px solid rgba(255,255,255,.12);border-radius:14px;color:#e7e9f4;box-shadow:inset 0 0 0 1px rgba(255,255,255,.02)}#card-scrobbler .input:focus,#sec-scrobbler .input:focus{outline:none;border-color:rgba(124,92,255,.52);box-shadow:0 0 0 3px rgba(124,92,255,.18),inset 0 0 0 1px rgba(255,255,255,.03)}#card-scrobbler select.input,#sec-scrobbler select.input{background-color:#0a0a17;color:#e7e9f4}#card-scrobbler select.input option,#sec-scrobbler select.input option{background:#11131a;color:#fff}.sc-prov-wrap{position:relative;display:inline-block}.sc-prov-btn{width:140px;display:flex;align-items:center;justify-content:space-between;gap:10px;padding:8px 10px;cursor:pointer;background:#0a0a17;border:1px solid rgba(255,255,255,.12);border-radius:14px;box-shadow:inset 0 0 0 1px rgba(255,255,255,.02)}.sc-prov-left{display:inline-flex;align-items:center;gap:8px;min-width:0}.sc-prov-ico{width:18px;height:18px;object-fit:contain}.sc-prov-caret{opacity:.7}.sc-prov-menu{position:absolute;right:0;top:calc(100% + 6px);min-width:140px;border-radius:12px;background:var(--panel,#111);box-shadow:0 0 0 1px rgba(255,255,255,.08) inset,0 18px 50px rgba(0,0,0,.55);border:1px solid rgba(255,255,255,.10);overflow:hidden;z-index:1000}.sc-prov-menu.hidden{display:none}.sc-prov-item{width:100%;display:flex;align-items:center;gap:8px;padding:10px 10px;background:transparent;border:0;color:inherit;cursor:pointer;text-align:left}.sc-prov-item:hover{background:rgba(255,255,255,.05)}.sc-prov-item[aria-selected="true"]{background:rgba(34,197,94,.18)}.sc-prov-btn,.sc-prov-btn *{color:rgba(255,255,255,.92)!important;-webkit-text-fill-color:rgba(255,255,255,.92)!important}.sc-prov-btn:disabled,.sc-prov-btn:disabled *{color:rgba(255,255,255,.55)!important;-webkit-text-fill-color:rgba(255,255,255,.55)!important}#sc-provider,#sc-sink{color:rgba(255,255,255,.92)!important;-webkit-text-fill-color:rgba(255,255,255,.92)!important}#sc-provider:disabled,#sc-sink:disabled{color:rgba(255,255,255,.55)!important;-webkit-text-fill-color:rgba(255,255,255,.55)!important}#sc-provider option,#sc-sink option{color:#fff;background:#111}.sc-route-table table{width:100%;border-collapse:separate;border-spacing:0 8px}.sc-route-table th{font-size:12px;opacity:.8;text-align:left;padding:0 6px}.sc-route-table td{padding:0 6px;vertical-align:middle}.sc-route-row{background:rgba(255,255,255,.03);border:1px solid rgba(255,255,255,.06);border-radius:12px}.sc-route-row td{padding:8px 6px}.sc-route-actions{display:flex;gap:8px;justify-content:flex-end;flex-wrap:wrap}.sc-route-table select.input{height:34px}.sc-route-table .sc-prov-wrap{display:block;width:100%}.sc-route-table .sc-prov-btn{width:100%;height:34px;padding:6px 10px}.sc-route-table .sc-prov-menu{left:0;right:0;min-width:0}.sc-shell{display:grid;gap:14px}.sc-shell .cw-meta-provider-panel.active{display:grid;gap:14px}.sc-shell .cw-panel-head{padding:18px 18px 16px;border:1px solid rgba(255,255,255,.08);border-radius:22px;background:radial-gradient(120% 145% at 0% 0%,rgba(124,92,255,.16),transparent 40%),linear-gradient(180deg,rgba(11,14,21,.96),rgba(6,8,12,.985));box-shadow:0 18px 36px rgba(0,0,0,.24),inset 0 1px 0 rgba(255,255,255,.03)}.sc-shell .cw-panel-head-main{display:grid;gap:6px}.sc-shell .cw-panel-title{font-size:24px;font-weight:900;letter-spacing:-.02em;color:#f4f7ff}.sc-shell .muted,.sc-shell .micro-note{color:rgba(196,204,222,.74)}.sc-shell .cw-subtiles{display:flex;gap:10px;flex-wrap:wrap;justify-content:flex-end}.sc-shell .cw-subtile{min-height:38px;padding:0 14px;border-radius:999px;border:1px solid rgba(255,255,255,.10);background:rgba(255,255,255,.035);color:#eef3ff;font-weight:800;letter-spacing:.04em;transition:transform .14s ease,border-color .14s ease,background .14s ease,box-shadow .14s ease}.sc-shell .cw-subtile:hover{transform:translateY(-1px);border-color:rgba(124,92,255,.28);background:rgba(255,255,255,.06)}.sc-shell .cw-subtile.active{border-color:rgba(124,92,255,.40);background:linear-gradient(180deg,rgba(124,92,255,.20),rgba(45,161,255,.10));box-shadow:0 10px 22px rgba(18,22,40,.28),inset 0 1px 0 rgba(255,255,255,.06)}.sc-shell .cw-subpanels{display:grid;gap:14px}.sc-shell .cw-subpanel.active{display:grid;gap:14px}.sc-shell .sc-subbox,.sc-shell .cc-card,.sc-shell #sc-filters,.sc-shell #sc-advanced,.sc-shell #sc-routes-wrap{border-radius:22px;background:linear-gradient(180deg,rgba(255,255,255,.04),rgba(255,255,255,.02));border:1px solid rgba(255,255,255,.08);box-shadow:0 18px 34px rgba(0,0,0,.18),inset 0 1px 0 rgba(255,255,255,.03)}.sc-shell .sc-subbox .head,.sc-shell .cc-head{padding:16px 16px 12px;font-size:12px;font-weight:900;letter-spacing:.12em;text-transform:uppercase;color:rgba(224,230,246,.7)}.sc-shell .sc-subbox .body,.sc-shell #sc-routes-wrap>.body{padding:14px 16px 16px;border-top:1px solid rgba(255,255,255,.06)}.sc-shell .cc-card{padding:16px}.sc-shell .cc-head{display:flex;align-items:center;justify-content:space-between;gap:10px;margin:0 0 12px;padding:0}.sc-shell .cc-body{display:grid;gap:14px}.sc-shell .cc-gauge{min-height:74px;padding:16px 18px;border-radius:18px;background:linear-gradient(180deg,rgba(8,12,19,.78),rgba(4,6,10,.90));border:1px solid rgba(255,255,255,.08);box-shadow:inset 0 1px 0 rgba(255,255,255,.04)}.sc-shell .cc-state .lbl{font-size:11px;letter-spacing:.12em;text-transform:uppercase;color:rgba(196,204,222,.64)}.sc-shell .cc-state .val{font-size:24px;font-weight:900;color:#f4f7ff}.sc-shell .cc-meta{display:flex;gap:12px;flex-wrap:wrap;font-size:12px;color:rgba(196,204,222,.72)}.sc-shell .cc-actions{display:flex;gap:10px;flex-wrap:wrap}.sc-shell .cc-actions .btn,.sc-shell .codepair .btn,.sc-shell .row .btn{min-height:40px;border-radius:14px}.sc-shell .cc-actions .btn:nth-child(1),.sc-shell #sc-route-add{background:linear-gradient(135deg,rgba(86,60,180,.42),rgba(56,106,208,.42));border-color:rgba(124,92,255,.24);box-shadow:0 14px 28px rgba(22,24,40,.24)}.sc-shell .codepair code{padding:9px 12px;border-radius:14px;background:linear-gradient(180deg,rgba(3,5,9,.96),rgba(1,3,6,.985));border:1px solid rgba(255,255,255,.08);color:#eef3ff}.sc-shell #sc-plexwatcher-url,.sc-shell #sc-webhook-url-plex,.sc-shell #sc-webhook-url-jf,.sc-shell #sc-webhook-url-emby{font-family:inherit;font-size:14px;font-weight:400;letter-spacing:normal;line-height:1.4}.sc-shell .sc-filter-grid,.sc-shell .sc-adv-grid,.sc-shell .cc-wrap{gap:16px}.sc-shell #sc-filters,.sc-shell #sc-advanced{padding:18px 20px 20px}.sc-shell #sc-filters>div:first-child,.sc-shell #sc-advanced>div:first-child{display:flex;justify-content:flex-end;margin:0 0 16px}.sc-shell #sc-filters>.body,.sc-shell #sc-advanced>.body{padding:0}.sc-shell #sc-route-filter-wrap{padding:14px 16px;border-radius:18px;background:linear-gradient(180deg,rgba(255,255,255,.03),rgba(255,255,255,.015));border:1px solid rgba(255,255,255,.08)}.sc-shell #sc-route-filter-wrap .muted,.sc-shell .sc-filter-grid>div>.muted{font-size:11px;font-weight:900;letter-spacing:.12em;text-transform:uppercase;color:rgba(224,230,246,.68)}.sc-shell .sc-filter-grid{grid-template-columns:repeat(2,minmax(0,1fr));align-items:start}.sc-shell .sc-filter-grid>div{display:grid;gap:10px;min-width:0}.sc-shell .sc-filter-grid .chips{min-height:40px;align-content:flex-start}.sc-shell .sc-user-pop,.sc-shell .sc-prov-menu{background:linear-gradient(180deg,rgba(12,14,23,.98),rgba(6,8,12,.985));border:1px solid rgba(255,255,255,.10);box-shadow:0 18px 42px rgba(0,0,0,.36)}.sc-shell .sc-prov-btn{width:164px;min-height:42px;border-radius:16px;background:linear-gradient(180deg,rgba(3,5,9,.96),rgba(1,3,6,.985));border:1px solid rgba(255,255,255,.10)}.sc-shell .sc-route-table table{border-spacing:0 10px}.sc-shell .sc-route-row{background:linear-gradient(180deg,rgba(255,255,255,.04),rgba(255,255,255,.02));border:1px solid rgba(255,255,255,.08)}.sc-shell .badge,.sc-shell .pill,.sc-shell .sc-pill{display:inline-flex;align-items:center;justify-content:center;min-height:28px;padding:0 10px;border-radius:999px;font-size:11px;font-weight:850;letter-spacing:.05em;text-transform:uppercase}.sc-shell .badge.is-off{background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.10);color:rgba(236,241,255,.78)}.sc-shell .badge.is-on{background:rgba(34,197,94,.16);border:1px solid rgba(34,197,94,.30);color:#dcffe7}.sc-shell .sc-pillbar{gap:8px}.sc-shell .sc-pill{min-height:34px;padding:0 12px;border-radius:999px;background:rgba(255,255,255,.04)}.sc-shell .sc-pill.on{background:linear-gradient(180deg,rgba(124,92,255,.22),rgba(45,161,255,.10));border-color:rgba(124,92,255,.34)}.sc-shell .sc-opt-row,.sc-shell .sc-opt-col{gap:12px}.sc-shell .cx-help{color:rgba(214,222,242,.72)}.sc-shell .cx-help:hover{color:#fff}.sc-shell .row .cx-toggle,.sc-shell .cc-auto .cx-toggle{padding:10px 12px;border-radius:16px;border:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(255,255,255,.04),rgba(255,255,255,.018))}.sc-shell #sc-note,.sc-shell #sc-webhook-warning,.sc-shell #sc-endpoint-note{padding:12px 14px;border-radius:18px;border:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(255,255,255,.04),rgba(255,255,255,.018))}.sc-shell #sc-webhook-warning.warn{border-color:rgba(245,158,11,.24);background:linear-gradient(180deg,rgba(245,158,11,.12),rgba(255,255,255,.018))}.sc-shell .input,.sc-shell select.input{min-height:44px;border-radius:16px}.sc-shell .field{display:grid;grid-template-columns:minmax(0,1fr) auto 92px;align-items:center;gap:10px;padding:14px 16px;border-radius:18px;border:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(255,255,255,.03),rgba(255,255,255,.015))}.sc-shell .field input{width:92px}.sc-shell .field label{font-size:12px;font-weight:900;letter-spacing:.08em;text-transform:uppercase;color:rgba(226,232,248,.74)}@media (max-width:980px){.sc-shell .cw-panel-head{gap:14px}.sc-shell .cw-subtiles{justify-content:flex-start}.sc-shell .cc-wrap,.sc-shell .sc-filter-grid{grid-template-columns:1fr}.sc-shell .sc-adv-grid{grid-template-columns:repeat(2,minmax(0,1fr))}.sc-shell .row{align-items:flex-start}}@media (max-width:640px){.sc-shell .cw-panel-head{padding:16px}.sc-shell .cw-panel-title{font-size:22px}.sc-shell .cw-subtile{width:100%;justify-content:center}.sc-shell .cc-actions,.sc-shell .row,.sc-shell .codepair{width:100%}.sc-shell .codepair{flex-wrap:wrap}.sc-shell .codepair code,.sc-shell .codepair .btn,.sc-shell .cc-actions .btn,.sc-shell .row .btn,.sc-shell .sc-prov-btn{width:100%}.sc-shell .sc-filter-grid,.sc-shell .sc-adv-grid{grid-template-columns:1fr}.sc-shell #sc-filters,.sc-shell #sc-advanced{padding:16px}.sc-shell .field{grid-template-columns:minmax(0,1fr) auto}.sc-shell .field input{grid-column:1 / -1;width:100%}}`;
    d.head.appendChild(s);
    const t = d.createElement("style");
    t.id = "sc-styles-tweaks";
    t.textContent = `.sc-shell #sc-server-required:empty,.sc-shell #sc-note:empty,.sc-shell #sc-endpoint-note:empty,.sc-shell #sc-webhook-warning:empty{display:none!important}.sc-shell .cc-head>div:first-child{display:inline-flex;align-items:center;gap:10px;min-width:0}.sc-shell .cx-switch-wrap,.sc-shell .sc-opt-row{display:flex;align-items:center;gap:12px;flex-wrap:wrap}.sc-shell .cx-switch-wrap .sc-toggle,.sc-shell .sc-opt-row .muted{display:inline-flex;align-items:center;min-height:40px;margin:0}.sc-shell .cx-switch-wrap .cx-help,.sc-shell .sc-opt-row .cx-help{display:inline-flex;align-items:center;justify-content:center;align-self:center;margin:0}.sc-shell .sc-inline-head{display:inline-flex;align-items:center;gap:8px;flex-wrap:wrap}.sc-shell .sc-route-select-host{display:block;width:100%}.sc-shell .sc-route-select-host>.cw-icon-select{width:100%}.sc-shell .sc-route-table .cw-icon-select-btn{min-height:34px;padding:0 10px;border-radius:14px}.sc-shell .sc-route-table .cw-icon-select-label{font-size:13px}.sc-shell #sc-filters.sc-filters-enhanced>.body{display:grid;grid-template-columns:minmax(0,1fr);gap:18px}.sc-shell #sc-filters.sc-filters-enhanced #sc-route-filter-wrap{display:grid;gap:10px;width:100%;max-width:none;margin:0!important;padding:16px 18px;border-radius:18px;background:linear-gradient(180deg,rgba(255,255,255,.03),rgba(255,255,255,.015));border:1px solid rgba(255,255,255,.08)}.sc-shell #sc-filters.sc-filters-enhanced .sc-filter-grid{grid-template-columns:repeat(2,minmax(0,1fr));gap:18px;align-items:stretch}.sc-shell #sc-filters.sc-filters-enhanced .sc-filter-grid>div{display:grid;gap:10px;align-content:start;min-width:0;padding:16px 18px;border-radius:18px;background:linear-gradient(180deg,rgba(255,255,255,.03),rgba(255,255,255,.015));border:1px solid rgba(255,255,255,.08)}.sc-shell #sc-filters.sc-filters-enhanced #sc-whitelist{min-height:44px;align-content:flex-start}.sc-shell #sc-filters.sc-filters-enhanced #sc-users-note,.sc-shell #sc-filters.sc-filters-enhanced #sc-uuid-note{min-height:18px}.sc-shell .sc-filter-input-row{display:grid!important;align-items:center;gap:8px}.sc-shell .sc-filter-input-row--actions{grid-template-columns:minmax(0,1fr) 84px 84px}.sc-shell .sc-filter-input-row--fetch .sc-filter-input-spacer{display:block;min-height:1px}.sc-shell .sc-filter-input-row .btn{width:100%}.sc-shell #sc-advanced .body{display:block}.sc-shell .sc-advanced-header{display:flex;align-items:center;margin:0 0 16px}.sc-shell .sc-advanced-title{display:inline-flex;align-items:center;gap:8px;min-height:28px;font-size:11px;font-weight:900;letter-spacing:.12em;text-transform:uppercase;color:rgba(224,230,246,.68)}.sc-shell .sc-advanced-title .cx-help{margin:0}.sc-shell .sc-advanced-fields{display:grid;gap:16px}.sc-shell .sc-advanced-note{margin-top:12px}.sc-shell .sc-adv-grid{grid-template-columns:repeat(3,minmax(0,1fr));gap:16px}.sc-shell .sc-adv-grid .field{grid-template-columns:minmax(0,1fr) 36px 112px;align-items:center;min-height:88px}.sc-shell .sc-adv-grid .field input{width:112px}.sc-shell .sc-adv-grid .field label{line-height:1.35}.sc-shell .sc-adv-grid .field .cx-help{justify-self:center;transform:none}@media (max-width:1180px){.sc-shell .sc-adv-grid{grid-template-columns:repeat(2,minmax(0,1fr))}}@media (max-width:980px){.sc-shell #sc-filters.sc-filters-enhanced .sc-filter-grid{grid-template-columns:1fr}}@media (max-width:640px){.sc-shell .sc-filter-input-row,.sc-shell .sc-filter-input-row--actions,.sc-shell .sc-filter-input-row--fetch{grid-template-columns:minmax(0,1fr)!important}.sc-shell .sc-filter-input-row--fetch .sc-filter-input-spacer{display:none}.sc-shell .sc-adv-grid{grid-template-columns:1fr}.sc-shell .sc-adv-grid .field{grid-template-columns:minmax(0,1fr) auto}.sc-shell .sc-adv-grid .field input{grid-column:1 / -1;width:100%}}`;
    d.head.appendChild(t);
  }

  const DEFAULTS = {
    watch: { pause_debounce_seconds: 5, suppress_start_at: 99 },
    trakt: { stop_pause_threshold: 80, force_stop_at: 80, regress_tolerance_percent: 5, progress_step: 25 },
  };

  const STATE = { mount: null, webhookIds: null, webhookHost: null, watcherHost: null, cfg: {}, users: [], ui: { watchProvider: null, watchSink: null, scrobbleEnabled: null, scrobbleMode: null, watchAutostart: null }, pf: { key: "cx_sc_watch_filters_by_provider_v1", store: {}, loaded: false }, _pfMute: false, _noSinkAutostartFixApplied: false };

  const deepSet = (o, p, v) =>
    p.split(".").reduce(
      (a, k, i, arr) =>
        i === arr.length - 1
          ? (a[k] = v)
          : (((a[k] && typeof a[k] === "object") || (a[k] = {})), a[k]),
      o
    );

  const read = (p, dflt) => p.split(".").reduce((v, k) => (v && typeof v === "object" ? v[k] : undefined), STATE.cfg) ?? dflt;

  function write(p, v) {
    deepSet(STATE.cfg, p, v);
    try {
      w._cfgCache ||= {};
      deepSet(w._cfgCache, p, v);
    } catch {}
    try {
      if (isRoutesMode() && String(p || "").startsWith("scrobble.watch.filters.")) {
        // Persist filter edits into the active route 
        syncActiveRouteFromView();
      }
    } catch {}
    try {
      syncHiddenServerInputs();
    } catch {}
    try {
      if (!STATE._pfMute && !isRoutesMode() && String(p || "").startsWith("scrobble.watch.filters.")) {
        saveCurrentProviderFilters();
      }
    } catch {}
  }

  const asArray = (v) => (Array.isArray(v) ? v.slice() : v == null || v === "" ? [] : [String(v)]);


  function pfLoadStore() {
    if (STATE.pf?.loaded) return;
    STATE.pf.loaded = true;
    try {
      const raw = localStorage.getItem(STATE.pf.key);
      const obj = raw ? JSON.parse(raw) : {};
      if (obj && typeof obj === "object") STATE.pf.store = obj;
    } catch {
      STATE.pf.store = {};
    }
  }

  function pfSaveStore() {
    try {
      localStorage.setItem(STATE.pf.key, JSON.stringify(STATE.pf.store || {}));
    } catch {}
  }

  function snapshotWatchFilters() {
    return {
      username_whitelist: asArray(read("scrobble.watch.filters.username_whitelist", [])),
      server_uuid: String(read("scrobble.watch.filters.server_uuid", "") || "").trim(),
      user_id: String(read("scrobble.watch.filters.user_id", "") || "").trim(),
    };
  }

  function saveCurrentProviderFilters(provOverride) {
    pfLoadStore();
    const prov = String(provOverride || provider() || "plex").toLowerCase().trim();
    if (!["plex", "emby", "jellyfin"].includes(prov)) return;
    STATE.pf.store ||= {};
    STATE.pf.store[prov] = snapshotWatchFilters();
    pfSaveStore();
  }

  function applyProviderFilters(provOverride) {
    pfLoadStore();
    const prov = String(provOverride || provider() || "plex").toLowerCase().trim();
    if (!["plex", "emby", "jellyfin"].includes(prov)) return;

    const snap = STATE.pf.store?.[prov] || null;
    STATE._pfMute = true;
    try {
      if (snap) {
        write("scrobble.watch.filters.username_whitelist", asArray(snap.username_whitelist));
        if (prov === "plex") {
          write("scrobble.watch.filters.server_uuid", String(snap.server_uuid || "").trim());
          write("scrobble.watch.filters.user_id", "");
        } else {
          const uid = String(snap.user_id || snap.server_uuid || "").trim();
          write("scrobble.watch.filters.user_id", uid);
          write("scrobble.watch.filters.server_uuid", uid);
        }
      } else {
        write("scrobble.watch.filters.username_whitelist", []);
        write("scrobble.watch.filters.server_uuid", "");
        write("scrobble.watch.filters.user_id", "");
      }
    } finally {
      STATE._pfMute = false;
    }
    saveCurrentProviderFilters(prov);
  }
  const clamp100 = (n) => Math.min(100, Math.max(1, Math.round(Number(n))));
  const norm100 = (n, dflt) => clamp100(Number.isFinite(+n) ? +n : dflt);
  const clampRange = (n, min, max) => Math.min(max, Math.max(min, Math.round(Number(n))));
  const normRange = (n, dflt, min, max) => clampRange(Number.isFinite(+n) ? +n : dflt, min, max);
  const provider = () => String(read("scrobble.watch.provider", "plex") || "plex").toLowerCase();
  const providerMeta = () => window.CW?.ProviderMeta || {};
  const providerLabel = (name) => providerMeta().label?.(name) || String(name || "");
  const providerLogLogo = (name) => providerMeta().logLogoPath?.(name) || "";
  const scrobblerSinkKeys = () => {
    const meta = providerMeta();
    if (typeof meta.scrobblerSinks === "function") {
      const keys = meta.scrobblerSinks().map((key) => String(key || "").toLowerCase()).filter(Boolean);
      if (keys.length) return keys;
    }
    return ["trakt", "simkl", "mdblist"];
  };
  const providerLogImg = (name, cls = "wh-logo") => {
    const src = providerLogLogo(name);
    const label = providerLabel(name);
    return src ? `<img class="${cls}" src="${src}" alt="${label}">` : "";
  };

  const PROVIDER_META = {
    plex: { label: "Plex", icon: providerLogLogo("plex"), alt: "Plex" },
    emby: { label: "Emby", icon: providerLogLogo("emby"), alt: "Emby" },
    jellyfin: { label: "Jellyfin", icon: providerLogLogo("jellyfin"), alt: "Jellyfin" },
  };

  const SINK_META = {
    trakt: { label: "Trakt", icon: providerLogLogo("trakt"), alt: "Trakt" },
    simkl: { label: "SIMKL", icon: providerLogLogo("simkl"), alt: "SIMKL" },
    mdblist: { label: "MDBList", icon: providerLogLogo("mdblist"), alt: "MDBList" },
  };
  function makeRouteIconDropdown(sel, metaMap, labelFallback) {
    const host = el("div", { className: "sc-route-select-host" });
    host.appendChild(sel);
    const helper = w.CW?.IconSelect?.enhance;
    if (typeof helper === "function") {
      helper(sel, {
        className: "sc-route-icon-select",
        getOptionData: (value, opt) => {
          const v = String(value || "").toLowerCase().trim();
          const key = v === "embv" ? "emby" : v;
          const meta = metaMap[key] || { label: opt?.textContent || labelFallback || v, icon: providerLogLogo(key), alt: opt?.textContent || v };
          return {
            label: meta.label || opt?.textContent || labelFallback || v || "Select",
            icons: meta.icon ? [{ src: meta.icon, alt: meta.alt || meta.label || v }] : [],
            disabled: !!opt?.disabled,
          };
        },
      });
    }
    return host;
  }


  function syncProviderPickerUi() {
    const sel = $("#sc-provider", STATE.mount);
    const btn = $("#sc-provider-btn", STATE.mount);
    const icon = $("#sc-provider-icon", STATE.mount);
    const label = $("#sc-provider-label", STATE.mount);
    const menu = $("#sc-provider-menu", STATE.mount);
    const v = String(sel?.value || provider() || "plex").toLowerCase().trim();
    const meta = PROVIDER_META[v] || PROVIDER_META.plex;

    if (icon) {
      icon.src = meta.icon;
      icon.alt = meta.alt;
    }
    if (label) label.textContent = meta.label;
    if (btn) btn.title = `Pick ${meta.label} provider`;

    if (menu) {
      $all(".sc-prov-item[data-value]", menu).forEach((it) => {
        const iv = String(it.getAttribute("data-value") || "").toLowerCase().trim();
        it.setAttribute("aria-selected", iv === v ? "true" : "false");
      });
    }
  }

  function closeProviderMenu() {
    const menu = $("#sc-provider-menu", STATE.mount);
    const btn = $("#sc-provider-btn", STATE.mount);
    if (menu) menu.classList.add("hidden");
    if (btn) btn.setAttribute("aria-expanded", "false");
  }

  function toggleProviderMenu() {
    const menu = $("#sc-provider-menu", STATE.mount);
    const btn = $("#sc-provider-btn", STATE.mount);
    if (!menu || !btn) return;
    const open = menu.classList.contains("hidden");
    if (open) {
      menu.classList.remove("hidden");
      btn.setAttribute("aria-expanded", "true");
      syncProviderPickerUi();
    } else {
      closeProviderMenu();
    }
  }
  const SINK_ORDER = scrobblerSinkKeys();
  function normSinkCsv(raw) {
    const parts = String(raw || "")
      .split(",")
      .map((s) => s.trim().toLowerCase())
      .filter(Boolean);
    const uniq = [...new Set(parts)];
    uniq.sort((a, b) => {
      const ia = SINK_ORDER.indexOf(a);
      const ib = SINK_ORDER.indexOf(b);
      if (ia === -1 && ib === -1) return a.localeCompare(b);
      if (ia === -1) return 1;
      if (ib === -1) return -1;
      return ia - ib;
    });
    return uniq.join(",");
  }
  function normSinkCsvOrDefault(raw, dflt = "trakt") {
    const v = normSinkCsv(raw);
    return v || String(dflt || "trakt").toLowerCase();
  }

  function ensureSinkPillBar(bar) {
    if (!bar || bar.dataset.scBuilt === "1") return;
    bar.dataset.scBuilt = "1";
    bar.innerHTML = SINK_ORDER.map((k) => `<button type="button" class="sc-pill off" data-sink="${k}" aria-pressed="false">${providerLabel(k)}</button>`).join("");
  }

  function csvFromSelect(sel, allowNone = false) {
    const raw = String(sel?.value || "").toLowerCase().trim();
    if (allowNone && raw === "none") return "";
    return normSinkCsv(raw);
  }

  function syncPillBar(bar, csv) {
    if (!bar) return;
    ensureSinkPillBar(bar);
    const on = new Set(String(csv || "").split(",").filter(Boolean));
    $all("button[data-sink]", bar).forEach((btn) => {
      const k = String(btn.getAttribute("data-sink") || "");
      const active = on.has(k);
      btn.classList.toggle("on", active);
      btn.classList.toggle("off", !active);
      btn.setAttribute("aria-pressed", active ? "true" : "false");
    });
  }

  function syncSinkPillsFromSelect() {
    const sel = $("#sc-sink", STATE.mount);
    const bar = $("#sc-sink-pills", STATE.mount);
    if (!sel || !bar) return;
    syncPillBar(bar, normSinkCsv(sel.value));
  }

  function syncPlexRatingsPillsFromSelect() {
    const sel = $("#sc-plex-ratings", STATE.mount);
    const bar = $("#sc-plex-ratings-pills", STATE.mount);
    if (!sel || !bar) return;
    syncPillBar(bar, csvFromSelect(sel, true));
  }


  const API = {
    cfgGet: () => j("/api/config"),
    providerInstances: (p) => j(`/api/provider-instances/${encodeURIComponent(String(p || ""))}?ts=${Date.now()}`),
    users: async (instanceId) => {
  const prov = provider();
  const inst = String(instanceId || "default");
  if (prov === "emby") {
    const x = await j(`/api/emby/users?instance=${encodeURIComponent(inst)}`);
    const a = Array.isArray(x) ? x : Array.isArray(x?.users) ? x.users : [];
    return Array.isArray(a) ? a : [];
  }
  if (prov === "jellyfin") {
    const x = await j(`/api/jellyfin/users?instance=${encodeURIComponent(inst)}`);
    const a = Array.isArray(x) ? x : Array.isArray(x?.users) ? x.users : [];
    return Array.isArray(a) ? a : [];
  }
  const x = await j(`/api/plex/users?instance=${encodeURIComponent(inst)}`);
  const a = Array.isArray(x) ? x : Array.isArray(x?.users) ? x.users : [];
  return Array.isArray(a) ? a : [];
},
serverUUID: async (instanceId) => {
  const prov = provider();
  const inst = String(instanceId || "default");
  if (prov === "emby") {
    const x = await j(`/api/emby/inspect?instance=${encodeURIComponent(inst)}`);
    const uid = x?.user_id || x?.user?.Id || x?.id || "";
    return { id: uid };
  }
  if (prov === "jellyfin") {
    const x = await j(`/api/jellyfin/inspect?instance=${encodeURIComponent(inst)}`);
    const uid = x?.user_id || x?.user?.Id || x?.id || "";
    return { id: uid };
  }
  // Plex: inspect no longer returns server UUID; use the dedicated endpoint.
  const x = await j(`/api/plex/server_uuid?instance=${encodeURIComponent(inst)}`);
  return { server_uuid: x?.server_uuid || x?.uuid || x?.serverUUID || "" };
},
    watch: {
      status: () => j("/api/watch/status"),
      start: (prov, sink) => (prov && sink) ? j(`/api/watch/start?provider=${encodeURIComponent(prov)}&sink=${encodeURIComponent(sink)}`, { method: "POST" }) : j("/api/watch/start", { method: "POST" }),
      stop: () => j("/api/watch/stop", { method: "POST" }),
    },
  };

  async function persistConfigPaths(pairs, noteId) {
    try {
      const serverCfg = await API.cfgGet();
      const cfg = typeof structuredClone === "function" ? structuredClone(serverCfg || {}) : JSON.parse(JSON.stringify(serverCfg || {}));
      for (const [path, value] of pairs || []) deepSet(cfg, String(path || ""), value);
      const r = await fetch("/api/config", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        cache: "no-store",
        body: JSON.stringify(cfg),
      });
      if (!r.ok) throw new Error(`POST /api/config ${r.status}`);
    } catch (e) {
      console.warn("[scrobbler] save failed:", e);
      if (noteId) setNote(noteId, "Couldn’t save settings. Hit Save or check logs.", "err");
    }
  }

  
  // Routes mode support
  const ROUTES_NOTICE_KEY = "cw.scrobble.migrated_notice.v1";
  const ROUTES_TAB_KEY = "cw.ui.scrobbler.routes.active.v1";
  const ROUTE_PROVIDERS = ["plex", "emby", "jellyfin"];
  const ROUTE_SINKS = ["trakt", "simkl", "mdblist"];

  const hasOwn = (o, k) => !!o && Object.prototype.hasOwnProperty.call(o, k);

  function isRoutesMode() {
    const wcfg = STATE.cfg?.scrobble?.watch;
    return !!wcfg && hasOwn(wcfg, "routes") && Array.isArray(wcfg.routes);
  }

  function getRoutes() {
    const wcfg = STATE.cfg?.scrobble?.watch;
    return Array.isArray(wcfg?.routes) ? wcfg.routes : [];
  }

  function setRoutes(routes) {
    STATE.cfg.scrobble ||= {};
    STATE.cfg.scrobble.watch ||= {};
    STATE.cfg.scrobble.watch.routes = Array.isArray(routes) ? routes : [];
  }

  function deepClone(v) {
    try { return typeof structuredClone === "function" ? structuredClone(v) : JSON.parse(JSON.stringify(v)); } catch { return v; }
  }
  function canonicalInstanceId(id){
    const fn = (window.cwMediaUserPicker && typeof window.cwMediaUserPicker.canonicalInstanceId === "function") ? window.cwMediaUserPicker.canonicalInstanceId : null;
    if (fn) return fn(id);
    const s = String(id || "").trim();
    const out = s.replace(/:\d+$/, "").trim();
    return out || "default";
  }


  function normalizeRoute(r, idFallback) {
    const x = (r && typeof r === "object") ? r : {};
    const p0 = x.provider;
    const s0 = x.sink;
    const pi0 = x.provider_instance;
    const si0 = x.sink_instance;
    const p = (p0 === undefined || p0 === null) ? "plex" : String(p0);
    const s = (s0 === undefined || s0 === null) ? "trakt" : String(s0);
    const pi = (pi0 === undefined || pi0 === null) ? "default" : String(pi0);
    const si = (si0 === undefined || si0 === null) ? "default" : String(si0);
    return {
      id: String(x.id || idFallback || "").trim() || "R1",
      enabled: x.enabled !== false,
      provider: p.trim().toLowerCase(),
      provider_instance: canonicalInstanceId(pi),
      sink: s.trim().toLowerCase(),
      sink_instance: canonicalInstanceId(si),
      filters: deepClone(x.filters || {}),
    };
  }


  function nextRouteId() {
    const used = new Set(getRoutes().map(r => String(r?.id || "").trim()).filter(Boolean));
    let i = 1;
    while (used.has(`R${i}`)) i++;
    return `R${i}`;
  }

  function legacyToRoutesIfMissing() {
    const wcfg = STATE.cfg?.scrobble?.watch || {};
    const hasRoutesKey = hasOwn(wcfg, "routes");
    const routesArr = Array.isArray(wcfg.routes) ? wcfg.routes : null;
    if (routesArr && routesArr.length) return { migrated: false };
    if (hasRoutesKey && !routesArr) return { migrated: false };

    const prov = String(wcfg.provider || "").trim();
    const sink = String(wcfg.sink || "").trim();
    if (!prov || !sink) return { migrated: false };
    const sinks = sink.split(",").map(s => s.trim()).filter(Boolean);
    const filters = deepClone(wcfg.filters || {});
    const routes = sinks.map((s, i) => normalizeRoute({ id: `R${i + 1}`, enabled: true, provider: prov, provider_instance: "default", sink: s, sink_instance: "default", filters }, `R${i + 1}`));
    STATE.cfg.scrobble ||= {};
    STATE.cfg.scrobble.watch ||= {};
    STATE.cfg.scrobble.watch.routes = routes;
    STATE.cfg.scrobble.watch.routes_migrated_from_legacy = true;
    return { migrated: true };
  }

  function activeRouteId() {
    const routes = getRoutes();
    if (!routes.length) return null;
    const saved = String(localStorage.getItem(ROUTES_TAB_KEY) || "").trim();
    if (saved && routes.some(r => r.id === saved)) return saved;
    return routes[0].id;
  }

  function setActiveRouteId(id) {
    const rid = String(id || "").trim();
    if (!rid) return;
    localStorage.setItem(ROUTES_TAB_KEY, rid);
  }

  function getActiveRoute() {
    const rid = activeRouteId();
    if (!rid) return null;
    return getRoutes().find(r => r.id === rid) || null;
  }

  function routeLabel(r) {
    const pi = String(r.provider_instance || "default");
    const si = String(r.sink_instance || "default");
    const p0 = String(r.provider || "").trim();
    const s0 = String(r.sink || "").trim();
    const p = p0 ? p0 : "—";
    const s = s0 ? s0 : "—";
    return `${r.id} ${p}(${pi}) → ${s}(${si})`;
  }


function routeKey(r) {
  const p = String(r?.provider || "").trim().toLowerCase();
  const s = String(r?.sink || "").trim().toLowerCase();
  if (!p || !s) return "";
  const pi = String(r?.provider_instance || "default").trim().toLowerCase() || "default";
  const si = String(r?.sink_instance || "default").trim().toLowerCase() || "default";
  return `${p}|${pi}|${s}|${si}`;
}



function findDuplicateRouteKeys(routes) {
  const map = new Map();
  for (const r of (routes || [])) {
    const k = routeKey(r);
    if (!k) continue;
    const arr = map.get(k) || [];
    arr.push(String(r?.id || ""));
    map.set(k, arr);
  }
  const dups = [];
  for (const [k, ids] of map.entries()) {
    if (ids.length > 1) dups.push({ key: k, ids });
  }
  return dups;
}




  function applyRouteView(route) {
    if (!route) return;
    // Clear transient route-specific picker notes when switching routes
    try {
      setNote("sc-users-note", "");
      setNote("sc-uuid-note", "");
    } catch {}
    const prov = String(route.provider || "").trim().toLowerCase();
    const sink = String(route.sink || "").trim().toLowerCase();
    if (prov) deepSet(STATE.cfg, "scrobble.watch.provider", prov);
    if (sink) deepSet(STATE.cfg, "scrobble.watch.sink", sink);
    deepSet(STATE.cfg, "scrobble.watch.filters", deepClone(route.filters || {}));
    try {
      const pvSel = $("#sc-provider", STATE.mount);
      if (pvSel && prov) pvSel.value = prov;
      const skSel = $("#sc-sink", STATE.mount);
      if (skSel && sink) skSel.value = sink;
      try { syncProviderPickerUi(); } catch {}
      try { syncSinkPillsFromSelect(); } catch {}
    } catch {}
  }

  function syncActiveRouteFromView(filtersObj) {
    const r = getActiveRoute();
    if (!r) return;
    const f = filtersObj || read("scrobble.watch.filters", {}) || {};
    r.filters = deepClone(f);
  }

  function activeProviderInstance() {
    const r = getActiveRoute();
    return r ? canonicalInstanceId(r.provider_instance || "default") : "default";
  }

  async function getInstanceOptions(providerName) {
    const p = String(providerName || "").toLowerCase();
    if (!p) return [{ id: "default", name: "Default" }];
    STATE._routesCache ||= {};
    if (STATE._routesCache[p]) return STATE._routesCache[p];
    try {
      const x = await API.providerInstances(p);
      const items0 = Array.isArray(x) ? x : (x?.instances || []);
      const items = (items0 || []).map(i => ({
        id: String(i?.id || "").trim(),
        name: String(i?.label || i?.name || i?.id || "").trim() || String(i?.id || "").trim(),
      })).filter(i => i.id);

      // Deduplicate by id
      const seen = new Set();
      const uniq = [];
      for (const it of items) {
        const k = it.id.toLowerCase();
        if (seen.has(k)) continue;
        seen.add(k);
        uniq.push(it);
      }

      const hasDefault = uniq.some(i => i.id.toLowerCase() === "default");
      const def = { id: "default", name: "Default" };
      const list = hasDefault
        ? uniq.map(i => (i.id.toLowerCase() === "default" ? { id: "default", name: i.name || "Default" } : i))
        : [def].concat(uniq);

      STATE._routesCache[p] = list;
      return list;
    } catch {
      const fallback = [{ id: "default", name: "Default" }];
      STATE._routesCache[p] = fallback;
      return fallback;
    }
  }

  function overlayCfgFor(name, inst) {
    const p = String(name || "").toLowerCase();
    const iid = String(inst || "default");
    const base = (STATE.cfg && STATE.cfg[p]) ? STATE.cfg[p] : {};
    if (iid && iid !== "default" && base?.instances && base.instances[iid]) return Object.assign({}, base, base.instances[iid]);
    return base || {};
  }

  function activeRouteContext() {
    if (!isRoutesMode()) return { provider_instance: "default", sink_instance: "default" };
    const r = getActiveRoute() || null;
    return { provider_instance: String(r?.provider_instance || "default"), sink_instance: String(r?.sink_instance || "default") };
  }

  function activeProviderServerUrl() {
    const prov = provider();
    const ctx = activeRouteContext();
    if (prov === "plex") return String(overlayCfgFor("plex", ctx.provider_instance)?.server_url || "");
    if (prov === "emby") return String(overlayCfgFor("emby", ctx.provider_instance)?.server || "");
    return String(overlayCfgFor("jellyfin", ctx.provider_instance)?.server || "");
  }

  function syncServerPreviewUi() {
    const inp = $("#sc-pms-input", STATE.mount);
    if (!inp) return;
    inp.value = activeProviderServerUrl();
    inp.disabled = true;
  }

  function syncRouteActiveRowUi(rid) {
    const host = $("#sc-routes", STATE.mount);
    if (!host) return;
    $all("tr.sc-route-row", host).forEach((tr) => {
      const id = String(tr?.dataset?.rid || "").trim();
      tr.classList.toggle("sc-route-active", id && id === String(rid || "").trim());
    });
  }

  function setActiveRouteFromUi(rid) {
    const id = String(rid || "").trim();
    if (!id || id === activeRouteId()) return;
    try { syncActiveRouteFromView(); } catch {}
    setActiveRouteId(id);
    const r = getActiveRoute();
    if (r) applyRouteView(r);
    try {
      const sel = $("#sc-route-select", STATE.mount);
      if (sel) sel.value = id;
    } catch {}
    try { syncRouteActiveRowUi(id); } catch {}
    try { syncServerPreviewUi(); } catch {}
    try { applyModeDisable(); } catch {}
  }

  function providerAuthOkForRoute(r) {
    const p = String(r?.provider || "").toLowerCase();
    const ov = overlayCfgFor(p, r?.provider_instance);
    if (p === "plex") return !!String(ov.account_token || "").trim() && isValidServerUrl(String(ov.server_url || ""));
    if (p === "emby") return !!String(ov.access_token || "").trim();
    if (p === "jellyfin") return !!String(ov.access_token || "").trim();
    return false;
  }

  function sinkAuthOkForRoute(r) {
    const s = String(r?.sink || "").toLowerCase();
    const ov = overlayCfgFor(s, r?.sink_instance);
    if (s === "trakt") return !!String(ov.access_token || "").trim();
    if (s === "simkl") return !!String(ov.access_token || "").trim();
    if (s === "mdblist") return !!String(ov.api_key || "").trim();
    return false;
  }

  function anyStartableRoute() {
    return getRoutes().some(r => r?.enabled && providerAuthOkForRoute(r) && sinkAuthOkForRoute(r));
  }

  async function renderRouteSelector() {
    const wrap = $("#sc-route-filter-wrap", STATE.mount);
    const sel = $("#sc-route-select", STATE.mount);
    if (!wrap || !sel) return;
    const on = isRoutesMode();
    wrap.style.display = on ? "" : "none";
    if (!on) return;
    sel.innerHTML = "";
    const routes = getRoutes();
    routes.forEach((r) => sel.appendChild(el("option", { value: r.id, textContent: routeLabel(r) })));
    const rid = activeRouteId();
    if (rid) sel.value = rid;
  }

  function renderMigrateBanner(migrated) {
    const host = $("#sc-migrate-banner", STATE.mount);
    if (!host) return;
    if (!migrated) { host.style.display = "none"; host.innerHTML = ""; return; }
    if (localStorage.getItem(ROUTES_NOTICE_KEY) === "1") { host.style.display = "none"; host.innerHTML = ""; return; }

    host.style.display = "";
    host.innerHTML = `<div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap"><span>Routes were migrated from legacy watcher config.</span><button type="button" id="sc-migrate-save" class="btn small">Upgrade watcher config</button><button type="button" id="sc-migrate-dismiss" class="btn small">Dismiss</button></div>`;
    const saveBtn = $("#sc-migrate-save", host);
    const disBtn = $("#sc-migrate-dismiss", host);
    on(saveBtn, "click", async () => {
      localStorage.setItem(ROUTES_NOTICE_KEY, "1");
      try {
        const sc = getScrobbleConfig();
        const serverCfg = await API.cfgGet();
        const cfg = deepClone(serverCfg || {});
        cfg.scrobble = sc;
        const rp = getRootPatch();
        cfg.plex = Object.assign({}, cfg.plex || {}, rp.plex || {});
        cfg.emby = Object.assign({}, cfg.emby || {}, rp.emby || {});
        cfg.jellyfin = Object.assign({}, cfg.jellyfin || {}, rp.jellyfin || {});
        const r = await fetch("/api/config", { method: "POST", headers: { "Content-Type": "application/json" }, cache: "no-store", body: JSON.stringify(cfg) });
        if (!r.ok) throw new Error(`POST /api/config ${r.status}`);
        w._cfgCache = cfg;
        STATE.cfg = cfg;
        setNote("sc-note", "Watcher config upgraded.");
      } catch {
        setNote("sc-note", "Couldn’t upgrade watcher config. Hit Save or check logs.", "err");
      }
      host.style.display = "none";
      host.innerHTML = "";
    });
    on(disBtn, "click", () => {
      localStorage.setItem(ROUTES_NOTICE_KEY, "1");
      host.style.display = "none";
      host.innerHTML = "";
    });
  }

  async function renderRoutesUi() {
    const wrap = $("#sc-routes-wrap", STATE.mount);
    const host = $("#sc-routes", STATE.mount);
    const legacy = $("#sc-legacy-picks", STATE.mount);
    const onMode = isRoutesMode();
    if (wrap) wrap.style.display = onMode ? "" : "none";
    if (legacy) legacy.style.display = onMode ? "none" : "";
    if (!host) return;
    if (!onMode) { host.innerHTML = ""; return; }

try {
  if (!d.getElementById("sc-routes-style")) {
    const st = d.createElement("style");
    st.id = "sc-routes-style";
    st.textContent = ".sc-route-row{cursor:pointer}.sc-route-active{box-shadow:0 0 0 2px rgba(124,92,255,.38) inset}.sc-route-dup{outline:2px solid rgba(220,53,69,.6);border-radius:6px}.sc-dup-badge{display:inline-block;font-size:11px;padding:2px 6px;border-radius:10px;background:rgba(220,53,69,.15);color:#dc3545;margin-right:8px}";
    d.head.appendChild(st);
  }
} catch {}

    const routes = getRoutes().map((r, i) => normalizeRoute(r, `R${i + 1}`));
    setRoutes(routes);

    const dups = findDuplicateRouteKeys(routes);
    STATE._dupRouteIds = new Set(dups.flatMap(d => d.ids || []));

    const rid0 = activeRouteId();
    if (!rid0 && routes.length) setActiveRouteId(routes[0].id);

    // Build table
    const table = el("table");
    const thead = el("thead");
    thead.innerHTML = "<tr><th>On</th><th>Provider</th><th>Profile</th><th>Sink</th><th>Profile</th><th></th></tr>";
    table.appendChild(thead);
    const tbody = el("tbody");
    table.appendChild(tbody);

    const activeRid = activeRouteId();

    for (const r of routes) {
            const isActive = String(r.id || "") === String(activeRid || "");
      const isDup = STATE._dupRouteIds && STATE._dupRouteIds.has(String(r.id || ""));
      const tr = el("tr", { className: "sc-route-row" + (isActive ? " sc-route-active" : "") + (isDup ? " sc-route-dup" : "") });
      tr.dataset.rid = r.id;

      const cOn = el("td");
      const chk = bindFieldIdentity(el("input", { type: "checkbox", checked: !!r.enabled }), "sc_route_enabled", r.id, "route");
      chk.dataset.rid = r.id;
      chk.dataset.f = "enabled";
      cOn.appendChild(chk);
      tr.appendChild(cOn);

      const cP = el("td");
      const pSel = bindFieldIdentity(el("select", { className: "input" }), "sc_route_provider", r.id, "route");
      pSel.appendChild(el("option", { value: "", textContent: "Select…" }));
      ROUTE_PROVIDERS.forEach((p) => {
        const meta = PROVIDER_META[p] || { label: p, icon: "", alt: p };
        pSel.appendChild(el("option", { value: p, textContent: meta.label || p }));
      });
      pSel.value = r.provider;
      pSel.dataset.rid = r.id;
      pSel.dataset.f = "provider";
      cP.appendChild(makeRouteIconDropdown(pSel, PROVIDER_META, "Provider"));
      tr.appendChild(cP);

      const cPI = el("td");
      const piSel = bindFieldIdentity(el("select", { className: "input" }), "sc_route_provider_instance", r.id, "route");
      const hasP = !!String(r.provider || "").trim();
      const pOpts = hasP ? await getInstanceOptions(r.provider) : [{ id: "default", name: "Default" }];
      pOpts.forEach(i => piSel.appendChild(el("option", { value: i.id, textContent: i.name })));
      piSel.value = r.provider_instance || "default";
      piSel.disabled = !hasP;
      piSel.dataset.rid = r.id;
      piSel.dataset.f = "provider_instance";
      cPI.appendChild(piSel);
      tr.appendChild(cPI);

      const cS = el("td");
      const sSel = bindFieldIdentity(el("select", { className: "input" }), "sc_route_sink", r.id, "route");
      sSel.appendChild(el("option", { value: "", textContent: "Select…" }));
      ROUTE_SINKS.forEach((s) => {
        const meta = SINK_META[s] || { label: s, icon: "", alt: s };
        sSel.appendChild(el("option", { value: s, textContent: meta.label || s }));
      });
      sSel.value = r.sink;
      sSel.dataset.rid = r.id;
      sSel.dataset.f = "sink";
      cS.appendChild(makeRouteIconDropdown(sSel, SINK_META, "Sink"));
      tr.appendChild(cS);

      const cSI = el("td");
      const siSel = bindFieldIdentity(el("select", { className: "input" }), "sc_route_sink_instance", r.id, "route");
      const hasS = !!String(r.sink || "").trim();
      const sOpts = hasS ? await getInstanceOptions(r.sink) : [{ id: "default", name: "Default" }];
      sOpts.forEach(i => siSel.appendChild(el("option", { value: i.id, textContent: i.name })));
      siSel.value = r.sink_instance || "default";
      siSel.disabled = !hasS;
      siSel.dataset.rid = r.id;
      siSel.dataset.f = "sink_instance";
      cSI.appendChild(siSel);
      tr.appendChild(cSI);

      const cA = el("td", { className: "sc-route-actions" });
      const filt = el("button", { type: "button", className: "btn small", textContent: "Filters" });
      filt.dataset.act = "filters";
      filt.dataset.rid = r.id;
      const rm = el("button", { type: "button", className: "btn small", textContent: "Remove" });
      rm.dataset.act = "remove";
      rm.dataset.rid = r.id;
      cA.append(filt, rm);
      tr.appendChild(cA);

      tbody.appendChild(tr);
    }

    host.innerHTML = "";
    host.appendChild(table);
    try { host.classList.toggle("sc-routes-auto", routes.length <= 3); } catch {}

    // Keep active view valid
    const rid = activeRouteId();
    if (!rid && routes.length) setActiveRouteId(routes[0].id);
    const ar = getActiveRoute() || routes[0] || null;
    if (ar) applyRouteView(ar);

    await renderRouteSelector();
    applyModeDisable();
  }
function chip(text, onRemove, onClick) {
    const c = el("span", { className: "chip" });
    const t = el("span", { textContent: text });
    if (onClick) {
      t.style.cursor = "pointer";
      t.title = "Click to select";
      on(t, "click", () => onClick(text));
    }
    const rm = el("span", { className: "rm", textContent: "×" });
    on(rm, "click", () => onRemove(text));
    c.append(t, rm);
    return c;
  }

  function isValidServerUrl(s) {
    try {
      const u = new URL(String(s || "").trim());
      return u.protocol === "http:" || u.protocol === "https:";
    } catch {
      return false;
    }
  }

  function setWatcherStatus(st) {
    const dot = $("#sc-status-dot", STATE.mount);
    const badge = $("#sc-status-badge", STATE.mount);
    const text = $("#sc-status-text", STATE.mount);
    const last = $("#sc-status-last", STATE.mount);
    const up = $("#sc-status-up", STATE.mount);
    const alive = !!st?.alive;
    if (dot) dot.className = "status-dot " + (alive ? "on" : "off");
    if (badge) {
      badge.className = "badge " + (alive ? "is-on" : "is-off");
      badge.textContent = alive ? "Running" : "Stopped";
    }
    if (text) text.textContent = alive ? "Active" : "Inactive";
    if (last) last.textContent = st?.last_run ? `Last: ${st.last_run}` : "";
    if (up) up.textContent = st?.uptime ? `Up: ${st.uptime}` : "";
  }

  function applyModeDisable() {
  const enabled = !!read("scrobble.enabled", false);
  const mode = String(read("scrobble.mode", "webhook")).toLowerCase();
  const useWebhook = enabled && mode === "webhook";
  const useWatch = enabled && mode === "watch";

  const webRoot = STATE.webhookHost;
  const watchRoot = STATE.watcherHost;
  if (!webRoot || !watchRoot) return;

  const wh = $("#sc-enable-webhook", STATE.mount);
  const wa = $("#sc-enable-watcher", STATE.mount);

  const webhookOn = !!wh?.checked && useWebhook;
  const watcherOn = !!wa?.checked && useWatch;

  // Routes mode: show routes editor and per-route filters
  try {
    const routesMode = isRoutesMode();
    const legacy = $("#sc-legacy-picks", STATE.mount);
    const routesWrap = $("#sc-routes-wrap", STATE.mount);
    const routeFilterWrap = $("#sc-route-filter-wrap", STATE.mount);
    if (legacy) legacy.style.display = routesMode ? "none" : "";
    if (routesWrap) routesWrap.style.display = routesMode ? "" : "none";
    if (routeFilterWrap) routeFilterWrap.style.display = routesMode ? "" : "none";
  } catch {}

  $all(".input, input, button, select, textarea", webRoot).forEach((n) => {
    if (!String(n.id || "").startsWith("sc-enable-webhook")) n.disabled = !webhookOn;
  });
  $all(".input, input, button, select, textarea", watchRoot).forEach((n) => {
    if (n.id !== "sc-enable-watcher") n.disabled = !watcherOn;
  });

  const prov = provider();
  const ctx = activeRouteContext();
  const provInst = isRoutesMode() ? ctx.provider_instance : "default";
  const sinkInst = isRoutesMode() ? ctx.sink_instance : "default";

  const plexCfg = overlayCfgFor("plex", provInst);
  const embyCfg = overlayCfgFor("emby", provInst);
  const jellyCfg = overlayCfgFor("jellyfin", provInst);

  const srv =
    prov === "plex"
      ? String(plexCfg?.server_url || read("plex.server_url", "") || "")
      : prov === "emby"
      ? String(embyCfg?.server || read("emby.server", "") || "")
      : String(jellyCfg?.server || read("jellyfin.server", "") || "");
  const lbl = prov === "plex" ? "Plex Server" : prov === "emby" ? "Emby Server" : "Jellyfin Server";
  const req = $("#sc-server-required", STATE.mount);
  if (req) req.style.display = prov === "plex" && String(req.textContent || "").trim() ? "" : "none";
  const lab = $("#sc-server-label", STATE.mount);
  if (lab) lab.textContent = lbl;

  const loadBtn = $("#sc-load-users", STATE.mount);
  if (loadBtn) {
    loadBtn.style.display = "";
    loadBtn.textContent = "Pick";
    loadBtn.title = prov === "plex" ? "Pick Plex user" : prov === "emby" ? "Pick Emby user" : "Pick Jellyfin user";
  }
  const fetchUuid = $("#sc-fetch-uuid", STATE.mount);
  if (fetchUuid) fetchUuid.disabled = false;
  const uuidLabel = $("#sc-uuid-label", STATE.mount);
  if (uuidLabel) uuidLabel.textContent = prov === "plex" ? "Server UUID" : "User ID";
  const uuidInput = $("#sc-server-uuid", STATE.mount);
  if (uuidInput) uuidInput.placeholder = prov === "plex" ? "e.g. abcd1234..." : "e.g. 80ee72c0...";

  const plexTokenOk = !!String(plexCfg?.account_token || read("plex.account_token", "") || "").trim();
  const embyTokenOk = !!String(embyCfg?.access_token || read("emby.access_token", "") || "").trim();
  const jellyTokenOk = !!String(jellyCfg?.access_token || read("jellyfin.access_token", "") || "").trim();

  const sinkRaw = read("scrobble.watch.sink", "trakt");
  const sink = normSinkCsv(sinkRaw == null ? "trakt" : sinkRaw);
  const hasSink = !!sink;

  const traktCfg = overlayCfgFor("trakt", sinkInst);
  const simklCfg = overlayCfgFor("simkl", sinkInst);
  const mdblCfg = overlayCfgFor("mdblist", sinkInst);

  const traktTokenOk = !!String(traktCfg?.access_token || read("trakt.access_token", "") || "").trim();
  const simklTokenOk = !!String(simklCfg?.access_token || read("simkl.access_token", "") || "").trim();
  const mdblTokenOk = !!String(mdblCfg?.api_key || read("mdblist.api_key", "") || "").trim();

  let sinkOk = true;
  let sinkErr = "";

  if (!hasSink) {
    sinkOk = false;
  } else {
    const wantsTrakt = sink.includes("trakt");
    const wantsSimkl = sink.includes("simkl");
    const wantsMDBList = sink.includes("mdblist");

    const missing = [];
    if (wantsTrakt && !traktTokenOk) missing.push("Trakt");
    if (wantsSimkl && !simklTokenOk) missing.push("SIMKL");
    if (wantsMDBList && !mdblTokenOk) missing.push("MDBList");

    if (missing.length) {
      sinkOk = false;
      const plural = missing.length > 1 ? "are" : "is";
      sinkErr = `${missing.join(" and ")} ${plural} not configured. Go to Authentication and configure it, or refresh your browser if you already configured it.`;
    }
  }

  rebuildPlexRatingsDropdown();

  if (watcherOn && !hasSink) setNote("sc-note", "You must select at least one sink to start the watcher.", "warn");
  else setNote("sc-note", "");

  if (watcherOn) {
    if (prov === "plex") {
      if (!plexTokenOk) setNote("sc-pms-note", "Not connected to Plex. Go to Authentication - Plex, or refresh your browser if you already configured it", "err");
      else if (!isValidServerUrl(srv)) setNote("sc-pms-note", "Plex Server is required (http(s)://…)", "err");
      else if (hasSink && !sinkOk) setNote("sc-pms-note", sinkErr, "err");
      else setNote("sc-pms-note", "");
    } else if (prov === "emby") {
      if (!embyTokenOk) setNote("sc-pms-note", "Not connected to Emby. Go to Authentication - Emby, or refresh your browser if you already configured it", "err");
      else if (hasSink && !sinkOk) setNote("sc-pms-note", sinkErr, "err");
      else setNote("sc-pms-note", "");
    } else {
      if (!jellyTokenOk) setNote("sc-pms-note", "Not connected to Jellyfin. Go to Authentication - Jellyfin, or refresh your browser if you already configured it", "err");
      else if (hasSink && !sinkOk) setNote("sc-pms-note", sinkErr, "err");
      else setNote("sc-pms-note", "");
    }
  } else {
    setNote("sc-pms-note", "");
    setNote("sc-note", "");
  }

  if (loadBtn) {
    if (prov === "plex" && !plexTokenOk) loadBtn.disabled = true;
    else if (prov === "emby" && !embyTokenOk) loadBtn.disabled = true;
    else if (prov === "jellyfin" && !jellyTokenOk) loadBtn.disabled = true;
    else loadBtn.disabled = !watcherOn;
  }
  if (fetchUuid) {
    if (prov === "plex" && !plexTokenOk) fetchUuid.disabled = true;
    else if (prov === "emby" && !embyTokenOk) fetchUuid.disabled = true;
    else if (prov === "jellyfin" && !jellyTokenOk) fetchUuid.disabled = true;
    else fetchUuid.disabled = !watcherOn;
  }

  const auto = $("#sc-autostart", STATE.mount);
  if (auto) {
    if (watcherOn && !hasSink) {
      auto.checked = false;
      auto.disabled = true;
      if (!!read("scrobble.watch.autostart", false)) {
        write("scrobble.watch.autostart", false);
        STATE.ui.watchAutostart = false;
        if (!STATE._noSinkAutostartFixApplied) {
          STATE._noSinkAutostartFixApplied = true;
          persistConfigPaths([["scrobble.watch.autostart", false]], "sc-pms-note");
        }
      }
    } else {
      auto.disabled = !watcherOn;
    }
  }

  const startBtn = $("#sc-watch-start", STATE.mount);
  if (startBtn) {
    if (isRoutesMode()) {
      startBtn.disabled = !watcherOn || !anyStartableRoute();
    } else {
      const providerOk = prov === "plex" ? plexTokenOk && isValidServerUrl(srv) : prov === "emby" ? embyTokenOk : jellyTokenOk;
      startBtn.disabled = !watcherOn || !providerOk || !sinkOk;
    }
  }
}


  function buildAdvField(id, label, tipId, placeholder, opts = {}) {
    const min = Number.isFinite(+opts.min) ? +opts.min : 1;
    const max = Number.isFinite(+opts.max) ? +opts.max : 100;
    const step = Number.isFinite(+opts.step) ? +opts.step : 1;
    return `<div class="field"><label for="${id}">${label}</label>${helpBtn(tipId)}<input id="${id}" class="input" type="number" inputmode="numeric" min="${min}" max="${max}" step="${step}" placeholder="${placeholder}"></div>`;
  }

  function buildHeaderToggle(id, text) {
    return `<label class="cx-toggle sc-shell-toggle"><input type="checkbox" id="${id}"><span class="cx-toggle-ui" aria-hidden="true"></span><span class="cx-toggle-text">${text}</span><span class="cx-toggle-state" aria-hidden="true"></span></label>`;
  }

  function buildShellHeader({ kicker = "", title = "", copy = "", tiles = "", toggleId = "", toggleText = "Enable", tilesLabel = "Sections" } = {}) {
    return `<div class="cw-panel-head sc-shell-head"><div class="sc-shell-head-copy">${kicker ? `<div class="sc-shell-head-kicker">${kicker}</div>` : ""}<div class="cw-panel-title sc-shell-head-title">${title}</div><div class="muted sc-shell-head-copy-text">${copy}</div></div><div class="sc-shell-head-side">${tiles ? `<div class="cw-subtiles" aria-label="${tilesLabel}">${tiles}</div>` : ""}${toggleId ? buildHeaderToggle(toggleId, toggleText) : ""}</div></div>`;
  }
  

  function buildUI() {
    injectStyles();

        if (STATE.webhookHost) {
      STATE.webhookHost.innerHTML = `<div class="cw-panel"><div class="cw-meta-provider-panel active" data-provider="webhook"><div class="cw-panel-head"><div class="cw-panel-head-main"><div class="cw-panel-title">Webhooks</div><div class="muted">Legacy endpoints that scrobble to Trakt.</div></div><div class="cw-subtiles" aria-label="Webhook sections"><button type="button" class="cw-subtile active" data-sub="plex">Plex</button><button type="button" class="cw-subtile" data-sub="jellyfin">Jellyfin</button><button type="button" class="cw-subtile" data-sub="emby">Emby</button><button type="button" class="cw-subtile" data-sub="advanced">Advanced</button></div></div><div id="sc-webhook-warning" class="micro-note" style="margin-top:10px"></div><div id="sc-endpoint-note" class="micro-note"></div><div class="cw-subpanels" style="gap:8px"><div class="cw-subpanel active" data-sub="plex"><div class="row" style="justify-content:space-between;align-items:center;margin-top:6px"><label class="cx-toggle"><input type="checkbox" id="sc-enable-webhook"><span class="cx-toggle-ui" aria-hidden="true"></span><span class="cx-toggle-text">Enable</span><span class="cx-toggle-state" aria-hidden="true"></span></label><div class="codepair right" style="margin-left:auto">${providerLogImg("plex")}<code id="sc-webhook-url-plex"></code><button id="sc-copy-plex" class="btn small">Copy</button></div></div><div class="sc-subbox"><div class="head">Options</div><div class="body"><span class="cx-switch-wrap"><label class="sc-toggle"><input type="checkbox" id="sc-delete-plex-webhook"><span class="one-line">Auto-remove from Watchlists</span></label>${helpBtn("sc-help-auto-remove")}</span></div></div><div class="sc-subbox"><div class="head">Filters</div><div class="body"><div class="sc-filter-grid"><div><div class="muted">Username whitelist</div><div id="sc-whitelist-webhook" class="chips" style="margin-top:4px"></div><div id="sc-users-note-webhook" class="micro-note"></div><div style="display:flex;gap:8px;margin-top:6px"><input id="sc-user-input-webhook" class="input" placeholder="Add username..." style="flex:1"><button id="sc-add-user-webhook" class="btn small">Add</button><button id="sc-load-users-webhook" class="btn small">Pick</button></div></div><div><div class="muted">Server UUID</div><div id="sc-uuid-note-webhook" class="micro-note"></div><div style="display:flex;gap:8px;align-items:center;margin-top:6px"><input id="sc-server-uuid-webhook" class="input" placeholder="e.g. abcd1234..." style="flex:1"><button id="sc-fetch-uuid-webhook" class="btn small">Fetch</button></div></div></div></div></div><div class="sc-subbox"><div class="head">Plex settings</div><div class="body"><span class="cx-switch-wrap"><label class="sc-toggle"><input type="checkbox" id="sc-webhook-plex-ratings"><span class="one-line">Enable ratings</span></label>${helpBtn("sc-help-webhook-plex-ratings")}</span></div></div></div><div class="cw-subpanel" data-sub="jellyfin"><div class="row" style="justify-content:space-between;align-items:center;margin-top:6px"><label class="cx-toggle"><input type="checkbox" id="sc-enable-webhook-jf"><span class="cx-toggle-ui" aria-hidden="true"></span><span class="cx-toggle-text">Enable</span><span class="cx-toggle-state" aria-hidden="true"></span></label><div class="codepair right" style="margin-left:auto">${providerLogImg("jellyfin")}<code id="sc-webhook-url-jf"></code><button id="sc-copy-jf" class="btn small">Copy</button></div></div><div class="sc-subbox"><div class="head">Options</div><div class="body"><span class="cx-switch-wrap"><label class="sc-toggle"><input type="checkbox" id="sc-delete-plex-webhook-jf"><span class="one-line">Auto-remove from Watchlists</span></label>${helpBtn("sc-help-auto-remove")}</span></div></div></div><div class="cw-subpanel" data-sub="emby"><div class="row" style="justify-content:space-between;align-items:center;margin-top:6px"><label class="cx-toggle"><input type="checkbox" id="sc-enable-webhook-emby"><span class="cx-toggle-ui" aria-hidden="true"></span><span class="cx-toggle-text">Enable</span><span class="cx-toggle-state" aria-hidden="true"></span></label><div class="codepair right" style="margin-left:auto">${providerLogImg("emby")}<code id="sc-webhook-url-emby"></code><button id="sc-copy-emby" class="btn small">Copy</button></div></div><div class="sc-subbox"><div class="head">Options</div><div class="body"><span class="cx-switch-wrap"><label class="sc-toggle"><input type="checkbox" id="sc-delete-plex-webhook-emby"><span class="one-line">Auto-remove from Watchlists</span></label>${helpBtn("sc-help-auto-remove")}</span></div></div></div><div class="cw-subpanel" data-sub="advanced"><div class="row" style="justify-content:flex-start;align-items:center;margin-top:6px"><label class="cx-toggle"><input type="checkbox" id="sc-enable-webhook-adv"><span class="cx-toggle-ui" aria-hidden="true"></span><span class="cx-toggle-text">Enable</span><span class="cx-toggle-state" aria-hidden="true"></span></label></div><div class="sc-subbox"><div class="head">Advanced</div><div class="body"><div class="sc-adv-grid">${buildAdvField("sc-pause-debounce-webhook", "Pause", "sc-help-adv-pause", DEFAULTS.watch.pause_debounce_seconds)}${buildAdvField("sc-suppress-start-webhook", "Suppress", "sc-help-adv-suppress", DEFAULTS.watch.suppress_start_at)}${buildAdvField("sc-regress-webhook", "Regress %", "sc-help-adv-regress", DEFAULTS.trakt.regress_tolerance_percent)}${buildAdvField("sc-stop-pause-webhook", "Stop pause >=", "sc-help-adv-stop-pause", DEFAULTS.trakt.stop_pause_threshold)}${buildAdvField("sc-force-stop-webhook", "Force stop", "sc-help-adv-force-stop", DEFAULTS.trakt.force_stop_at)}</div><div class="micro-note" style="margin-top:6px">Empty resets to defaults. Values are 1�100.</div></div></div></div></div></div></div>`;

      STATE.webhookHost.querySelector(".cw-panel")?.classList.add("sc-shell");
      enhanceWebhookFiltersUI(STATE.webhookHost);

      // Tabs: Plex / Jellyfin / Emby / Advanced
      const tabKey = "cw.ui.scrobbler.webhook.tab.v1";
      const root = STATE.webhookHost;
      const selectTab = (sub, opts = {}) => {
        const want = (sub || "plex").toLowerCase();
        root.querySelectorAll('.cw-subtile[data-sub]').forEach((btn) => {
          btn.classList.toggle("active", (btn.dataset.sub || "").toLowerCase() === want);
        });
        root.querySelectorAll('.cw-subpanel[data-sub]').forEach((sp) => {
          sp.classList.toggle("active", (sp.dataset.sub || "").toLowerCase() === want);
        });
        if (opts.persist !== false) {
          try { localStorage.setItem(tabKey, want); } catch {}
        }
      };
      STATE._watcherSelectTab = selectTab;

      root.querySelectorAll('.cw-subtile[data-sub]').forEach((btn) => {
        btn.addEventListener("click", () => selectTab(btn.dataset.sub || "plex"));
      });

      try { selectTab(localStorage.getItem(tabKey) || "plex", { persist: false }); } catch { selectTab("plex", { persist: false }); }
    }

    if (STATE.watcherHost) {
      STATE.watcherHost.innerHTML = `<style> .cc-wrap{display:grid;grid-template-columns:1fr 1fr;gap:16px} .cc-card{padding:14px;border-radius:12px;background:var(--panel,#111);box-shadow:0 0 0 1px rgba(255,255,255,.05) inset} .cc-head{display:flex;justify-content:space-between;align-items:center;margin-bottom:10px} .cc-body{display:grid;gap:14px} .cc-gauge{width:100%;min-height:68px;display:flex;align-items:center;gap:14px;flex-wrap:wrap;padding:14px 16px;border-radius:14px;background:rgba(255,255,255,.05);box-shadow:inset 0 0 0 1px rgba(255,255,255,.08)} .cc-state{display:flex;flex-direction:column;line-height:1.15} .cc-state .lbl{font-size:12px;opacity:.75} .cc-state .val{font-size:22px;font-weight:800;letter-spacing:.2px} .cc-meta{display:flex;gap:16px;flex-wrap:wrap;font-size:12px;opacity:.85} .cc-actions{display:flex;gap:12px;justify-content:center;flex-wrap:wrap} .cc-auto{display:flex;justify-content:center;margin-top:2px} #scrob-watcher .status-dot{width:16px;height:16px;border-radius:50%;box-shadow:0 0 18px currentColor} #scrob-watcher .status-dot.on{background:#22c55e;color:#22c55e} #scrob-watcher .status-dot.off{background:#ef4444;color:#ef4444} @media (max-width:900px){.cc-wrap{grid-template-columns:1fr}} .sc-box{display:block;margin-top:12px;border-radius:12px;background:var(--panel,#111);box-shadow:0 0 0 1px rgba(255,255,255,.05) inset} .sc-box>.body{padding:12px 14px} </style><div class="cw-panel"><div class="cw-meta-provider-panel active" data-provider="watcher"><div class="cw-panel-head"><div class="cw-panel-head-main"><div class="cw-panel-title">Watcher</div><div class="muted">Monitor playback and scrobble automatically.</div></div><div style="display:grid;justify-items:end;gap:10px"><div class="cw-subtiles" aria-label="Watcher sections"><button type="button" class="cw-subtile active" data-sub="watcher">Watcher</button><button type="button" class="cw-subtile" data-sub="filters">Filters</button><button type="button" class="cw-subtile" data-sub="advanced">Advanced</button></div><div style="display:flex;justify-content:flex-end"><label class="cx-toggle"><input type="checkbox" id="sc-enable-watcher"><span class="cx-toggle-ui" aria-hidden="true"></span><span class="cx-toggle-text">Enable</span><span class="cx-toggle-state" aria-hidden="true"></span></label></div></div></div><div class="cw-subpanels" style="gap:8px"><div class="cw-subpanel active" data-sub="watcher"><div style="display:flex;align-items:center;gap:10px;margin-bottom:2px;flex-wrap:wrap"><div id="sc-legacy-picks" style="margin-left:auto;display:flex;gap:8px;align-items:center;flex-wrap:wrap"><span style="opacity:.75;font-size:12px">Sink</span><div id="sc-sink-pills" class="sc-pillbar" role="group" aria-label="Sink"></div><select id="sc-sink" class="input" style="display:none;width:240px"><option value="">None</option><option value="trakt">Trakt</option><option value="simkl">SIMKL</option><option value="mdblist">MDBList</option><option value="simkl,trakt">Trakt & SIMKL</option><option value="trakt,mdblist">Trakt & MDBList</option><option value="simkl,mdblist">SIMKL & MDBList</option><option value="simkl,trakt,mdblist">Trakt & SIMKL & MDBList</option></select><span style="opacity:.75;font-size:12px">Provider</span><div class="sc-prov-wrap"><button type="button" id="sc-provider-btn" class="input sc-prov-btn" aria-haspopup="listbox" aria-expanded="false"><span class="sc-prov-left">${providerLogImg("plex", "wh-logo sc-prov-ico").replace("<img class=\"wh-logo sc-prov-ico\"", "<img id=\"sc-provider-icon\" class=\"wh-logo sc-prov-ico\"")}<span id="sc-provider-label">Plex</span></span><span class="sc-prov-caret" aria-hidden="true">&#9662;</span></button><div id="sc-provider-menu" class="sc-prov-menu hidden" role="listbox" aria-label="Provider"><button type="button" class="sc-prov-item" role="option" data-value="plex" aria-selected="true">${providerLogImg("plex", "wh-logo sc-prov-ico")}<span>Plex</span></button><button type="button" class="sc-prov-item" role="option" data-value="emby" aria-selected="false">${providerLogImg("emby", "wh-logo sc-prov-ico")}<span>Emby</span></button><button type="button" class="sc-prov-item" role="option" data-value="jellyfin" aria-selected="false">${providerLogImg("jellyfin", "wh-logo sc-prov-ico")}<span>Jellyfin</span></button></div><select id="sc-provider" class="input" style="display:none"><option value="plex">Plex</option><option value="emby">Emby</option><option value="jellyfin">Jellyfin</option></select></div></div></div><div id="sc-routes-wrap" class="sc-box" style="display:none;margin:2px 0 10px"><div class="body"><div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:8px"><div style="display:inline-flex;align-items:center;gap:8px;font-size:12px;font-weight:900;letter-spacing:.12em;text-transform:uppercase;color:rgba(224,230,246,.7)">Routes ${helpBtn("sc-help-watch-routes")}</div><div style="margin-left:auto;display:flex;gap:8px;align-items:center;flex-wrap:wrap"><button type="button" id="sc-route-add" class="btn small">Add Route</button></div></div><div id="sc-routes" class="sc-route-table"></div><div id="sc-migrate-banner" class="micro-note" style="margin-top:8px;display:none"></div></div></div><div id="sc-note" class="micro-note" style="display:none;margin:0"></div><div class="cc-wrap"><div class="cc-card" id="sc-card-status"><div class="cc-head"><div>Watcher Status</div><span id="sc-status-badge" class="badge is-off">Stopped</span></div><div class="cc-body"><div class="cc-gauge"><span id="sc-status-dot" class="status-dot off"></span><div class="cc-state"><span class="lbl">Status</span><span id="sc-status-text" class="val">Inactive</span></div></div><div class="cc-meta"><span id="sc-status-last" class="micro-note"></span><span id="sc-status-up" class="micro-note"></span></div><div class="cc-actions"><button id="sc-watch-start" class="btn small">Start</button><button id="sc-watch-stop" class="btn small">Stop</button><button id="sc-watch-refresh" class="btn small">Refresh</button></div><div class="cc-auto"><label class="cx-toggle"><input type="checkbox" id="sc-autostart"><span class="cx-toggle-ui" aria-hidden="true"></span><span class="cx-toggle-text">Autostart on boot</span><span class="cx-toggle-state" aria-hidden="true"></span></label></div></div></div><div class="cc-card" id="sc-card-server"><div class="cc-head"><div><span id="sc-server-label">Media Server</span><span id="sc-server-required" class="pill req"></span></div></div><div id="sc-pms-note" class="micro-note" style="margin-top:2px"></div><div style="margin-top:12px"><div class="muted">Server URL (http(s)://host[:port])</div><input id="sc-pms-input" class="input" placeholder="http://192.168.1.10:32400" readonly/></div><div class="sc-subbox" style="margin-top:14px"><div class="head">Options</div><div class="body"><div class="sc-opt-col"><span class="cx-switch-wrap"><label class="sc-toggle"><input type="checkbox" id="sc-delete-plex-watch"><span class="one-line">Auto-remove from Watchlists</span></label>${helpBtn("sc-help-auto-remove")}</span><div id="sc-plex-ratings-wrap" style="display:none"><div class="sc-opt-row"><div class="muted" style="margin:0">Enable ratings</div>${helpBtn("sc-help-watch-plex-ratings")}<div id="sc-plex-ratings-pills" class="sc-pillbar" role="group" aria-label="Ratings"></div></div><div class="sc-opt-row" style="margin-top:6px"><select id="sc-plex-ratings" class="input" style="display:none;width:240px"><option value="none">None</option><option value="trakt">Trakt</option><option value="simkl">SIMKL</option><option value="mdblist">MDBList</option><option value="simkl,trakt">Trakt & SIMKL</option><option value="trakt,mdblist">Trakt & MDBList</option><option value="simkl,mdblist">SIMKL & MDBList</option><option value="simkl,trakt,mdblist">Trakt & SIMKL & MDBList</option></select><div id="sc-plexwatcher-url-wrap" class="codepair" style="display:none"><code id="sc-plexwatcher-url"></code><button id="sc-copy-plexwatcher" class="btn small">Copy</button></div></div><div id="sc-plexwatcher-note" class="micro-note" style="margin-top:6px"></div></div></div></div></div></div></div></div><div class="cw-subpanel" data-sub="filters"><div class="sc-box" id="sc-filters"><div style="display:flex;justify-content:flex-end;margin-bottom:10px">${helpBtn("sc-help-watch-filters")}</div><div class="body"><div id="sc-route-filter-wrap" style="display:none;margin-bottom:10px"><div class="muted">Filters for</div><select id="sc-route-select" class="input" style="width:100%;max-width:100%;margin-top:6px"></select></div><div class="sc-filter-grid"><div><div class="muted">Username whitelist</div><div id="sc-whitelist" class="chips" style="margin-top:4px"></div><div id="sc-users-note" class="micro-note"></div><div style="display:flex; gap:8px; margin-top:6px"><input id="sc-user-input" class="input" placeholder="Add username..." style="flex:1"><button id="sc-add-user" class="btn small">Add</button><button id="sc-load-users" class="btn small">Pick</button></div></div><div><div class="muted" id="sc-uuid-label">Server UUID</div><div id="sc-uuid-note" class="micro-note"></div><div style="display:flex; gap:8px; align-items:center; margin-top:6px"><input id="sc-server-uuid" class="input" placeholder="e.g. abcd1234..." style="flex:1"><button id="sc-fetch-uuid" class="btn small">Fetch</button></div></div></div></div></div></div><div class="cw-subpanel" data-sub="advanced"><div class="sc-box sc-advanced" id="sc-advanced"><div style="display:flex;justify-content:flex-end;margin-bottom:10px">${helpBtn("sc-help-watch-advanced")}</div><div class="body"><div class="sc-adv-grid">${buildAdvField("sc-pause-debounce", "Pause", "sc-help-adv-pause", DEFAULTS.watch.pause_debounce_seconds)}${buildAdvField("sc-suppress-start", "Suppress", "sc-help-adv-suppress", DEFAULTS.watch.suppress_start_at)}${buildAdvField("sc-regress", "Regress", "sc-help-adv-regress", DEFAULTS.trakt.regress_tolerance_percent)}${buildAdvField("sc-stop-pause", "Stop pause >=", "sc-help-adv-stop-pause", DEFAULTS.trakt.stop_pause_threshold)}${buildAdvField("sc-force-stop", "Force stop", "sc-help-adv-force-stop", DEFAULTS.trakt.force_stop_at)}</div><div class="sc-adv-grid" style="grid-template-columns:repeat(1,minmax(0,1fr));margin-top:10px">${buildAdvField("sc-progress-step", "Progress step", "sc-help-adv-progress-step", DEFAULTS.trakt.progress_step, { min: 1, max: 25, step: 1 })}</div><div class="micro-note" style="margin-top:6px">Empty resets to defaults. Percent fields are 1�100. Progress step is 1�25.</div></div></div></div></div></div></div>`;

      STATE.watcherHost.querySelector(".cw-panel")?.classList.add("sc-shell");
      enhanceWatcherFiltersUI(STATE.watcherHost);
      enhanceWatcherAdvancedUI(STATE.watcherHost);

      // Tabs: Watcher / Filters / Advanced
      const tabKey = "cw.ui.scrobbler.watcher.tab.v1";
      const root = STATE.watcherHost;
      const selectTab = (sub, opts = {}) => {
        const want = (sub || "watcher").toLowerCase();
        root.querySelectorAll('.cw-subtile[data-sub]').forEach((btn) => {
          btn.classList.toggle("active", (btn.dataset.sub || "").toLowerCase() === want);
        });
        root.querySelectorAll('.cw-subpanel[data-sub]').forEach((sp) => {
          sp.classList.toggle("active", (sp.dataset.sub || "").toLowerCase() === want);
        });
        if (opts.persist !== false) {
          try { localStorage.setItem(tabKey, want); } catch {}
        }
      };
      STATE._watcherSelectTab = selectTab;


      root.querySelectorAll('.cw-subtile[data-sub]').forEach((btn) => {
        btn.addEventListener("click", () => selectTab(btn.dataset.sub || "watcher"));
      });

      try { selectTab(localStorage.getItem(tabKey) || "watcher", { persist: false }); } catch { selectTab("watcher", { persist: false }); }

    }

    bindHelpTips(STATE.mount || d);
  }

  function ensureHiddenServerInputs() {
  const form = d.querySelector("form#settings, form#settings-form, form[data-settings]") || (STATE.mount || d.body);
  let h1 = d.getElementById("cfg-plex-server-url");
  if (!h1) {
    h1 = el("input", { type: "hidden", id: "cfg-plex-server-url", name: "plex.server_url" });
    form.appendChild(h1);
  }
  let h2 = d.getElementById("cfg-emby-server-url");
  if (!h2) {
    h2 = el("input", { type: "hidden", id: "cfg-emby-server-url", name: "emby.server" });
    form.appendChild(h2);
  }
  let h3 = d.getElementById("cfg-jellyfin-server-url");
  if (!h3) {
    h3 = el("input", { type: "hidden", id: "cfg-jellyfin-server-url", name: "jellyfin.server" });
    form.appendChild(h3);
  }
  let h4 = d.getElementById("cfg-trakt-progress-step");
  if (!h4) {
    h4 = el("input", { type: "hidden", id: "cfg-trakt-progress-step", name: "scrobble.trakt.progress_step" });
    form.appendChild(h4);
  }
  let h5 = d.getElementById("cfg-trakt-stop-pause-threshold");
  if (!h5) {
    h5 = el("input", { type: "hidden", id: "cfg-trakt-stop-pause-threshold", name: "scrobble.trakt.stop_pause_threshold" });
    form.appendChild(h5);
  }
  let h6 = d.getElementById("cfg-trakt-force-stop-at");
  if (!h6) {
    h6 = el("input", { type: "hidden", id: "cfg-trakt-force-stop-at", name: "scrobble.trakt.force_stop_at" });
    form.appendChild(h6);
  }
  let h7 = d.getElementById("cfg-trakt-regress-tolerance");
  if (!h7) {
    h7 = el("input", { type: "hidden", id: "cfg-trakt-regress-tolerance", name: "scrobble.trakt.regress_tolerance_percent" });
    form.appendChild(h7);
  }
  syncHiddenServerInputs();
}


  function syncHiddenServerInputs() {
  const h1 = d.getElementById("cfg-plex-server-url");
  if (h1) h1.value = String(read("plex.server_url", "") || "");
  const h2 = d.getElementById("cfg-emby-server-url");
  if (h2) h2.value = String(read("emby.server", "") || "");
  const h3 = d.getElementById("cfg-jellyfin-server-url");
  if (h3) h3.value = String(read("jellyfin.server", "") || "");
  const h4 = d.getElementById("cfg-trakt-progress-step");
  if (h4) h4.value = String(read("scrobble.trakt.progress_step", DEFAULTS.trakt.progress_step) ?? DEFAULTS.trakt.progress_step);
  const h5 = d.getElementById("cfg-trakt-stop-pause-threshold");
  if (h5) h5.value = String(read("scrobble.trakt.stop_pause_threshold", DEFAULTS.trakt.stop_pause_threshold) ?? DEFAULTS.trakt.stop_pause_threshold);
  const h6 = d.getElementById("cfg-trakt-force-stop-at");
  if (h6) h6.value = String(read("scrobble.trakt.force_stop_at", DEFAULTS.trakt.force_stop_at) ?? DEFAULTS.trakt.force_stop_at);
  const h7 = d.getElementById("cfg-trakt-regress-tolerance");
  if (h7) h7.value = String(read("scrobble.trakt.regress_tolerance_percent", DEFAULTS.trakt.regress_tolerance_percent) ?? DEFAULTS.trakt.regress_tolerance_percent);
}

  function restoreDetailsState(sel, def, key) {
    const n = $(sel, STATE.mount);
    if (!n) return;
    let open = def;
    try {
      const v = localStorage.getItem(key);
      if (v != null) open = v === "1";
    } catch {}
    n.open = !!open;
    on(n, "toggle", () => {
      try {
        localStorage.setItem(key, n.open ? "1" : "0");
      } catch {}
    });
  }

  const readNum = (sel, dflt) => {
    const n = $(sel, STATE.mount);
    if (!n) return null;
    const raw = String(n.value ?? "").trim();
    return raw === "" ? clamp100(dflt) : norm100(raw, dflt);
  };

  const readRange = (sel, dflt, min, max) => {
    const n = $(sel, STATE.mount);
    if (!n) return null;
    const raw = String(n.value ?? "").trim();
    return raw === "" ? clampRange(dflt, min, max) : normRange(raw, dflt, min, max);
  };

  async function copyText(s) {
    try {
      await navigator.clipboard.writeText(s);
      return true;
    } catch {
      try {
        const ta = el("textarea", { style: "position:fixed;left:-9999px;top:-9999px" });
        ta.value = s;
        d.body.appendChild(ta);
        ta.select();
        const ok = d.execCommand ? d.execCommand("copy") : document.execCommand("copy");
        d.body.removeChild(ta);
        return !!ok;
      } catch {
        return false;
      }
    }
  }

  function commitAdvancedInputsWatch() {
    const pd = readNum("#sc-pause-debounce", DEFAULTS.watch.pause_debounce_seconds);
    if (pd != null) write("scrobble.watch.pause_debounce_seconds", pd);
    const ss = readNum("#sc-suppress-start", DEFAULTS.watch.suppress_start_at);
    if (ss != null) write("scrobble.watch.suppress_start_at", ss);
  }

  function commitAdvancedInputsWebhook() {
    const pd = readNum("#sc-pause-debounce-webhook", DEFAULTS.watch.pause_debounce_seconds);
    if (pd != null) write("scrobble.webhook.pause_debounce_seconds", pd);
    const ss = readNum("#sc-suppress-start-webhook", DEFAULTS.watch.suppress_start_at);
    if (ss != null) write("scrobble.webhook.suppress_start_at", ss);
  }

  function commitAdvancedInputsTrakt() {
    const mode = String(read("scrobble.mode", "webhook")).toLowerCase();
    const preferWebhook = mode === "webhook";
    const keys = preferWebhook
      ? [
          ["#sc-stop-pause-webhook", "scrobble.trakt.stop_pause_threshold", DEFAULTS.trakt.stop_pause_threshold],
          ["#sc-force-stop-webhook", "scrobble.trakt.force_stop_at", DEFAULTS.trakt.force_stop_at],
          ["#sc-regress-webhook", "scrobble.trakt.regress_tolerance_percent", DEFAULTS.trakt.regress_tolerance_percent],
          ["#sc-stop-pause", "scrobble.trakt.stop_pause_threshold", DEFAULTS.trakt.stop_pause_threshold],
          ["#sc-force-stop", "scrobble.trakt.force_stop_at", DEFAULTS.trakt.force_stop_at],
          ["#sc-regress", "scrobble.trakt.regress_tolerance_percent", DEFAULTS.trakt.regress_tolerance_percent],
        ]
      : [
          ["#sc-stop-pause", "scrobble.trakt.stop_pause_threshold", DEFAULTS.trakt.stop_pause_threshold],
          ["#sc-force-stop", "scrobble.trakt.force_stop_at", DEFAULTS.trakt.force_stop_at],
          ["#sc-regress", "scrobble.trakt.regress_tolerance_percent", DEFAULTS.trakt.regress_tolerance_percent],
          ["#sc-stop-pause-webhook", "scrobble.trakt.stop_pause_threshold", DEFAULTS.trakt.stop_pause_threshold],
          ["#sc-force-stop-webhook", "scrobble.trakt.force_stop_at", DEFAULTS.trakt.force_stop_at],
          ["#sc-regress-webhook", "scrobble.trakt.regress_tolerance_percent", DEFAULTS.trakt.regress_tolerance_percent],
        ];

    const wrote = new Set();
    for (const [sel, path, dflt] of keys) {
      if (wrote.has(path)) continue;
      const v = readNum(sel, dflt);
      if (v == null) continue;
      write(path, v);
      wrote.add(path);
    }

    const ps = readRange("#sc-progress-step", DEFAULTS.trakt.progress_step, 1, 25);
    if (ps != null) write("scrobble.trakt.progress_step", ps);
  }

  function bindPercentInput(sel, path, dflt) {
    const n = $(sel, STATE.mount);
    if (!n) return;
    const set = (val, commitEmpty = false) => {
      const raw = String(val ?? n.value ?? "").trim();
      if (raw === "") {
        if (commitEmpty) {
          const v = clamp100(dflt);
          write(path, v);
          n.value = v;
        }
        return;
      }
      const v = norm100(raw, dflt);
      write(path, v);
      n.value = v;
    };
    on(n, "input", () => set(n.value, false));
    on(n, "change", () => set(n.value, true));
    on(n, "blur", () => set(n.value, true));
  }

  function bindRangeInput(sel, path, dflt, min, max) {
    const n = $(sel, STATE.mount);
    if (!n) return;
    const set = (val, commitEmpty = false) => {
      const raw = String(val ?? n.value ?? "").trim();
      if (raw === "") {
        if (commitEmpty) {
          const v = clampRange(dflt, min, max);
          write(path, v);
          n.value = v;
        }
        return;
      }
      const v = normRange(raw, dflt, min, max);
      write(path, v);
      n.value = v;
    };
    on(n, "input", () => set(n.value, false));
    on(n, "change", () => set(n.value, true));
    on(n, "blur", () => set(n.value, true));
  }


  function namesFromChips(hostId) {
    const host = $(hostId, STATE.mount);
    if (!host) return [];
    return $all(".chip > span:first-child", host)
      .map((s) => String(s.textContent || "").trim())
      .filter(Boolean);
  }


  function onSelectWatchUser(name) {
  const prov = provider();
  if (prov !== "emby" && prov !== "jellyfin") return;
  const list = Array.isArray(STATE.users) ? STATE.users : [];
  const hit = list.find((u) => String(u?.username || u?.Name || u?.name || "").toLowerCase() === String(name || "").toLowerCase());
  const id = hit?.id || hit?.Id;
  if (id) {
    const inp = $("#sc-server-uuid", STATE.mount);
    if (inp) inp.value = id;
    write("scrobble.watch.filters.server_uuid", id);
    write("scrobble.watch.filters.user_id", id);
    setNote("sc-uuid-note", "User ID set from username");
  } else {
    setNote("sc-uuid-note", "User not found", "err");
  }
}

  const scUserPicker = w.CW?.ScrobblerUserPicker?.create({
    STATE,
    el,
    on,
    $,
    j,
    API,
    provider,
    activeProviderInstance,
    asArray,
    read,
    write,
    setNote,
    chip,
    removeUserWatch,
    removeUserWebhook,
    onSelectWatchUser,
  }) || {};
  const closeUserPicker = scUserPicker.closeUserPicker || (() => {});
  const openUserPicker = scUserPicker.openUserPicker || (async () => {});


  function normalizeRatingsSelection(v) {
    const val = String(v || "none").toLowerCase().trim();
    if (val === "both") return "trakt,simkl";
    if (val === "none") return "none";
    return normSinkCsvOrDefault(val, "");
  }

  function getPlexRatingsSelectionFromCfg() {
    const parts = [];
    if (!!read("scrobble.watch.plex_trakt_ratings", false)) parts.push("trakt");
    if (!!read("scrobble.watch.plex_simkl_ratings", false)) parts.push("simkl");
    if (!!read("scrobble.watch.plex_mdblist_ratings", false)) parts.push("mdblist");
    return parts.length ? normSinkCsv(parts.join(",")) : "none";
  }

  function setPlexRatingsFlagsFromSelection(v) {
    const sel = normalizeRatingsSelection(v);
    const on = sel !== "none";
    write("scrobble.watch.plex_trakt_ratings", on && sel.includes("trakt"));
    write("scrobble.watch.plex_simkl_ratings", on && sel.includes("simkl"));
    write("scrobble.watch.plex_mdblist_ratings", on && sel.includes("mdblist"));
  }

  function rebuildPlexRatingsDropdown() {
    const wrap = $("#sc-plex-ratings-wrap", STATE.mount);
    const sel = $("#sc-plex-ratings", STATE.mount);
    if (!wrap || !sel) return;

    const prov = provider();
    wrap.style.display = prov === "plex" ? "" : "none";
    if (prov !== "plex") return;

    const options = [
      ["none", "None"],
      ["trakt", "Trakt"],
      ["simkl", "SIMKL"],
      ["mdblist", "MDBList"],
      ["simkl,trakt", "Trakt & SIMKL"],
      ["trakt,mdblist", "Trakt & MDBList"],
      ["simkl,mdblist", "SIMKL & MDBList"],
      ["simkl,trakt,mdblist", "Trakt & SIMKL & MDBList"],
    ];

    const cfgSel = getPlexRatingsSelectionFromCfg();
    const rawSel = normalizeRatingsSelection(sel.value || "");
    const cur = sel.dataset.cxUserChanged === "1" ? (rawSel || cfgSel) : cfgSel;

    sel.innerHTML = options.map(([v, label]) => `<option value="${v}">${label}</option>`).join("");
    const allowed = new Set(options.map((o) => o[0]));
    const next = allowed.has(cur) ? cur : "none";

    sel.value = next;
    setPlexRatingsFlagsFromSelection(next);
    try { syncPlexRatingsPillsFromSelect(); } catch {}

    try {
      updatePlexWatcherWebhookUrl();
    } catch {}
  }

  function updatePlexWatcherWebhookUrl() {
    const sel = $("#sc-plex-ratings", STATE.mount);
    const wrap = $("#sc-plexwatcher-url-wrap", STATE.mount);
    const code = $("#sc-plexwatcher-url", STATE.mount);
    if (!sel || !wrap || !code) return;

    const v = String(sel.value || "none").toLowerCase();
    const on = v !== "none";
    wrap.style.display = on ? "flex" : "none";

    try {
      const btn = $("#sc-copy-plexwatcher", STATE.mount);
      wrap.style.width = "100%";
      wrap.style.maxWidth = "100%";
      wrap.style.minWidth = "0";
      wrap.style.flex = "1 1 100%";
      wrap.style.alignItems = "center";
      wrap.style.gap = "8px";
      wrap.style.flexWrap = "nowrap";
      wrap.style.overflow = "hidden";
      code.style.flex = "1 1 0";
      code.style.width = "0";
      code.style.minWidth = "0";
      code.style.maxWidth = "100%";
      code.style.display = "block";
      code.style.boxSizing = "border-box";
      code.style.overflow = "hidden";
      code.style.textOverflow = "ellipsis";
      code.style.whiteSpace = "nowrap";

      if (btn) {
        btn.style.flex = "0 0 auto";
        btn.style.whiteSpace = "nowrap";
      }
    } catch {}

    if (on) {
      const id = STATE.webhookIds && STATE.webhookIds.plexwatcher ? String(STATE.webhookIds.plexwatcher) : "";
      code.textContent = `${location.origin}/webhook/plexwatcher${id ? "?" + id : ""}`;
    }
    else setNote("sc-plexwatcher-note", "");
  }

  async function refreshCfgBeforePopulate() {
    try {
      const fresh = await API.cfgGet();
      if (fresh && typeof fresh === "object") {
        const backendProv = String(fresh?.scrobble?.watch?.provider || "").toLowerCase().trim();
        const backendSinkRaw = fresh?.scrobble?.watch?.sink;
        const backendSink = normSinkCsv(backendSinkRaw == null ? "trakt" : backendSinkRaw);

        STATE.cfg = fresh;

        // Provider instances (profiles) can change outside this view; drop instance options cache.
        try { delete STATE._routesCache; } catch {}

        const uiProv = String(STATE.ui?.watchProvider || "").toLowerCase().trim();
        const uiSinkRaw = STATE.ui?.watchSink;
        const uiEnabled = STATE.ui?.scrobbleEnabled;
        const uiModeRaw = String(STATE.ui?.scrobbleMode || "").toLowerCase().trim();
        const uiAutostart = STATE.ui?.watchAutostart;

        const backendEnabled = !!fresh?.scrobble?.enabled;
        const backendMode = String(fresh?.scrobble?.mode || "webhook").toLowerCase().trim();
        const backendAutostart = !!fresh?.scrobble?.watch?.autostart;

        if (uiProv) {
          if (backendProv === uiProv) STATE.ui.watchProvider = null;
          else deepSet(STATE.cfg, "scrobble.watch.provider", uiProv);
        }
        if (uiSinkRaw != null) {
          const uiSink = normSinkCsv(uiSinkRaw);
          if (backendSink === uiSink) STATE.ui.watchSink = null;
          else deepSet(STATE.cfg, "scrobble.watch.sink", uiSink);
        }
        if (typeof uiEnabled === "boolean") {
          if (backendEnabled === uiEnabled) STATE.ui.scrobbleEnabled = null;
          else deepSet(STATE.cfg, "scrobble.enabled", uiEnabled);
        }
        if (uiModeRaw) {
          if (backendMode === uiModeRaw) STATE.ui.scrobbleMode = null;
          else deepSet(STATE.cfg, "scrobble.mode", uiModeRaw);
        }
        if (typeof uiAutostart === "boolean") {
          if (backendAutostart === uiAutostart) STATE.ui.watchAutostart = null;
          else deepSet(STATE.cfg, "scrobble.watch.autostart", uiAutostart);
        }

        try {
          w._cfgCache = STATE.cfg;
        } catch {}
      }
    } catch {}
  }

  function populate() {
  const mig = legacyToRoutesIfMissing();
  try {
    if (isRoutesMode()) {
      const ar = getActiveRoute() || getRoutes()[0] || null;
      if (ar) applyRouteView(ar);
      renderRoutesUi().catch(() => {});
      renderMigrateBanner(!!mig?.migrated);
    }
  } catch {}
  const enabled = !!read("scrobble.enabled", false);
  const mode = String(read("scrobble.mode", "webhook")).toLowerCase();
  const useWebhook = enabled && mode === "webhook";
  const useWatch = enabled && mode === "watch";
  const prov = provider();

  const whEl = $("#sc-enable-webhook", STATE.mount);
  const waEl = $("#sc-enable-watcher", STATE.mount);
  const pvSel = $("#sc-provider", STATE.mount);
  const skSel = $("#sc-sink", STATE.mount);

    if (whEl) whEl.checked = useWebhook;
  ["#sc-enable-webhook-jf", "#sc-enable-webhook-emby", "#sc-enable-webhook-adv"].forEach((id) => { const n = $(id, STATE.mount); if (n) n.checked = useWebhook; });
  if (waEl) waEl.checked = useWatch;
  if (pvSel) pvSel.value = prov;
  try { syncProviderPickerUi(); } catch {}
  if (skSel) {
    const raw = read("scrobble.watch.sink", "trakt");
    skSel.value = normSinkCsv(raw == null ? "trakt" : raw);
  }
  try { syncSinkPillsFromSelect(); } catch {}

  let wlWatch = asArray(read("scrobble.watch.filters.username_whitelist", []));
  

  const hostW = $("#sc-whitelist", STATE.mount);
  if (hostW) {
    hostW.innerHTML = "";
    wlWatch.forEach((u) => hostW.append(chip(u, removeUserWatch, (prov === "emby" || prov === "jellyfin") ? onSelectWatchUser : undefined)));
  }

  const suWatch = read("scrobble.watch.filters.server_uuid", "");
  const suInpW = $("#sc-server-uuid", STATE.mount);
  if (suInpW) suInpW.value = suWatch || "";

  const wlWeb = asArray(read("scrobble.webhook.filters_plex.username_whitelist", []));
  const hostWB = $("#sc-whitelist-webhook", STATE.mount);
  if (hostWB) {
    hostWB.innerHTML = "";
    wlWeb.forEach((u) => hostWB.append(chip(u, removeUserWebhook)));
  }

  const suWeb = read("scrobble.webhook.filters_plex.server_uuid", "");
  const suInpWB = $("#sc-server-uuid-webhook", STATE.mount);
  if (suInpWB) suInpWB.value = suWeb || "";

  const base = location.origin;
  const plexCode = $("#sc-webhook-url-plex", STATE.mount);
  const jfCode = $("#sc-webhook-url-jf", STATE.mount);
  const embyCode = $("#sc-webhook-url-emby", STATE.mount);
  function _withWebhookId(path, id) {
    return id ? `${path}?${id}` : path;
  }

  async function refreshWebhookIds() {
    try {
      const r = await j("/api/webhooks/urls");
      if (r && r.ok && r.ids) STATE.webhookIds = r.ids || null;
    } catch {
      STATE.webhookIds = null;
    }
  }

  function applyWebhookUrls() {
    const base = location.origin;
    const ids = STATE.webhookIds || {};
    const plexCode = $("#sc-webhook-url-plex", STATE.mount);
    const jfCode = $("#sc-webhook-url-jf", STATE.mount);
    const embyCode = $("#sc-webhook-url-emby", STATE.mount);
    if (plexCode) plexCode.textContent = _withWebhookId(`${base}/webhook/plextrakt`, ids.plextrakt);
    if (jfCode) jfCode.textContent = _withWebhookId(`${base}/webhook/jellyfintrakt`, ids.jellyfintrakt);
    if (embyCode) embyCode.textContent = _withWebhookId(`${base}/webhook/embytrakt`, ids.embytrakt);
  }

  async function regenWebhookIds() {
    const btn = $("#sc-regen-webhooks", STATE.mount);
    if (btn) btn.disabled = true;
    try {
      const r = await j("/api/webhooks/regenerate", { method: "POST" });
      if (r && r.ok && r.ids) {
        STATE.webhookIds = r.ids || null;
        applyWebhookUrls();
        try { updatePlexWatcherWebhookUrl(); } catch {}
      }
    } finally {
      if (btn) btn.disabled = false;
    }
  }
  refreshWebhookIds()
    .then(() => {
      applyWebhookUrls();
      try { updatePlexWatcherWebhookUrl(); } catch {}
    })
    .catch(() => {
      applyWebhookUrls();
      try { updatePlexWatcherWebhookUrl(); } catch {}
    });

  try {
    let btn = $("#sc-regen-webhooks", STATE.mount);
    const anchor =
      $("#sc-copy-plexwatcher", STATE.mount) ||
      $("#sc-copy-plex", STATE.mount) ||
      $("#sc-copy-jf", STATE.mount) ||
      $("#sc-copy-emby", STATE.mount);

    const host = anchor && anchor.parentElement ? anchor.parentElement : null;
    if (host) {
      if (!btn) {
        btn = el("button", { id: "sc-regen-webhooks", className: "btn small", textContent: "Regenerate IDs", title: wrapTooltipText("Generates new webhook IDs and invalidates the current URLs. Warning: you must update every media server webhook URL afterwards or scrobbling will stop working.") });
      }

      if (btn && btn.parentElement !== host) host.appendChild(btn);
    }

    on(btn, "click", async () => {
      if (!confirm("Regenerate webhook IDs? You must update all media server webhook URLs afterwards.")) return;
      try {
        await regenWebhookIds();
      } catch (e) {
        console.error(e);
      }
    });
  } catch {}

  const autostart = !!read("scrobble.watch.autostart", false);
  const auto = $("#sc-autostart", STATE.mount);
  if (auto) auto.checked = !!autostart;

  try { syncServerPreviewUi(); } catch {}

  const set = (id, v) => {
    const n = $(id, STATE.mount);
    if (n) n.value = norm100(v, v);
  };

  const setRange = (id, v, dflt, min, max) => {
    const n = $(id, STATE.mount);
    if (n) n.value = normRange(v, dflt, min, max);
  };

  set("#sc-pause-debounce", read("scrobble.watch.pause_debounce_seconds", DEFAULTS.watch.pause_debounce_seconds));
  set("#sc-suppress-start", read("scrobble.watch.suppress_start_at", DEFAULTS.watch.suppress_start_at));
  set("#sc-pause-debounce-webhook", read("scrobble.webhook.pause_debounce_seconds", DEFAULTS.watch.pause_debounce_seconds));
  set("#sc-suppress-start-webhook", read("scrobble.webhook.suppress_start_at", DEFAULTS.watch.suppress_start_at));
  set("#sc-stop-pause", read("scrobble.trakt.stop_pause_threshold", DEFAULTS.trakt.stop_pause_threshold));
  set("#sc-force-stop", read("scrobble.trakt.force_stop_at", DEFAULTS.trakt.force_stop_at));
  set("#sc-regress", read("scrobble.trakt.regress_tolerance_percent", DEFAULTS.trakt.regress_tolerance_percent));
  set("#sc-stop-pause-webhook", read("scrobble.trakt.stop_pause_threshold", DEFAULTS.trakt.stop_pause_threshold));
  set("#sc-force-stop-webhook", read("scrobble.trakt.force_stop_at", DEFAULTS.trakt.force_stop_at));
  set("#sc-regress-webhook", read("scrobble.trakt.regress_tolerance_percent", DEFAULTS.trakt.regress_tolerance_percent));
  setRange("#sc-progress-step", read("scrobble.trakt.progress_step", DEFAULTS.trakt.progress_step), DEFAULTS.trakt.progress_step, 1, 25);

  const delEnabled = !!read("scrobble.delete_plex", false);
  const delWh = $("#sc-delete-plex-webhook", STATE.mount);
    if (delWh) delWh.checked = delEnabled;
  ["#sc-delete-plex-webhook-jf", "#sc-delete-plex-webhook-emby"].forEach((id) => { const n = $(id, STATE.mount); if (n) n.checked = delEnabled; });
  const delW = $("#sc-delete-plex-watch", STATE.mount);
  if (delW) delW.checked = delEnabled;

  const whRat = $("#sc-webhook-plex-ratings", STATE.mount);
  if (whRat) whRat.checked = !!read("scrobble.webhook.plex_trakt_ratings", false);

  rebuildPlexRatingsDropdown();

  updatePlexWatcherWebhookUrl();

  restoreDetailsState("#sc-filters", false, "sc-filters-open-v2");
  restoreDetailsState("#sc-advanced", false, "sc-advanced-open-v2");

  syncHiddenServerInputs();
  applyModeDisable();
}


  async function refreshWatcher() {
    try {
      setWatcherStatus((await API.watch.status()) || {});
    } catch {
      setWatcherStatus({ alive: false });
    }
  }

  async function onWatchStart() {
  const routesMode = isRoutesMode();
  if (routesMode) {
    const routes = getRoutes();
    if (!routes.length) return setNote("sc-note", "Add at least one route before starting the watcher.", "warn");
    if (!anyStartableRoute()) return setNote("sc-note", "No startable routes. Check provider/sink authentication (and Plex server URL for that profile).", "warn");
    const pick = routes.find(r => r.enabled && providerAuthOkForRoute(r) && sinkAuthOkForRoute(r)) || routes[0];
    setActiveRouteId(pick.id);
    applyRouteView(pick);
    await renderRouteSelector();
  }

  const prov = String($("#sc-provider", STATE.mount)?.value || provider() || "plex")
    .toLowerCase()
    .trim();

  const sinkRawUi = $("#sc-sink", STATE.mount)?.value;
  const sinkRawCfg = read("scrobble.watch.sink", "trakt");
  const sink = normSinkCsv(sinkRawUi != null ? sinkRawUi : (sinkRawCfg == null ? "trakt" : sinkRawCfg));

  write("scrobble.watch.provider", prov);
  write("scrobble.watch.sink", sink);

  if (!sink) {
    write("scrobble.watch.autostart", false);
    STATE.ui.watchAutostart = false;
    const auto = $("#sc-autostart", STATE.mount);
    if (auto) {
      auto.checked = false;
      auto.disabled = true;
    }
    setNote("sc-note", "You must select at least one sink to start the watcher.", "warn");
    try { await API.watch.stop(); } catch {}
    try { await persistConfigPaths([["scrobble.watch.sink", ""], ["scrobble.watch.autostart", false]], "sc-pms-note"); } catch {}
    applyModeDisable();
    refreshWatcher();
    return;
  }

  setNote("sc-note", "");

  const srvProv = prov === "plex" ? "plex.server_url" : prov === "emby" ? "emby.server" : "jellyfin.server";
  const srv = String(read(srvProv, "") || "");
  const plexTokenOk = !!String(read("plex.account_token", "") || "").trim();
  const embyTokenOk = !!String(read("emby.access_token", "") || "").trim();
  const jellyTokenOk = !!String(read("jellyfin.access_token", "") || "").trim();

  if (prov === "plex") {
    if (!plexTokenOk) return setNote("sc-pms-note", "Not connected to Plex. Go to Authentication → Plex.", "err");
    if (!isValidServerUrl(srv)) return setNote("sc-pms-note", "Plex Server is required (http(s)://…)", "err");
  } else if (prov === "emby") {
    if (!embyTokenOk) return setNote("sc-pms-note", "Not connected to Emby. Go to Authentication → Emby.", "err");
  } else {
    if (!jellyTokenOk) return setNote("sc-pms-note", "Not connected to Jellyfin. Go to Authentication → Jellyfin.", "err");
  }

  try {
    const nextScrobble = getScrobbleConfig();
    const rootPatch = getRootPatch();

    const serverCfg = await API.cfgGet();
    const cfg = typeof structuredClone === "function" ? structuredClone(serverCfg || {}) : JSON.parse(JSON.stringify(serverCfg || {}));

    cfg.scrobble = nextScrobble;
    cfg.plex = Object.assign({}, cfg.plex || {}, rootPatch.plex || {});
    cfg.emby = Object.assign({}, cfg.emby || {}, rootPatch.emby || {});
    cfg.jellyfin = Object.assign({}, cfg.jellyfin || {}, rootPatch.jellyfin || {});

    const r = await fetch("/api/config", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      cache: "no-store",
      body: JSON.stringify(cfg),
    });
    if (!r.ok) throw new Error(`POST /api/config ${r.status}`);

    w._cfgCache = cfg;
    STATE.cfg = cfg;
    try {
      syncHiddenServerInputs();
    } catch {}
  } catch (e) {
    console.warn("[scrobbler] pre-start save failed:", e);
    return setNote("sc-pms-note", "Couldn’t save settings. Hit Save or check logs.", "err");
  }

  try {
    await API.watch.start(routesMode ? null : prov, routesMode ? null : sink);
  } catch {
    setNote("sc-pms-note", "Start failed", "err");
  }

  refreshWatcher();
}


  async function onWatchStop() {
    try {
      await API.watch.stop();
    } catch {
      setNote("sc-pms-note", "Stop failed", "err");
    }
    refreshWatcher();
  }


  async function fetchServerUUIDBase(noteId, path, endpoint, successText, emptyText, post) {
    try {
      const x = await endpoint();
      const v = x?.server_uuid || x?.uuid || x?.id || "";
      const inp = $(path.input, STATE.mount);
      if (!inp || !v) return setNote(noteId, emptyText, "err");
      inp.value = v;
      for (const p of path.write) write(p, v);
      if (typeof post === "function") post(v);
      setNote(noteId, successText);
    } catch {
      setNote(noteId, "Fetch failed", "err");
    }
  }

  const watchChipClick = () => (provider() === "emby" || provider() === "jellyfin") ? onSelectWatchUser : undefined;
  function redrawWhitelist(hostSel, path, removeFn, onClick) {
    const host = $(hostSel, STATE.mount);
    if (!host) return;
    host.innerHTML = "";
    asArray(read(path, [])).forEach((v) => host.append(chip(v, removeFn, onClick)));
  }
  function addWhitelistInput(inputSel, hostSel, path, removeFn, onClick) {
    const inp = $(inputSel, STATE.mount);
    const v = String(inp?.value || "").trim();
    if (!v || !addToWhitelist(hostSel, path, v, removeFn, onClick)) return;
    if (inp) inp.value = "";
  }
  function removeWhitelistItem(value, hostSel, path, removeFn, onClick) {
    write(path, asArray(read(path, [])).filter((x) => String(x) !== String(value)));
    redrawWhitelist(hostSel, path, removeFn, onClick);
  }

  async function fetchServerUUID() {
    const prov = provider();
    await fetchServerUUIDBase(
      "sc-uuid-note",
      { input: "#sc-server-uuid", write: ["scrobble.watch.filters.server_uuid"] },
      () => API.serverUUID(activeProviderInstance()),
      prov === "plex" ? "Server UUID fetched" : "User ID fetched",
      prov === "plex" ? "No server UUID" : "No user ID",
      (v) => { if (prov === "emby" || prov === "jellyfin") write("scrobble.watch.filters.user_id", v); }
    );
  }

  function onAddUserWatch() {
    addWhitelistInput("#sc-user-input", "#sc-whitelist", "scrobble.watch.filters.username_whitelist", removeUserWatch, watchChipClick());
  }

  function removeUserWatch(u) {
    removeWhitelistItem(u, "#sc-whitelist", "scrobble.watch.filters.username_whitelist", removeUserWatch, watchChipClick());
  }

  async function fetchServerUUIDWebhook() {
    await fetchServerUUIDBase(
      "sc-uuid-note-webhook",
      { input: "#sc-server-uuid-webhook", write: ["scrobble.webhook.filters_plex.server_uuid"] },
      () => j("/api/plex/server_uuid"),
      "Server UUID fetched",
      "No server UUID"
    );
  }

  function onAddUserWebhook() {
    addWhitelistInput("#sc-user-input-webhook", "#sc-whitelist-webhook", "scrobble.webhook.filters_plex.username_whitelist", removeUserWebhook);
  }

  function removeUserWebhook(u) {
    removeWhitelistItem(u, "#sc-whitelist-webhook", "scrobble.webhook.filters_plex.username_whitelist", removeUserWebhook);
  }

  async function hydrateEmby() {
    try {
      const info = await j("/api/emby/inspect");
      const server = String(info?.server || "").trim();
      const uid = String(info?.user_id || info?.user?.Id || "").trim();
      if (server) {
        write("emby.server", server);
        const inp = $("#sc-pms-input", STATE.mount);
        if (inp) inp.value = server;
      }
      if (uid) {
        const inp = $("#sc-server-uuid", STATE.mount);
        if (inp) inp.value = uid;
        write("scrobble.watch.filters.server_uuid", uid);
        write("scrobble.watch.filters.user_id", uid);
        setNote("sc-uuid-note", "User ID detected");
      }
    } catch {}
  }
async function hydrateJellyfin() {
  try {
    const info = await j("/api/jellyfin/inspect");
    const server = String(info?.server || "").trim();
    const uid = String(info?.user_id || info?.user?.Id || "").trim();
    if (server) {
      write("jellyfin.server", server);
      const inp = $("#sc-pms-input", STATE.mount);
      if (inp && provider() === "jellyfin") inp.value = server;
    }
    if (uid) {
      const inp = $("#sc-server-uuid", STATE.mount);
      if (inp) inp.value = uid;
      write("scrobble.watch.filters.server_uuid", uid);
      write("scrobble.watch.filters.user_id", uid);
      setNote("sc-uuid-note", "User ID detected");
    }
  } catch {}
}


  function wire() {
    ensureHiddenServerInputs();
    setNote("sc-webhook-warning", "These legacy webhooks scrobble only to Trakt and will be removed in a future release; they’re no longer maintained or supported, please switch to Watcher ASAP.", "warn");
    // Copy the displayed webhook URL
    function getCodeText(id) {
      const n = $(id, STATE.mount);
      return n && n.textContent ? String(n.textContent).trim() : "";
    }

    async function copyWebhookFromCode(codeId, noteId, successMsg, fallbackUrl) {
      // Ensure tokens are loaded before copying
      if (!STATE.webhookIds) {
        try { await refreshWebhookIds(); } catch {}
        try { applyWebhookUrls(); } catch {}
        try { updatePlexWatcherWebhookUrl(); } catch {}
      }

      const url = getCodeText(codeId) || fallbackUrl;
      const ok = await copyText(url);
      setNote(noteId, ok ? successMsg : "Copy failed", ok ? "" : "err");
    }

    on($("#sc-copy-plex", STATE.mount), "click", async () => {
      await copyWebhookFromCode("#sc-webhook-url-plex", "sc-endpoint-note", "Plex endpoint copied", `${location.origin}/webhook/plextrakt`);
    });
    on($("#sc-copy-jf", STATE.mount), "click", async () => {
      await copyWebhookFromCode("#sc-webhook-url-jf", "sc-endpoint-note", "Jellyfin endpoint copied", `${location.origin}/webhook/jellyfintrakt`);
    });
    on($("#sc-copy-emby", STATE.mount), "click", async () => {
      await copyWebhookFromCode("#sc-webhook-url-emby", "sc-endpoint-note", "Emby endpoint copied", `${location.origin}/webhook/embytrakt`);
    });

    on($("#sc-copy-plexwatcher", STATE.mount), "click", async () => {
      await copyWebhookFromCode("#sc-plexwatcher-url", "sc-plexwatcher-note", "Watcher endpoint copied", `${location.origin}/webhook/plexwatcher`);
    });

    on($("#sc-add-user", STATE.mount), "click", onAddUserWatch);
    on($("#sc-load-users", STATE.mount), "click", (e) => {
      e.preventDefault();
      openUserPicker("watch", e.currentTarget);
    });
    on($("#sc-watch-start", STATE.mount), "click", onWatchStart);
    on($("#sc-watch-stop", STATE.mount), "click", onWatchStop);
    on($("#sc-watch-refresh", STATE.mount), "click", () => {
      refreshWatcher();
      try {
        w.refreshWatchLogs?.();
      } catch {}
    });

    // Routes UI
    on($("#sc-route-add", STATE.mount), "click", async (e) => {
      e.preventDefault();
      if (!isRoutesMode()) return;
      try { syncActiveRouteFromView(); } catch {}
      const routes = getRoutes().map((r, i) => normalizeRoute(r, `R${i + 1}`));
      const id = nextRouteId();
      const nr = normalizeRoute({ id, enabled: true, provider: "", provider_instance: "default", sink: "", sink_instance: "default", filters: {} }, id);
      routes.push(nr);
      setRoutes(routes);
      setActiveRouteId(id);
      await renderRoutesUi();
    });

    const rSel = $("#sc-route-select", STATE.mount);
    on(rSel, "change", async (e) => {
      if (!isRoutesMode()) return;
      syncActiveRouteFromView();
      const rid = String(e.target.value || "").trim();
      if (!rid) return;
      setActiveRouteId(rid);
      const r = getActiveRoute();
      if (r) applyRouteView(r);
      populate();
      await renderRoutesUi();
    });

    const rHost = $("#sc-routes", STATE.mount);
    on(rHost, "change", async (e) => {
      if (!isRoutesMode()) return;
      const t = e.target;
      const rid = String(t?.dataset?.rid || "").trim();
      const f = String(t?.dataset?.f || "").trim();
      if (!rid || !f) return;
      const routes = getRoutes().map((r, i) => normalizeRoute(r, `R${i + 1}`));
      const r = routes.find(x => x.id === rid);
      if (!r) return;
      if (f === "enabled") r.enabled = !!t.checked;
      else r[f] = String(t.value || "").trim() || (f.endsWith("_instance") ? "default" : r[f]);
      if (f === "provider") { r.provider_instance = "default"; r.filters ||= {}; }
      if (f === "sink") { r.sink_instance = "default"; }
      setRoutes(routes);
      try {
        const dd = findDuplicateRouteKeys(getRoutes());
        if (!dd.length) clearStickyNote("sc-note");
      } catch {}

      if (activeRouteId() !== rid) { try { syncActiveRouteFromView(); } catch {} }
      setActiveRouteId(rid);
      applyRouteView(r);
      try { syncRouteActiveRowUi(rid); } catch {}
      try { syncServerPreviewUi(); } catch {}
      try { applyModeDisable(); } catch {}
      await renderRoutesUi();
    });

    on(rHost, "click", async (e) => {
      if (!isRoutesMode()) return;
      const btn = e.target?.closest?.("button[data-act]");
      if (!btn) {
        const tr = e.target?.closest?.("tr.sc-route-row");
        const rid = String(tr?.dataset?.rid || "").trim();
        if (rid) setActiveRouteFromUi(rid);
        return;
      }
      e.preventDefault();
      const act = String(btn.dataset.act || "");
      const rid = String(btn.dataset.rid || "");
      if (!act || !rid) return;
      if (act === "remove") {
        const routes = getRoutes().filter(r => String(r?.id || "") !== rid);
        setRoutes(routes);
        try {
          const dd = findDuplicateRouteKeys(getRoutes());
          if (!dd.length) clearStickyNote("sc-note");
        } catch {}

        if (activeRouteId() === rid) setActiveRouteId(routes[0]?.id || "");
        await renderRoutesUi();
        populate();
        return;
      }
      if (act === "filters") {
        syncActiveRouteFromView();
        setActiveRouteId(rid);
        const r = getRoutes().find(x => x.id === rid) || null;
        if (r) applyRouteView(r);
        await renderRouteSelector();
        try { STATE._watcherSelectTab?.("filters"); } catch {}
        populate();
      }
    });
    on($("#sc-fetch-uuid", STATE.mount), "click", () => {
      fetchServerUUID();
    });
    on($("#sc-server-uuid", STATE.mount), "input", (e) => {
      const v = String(e.target.value || "").trim();
      write("scrobble.watch.filters.server_uuid", v);
      if (provider() === "emby" || provider() === "jellyfin") write("scrobble.watch.filters.user_id", v);
    });

    on($("#sc-add-user-webhook", STATE.mount), "click", onAddUserWebhook);
    on($("#sc-load-users-webhook", STATE.mount), "click", (e) => {
      e.preventDefault();
      openUserPicker("webhook", e.currentTarget);
    });
    on($("#sc-fetch-uuid-webhook", STATE.mount), "click", fetchServerUUIDWebhook);
    on($("#sc-server-uuid-webhook", STATE.mount), "input", (e) => write("scrobble.webhook.filters_plex.server_uuid", String(e.target.value || "").trim()));

    bindPercentInput("#sc-pause-debounce", "scrobble.watch.pause_debounce_seconds", DEFAULTS.watch.pause_debounce_seconds);
    bindPercentInput("#sc-suppress-start", "scrobble.watch.suppress_start_at", DEFAULTS.watch.suppress_start_at);
    bindPercentInput("#sc-pause-debounce-webhook", "scrobble.webhook.pause_debounce_seconds", DEFAULTS.watch.pause_debounce_seconds);
    bindPercentInput("#sc-suppress-start-webhook", "scrobble.webhook.suppress_start_at", DEFAULTS.watch.suppress_start_at);

    bindPercentInput("#sc-stop-pause", "scrobble.trakt.stop_pause_threshold", DEFAULTS.trakt.stop_pause_threshold);
    bindPercentInput("#sc-force-stop", "scrobble.trakt.force_stop_at", DEFAULTS.trakt.force_stop_at);
    bindPercentInput("#sc-regress", "scrobble.trakt.regress_tolerance_percent", DEFAULTS.trakt.regress_tolerance_percent);

    bindPercentInput("#sc-stop-pause-webhook", "scrobble.trakt.stop_pause_threshold", DEFAULTS.trakt.stop_pause_threshold);
    bindPercentInput("#sc-force-stop-webhook", "scrobble.trakt.force_stop_at", DEFAULTS.trakt.force_stop_at);
    bindPercentInput("#sc-regress-webhook", "scrobble.trakt.regress_tolerance_percent", DEFAULTS.trakt.regress_tolerance_percent);
    bindRangeInput("#sc-progress-step", "scrobble.trakt.progress_step", DEFAULTS.trakt.progress_step, 1, 25);

    const wh = $("#sc-enable-webhook", STATE.mount);
    const wa = $("#sc-enable-watcher", STATE.mount);
    const pv = $("#sc-provider", STATE.mount);

    const mirrorToggle = (masterSel, clones) => {
      const master = $(masterSel, STATE.mount);
      if (!master) return;
      let mute = false;
      const sync = () => {
        if (mute) return;
        mute = true;
        clones.forEach((sel) => {
          const c = $(sel, STATE.mount);
          if (c) c.checked = !!master.checked;
        });
        mute = false;
      };
      clones.forEach((sel) => {
        const c = $(sel, STATE.mount);
        if (!c) return;
        on(c, "change", () => {
          if (mute) return;
          mute = true;
          master.checked = !!c.checked;
          master.dispatchEvent(new Event("change", { bubbles: true }));
          mute = false;
        });
      });
      on(master, "change", sync);
      sync();
    };

    mirrorToggle("#sc-enable-webhook", ["#sc-enable-webhook-jf", "#sc-enable-webhook-emby", "#sc-enable-webhook-adv"]);
    mirrorToggle("#sc-delete-plex-webhook", ["#sc-delete-plex-webhook-jf", "#sc-delete-plex-webhook-emby"]);

    const pvBtn = $("#sc-provider-btn", STATE.mount);
    const pvMenu = $("#sc-provider-menu", STATE.mount);

    on(pvBtn, "click", (e) => {
      e.preventDefault();
      toggleProviderMenu();
    });

    if (pvMenu) {
      $all(".sc-prov-item[data-value]", pvMenu).forEach((it) => {
        on(it, "click", (e) => {
          e.preventDefault();
          const v = String(it.getAttribute("data-value") || "").toLowerCase().trim();
          const sel = $("#sc-provider", STATE.mount);
          if (sel && v) {
            sel.value = v;
            sel.dispatchEvent(new Event("change", { bubbles: true }));
          }
          closeProviderMenu();
        });
      });
    }

    if (!STATE.__scProvAwayBound) {
      STATE.__scProvAwayBound = true;
      d.addEventListener("click", (e) => {
        const menu = $("#sc-provider-menu", STATE.mount);
        const btn = $("#sc-provider-btn", STATE.mount);
        if (!menu || menu.classList.contains("hidden")) return;
        if (menu.contains(e.target)) return;
        if (btn && (btn === e.target || btn.contains(e.target))) return;
        closeProviderMenu();
      });
      d.addEventListener("keydown", (e) => {
        if (e.key === "Escape") closeProviderMenu();
      });
    }
    const sk = $("#sc-sink", STATE.mount);

    const skPills = $("#sc-sink-pills", STATE.mount);
    if (sk && skPills) {
      ensureSinkPillBar(skPills);
      syncPillBar(skPills, normSinkCsv(sk.value));
      on(skPills, "click", (e) => {
        const btn = e.target && e.target.closest ? e.target.closest("button[data-sink]") : null;
        if (!btn || btn.disabled) return;
        const key = String(btn.getAttribute("data-sink") || "").toLowerCase().trim();
        if (!key) return;
        const curArr = normSinkCsv(sk.value || "").split(",").filter(Boolean);
        const has = curArr.includes(key);
        const nextArr = has ? curArr.filter((x) => x !== key) : [...curArr, key];
        const next = normSinkCsv(nextArr.join(","));
        if (sk.value !== next) {
          sk.value = next;
          sk.dispatchEvent(new Event("change", { bubbles: true }));
        } else {
          syncPillBar(skPills, next);
        }
      });
    }

    const ratSel = $("#sc-plex-ratings", STATE.mount);
    const ratPills = $("#sc-plex-ratings-pills", STATE.mount);
    if (ratSel && ratPills) {
      ensureSinkPillBar(ratPills);
      syncPillBar(ratPills, csvFromSelect(ratSel, true));
      on(ratPills, "click", (e) => {
        const btn = e.target && e.target.closest ? e.target.closest("button[data-sink]") : null;
        if (!btn || btn.disabled) return;
        const key = String(btn.getAttribute("data-sink") || "").toLowerCase().trim();
        if (!key) return;
        const curArr = csvFromSelect(ratSel, true).split(",").filter(Boolean);
        const has = curArr.includes(key);
        const nextArr = has ? curArr.filter((x) => x !== key) : [...curArr, key];
        const nextCsv = normSinkCsv(nextArr.join(","));
        const nextSel = nextCsv ? nextCsv : "none";
        if (ratSel.value !== nextSel) {
          ratSel.value = nextSel;
          ratSel.dispatchEvent(new Event("change", { bubbles: true }));
        } else {
          syncPillBar(ratPills, nextCsv);
        }
      });
    }

    const syncExclusive = async (src) => {
      const webOn = !!wh?.checked;
      const watOn = !!wa?.checked;
      if (src === "webhook" && webOn && wa) wa.checked = false;
      if (src === "watch" && watOn && wh) wh.checked = false;

      write("scrobble.enabled", (!!wh?.checked) || (!!wa?.checked));
      write("scrobble.mode", (!!wa?.checked) ? "watch" : "webhook");

      if (src === "watch" && !wa.checked) {
        write("scrobble.watch.autostart", false);
        const auto = $("#sc-autostart", STATE.mount);
        if (auto) auto.checked = false;
      }
      applyModeDisable();

      const enabled = (!!wh?.checked) || (!!wa?.checked);
      const mode = (!!wa?.checked) ? "watch" : "webhook";
      const pairs = [
        ["scrobble.enabled", enabled],
        ["scrobble.mode", mode],
      ];
      if (src === "watch" && !wa.checked) pairs.push(["scrobble.watch.autostart", false]);
      const noteId = mode === "watch" ? "sc-pms-note" : "sc-endpoint-note";
      STATE.ui.scrobbleEnabled = enabled;
      STATE.ui.scrobbleMode = mode;
      if (src === "watch" && !wa.checked) STATE.ui.watchAutostart = false;
      await persistConfigPaths(pairs, noteId);
    };

    if (wh) on(wh, "change", () => syncExclusive("webhook"));
    if (wa) on(wa, "change", () => syncExclusive("watch"));

    on($("#sc-autostart", STATE.mount), "change", (e) => {
      const v = !!e.target.checked;
      write("scrobble.watch.autostart", v);
      STATE.ui.watchAutostart = v;
      persistConfigPaths([["scrobble.watch.autostart", v]], "sc-pms-note");
    });

    on(pv, "change", async (e) => {
      const prev = provider();
      const val = String(e.target.value || "plex").toLowerCase();
      try {
        await API.watch.stop();
      } catch {}
      try {
        refreshWatcher();
      } catch {}
      try {
        if (!isRoutesMode()) saveCurrentProviderFilters(prev);
      } catch {}
      STATE.ui.watchProvider = val;
      write("scrobble.watch.provider", val);
      persistConfigPaths([["scrobble.watch.provider", val]], "sc-pms-note");
      try {
        if (!isRoutesMode()) applyProviderFilters(val);
      } catch {}
      try {
        closeUserPicker();
      } catch {}
      populate();
      try { syncProviderPickerUi(); } catch {}
      if (val === "emby") await hydrateEmby();
      if (val === "jellyfin") await hydrateJellyfin();
      applyModeDisable();
    });

    on(sk, "change", async (e) => {
      const val = normSinkCsv(e.target?.value ?? "");
      try { await refreshWatcher(); } catch {}
      STATE.ui.watchSink = val;
      write("scrobble.watch.sink", val);

      const pairs = [["scrobble.watch.sink", val]];
      if (!val) {
        write("scrobble.watch.autostart", false);
        STATE.ui.watchAutostart = false;
        pairs.push(["scrobble.watch.autostart", false]);
        const auto = $("#sc-autostart", STATE.mount);
        if (auto) auto.checked = false;
        setNote("sc-note", "You must select at least one sink to start the watcher.", "warn");
        try { await API.watch.stop(); } catch {}
        try { await refreshWatcher(); } catch {}
      } else {
        setNote("sc-note", "");
      }

      await persistConfigPaths(pairs, "sc-pms-note");
      try { syncSinkPillsFromSelect(); } catch {}
      rebuildPlexRatingsDropdown();
      applyModeDisable();
    });

    const ratingsSel = $("#sc-plex-ratings", STATE.mount);
    if (ratingsSel) {
      on(ratingsSel, "change", (e) => {
        ratingsSel.dataset.cxUserChanged = "1";
        setPlexRatingsFlagsFromSelection(e.target.value);
        try { syncPlexRatingsPillsFromSelect(); } catch {}
        try {
          updatePlexWatcherWebhookUrl();
        } catch {}
      });
    }

    const whRatings = $("#sc-webhook-plex-ratings", STATE.mount);
    if (whRatings) {
      on(whRatings, "change", (e) => {
        const v = !!e.target.checked;
        write("scrobble.webhook.plex_trakt_ratings", v);
        persistConfigPaths([["scrobble.webhook.plex_trakt_ratings", v]], "sc-endpoint-note");
      });
    }

    on($("#sc-delete-plex-webhook", STATE.mount), "change", (e) => {
      const v = !!e.target.checked;
      write("scrobble.delete_plex", v);
      const other = $("#sc-delete-plex-watch", STATE.mount);
      if (other) other.checked = v;
      persistConfigPaths([["scrobble.delete_plex", v]], "sc-endpoint-note");
    });

    on($("#sc-delete-plex-watch", STATE.mount), "change", (e) => {
      const v = !!e.target.checked;
      write("scrobble.delete_plex", v);
      const other = $("#sc-delete-plex-webhook", STATE.mount);
      if (other) other.checked = v;
      persistConfigPaths([["scrobble.delete_plex", v]], "sc-pms-note");
    });
  }

  function getScrobbleConfig() {
  commitAdvancedInputsWatch();
  commitAdvancedInputsWebhook();
  commitAdvancedInputsTrakt();

  const enabled = !!read("scrobble.enabled", false);
  const mode = String(read("scrobble.mode", "webhook")).toLowerCase();
  const prov = provider();

  const wlWebHost = $("#sc-whitelist-webhook", STATE.mount);
  const wlWeb = wlWebHost ? namesFromChips("#sc-whitelist-webhook") : asArray(read("scrobble.webhook.filters_plex.username_whitelist", []));
  const suWeb = String($("#sc-server-uuid-webhook", STATE.mount)?.value ?? read("scrobble.webhook.filters_plex.server_uuid", "")).trim();

  const wlWatchHost = $("#sc-whitelist", STATE.mount);
  const wlWatch = wlWatchHost ? namesFromChips("#sc-whitelist") : asArray(read("scrobble.watch.filters.username_whitelist", []));
  const suWatch = String($("#sc-server-uuid", STATE.mount)?.value ?? read("scrobble.watch.filters.server_uuid", "")).trim();
  const userIdWatch = String(read("scrobble.watch.filters.user_id", "") || "").trim();

  let filtersWatch = {
    username_whitelist: wlWatch,
  };

  if (prov === "plex") {
    filtersWatch.server_uuid = suWatch || "";
  } else {
    const uid = suWatch || userIdWatch || "";
    if (uid) filtersWatch.user_id = uid;
  }


    const routesMode = isRoutesMode();
    let routesOut = undefined;

    if (routesMode) {
      syncActiveRouteFromView(filtersWatch);
      const routesRaw = getRoutes().map((r, i) => normalizeRoute(r, `R${i + 1}`));
      const routesKeep = routesRaw.filter((r) => {
        const p = String(r?.provider || "").trim();
        const s = String(r?.sink || "").trim();
        return !!(p && s);
      });
      const dropped = routesRaw.length - routesKeep.length;
      if (dropped > 0) {
        const msg = `Ignored ${dropped} incomplete route${dropped === 1 ? "" : "s"} (pick Provider + Sink).`;
        if (!(STICKY_NOTES["sc-note"] && STICKY_NOTES["sc-note"].kind === "err")) setNote("sc-note", msg, "warn");
      }

      routesOut = routesKeep;

const dups = findDuplicateRouteKeys(routesOut);
if (dups.length) {
  const msg = "Duplicate routes are not allowed. Fix these before saving: " + dups.map(d => d.key).join(" · ");
  setStickyNote("sc-note", msg, "err");
  throw new Error(msg);
}

      // Per-route filters are stored on watch.routes[].filters.
      try {
        const r1 = routesRaw.find(r => String(r?.id || "").toUpperCase() === "R1") || routesRaw[0];
        if (r1 && r1.filters && typeof r1.filters === "object") filtersWatch = deepClone(r1.filters);
      } catch {}

      setRoutes(routesRaw);
    }

  return {
    enabled,
    mode: mode === "watch" ? "watch" : "webhook",
    delete_plex: !!read("scrobble.delete_plex", false),
    delete_plex_types: read("scrobble.delete_plex_types", ["movie"]),

    webhook: {
      pause_debounce_seconds: read("scrobble.webhook.pause_debounce_seconds", DEFAULTS.watch.pause_debounce_seconds),
      suppress_start_at: read("scrobble.webhook.suppress_start_at", DEFAULTS.watch.suppress_start_at),
      plex_trakt_ratings: !!read("scrobble.webhook.plex_trakt_ratings", false),
      filters_plex: {
        username_whitelist: wlWeb,
        server_uuid: suWeb || "",
      },
      filters_jellyfin: read("scrobble.webhook.filters_jellyfin", {}) || { username_whitelist: [] },
    },

    watch: {
      provider: prov,
      sink: normSinkCsv(read("scrobble.watch.sink", "trakt")),
      routes: routesOut,
      autostart: !!read("scrobble.watch.autostart", false),
      plex_simkl_ratings: !!read("scrobble.watch.plex_simkl_ratings", false),
      plex_trakt_ratings: !!read("scrobble.watch.plex_trakt_ratings", false),
      plex_mdblist_ratings: !!read("scrobble.watch.plex_mdblist_ratings", false),
      pause_debounce_seconds: read("scrobble.watch.pause_debounce_seconds", DEFAULTS.watch.pause_debounce_seconds),
      suppress_start_at: read("scrobble.watch.suppress_start_at", DEFAULTS.watch.suppress_start_at),
      filters: filtersWatch,
    },

    trakt: {
      progress_step: read("scrobble.trakt.progress_step", DEFAULTS.trakt.progress_step),
      stop_pause_threshold: read("scrobble.trakt.stop_pause_threshold", DEFAULTS.trakt.stop_pause_threshold),
      force_stop_at: read("scrobble.trakt.force_stop_at", DEFAULTS.trakt.force_stop_at),
      regress_tolerance_percent: read("scrobble.trakt.regress_tolerance_percent", DEFAULTS.trakt.regress_tolerance_percent),
    },
  };
}


  const getRootPatch = () => ({
  plex: { server_url: String(read("plex.server_url", "") || "") },
  emby: { server: String(read("emby.server", "") || "") },
  jellyfin: { server: String(read("jellyfin.server", "") || "") },
});
async function init(opts = {}) {
    STATE.mount = opts.mountId ? d.getElementById(opts.mountId) : d;
    STATE.cfg = opts.cfg || w._cfgCache || {};
    STATE.webhookHost = $("#scrob-webhook", STATE.mount);
    STATE.watcherHost = $("#scrob-watcher", STATE.mount);

    if (!STATE.webhookHost || !STATE.watcherHost) {
      const root = STATE.mount || d.body;
      const makeSec = (id, title) => {
        const className = ["section", "cw-settings-section", "cw-settings-provider-section"];
        if (id === "sc-sec-watch") className.push("open");
        const sec = el("div", { className: className.join(" "), id });
        sec.innerHTML = `<div class="head" onclick="toggleSection('${id}')"><span class="chev">▶</span><strong>${title}</strong></div><div class="body"><div id="${id === "sc-sec-webhook" ? "scrob-webhook" : "scrob-watcher"}"></div></div>`;
        root.append(sec);
      };
      if (!STATE.webhookHost) {
        makeSec("sc-sec-webhook", "Webhook");
        STATE.webhookHost = $("#scrob-webhook", STATE.mount);
      }
      if (!STATE.watcherHost) {
        makeSec("sc-sec-watch", "Watcher");
        STATE.watcherHost = $("#scrob-watcher", STATE.mount);
      }
    }

    buildUI();
    wire();
    if (!STATE.__authChangedBound) {
      STATE.__authChangedBound = true;
      let t = null;
      let busy = false;

      const run = async () => {
        if (busy) return;
        busy = true;
        try {
          if (!d.hidden) {
            try { await refreshCfgBeforePopulate(); } catch {}
            try { if (isRoutesMode()) await renderRoutesUi(); } catch {}
            try { await refreshWatcher(); } catch {}
            try { applyModeDisable(); } catch {}
          }
        } finally {
          busy = false;
        }
      };

      window.addEventListener("auth-changed", () => {
        if (t) return;
        t = setTimeout(() => {
          t = null;
          run();
        }, 400);
      });
    }

    await refreshCfgBeforePopulate();
    try { legacyToRoutesIfMissing(); } catch {}
    try {
      if (!isRoutesMode()) {
        pfLoadStore();
        const prov = provider();
        if (STATE.pf.store?.[prov]) applyProviderFilters(prov);
        else saveCurrentProviderFilters(prov);
      }
    } catch {}
    populate();
    await refreshWatcher();
    if (provider() === "emby") await hydrateEmby();
    if (provider() === "jellyfin") await hydrateJellyfin();
  }

  function mountLegacy(targetEl, cfg) {
    return init({ mountId: targetEl?.id, cfg: cfg || (w._cfgCache || {}) });
  }

  w.ScrobUI = { $, $all, el, on, setNote, injectStyles, DEFAULTS, STATE, read, write, asArray, clamp100, norm100, API };
  w.Scrobbler = { init, mount: mountLegacy, getConfig: getScrobbleConfig, getRootPatch };
  w.getScrobbleConfig = getScrobbleConfig;
  w.getRootPatch = getRootPatch;

  d.addEventListener("DOMContentLoaded", () => {
    const root = d.getElementById("scrobble-mount");
    if (!root) return;
    init({ mountId: "scrobble-mount" });
  });
})(window, document);

