/* assets/js/scrobbler.js */
/* Scrobbler configuration UI and logic. */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function (w, d) {
  const authSetupPending = () => w.cwIsAuthSetupPending?.() === true;
  const $ = (s, r) => (r || d).querySelector(s);
  const $all = (s, r) => [...(r || d).querySelectorAll(s)];
  const el = (t, a) => {
    const node = d.createElement(t);
    if (!a) return node;
    const attrs = { ...a };
    const dataset = attrs.dataset && typeof attrs.dataset === "object" ? attrs.dataset : null;
    if (dataset) delete attrs.dataset;
    Object.assign(node, attrs);
    if (dataset) Object.assign(node.dataset, dataset);
    return node;
  };
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
    n.classList.remove("err", "warn", "ok");
    if (kind) n.classList.add(kind);
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
      "When you finish a movie, CW will automatically remove that title from your configured Watchlists. It's currently movies-only. It honors your filters (username/server). If the movie isn't on your Watchlist, nothing happens, your libraries and other services remain untouched.",
    "sc-help-webhook-plex-ratings":
      "When enabled, we'll send Plex rating events to the selected webhook destinations.",
    "sc-help-watch-plex-ratings":
      "When enabled, we'll send ratings to Trakt, SIMKL, and/or MDBList.\nTrakt: Movies, shows, seasons, and episodes are supported.\nMDBList: Movies, shows, seasons, and episodes are supported.\nSIMKL: Movies and shows are supported.\nAdd the below webhook to your Plex instance to enable ratings.",

    "sc-help-adv-pause":
      "Pause debounce (sec) (default 5) - Ignore rapid, duplicate pause events.",
    "sc-help-adv-suppress":
      "Suppress start @ (%) (default 99) - If play/resume is at or above this %, don't send /scrobble/start.",
    "sc-help-adv-regress":
      "Regress tol % (default 5) - Block progress rollbacks bigger than this %.",
    "sc-help-adv-watched-at":
      "Watched threshold (default 90) - CrossWatch records local watched activity, statistics, and watchlist removal at or above this percent.",
    "sc-help-adv-force-stop":
      "Defensive final-stop trust threshold (default 95) - Final stop progress at or above this percent bypasses debounce and suspicious-stop protection.",
    "sc-help-adv-progress-step":
      "Progress updates in percentages, which can significantly reduce or increase the number of API calls required. When in doubt, default to 25% increments.",
    "sc-help-watch-filters":
      "Don't skip the filtering step! While optional for solo media server users, it becomes essential the moment you share your server with other users. Without filters, the system will scrobble everything",
    "sc-help-watch-username-whitelist":
      "Only scrobble activity for the usernames listed here.",
    "sc-help-watch-server-uuid":
      "Optional filter for one specific server or user identity.",
    "sc-help-watch-server-uuid-allow":
      "Accept watcher events only from these Plex server UUIDs. Leave empty to accept all servers unless they are blacklisted.",
    "sc-help-watch-server-uuid-block":
      "Ignore watcher events from these Plex server UUIDs. The blacklist wins over the allowlist.",
    "sc-help-webhook-server-uuid-allow":
      "Accept Plex webhook events only from these server UUIDs. Leave empty to accept all servers unless they are blacklisted.",
    "sc-help-webhook-server-uuid-block":
      "Ignore Plex webhook events from these server UUIDs. The blacklist wins over the allowlist.",
    "sc-help-watch-other-filters":
      "Extra Plex-only route filters for additional filtering options.",
    "sc-help-watch-ignore-live-tv-dvr":
      "Ignore Plex Live TV & DVR playback detected via tv.plex.xmltv GUIDs so those sessions are not scrobbled through this route.",
        "sc-help-watch-advanced":
      "Do not alter the Advanced settings unless you fully understand their impact. When in doubt, leave them untouched.",
  };

  function helpTextForId(id) {
    const key = String(id || "");
    if (key === "sc-help-watch-username-whitelist") {
      const prov = String(provider() || "plex").toLowerCase();
      if (prov === "plex") {
        return "Only scrobble activity for the Plex usernames listed here. Picking a user only adds the username to this whitelist.";
      }
      const label = prov === "emby" ? "Emby" : "Jellyfin";
      return `Only scrobble activity for the ${label} usernames listed here. Picking a user only adds the username to this whitelist.`;
    }
    if (key === "sc-help-watch-server-uuid") {
      const prov = String(provider() || "plex").toLowerCase();
      if (prov === "plex") {
        return "Optional: limit this route to one Plex server UUID. Fetch gets the server UUID for the configured Plex instance.";
      }
      const label = prov === "emby" ? "Emby" : "Jellyfin";
      return `Optional: limit this route to one ${label} user ID. Fetch gets the user ID for the configured ${label} instance. Picking a username does not fill this field.`;
    }
    return HELP_TEXT[key] || (key === "sc-help-watch-routes" ? "Routes control which provider sends activity to which sink. You can create separate paths for different services or profiles. Do not forget to set Filters for each route, otherwise playback from the wrong users or server may be scrobbled." : "");
  }

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
  const esc = (value) => String(value ?? "").replace(/[&<>"']/g, (ch) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[ch] || ch));
  const scUi = w.CW?.ScrobblerUI || {};
  const helpBtnNode = scUi.helpBtnNode || (() => null);
  const ensureInlineHelp = scUi.ensureInlineHelp || (() => {});
  const enhanceWatcherAdvancedUI = scUi.enhanceWatcherAdvancedUI || (() => {});
  const enhanceWebhookFiltersUI = scUi.enhanceWebhookFiltersUI || (() => {});

  function bindHelpTips(root) {
    const scope = root || d;
    $all(".cx-help[data-tip-id]", scope).forEach((btn) => {
      const id = btn.getAttribute("data-tip-id") || "";
      const text = helpTextForId(id);
      if (text) btn.title = wrapTooltipText(text);

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
    s.textContent = `.row{display:flex;gap:14px;align-items:center;flex-wrap:wrap}.codepair{display:flex;gap:8px;align-items:center}.codepair.right{justify-content:flex-end}.codepair code{padding:6px 8px;border-radius:8px;background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.08)}#card-scrobbler .badge,#sec-scrobbler .badge{padding:4px 10px;border-radius:999px;font-weight:600;opacity:.9}#card-scrobbler .badge.is-on,#sec-scrobbler .badge.is-on{background:#0a3;color:#fff}#card-scrobbler .badge.is-off,#sec-scrobbler .badge.is-off{background:#333;color:#bbb;border:1px solid #444}#card-scrobbler .status-dot,#sec-scrobbler .status-dot{width:10px;height:10px;border-radius:50%}#card-scrobbler .status-dot.on,#sec-scrobbler .status-dot.on{background:#22c55e}#card-scrobbler .status-dot.off,#sec-scrobbler .status-dot.off{background:#ef4444}#card-scrobbler .chips,#sec-scrobbler .chips{display:flex;flex-wrap:wrap;gap:6px}#card-scrobbler .chip,#sec-scrobbler .chip{display:inline-flex;align-items:center;gap:6px;padding:4px 8px;border-radius:10px;background:rgba(255,255,255,.05);border:1px solid rgba(255,255,255,.08)}#card-scrobbler .chip .rm,#sec-scrobbler .chip .rm{cursor:pointer;opacity:.7}.sc-filter-grid{display:grid;grid-template-columns:1fr 1fr;gap:14px;align-items:start}.sc-filter-grid>div{display:grid;gap:10px;min-width:0}.sc-adv-grid{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:14px}.sc-adv-grid .field{display:grid;grid-template-columns:minmax(0,1fr) auto 92px;align-items:center;gap:10px}.sc-adv-grid .field label{min-width:0;font-size:12px;opacity:.8;letter-spacing:.04em;text-transform:uppercase}.sc-adv-grid .field .cx-help{flex:0 0 auto}.sc-adv-grid .field input{width:92px;max-width:100%;justify-self:end}@media (max-width:1380px){.sc-adv-grid{grid-template-columns:repeat(4,minmax(0,1fr));}}@media (max-width:980px){.sc-adv-grid{grid-template-columns:repeat(2,minmax(0,1fr));}}@media (max-width:640px){.sc-adv-grid,.sc-filter-grid{grid-template-columns:1fr;}}.sc-subbox{margin-top:12px;border-radius:12px;background:rgba(255,255,255,.04);box-shadow:0 0 0 1px rgba(255,255,255,.06) inset}.sc-subbox .head{padding:12px 14px;font-weight:700;opacity:.92}.sc-subbox .body{padding:12px 14px;border-top:1px solid rgba(255,255,255,.06)}.sc-toggle{display:inline-flex;align-items:center;gap:8px;font-size:12px;opacity:.9;white-space:nowrap}.wh-logo{width:var(--wh-logo,24px);height:var(--wh-logo,24px);aspect-ratio:1/1;object-fit:contain;display:block;transform-origin:center}.wh-logo[alt="Plex"]{transform:scale(1.15)}.wh-logo[alt="Jellyfin"]{transform:scale(1)}.wh-logo[alt="Emby"]{transform:scale(1.15)}.sc-opt-col{display:flex;flex-direction:column;gap:10px}.sc-opt-row{display:flex;align-items:center;gap:10px;flex-wrap:wrap}.sc-pillbar{display:flex;align-items:center;gap:8px;flex-wrap:wrap}.sc-pill{display:inline-flex;align-items:center;justify-content:center;padding:7px 10px;border-radius:999px;border:1px solid rgba(255,255,255,.12);background:rgba(255,255,255,.05);color:rgba(255,255,255,.92);font-size:12px;line-height:1;cursor:pointer;user-select:none;transition:background .15s ease,border-color .15s ease,opacity .15s ease}.sc-pill.off{opacity:.78}.sc-pill.on{background:rgba(34,197,94,.18);border-color:rgba(34,197,94,.45);opacity:1}.sc-pill:hover{border-color:rgba(255,255,255,.22)}.sc-pill:focus-visible{outline:0;box-shadow:0 0 0 2px rgba(255,255,255,.14),0 0 0 6px rgba(34,197,94,.15)}.sc-pill:disabled{cursor:default;opacity:.45}.sc-user-pop{position:fixed;z-index:9999;width:min(360px,calc(100vw - 24px));max-height:min(420px,calc(100vh - 24px));border-radius:14px;background:var(--panel,#111);box-shadow:0 0 0 1px rgba(255,255,255,.08) inset,0 18px 50px rgba(0,0,0,.55);border:1px solid rgba(255,255,255,.10);overflow:hidden}.sc-user-pop.hidden{display:none}.sc-user-pop .head{display:flex;justify-content:space-between;align-items:center;gap:10px;padding:10px 12px;border-bottom:1px solid rgba(255,255,255,.06)}.sc-user-pop .title{font-weight:800}.sc-user-pop .body{padding:10px 12px;display:grid;gap:10px}.sc-user-pop .list{overflow:auto;border:1px solid rgba(255,255,255,.08);border-radius:12px;max-height:280px;scrollbar-width:thin;scrollbar-color:rgba(124,92,255,.92) rgba(255,255,255,.06)}.sc-user-pop .list::-webkit-scrollbar{width:12px}.sc-user-pop .list::-webkit-scrollbar-track{background:rgba(255,255,255,.06);border-radius:999px}.sc-user-pop .list::-webkit-scrollbar-thumb{background:linear-gradient(180deg,rgba(124,92,255,.95),rgba(86,60,180,.92));border-radius:999px;border:2px solid rgba(7,9,14,.88)}.sc-user-pop .list::-webkit-scrollbar-thumb:hover{background:linear-gradient(180deg,rgba(145,116,255,.98),rgba(104,79,206,.95))}.sc-user-pop .userrow{width:100%;text-align:left;background:transparent;border:0;color:inherit;padding:10px 10px;cursor:pointer}.sc-user-pop .userrow:hover{background:rgba(255,255,255,.05)}.sc-user-pop .row1{display:flex;justify-content:space-between;align-items:center;gap:8px}.sc-user-pop .sub{font-size:12px;opacity:.7;padding:10px}.sc-user-pop .tag{font-size:11px;padding:2px 8px;border-radius:999px;background:rgba(255,255,255,.08);border:1px solid rgba(255,255,255,.10);opacity:.85}#card-scrobbler .input,#sec-scrobbler .input{background:#0a0a17;border:1px solid rgba(255,255,255,.12);border-radius:14px;color:#e7e9f4;box-shadow:inset 0 0 0 1px rgba(255,255,255,.02)}#card-scrobbler .input:focus,#sec-scrobbler .input:focus{outline:none;border-color:rgba(124,92,255,.52);box-shadow:0 0 0 3px rgba(124,92,255,.18),inset 0 0 0 1px rgba(255,255,255,.03)}#card-scrobbler select.input,#sec-scrobbler select.input{background-color:#0a0a17;color:#e7e9f4}#card-scrobbler select.input option,#sec-scrobbler select.input option{background:#11131a;color:#fff}.sc-prov-wrap{position:relative;display:inline-block}.sc-prov-btn{width:140px;display:flex;align-items:center;justify-content:space-between;gap:10px;padding:8px 10px;cursor:pointer;background:#0a0a17;border:1px solid rgba(255,255,255,.12);border-radius:14px;box-shadow:inset 0 0 0 1px rgba(255,255,255,.02)}.sc-prov-left{display:inline-flex;align-items:center;gap:8px;min-width:0}.sc-prov-ico{width:18px;height:18px;object-fit:contain}.sc-prov-caret{opacity:.7}.sc-prov-menu{position:absolute;right:0;top:calc(100% + 6px);min-width:140px;border-radius:12px;background:var(--panel,#111);box-shadow:0 0 0 1px rgba(255,255,255,.08) inset,0 18px 50px rgba(0,0,0,.55);border:1px solid rgba(255,255,255,.10);overflow:hidden;z-index:1000}.sc-prov-menu.hidden{display:none}.sc-prov-item{width:100%;display:flex;align-items:center;gap:8px;padding:10px 10px;background:transparent;border:0;color:inherit;cursor:pointer;text-align:left}.sc-prov-item:hover{background:rgba(255,255,255,.05)}.sc-prov-item[aria-selected="true"]{background:rgba(34,197,94,.18)}.sc-prov-btn,.sc-prov-btn *{color:rgba(255,255,255,.92)!important;-webkit-text-fill-color:rgba(255,255,255,.92)!important}.sc-prov-btn:disabled,.sc-prov-btn:disabled *{color:rgba(255,255,255,.55)!important;-webkit-text-fill-color:rgba(255,255,255,.55)!important}#sc-provider,#sc-sink{color:rgba(255,255,255,.92)!important;-webkit-text-fill-color:rgba(255,255,255,.92)!important}#sc-provider:disabled,#sc-sink:disabled{color:rgba(255,255,255,.55)!important;-webkit-text-fill-color:rgba(255,255,255,.55)!important}#sc-provider option,#sc-sink option{color:#fff;background:#111}.sc-route-table table{width:100%;border-collapse:separate;border-spacing:0 8px}.sc-route-table th{font-size:12px;opacity:.8;text-align:left;padding:0 6px}.sc-route-table td{padding:0 6px;vertical-align:middle}.sc-route-row{background:rgba(255,255,255,.03);border:1px solid rgba(255,255,255,.06);border-radius:12px}.sc-route-row td{padding:8px 6px}.sc-route-actions{display:flex;gap:8px;justify-content:flex-end;flex-wrap:wrap}.sc-route-table select.input{height:34px}.sc-route-table .sc-prov-wrap{display:block;width:100%}.sc-route-table .sc-prov-btn{width:100%;height:34px;padding:6px 10px}.sc-route-table .sc-prov-menu{left:0;right:0;min-width:0}.sc-shell{display:grid;gap:14px}.sc-shell .cw-meta-provider-panel.active{display:grid;gap:14px}.sc-shell .cw-panel-head{padding:18px 18px 16px;border:1px solid rgba(255,255,255,.08);border-radius:22px;background:radial-gradient(120% 145% at 0% 0%,rgba(124,92,255,.16),transparent 40%),linear-gradient(180deg,rgba(11,14,21,.96),rgba(6,8,12,.985));box-shadow:0 18px 36px rgba(0,0,0,.24),inset 0 1px 0 rgba(255,255,255,.03)}.sc-shell .cw-panel-head-main{display:grid;gap:6px}.sc-shell .cw-panel-title{font-size:24px;font-weight:900;letter-spacing:-.02em;color:#f4f7ff}.sc-shell .muted,.sc-shell .micro-note{color:rgba(196,204,222,.74)}.sc-shell .cw-subtiles{display:flex;gap:10px;flex-wrap:wrap;justify-content:flex-end}.sc-shell .cw-subtile{min-height:38px;padding:0 14px;border-radius:999px;border:1px solid rgba(255,255,255,.10);background:rgba(255,255,255,.035);color:#eef3ff;font-weight:800;letter-spacing:.04em;transition:transform .14s ease,border-color .14s ease,background .14s ease,box-shadow .14s ease}.sc-shell .cw-subtile:hover{transform:translateY(-1px);border-color:rgba(124,92,255,.28);background:rgba(255,255,255,.06)}.sc-shell .cw-subtile.active{border-color:rgba(124,92,255,.40);background:linear-gradient(180deg,rgba(124,92,255,.20),rgba(45,161,255,.10));box-shadow:0 10px 22px rgba(18,22,40,.28),inset 0 1px 0 rgba(255,255,255,.06)}.sc-shell .cw-subpanels{display:grid;gap:14px}.sc-shell .cw-subpanel.active{display:grid;gap:14px}.sc-shell .sc-subbox,.sc-shell .cc-card,.sc-shell #sc-filters,.sc-shell #sc-advanced,.sc-shell #sc-routes-wrap{border-radius:22px;background:linear-gradient(180deg,rgba(255,255,255,.04),rgba(255,255,255,.02));border:1px solid rgba(255,255,255,.08);box-shadow:0 18px 34px rgba(0,0,0,.18),inset 0 1px 0 rgba(255,255,255,.03)}.sc-shell .sc-subbox .head,.sc-shell .cc-head{padding:16px 16px 12px;font-size:12px;font-weight:900;letter-spacing:.12em;text-transform:uppercase;color:rgba(224,230,246,.7)}.sc-shell .sc-subbox .body,.sc-shell #sc-routes-wrap>.body{padding:14px 16px 16px;border-top:1px solid rgba(255,255,255,.06)}.sc-shell .cc-card{padding:16px}.sc-shell .cc-head{display:flex;align-items:center;justify-content:space-between;gap:10px;margin:0 0 12px;padding:0}.sc-shell .cc-body{display:grid;gap:14px}.sc-shell .cc-gauge{min-height:74px;padding:16px 18px;border-radius:18px;background:linear-gradient(180deg,rgba(8,12,19,.78),rgba(4,6,10,.90));border:1px solid rgba(255,255,255,.08);box-shadow:inset 0 1px 0 rgba(255,255,255,.04)}.sc-shell .cc-state .lbl{font-size:11px;letter-spacing:.12em;text-transform:uppercase;color:rgba(196,204,222,.64)}.sc-shell .cc-state .val{font-size:24px;font-weight:900;color:#f4f7ff}.sc-shell .cc-meta{display:flex;gap:12px;flex-wrap:wrap;font-size:12px;color:rgba(196,204,222,.72)}.sc-shell .cc-actions{display:flex;gap:10px;flex-wrap:wrap}.sc-shell .cc-actions .btn,.sc-shell .codepair .btn,.sc-shell .row .btn{min-height:40px;border-radius:14px}.sc-shell .cc-actions .btn:nth-child(1),.sc-shell #sc-route-add{background:linear-gradient(135deg,rgba(86,60,180,.42),rgba(56,106,208,.42));border-color:rgba(124,92,255,.24);box-shadow:0 14px 28px rgba(22,24,40,.24)}.sc-shell .codepair code{padding:9px 12px;border-radius:14px;background:linear-gradient(180deg,rgba(3,5,9,.96),rgba(1,3,6,.985));border:1px solid rgba(255,255,255,.08);color:#eef3ff}.sc-shell #sc-plexwatcher-url,.sc-shell #sc-webhook-url-plex,.sc-shell #sc-webhook-url-jf,.sc-shell #sc-webhook-url-emby{font-family:inherit;font-size:14px;font-weight:400;letter-spacing:normal;line-height:1.4}.sc-shell .sc-filter-grid,.sc-shell .sc-adv-grid,.sc-shell .cc-wrap{gap:16px}.sc-shell #sc-filters,.sc-shell #sc-advanced{padding:18px 20px 20px}.sc-shell #sc-filters>div:first-child,.sc-shell #sc-advanced>div:first-child{display:flex;justify-content:flex-end;margin:0 0 16px}.sc-shell #sc-filters>.body,.sc-shell #sc-advanced>.body{padding:0}.sc-shell #sc-route-filter-wrap{padding:14px 16px;border-radius:18px;background:linear-gradient(180deg,rgba(255,255,255,.03),rgba(255,255,255,.015));border:1px solid rgba(255,255,255,.08)}.sc-shell #sc-route-filter-wrap .muted,.sc-shell .sc-filter-grid>div>.muted{font-size:11px;font-weight:900;letter-spacing:.12em;text-transform:uppercase;color:rgba(224,230,246,.68)}.sc-shell .sc-filter-grid{grid-template-columns:repeat(2,minmax(0,1fr));align-items:start}.sc-shell .sc-filter-grid>div{display:grid;gap:10px;min-width:0}.sc-shell .sc-filter-grid .chips{min-height:40px;align-content:flex-start}.sc-shell .sc-user-pop,.sc-shell .sc-prov-menu{background:linear-gradient(180deg,rgba(12,14,23,.98),rgba(6,8,12,.985));border:1px solid rgba(255,255,255,.10);box-shadow:0 18px 42px rgba(0,0,0,.36)}.sc-shell .sc-prov-btn{width:164px;min-height:42px;border-radius:16px;background:linear-gradient(180deg,rgba(3,5,9,.96),rgba(1,3,6,.985));border:1px solid rgba(255,255,255,.10)}.sc-shell .sc-route-table table{border-spacing:0 10px}.sc-shell .sc-route-row{background:linear-gradient(180deg,rgba(255,255,255,.04),rgba(255,255,255,.02));border:1px solid rgba(255,255,255,.08)}.sc-shell .badge,.sc-shell .pill,.sc-shell .sc-pill{display:inline-flex;align-items:center;justify-content:center;min-height:28px;padding:0 10px;border-radius:999px;font-size:11px;font-weight:850;letter-spacing:.05em;text-transform:uppercase}.sc-shell .badge.is-off{background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.10);color:rgba(236,241,255,.78)}.sc-shell .badge.is-on{background:rgba(34,197,94,.16);border:1px solid rgba(34,197,94,.30);color:#dcffe7}.sc-shell .sc-pillbar{gap:8px}.sc-shell .sc-pill{min-height:34px;padding:0 12px;border-radius:999px;background:rgba(255,255,255,.04)}.sc-shell .sc-pill.on{background:linear-gradient(180deg,rgba(124,92,255,.22),rgba(45,161,255,.10));border-color:rgba(124,92,255,.34)}.sc-shell .sc-opt-row,.sc-shell .sc-opt-col{gap:12px}.sc-shell .cx-help{color:rgba(214,222,242,.72)}.sc-shell .cx-help:hover{color:#fff}.sc-shell .row .cx-toggle,.sc-shell .cc-auto .cx-toggle{padding:10px 12px;border-radius:16px;border:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(255,255,255,.04),rgba(255,255,255,.018))}.sc-shell #sc-note,.sc-shell #sc-webhook-warning,.sc-shell #sc-endpoint-note{padding:12px 14px;border-radius:18px;border:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(255,255,255,.04),rgba(255,255,255,.018))}.sc-shell #sc-webhook-warning.warn{border-color:rgba(245,158,11,.24);background:linear-gradient(180deg,rgba(245,158,11,.12),rgba(255,255,255,.018))}.sc-shell .input,.sc-shell select.input{min-height:44px;border-radius:16px}.sc-shell .field{display:grid;grid-template-columns:minmax(0,1fr) auto 92px;align-items:center;gap:10px;padding:14px 16px;border-radius:18px;border:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(255,255,255,.03),rgba(255,255,255,.015))}.sc-shell .field input{width:92px}.sc-shell .field label{font-size:12px;font-weight:900;letter-spacing:.08em;text-transform:uppercase;color:rgba(226,232,248,.74)}@media (max-width:980px){.sc-shell .cw-panel-head{gap:14px}.sc-shell .cw-subtiles{justify-content:flex-start}.sc-shell .cc-wrap,.sc-shell .sc-filter-grid{grid-template-columns:1fr}.sc-shell .sc-adv-grid{grid-template-columns:repeat(2,minmax(0,1fr))}.sc-shell .row{align-items:flex-start}}@media (max-width:640px){.sc-shell .cw-panel-head{padding:16px}.sc-shell .cw-panel-title{font-size:22px}.sc-shell .cw-subtile{width:100%;justify-content:center}.sc-shell .cc-actions,.sc-shell .row,.sc-shell .codepair{width:100%}.sc-shell .codepair{flex-wrap:wrap}.sc-shell .codepair code,.sc-shell .codepair .btn,.sc-shell .cc-actions .btn,.sc-shell .row .btn,.sc-shell .sc-prov-btn{width:100%}.sc-shell .sc-filter-grid,.sc-shell .sc-adv-grid{grid-template-columns:1fr}.sc-shell #sc-filters,.sc-shell #sc-advanced{padding:16px}.sc-shell .field{grid-template-columns:minmax(0,1fr) auto}.sc-shell .field input{grid-column:1 / -1;width:100%}}`;
    d.head.appendChild(s);
    const t = d.createElement("style");
    t.id = "sc-styles-tweaks";
    t.textContent = `.sc-shell #sc-server-required:empty,.sc-shell #sc-note:empty,.sc-shell #sc-endpoint-note:empty,.sc-shell #sc-webhook-warning:empty{display:none!important}.sc-shell .cc-head>div:first-child{display:inline-flex;align-items:center;gap:10px;min-width:0}.sc-shell .cx-switch-wrap,.sc-shell .sc-opt-row{display:flex;align-items:center;gap:12px;flex-wrap:wrap}.sc-shell .cx-switch-wrap .sc-toggle,.sc-shell .sc-opt-row .muted{display:inline-flex;align-items:center;min-height:40px;margin:0}.sc-shell .cx-switch-wrap .cx-help,.sc-shell .sc-opt-row .cx-help{display:inline-flex;align-items:center;justify-content:center;align-self:center;margin:0}.sc-shell .sc-inline-head{display:inline-flex;align-items:center;gap:8px;flex-wrap:wrap}.sc-shell .sc-webhook-filter-grid{grid-template-columns:repeat(3,minmax(0,1fr));align-items:start}.sc-shell .sc-webhook-filter-grid>div{align-content:start}.sc-shell .sc-webhook-filter-grid .chips{min-height:32px}.sc-shell .sc-route-select-host{display:block;width:100%}.sc-shell .sc-route-select-host>.cw-icon-select{width:100%}.sc-shell .sc-route-table .cw-icon-select-btn{min-height:34px;padding:0 10px;border-radius:14px}.sc-shell .sc-route-table .cw-icon-select-label{font-size:13px}.sc-shell #sc-filters.sc-filters-enhanced>.body{display:grid;grid-template-columns:minmax(0,1fr);gap:18px}.sc-shell #sc-filters.sc-filters-enhanced #sc-route-filter-wrap{display:grid;gap:10px;width:100%;max-width:none;margin:0!important;padding:16px 18px;border-radius:18px;background:linear-gradient(180deg,rgba(255,255,255,.03),rgba(255,255,255,.015));border:1px solid rgba(255,255,255,.08)}.sc-shell #sc-filters.sc-filters-enhanced .sc-filter-grid{grid-template-columns:repeat(2,minmax(0,1fr));gap:18px;align-items:stretch}.sc-shell #sc-filters.sc-filters-enhanced .sc-filter-grid>div{display:grid;gap:10px;align-content:start;min-width:0;padding:16px 18px;border-radius:18px;background:linear-gradient(180deg,rgba(255,255,255,.03),rgba(255,255,255,.015));border:1px solid rgba(255,255,255,.08)}.sc-shell #sc-filters.sc-filters-enhanced #sc-whitelist{min-height:44px;align-content:flex-start}.sc-shell #sc-filters.sc-filters-enhanced #sc-users-note,.sc-shell #sc-filters.sc-filters-enhanced #sc-uuid-note{min-height:18px}.sc-shell .sc-filter-input-row{display:grid!important;align-items:center;gap:8px}.sc-shell .sc-filter-input-row--actions{grid-template-columns:minmax(0,1fr) 84px 84px}.sc-shell .sc-filter-input-row--fetch .sc-filter-input-spacer{display:block;min-height:1px}.sc-shell .sc-filter-input-row .btn{width:100%}.sc-shell #sc-advanced .body{display:block}.sc-shell .sc-advanced-header{display:flex;align-items:center;margin:0 0 16px}.sc-shell .sc-advanced-title{display:inline-flex;align-items:center;gap:8px;min-height:28px;font-size:11px;font-weight:900;letter-spacing:.12em;text-transform:uppercase;color:rgba(224,230,246,.68)}.sc-shell .sc-advanced-title .cx-help{margin:0}.sc-shell .sc-advanced-fields{display:grid;gap:16px}.sc-shell .sc-advanced-note{margin-top:12px}.sc-shell .sc-adv-grid{grid-template-columns:repeat(3,minmax(0,1fr));gap:16px}.sc-shell .sc-adv-grid .field{grid-template-columns:minmax(0,1fr) 36px 112px;align-items:center;min-height:88px}.sc-shell .sc-adv-grid .field input{width:112px}.sc-shell .sc-adv-grid .field label{line-height:1.35}.sc-shell .sc-adv-grid .field .cx-help{justify-self:center;transform:none}@media (max-width:1180px){.sc-shell .sc-adv-grid{grid-template-columns:repeat(2,minmax(0,1fr))}.sc-shell .sc-webhook-filter-grid{grid-template-columns:1fr}}@media (max-width:980px){.sc-shell #sc-filters.sc-filters-enhanced .sc-filter-grid{grid-template-columns:1fr}}@media (max-width:640px){.sc-shell .sc-filter-input-row,.sc-shell .sc-filter-input-row--actions,.sc-shell .sc-filter-input-row--fetch{grid-template-columns:minmax(0,1fr)!important}.sc-shell .sc-filter-input-row--fetch .sc-filter-input-spacer{display:none}.sc-shell .sc-adv-grid{grid-template-columns:1fr}.sc-shell .sc-adv-grid .field{grid-template-columns:minmax(0,1fr) auto}.sc-shell .sc-adv-grid .field input{grid-column:1 / -1;width:100%}}`;
    t.textContent += `.sc-user-pop{z-index:10050!important}.cw-media-user-picker{z-index:10060!important}.sc-route-modal-card{scrollbar-width:thin;scrollbar-color:rgba(124,92,255,.92) rgba(255,255,255,.06)}.sc-route-modal-card::-webkit-scrollbar{width:12px}.sc-route-modal-card::-webkit-scrollbar-track{background:rgba(255,255,255,.06);border-radius:999px}.sc-route-modal-card::-webkit-scrollbar-thumb{background:linear-gradient(180deg,rgba(124,92,255,.95),rgba(86,60,180,.92));border-radius:999px;border:2px solid rgba(7,9,14,.88)}.sc-route-modal-card::-webkit-scrollbar-thumb:hover{background:linear-gradient(180deg,rgba(145,116,255,.98),rgba(104,79,206,.95))}.sc-route-modal .chips{min-width:0}.sc-route-modal .chip{display:inline-grid!important;grid-template-columns:minmax(0,1fr) auto;align-items:center;column-gap:6px;max-width:100%;min-width:0;white-space:nowrap;overflow:hidden}.sc-route-modal .chip>span:first-child{display:block;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.sc-route-modal .chip .rm{display:inline-flex;align-items:center;justify-content:center;min-width:16px}.sc-route-filter-uuid-list .chip{width:100%}.sc-route-filter-uuid-list .chip>span:first-child{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:.92em}`;
    t.textContent += `@media (max-width:980px){.sc-route-filter-grid{grid-template-columns:1fr!important}}`;
    t.textContent += `.sc-shell-head{display:grid;grid-template-columns:minmax(0,1fr) auto;align-items:center;gap:18px}.sc-shell-head-side{display:grid;justify-items:end;gap:10px}.sc-shell-toggle{justify-content:flex-end}.sc-webhook-recommend{display:grid;gap:6px;padding:16px 18px!important;font-size:13px!important;line-height:1.5!important}.sc-webhook-recommend strong{font-size:14px;color:#f6f8ff}.sc-webhook-grid{display:grid;grid-template-columns:minmax(0,1.2fr) minmax(280px,.8fr);gap:14px}.sc-webhook-card{display:grid;gap:12px;padding:16px;border-radius:18px;border:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(255,255,255,.04),rgba(255,255,255,.018));box-shadow:inset 0 1px 0 rgba(255,255,255,.03)}.sc-webhook-card-wide{grid-column:1/-1}.sc-webhook-card-head{display:flex;align-items:center;justify-content:space-between;gap:12px}.sc-webhook-card-title{display:flex;align-items:center;gap:10px;min-width:0;font-size:12px;font-weight:900;letter-spacing:.12em;text-transform:uppercase;color:rgba(224,230,246,.74)}.sc-webhook-card-title .wh-logo{width:24px;height:24px}.sc-webhook-card-sub{font-size:12px;line-height:1.45;color:rgba(196,204,222,.72)}.sc-webhook-url-row{display:grid;grid-template-columns:128px auto minmax(0,1fr) auto auto;gap:10px;align-items:center}.sc-webhook-url-row code,.sc-webhook-profile-row code{min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.sc-webhook-profile-toggle{display:inline-flex;align-items:center;gap:8px;width:max-content;max-width:100%;font-size:12px;color:rgba(196,204,222,.78)}.sc-webhook-profile-list{display:none;gap:8px}.sc-webhook-profile-list.is-open{display:grid}.sc-webhook-profile-row{display:grid;grid-template-columns:128px auto minmax(0,1fr) auto auto auto;gap:10px;align-items:center}.sc-webhook-enable{display:inline-flex;align-items:center;gap:7px;white-space:nowrap;font-size:12px;color:rgba(196,204,222,.78)}html[data-cw-theme="flat-light"] .sc-webhook-enable{color:#4b5565!important}.sc-webhook-enable.is-disabled{opacity:.5;cursor:not-allowed}.sc-webhook-sinks-note{margin-top:8px}.sc-webhook-sinks-note:empty{display:none}.sc-webhook-sinks-note.warn{display:flex;align-items:center;gap:6px;opacity:1;background:none;border:0;padding:0;font-size:12px;font-weight:700;color:#ffb4a2}html[data-cw-theme="flat-light"] .sc-webhook-sinks-note.warn{color:#b42318!important}.sc-webhook-profile-name,.sc-webhook-row-label{min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:12px;font-weight:900;letter-spacing:.08em;text-transform:uppercase;color:rgba(224,230,246,.68)}.sc-webhook-icon-btn{width:94px;min-width:94px;min-height:44px;padding:0!important;font-size:24px!important}.sc-webhook-destinations{display:grid;gap:10px;align-content:start}.sc-webhook-destinations .sc-pillbar{min-height:40px}.sc-webhook-actions{display:flex;align-items:center;gap:12px;flex-wrap:wrap}.sc-webhook-filter-grid{grid-template-columns:repeat(3,minmax(0,1fr))!important}.sc-webhook-advanced{display:grid;gap:14px}.sc-webhook-advanced .sc-subbox{margin-top:0}@media (max-width:1120px){.sc-webhook-grid{grid-template-columns:1fr}.sc-webhook-filter-grid{grid-template-columns:1fr!important}.sc-shell-head{grid-template-columns:1fr}.sc-shell-head-side{justify-items:start}}@media (max-width:760px){.sc-webhook-url-row{grid-template-columns:88px auto minmax(0,1fr) auto auto}.sc-webhook-profile-row{grid-template-columns:88px auto minmax(0,1fr) auto auto auto}.sc-webhook-icon-btn{width:62px;min-width:62px}}html[data-cw-theme="flat-light"] .sc-webhook-card{background:#f8fafc!important;border-color:rgba(16,24,40,.16)!important;color:#172033!important}html[data-cw-theme="flat-light"] .sc-webhook-card-title{color:#172033!important}html[data-cw-theme="flat-light"] .sc-webhook-card-sub{color:#4b5565!important}html[data-cw-theme="flat-light"] .sc-webhook-recommend strong{color:#111827!important}`;
    t.textContent += `.sc-shell .sc-webhook-card-title .wh-logo{width:36px;height:36px;border-radius:999px;padding:7px;background:radial-gradient(circle at 28% 22%,rgba(255,255,255,.26),transparent 33%),linear-gradient(145deg,rgba(255,255,255,.12),rgba(255,255,255,.025));border:1px solid rgba(255,255,255,.11);box-shadow:inset 0 1px 0 rgba(255,255,255,.14),0 10px 20px rgba(0,0,0,.24)}#card-scrobbler button.sc-webhook-round-btn,#sec-scrobbler button.sc-webhook-round-btn,button.sc-webhook-round-btn{display:inline-flex!important;align-items:center!important;justify-content:center!important;box-sizing:border-box!important;width:42px!important;min-width:42px!important;max-width:42px!important;height:42px!important;min-height:42px!important;max-height:42px!important;aspect-ratio:1/1!important;padding:0!important;border-radius:50%!important;font-size:21px!important;line-height:1!important;flex:0 0 42px!important}#card-scrobbler button.sc-webhook-round-danger,#sec-scrobbler button.sc-webhook-round-danger,button.sc-webhook-round-danger{color:#ffdbe2!important;background:linear-gradient(180deg,rgba(190,42,72,.34),rgba(100,22,42,.42))!important;border-color:rgba(255,105,135,.52)!important;box-shadow:0 10px 22px rgba(120,20,45,.22),inset 0 1px 0 rgba(255,255,255,.08)!important}#card-scrobbler button.sc-webhook-round-danger:hover,#sec-scrobbler button.sc-webhook-round-danger:hover,button.sc-webhook-round-danger:hover{background:linear-gradient(180deg,rgba(220,54,88,.44),rgba(126,27,50,.50))!important;border-color:rgba(255,128,154,.68)!important}.sc-profile-filter-modal-card{display:grid;gap:12px}.sc-profile-filter-head{display:flex;align-items:center;justify-content:space-between;gap:14px}.sc-profile-filter-title{display:grid;gap:4px}.sc-profile-filter-title-main{font-size:19px;font-weight:900;color:#f4f7ff;letter-spacing:-.01em}.sc-profile-filter-grid{display:grid;gap:10px}.sc-profile-filter-box{display:grid;gap:8px;min-width:0;padding:12px;border-radius:16px;border:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(255,255,255,.035),rgba(255,255,255,.015))}.sc-profile-filter-box-title{display:inline-flex;align-items:center;gap:8px;min-height:28px;font-size:11px;font-weight:900;letter-spacing:.12em;text-transform:uppercase;color:rgba(224,230,246,.72)}.sc-profile-filter-box-title .cx-help{margin:0}.sc-profile-filter-actions{display:grid;gap:8px;align-items:center}.sc-profile-filter-actions .btn{min-height:40px;border-radius:14px}.sc-profile-filter-foot{display:flex;justify-content:flex-end;gap:10px}.sc-modal-close{display:inline-flex!important;align-items:center!important;gap:8px!important;min-height:42px!important;padding:0 14px!important;border-radius:12px!important;font-weight:900!important;letter-spacing:.05em!important;text-transform:uppercase!important}.sc-modal-close .material-symbols-rounded{font-size:22px!important}@media (min-width:760px){.sc-profile-filter-grid.is-plex{grid-template-columns:repeat(2,minmax(0,1fr))}.sc-profile-filter-grid:not(.is-plex){grid-template-columns:minmax(0,1fr)}}@media (max-width:760px){.sc-webhook-url-row{grid-template-columns:88px auto minmax(0,1fr) 42px 42px}.sc-webhook-profile-row{grid-template-columns:88px auto minmax(0,1fr) 42px 42px 42px}.sc-profile-filter-modal-card{width:calc(100vw - 24px)!important}.sc-profile-filter-actions{grid-template-columns:minmax(0,1fr)!important}}`;
    d.head.appendChild(t);
  }

  const DEFAULTS = {
    watch: { pause_debounce_seconds: 5, suppress_start_at: 99 },
    trakt: { stop_pause_threshold: 80, watched_at: 90, force_stop_at: 95, regress_tolerance_percent: 5, progress_step: 25 },
  };

  const STATE = { mount: null, webhookIds: null, routeWebhookHooks: [], profileWebhookHooks: [], webhookHost: null, watcherHost: null, cfg: {}, users: [], ui: { scrobbleEnabled: null, scrobbleSources: null, watchAutostart: null }, _noSinkAutostartFixApplied: false };

  const deepSet = (o, p, v) =>
    p.split(".").reduce(
      (a, k, i, arr) =>
        i === arr.length - 1
          ? (a[k] = v)
          : (((a[k] && typeof a[k] === "object") || (a[k] = {})), a[k]),
      o
    );

  function _readCfgPath(p, dflt) {
    return p.split(".").reduce((v, k) => (v && typeof v === "object" ? v[k] : undefined), STATE.cfg) ?? dflt;
  }

  function _activeRouteFilterValue(path, dflt) {
    const route = getActiveRoute() || getRoutes()[0] || null;
    const filters = route?.filters;
    const rel = String(path || "").replace(/^scrobble\.watch\.filters\./, "");
    if (!filters || !rel) return dflt;
    return rel.split(".").reduce((v, k) => (v && typeof v === "object" ? v[k] : undefined), filters) ?? dflt;
  }

  function read(p, dflt) {
    const path = String(p || "");
    if (path === "scrobble.watch.provider") {
      return String((getActiveRoute() || getRoutes()[0] || {}).provider || "").toLowerCase();
    }
    if (path === "scrobble.watch.sink") {
      return String((getActiveRoute() || getRoutes()[0] || {}).sink || "").toLowerCase();
    }
    if (path.startsWith("scrobble.watch.filters.")) {
      return _activeRouteFilterValue(path, dflt);
    }
    return _readCfgPath(path, dflt);
  }

  function write(p, v) {
    const path = String(p || "");
    if (path === "scrobble.watch.provider" || path === "scrobble.watch.sink" || path.startsWith("scrobble.watch.filters.")) {
      const route = getActiveRoute() || getRoutes()[0] || null;
      if (route) {
        if (path === "scrobble.watch.provider") {
          route.provider = String(v || "").trim().toLowerCase();
        } else if (path === "scrobble.watch.sink") {
          route.sink = String(v || "").trim().toLowerCase();
        } else {
          route.filters ||= {};
          const rel = path.replace(/^scrobble\.watch\.filters\./, "").split(".");
          let target = route.filters;
          for (let i = 0; i < rel.length - 1; i += 1) {
            const key = rel[i];
            if (!target[key] || typeof target[key] !== "object") target[key] = {};
            target = target[key];
          }
          target[rel[rel.length - 1]] = v;
        }
      }
      try { w._cfgCache = STATE.cfg; } catch {}
      try { syncHiddenServerInputs(); } catch {}
      return;
    }
    deepSet(STATE.cfg, path, v);
    try { w._cfgCache ||= {}; deepSet(w._cfgCache, path, v); } catch {}
    try { syncHiddenServerInputs(); } catch {}
  }

  function scrobbleSourceState(cfg = STATE.cfg) {
    const sc = cfg?.scrobble && typeof cfg.scrobble === "object" ? cfg.scrobble : {};
    const enabled = !!sc.enabled;
    if (!enabled) return { webhook: false, watcher: false };
    const src = sc.sources && typeof sc.sources === "object" ? sc.sources : null;
    if (src) {
      return {
        webhook: !!src.webhook,
        watcher: !!(src.watcher ?? src.watch),
      };
    }
    const mode = String(sc.mode || "").trim().toLowerCase();
    return {
      webhook: mode === "webhook",
      watcher: mode === "watch",
    };
  }

  function scrobbleLegacyMode(sources) {
    return sources?.watcher && !sources?.webhook ? "watch" : "webhook";
  }

  function writeScrobbleSources(webhook, watcher) {
    const sources = { webhook: !!webhook, watcher: !!watcher };
    write("scrobble.sources", sources);
    write("scrobble.enabled", sources.webhook || sources.watcher);
    write("scrobble.mode", scrobbleLegacyMode(sources));
    STATE.ui.scrobbleEnabled = sources.webhook || sources.watcher;
    STATE.ui.scrobbleSources = { ...sources };
    return sources;
  }

  function refreshHybridWarning() {
    const sources = scrobbleSourceState();
    if (sources.webhook && sources.watcher) {
      setStickyNote("sc-webhook-warning", "Webhook and Watcher are both enabled, do not use both for the same trackers.", "warn");
    } else {
      setStickyNote("sc-webhook-warning", "I recommend using Watcher instead of webhooks unless you have a specific reason not to. Watcher is more flexible, does not require Plex Pass or Emby Premiere, and does not require any client configuration.", "warn");
    }
  }

  const asArray = (v) => (Array.isArray(v) ? v.slice() : v == null || v === "" ? [] : [String(v)]);
  const uniqStrings = (values) => {
    const out = [];
    const seen = new Set();
    asArray(values).forEach((value) => {
      const clean = String(value || "").trim();
      if (!clean || seen.has(clean)) return;
      seen.add(clean);
      out.push(clean);
    });
    return out;
  };
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

  function sinkConnectionAvailability(instanceId = "default") {
    const inst = canonicalInstanceId(instanceId || "default");
    const traktCfg = overlayCfgFor("trakt", inst);
    const simklCfg = overlayCfgFor("simkl", inst);
    const mdblCfg = overlayCfgFor("mdblist", inst);
    const defaultInst = inst === "default";
    return {
      trakt: !!String(traktCfg?.access_token || (defaultInst ? read("auth.trakt.access_token", "") : "") || "").trim(),
      simkl: !!String(simklCfg?.access_token || (defaultInst ? read("auth.simkl.access_token", "") : "") || "").trim(),
      mdblist: !!String(mdblCfg?.api_key || mdblCfg?.access_token || (defaultInst ? read("auth.mdblist.api_key", "") || read("auth.mdblist.access_token", "") : "") || "").trim(),
    };
  }

  function availableSinkCsv(csv, availability) {
    const map = availability || {};
    return normSinkCsv(
      String(csv || "")
        .split(",")
        .map((s) => s.trim().toLowerCase())
        .filter((s) => s && map[s] !== false)
        .join(",")
    );
  }

  function sinkCsvItems(csv) {
    return String(csv || "")
      .split(",")
      .map((s) => s.trim().toLowerCase())
      .filter(Boolean);
  }

  function syncPillBar(bar, csv, availability = null) {
    if (!bar) return;
    ensureSinkPillBar(bar);
    const canUse = availability || null;
    const selectedCsv = canUse ? availableSinkCsv(csv, canUse) : String(csv || "");
    const on = new Set(String(selectedCsv || "").split(",").filter(Boolean));
    const watcherDisabled = !!bar.closest("#scrob-watcher") && !$("#sc-enable-watcher", STATE.mount)?.checked;
    $all("button[data-sink]", bar).forEach((btn) => {
      const k = String(btn.getAttribute("data-sink") || "");
      const unavailable = !!canUse && canUse[k] === false;
      const active = !unavailable && on.has(k);
      btn.classList.toggle("on", active);
      btn.classList.toggle("off", !active);
      btn.classList.toggle("unavailable", unavailable);
      if (canUse) {
        btn.disabled = watcherDisabled || unavailable;
        btn.title = unavailable ? `Configure ${providerLabel(k)} in Connections first` : "";
        btn.setAttribute("aria-disabled", unavailable ? "true" : "false");
      }
      btn.setAttribute("aria-pressed", active ? "true" : "false");
    });
  }

  function webhookSinkPath(providerKey) {
    const key = String(providerKey || "").trim().toLowerCase();
    return key ? `scrobble.webhook.providers.${key}.sinks` : "scrobble.webhook.sinks";
  }

  function mediaProviderConnected(providerKey, instance = "default") {
    const key = String(providerKey || "").trim().toLowerCase();
    const ov = overlayCfgFor(key, instance) || {};
    if (key === "plex") return !!String(ov.account_token || ov.pms_token || "").trim();
    if (key === "emby" || key === "jellyfin") return !!String(ov.access_token || "").trim();
    return false;
  }

  function webhookProfileEnabledPath(providerKey, instance) {
    const key = String(providerKey || "").trim().toLowerCase();
    const inst = String(instance || "default").trim() || "default";
    return `scrobble.webhook.profiles.${key}.${inst}.enabled`;
  }

  function webhookProfileEnabled(providerKey, instance) {
    const key = String(providerKey || "").trim().toLowerCase();
    const inst = String(instance || "default").trim() || "default";
    const stored = read(webhookProfileEnabledPath(key, inst), null);
    if (stored != null) return stored === true;
    if (inst === "default") {
      const legacy = read(`scrobble.webhook.providers.${key}.enabled`, null);
      if (legacy != null) return legacy === true;
    }
    return true;
  }

  function webhookSinkCsv(providerKey = "") {
    const key = String(providerKey || "").trim().toLowerCase();
    if (key) {
      const own = read(webhookSinkPath(key), null);
      if (own != null) return normSinkCsv(own);
    }
    const base = read("scrobble.webhook.sinks", null);
    return base == null ? "" : normSinkCsv(base);
  }

  function webhookSinkInstanceMap(providerKey = "") {
    const key = String(providerKey || "").trim().toLowerCase();
    const layers = [read("scrobble.webhook.sink_instances", null)];
    if (key) layers.push(read(`scrobble.webhook.providers.${key}.sink_instances`, null));
    const out = {};
    layers.forEach((src) => {
      if (!src || typeof src !== "object") return;
      Object.keys(src).forEach((k) => {
        const sink = String(k || "").trim().toLowerCase();
        const inst = String(src[k] || "").trim();
        if (sink && inst) out[sink] = inst;
      });
    });
    return out;
  }

  function webhookSinkAvailability(providerKey = "") {
    const instances = webhookSinkInstanceMap(providerKey);
    const cache = {};
    const out = {};
    SINK_ORDER.forEach((key) => {
      const inst = canonicalInstanceId(instances[key] || "default");
      if (!cache[inst]) cache[inst] = sinkConnectionAvailability(inst);
      out[key] = !!cache[inst][key];
    });
    return out;
  }

  function webhookAvailableSinkCsv(providerKey = "") {
    return availableSinkCsv(webhookSinkCsv(providerKey), webhookSinkAvailability(providerKey));
  }

  function globalPlexRatingsAvailability() {
    return sinkConnectionAvailability("default");
  }

  function globalPlexRatingsTargets() {
    const targets = [];
    if (read("scrobble.watch.plex_trakt_ratings", false)) targets.push("trakt");
    if (read("scrobble.watch.plex_simkl_ratings", false)) targets.push("simkl");
    if (read("scrobble.watch.plex_mdblist_ratings", false)) targets.push("mdblist");
    return normSinkCsv(targets.join(","));
  }

  function globalPlexRatingsPairs(csv) {
    const targets = new Set(sinkCsvItems(csv));
    return [
      ["scrobble.watch.plex_trakt_ratings", targets.has("trakt")],
      ["scrobble.watch.plex_simkl_ratings", targets.has("simkl")],
      ["scrobble.watch.plex_mdblist_ratings", targets.has("mdblist")],
    ];
  }

  function setGlobalPlexRatingsCsv(csv) {
    const pairs = globalPlexRatingsPairs(normSinkCsv(csv));
    pairs.forEach(([path, value]) => write(path, value));
    return pairs;
  }

  function syncWebhookSinkPills(root = STATE.mount) {
    $all("[data-webhook-sinks-provider]", root || d).forEach((bar) => {
      const providerKey = bar.getAttribute("data-webhook-sinks-provider");
      syncPillBar(bar, webhookSinkCsv(providerKey), webhookSinkAvailability(providerKey));
    });
    syncWebhookSinkNotes(root);
  }

  function syncWebhookSinkNotes(root = STATE.mount) {
    $all("[data-webhook-sinks-note]", root || d).forEach((note) => {
      const providerKey = String(note.getAttribute("data-webhook-sinks-note") || "").trim().toLowerCase();
      const selected = sinkCsvItems(webhookSinkCsv(providerKey));
      const availability = webhookSinkAvailability(providerKey);
      const usable = selected.filter((sink) => availability[sink] === true);
      note.classList.remove("warn");
      if (!selected.length) {
        note.textContent = "No destination selected - webhook events for this provider are ignored.";
        note.classList.add("warn");
        return;
      }
      if (!usable.length) {
        note.textContent = `Not connected: ${selected.map((s) => providerLabel(s) || s).join(", ")} - webhook events are ignored.`;
        note.classList.add("warn");
        return;
      }
      note.textContent = "";
    });
  }

  function syncPlexRatingsPillsFromSelect() {
    const sel = $("#sc-plex-ratings", STATE.mount);
    const bar = $("#sc-plex-ratings-pills", STATE.mount);
    if (!sel || !bar) return;
    const availability = globalPlexRatingsAvailability();
    const selected = globalPlexRatingsTargets();
    sel.value = selected || "none";
    $all("option", sel).forEach((option) => {
      const val = String(option.value || "").toLowerCase();
      option.disabled = val !== "none" && sinkCsvItems(val).some((k) => availability[k] === false);
    });
    syncPillBar(bar, selected, availability);
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
      start: () => j("/api/watch/start", { method: "POST" }),
      stop: () => j("/api/watch/stop", { method: "POST" }),
      refresh: () => j("/api/watch/refresh", { method: "POST" }),
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
      if (noteId) setNote(noteId, "Couldn't save settings. Hit Save or check logs.", "err");
    }
  }

  async function persistCurrentScrobblerState(noteId, opts = {}) {
    try {
      const serverCfg = await API.cfgGet();
      const cfg = typeof structuredClone === "function" ? structuredClone(serverCfg || {}) : JSON.parse(JSON.stringify(serverCfg || {}));
      cfg.scrobble = getScrobbleConfig();
      const rootPatch = getRootPatch();
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
    } catch (e) {
      console.warn("[scrobbler] save failed:", e);
      if (noteId) setNote(noteId, "Couldn't save settings. Hit Save or check logs.", "err");
      if (opts?.throwOnError) throw e;
    }
  }

  
  // Routes mode support
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
    const options = normalizeRouteOptions(x.options || {});
    return {
      id: String(x.id || idFallback || "").trim() || "R1",
      enabled: x.enabled !== false,
      provider: p.trim().toLowerCase(),
      provider_instance: canonicalInstanceId(pi),
      sink: s.trim().toLowerCase(),
      sink_instance: canonicalInstanceId(si),
      filters: deepClone(x.filters || {}),
      options,
    };
  }

  function normalizeRouteOptions(raw) {
    const src = (raw && typeof raw === "object") ? raw : {};
    const autoRemoveRaw = String(src.auto_remove_watchlist || "inherit").trim().toLowerCase();
    const autoRemove = ["inherit", "on", "off"].includes(autoRemoveRaw) ? autoRemoveRaw : "inherit";
    const ratingsSrc = (src.ratings && typeof src.ratings === "object") ? src.ratings : {};
    const ratingsModeRaw = String(ratingsSrc.mode || "off").trim().toLowerCase();
    const ratingsMode = ["off", "custom"].includes(ratingsModeRaw) ? ratingsModeRaw : "off";
    const targetsIn = Array.isArray(ratingsSrc.targets) ? ratingsSrc.targets : (ratingsSrc.targets ? [ratingsSrc.targets] : []);
    const targets = [];
    const seen = new Set();
    targetsIn.forEach((item) => {
      const key = String(item || "").trim().toLowerCase();
      if (!key || !["trakt", "simkl", "mdblist"].includes(key) || seen.has(key)) return;
      seen.add(key);
      targets.push(key);
    });
    const webhookId = String(ratingsSrc.webhook_id || "").trim();
    const webhookToken = String(ratingsSrc.webhook_token || "").trim();
    return {
      auto_remove_watchlist: autoRemove,
      ratings: {
        mode: ratingsMode,
        targets,
        webhook_id: webhookId,
        webhook_token: webhookToken,
      },
    };
  }

  function routeOptions(route) {
    return normalizeRouteOptions(route?.options || {});
  }

  function routeAutoRemoveMode(route) {
    return routeOptions(route).auto_remove_watchlist;
  }

  function routeAutoRemoveEffective(route) {
    const mode = routeAutoRemoveMode(route);
    if (mode === "on") return true;
    if (mode === "off") return false;
    return !!read("scrobble.delete_plex", false);
  }

  function routeOverrideSummary(route) {
    const opts = routeOptions(route);
    const parts = [];
    if (opts.auto_remove_watchlist !== "inherit") {
      parts.push(`Auto-remove ${opts.auto_remove_watchlist === "on" ? "On" : "Off"}`);
    }
    if (String(route?.provider || "").toLowerCase() === "plex" && opts.ratings.mode === "custom") {
      parts.push("Ratings webhook");
    }
    return parts;
  }

  function routeOptionsSummaryText(route) {
    const parts = routeOverrideSummary(route);
    return parts.join(" • ");
  }

  function routeFilters(route) {
    const filters = (route && typeof route.filters === "object" && route.filters) ? route.filters : {};
    const whitelist = asArray(filters.username_whitelist || []);
    const serverUuidAllow = uniqStrings([
      ...asArray(filters.server_uuid_whitelist || []),
      String(filters.server_uuid || "").trim(),
    ]);
    const serverUuidBlock = uniqStrings(filters.server_uuid_blacklist || []);
    const serverUuid = serverUuidAllow[0] || "";
    const userId = String(filters.user_id || "").trim();
    const ignoreLiveTvDvr = !!filters.ignore_live_tv_dvr;
    return {
      whitelist,
      server_uuid: serverUuid,
      server_uuid_whitelist: serverUuidAllow,
      server_uuid_blacklist: serverUuidBlock,
      user_id: userId,
      ignore_live_tv_dvr: ignoreLiveTvDvr,
    };
  }

  function routeFilterSummaryText(route) {
    const filters = routeFilters(route);
    const parts = [];
    if (filters.whitelist.length) parts.push(`${filters.whitelist.length} user${filters.whitelist.length === 1 ? "" : "s"}`);
    if (String(route?.provider || "").toLowerCase() === "plex") {
      if (filters.server_uuid_whitelist.length) parts.push(`${filters.server_uuid_whitelist.length} UUID allow`);
      if (filters.server_uuid_blacklist.length) parts.push(`${filters.server_uuid_blacklist.length} UUID block`);
      if (filters.ignore_live_tv_dvr) parts.push("Live TV ignored");
    } else if (filters.user_id) {
      parts.push("User ID set");
    }
    return parts.join(" • ");
  }

  function humanAndList(items) {
    const values = asArray(items).map((item) => String(item || "").trim()).filter(Boolean);
    if (!values.length) return "";
    if (values.length === 1) return values[0];
    if (values.length === 2) return `${values[0]} and ${values[1]}`;
    return `${values.slice(0, -1).join(", ")}, and ${values[values.length - 1]}`;
  }

  function routeHookMeta(route) {
    const rid = String(route?.id || "").trim();
    return asArray(STATE.routeWebhookHooks).find((item) => String(item?.route_id || "").trim() === rid) || null;
  }

  function routeCustomRatingsUrl(route) {
    const opts = routeOptions(route);
    const hook = routeHookMeta(route);
    const hookId = String(opts?.ratings?.webhook_id || hook?.webhook_id || "").trim();
    const hookToken = String(opts?.ratings?.webhook_token || hook?.webhook_token || "").trim();
    if (!hookId || !hookToken) return "";
    return `${location.origin}/webhook/plexwatcher?route=${encodeURIComponent(hookId)}&token=${encodeURIComponent(hookToken)}`;
  }

  function routeRatingsCsv(route) {
    if (String(route?.provider || "").toLowerCase() !== "plex") return "";
    const opts = routeOptions(route);
    if (opts.ratings.mode !== "custom") return "";
    return normSinkCsv((opts.ratings.targets || []).join(","));
  }

  function routeRatingsAvailability(route) {
    const availability = sinkConnectionAvailability(route?.sink_instance || "default");
    const out = {};
    SINK_ORDER.forEach((key) => {
      out[key] = !!availability[key];
    });
    return out;
  }

  function setActiveRouteRatingsCsv(csv) {
    const rid = activeRouteId();
    const routes = getRoutes().map((item, index) => normalizeRoute(item, `R${index + 1}`));
    const route = routes.find((item) => String(item.id || "") === String(rid || ""));
    if (!route || String(route.provider || "").toLowerCase() !== "plex") return null;
    const selectedTargets = sinkCsvItems(normSinkCsv(csv))
      .filter((k) => ROUTE_SINKS.includes(k));
    const selected = selectedTargets.length > 0;
    const prev = routeOptions(route);
    route.options = normalizeRouteOptions({
      ...(route.options || {}),
      ratings: {
        ...(prev.ratings || {}),
        mode: selected ? "custom" : "off",
        targets: selected ? selectedTargets : [],
        webhook_id: selected ? prev.ratings.webhook_id : "",
        webhook_token: selected ? prev.ratings.webhook_token : "",
      },
    });
    setRoutes(routes);
    return route;
  }

  function mergeRouteWebhookHooksIntoState() {
    const hooks = asArray(STATE.routeWebhookHooks);
    if (!hooks.length) return;
    const routes = getRoutes().map((item, index) => normalizeRoute(item, `R${index + 1}`));
    let changed = false;
    routes.forEach((route) => {
      const hook = hooks.find((item) => String(item?.route_id || "").trim() === String(route.id || "").trim());
      if (!hook) return;
      const opts = routeOptions(route);
      const next = normalizeRouteOptions({
        ...opts,
        ratings: {
          ...(opts.ratings || {}),
          webhook_id: String(hook.webhook_id || "").trim(),
          webhook_token: String(hook.webhook_token || "").trim(),
        },
      });
      if (
        next.ratings.webhook_id !== opts.ratings.webhook_id ||
        next.ratings.webhook_token !== opts.ratings.webhook_token
      ) {
        route.options = next;
        changed = true;
      }
    });
    if (changed) setRoutes(routes);
  }

  async function refreshWebhookIds() {
    try {
      const r = await j("/api/webhooks/urls");
      if (r && r.ok) {
        STATE.webhookIds = r.ids || null;
        STATE.routeWebhookHooks = Array.isArray(r.route_hooks) ? r.route_hooks : [];
        STATE.profileWebhookHooks = Array.isArray(r.profile_hooks) ? r.profile_hooks : [];
        mergeRouteWebhookHooksIntoState();
      }
    } catch {
      STATE.webhookIds = null;
      STATE.routeWebhookHooks = [];
      STATE.profileWebhookHooks = [];
    }
  }

  function currentGlobalDefaultsSummary() {
    return `Global auto-remove is currently ${read("scrobble.delete_plex", false) ? "On" : "Off"}.`;
  }


  function nextRouteId() {
    const used = new Set(getRoutes().map(r => String(r?.id || "").trim()).filter(Boolean));
    let i = 1;
    while (used.has(`R${i}`)) i++;
    return `R${i}`;
  }

  function ensureWatchRoutesArray() {
    STATE.cfg.scrobble ||= {};
    STATE.cfg.scrobble.watch ||= {};
    if (!Array.isArray(STATE.cfg.scrobble.watch.routes)) STATE.cfg.scrobble.watch.routes = [];
    return STATE.cfg.scrobble.watch.routes;
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
    if (!rid) {
      try { localStorage.removeItem(ROUTES_TAB_KEY); } catch {}
      return;
    }
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
    const p = p0 ? p0 : "-";
    const s = s0 ? s0 : "-";
    return `${r.id} ${p}(${pi}) → ${s}(${si})`;
  }

  function routeInstanceBadge(value) {
    const raw = String(value || "").trim();
    if (!raw || raw.toLowerCase() === "default") return "Default";
    const upper = raw.toUpperCase();
    const prof = upper.match(/(^|[^A-Z0-9])(P\d{1,3})(?=[^A-Z0-9]|$)/);
    if (prof?.[2]) return prof[2];
    const parts = upper.split(/[^A-Z0-9]+/).filter(Boolean);
    if (parts.length) {
      const last = parts[parts.length - 1];
      if (last.length <= 4) return last;
    }
    return upper.slice(0, 4);
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

function findSharedSinkProfileProviderConflicts(routes) {
  const groups = new Map();
  for (const r of (routes || [])) {
    if (!r?.enabled) continue;
    const provider = String(r?.provider || "").trim().toLowerCase();
    const sink = String(r?.sink || "").trim().toLowerCase();
    const sinkInstance = canonicalInstanceId(r?.sink_instance || "default");
    if (!provider || !sink) continue;
    const key = `${sink}|${sinkInstance}`;
    const item = groups.get(key) || { sink, sinkInstance, providers: new Set(), ids: [] };
    item.providers.add(provider);
    item.ids.push(String(r?.id || ""));
    groups.set(key, item);
  }
  return [...groups.values()].filter((item) => item.providers.size > 1);
}




  function applyRouteView(route) {
    if (!route) return;
    try { syncServerPreviewUi(); } catch {}
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

  function decorateWatcherCards() {
    const root = STATE.watcherHost;
    if (!root) return;
    const addHeadIcon = (sel, icon) => {
      const head = $(sel, root);
      if (!head || head.querySelector(".sc-card-title-icon")) return;
      head.classList.add("sc-card-title-main");
      head.insertBefore(el("span", { className: "material-symbols-rounded sc-card-title-icon", textContent: icon }), head.firstChild || null);
    };
    addHeadIcon("#sc-card-status .cc-head > div:first-child", "visibility");
    addHeadIcon("#sc-card-server .cc-head > div:first-child", "dns");
    const optHead = $("#sc-card-server .sc-subbox .head", root);
    if (optHead && !optHead.querySelector(".sc-card-title-icon")) {
      optHead.classList.add("sc-card-title-main");
      optHead.insertBefore(el("span", { className: "material-symbols-rounded sc-card-title-icon", textContent: "settings" }), optHead.firstChild || null);
    }
    const actionIcons = [["sc-watch-start", "play_arrow"], ["sc-watch-stop", "stop"], ["sc-watch-refresh", "refresh"]];
    actionIcons.forEach(([id, icon]) => {
      const btn = $(`#${id}`, root);
      if (!btn || btn.querySelector(".material-symbols-rounded")) return;
      const label = btn.textContent.trim();
      btn.textContent = "";
      const ico = el("span", { className: "material-symbols-rounded", textContent: icon });
      ico.setAttribute("aria-hidden", "true");
      btn.appendChild(ico);
      btn.appendChild(el("span", { textContent: label }));
    });
    const inp = $("#sc-pms-input", root);
    if (inp && !inp.closest(".sc-server-url-field")) {
      const wrap = el("div", { className: "sc-server-url-field" });
      inp.parentElement.insertBefore(wrap, inp);
      wrap.appendChild(inp);
      const copyBtn = el("button", { id: "sc-copy-pms", type: "button", className: "btn small material-symbols-rounded", title: "Copy server URL", textContent: "content_copy" });
      copyBtn.setAttribute("aria-label", "Copy server URL");
      wrap.appendChild(copyBtn);
    }
  }

  function syncWatcherDefaultsNote() {
    const body = $("#sc-card-server .sc-subbox .body", STATE.mount);
    if (!body) return;
    let note = $("#sc-watch-route-defaults-note", body);
    if (!note) {
      note = el("div", {
        id: "sc-watch-route-defaults-note",
        className: "micro-note",
        style: "margin:0 0 10px;padding:12px 14px;border-radius:16px;border:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(255,255,255,.04),rgba(255,255,255,.018))",
      });
      body.insertBefore(note, body.firstChild || null);
    }
    note.textContent = "These watcher options are global defaults. Use Route Options on a route when that route should behave differently.";
  }

  function syncWatcherRouteWarnings() {
    const body = $("#sc-card-status .cc-body", STATE.mount);
    const auto = $("#sc-card-status .cc-auto", STATE.mount);
    if (!body || !auto) return;
    let note = $("#sc-watch-route-warning", body);
    if (!note) {
      note = el("div", {
        id: "sc-watch-route-warning",
        className: "micro-note",
        style: "display:none;margin-top:10px;padding:12px 14px;border-radius:16px;border:1px solid rgba(245,158,11,.38);background:linear-gradient(180deg,rgba(49,30,7,.96),rgba(28,18,5,.96));color:rgba(255,236,205,.96);box-shadow:0 12px 24px rgba(0,0,0,.22),inset 0 1px 0 rgba(255,255,255,.03)",
      });
      auto.insertAdjacentElement("afterend", note);
    }
    const routes = getRoutes().map((r, i) => normalizeRoute(r, `R${i + 1}`));
    const problems = [];
    const dups = findDuplicateRouteKeys(routes.filter((r) => r.enabled));
    if (dups.length) {
      const ids = dups.flatMap((item) => item.ids || []).filter(Boolean).join(", ");
      problems.push(`Don't configure two identical enabled routes. Duplicate routes found: ${ids}.`);
    }
    const conflicts = findSharedSinkProfileProviderConflicts(routes);
    conflicts.forEach((item) => {
      const sinkName = providerLabel(item.sink) || item.sink;
      const profile = routeInstanceBadge(item.sinkInstance);
      const providers = humanAndList([...item.providers].map((name) => providerLabel(name) || name));
      problems.push(`${providers} to ${sinkName} ${profile} cannot scrobble at the same time on the same tracker profile.`);
    });
    if (!problems.length) {
      note.textContent = "";
      note.style.display = "none";
      return;
    }
    note.textContent = problems.join(" ");
    note.style.display = "";
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
    setActiveRouteId(id);
    const r = getActiveRoute();
    if (r) applyRouteView(r);
    try { syncRouteActiveRowUi(id); } catch {}
    try { syncServerPreviewUi(); } catch {}
    try { rebuildPlexRatingsDropdown(); } catch {}
    try { updatePlexWatcherWebhookUrl(); } catch {}
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
    if (s === "mdblist") return !!String(ov.api_key || ov.access_token || "").trim();
    return false;
  }

  function anyStartableRoute() {
    return getRoutes().some(r => r?.enabled && providerAuthOkForRoute(r) && sinkAuthOkForRoute(r));
  }

  async function renderRoutesUi() {
    const wrap = $("#sc-routes-wrap", STATE.mount);
    const host = $("#sc-routes", STATE.mount);
    const onMode = isRoutesMode();
    if (wrap) wrap.style.display = onMode ? "" : "none";
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
      pSel.appendChild(el("option", { value: "", textContent: "Select..." }));
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
      sSel.appendChild(el("option", { value: "", textContent: "Select..." }));
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

      const cA = el("td");
      const cACol = el("div", { className: "sc-route-actions-col", style: "display:grid;gap:6px;justify-items:end" });
      const cABtns = el("div", { className: "sc-route-actions" });
      const filt = el("button", { type: "button", className: "btn small", textContent: "Filters" });
      filt.dataset.act = "filters";
      filt.dataset.rid = r.id;
      filt.disabled = !hasP;
      const opts = el("button", { type: "button", className: "btn small", textContent: "Options" });
      opts.dataset.act = "options";
      opts.dataset.rid = r.id;
      const rm = el("button", { type: "button", className: "btn small", textContent: "Remove" });
      rm.dataset.act = "remove";
      rm.dataset.rid = r.id;
      cABtns.append(filt, opts, rm);
      cACol.appendChild(cABtns);
      const summaries = [routeFilterSummaryText(r), routeOptionsSummaryText(r)].filter(Boolean);
      if (summaries.length) {
        cACol.appendChild(el("div", {
          className: "micro-note",
          textContent: summaries.join(" • "),
          style: "text-align:right;font-size:11px;max-width:220px",
        }));
      }
      cA.appendChild(cACol);
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

    try { syncWatcherRouteWarnings(); } catch {}
    applyModeDisable();
  }

  function ensureRouteOptionsModal() {
    if (STATE.routeOptionsModal?.overlay?.isConnected) return STATE.routeOptionsModal;

    const overlay = el("div", {
      className: "sc-route-modal hidden",
      style: "position:fixed;inset:0;z-index:10000;background:rgba(4,7,14,.62);backdrop-filter:blur(8px);display:flex;align-items:center;justify-content:center;padding:20px",
    });
    const card = el("div", {
      className: "sc-route-modal-card",
      style: "width:min(560px,calc(100vw - 32px));max-height:min(88vh,820px);overflow:auto;border-radius:22px;background:linear-gradient(180deg,rgba(12,14,23,.98),rgba(6,8,12,.99));border:1px solid rgba(255,255,255,.10);box-shadow:0 24px 60px rgba(0,0,0,.45);padding:18px 18px 16px",
    });
    overlay.appendChild(card);

    const head = el("div", { style: "display:flex;align-items:flex-start;gap:12px;justify-content:space-between;margin-bottom:14px" });
    const headText = el("div", { style: "display:grid;gap:6px" });
    const title = el("div", { style: "font-size:20px;font-weight:900;color:#f4f7ff", textContent: "Route Options" });
    const subtitle = el("div", { className: "micro-note", style: "max-width:420px" });
    headText.append(title, subtitle);
    const close = el("button", { type: "button", className: "btn small", textContent: "Close" });
    head.append(headText, close);
    card.appendChild(head);

    const defaultsNote = el("div", {
      className: "micro-note",
      style: "margin:0 0 14px;padding:12px 14px;border-radius:16px;border:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(255,255,255,.04),rgba(255,255,255,.018))",
    });
    card.appendChild(defaultsNote);

    const autoBox = el("div", {
      style: "display:grid;gap:10px;padding:14px 16px;border-radius:18px;border:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(255,255,255,.03),rgba(255,255,255,.015))",
    });
    autoBox.append(
      el("div", { style: "font-size:12px;font-weight:900;letter-spacing:.12em;text-transform:uppercase;color:rgba(224,230,246,.7)", textContent: "Auto-remove from Watchlists" }),
      el("div", { className: "micro-note", textContent: "Choose whether this route follows the global watcher default or overrides it." }),
    );
    const autoSelect = el("select", {
      id: "sc-route-options-auto-select",
      name: "route_options_auto_remove",
      className: "input",
      style: "width:100%",
    });
    [
      ["inherit", "Inherit"],
      ["on", "On"],
      ["off", "Off"],
    ].forEach(([value, label]) => autoSelect.appendChild(el("option", { value, textContent: label })));
    const autoState = el("div", { className: "micro-note" });
    autoBox.append(autoSelect, autoState);
    card.appendChild(autoBox);

    const ratingsBox = el("div", {
      style: "display:grid;gap:10px;margin-top:14px;padding:14px 16px;border-radius:18px;border:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(255,255,255,.03),rgba(255,255,255,.015))",
    });
    ratingsBox.append(
      el("div", { style: "font-size:12px;font-weight:900;letter-spacing:.12em;text-transform:uppercase;color:rgba(224,230,246,.7)", textContent: "Ratings" }),
      el("div", { className: "micro-note", textContent: "Off uses the shared global ratings webhook when global ratings are configured. Enabled creates a separate ratings webhook for this route with its own destinations and profile routing." }),
    );
    const ratingsSelect = el("select", {
      id: "sc-route-options-ratings-select",
      name: "route_options_ratings_mode",
      className: "input",
      style: "width:100%",
    });
    [
      ["off", "Off"],
      ["custom", "Enabled"],
    ].forEach(([value, label]) => ratingsSelect.appendChild(el("option", { value, textContent: label })));
    const ratingsState = el("div", { className: "micro-note" });
    const ratingsTargetsWrap = el("div", { style: "display:grid;gap:8px" });
    ratingsTargetsWrap.appendChild(el("div", { className: "micro-note", textContent: "Send ratings to" }));
    const ratingsTargetsBar = el("div", { className: "sc-pillbar", role: "group", ariaLabel: "Route ratings targets" });
    ["trakt", "simkl", "mdblist"].forEach((sinkKey) => {
      ratingsTargetsBar.appendChild(el("button", {
        type: "button",
        className: "sc-pill off",
        textContent: providerLabel(sinkKey),
        dataset: { sink: sinkKey },
      }));
    });
    ratingsTargetsWrap.appendChild(ratingsTargetsBar);
    const ratingsProviderNote = el("div", { className: "micro-note", style: "display:none" });
    const ratingsUrlWrap = el("div", { style: "display:grid;gap:8px" });
    ratingsUrlWrap.appendChild(el("div", { className: "micro-note", textContent: "Route webhook URL" }));
    const ratingsUrlRow = el("div", { className: "codepair", style: "display:flex;align-items:center;gap:8px;flex-wrap:nowrap;width:100%;overflow:hidden" });
    const ratingsUrlCode = el("code", { style: "flex:1 1 0;width:0;min-width:0;max-width:100%;box-sizing:border-box;padding:9px 12px;border-radius:14px;background:linear-gradient(180deg,rgba(3,5,9,.96),rgba(1,3,6,.985));border:1px solid rgba(255,255,255,.08);color:#eef3ff;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" });
    const ratingsCopy = el("button", { type: "button", className: "btn small sc-webhook-round-btn material-symbols-rounded", textContent: "content_copy", title: "Copy endpoint" });
    ratingsCopy.setAttribute("aria-label", "Copy endpoint");
    const ratingsRegen = el("button", { type: "button", className: "btn small sc-webhook-round-btn sc-webhook-round-danger material-symbols-rounded", textContent: "refresh", title: "Regenerate endpoint" });
    ratingsRegen.setAttribute("aria-label", "Regenerate endpoint");
    ratingsUrlRow.append(ratingsUrlCode, ratingsCopy, ratingsRegen);
    ratingsUrlWrap.appendChild(ratingsUrlRow);
    ratingsBox.append(ratingsSelect, ratingsState, ratingsTargetsWrap, ratingsProviderNote, ratingsUrlWrap);
    card.appendChild(ratingsBox);

    const foot = el("div", { style: "display:flex;justify-content:flex-end;gap:10px;margin-top:16px" });
    const cancel = el("button", { type: "button", className: "btn small", textContent: "Cancel" });
    const save = el("button", { type: "button", className: "btn small", textContent: "Save" });
    foot.append(cancel, save);
    card.appendChild(foot);

    const modalState = { rid: "", provider: "", routeSink: "", ratingsTargets: [] };

    const syncRatingsTargetPills = () => {
      const route = getRoutes().find((item) => String(item?.id || "") === String(modalState.rid || ""));
      const availability = route ? routeRatingsAvailability(route) : {};
      $all("[data-sink]", ratingsTargetsBar).forEach((btn) => {
        const key = String(btn.dataset.sink || "").trim().toLowerCase();
        const enabled = availability[key] !== false;
        const onState = modalState.ratingsTargets.includes(key);
        btn.classList.toggle("on", onState);
        btn.classList.toggle("off", !onState);
        btn.classList.toggle("unavailable", !enabled);
        btn.disabled = !enabled;
        btn.setAttribute("aria-pressed", onState ? "true" : "false");
      });
    };

    const syncRatingsUi = (route, modeOverride) => {
      const mode = String(modeOverride != null ? modeOverride : (ratingsSelect.value || "off")).trim().toLowerCase();
      const isPlex = String(modalState.provider || "").toLowerCase() === "plex";
      ratingsBox.style.display = isPlex ? "grid" : "none";
      [...ratingsSelect.options].forEach((opt) => {
        if (String(opt.value || "") === "custom") opt.disabled = !isPlex;
      });
      ratingsState.textContent = mode === "off"
          ? "Off uses the shared global ratings webhook when global ratings are configured."
          : isPlex
            ? "Enabled creates a separate ratings webhook for this route with its own destinations and profile routing."
            : "Custom route ratings webhooks currently apply to Plex routes only.";
      ratingsTargetsWrap.style.display = mode === "custom" && isPlex ? "" : "none";
      ratingsUrlWrap.style.display = mode === "custom" && isPlex ? "" : "none";
      ratingsProviderNote.style.display = mode === "custom" && !isPlex ? "" : "none";
      ratingsProviderNote.textContent = "Route ratings webhooks are currently available for Plex routes only.";
      if (mode === "custom" && isPlex) {
        const fallback = String(route?.sink || "").trim().toLowerCase();
        if (["trakt", "simkl", "mdblist"].includes(fallback)) {
          modalState.routeSink = fallback;
          if (!modalState.ratingsTargets.length) modalState.ratingsTargets = [fallback];
        }
      } else {
        modalState.routeSink = "";
      }
      ratingsUrlCode.textContent = mode === "custom" && isPlex
        ? (routeCustomRatingsUrl(route) || "Save once to generate the route-specific webhook URL.")
        : "";
      syncRatingsTargetPills();
    };

    const closeModal = () => {
      overlay.classList.add("hidden");
      overlay.style.display = "none";
      overlay.dataset.rid = "";
      modalState.rid = "";
      modalState.provider = "";
      modalState.routeSink = "";
      modalState.ratingsTargets = [];
    };

    const openModal = (rid) => {
      const route = getRoutes().find((item) => String(item?.id || "") === String(rid || ""));
      if (!route) return;
      const opts = routeOptions(route);
      modalState.rid = route.id;
      modalState.provider = String(route.provider || "").toLowerCase();
      modalState.routeSink = String(route.sink || "").toLowerCase();
      modalState.ratingsTargets = sinkCsvItems(normSinkCsv((opts.ratings.targets || []).join(",")));
      overlay.dataset.rid = route.id;
      title.textContent = `Route Options • ${route.id}`;
      subtitle.textContent = routeLabel(route);
      defaultsNote.textContent = `Shared watcher defaults still apply to route behavior. Use route options when this route should behave differently. ${currentGlobalDefaultsSummary()}`;
      autoSelect.value = opts.auto_remove_watchlist;
      [...ratingsSelect.options].forEach((opt) => {
        if (String(opt.value || "") === "custom") opt.disabled = modalState.provider !== "plex";
      });
      const ratingsMode = opts.ratings.mode === "custom" ? "custom" : "off";
      ratingsSelect.value = ratingsMode;
      autoState.textContent = opts.auto_remove_watchlist === "inherit"
        ? `Inherit means this route follows the global auto-remove setting. Effective value: ${routeAutoRemoveEffective(route) ? "On" : "Off"}.`
        : `This route forces auto-remove ${opts.auto_remove_watchlist === "on" ? "On" : "Off"}.`;
      syncRatingsUi(route, ratingsMode);
      overlay.classList.remove("hidden");
      overlay.style.display = "flex";
    };

    on(close, "click", closeModal);
    on(cancel, "click", closeModal);
    on(overlay, "click", (e) => {
      if (e.target === overlay) closeModal();
    });
    on(autoSelect, "change", () => {
      const mode = String(autoSelect.value || "inherit");
      autoState.textContent = mode === "inherit"
        ? `Inherit means this route follows the global auto-remove setting. Effective value: ${read("scrobble.delete_plex", false) ? "On" : "Off"}.`
        : `This route forces auto-remove ${mode === "on" ? "On" : "Off"}.`;
    });
    on(ratingsSelect, "change", () => {
      const route = getRoutes().find((item) => String(item?.id || "") === String(overlay.dataset.rid || "")) || null;
      syncRatingsUi(route);
    });
    $all("[data-sink]", ratingsTargetsBar).forEach((btn) => {
      on(btn, "click", () => {
        const key = String(btn.dataset.sink || "").trim().toLowerCase();
        const route = getRoutes().find((item) => String(item?.id || "") === String(modalState.rid || ""));
        const availability = route ? routeRatingsAvailability(route) : {};
        if (!key || availability[key] === false) return;
        if (modalState.ratingsTargets.includes(key)) {
          modalState.ratingsTargets = modalState.ratingsTargets.filter((item) => item !== key);
        } else {
          modalState.ratingsTargets = [...modalState.ratingsTargets, key];
        }
        syncRatingsTargetPills();
      });
    });
    on(ratingsCopy, "click", async () => {
      const text = String(ratingsUrlCode.textContent || "").trim();
      if (!/^https?:\/\//i.test(text)) return;
      const prev = ratingsCopy.textContent;
      ratingsCopy.textContent = (await copyText(text)) ? "done" : "error";
      setTimeout(() => { ratingsCopy.textContent = prev; }, 900);
    });
    on(ratingsRegen, "click", async () => {
      const rid = String(modalState.rid || overlay.dataset.rid || "").trim();
      const route = getRoutes().find((item) => String(item?.id || "") === rid);
      if (!route || String(route.provider || "").toLowerCase() !== "plex") return;
      if (String(ratingsSelect.value || "off").trim().toLowerCase() !== "custom") return;
      if (!confirm(`Regenerate this webhook URL?\n\n${routeLabel(route)}\n\nOnly this route URL will change.`)) return;
      const prev = ratingsRegen.textContent;
      try {
        if (typeof STATE.regenWebhookIds !== "function") throw new Error("regenerate_not_ready");
        ratingsRegen.textContent = "sync";
        await STATE.regenWebhookIds({ route_id: route.id }, ratingsRegen);
        const fresh = getRoutes().find((item) => String(item?.id || "") === rid);
        ratingsUrlCode.textContent = routeCustomRatingsUrl(fresh) || "Save once to generate the route-specific webhook URL.";
        ratingsRegen.textContent = "check";
      } catch {
        ratingsRegen.textContent = "error";
      } finally {
        setTimeout(() => { ratingsRegen.textContent = prev; }, 1200);
      }
    });
    on(save, "click", async () => {
      const rid = String(overlay.dataset.rid || "").trim();
      if (!rid) return closeModal();
      const routes = getRoutes().map((item, index) => normalizeRoute(item, `R${index + 1}`));
      const route = routes.find((item) => item.id === rid);
      if (!route) return closeModal();
      const prev = routeOptions(route);
      const isPlex = String(route.provider || "").toLowerCase() === "plex";
      const ratingsMode = String(ratingsSelect.value || "off");
      const ratingsTargets = modalState.ratingsTargets.slice();
      const nextRatingsMode = !isPlex || ratingsMode !== "custom" || !ratingsTargets.length ? "off" : "custom";
      route.options = normalizeRouteOptions({
        ...(route.options || {}),
        auto_remove_watchlist: String(autoSelect.value || "inherit"),
        ratings: {
          ...(prev.ratings || {}),
          mode: nextRatingsMode,
          targets: nextRatingsMode === "custom" ? ratingsTargets : [],
          webhook_id: nextRatingsMode === "custom" ? prev.ratings.webhook_id : "",
          webhook_token: nextRatingsMode === "custom" ? prev.ratings.webhook_token : "",
        },
      });
      setRoutes(routes);
      await persistCurrentScrobblerState("sc-pms-note");
      await refreshWebhookIds();
      closeModal();
      await renderRoutesUi();
      populate();
    });
    on(d, "keydown", (e) => {
      if (e.key === "Escape" && overlay.style.display !== "none") closeModal();
    });

    d.body.appendChild(overlay);
    STATE.routeOptionsModal = { overlay, open: openModal, close: closeModal };
    closeModal();
    return STATE.routeOptionsModal;
  }

  async function openRouteOptionsModal(rid) {
    const modal = ensureRouteOptionsModal();
    modal.open(rid);
  }

  function normalizeRouteFiltersForSave(route, whitelist, rawId, options = {}) {
    const next = { username_whitelist: asArray(whitelist).map((x) => String(x || "").trim()).filter(Boolean) };
    const value = String(rawId || "").trim();
    if (String(route?.provider || "").toLowerCase() === "plex") {
      const allow = uniqStrings(options.server_uuid_whitelist || []);
      const block = uniqStrings(options.server_uuid_blacklist || []);
      next.server_uuid = allow[0] || value || "";
      next.server_uuid_whitelist = allow.length ? allow : (value ? [value] : []);
      next.server_uuid_blacklist = block;
      if (options.ignore_live_tv_dvr) next.ignore_live_tv_dvr = true;
    } else if (value) {
      next.user_id = value;
    }
    return next;
  }

  function ensureRouteFiltersModal() {
    if (STATE.routeFiltersModal?.overlay?.isConnected) return STATE.routeFiltersModal;

    const overlay = el("div", {
      className: "sc-route-modal hidden",
      style: "position:fixed;inset:0;z-index:10000;background:rgba(4,7,14,.62);backdrop-filter:blur(8px);display:flex;align-items:center;justify-content:center;padding:20px",
    });
    const card = el("div", {
      className: "sc-route-modal-card",
      style: "width:min(1080px,calc(100vw - 32px));max-height:min(88vh,820px);overflow:auto;border-radius:22px;background:linear-gradient(180deg,rgba(12,14,23,.98),rgba(6,8,12,.99));border:1px solid rgba(255,255,255,.10);box-shadow:0 24px 60px rgba(0,0,0,.45);padding:18px 18px 16px",
    });
    overlay.appendChild(card);

    const head = el("div", { style: "display:flex;align-items:flex-start;gap:12px;justify-content:space-between;margin-bottom:14px" });
    const headText = el("div", { style: "display:grid;gap:6px" });
    const title = el("div", { style: "font-size:20px;font-weight:900;color:#f4f7ff", textContent: "Route Filters" });
    const subtitle = el("div", { className: "micro-note", style: "max-width:480px" });
    headText.append(title, subtitle);
    const close = el("button", { type: "button", className: "btn small", textContent: "Close" });
    head.append(headText, close);
    card.appendChild(head);

    const note = el("div", {
      className: "micro-note",
      style: "margin:0 0 14px;padding:12px 14px;border-radius:16px;border:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(255,255,255,.04),rgba(255,255,255,.018))",
      textContent: "Filters are route-specific. Only playback that matches this route's filters will be scrobbled through this route.",
    });
    card.appendChild(note);

    const grid = el("div", { className: "sc-route-filter-grid", style: "display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:16px" });
    const namesBox = el("div", { style: "display:grid;gap:10px;padding:16px 18px;border-radius:18px;border:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(255,255,255,.03),rgba(255,255,255,.015))" });
    const idBox = el("div", { style: "display:grid;gap:10px;padding:16px 18px;border-radius:18px;border:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(255,255,255,.03),rgba(255,255,255,.015))" });
    const uuidAllowBox = el("div", { style: "display:grid;gap:10px;padding:16px 18px;border-radius:18px;border:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(255,255,255,.03),rgba(255,255,255,.015))" });
    const uuidBlockBox = el("div", { style: "display:grid;gap:10px;padding:16px 18px;border-radius:18px;border:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(255,255,255,.03),rgba(255,255,255,.015))" });
    grid.append(namesBox, idBox, uuidAllowBox, uuidBlockBox);
    card.appendChild(grid);

    const otherFiltersBox = el("div", { style: "display:none;grid-column:1 / -1;gap:10px;padding:16px 18px;border-radius:18px;border:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(255,255,255,.03),rgba(255,255,255,.015))" });
    const otherFiltersHead = el("div", { style: "display:inline-flex;align-items:center;gap:8px;flex-wrap:wrap" });
    otherFiltersHead.append(
      el("div", { style: "font-size:12px;font-weight:900;letter-spacing:.12em;text-transform:uppercase;color:rgba(224,230,246,.7)", textContent: "Other Filters" }),
      helpBtnNode ? helpBtnNode("sc-help-watch-other-filters") : el("span")
    );
    const liveTvWrap = el("div", { style: "display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap" });
    const liveTvLabelWrap = el("div", { style: "display:inline-flex;align-items:center;gap:8px;flex-wrap:wrap" });
    const liveTvToggle = el("label", { className: "cx-toggle" });
    const liveTvInput = el("input", { type: "checkbox" });
    const liveTvUi = el("span", { className: "cx-toggle-ui" });
    liveTvUi.setAttribute("aria-hidden", "true");
    const liveTvText = el("span", { className: "cx-toggle-text", textContent: "Ignore Live TV & DVR" });
    const liveTvState = el("span", { className: "cx-toggle-state" });
    liveTvState.setAttribute("aria-hidden", "true");
    liveTvToggle.append(liveTvInput, liveTvUi, liveTvText, liveTvState);
    liveTvLabelWrap.append(
      liveTvToggle,
      helpBtnNode ? helpBtnNode("sc-help-watch-ignore-live-tv-dvr") : el("span")
    );
    liveTvWrap.append(liveTvLabelWrap);
    otherFiltersBox.append(otherFiltersHead, liveTvWrap);
    grid.appendChild(otherFiltersBox);

    const namesHead = el("div", { style: "display:inline-flex;align-items:center;gap:8px;flex-wrap:wrap" });
    namesHead.append(
      el("div", { style: "font-size:12px;font-weight:900;letter-spacing:.12em;text-transform:uppercase;color:rgba(224,230,246,.7)", textContent: "Username whitelist" }),
      helpBtnNode ? helpBtnNode("sc-help-watch-username-whitelist") : el("span")
    );
    const namesChips = el("div", { style: "display:flex;flex-wrap:wrap;gap:6px;min-height:40px;align-content:flex-start" });
    const namesNote = el("div", { className: "micro-note", style: "min-height:18px" });
    const namesRow = el("div", { style: "display:grid;grid-template-columns:minmax(0,1fr) 92px 92px;gap:8px" });
    const namesInput = el("input", {
      id: "sc-route-filter-username-input",
      name: "route_filter_username",
      className: "input",
      placeholder: "Add username...",
    });
    const addBtn = el("button", { type: "button", className: "btn small", textContent: "Add" });
    const pickBtn = el("button", { type: "button", className: "btn small", textContent: "Pick" });
    namesRow.append(namesInput, addBtn, pickBtn);
    namesBox.append(namesHead, namesChips, namesNote, namesRow);

    const idHead = el("div", { style: "display:inline-flex;align-items:center;gap:8px;flex-wrap:wrap" });
    const idLabel = el("div", { style: "font-size:12px;font-weight:900;letter-spacing:.12em;text-transform:uppercase;color:rgba(224,230,246,.7)" });
    idHead.append(idLabel, helpBtnNode ? helpBtnNode("sc-help-watch-server-uuid") : el("span"));
    const idNote = el("div", { className: "micro-note", style: "min-height:18px" });
    const idRow = el("div", { style: "display:grid;grid-template-columns:minmax(0,1fr) 120px;gap:8px;align-items:center" });
    const idInput = el("input", {
      id: "sc-route-filter-id-input",
      name: "route_filter_identity",
      className: "input",
    });
    const fetchBtn = el("button", { type: "button", className: "btn small", textContent: "Fetch" });
    idRow.append(idInput, fetchBtn);
    idBox.append(idHead, idNote, idRow);

    const uuidAllowHead = el("div", { style: "display:inline-flex;align-items:center;gap:8px;flex-wrap:wrap" });
    uuidAllowHead.append(
      el("div", { style: "font-size:12px;font-weight:900;letter-spacing:.12em;text-transform:uppercase;color:rgba(224,230,246,.7)", textContent: "Server UUID allowlist" }),
      helpBtnNode ? helpBtnNode("sc-help-watch-server-uuid-allow") : el("span")
    );
    const uuidAllowChips = el("div", { className: "chips sc-route-filter-uuid-list", style: "display:flex;flex-wrap:wrap;gap:6px;min-height:32px;align-content:flex-start" });
    const uuidAllowNote = el("div", { className: "micro-note", style: "min-height:18px" });
    const uuidAllowRow = el("div", { style: "display:grid;grid-template-columns:minmax(0,1fr) 78px 78px;gap:8px;align-items:center" });
    const uuidAllowInput = el("input", {
      id: "sc-route-filter-uuid-allow-input",
      name: "route_filter_uuid_allow",
      className: "input",
      placeholder: "Add server UUID...",
    });
    const uuidAllowAdd = el("button", { type: "button", className: "btn small", textContent: "Add" });
    const uuidAllowFetch = el("button", { type: "button", className: "btn small", textContent: "Fetch" });
    uuidAllowRow.append(uuidAllowInput, uuidAllowAdd, uuidAllowFetch);
    uuidAllowBox.append(uuidAllowHead, uuidAllowChips, uuidAllowNote, uuidAllowRow);

    const uuidBlockHead = el("div", { style: "display:inline-flex;align-items:center;gap:8px;flex-wrap:wrap" });
    uuidBlockHead.append(
      el("div", { style: "font-size:12px;font-weight:900;letter-spacing:.12em;text-transform:uppercase;color:rgba(224,230,246,.7)", textContent: "Server UUID blacklist" }),
      helpBtnNode ? helpBtnNode("sc-help-watch-server-uuid-block") : el("span")
    );
    const uuidBlockChips = el("div", { className: "chips sc-route-filter-uuid-list", style: "display:flex;flex-wrap:wrap;gap:6px;min-height:32px;align-content:flex-start" });
    const uuidBlockNote = el("div", { className: "micro-note", style: "min-height:18px" });
    const uuidBlockRow = el("div", { style: "display:grid;grid-template-columns:minmax(0,1fr) 78px 78px;gap:8px;align-items:center" });
    const uuidBlockInput = el("input", {
      id: "sc-route-filter-uuid-block-input",
      name: "route_filter_uuid_block",
      className: "input",
      placeholder: "Add server UUID...",
    });
    const uuidBlockAdd = el("button", { type: "button", className: "btn small", textContent: "Add" });
    const uuidBlockFetch = el("button", { type: "button", className: "btn small", textContent: "Fetch" });
    uuidBlockRow.append(uuidBlockInput, uuidBlockAdd, uuidBlockFetch);
    uuidBlockBox.append(uuidBlockHead, uuidBlockChips, uuidBlockNote, uuidBlockRow);

    const foot = el("div", { style: "display:flex;justify-content:flex-end;gap:10px;margin-top:16px" });
    const cancel = el("button", { type: "button", className: "btn small", textContent: "Cancel" });
    const save = el("button", { type: "button", className: "btn small", textContent: "Save" });
    foot.append(cancel, save);
    card.appendChild(foot);

    const modalState = { rid: "", provider: "plex", instance: "default", whitelist: [], uuidAllow: [], uuidBlock: [] };

    const closeModal = () => {
      overlay.classList.add("hidden");
      overlay.style.display = "none";
      overlay.dataset.rid = "";
      modalState.rid = "";
    };

    const renderUuidList = (host, values, removeFn) => {
      host.innerHTML = "";
      values.forEach((value) => host.append(chip(value, removeFn)));
    };

    const addUuidValue = (target, value, source = "added") => {
      const clean = String(value || "").trim();
      if (!clean) return;
      const key = target === "block" ? "uuidBlock" : "uuidAllow";
      const note = target === "block" ? uuidBlockNote : uuidAllowNote;
      const host = target === "block" ? uuidBlockChips : uuidAllowChips;
      const input = target === "block" ? uuidBlockInput : uuidAllowInput;
      if (modalState[key].includes(clean)) {
        note.textContent = `${clean} already added`;
        return;
      }
      modalState[key] = [...modalState[key], clean];
      renderUuidList(host, modalState[key], (item) => removeUuidValue(target, item));
      input.value = "";
      note.textContent = source === "fetched" ? `Fetched ${clean}` : `Added ${clean}`;
    };

    const removeUuidValue = (target, value) => {
      const key = target === "block" ? "uuidBlock" : "uuidAllow";
      const host = target === "block" ? uuidBlockChips : uuidAllowChips;
      modalState[key] = modalState[key].filter((item) => item !== value);
      renderUuidList(host, modalState[key], (item) => removeUuidValue(target, item));
    };

    const renderWhitelist = () => {
      namesChips.innerHTML = "";
      modalState.whitelist.forEach((name) => {
        namesChips.append(chip(name, (value) => {
          modalState.whitelist = modalState.whitelist.filter((item) => item !== value);
          renderWhitelist();
        }));
      });
    };

    const addWhitelistName = (name, source = "added") => {
      const clean = String(name || "").trim();
      if (!clean) return;
      if (modalState.whitelist.includes(clean)) {
        namesNote.textContent = `${clean} already added`;
        return;
      }
      modalState.whitelist = [...modalState.whitelist, clean];
      renderWhitelist();
      namesInput.value = "";
      namesNote.textContent = source === "picked" ? `Picked ${clean}` : `Added ${clean}`;
    };

    const openModal = (rid) => {
      const route = getRoutes().find((item) => String(item?.id || "") === String(rid || ""));
      if (!route) return;
      const prov = String(route.provider || "plex").toLowerCase();
      const filters = routeFilters(route);
      modalState.rid = route.id;
      modalState.provider = prov;
      modalState.instance = String(route.provider_instance || "default");
      modalState.whitelist = filters.whitelist.slice();
      modalState.uuidAllow = filters.server_uuid_whitelist.slice();
      modalState.uuidBlock = filters.server_uuid_blacklist.slice();
      overlay.dataset.rid = route.id;
      title.textContent = `Route Filters - ${route.id}`;
      subtitle.textContent = routeLabel(route);
      namesInput.value = "";
      namesNote.textContent = "";
      idNote.textContent = prov === "plex"
        ? "Optional. Fetch gets the server UUID for the configured Plex instance."
        : "Optional. Fetch gets the user ID for the configured instance.";
      idLabel.textContent = prov === "plex" ? "Server UUID" : "User ID";
      idInput.placeholder = prov === "plex" ? "e.g. abcd1234..." : "e.g. 80ee72c0...";
      idInput.value = prov === "plex" ? "" : filters.user_id;
      uuidAllowInput.value = "";
      uuidBlockInput.value = "";
      uuidAllowNote.textContent = "";
      uuidBlockNote.textContent = "";
      grid.style.gridTemplateColumns = prov === "plex" ? "repeat(3,minmax(0,1fr))" : "1fr 1fr";
      idBox.style.display = prov === "plex" ? "none" : "grid";
      uuidAllowBox.style.display = prov === "plex" ? "grid" : "none";
      uuidBlockBox.style.display = prov === "plex" ? "grid" : "none";
      otherFiltersBox.style.display = prov === "plex" ? "grid" : "none";
      liveTvInput.checked = prov === "plex" && !!filters.ignore_live_tv_dvr;
      renderWhitelist();
      renderUuidList(uuidAllowChips, modalState.uuidAllow, (item) => removeUuidValue("allow", item));
      renderUuidList(uuidBlockChips, modalState.uuidBlock, (item) => removeUuidValue("block", item));
      bindHelpTips(card);
      overlay.classList.remove("hidden");
      overlay.style.display = "flex";
    };

    on(addBtn, "click", () => addWhitelistName(namesInput.value, "added"));
    on(namesInput, "keydown", (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        addWhitelistName(namesInput.value, "added");
      }
    });
    on(pickBtn, "click", async (e) => {
      e.preventDefault();
      const prov = modalState.provider;
      await openUserPicker("watch", pickBtn, {
        provider: prov,
        instance: modalState.instance,
        title: prov === "plex" ? "Pick Plex user" : prov === "emby" ? "Pick Emby user" : "Pick Jellyfin user",
        onPick: (u) => addWhitelistName(u?.name, "picked"),
      });
    });
    const fetchRouteUuid = async () => {
      if (modalState.rid) setActiveRouteFromUi(modalState.rid);
      const result = await API.serverUUID(modalState.instance);
      return String(result?.server_uuid || result?.uuid || result?.id || "").trim();
    };
    const fetchIntoUuidList = async (target) => {
      const note = target === "block" ? uuidBlockNote : uuidAllowNote;
      try {
        const value = await fetchRouteUuid();
        if (!value) {
          note.textContent = "No server UUID";
          return;
        }
        addUuidValue(target, value, "fetched");
      } catch {
        note.textContent = "Fetch failed";
      }
    };
    on(uuidAllowAdd, "click", () => addUuidValue("allow", uuidAllowInput.value, "added"));
    on(uuidAllowInput, "keydown", (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        addUuidValue("allow", uuidAllowInput.value, "added");
      }
    });
    on(uuidAllowFetch, "click", () => fetchIntoUuidList("allow"));
    on(uuidBlockAdd, "click", () => addUuidValue("block", uuidBlockInput.value, "added"));
    on(uuidBlockInput, "keydown", (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        addUuidValue("block", uuidBlockInput.value, "added");
      }
    });
    on(uuidBlockFetch, "click", () => fetchIntoUuidList("block"));
    on(fetchBtn, "click", async () => {
      try {
        const value = await fetchRouteUuid();
        if (!value) {
          idNote.textContent = modalState.provider === "plex" ? "No server UUID" : "No user ID";
          return;
        }
        idInput.value = value;
        idNote.textContent = modalState.provider === "plex" ? "Server UUID fetched" : "User ID fetched";
      } catch {
        idNote.textContent = "Fetch failed";
      }
    });
    on(close, "click", closeModal);
    on(cancel, "click", closeModal);
    on(overlay, "click", (e) => {
      if (e.target === overlay) closeModal();
    });
    on(save, "click", async () => {
      const rid = String(overlay.dataset.rid || "").trim();
      if (!rid) return closeModal();
      const routes = getRoutes().map((item, index) => normalizeRoute(item, `R${index + 1}`));
      const route = routes.find((item) => item.id === rid);
      if (!route) return closeModal();
      route.filters = normalizeRouteFiltersForSave(route, modalState.whitelist, idInput.value, {
        ignore_live_tv_dvr: liveTvInput.checked,
        server_uuid_whitelist: modalState.uuidAllow,
        server_uuid_blacklist: modalState.uuidBlock,
      });
      setRoutes(routes);
      await persistCurrentScrobblerState("sc-pms-note");
      closeModal();
      await renderRoutesUi();
      populate();
    });
    on(d, "keydown", (e) => {
      if (e.key === "Escape" && overlay.style.display !== "none") closeModal();
    });

    d.body.appendChild(overlay);
    STATE.routeFiltersModal = { overlay, open: openModal, close: closeModal };
    closeModal();
    return STATE.routeFiltersModal;
  }

  async function openRouteFiltersModal(rid) {
    const modal = ensureRouteFiltersModal();
    modal.open(rid);
  }

function chip(text, onRemove, onClick) {
    const c = el("span", { className: "chip" });
    const t = el("span", { textContent: text });
    c.title = String(text || "");
    t.title = String(text || "");
    if (onClick) {
      t.style.cursor = "pointer";
      t.title = "Click to select";
      on(t, "click", () => onClick(text));
    }
    const rm = el("span", { className: "rm material-symbols-rounded", textContent: "close" });
    rm.setAttribute("aria-label", `Remove ${text}`);
    rm.title = `Remove ${text}`;
    rm.style.fontSize = "16px";
    rm.style.lineHeight = "1";
    rm.style.fontVariationSettings = "'FILL' 1, 'wght' 400, 'GRAD' 0, 'opsz' 20";
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

  function renderWatcherButton(btn, icon, label) {
    if (!btn) return;
    btn.textContent = "";
    btn.append(
      el("span", { className: "material-symbols-rounded", textContent: icon }),
      el("span", { textContent: label })
    );
  }

  function resetWatcherButton(btn, token) {
    if (!btn || (token && btn.dataset.scActionToken !== token)) return;
    if (btn.dataset.scIdleHtml) btn.innerHTML = btn.dataset.scIdleHtml;
    btn.classList.remove("is-busy", "is-ok", "is-err");
    btn.disabled = btn.dataset.scActionDisabled === "1";
    delete btn.dataset.scActionToken;
    delete btn.dataset.scActionDisabled;
  }

  function setWatcherButtonState(btn, state, label, icon) {
    if (!btn) return "";
    if (!btn.dataset.scIdleHtml) btn.dataset.scIdleHtml = btn.innerHTML;
    if (!btn.dataset.scActionToken) btn.dataset.scActionDisabled = btn.disabled ? "1" : "0";
    const token = `${Date.now()}-${Math.random()}`;
    btn.dataset.scActionToken = token;
    btn.classList.remove("is-busy", "is-ok", "is-err");
    btn.classList.add(`is-${state}`);
    btn.disabled = state === "busy";
    renderWatcherButton(btn, icon, label);
    if (state !== "busy") setTimeout(() => resetWatcherButton(btn, token), 1300);
    return token;
  }

  function applyModeDisable() {
  const sources = scrobbleSourceState();
  const useWebhook = !!sources.webhook;
  const useWatch = !!sources.watcher;

  const webRoot = STATE.webhookHost;
  const watchRoot = STATE.watcherHost;
  if (!webRoot || !watchRoot) return;

  const wh = $("#sc-enable-webhook", STATE.mount);
  const wa = $("#sc-enable-watcher", STATE.mount);

  const webhookOn = !!wh?.checked && useWebhook;
  const watcherOn = !!wa?.checked && useWatch;

  try {
    const routesWrap = $("#sc-routes-wrap", STATE.mount);
    if (routesWrap) routesWrap.style.display = "";
  } catch {}

  $all(".input, input, button, select, textarea", webRoot).forEach((n) => {
    if (!String(n.id || "").startsWith("sc-enable-webhook")) n.disabled = !webhookOn;
  });
  $all(".input, input, button, select, textarea", watchRoot).forEach((n) => {
    if (n.id !== "sc-enable-watcher") n.disabled = !watcherOn;
  });

  const routes = getRoutes();
  const activeRoute = getActiveRoute() || routes[0] || null;
  const prov = String(activeRoute?.provider || "plex").toLowerCase();
  const ctx = activeRouteContext();
  const provInst = ctx.provider_instance;
  const sinkInst = ctx.sink_instance;

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

  bindHelpTips(STATE.mount);

  const plexTokenOk = !!String(plexCfg?.account_token || read("plex.account_token", "") || "").trim();
  const embyTokenOk = !!String(embyCfg?.access_token || read("emby.access_token", "") || "").trim();
  const jellyTokenOk = !!String(jellyCfg?.access_token || read("jellyfin.access_token", "") || "").trim();

  const sinkRaw = String(activeRoute?.sink || "").trim();
  const sink = normSinkCsv(sinkRaw);
  const hasSink = !!sink;

  const traktCfg = overlayCfgFor("trakt", sinkInst);
  const simklCfg = overlayCfgFor("simkl", sinkInst);
  const mdblCfg = overlayCfgFor("mdblist", sinkInst);

  const traktTokenOk = !!String(traktCfg?.access_token || read("trakt.access_token", "") || "").trim();
  const simklTokenOk = !!String(simklCfg?.access_token || read("simkl.access_token", "") || "").trim();
  const mdblTokenOk = !!String(mdblCfg?.api_key || mdblCfg?.access_token || read("mdblist.api_key", "") || read("mdblist.access_token", "") || "").trim();

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
      const item = missing.length > 1 ? "them" : "it";
      const routePart = activeRoute ? ` for ${routeLabel(activeRoute)}` : "";
      sinkErr = `${missing.join(" and ")} ${plural} not configured${routePart}. Go to Connections and configure ${item}, or refresh your browser if you already configured ${item}.`;
    }
  }

  rebuildPlexRatingsDropdown();

  if (watcherOn && !routes.length) setNote("sc-note", "Add at least one route before starting the watcher.", "warn");
  else if (watcherOn && activeRoute && !hasSink) setNote("sc-note", "Choose a sink for the active route before starting the watcher.", "warn");
  else setNote("sc-note", "");

  if (watcherOn && !routes.length) {
    setNote("sc-pms-note", "");
  } else if (watcherOn) {
    if (prov === "plex") {
      if (!plexTokenOk) setNote("sc-pms-note", "Not connected to Plex. Go to Connections - Plex, or refresh your browser if you already configured it", "err");
      else if (!isValidServerUrl(srv)) setNote("sc-pms-note", "Plex Server is required (http(s)://...)", "err");
      else if (hasSink && !sinkOk) setNote("sc-pms-note", sinkErr, "err");
      else setNote("sc-pms-note", "");
    } else if (prov === "emby") {
      if (!embyTokenOk) setNote("sc-pms-note", "Not connected to Emby. Go to Connections - Emby, or refresh your browser if you already configured it", "err");
      else if (hasSink && !sinkOk) setNote("sc-pms-note", sinkErr, "err");
      else setNote("sc-pms-note", "");
    } else {
      if (!jellyTokenOk) setNote("sc-pms-note", "Not connected to Jellyfin. Go to Connections - Jellyfin, or refresh your browser if you already configured it", "err");
      else if (hasSink && !sinkOk) setNote("sc-pms-note", sinkErr, "err");
      else setNote("sc-pms-note", "");
    }
  } else {
    setNote("sc-pms-note", "");
    setNote("sc-note", "");
  }

  const auto = $("#sc-autostart", STATE.mount);
  if (auto) {
    if (watcherOn && !anyStartableRoute()) {
      auto.checked = false;
      auto.disabled = true;
    } else {
      auto.disabled = !watcherOn;
    }
  }

  const startBtn = $("#sc-watch-start", STATE.mount);
  if (startBtn) {
    startBtn.disabled = !watcherOn || !anyStartableRoute();
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

  function buildWebhookDestinations(providerKey) {
    return `<div class="sc-webhook-card sc-webhook-destinations"><div class="sc-webhook-card-head"><div><div class="sc-webhook-card-title">Destinations</div><div class="sc-webhook-card-sub">Choose where this provider sends webhook scrobbles.</div></div></div><div class="sc-pillbar" data-webhook-sinks-provider="${providerKey}"></div><div class="micro-note sc-webhook-sinks-note" data-webhook-sinks-note="${providerKey}"></div></div>`;
  }

  function buildWebhookEndpoint(providerKey, label, codeId, copyId) {
    const key = providerKey === "jellyfin" ? "jellyfintrakt" : providerKey === "emby" ? "embytrakt" : "plextrakt";
    return `<div class="sc-webhook-card sc-webhook-card-wide"><div class="sc-webhook-card-head"><div><div class="sc-webhook-card-title">${providerLogImg(providerKey)}<span>${label} endpoint</span></div><div class="sc-webhook-card-sub">Enable the profiles that may scrobble through webhooks.</div></div></div><div class="micro-note sc-webhook-sinks-note" data-webhook-conn-note="${providerKey}"></div><div class="sc-webhook-url-row"><div class="sc-webhook-row-label">Default</div><label class="sc-toggle sc-webhook-enable" title="Use this profile for webhook scrobbling"><input type="checkbox" data-webhook-enable="${providerKey}" data-webhook-enable-instance="default"><span class="one-line">Enabled</span></label><code id="${codeId}"></code><button id="${copyId}" class="btn small sc-webhook-round-btn material-symbols-rounded" title="Copy endpoint" aria-label="Copy endpoint">content_copy</button><button type="button" class="btn small sc-webhook-round-btn sc-webhook-round-danger material-symbols-rounded" title="Regenerate endpoint" aria-label="Regenerate endpoint" data-webhook-regen-key="${key}" data-webhook-regen-label="${label} default endpoint">refresh</button></div><label class="sc-webhook-profile-toggle"><input type="checkbox" data-webhook-profile-toggle="${providerKey}"><span>Show profile-specific URLs</span></label><div class="sc-webhook-profile-list" data-webhook-profile-list="${providerKey}"></div></div>`;
  }

  function buildWebhookOptions(providerKey, { plex = false } = {}) {
    const suffix = providerKey === "plex" ? "" : providerKey === "jellyfin" ? "-jf" : "-emby";
    const ratings = plex ? `<span class="cx-switch-wrap"><label class="sc-toggle"><input type="checkbox" id="sc-webhook-plex-ratings"><span class="one-line">Enable ratings</span></label>${helpBtn("sc-help-webhook-plex-ratings")}</span>` : "";
    return `<div class="sc-webhook-card"><div class="sc-webhook-card-head"><div><div class="sc-webhook-card-title">Options</div><div class="sc-webhook-card-sub">Configure how webhook events are handled.</div></div></div><div class="sc-webhook-actions"><span class="cx-switch-wrap"><label class="sc-toggle"><input type="checkbox" id="sc-delete-plex-webhook${suffix}"><span class="one-line">Auto-remove from Watchlists</span></label>${helpBtn("sc-help-auto-remove")}</span>${ratings}</div></div>`;
  }

  function buildWebhookPlexFilters() {
    return `<div class="sc-webhook-card sc-webhook-card-wide"><div class="sc-webhook-card-head"><div><div class="sc-webhook-card-title">Filters</div><div class="sc-webhook-card-sub">Limit accepted Plex webhook events before they can scrobble.</div></div></div><div class="sc-filter-grid sc-webhook-filter-grid"><div><div class="muted">Username whitelist</div><div id="sc-whitelist-webhook" class="chips" style="margin-top:4px"></div><div id="sc-users-note-webhook" class="micro-note"></div><div class="sc-filter-input-row sc-filter-input-row--actions" style="margin-top:6px"><input id="sc-user-input-webhook" class="input" placeholder="Add username..."><button id="sc-add-user-webhook" class="btn small">Add</button><button id="sc-load-users-webhook" class="btn small">Pick</button></div></div><div><div class="muted">Server UUID allowlist</div><div id="sc-server-uuid-allow-webhook" class="chips" style="margin-top:4px"></div><div id="sc-uuid-note-webhook" class="micro-note"></div><div class="sc-filter-input-row sc-filter-input-row--actions" style="margin-top:6px"><input id="sc-server-uuid-allow-input-webhook" class="input" placeholder="Add server UUID..."><button id="sc-add-server-uuid-allow-webhook" class="btn small">Add</button><button id="sc-fetch-uuid-allow-webhook" class="btn small">Fetch</button></div></div><div><div class="muted">Server UUID blacklist</div><div id="sc-server-uuid-block-webhook" class="chips" style="margin-top:4px"></div><div id="sc-uuid-block-note-webhook" class="micro-note"></div><div class="sc-filter-input-row sc-filter-input-row--actions" style="margin-top:6px"><input id="sc-server-uuid-block-input-webhook" class="input" placeholder="Add server UUID..."><button id="sc-add-server-uuid-block-webhook" class="btn small">Add</button><button id="sc-fetch-uuid-webhook" class="btn small">Fetch</button></div></div></div></div>`;
  }

  function buildWebhookProviderPanel(providerKey, label, codeId, copyId, opts = {}) {
    return `<div class="cw-subpanel${opts.active ? " active" : ""}" data-sub="${providerKey}"><div class="sc-webhook-grid">${buildWebhookEndpoint(providerKey, label, codeId, copyId)}${buildWebhookDestinations(providerKey)}${buildWebhookOptions(providerKey, opts)}${opts.plex ? buildWebhookPlexFilters() : ""}</div></div>`;
  }
  

  function buildUI() {
    injectStyles();

        if (STATE.webhookHost) {
      STATE.webhookHost.innerHTML = `<div class="cw-panel"><div class="cw-meta-provider-panel active" data-provider="webhook">${buildShellHeader({ title: "Webhooks", copy: "Media-server callbacks for setups where Watcher is not the right fit.", tiles: `<button type="button" class="cw-subtile active" data-sub="plex">Plex</button><button type="button" class="cw-subtile" data-sub="jellyfin">Jellyfin</button><button type="button" class="cw-subtile" data-sub="emby">Emby</button><button type="button" class="cw-subtile" data-sub="advanced">Advanced</button>`, toggleId: "sc-enable-webhook", toggleText: "Enable", tilesLabel: "Webhook sections" })}<div id="sc-webhook-warning" class="micro-note sc-webhook-recommend" style="margin-top:10px"></div><div id="sc-endpoint-note" class="micro-note"></div><div class="cw-subpanels" style="gap:14px">${buildWebhookProviderPanel("plex", "Plex", "sc-webhook-url-plex", "sc-copy-plex", { active: true, plex: true })}${buildWebhookProviderPanel("jellyfin", "Jellyfin", "sc-webhook-url-jf", "sc-copy-jf")}${buildWebhookProviderPanel("emby", "Emby", "sc-webhook-url-emby", "sc-copy-emby")}<div class="cw-subpanel" data-sub="advanced"><div class="sc-webhook-advanced"><div class="sc-subbox"><div class="head">Advanced</div><div class="body"><div class="sc-adv-grid">${buildAdvField("sc-pause-debounce-webhook", "Pause", "sc-help-adv-pause", DEFAULTS.watch.pause_debounce_seconds)}${buildAdvField("sc-suppress-start-webhook", "Suppress", "sc-help-adv-suppress", DEFAULTS.watch.suppress_start_at)}${buildAdvField("sc-regress-webhook", "Regress %", "sc-help-adv-regress", DEFAULTS.trakt.regress_tolerance_percent)}${buildAdvField("sc-watched-at-webhook", "Watched threshold", "sc-help-adv-watched-at", DEFAULTS.trakt.watched_at)}${buildAdvField("sc-force-stop-webhook", "Defensive final-stop trust threshold", "sc-help-adv-force-stop", DEFAULTS.trakt.force_stop_at)}</div><div class="micro-note" style="margin-top:6px">Empty resets to defaults. Values are 1-100.</div></div></div></div></div></div></div></div>`;

      STATE.webhookHost.querySelector(".cw-panel")?.classList.add("sc-shell");
      enhanceWebhookFiltersUI(STATE.webhookHost);
      syncWebhookSinkPills(STATE.webhookHost);

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
      STATE.watcherHost.innerHTML = `<style> .cc-wrap{display:grid;grid-template-columns:1fr 1fr;gap:16px} .cc-card{padding:14px;border-radius:12px;background:var(--panel,#111);box-shadow:0 0 0 1px rgba(255,255,255,.05) inset} .cc-head{display:flex;justify-content:space-between;align-items:center;margin-bottom:10px} .cc-body{display:grid;gap:14px} .cc-gauge{width:100%;min-height:68px;display:flex;align-items:center;gap:14px;flex-wrap:wrap;padding:14px 16px;border-radius:14px;background:rgba(255,255,255,.05);box-shadow:inset 0 0 0 1px rgba(255,255,255,.08)} .cc-state{display:flex;flex-direction:column;line-height:1.15} .cc-state .lbl{font-size:12px;opacity:.75} .cc-state .val{font-size:22px;font-weight:800;letter-spacing:.2px} .cc-meta{display:flex;gap:16px;flex-wrap:wrap;font-size:12px;opacity:.85} .cc-actions{display:flex;gap:12px;justify-content:center;flex-wrap:wrap} .cc-auto{display:flex;justify-content:center;margin-top:2px} #scrob-watcher .status-dot{width:16px;height:16px;border-radius:50%;box-shadow:0 0 18px currentColor} #scrob-watcher .status-dot.on{background:#22c55e;color:#22c55e} #scrob-watcher .status-dot.off{background:#ef4444;color:#ef4444} @media (max-width:900px){.cc-wrap{grid-template-columns:1fr}} .sc-box{display:block;margin-top:12px;border-radius:12px;background:var(--panel,#111);box-shadow:0 0 0 1px rgba(255,255,255,.05) inset} .sc-box>.body{padding:12px 14px} </style><div class="cw-panel"><div class="cw-meta-provider-panel active" data-provider="watcher"><div class="cw-panel-head"><div class="cw-panel-head-main"><div class="cw-panel-title">Watcher</div><div class="muted">Monitor playback and scrobble automatically.</div></div><div style="display:grid;justify-items:end;gap:10px"><div class="cw-subtiles" aria-label="Watcher sections"><button type="button" class="cw-subtile active" data-sub="watcher">Watcher</button><button type="button" class="cw-subtile" data-sub="advanced">Advanced</button></div><div style="display:flex;justify-content:flex-end"><label class="cx-toggle"><input type="checkbox" id="sc-enable-watcher"><span class="cx-toggle-ui" aria-hidden="true"></span><span class="cx-toggle-text">Enable</span><span class="cx-toggle-state" aria-hidden="true"></span></label></div></div></div><div class="cw-subpanels" style="gap:8px"><div class="cw-subpanel active" data-sub="watcher"><div id="sc-routes-wrap" class="sc-box" style="display:none;margin:2px 0 10px"><div class="body"><div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:8px"><div style="display:inline-flex;align-items:center;gap:8px;font-size:12px;font-weight:900;letter-spacing:.12em;text-transform:uppercase;color:rgba(224,230,246,.7)">Routes ${helpBtn("sc-help-watch-routes")}</div><div style="margin-left:auto;display:flex;gap:8px;align-items:center;flex-wrap:wrap"><button type="button" id="sc-route-add" class="btn small">Add Route</button></div></div><div id="sc-routes" class="sc-route-table"></div></div></div><div id="sc-note" class="micro-note" style="display:none;margin:0"></div><div class="cc-wrap"><div class="cc-card" id="sc-card-status"><div class="cc-head"><div>Watcher Status</div><span id="sc-status-badge" class="badge is-off">Stopped</span></div><div class="cc-body"><div class="cc-gauge"><span id="sc-status-dot" class="status-dot off"></span><div class="cc-state"><span class="lbl">Status</span><span id="sc-status-text" class="val">Inactive</span></div></div><div class="cc-meta"><span id="sc-status-last" class="micro-note"></span><span id="sc-status-up" class="micro-note"></span></div><div class="cc-actions"><button id="sc-watch-start" class="btn small">Start</button><button id="sc-watch-stop" class="btn small">Stop</button><button id="sc-watch-refresh" class="btn small">Refresh</button></div><div class="cc-auto"><label class="cx-toggle"><input type="checkbox" id="sc-autostart"><span class="cx-toggle-ui" aria-hidden="true"></span><span class="cx-toggle-text">Autostart on boot</span><span class="cx-toggle-state" aria-hidden="true"></span></label></div></div></div><div class="cc-card" id="sc-card-server"><div class="cc-head"><div><span id="sc-server-label">Media Server</span><span id="sc-server-required" class="pill req"></span></div></div><div id="sc-pms-note" class="micro-note" style="margin-top:2px"></div><div style="margin-top:12px"><div class="muted">Server URL (http(s)://host[:port])</div><input id="sc-pms-input" class="input" placeholder="http://192.168.1.10:32400" readonly/></div><div class="sc-subbox" style="margin-top:14px"><div class="head">Options</div><div class="body"><div class="sc-opt-col"><span class="cx-switch-wrap"><label class="sc-toggle"><input type="checkbox" id="sc-delete-plex-watch"><span class="one-line">Auto-remove from Watchlists</span></label>${helpBtn("sc-help-auto-remove")}</span><div id="sc-plex-ratings-wrap" style="display:none"><div class="sc-opt-row"><div class="muted" style="margin:0">Enable ratings</div>${helpBtn("sc-help-watch-plex-ratings")}<div id="sc-plex-ratings-pills" class="sc-pillbar" role="group" aria-label="Ratings"></div></div><div class="sc-opt-row" style="margin-top:6px"><select id="sc-plex-ratings" class="input" style="display:none;width:240px"><option value="none">None</option><option value="trakt">Trakt</option><option value="simkl">SIMKL</option><option value="mdblist">MDBList</option><option value="simkl,trakt">Trakt & SIMKL</option><option value="trakt,mdblist">Trakt & MDBList</option><option value="simkl,mdblist">SIMKL & MDBList</option><option value="simkl,trakt,mdblist">Trakt & SIMKL & MDBList</option></select><div id="sc-plexwatcher-url-wrap" class="codepair" style="display:none"><code id="sc-plexwatcher-url"></code><button id="sc-copy-plexwatcher" class="btn small">Copy</button></div></div><div id="sc-plexwatcher-note" class="micro-note" style="margin-top:6px"></div></div></div></div></div></div></div></div></div><div class="cw-subpanel" data-sub="advanced"><div class="sc-box sc-advanced" id="sc-advanced"><div style="display:flex;justify-content:flex-end;margin-bottom:10px">${helpBtn("sc-help-watch-advanced")}</div><div class="body"><div class="sc-adv-grid">${buildAdvField("sc-pause-debounce", "Pause", "sc-help-adv-pause", DEFAULTS.watch.pause_debounce_seconds)}${buildAdvField("sc-suppress-start", "Suppress", "sc-help-adv-suppress", DEFAULTS.watch.suppress_start_at)}${buildAdvField("sc-regress", "Regress", "sc-help-adv-regress", DEFAULTS.trakt.regress_tolerance_percent)}${buildAdvField("sc-watched-at", "Watched threshold", "sc-help-adv-watched-at", DEFAULTS.trakt.watched_at)}${buildAdvField("sc-force-stop", "Defensive final-stop trust threshold", "sc-help-adv-force-stop", DEFAULTS.trakt.force_stop_at)}</div><div class="sc-adv-grid" style="grid-template-columns:repeat(1,minmax(0,1fr));margin-top:10px">${buildAdvField("sc-progress-step", "Progress step", "sc-help-adv-progress-step", DEFAULTS.trakt.progress_step, { min: 1, max: 25, step: 1 })}</div><div class="micro-note" style="margin-top:6px">Empty resets to defaults. Percent fields are 1-100. Progress step is 1-25.</div></div></div></div></div></div></div>`;
      const watcherCopy = $("#sc-copy-plexwatcher", STATE.watcherHost);
      watcherCopy?.classList.add("sc-webhook-round-btn", "material-symbols-rounded");
      if (watcherCopy) {
        watcherCopy.type = "button";
        watcherCopy.textContent = "content_copy";
        watcherCopy.title = "Copy endpoint";
        watcherCopy.setAttribute("aria-label", "Copy endpoint");
        if (!$("#sc-regenerate-plexwatcher", STATE.watcherHost)) {
          const regen = document.createElement("button");
          regen.type = "button";
          regen.id = "sc-regenerate-plexwatcher";
          regen.className = "btn small sc-webhook-round-btn sc-webhook-round-danger material-symbols-rounded";
          regen.title = "Regenerate endpoint";
          regen.setAttribute("aria-label", "Regenerate endpoint");
          regen.textContent = "refresh";
          watcherCopy.insertAdjacentElement("afterend", regen);
        }
      }

      STATE.watcherHost.querySelector(".cw-panel")?.classList.add("sc-shell");
      decorateWatcherCards();
      enhanceWatcherAdvancedUI(STATE.watcherHost);
      try {
        const reloadBtn = $("#sc-watch-refresh", STATE.watcherHost);
        if (reloadBtn) {
          reloadBtn.textContent = "Reload";
          reloadBtn.title = "Reload the running watcher from the current config.";
        }
      } catch {}

      // Tabs: Watcher / Advanced
      const tabKey = "cw.ui.scrobbler.watcher.tab.v1";
      const root = STATE.watcherHost;
      const selectTab = (sub, opts = {}) => {
        const raw = (sub || "watcher").toLowerCase();
        const want = raw === "advanced" ? "advanced" : "watcher";
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
          ["#sc-watched-at-webhook", "scrobble.trakt.watched_at", DEFAULTS.trakt.watched_at],
          ["#sc-force-stop-webhook", "scrobble.trakt.force_stop_at", DEFAULTS.trakt.force_stop_at],
          ["#sc-regress-webhook", "scrobble.trakt.regress_tolerance_percent", DEFAULTS.trakt.regress_tolerance_percent],
          ["#sc-watched-at", "scrobble.trakt.watched_at", DEFAULTS.trakt.watched_at],
          ["#sc-force-stop", "scrobble.trakt.force_stop_at", DEFAULTS.trakt.force_stop_at],
          ["#sc-regress", "scrobble.trakt.regress_tolerance_percent", DEFAULTS.trakt.regress_tolerance_percent],
        ]
      : [
          ["#sc-watched-at", "scrobble.trakt.watched_at", DEFAULTS.trakt.watched_at],
          ["#sc-force-stop", "scrobble.trakt.force_stop_at", DEFAULTS.trakt.force_stop_at],
          ["#sc-regress", "scrobble.trakt.regress_tolerance_percent", DEFAULTS.trakt.regress_tolerance_percent],
          ["#sc-watched-at-webhook", "scrobble.trakt.watched_at", DEFAULTS.trakt.watched_at],
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

  function webhookServerUuidAllowlist() {
    return uniqStrings([
      ...asArray(read("scrobble.webhook.filters_plex.server_uuid_whitelist", [])),
      String(read("scrobble.webhook.filters_plex.server_uuid", "") || "").trim(),
    ]);
  }

  function webhookServerUuidBlacklist() {
    return uniqStrings(read("scrobble.webhook.filters_plex.server_uuid_blacklist", []));
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
    removeUserWebhook,
  }) || {};
  const closeUserPicker = scUserPicker.closeUserPicker || (() => {});
  const openUserPicker = scUserPicker.openUserPicker || (async () => {});


  function webhookPlexRatingsEnabled() {
    return !!(
      read("scrobble.webhook.plex_trakt_ratings", false) ||
      read("scrobble.webhook.plex_simkl_ratings", false) ||
      read("scrobble.webhook.plex_mdblist_ratings", false)
    );
  }

  function webhookPlexRatingsPairs(enabled) {
    const targets = new Set(sinkCsvItems(webhookAvailableSinkCsv("plex")));
    const on = !!enabled;
    return [
      ["scrobble.webhook.plex_trakt_ratings", on && targets.has("trakt")],
      ["scrobble.webhook.plex_simkl_ratings", on && targets.has("simkl")],
      ["scrobble.webhook.plex_mdblist_ratings", on && targets.has("mdblist")],
    ];
  }

  function setWebhookPlexRatings(enabled) {
    const pairs = webhookPlexRatingsPairs(enabled);
    pairs.forEach(([path, value]) => write(path, value));
    return pairs;
  }

  function rebuildPlexRatingsDropdown() {
    const wrap = $("#sc-plex-ratings-wrap", STATE.mount);
    const sel = $("#sc-plex-ratings", STATE.mount);
    if (!wrap || !sel) return;

    wrap.style.display = "";
    const label = wrap.querySelector(".sc-opt-row .muted");
    if (label) label.textContent = "Enable ratings";

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

    sel.innerHTML = options.map(([v, label]) => `<option value="${v}">${label}</option>`).join("");
    sel.value = globalPlexRatingsTargets() || "none";
    try { syncPlexRatingsPillsFromSelect(); } catch {}

    try {
      updatePlexWatcherWebhookUrl();
    } catch {}
  }

  function updatePlexWatcherWebhookUrl() {
    const wrap = $("#sc-plexwatcher-url-wrap", STATE.mount);
    const code = $("#sc-plexwatcher-url", STATE.mount);
    if (!wrap || !code) return;

    const on = !!globalPlexRatingsTargets();
    wrap.style.display = on ? "flex" : "none";

    try {
      const buttons = [$("#sc-copy-plexwatcher", STATE.mount), $("#sc-regenerate-plexwatcher", STATE.mount)].filter(Boolean);
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

      buttons.forEach((btn) => {
        btn.style.flex = "0 0 auto";
        btn.style.whiteSpace = "nowrap";
      });
    } catch {}

    if (on) {
      code.textContent = `${location.origin}/webhook/plexwatcher?${STATE.webhookIds?.plexwatcher || ""}`;
    }
    else setNote("sc-plexwatcher-note", "");
  }

  async function refreshCfgBeforePopulate() {
    try {
      const fresh = await API.cfgGet();
      if (fresh && typeof fresh === "object") {
        STATE.cfg = fresh;
        ensureWatchRoutesArray();

        // Provider instances (profiles) can change outside this view; drop instance options cache.
        try { delete STATE._routesCache; } catch {}

        const uiEnabled = STATE.ui?.scrobbleEnabled;
        const uiSources = STATE.ui?.scrobbleSources;
        const uiAutostart = STATE.ui?.watchAutostart;

        const backendEnabled = !!fresh?.scrobble?.enabled;
        const backendSources = scrobbleSourceState(fresh);
        const backendAutostart = !!fresh?.scrobble?.watch?.autostart;

        if (typeof uiEnabled === "boolean") {
          if (backendEnabled === uiEnabled) STATE.ui.scrobbleEnabled = null;
          else deepSet(STATE.cfg, "scrobble.enabled", uiEnabled);
        }
        if (uiSources && typeof uiSources === "object") {
          const nextSources = { webhook: !!uiSources.webhook, watcher: !!uiSources.watcher };
          if (backendSources.webhook === nextSources.webhook && backendSources.watcher === nextSources.watcher) {
            STATE.ui.scrobbleSources = null;
          } else {
            deepSet(STATE.cfg, "scrobble.sources", nextSources);
            deepSet(STATE.cfg, "scrobble.enabled", nextSources.webhook || nextSources.watcher);
            deepSet(STATE.cfg, "scrobble.mode", scrobbleLegacyMode(nextSources));
          }
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
  ensureWatchRoutesArray();
  try {
    if (isRoutesMode()) {
      const ar = getActiveRoute() || getRoutes()[0] || null;
      if (ar) applyRouteView(ar);
      renderRoutesUi().catch(() => {});
    }
  } catch {}
  const sources = scrobbleSourceState();
  const useWebhook = !!sources.webhook;
  const useWatch = !!sources.watcher;
  const prov = provider();

  const whEl = $("#sc-enable-webhook", STATE.mount);
  const waEl = $("#sc-enable-watcher", STATE.mount);

  if (whEl) whEl.checked = useWebhook;
  ["#sc-enable-webhook-jf", "#sc-enable-webhook-emby", "#sc-enable-webhook-adv"].forEach((id) => { const n = $(id, STATE.mount); if (n) n.checked = useWebhook; });
  if (waEl) waEl.checked = useWatch;
  refreshHybridWarning();

  const wlWeb = asArray(read("scrobble.webhook.filters_plex.username_whitelist", []));
  const hostWB = $("#sc-whitelist-webhook", STATE.mount);
  if (hostWB) {
    hostWB.innerHTML = "";
    wlWeb.forEach((u) => hostWB.append(chip(u, removeUserWebhook)));
  }

  const suAllowHost = $("#sc-server-uuid-allow-webhook", STATE.mount);
  if (suAllowHost) {
    suAllowHost.innerHTML = "";
    webhookServerUuidAllowlist().forEach((uuid) => suAllowHost.append(chip(uuid, removeWebhookServerUuidAllow)));
  }
  const suBlockHost = $("#sc-server-uuid-block-webhook", STATE.mount);
  if (suBlockHost) {
    suBlockHost.innerHTML = "";
    webhookServerUuidBlacklist().forEach((uuid) => suBlockHost.append(chip(uuid, removeWebhookServerUuidBlock)));
  }

  const base = location.origin;
  const plexCode = $("#sc-webhook-url-plex", STATE.mount);
  const jfCode = $("#sc-webhook-url-jf", STATE.mount);
  const embyCode = $("#sc-webhook-url-emby", STATE.mount);
  function _withWebhookId(path, id) {
    return id ? `${path}?${id}` : path;
  }

  function applyWebhookUrls() {
    const base = location.origin;
    const ids = STATE.webhookIds || {};
    const plexCode = $("#sc-webhook-url-plex", STATE.mount);
    const jfCode = $("#sc-webhook-url-jf", STATE.mount);
    const embyCode = $("#sc-webhook-url-emby", STATE.mount);
    if (plexCode) plexCode.textContent = _withWebhookId(`${base}/webhook/plex`, ids.plextrakt);
    if (jfCode) jfCode.textContent = _withWebhookId(`${base}/webhook/jellyfin`, ids.jellyfintrakt);
    if (embyCode) embyCode.textContent = _withWebhookId(`${base}/webhook/emby`, ids.embytrakt);
    renderWebhookProfileUrls();
  }

  function webhookPathForProvider(providerKey) {
    const key = String(providerKey || "").trim().toLowerCase();
    if (key === "jellyfin") return "/webhook/jellyfin";
    if (key === "emby") return "/webhook/emby";
    return "/webhook/plex";
  }

  function profileWebhookUrl(providerKey, token) {
    const secret = String(token || "").trim();
    return `${location.origin}${webhookPathForProvider(providerKey)}${secret ? `?profile=${encodeURIComponent(secret)}` : ""}`;
  }

  function renderWebhookProfileUrls() {
    $all("[data-webhook-profile-list]", STATE.mount || d).forEach((host) => {
      const providerKey = String(host.getAttribute("data-webhook-profile-list") || "").trim().toLowerCase();
      const hooks = asArray(STATE.profileWebhookHooks).filter((hook) => {
        const provider = String(hook?.provider || "").trim().toLowerCase();
        const inst = String(hook?.instance || "default").trim().toLowerCase() || "default";
        return provider === providerKey && inst !== "default";
      });
      host.innerHTML = hooks.length
        ? hooks.map((hook) => {
            const inst = String(hook?.instance || "default").trim() || "default";
            const url = profileWebhookUrl(providerKey, hook?.webhook_token);
            return `<div class="sc-webhook-profile-row"><div class="sc-webhook-profile-name">${esc(inst)}</div><label class="sc-toggle sc-webhook-enable" title="Use this profile for webhook scrobbling"><input type="checkbox" data-webhook-enable="${esc(providerKey)}" data-webhook-enable-instance="${esc(inst)}"${webhookProfileEnabled(providerKey, inst) ? " checked" : ""}><span class="one-line">Enabled</span></label><code>${esc(url)}</code><button type="button" class="btn small sc-webhook-round-btn material-symbols-rounded" title="Profile filters" aria-label="Profile filters" data-webhook-profile-filter="${esc(providerKey)}" data-webhook-profile-instance="${esc(inst)}">settings</button><button type="button" class="btn small sc-webhook-round-btn material-symbols-rounded" title="Copy endpoint" aria-label="Copy endpoint" data-copy-webhook-url="${esc(url)}">content_copy</button><button type="button" class="btn small sc-webhook-round-btn sc-webhook-round-danger material-symbols-rounded" title="Regenerate endpoint" aria-label="Regenerate endpoint" data-webhook-regen-provider="${esc(providerKey)}" data-webhook-regen-instance="${esc(inst)}">refresh</button></div>`;
          }).join("")
        : "";
      const toggle = $(`[data-webhook-profile-toggle="${providerKey}"]`, STATE.mount || d);
      const toggleWrap = toggle?.closest?.(".sc-webhook-profile-toggle");
      if (toggleWrap) toggleWrap.style.display = hooks.length ? "" : "none";
      if (toggle && !hooks.length) toggle.checked = false;
      host.classList.toggle("is-open", !!toggle?.checked);
    });
    syncWebhookEnableToggles();
  }

  function syncWebhookEnableToggles(root = STATE.mount) {
    $all("[data-webhook-enable]", root || d).forEach((input) => {
      const providerKey = String(input.getAttribute("data-webhook-enable") || "").trim().toLowerCase();
      const inst = String(input.getAttribute("data-webhook-enable-instance") || "default").trim() || "default";
      if (!providerKey) return;
      const connected = mediaProviderConnected(providerKey, inst);
      input.disabled = !connected;
      input.checked = connected && webhookProfileEnabled(providerKey, inst);
      const label = input.closest(".sc-webhook-enable");
      if (label) {
        label.classList.toggle("is-disabled", !connected);
        label.title = connected
          ? "Use this profile for webhook scrobbling"
          : `Connect ${providerLabel(providerKey) || providerKey} first`;
      }
    });
    syncWebhookConnNotes(root);
  }

  function syncWebhookConnNotes(root = STATE.mount) {
    $all("[data-webhook-conn-note]", root || d).forEach((note) => {
      const providerKey = String(note.getAttribute("data-webhook-conn-note") || "").trim().toLowerCase();
      const name = providerLabel(providerKey) || providerKey;
      const connected = mediaProviderConnected(providerKey, "default");
      note.classList.toggle("warn", !connected);
      note.textContent = connected
        ? ""
        : `${name} is not connected - connect it in Connections before using this webhook.`;
    });
  }

  function webhookProfileFilterKey(providerKey) {
    const key = String(providerKey || "").trim().toLowerCase();
    return key === "emby" ? "filters_emby" : key === "jellyfin" ? "filters_jellyfin" : "filters_plex";
  }

  function webhookProfileFilterPath(providerKey, instance) {
    const key = String(providerKey || "").trim().toLowerCase();
    const inst = String(instance || "default").trim() || "default";
    return `scrobble.webhook.profiles.${key}.${inst}.${webhookProfileFilterKey(key)}`;
  }

  function webhookProfileFilters(providerKey, instance) {
    const key = String(providerKey || "").trim().toLowerCase();
    const current = read(webhookProfileFilterPath(key, instance), {}) || {};
    if (key === "plex") {
      const allow = uniqStrings([
        ...asArray(current.server_uuid_whitelist || []),
        String(current.server_uuid || "").trim(),
      ]);
      return {
        username_whitelist: uniqStrings(current.username_whitelist || []),
        server_uuid: allow[0] || "",
        server_uuid_whitelist: allow,
        server_uuid_blacklist: uniqStrings(current.server_uuid_blacklist || []),
      };
    }
    return { username_whitelist: uniqStrings(current.username_whitelist || []) };
  }

  function openWebhookProfileFilters(providerKey, instance) {
    const key = String(providerKey || "").trim().toLowerCase();
    const inst = String(instance || "default").trim() || "default";
    const filters = webhookProfileFilters(key, inst);
    const overlay = el("div", {
      className: "sc-route-modal sc-profile-filter-modal",
      style: "position:fixed;inset:0;z-index:10000;background:rgba(4,7,14,.62);backdrop-filter:blur(8px);display:flex;align-items:center;justify-content:center;padding:16px",
    });
    const card = el("div", {
      className: "sc-route-modal-card sc-profile-filter-modal-card",
      style: "width:min(760px,calc(100vw - 28px));max-height:min(84vh,680px);overflow:auto;border-radius:22px;background:linear-gradient(180deg,rgba(12,14,23,.98),rgba(6,8,12,.99));border:1px solid rgba(255,255,255,.10);box-shadow:0 24px 60px rgba(0,0,0,.45);padding:16px",
    });
    overlay.appendChild(card);
    const head = el("div", { className: "sc-profile-filter-head" });
    const headText = el("div", { className: "sc-profile-filter-title" });
    headText.append(
      el("div", { className: "sc-profile-filter-title-main", textContent: "Profile Filters" }),
      el("div", { className: "micro-note", textContent: `${providerLabel(key) || key} - ${inst}` })
    );
    const close = el("button", { type: "button", className: "btn small sc-modal-close", innerHTML: `<span class="material-symbols-rounded">close</span><span>CLOSE</span>` });
    head.append(headText, close);
    card.appendChild(head);

    const grid = el("div", { className: `sc-profile-filter-grid${key === "plex" ? " is-plex" : ""}` });
    const makeListBox = (titleText, values, placeholder, extraButtons = [], helpId = "") => {
      const box = el("div", { className: "sc-profile-filter-box" });
      const title = el("div", { className: "sc-profile-filter-box-title" });
      title.append(el("span", { textContent: titleText }));
      if (helpId) {
        const help = helpBtnNode ? helpBtnNode(helpId) : null;
        if (help) title.append(help);
      }
      const chips = el("div", { className: "chips", style: "display:flex;flex-wrap:wrap;gap:6px;min-height:40px;align-content:flex-start" });
      const note = el("div", { className: "micro-note", style: "min-height:18px" });
      const input = el("input", { className: "input", placeholder });
      const add = el("button", { type: "button", className: "btn small", textContent: "Add" });
      const buttons = [add, ...extraButtons];
      const row = el("div", { className: "sc-profile-filter-actions", style: `grid-template-columns:minmax(0,1fr) repeat(${buttons.length},68px)` });
      const state = { values: uniqStrings(values || []) };
      const redraw = () => {
        chips.innerHTML = "";
        state.values.forEach((value) => chips.append(chip(value, () => {
          state.values = state.values.filter((item) => item !== value);
          redraw();
        })));
      };
      const addValue = () => {
        const value = String(input.value || "").trim();
        if (!value) return false;
        if (state.values.includes(value)) {
          note.textContent = `${value} already added`;
          return false;
        }
        state.values.push(value);
        input.value = "";
        redraw();
        note.textContent = `Added ${value}`;
        return true;
      };
      on(add, "click", addValue);
      on(input, "keydown", (e) => {
        if (e.key === "Enter") {
          e.preventDefault();
          addValue();
        }
      });
      row.append(input, ...buttons);
      box.append(title, chips, note, row);
      redraw();
      return { box, state, input, note, redraw };
    };

    const pickUser = el("button", { type: "button", className: "btn small", textContent: "Pick" });
    const users = makeListBox("Username whitelist", filters.username_whitelist, "Add username...", [pickUser], "sc-help-watch-username-whitelist");
    grid.appendChild(users.box);
    let allow = null;
    let block = null;
    if (key === "plex") {
      const fetchAllow = el("button", { type: "button", className: "btn small", textContent: "Fetch" });
      const fetchBlock = el("button", { type: "button", className: "btn small", textContent: "Fetch" });
      allow = makeListBox("Server UUID allowlist", filters.server_uuid_whitelist, "Add server UUID...", [fetchAllow], "sc-help-watch-server-uuid-allow");
      block = makeListBox("Server UUID blacklist", filters.server_uuid_blacklist, "Add server UUID...", [fetchBlock], "sc-help-watch-server-uuid-block");
      grid.append(allow.box, block.box);
      const fetchUuidInto = async (target) => {
        const box = target === "block" ? block : allow;
        try {
          const result = await j(`/api/plex/server_uuid?instance=${encodeURIComponent(inst)}`);
          const value = String(result?.server_uuid || result?.uuid || result?.id || "").trim();
          if (!value) {
            box.note.textContent = "No server UUID";
            return;
          }
          if (box.state.values.includes(value)) {
            box.note.textContent = `${value} already added`;
            return;
          }
          box.state.values.push(value);
          box.redraw();
          box.note.textContent = `Fetched ${value}`;
        } catch {
          box.note.textContent = "Fetch failed";
        }
      };
      on(fetchAllow, "click", () => fetchUuidInto("allow"));
      on(fetchBlock, "click", () => fetchUuidInto("block"));
    }
    on(pickUser, "click", async (e) => {
      e.preventDefault();
      await openUserPicker("watch", pickUser, {
        provider: key,
        instance: inst,
        title: key === "plex" ? "Pick Plex user" : key === "emby" ? "Pick Emby user" : "Pick Jellyfin user",
        onPick: (u) => {
          const name = String(u?.name || "").trim();
          if (!name) return;
          if (users.state.values.includes(name)) {
            users.note.textContent = `${name} already added`;
            return;
          }
          users.state.values.push(name);
          users.redraw();
          users.note.textContent = `Picked ${name}`;
        },
      });
    });
    card.appendChild(grid);
    const foot = el("div", { className: "sc-profile-filter-foot" });
    const cancel = el("button", { type: "button", className: "btn small", textContent: "Cancel" });
    const save = el("button", { type: "button", className: "btn small", textContent: "Save" });
    foot.append(cancel, save);
    card.appendChild(foot);
    const closeModal = () => overlay.remove();
    on(close, "click", closeModal);
    on(cancel, "click", closeModal);
    on(overlay, "click", (e) => {
      if (e.target === overlay) closeModal();
    });
    on(save, "click", () => {
      const next = key === "plex"
        ? {
            username_whitelist: users.state.values,
            server_uuid: allow?.state.values?.[0] || "",
            server_uuid_whitelist: allow?.state.values || [],
            server_uuid_blacklist: block?.state.values || [],
          }
        : { username_whitelist: users.state.values };
      const path = webhookProfileFilterPath(key, inst);
      write(path, next);
      persistConfigPaths([[path, next]], "sc-endpoint-note");
      closeModal();
    });
    bindHelpTips(card);
    d.body.appendChild(overlay);
  }

  async function regenWebhookIds(payload, btn = null) {
    if (btn) btn.disabled = true;
    try {
      const init = payload ? { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) } : { method: "POST" };
      const r = await j("/api/webhooks/regenerate", init);
      if (r && r.ok && r.ids) {
        STATE.webhookIds = r.ids || null;
        STATE.routeWebhookHooks = Array.isArray(r.route_hooks) ? r.route_hooks : [];
        STATE.profileWebhookHooks = Array.isArray(r.profile_hooks) ? r.profile_hooks : [];
        try { await refreshWebhookIds(); } catch {}
        mergeRouteWebhookHooksIntoState();
        applyWebhookUrls();
        try { updatePlexWatcherWebhookUrl(); } catch {}
        try { renderRoutesUi(); } catch {}
      }
    } finally {
      if (btn) btn.disabled = false;
    }
  }
  STATE.openWebhookProfileFilters = openWebhookProfileFilters;
  STATE.regenWebhookIds = regenWebhookIds;
  STATE.applyWebhookUrls = applyWebhookUrls;
  refreshWebhookIds()
    .then(() => {
      applyWebhookUrls();
      try { updatePlexWatcherWebhookUrl(); } catch {}
    })
    .catch(() => {
      applyWebhookUrls();
      try { updatePlexWatcherWebhookUrl(); } catch {}
    });

  const autostart = !!read("scrobble.watch.autostart", false);
  const auto = $("#sc-autostart", STATE.mount);
  if (auto) auto.checked = !!autostart;

  try { syncServerPreviewUi(); } catch {}
  try { syncWatcherDefaultsNote(); } catch {}
  try { syncWatcherRouteWarnings(); } catch {}

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
  set("#sc-watched-at", read("scrobble.trakt.watched_at", DEFAULTS.trakt.watched_at));
  set("#sc-force-stop", read("scrobble.trakt.force_stop_at", DEFAULTS.trakt.force_stop_at));
  set("#sc-regress", read("scrobble.trakt.regress_tolerance_percent", DEFAULTS.trakt.regress_tolerance_percent));
  set("#sc-watched-at-webhook", read("scrobble.trakt.watched_at", DEFAULTS.trakt.watched_at));
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
  if (whRat) whRat.checked = webhookPlexRatingsEnabled();
  syncWebhookSinkPills(STATE.mount);

  rebuildPlexRatingsDropdown();

  updatePlexWatcherWebhookUrl();

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

  async function reloadWatcherRuntime(btn = null) {
    setNote("sc-note", "");
    setNote("sc-pms-note", "");
    setWatcherButtonState(btn || $("#sc-watch-refresh", STATE.mount), "busy", "Reloading", "sync");
    try {
      await persistCurrentScrobblerState(null, { throwOnError: true });
      await API.watch.refresh();
      await refreshWatcher();
      try { w.refreshWatchLogs?.(); } catch {}
      setWatcherButtonState(btn || $("#sc-watch-refresh", STATE.mount), "ok", "Reloaded", "check");
    } catch {
      setWatcherButtonState(btn || $("#sc-watch-refresh", STATE.mount), "err", "Failed", "error");
      await refreshWatcher();
    }
  }

  async function onWatchStart(btn = null) {
  const actionBtn = btn || $("#sc-watch-start", STATE.mount);
  const routes = getRoutes();
  setNote("sc-note", "");
  setNote("sc-pms-note", "");
  if (!routes.length) {
    setWatcherButtonState(actionBtn, "err", "No route", "error");
    return;
  }
  if (!anyStartableRoute()) {
    setWatcherButtonState(actionBtn, "err", "Not ready", "error");
    return;
  }
  const current = getActiveRoute() || routes[0] || null;
  if (current?.id) {
    setActiveRouteFromUi(current.id);
  }

  setWatcherButtonState(actionBtn, "busy", "Starting", "sync");

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
    setWatcherButtonState(actionBtn, "err", "Save failed", "error");
    return;
  }

  try {
    await API.watch.start(null, null);
    setWatcherButtonState(actionBtn, "ok", "Started", "check");
  } catch {
    setWatcherButtonState(actionBtn, "err", "Failed", "error");
  }

  refreshWatcher();
}


  async function onWatchStop(btn = null) {
    const actionBtn = btn || $("#sc-watch-stop", STATE.mount);
    setNote("sc-note", "");
    setNote("sc-pms-note", "");
    setWatcherButtonState(actionBtn, "busy", "Stopping", "sync");
    setWatcherStatus({ alive: false });
    try {
      await API.watch.stop();
      setWatcherButtonState(actionBtn, "ok", "Stopped", "check");
    } catch {
      setWatcherButtonState(actionBtn, "err", "Failed", "error");
    }
    refreshWatcher();
  }


  function redrawWhitelist(hostSel, path, removeFn, onClick) {
    const host = $(hostSel, STATE.mount);
    if (!host) return;
    host.innerHTML = "";
    uniqStrings(read(path, [])).forEach((v) => host.append(chip(v, removeFn, onClick)));
  }
  function addToWhitelist(hostSel, path, name, removeFn, onClick) {
    const clean = String(name || "").trim();
    if (!clean) return false;
    const cur = uniqStrings(read(path, []));
    if (cur.includes(clean)) return false;
    write(path, [...cur, clean]);
    const host = $(hostSel, STATE.mount);
    if (host) host.append(chip(clean, removeFn, onClick));
    return true;
  }
  function addWhitelistInput(inputSel, hostSel, path, removeFn, onClick) {
    const inp = $(inputSel, STATE.mount);
    const v = String(inp?.value || "").trim();
    if (!v || !addToWhitelist(hostSel, path, v, removeFn, onClick)) return false;
    if (inp) inp.value = "";
    return true;
  }
  function removeWhitelistItem(value, hostSel, path, removeFn, onClick) {
    write(path, asArray(read(path, [])).filter((x) => String(x) !== String(value)));
    redrawWhitelist(hostSel, path, removeFn, onClick);
  }
  async function fetchServerUUIDToWebhookList({ hostSel, path, removeFn, noteId, targetName, syncAllowLegacy = false }) {
    try {
      const x = await j("/api/plex/server_uuid");
      const v = String(x?.server_uuid || x?.uuid || x?.id || "").trim();
      if (!v) return setNote(noteId, "No server UUID", "err");
      const added = addToWhitelist(hostSel, path, v, removeFn);
      if (syncAllowLegacy) {
        const allow = webhookServerUuidAllowlist();
        write("scrobble.webhook.filters_plex.server_uuid", allow[0] || "");
      }
      setNote(noteId, added ? `Server UUID added to ${targetName}` : `Server UUID already in ${targetName}`);
    } catch {
      setNote(noteId, "Fetch failed", "err");
    }
  }

  const fetchServerUUIDWebhookAllow = () =>
    fetchServerUUIDToWebhookList({
      hostSel: "#sc-server-uuid-allow-webhook",
      path: "scrobble.webhook.filters_plex.server_uuid_whitelist",
      removeFn: removeWebhookServerUuidAllow,
      noteId: "sc-uuid-note-webhook",
      targetName: "allowlist",
      syncAllowLegacy: true,
    });

  const fetchServerUUIDWebhookBlock = () =>
    fetchServerUUIDToWebhookList({
      hostSel: "#sc-server-uuid-block-webhook",
      path: "scrobble.webhook.filters_plex.server_uuid_blacklist",
      removeFn: removeWebhookServerUuidBlock,
      noteId: "sc-uuid-block-note-webhook",
      targetName: "blacklist",
    });

  function onAddUserWebhook() {
    addWhitelistInput("#sc-user-input-webhook", "#sc-whitelist-webhook", "scrobble.webhook.filters_plex.username_whitelist", removeUserWebhook);
  }

  function removeUserWebhook(u) {
    removeWhitelistItem(u, "#sc-whitelist-webhook", "scrobble.webhook.filters_plex.username_whitelist", removeUserWebhook);
  }

  function onAddWebhookServerUuidAllow() {
    const added = addWhitelistInput(
      "#sc-server-uuid-allow-input-webhook",
      "#sc-server-uuid-allow-webhook",
      "scrobble.webhook.filters_plex.server_uuid_whitelist",
      removeWebhookServerUuidAllow
    );
    if (added) {
      const allow = webhookServerUuidAllowlist();
      write("scrobble.webhook.filters_plex.server_uuid", allow[0] || "");
    }
  }

  function onAddWebhookServerUuidBlock() {
    addWhitelistInput(
      "#sc-server-uuid-block-input-webhook",
      "#sc-server-uuid-block-webhook",
      "scrobble.webhook.filters_plex.server_uuid_blacklist",
      removeWebhookServerUuidBlock
    );
  }

  function removeWebhookServerUuidAllow(uuid) {
    const allow = webhookServerUuidAllowlist().filter((x) => String(x) !== String(uuid));
    write("scrobble.webhook.filters_plex.server_uuid_whitelist", allow);
    write("scrobble.webhook.filters_plex.server_uuid", allow[0] || "");
    const host = $("#sc-server-uuid-allow-webhook", STATE.mount);
    if (host) {
      host.innerHTML = "";
      allow.forEach((value) => host.append(chip(value, removeWebhookServerUuidAllow)));
    }
  }

  function removeWebhookServerUuidBlock(uuid) {
    removeWhitelistItem(
      uuid,
      "#sc-server-uuid-block-webhook",
      "scrobble.webhook.filters_plex.server_uuid_blacklist",
      removeWebhookServerUuidBlock
    );
  }

  async function hydrateEmby() {
    try {
      STATE._embyInspect = await j("/api/emby/inspect");
      if (provider() === "emby") syncServerPreviewUi();
    } catch {}
  }
async function hydrateJellyfin() {
  try {
    STATE._jellyfinInspect = await j("/api/jellyfin/inspect");
    if (provider() === "jellyfin") syncServerPreviewUi();
  } catch {}
}


  function wire() {
    ensureHiddenServerInputs();
    refreshHybridWarning();
    // Copy the displayed webhook URL
    function getCodeText(id) {
      const n = $(id, STATE.mount);
      return n && n.textContent ? String(n.textContent).trim() : "";
    }

    async function copyWebhookFromCode(codeId, noteId, successMsg, fallbackUrl, btn = null) {
      if (!STATE.webhookIds) {
        try { await refreshWebhookIds(); } catch {}
        try { STATE.applyWebhookUrls?.(); } catch {}
        try { updatePlexWatcherWebhookUrl(); } catch {}
      }

      const url = getCodeText(codeId) || fallbackUrl;
      const ok = await copyText(url);
      if (btn) {
        const old = btn.textContent;
        const iconMode = btn.classList?.contains("material-symbols-rounded");
        btn.textContent = ok ? (iconMode ? "check" : "Copied") : (iconMode ? "error" : "Failed");
        setTimeout(() => { btn.textContent = old; }, 1200);
      }
      if (noteId) setNote(noteId, ok ? successMsg : "Copy failed", ok ? "ok" : "err");
    }

    on($("#sc-copy-plex", STATE.mount), "click", async (e) => {
      await copyWebhookFromCode("#sc-webhook-url-plex", "sc-endpoint-note", "Plex endpoint copied", `${location.origin}/webhook/plex`, e.currentTarget);
    });
    on($("#sc-copy-jf", STATE.mount), "click", async (e) => {
      await copyWebhookFromCode("#sc-webhook-url-jf", "sc-endpoint-note", "Jellyfin endpoint copied", `${location.origin}/webhook/jellyfin`, e.currentTarget);
    });
    on($("#sc-copy-emby", STATE.mount), "click", async (e) => {
      await copyWebhookFromCode("#sc-webhook-url-emby", "sc-endpoint-note", "Emby endpoint copied", `${location.origin}/webhook/emby`, e.currentTarget);
    });

    on($("#sc-copy-plexwatcher", STATE.mount), "click", async (e) => {
      setNote("sc-plexwatcher-note", "");
      if (!STATE.webhookIds?.plexwatcher) {
        try { await refreshWebhookIds(); } catch {}
        try { updatePlexWatcherWebhookUrl(); } catch {}
      }
      const url = `${location.origin}/webhook/plexwatcher?${STATE.webhookIds?.plexwatcher || ""}`;
      if (!url) return;
      await copyWebhookFromCode("#sc-plexwatcher-url", "", "", url, e.currentTarget);
    });
    on($("#sc-regenerate-plexwatcher", STATE.mount), "click", async (e) => {
      const btn = e.currentTarget;
      const prev = btn.textContent;
      setNote("sc-plexwatcher-note", "");
      if (!confirm("Regenerate the global Plex ratings webhook URL?\n\nOnly this URL will change.")) return;
      try {
        if (typeof STATE.regenWebhookIds !== "function") throw new Error("regenerate_not_ready");
        btn.textContent = "sync";
        await STATE.regenWebhookIds({ key: "plexwatcher" }, btn);
        try { updatePlexWatcherWebhookUrl(); } catch {}
        btn.textContent = "check";
      } catch {
        btn.textContent = "error";
      } finally {
        setTimeout(() => { btn.textContent = prev; }, 1200);
      }
    });
    on($("#sc-copy-pms", STATE.mount), "click", async (e) => {
      const btn = e.currentTarget;
      const inp = $("#sc-pms-input", STATE.mount);
      const url = String(inp?.value || "").trim();
      if (!url) return;
      const prev = btn.textContent;
      btn.textContent = (await copyText(url)) ? "done" : "error";
      setTimeout(() => { btn.textContent = prev; }, 900);
    });

    on($("#sc-watch-start", STATE.mount), "click", (e) => onWatchStart(e.currentTarget));
    on($("#sc-watch-stop", STATE.mount), "click", (e) => onWatchStop(e.currentTarget));
    on($("#sc-watch-refresh", STATE.mount), "click", (e) => {
      reloadWatcherRuntime(e.currentTarget);
    });

    // Routes UI
    on($("#sc-route-add", STATE.mount), "click", async (e) => {
      e.preventDefault();
      if (!isRoutesMode()) return;
      const routes = getRoutes().map((r, i) => normalizeRoute(r, `R${i + 1}`));
      const id = nextRouteId();
      const nr = normalizeRoute({ id, enabled: true, provider: "", provider_instance: "default", sink: "", sink_instance: "default", filters: {} }, id);
      routes.push(nr);
      setRoutes(routes);
      setActiveRouteFromUi(id);
      await renderRoutesUi();
      populate();
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
      if (f === "provider") {
        r.provider_instance = "default";
        const names = asArray(r.filters?.username_whitelist || []);
        r.filters = names.length ? { username_whitelist: names } : {};
      }
      if (f === "sink") { r.sink_instance = "default"; }
      setRoutes(routes);
      try {
        const dd = findDuplicateRouteKeys(getRoutes());
        if (!dd.length) clearStickyNote("sc-note");
      } catch {}
      setActiveRouteFromUi(rid);
      populate();
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
        setActiveRouteFromUi(rid);
        await openRouteFiltersModal(rid);
        return;
      }
      if (act === "options") {
        setActiveRouteFromUi(rid);
        await openRouteOptionsModal(rid);
      }
    });
    on($("#sc-add-user-webhook", STATE.mount), "click", onAddUserWebhook);
    on($("#sc-load-users-webhook", STATE.mount), "click", (e) => {
      e.preventDefault();
      openUserPicker("webhook", e.currentTarget);
    });
    on($("#sc-add-server-uuid-allow-webhook", STATE.mount), "click", onAddWebhookServerUuidAllow);
    on($("#sc-add-server-uuid-block-webhook", STATE.mount), "click", onAddWebhookServerUuidBlock);
    on($("#sc-server-uuid-allow-input-webhook", STATE.mount), "keydown", (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        onAddWebhookServerUuidAllow();
      }
    });
    on($("#sc-server-uuid-block-input-webhook", STATE.mount), "keydown", (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        onAddWebhookServerUuidBlock();
      }
    });
    on($("#sc-fetch-uuid-allow-webhook", STATE.mount), "click", fetchServerUUIDWebhookAllow);
    on($("#sc-fetch-uuid-webhook", STATE.mount), "click", fetchServerUUIDWebhookBlock);

    bindPercentInput("#sc-pause-debounce", "scrobble.watch.pause_debounce_seconds", DEFAULTS.watch.pause_debounce_seconds);
    bindPercentInput("#sc-suppress-start", "scrobble.watch.suppress_start_at", DEFAULTS.watch.suppress_start_at);
    bindPercentInput("#sc-pause-debounce-webhook", "scrobble.webhook.pause_debounce_seconds", DEFAULTS.watch.pause_debounce_seconds);
    bindPercentInput("#sc-suppress-start-webhook", "scrobble.webhook.suppress_start_at", DEFAULTS.watch.suppress_start_at);

    bindPercentInput("#sc-watched-at", "scrobble.trakt.watched_at", DEFAULTS.trakt.watched_at);
    bindPercentInput("#sc-force-stop", "scrobble.trakt.force_stop_at", DEFAULTS.trakt.force_stop_at);
    bindPercentInput("#sc-regress", "scrobble.trakt.regress_tolerance_percent", DEFAULTS.trakt.regress_tolerance_percent);

    bindPercentInput("#sc-watched-at-webhook", "scrobble.trakt.watched_at", DEFAULTS.trakt.watched_at);
    bindPercentInput("#sc-force-stop-webhook", "scrobble.trakt.force_stop_at", DEFAULTS.trakt.force_stop_at);
    bindPercentInput("#sc-regress-webhook", "scrobble.trakt.regress_tolerance_percent", DEFAULTS.trakt.regress_tolerance_percent);
    bindRangeInput("#sc-progress-step", "scrobble.trakt.progress_step", DEFAULTS.trakt.progress_step, 1, 25);

    const wh = $("#sc-enable-webhook", STATE.mount);
    const wa = $("#sc-enable-watcher", STATE.mount);

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
          mute = false;
          master.dispatchEvent(new Event("change", { bubbles: true }));
        });
      });
      on(master, "change", sync);
      sync();
    };

    mirrorToggle("#sc-enable-webhook", ["#sc-enable-webhook-jf", "#sc-enable-webhook-emby", "#sc-enable-webhook-adv"]);
    mirrorToggle("#sc-delete-plex-webhook", ["#sc-delete-plex-webhook-jf", "#sc-delete-plex-webhook-emby"]);

    on(STATE.webhookHost, "change", (e) => {
      const toggle = e.target?.closest?.("[data-webhook-profile-toggle]");
      if (!toggle) return;
      const providerKey = String(toggle.getAttribute("data-webhook-profile-toggle") || "").trim().toLowerCase();
      const host = $(`[data-webhook-profile-list="${providerKey}"]`, STATE.webhookHost);
      if (host) host.classList.toggle("is-open", !!toggle.checked);
    });

    on(STATE.webhookHost, "change", (e) => {
      const input = e.target?.closest?.("[data-webhook-enable]");
      if (!input) return;
      const providerKey = String(input.getAttribute("data-webhook-enable") || "").trim().toLowerCase();
      const inst = String(input.getAttribute("data-webhook-enable-instance") || "default").trim() || "default";
      if (!providerKey) return;
      const path = webhookProfileEnabledPath(providerKey, inst);
      const value = !!input.checked;
      write(path, value);
      const name = inst === "default" ? providerLabel(providerKey) || providerKey : inst;
      persistConfigPaths([[path, value]], "sc-endpoint-note");
      setNote("sc-endpoint-note", `${name} webhook ${value ? "enabled" : "disabled"}`, "ok");
    });

    on(STATE.webhookHost, "click", (e) => {
      const btn = e.target?.closest?.("button[data-sink]");
      const bar = btn?.closest?.("[data-webhook-sinks-provider]");
      if (!btn || !bar) return;
      e.preventDefault();
      const providerKey = String(bar.getAttribute("data-webhook-sinks-provider") || "").trim().toLowerCase();
      const key = String(btn.getAttribute("data-sink") || "").trim().toLowerCase();
      const availability = webhookSinkAvailability(providerKey);
      if (!key || availability[key] !== true) return;
      const cur = sinkCsvItems(availableSinkCsv(webhookSinkCsv(providerKey), availability));
      const next = cur.includes(key) ? cur.filter((x) => x !== key) : [...cur, key];
      const csv = normSinkCsv(next.join(","));
      const path = webhookSinkPath(providerKey);
      write(path, csv);
      syncWebhookSinkPills(STATE.webhookHost);
      const pairs = [[path, csv]];
      const ratingsToggle = $("#sc-webhook-plex-ratings", STATE.mount);
      if (providerKey === "plex" && ratingsToggle?.checked) {
        pairs.push(...setWebhookPlexRatings(true));
      }
      persistConfigPaths(pairs, "sc-endpoint-note");
    });

    on(STATE.webhookHost, "click", async (e) => {
      const copyBtn = e.target?.closest?.("[data-copy-webhook-url]");
      if (!copyBtn) return;
      e.preventDefault();
      const url = String(copyBtn.getAttribute("data-copy-webhook-url") || "").trim();
      const ok = await copyText(url);
      const old = copyBtn.textContent;
      const iconMode = copyBtn.classList?.contains("material-symbols-rounded");
      copyBtn.textContent = ok ? (iconMode ? "check" : "Copied") : (iconMode ? "error" : "Failed");
      setTimeout(() => { copyBtn.textContent = old; }, 1200);
      setNote("sc-endpoint-note", ok ? "Profile endpoint copied" : "Copy failed", ok ? "ok" : "err");
    });

    on(STATE.webhookHost, "click", (e) => {
      const btn = e.target?.closest?.("[data-webhook-profile-filter]");
      if (!btn) return;
      e.preventDefault();
      if (typeof STATE.openWebhookProfileFilters !== "function") {
        setNote("sc-endpoint-note", "Profile filters are still loading", "warn");
        return;
      }
      STATE.openWebhookProfileFilters(
        btn.getAttribute("data-webhook-profile-filter"),
        btn.getAttribute("data-webhook-profile-instance")
      );
    });

    on(STATE.webhookHost, "click", async (e) => {
      const btn = e.target?.closest?.("[data-webhook-regen-key],[data-webhook-regen-provider]");
      if (!btn) return;
      e.preventDefault();
      const key = String(btn.getAttribute("data-webhook-regen-key") || "").trim();
      const providerKey = String(btn.getAttribute("data-webhook-regen-provider") || "").trim();
      const instance = String(btn.getAttribute("data-webhook-regen-instance") || "").trim();
      const label = String(btn.getAttribute("data-webhook-regen-label") || "").trim() || (key || `${providerKey}:${instance || "default"}`);
      if (!confirm(`Regenerate this webhook URL?\n\n${label}\n\nOnly this URL will change.`)) return;
      try {
        if (typeof STATE.regenWebhookIds !== "function") throw new Error("regenerate_not_ready");
        await STATE.regenWebhookIds(key ? { key } : { provider: providerKey, instance }, btn);
        setNote("sc-endpoint-note", "Webhook URL regenerated");
      } catch (err) {
        console.error(err);
        setNote("sc-endpoint-note", "Regenerate failed", "err");
      }
    });

    const persistGlobalPlexRatings = async (pairs) => {
      try {
        await persistConfigPaths(pairs || globalPlexRatingsPairs(globalPlexRatingsTargets()), "sc-pms-note");
        await refreshWebhookIds();
      } catch {}
      try { syncPlexRatingsPillsFromSelect(); } catch {}
      try { updatePlexWatcherWebhookUrl(); } catch {}
    };

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
        const availability = globalPlexRatingsAvailability();
        if (availability[key] === false) return;
        const curArr = sinkCsvItems(globalPlexRatingsTargets());
        const has = curArr.includes(key);
        const nextArr = has ? curArr.filter((x) => x !== key) : [...curArr, key];
        const nextCsv = normSinkCsv(nextArr.join(","));
        const nextSel = nextCsv ? nextCsv : "none";
        const pairs = setGlobalPlexRatingsCsv(nextCsv);
        if (ratSel.value !== nextSel) {
          ratSel.value = nextSel;
          ratSel.dispatchEvent(new Event("change", { bubbles: true }));
        } else {
          syncPillBar(ratPills, nextCsv, availability);
          persistGlobalPlexRatings(pairs);
        }
      });
    }

    const syncSources = async (src) => {
      const webOn = !!wh?.checked;
      const watOn = !!wa?.checked;
      const sources = writeScrobbleSources(webOn, watOn);

      if (src === "watch" && !watOn) {
        write("scrobble.watch.autostart", false);
        const auto = $("#sc-autostart", STATE.mount);
        if (auto) auto.checked = false;
      }
      applyModeDisable();
      refreshHybridWarning();

      const enabled = sources.webhook || sources.watcher;
      const mode = scrobbleLegacyMode(sources);
      const pairs = [
        ["scrobble.enabled", enabled],
        ["scrobble.sources.webhook", sources.webhook],
        ["scrobble.sources.watcher", sources.watcher],
        ["scrobble.mode", mode],
      ];
      if (src === "watch" && !watOn) pairs.push(["scrobble.watch.autostart", false]);
      const noteId = src === "watch" ? "sc-pms-note" : "sc-endpoint-note";
      if (src === "watch" && !watOn) STATE.ui.watchAutostart = false;
      await persistConfigPaths(pairs, noteId);
    };

    if (wh) on(wh, "change", () => syncSources("webhook"));
    if (wa) on(wa, "change", () => syncSources("watch"));

    on($("#sc-autostart", STATE.mount), "change", (e) => {
      const v = !!e.target.checked;
      write("scrobble.watch.autostart", v);
      STATE.ui.watchAutostart = v;
      persistConfigPaths([["scrobble.watch.autostart", v]], "sc-pms-note");
    });

    const ratingsSel = $("#sc-plex-ratings", STATE.mount);
    if (ratingsSel) {
      on(ratingsSel, "change", (e) => {
        ratingsSel.dataset.cxUserChanged = "1";
        const selected = csvFromSelect(e.target, true);
        const pairs = setGlobalPlexRatingsCsv(selected);
        persistGlobalPlexRatings(pairs);
      });
    }

    const whRatings = $("#sc-webhook-plex-ratings", STATE.mount);
    if (whRatings) {
      on(whRatings, "change", (e) => {
        const v = !!e.target.checked;
        persistConfigPaths(setWebhookPlexRatings(v), "sc-endpoint-note");
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

  const sources = scrobbleSourceState();
  const enabled = !!(sources.webhook || sources.watcher);
  const mode = scrobbleLegacyMode(sources);

  const wlWebHost = $("#sc-whitelist-webhook", STATE.mount);
  const wlWeb = wlWebHost ? namesFromChips("#sc-whitelist-webhook") : asArray(read("scrobble.webhook.filters_plex.username_whitelist", []));
  const suAllowHost = $("#sc-server-uuid-allow-webhook", STATE.mount);
  const suAllow = uniqStrings(suAllowHost ? namesFromChips("#sc-server-uuid-allow-webhook") : webhookServerUuidAllowlist());
  const suBlockHost = $("#sc-server-uuid-block-webhook", STATE.mount);
  const suBlock = uniqStrings(suBlockHost ? namesFromChips("#sc-server-uuid-block-webhook") : webhookServerUuidBlacklist());

  const routesRaw = getRoutes().map((r, i) => normalizeRoute(r, `R${i + 1}`));
  const routesOut = routesRaw.filter((r) => {
    const p = String(r?.provider || "").trim();
    const s = String(r?.sink || "").trim();
    return !!(p && s);
  });
  const dropped = routesRaw.length - routesOut.length;
  if (dropped > 0) {
    const msg = `Ignored ${dropped} incomplete route${dropped === 1 ? "" : "s"} (pick Provider + Sink).`;
    if (!(STICKY_NOTES["sc-note"] && STICKY_NOTES["sc-note"].kind === "err")) setNote("sc-note", msg, "warn");
  }

  const dups = findDuplicateRouteKeys(routesOut);
  if (dups.length) {
    const msg = "Duplicate routes are not allowed. Fix these before saving: " + dups.map(d => d.key).join(" | ");
    setStickyNote("sc-note", msg, "err");
    throw new Error(msg);
  }

  setRoutes(routesRaw);

  return {
    enabled,
    mode: mode === "watch" ? "watch" : "webhook",
    sources: {
      webhook: !!sources.webhook,
      watcher: !!sources.watcher,
    },
    delete_plex: !!read("scrobble.delete_plex", false),
    delete_plex_types: read("scrobble.delete_plex_types", ["movie"]),

    webhook: {
      pause_debounce_seconds: read("scrobble.webhook.pause_debounce_seconds", DEFAULTS.watch.pause_debounce_seconds),
      suppress_start_at: read("scrobble.webhook.suppress_start_at", DEFAULTS.watch.suppress_start_at),
      plex_trakt_ratings: !!read("scrobble.webhook.plex_trakt_ratings", false),
      plex_simkl_ratings: !!read("scrobble.webhook.plex_simkl_ratings", false),
      plex_mdblist_ratings: !!read("scrobble.webhook.plex_mdblist_ratings", false),
      sinks: webhookSinkCsv(""),
      providers: read("scrobble.webhook.providers", {}) || {},
      profiles: read("scrobble.webhook.profiles", {}) || {},
      filters_plex: {
        username_whitelist: wlWeb,
        server_uuid: suAllow[0] || "",
        server_uuid_whitelist: suAllow,
        server_uuid_blacklist: suBlock,
      },
      filters_jellyfin: read("scrobble.webhook.filters_jellyfin", {}) || { username_whitelist: [] },
    },

    watch: {
      routes: routesOut,
      autostart: !!read("scrobble.watch.autostart", false),
      plex_simkl_ratings: !!read("scrobble.watch.plex_simkl_ratings", false),
      plex_trakt_ratings: !!read("scrobble.watch.plex_trakt_ratings", false),
      plex_mdblist_ratings: !!read("scrobble.watch.plex_mdblist_ratings", false),
      pause_debounce_seconds: read("scrobble.watch.pause_debounce_seconds", DEFAULTS.watch.pause_debounce_seconds),
      suppress_start_at: read("scrobble.watch.suppress_start_at", DEFAULTS.watch.suppress_start_at),
    },

    trakt: {
      progress_step: read("scrobble.trakt.progress_step", DEFAULTS.trakt.progress_step),
      stop_pause_threshold: read("scrobble.trakt.stop_pause_threshold", DEFAULTS.trakt.stop_pause_threshold),
      watched_at: read("scrobble.trakt.watched_at", DEFAULTS.trakt.watched_at),
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
        sec.innerHTML = `<div class="head" data-toggle-section="${id}"><span class="chev">></span><strong>${title}</strong></div><div class="body"><div id="${id === "sc-sec-webhook" ? "scrob-webhook" : "scrob-watcher"}"></div></div>`;
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
            try { populate(); } catch {}
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
    try { ensureWatchRoutesArray(); } catch {}
    populate();
    await refreshWatcher();
    if (provider() === "emby") await hydrateEmby();
    if (provider() === "jellyfin") await hydrateJellyfin();
  }

  async function refresh() {
    if (!STATE.mount) return;
    try { await refreshCfgBeforePopulate(); } catch {}
    try { ensureWatchRoutesArray(); } catch {}
    try { populate(); } catch {}
    try { if (isRoutesMode()) await renderRoutesUi(); } catch {}
    try { await refreshWatcher(); } catch {}
    try { applyModeDisable(); } catch {}
  }

  function mountLegacy(targetEl, cfg) {
    return init({ mountId: targetEl?.id, cfg: cfg || (w._cfgCache || {}) });
  }

  w.ScrobUI = { $, $all, el, on, setNote, injectStyles, DEFAULTS, STATE, read, write, asArray, clamp100, norm100, API };
  w.Scrobbler = { init, mount: mountLegacy, refresh, getConfig: getScrobbleConfig, getRootPatch };
  w.getScrobbleConfig = getScrobbleConfig;
  w.getRootPatch = getRootPatch;

  d.addEventListener("DOMContentLoaded", () => {
    const root = d.getElementById("scrobble-mount");
    if (!root) return;
    init({ mountId: "scrobble-mount" });
  });
})(window, document);
