/* assets/js/scheduler.js */
/* CrossWatch - Advanced Scheduling UI */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(() => {
  "use strict";
  if (window.__SCHED_UI_INIT__) return; window.__SCHED_UI_INIT__ = true;
  const authSetupPending = () => window.cwIsAuthSetupPending?.() === true;

  // tiny helpers
  const $ = (s, r = document) => r.querySelector(s);
  const el = (t, c) => Object.assign(document.createElement(t), c ? { className: c } : {});

  const fieldKey = (value, fallback = "field") => String(value || fallback).replace(/[^a-z0-9_-]+/gi, "_");

  // stable id (UUID when possible)
  const genId = (() => {
    const withCrypto = () => {
      try {
        const b = new Uint8Array(16); crypto.getRandomValues(b);
        b[6] = (b[6] & 0x0f) | 0x40; b[8] = (b[8] & 0x3f) | 0x80;
        const h = [...b].map(x => x.toString(16).padStart(2, "0"));
        return `${h.slice(0,4).join("")}-${h.slice(4,6).join("")}-${h.slice(6,8).join("")}-${h.slice(8,10).join("")}-${h.slice(10).join("")}`;
      } catch { return null; }
    };
    return () => crypto?.randomUUID?.() || withCrypto() || `id_${Date.now().toString(36)}_${Math.random().toString(36).slice(2,10)}`;
  })();

  // styles (once)
  document.head.appendChild(Object.assign(el("style"), { id: "sch-css", textContent: `
#sec-scheduling{--sch-shell-bg:radial-gradient(120% 140% at 0% 0%,rgba(90,74,201,.11) 0%,rgba(90,74,201,0) 42%),radial-gradient(110% 130% at 100% 100%,rgba(41,110,214,.08) 0%,rgba(41,110,214,0) 44%),linear-gradient(180deg,rgba(8,11,18,.985),rgba(3,5,10,.99));--sch-card-bg:linear-gradient(180deg,rgba(10,13,21,.92),rgba(4,6,11,.96));--sch-card-bg-soft:linear-gradient(180deg,rgba(14,18,30,.84),rgba(5,7,13,.9));--sch-border:rgba(255,255,255,.08);--sch-border-soft:rgba(255,255,255,.055);--sch-shadow:0 22px 56px rgba(0,0,0,.4),inset 0 1px 0 rgba(255,255,255,.03);--sch-fg:#f3f7ff;--sch-fg-soft:rgba(208,217,233,.7)}
#sec-scheduling .cw-subpanel[data-sub]{padding-top:6px}
#sec-scheduling .cw-subpanel[data-sub="basic"] .auth-card,#schAdv{position:relative;border:1px solid var(--sch-border);border-radius:22px;background:var(--sch-shell-bg);box-shadow:var(--sch-shadow);overflow:hidden}
#sec-scheduling .cw-subpanel[data-sub="basic"] .auth-card::before,#schAdv::before{content:"";position:absolute;inset:0;pointer-events:none;background:linear-gradient(135deg,rgba(255,255,255,.045),transparent 38%),radial-gradient(90% 120% at 0% 0%,rgba(92,76,204,.1),transparent 52%)}
#sec-scheduling .cw-subpanel[data-sub="basic"] .auth-card-fields{position:relative;z-index:1;display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px;padding:16px}
#sec-scheduling .cw-subpanel[data-sub="basic"] .field{display:grid;gap:8px;padding:14px 16px;border:1px solid var(--sch-border-soft);border-radius:18px;background:var(--sch-card-bg);box-shadow:inset 0 1px 0 rgba(255,255,255,.025)}
#sec-scheduling .cw-subpanel[data-sub="basic"] .field:first-child{grid-column:1 / -1}
#sec-scheduling .cw-subpanel[data-sub="basic"] .field>.muted{margin:0!important;font-size:11px;font-weight:800;letter-spacing:.14em;text-transform:uppercase;color:rgba(214,223,238,.58)}
#sec-scheduling .cw-subpanel[data-sub="basic"] .auth-card-notes{margin-top:0;color:var(--sch-fg-soft);font-size:12px;line-height:1.45}
#sec-scheduling .cw-subpanel[data-sub="basic"] select,#sec-scheduling .cw-subpanel[data-sub="basic"] input[type=time],#sec-scheduling .cw-subpanel[data-sub="basic"] input[type=number],.sch-adv select,.sch-adv input[type=time],.sch-adv input[type=number],.sch-adv input[type=text]{width:100%;min-height:46px;padding:0 14px;border:1px solid rgba(255,255,255,.08);border-radius:16px;background:linear-gradient(180deg,rgba(4,6,11,.94),rgba(2,4,8,.98));color:var(--sch-fg);box-shadow:inset 0 1px 0 rgba(255,255,255,.02);transition:border-color .18s ease,background .18s ease,box-shadow .18s ease,transform .18s ease}
#sec-scheduling .cw-subpanel[data-sub="basic"] select:hover,#sec-scheduling .cw-subpanel[data-sub="basic"] input[type=time]:hover,#sec-scheduling .cw-subpanel[data-sub="basic"] input[type=number]:hover,.sch-adv select:hover,.sch-adv input[type=time]:hover,.sch-adv input[type=number]:hover,.sch-adv input[type=text]:hover{border-color:rgba(255,255,255,.13);background:linear-gradient(180deg,rgba(7,10,18,.96),rgba(3,5,10,.985))}
#sec-scheduling .cw-subpanel[data-sub="basic"] select:focus,#sec-scheduling .cw-subpanel[data-sub="basic"] input[type=time]:focus,#sec-scheduling .cw-subpanel[data-sub="basic"] input[type=number]:focus,.sch-adv select:focus,.sch-adv input[type=time]:focus,.sch-adv input[type=number]:focus,.sch-adv input[type=text]:focus{outline:none;border-color:rgba(122,120,255,.42);box-shadow:0 0 0 3px rgba(101,107,255,.12),inset 0 1px 0 rgba(255,255,255,.025)}
.sch-adv{padding:16px}
.sch-adv .cw-panel-head{position:relative;z-index:1;display:flex;align-items:center;min-height:104px;margin:0 0 14px;padding:14px 16px;border:1px solid var(--sch-border-soft);border-radius:18px;background:var(--sch-card-bg);box-shadow:inset 0 1px 0 rgba(255,255,255,.025)}
.sch-adv .cw-panel-head::before{content:"Enable";position:absolute;top:14px;left:16px;font-size:11px;font-weight:800;letter-spacing:.14em;text-transform:uppercase;color:rgba(214,223,238,.58)}
.sch-adv .cw-panel-head .cx-toggle{margin-top:20px}
.sch-adv .mini,.sch-adv .status{position:relative;z-index:1}
.sch-adv .mini{font-size:12px;color:var(--sch-fg-soft)}
.sch-adv .status{display:flex;align-items:center;min-height:22px;font-size:12px;font-weight:700;color:rgba(255,214,128,.88)}
.sch-adv .status:empty{display:none}
.sch-adv-section{position:relative;z-index:1;display:grid;gap:10px;margin-top:14px;padding:14px;border:1px solid var(--sch-border-soft);border-radius:18px;background:var(--sch-card-bg-soft);box-shadow:inset 0 1px 0 rgba(255,255,255,.02)}
.sch-adv-section:first-of-type{margin-top:0}
.sch-adv-section-head{display:flex;align-items:flex-start;justify-content:space-between;gap:12px;flex-wrap:wrap}
.sch-adv-section-title{font-size:12px;font-weight:900;letter-spacing:.12em;text-transform:uppercase;color:#f7f9ff}
.sch-adv-section-copy{max-width:70ch;font-size:12px;line-height:1.45;color:var(--sch-fg-soft)}
.sch-adv table{position:relative;z-index:1;width:100%;border-collapse:separate;border-spacing:0 10px;margin-top:0;table-layout:fixed}
.sch-adv thead th{padding:0 10px 6px;text-align:left;font-size:11px;font-weight:800;letter-spacing:.12em;text-transform:uppercase;color:rgba(214,223,238,.56);border-bottom:none}
.sch-adv .th-help{display:inline-flex;align-items:center;gap:8px}
.sch-adv .sch-help{position:relative;display:inline-flex;align-items:center;justify-content:center;width:28px;height:28px;border-radius:999px;border:1px solid rgba(255,255,255,.12);background:linear-gradient(180deg,rgba(255,255,255,.06),rgba(255,255,255,.025));color:#edf4ff;cursor:help;box-shadow:inset 0 1px 0 rgba(255,255,255,.03)}
.sch-adv .sch-help::before{content:"help";font-family:"Material Symbols Rounded","Material Symbols Outlined","Segoe UI Symbol",sans-serif;font-size:18px;line-height:1}
.sch-adv .sch-help:focus-visible{outline:none;box-shadow:0 0 0 3px rgba(101,107,255,.12),inset 0 1px 0 rgba(255,255,255,.03)}
.sch-adv tbody tr{background:var(--sch-card-bg);box-shadow:inset 0 1px 0 rgba(255,255,255,.02)}
.sch-adv tbody td{padding:12px 10px;vertical-align:middle;border-top:1px solid var(--sch-border-soft);border-bottom:1px solid var(--sch-border-soft)}
.sch-adv tbody td:first-child{border-left:1px solid var(--sch-border-soft);border-radius:18px 0 0 18px}
.sch-adv tbody td:last-child{border-right:1px solid var(--sch-border-soft);border-radius:0 18px 18px 0}
.sch-adv .chipdays{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:8px;align-items:center}
.sch-adv .chipdays label{display:inline-flex;align-items:center;justify-content:center;gap:7px;min-height:38px;padding:0 10px;border:1px solid var(--sch-border-soft);border-radius:999px;cursor:pointer;width:100%;background:var(--sch-card-bg-soft);color:rgba(236,241,251,.78);font-size:12px;font-weight:700;transition:border-color .18s ease,background .18s ease,transform .18s ease,color .18s ease}
.sch-adv .chipdays label:hover{transform:translateY(-1px);border-color:rgba(255,255,255,.13);background:linear-gradient(180deg,rgba(16,20,33,.92),rgba(7,9,15,.96))}
.sch-adv .chipdays input{accent-color:#7c76ff;transform:translateY(1px)}
.sch-adv .chipdays label:has(input:checked){color:#f7f9ff;border-color:rgba(122,120,255,.34);background:linear-gradient(180deg,rgba(89,86,196,.22),rgba(16,18,34,.96))}
.sch-adv .chipdays .chipspacer{width:100%;height:1px;visibility:hidden;pointer-events:none}
.sch-adv .stack{display:grid;gap:8px}
.sch-adv .stack.two{grid-template-columns:repeat(2,minmax(0,1fr))}
.sch-adv .stack.three{grid-template-columns:repeat(3,minmax(0,1fr))}
.sch-adv .subnote{font-size:10px;font-weight:800;letter-spacing:.1em;text-transform:uppercase;color:rgba(214,223,238,.5)}
.sch-adv .field-mini{display:grid;gap:6px;min-width:0}
.sch-adv td[data-label="Source"]{min-width:260px}
.sch-adv td[data-label="Action"]{min-width:240px}
.sch-adv td[data-label="Source"] .stack,.sch-adv td[data-label="Action"] .stack{width:100%}
.sch-adv td[data-label="Source"] select,.sch-adv td[data-label="Action"] select{width:100%!important;min-width:0!important}
.sch-adv td[data-label="Source"] select,.sch-adv td[data-label="Event"] select{min-width:146px}
.sch-adv .event-filter-grid{grid-template-columns:minmax(220px,1.18fr) minmax(132px,.82fr);column-gap:12px}
.sch-adv .event-filter-grid input,.sch-adv .event-filter-grid select{min-width:0}
.sch-adv .checkline{display:flex;align-items:center;gap:8px;min-height:18px;font-size:12px;color:var(--sch-fg-soft)}
.sch-adv .checkline input{width:16px;height:16px;accent-color:#7c76ff}
.sch-adv .row-disabled{opacity:.5;filter:grayscale(.24)}
.sch-adv option[disabled]{color:#666}
.sch-adv-actions{position:relative;z-index:1;display:flex;gap:10px;flex-wrap:wrap;margin-top:12px}
.sch-adv .btn,.sch-adv .btn.ghost{min-height:40px;padding:0 14px;border-radius:999px;border:1px solid rgba(255,255,255,.09);background:linear-gradient(180deg,rgba(255,255,255,.065),rgba(255,255,255,.03));color:var(--sch-fg);box-shadow:inset 0 1px 0 rgba(255,255,255,.03);transition:transform .18s ease,background .18s ease,border-color .18s ease}
.sch-adv .btn:hover,.sch-adv .btn.ghost:hover{transform:translateY(-1px);border-color:rgba(255,255,255,.14);background:linear-gradient(180deg,rgba(110,112,255,.16),rgba(255,255,255,.04))}
.sch-adv tbody .btn.ghost{min-width:38px;padding:0 12px}
.sch-adv.adv-disabled{opacity:.55;filter:saturate(.75)}
.sch-std-toggle{margin-top:0}
@media (max-width:980px){#sec-scheduling .cw-subpanel[data-sub="basic"] .auth-card-fields{grid-template-columns:1fr}.sch-adv .chipdays{grid-template-columns:repeat(3,minmax(0,1fr))}}
@media (max-width:760px){.sch-adv{padding:14px}.sch-adv .cw-panel-head{min-height:0;padding:14px}.sch-adv .cw-panel-head .cx-toggle{margin-top:18px}.sch-adv table,.sch-adv thead,.sch-adv tbody,.sch-adv tr,.sch-adv td,.sch-adv th{display:block}.sch-adv thead{display:none}.sch-adv tbody{display:grid;gap:10px}.sch-adv tbody tr{border:1px solid var(--sch-border-soft);border-radius:18px;overflow:hidden}.sch-adv tbody td{display:grid;gap:6px;border:none!important;border-radius:0!important;padding:10px 12px}.sch-adv tbody td[data-label]::before{content:attr(data-label);font-size:10px;font-weight:800;letter-spacing:.12em;text-transform:uppercase;color:rgba(214,223,238,.56)}.sch-adv .chipdays{grid-template-columns:repeat(2,minmax(0,1fr))}.sch-adv .stack.two,.sch-adv .stack.three,.sch-adv .event-filter-grid{grid-template-columns:1fr}}
` }));

  // state
  let _pairs = [], _jobs = [], _eventRules = [], _advEnabled = false, _loading = false;
  let _eventRoutes = { watcher: [], webhook: [] }, _eventRouteError = "";
  const DAY = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"];
  const EVENT_SOURCE_OPTIONS = [["watcher", "Watcher"], ["webhook", "Webhook"]];
  const EVENT_NAME_OPTIONS = [["start", "Start"], ["pause", "Pause"], ["stop", "Stop"]];
  const EVENT_MEDIA_OPTIONS = [["", "Any"], ["movie", "Movie"], ["episode", "Episode"]];
  const HELP_TIPS = {
    time_pair: "Pair:\nChoose which enabled sync pair this timed step should run.",
    time_time: "Time:\nChoose the local time when this step becomes due.",
    time_days: "Days:\nSelect which weekdays this step may run on.\nLeave all days unchecked to allow every day.",
    time_after: "After:\nOptional dependency.\nUse this to run the step only after another earlier step has completed.",
    source: "Source:\nChoose where the trigger comes from.\nThen choose the exact watcher or webhook route under it.",
    event: "Event:\nChoose which playback activity should trigger the rule.\nStart: playback begins or resumes.\nPause: playback is paused.\nStop: playback ends or stops.",
    filters: "Filters:\nMedia: only movies or episodes.\nMin %: require minimum playback progress.",
    action: "Action:\nChoose what happens when the rule matches.\nSync pair:\nRun one specific enabled sync pair immediately.",
    guardrails: "Mute (min):\nIgnore new triggers for this rule after it runs.\nDedupe (sec):\nSuppress identical repeated events for a short window.\nMax / hour:\nHard safety cap for this rule in one hour."
  };
  const defaultJob = () => ({ id: genId(), pair_id: null, at: null, days: [], after: null, active: true });
  const defaultEventRule = () => ({
    id: genId(),
    source: _eventRoutes.watcher?.length ? "watcher" : _eventRoutes.webhook?.length ? "webhook" : "watcher",
    event: "stop",
    filters: {
      route_id: "",
      provider: "",
      provider_instance: "",
      media_type: "",
      min_progress: null
    },
    action: {
      kind: "sync_pair",
      pair_id: null
    },
    guardrails: {
      cooldown_minutes: 15,
      dedupe_window_seconds: 30,
      max_runs_per_hour: 4
    },
    active: true
  });
  const setBooleanSelect = (sel, v) => {
    if (!sel) return;
    const want = v ? "true" : "false";
    const opts = [...(sel.options || [])];
    let hit = opts.find(o => String(o.value).trim().toLowerCase() === want);
    if (!hit) {
      const labels = v ? ["enabled","enable","on","yes","true","1"] : ["disabled","disable","off","no","false","0"];
      hit = opts.find(o => labels.includes(String(o.textContent).trim().toLowerCase()));
    }
    if (hit) sel.value = hit.value;
  };

const ensureStdEnabledToggle = () => {
  const sel = $("#schEnabled");
  if (!sel || sel.__toggleEnhanced) return;
  const box = sel.parentElement;
  if (!box) return;

  const lab = box.querySelector("label");
  if (lab) lab.remove();
  sel.style.display = "none";

  const t = el("label", "cx-toggle sch-std-toggle");
  t.innerHTML = `<input type="checkbox" id="schEnabledToggle"><span class="cx-toggle-ui" aria-hidden="true"></span><span class="cx-toggle-text"></span><span class="cx-toggle-state" aria-hidden="true"></span>`;
  box.appendChild(t);

  const cb = $("#schEnabledToggle", box);
  const syncFromSel = () => { cb.checked = String(sel.value || "").trim().toLowerCase() === "true"; };
  sel.__toggleSync = syncFromSel;
  syncFromSel();

  cb.onchange = () => {
    // Standard on -> force advanced off
    if (cb.checked) {
      const advCb = $("#schAdvEnabled");
      if (advCb && advCb.checked) advCb.checked = false;
      _advEnabled = !!$("#schAdvEnabled")?.checked;
    }

    setBooleanSelect(sel, cb.checked);
    try { sel.dispatchEvent(new Event("change", { bubbles: true })); } catch {}
    try { sel.dispatchEvent(new Event("input", { bubbles: true })); } catch {}

    applyModeLocks();
  };

  sel.addEventListener("change", syncFromSel);
  sel.__toggleEnhanced = true;
};

  const setAdvDisabled = (disabled) => {
    const adv = $("#schAdv");
    if (!adv) return;
    adv.classList.toggle("adv-disabled", !!disabled);
    [...adv.querySelectorAll("select,input,button")].forEach((n) => {
      if (n && n.id === "schAdvEnabled") return;
      n.disabled = !!disabled;
    });
  };

  const decorateStandardPanel = () => {
    const basic = $("#sec-scheduling .cw-subpanel[data-sub='basic'] .auth-card");
    if (!basic) return;
  };

  const applyModeLocks = () => {
    const sel = $("#schEnabled");
    const stdToggle = $("#schEnabledToggle");
    const advCb = $("#schAdvEnabled");

    const stdEnabled = String(sel?.value || "").trim().toLowerCase() === "true";
    const advEnabled = !!advCb?.checked;

    if (advEnabled) {
      // Advanced on -> force standard off
      if (stdEnabled) {
        setBooleanSelect(sel, false);
        try { sel.dispatchEvent(new Event("change", { bubbles: true })); } catch {}
        try { sel.dispatchEvent(new Event("input", { bubbles: true })); } catch {}
      }
      if (stdToggle) stdToggle.checked = false;
    } else if (stdEnabled) {
      // Standard on -> force advanced off
      if (advCb && advCb.checked) advCb.checked = false;
      _advEnabled = !!advCb?.checked;
    }

    const advOn = !!advCb?.checked;
    const stdOn = String(sel?.value || "").trim().toLowerCase() === "true";

    // Lock standard fields when advanced is on
    const lockStdFields = advOn;
    ["schMode", "schN", "schTime"].forEach((id) => {
      const n = $("#" + id);
      if (n) n.disabled = lockStdFields;
    });

    // Lock advanced fields when standard is on 
    setAdvDisabled(stdOn);

    try { window.refreshSchedulingBanner?.(); } catch {}
    try { window.cwSchedSettingsHubUpdate?.(); } catch {}
  };

  // data
  const fetchPairs = async () => {
    if (authSetupPending()) { _pairs = []; return; }
    try {
      const r = await fetch("/api/pairs", { cache: "no-store" });
      const arr = await r.json();
      _pairs = Array.isArray(arr) ? arr.map(p => ({
        id: String(p.id),
        label: `${String(p.source || "").toUpperCase()} -> ${String(p.target || "").toUpperCase()} ${String(p.mode || "")}`.trim(),
        enabled: !!p.enabled
      })) : [];
    } catch (e) { console.warn("[scheduler] /api/pairs failed", e); _pairs = []; }
  };
  const eventRoutesFor = source => Array.isArray(_eventRoutes?.[source]) ? _eventRoutes[source] : [];
  const eventSourceReady = source => eventRoutesFor(source).length > 0;
  const eventSourceLabel = source => String(source || "").trim().toLowerCase() === "webhook" ? "Webhook" : "Watcher";
  const findEventRoute = (source, routeId) => eventRoutesFor(source).find(r => String(r?.id || "") === String(routeId || ""));
  const fetchEventRoutes = async () => {
    _eventRoutes = { watcher: [], webhook: [] };
    _eventRouteError = "";
    if (authSetupPending()) return;
    try {
      const res = await fetch("/api/scrobble/event_routes", { cache: "no-store" });
      const data = await res.json();
      _eventRoutes = {
        watcher: Array.isArray(data?.watcher_routes) ? data.watcher_routes.filter(r => r && typeof r === "object" && String(r.id || "").trim()) : [],
        webhook: Array.isArray(data?.webhook_routes) ? data.webhook_routes.filter(r => r && typeof r === "object" && String(r.id || "").trim()) : []
      };
      if (!eventSourceReady("watcher") && !eventSourceReady("webhook")) _eventRouteError = "No enabled watcher or webhook routes configured for event triggers.";
    } catch (e) {
      console.warn("[scheduler] /api/scrobble/event_routes failed", e);
      _eventRouteError = "Unable to load watcher or webhook routes for event triggers.";
    }
  };
  const sourceOptions = current => EVENT_SOURCE_OPTIONS.map(([value, label]) => [
    value,
    eventSourceReady(value) ? label : `${label} (unavailable)`,
    eventSourceReady(value) ? null : { disabled: true, selected: String(current || "") === value }
  ]);
  const routeOptions = (source, current) => {
    const routes = eventRoutesFor(source);
    const emptyLabel = source === "webhook" ? "Select webhook" : "Select route";
    const missingLabel = source === "webhook" ? "No webhooks configured" : "No routes configured";
    const options = [["", routes.length ? emptyLabel : missingLabel]];
    if (current && !routes.some(r => String(r?.id || "") === String(current))) options.push([current, `Missing route (${current})`, { disabled: true, selected: true }]);
    routes.forEach(route => options.push([route.id, route.label || route.id]));
    return options;
  };
  const eventRuleIssue = rule => {
    const pairId = String(rule?.action?.pair_id || "").trim();
    const routeId = String(rule?.filters?.route_id || "").trim();
    const hasContent = !!pairId || !!routeId;
    if (!hasContent) return "";
    if (!eventSourceReady(rule?.source || "")) return `Select an enabled source for event trigger ${rule?.id || ""}.`.trim();
    if (!routeId) return `Select a ${eventSourceLabel(rule?.source).toLowerCase()} route for each event trigger.`;
    if (!pairId) return "Select a sync pair for each event trigger.";
    return "";
  };
  const serializableEventRules = () => {
    const out = [];
    const issues = [];
    _eventRules.forEach(rule => {
      const issue = eventRuleIssue(rule);
      if (issue) {
        issues.push(issue);
        return;
      }
      const pairId = String(rule?.action?.pair_id || "").trim();
      const routeId = String(rule?.filters?.route_id || "").trim();
      if (!pairId && !routeId) return;
      out.push(rule);
    });
    return { rules: out, issues };
  };
  const syncRuleRoute = rule => {
    if (!rule?.filters) rule.filters = {};
    if (findEventRoute(rule.source, rule.filters.route_id || "")) return;
    const routes = eventRoutesFor(rule.source);
    if (!routes.length) {
      rule.filters.route_id = "";
      return;
    }
    const provider = String(rule.filters.provider || "").trim().toLowerCase();
    const providerInstance = String(rule.filters.provider_instance || "").trim();
    let matches = routes.filter(route => !provider || String(route?.provider || "").trim().toLowerCase() === provider);
    if (providerInstance) matches = matches.filter(route => String(route?.provider_instance || "default").trim() === providerInstance);
    if (matches.length === 1) {
      rule.filters.route_id = String(matches[0].id || "");
      return;
    }
    if (!provider && routes.length === 1) {
      rule.filters.route_id = String(routes[0].id || "");
      return;
    }
    rule.filters.route_id = "";
  };
  const isEnabled = pid => !!_pairs.find(p => String(p.id) === String(pid) && p.enabled);
  const normalizeEventRule = (rule = {}) => {
    const filters = rule && typeof rule.filters === "object" ? rule.filters : {};
    const action = rule && typeof rule.action === "object" ? rule.action : {};
    const guardrails = rule && typeof rule.guardrails === "object" ? rule.guardrails : {};
    const def = defaultEventRule();
    return {
      id: rule.id || genId(),
      source: ["watcher", "webhook"].includes(String(rule.source || "").trim().toLowerCase()) ? String(rule.source).trim().toLowerCase() : def.source,
      event: ["start", "pause", "stop"].includes(String(rule.event || "").trim().toLowerCase()) ? String(rule.event).trim().toLowerCase() : def.event,
      filters: {
        route_id: String(filters.route_id || filters.routeId || "").trim(),
        provider: String(filters.provider || "").trim().toLowerCase(),
        provider_instance: String(filters.provider_instance || filters.providerInstance || "").trim(),
        media_type: ["movie", "episode"].includes(String(filters.media_type || "").trim().toLowerCase()) ? String(filters.media_type).trim().toLowerCase() : "",
        min_progress: filters.min_progress === "" || filters.min_progress == null ? null : Math.max(0, Math.min(100, parseInt(filters.min_progress, 10) || 0))
      },
      action: {
        kind: "sync_pair",
        pair_id: String(action.pair_id || action.pairId || "").trim() || null
      },
      guardrails: {
        cooldown_minutes: Math.max(0, parseInt(guardrails.cooldown_minutes ?? def.guardrails.cooldown_minutes, 10) || 0),
        dedupe_window_seconds: Math.max(0, parseInt(guardrails.dedupe_window_seconds ?? def.guardrails.dedupe_window_seconds, 10) || 0),
        max_runs_per_hour: Math.max(0, parseInt(guardrails.max_runs_per_hour ?? def.guardrails.max_runs_per_hour, 10) || 0)
      },
      active: rule.active !== false
    };
  };
  const tdCell = (label, ...children) => {
    const td = el("td");
    td.dataset.label = label;
    td.append(...children);
    return td;
  };
  const stackWrap = (cls, ...children) => {
    const wrap = el("div", cls);
    wrap.append(...children);
    return wrap;
  };
  const buildSelect = ({ id, value, options, onChange }) => {
    const sel = el("select");
    sel.id = id;
    sel.name = id;
    options.forEach(([optValue, label, extra]) => {
      sel.appendChild(Object.assign(el("option"), {
        value: optValue,
        textContent: label,
        selected: String(value ?? "") === String(optValue),
        ...(extra || {})
      }));
    });
    if (onChange) sel.onchange = () => onChange(sel.value);
    return sel;
  };
  const buildInput = ({ id, type = "text", value = "", min = null, max = null, placeholder = "", onChange }) => {
    const input = Object.assign(el("input"), { id, name: id, type, value, placeholder });
    if (min != null) input.min = String(min);
    if (max != null) input.max = String(max);
    if (onChange) input.onchange = () => onChange(input.value, input);
    return input;
  };
  const buildCheck = ({ id, checked, label, onChange }) => {
    const line = el("label", "checkline");
    const chk = Object.assign(el("input"), { id, name: id, type: "checkbox", checked: !!checked });
    if (onChange) chk.onchange = () => onChange(chk.checked);
    line.append(chk, Object.assign(el("span"), { textContent: label }));
    return line;
  };
  const fieldMini = (label, control) => stackWrap("field-mini", Object.assign(el("div", "subnote"), { textContent: label }), control);
  const guardInput = ({ id, value, placeholder, title, onChange }) => {
    const input = buildInput({ id, type: "number", min: 0, value, placeholder, onChange });
    if (title) input.title = title;
    return input;
  };
  const pairOptions = (selected, includeNoneText = "Select pair") => [
    ["", includeNoneText],
    ..._pairs.map(p => [p.id, p.label + (p.enabled ? "" : " (disabled)"), { disabled: !p.enabled }])
  ];

  // row builder
  const jobRow = j => {
    const tr = el("tr"); if (j.active !== false && j.pair_id && !isEnabled(j.pair_id)) tr.classList.add("row-disabled");
    const rowKey = fieldKey(j?.id, `job_${_jobs.indexOf(j) + 1}`);

    const sel = buildSelect({
      id: `sched_pair_${rowKey}`,
      value: j.pair_id || "",
      options: pairOptions(j.pair_id || null),
      onChange: value => { j.pair_id = value || null; }
    });

    const t = buildInput({
      id: `sched_time_${rowKey}`,
      type: "time",
      value: j.at || "",
      onChange: value => { j.at = (value || "").trim() || null; }
    });

    const wrap = el("div","chipdays"), cur = new Set(Array.isArray(j.days) ? j.days : []);
    DAY.forEach((d,i) => {
      const lab = el("label"), chk = Object.assign(el("input"), { type: "checkbox", checked: cur.has(i+1) }), txt = el("span");
      chk.id = `sched_days_${rowKey}_${i+1}`;
      chk.name = `sched_days_${rowKey}[]`;
      chk.onchange = () => { const S = new Set(Array.isArray(j.days) ? j.days : []); chk.checked ? S.add(i+1) : S.delete(i+1); j.days = [...S].sort((a,b)=>a-b); };
      txt.textContent = d;
      lab.append(chk, txt); wrap.appendChild(lab);
    if(i===2){ wrap.appendChild(el("span","chipspacer")); }
    });

    const sa = buildSelect({
      id: `sched_after_${rowKey}`,
      value: j.after || "",
      options: [["", "None"], ..._jobs.filter(x => x !== j).map((x, i) => [String(x.id), `Step ${i + 1}`])],
      onChange: value => { j.after = value || null; renderJobs(); }
    });

    const c = Object.assign(el("input"), { type: "checkbox", checked: j.active !== false });
    c.id = `sched_active_${rowKey}`;
    c.name = c.id;
    c.onchange = () => { j.active = !!c.checked; renderJobs(); };

    const del = Object.assign(el("button"), { className: "btn ghost", textContent: "x" });
    del.onclick = () => { _jobs = _jobs.filter(x => x !== j); renderJobs(); };

    const tdDays = tdCell("Days", wrap);
    tr.append(
      tdCell("Pair", sel),
      tdCell("Time", t),
      tdDays,
      tdCell("After", sa),
      tdCell("Active", c),
      tdCell("Remove", del)
    );
    return tr;
  };

  const eventRuleRow = r => {
    const tr = el("tr");
    const pairId = r?.action?.pair_id || null;
    if (r.active !== false && pairId && !isEnabled(pairId)) tr.classList.add("row-disabled");
    const rowKey = fieldKey(r?.id, `event_${_eventRules.indexOf(r) + 1}`);
    const sourceSel = buildSelect({
      id: `sched_evt_source_${rowKey}`,
      value: r.source || "watcher",
      options: sourceOptions(r.source || "watcher"),
      onChange: value => {
        r.source = value || (_eventRoutes.watcher?.length ? "watcher" : _eventRoutes.webhook?.length ? "webhook" : "watcher");
        r.filters.route_id = "";
        renderEventRules();
      }
    });
    sourceSel.style.width = "100%";
    sourceSel.style.minWidth = "0";
    const eventSel = buildSelect({ id: `sched_evt_name_${rowKey}`, value: r.event || "stop", options: EVENT_NAME_OPTIONS, onChange: value => { r.event = value || "stop"; } });
    eventSel.style.width = "15ch";
    eventSel.style.minWidth = "15ch";
    const routeSel = buildSelect({
      id: `sched_evt_route_${rowKey}`,
      value: r.filters?.route_id || "",
      options: routeOptions(r.source || "watcher", r.filters?.route_id || ""),
      onChange: value => {
        r.filters.route_id = value || "";
        renderEventRules();
      }
    });
    routeSel.disabled = !eventRoutesFor(r.source || "watcher").length;
    routeSel.style.width = "100%";
    routeSel.style.minWidth = "0";
    const mediaSel = buildSelect({ id: `sched_evt_media_${rowKey}`, value: r.filters?.media_type || "", options: EVENT_MEDIA_OPTIONS, onChange: value => { r.filters.media_type = value || ""; } });
    mediaSel.style.width = "15ch";
    mediaSel.style.minWidth = "15ch";
    const minProgressInput = buildInput({
      id: `sched_evt_progress_${rowKey}`,
      type: "number",
      min: 0,
      max: 100,
      value: r.filters?.min_progress ?? "",
      placeholder: "Min %",
      onChange: (value, input) => {
        const next = (value || "").trim();
        r.filters.min_progress = next === "" ? null : Math.max(0, Math.min(100, parseInt(next, 10) || 0));
        input.value = r.filters.min_progress == null ? "" : String(r.filters.min_progress);
      }
    });
    minProgressInput.style.width = "9ch";
    minProgressInput.style.minWidth = "9ch";
    const pairSel = buildSelect({
      id: `sched_evt_pair_${rowKey}`,
      value: r.action?.pair_id || "",
      options: pairOptions(r.action?.pair_id || null),
      onChange: value => {
        r.action.kind = "sync_pair";
        r.action.pair_id = value || null;
        renderEventRules();
      }
    });
    pairSel.style.width = "100%";
    pairSel.style.minWidth = "0";
    const numGuard = (suffix, key, placeholder, title) => guardInput({
      id: `sched_evt_${suffix}_${rowKey}`,
      value: r.guardrails?.[key] ?? 0,
      placeholder,
      title,
      onChange: (value, input) => {
        r.guardrails[key] = Math.max(0, parseInt(value || "0", 10) || 0);
        input.value = String(r.guardrails[key]);
      }
    });
    const cooldownInput = numGuard("cooldown", "cooldown_minutes", "15", "Ignore repeat triggers for this many minutes after a rule runs.");
    cooldownInput.style.width = "8ch";
    cooldownInput.style.minWidth = "8ch";
    const dedupeInput = numGuard("dedupe", "dedupe_window_seconds", "30", "Suppress identical events that arrive again within this many seconds.");
    dedupeInput.style.width = "8ch";
    dedupeInput.style.minWidth = "8ch";
    const rateInput = numGuard("rate", "max_runs_per_hour", "4", "Hard cap for how many times this rule can run in one hour.");
    rateInput.style.width = "8ch";
    rateInput.style.minWidth = "8ch";
    const activeChk = Object.assign(el("input"), { id: `sched_evt_active_${rowKey}`, name: `sched_evt_active_${rowKey}`, type: "checkbox", checked: r.active !== false });
    activeChk.onchange = () => { r.active = !!activeChk.checked; renderEventRules(); };
    const del = Object.assign(el("button"), { className: "btn ghost", textContent: "x" });
    del.onclick = () => { _eventRules = _eventRules.filter(x => x !== r); renderEventRules(); };

    tr.append(
      tdCell("Source", stackWrap("stack", sourceSel, routeSel)),
      tdCell("Event", eventSel),
      tdCell("Filters", stackWrap(
        "stack",
        stackWrap(
          "stack two event-filter-grid",
          mediaSel,
          minProgressInput
        )
      )),
      tdCell("Action", stackWrap("stack", Object.assign(el("div", "subnote"), { textContent: "Sync pair" }), pairSel)),
      tdCell("Guardrails", stackWrap(
        "stack",
        stackWrap(
          "stack three",
          fieldMini("Mute (min)", cooldownInput),
          fieldMini("Dedupe (sec)", dedupeInput),
          fieldMini("Max / hour", rateInput)
        )
      )),
      tdCell("Active", activeChk),
      tdCell("Remove", del)
    );
    return tr;
  };

  // mount UI
  const ensureUI = () => {
    const host = $("#sched_advanced_mount") || $("#sec-scheduling .body");
    if (!host || $("#schAdv")) return;

    const adv = Object.assign(el("div", "sch-adv"), { id: "schAdv" });
    adv.innerHTML = `
<div class="cw-panel-head">
  <label class="cx-toggle">
    <input type="checkbox" id="schAdvEnabled">
    <span class="cx-toggle-ui" aria-hidden="true"></span>
    <span class="cx-toggle-text">Use advanced plan</span>
    <span class="cx-toggle-state" aria-hidden="true"></span>
  </label>
</div>

<section class="sch-adv-section">
  <div class="sch-adv-section-head">
    <div>
      <div class="sch-adv-section-title">Time Plan</div>
      <div class="sch-adv-section-copy">Use timed steps for the current advanced schedule. Only enabled pairs are selectable.</div>
    </div>
  </div>
  <table>
    <thead><tr>
      <th style="width:32%"><span class="th-help">Pair<button type="button" class="sch-help" aria-label="Pair help" title="Pair help" data-help-key="time_pair"></button></span></th>
      <th style="width:14%"><span class="th-help">Time<button type="button" class="sch-help" aria-label="Time help" title="Time help" data-help-key="time_time"></button></span></th>
      <th style="width:30%"><span class="th-help">Days<button type="button" class="sch-help" aria-label="Days help" title="Days help" data-help-key="time_days"></button></span></th>
      <th style="width:14%"><span class="th-help">After<button type="button" class="sch-help" aria-label="After help" title="After help" data-help-key="time_after"></button></span></th>
      <th style="width:6%">Active</th>
      <th style="width:4%"></th>
    </tr></thead>
    <tbody id="schJobsBody"></tbody>
  </table>
  <div class="sch-adv-actions">
    <button class="btn" id="btnAddStep">Add step</button>
    <button class="btn" id="btnAutoFromPairs">Auto-create from enabled pairs</button>
  </div>
</section>

<section class="sch-adv-section">
  <div class="sch-adv-section-head">
    <div>
      <div class="sch-adv-section-title">Event Triggers</div>
      <div class="sch-adv-section-copy">Trigger a sync pair from watcher or webhook activity with filters and guardrails.</div>
    </div>
  </div>
  <table>
    <thead><tr>
      <th style="width:24%"><span class="th-help">Source<button type="button" class="sch-help" aria-label="Source help" title="Source help" data-help-key="source"></button></span></th>
      <th style="width:12%"><span class="th-help">Event<button type="button" class="sch-help" aria-label="Event help" title="Event help" data-help-key="event"></button></span></th>
      <th style="width:16%"><span class="th-help">Filters<button type="button" class="sch-help" aria-label="Filters help" title="Filters help" data-help-key="filters"></button></span></th>
      <th style="width:22%"><span class="th-help">Action<button type="button" class="sch-help" aria-label="Action help" title="Action help" data-help-key="action"></button></span></th>
      <th style="width:18%"><span class="th-help">Guardrails<button type="button" class="sch-help" aria-label="Guardrails help" title="Guardrails help" data-help-key="guardrails"></button></span></th>
      <th style="width:5%">Active</th>
      <th style="width:4%"></th>
    </tr></thead>
    <tbody id="schEventRulesBody"></tbody>
  </table>
  <div class="sch-adv-actions">
    <button class="btn" id="btnAddEventRule">Add event trigger</button>
  </div>
</section>

<div class="status" id="schAdvStatus"></div>
`;
    host.appendChild(adv);
    adv.querySelectorAll(".sch-help").forEach((btn) => {
      const tip = HELP_TIPS[btn.dataset.helpKey] || "";
      if (tip) {
        btn.title = tip;
      } else {
        btn.removeAttribute("title");
      }
      btn.onclick = (e) => {
        e.preventDefault();
        e.stopPropagation();
      };
    });

    $("#btnAddStep").onclick = () => { _jobs.push(defaultJob()); renderJobs(); };
    $("#btnAutoFromPairs").onclick = () => {
      const eps = _pairs.filter(p => p.enabled);
      _jobs = eps.map(p => ({ id: genId(), pair_id: p.id, at: null, days: [], after: null, active: true }));
      if (!_jobs.length) _jobs.push(defaultJob());
      renderJobs();
    };
    $("#btnAddEventRule").onclick = () => { _eventRules.push(defaultEventRule()); renderEventRules(); };
    $("#schAdvEnabled").onchange = () => {
      _advEnabled = !!$("#schAdvEnabled").checked;

      // Advanced on -> force standard off
      if (_advEnabled) {
        const sel = $("#schEnabled");
        setBooleanSelect(sel, false);
        try { sel.dispatchEvent(new Event("change", { bubbles: true })); } catch {}
        try { sel.dispatchEvent(new Event("input", { bubbles: true })); } catch {}
        try { sel.__toggleSync?.(); } catch {}
      }

      applyModeLocks();
    };
  };

  // render
  const updateAdvancedStatus = () => {
    const st = $("#schAdvStatus");
    if (!st) return;
    if (_eventRouteError) {
      st.textContent = _eventRouteError;
      return;
    }
    const blockedJobs = _jobs.some(j => j._blocked);
    const blockedRules = _eventRules.some(r => r._blocked);
    const blockedRuleRoutes = _eventRules.some(r => r._route_blocked);
    const invalidRules = _eventRules.map(eventRuleIssue).filter(Boolean);
    if (invalidRules.length) {
      st.textContent = invalidRules[0];
      return;
    }
    if (blockedRuleRoutes) {
      st.textContent = "Some event triggers need a valid configured watcher or webhook route.";
      return;
    }
    if (blockedJobs && blockedRules) {
      st.textContent = "Some timed steps and event triggers reference disabled pairs.";
      return;
    }
    if (blockedJobs) {
      st.textContent = "Some timed steps reference disabled pairs.";
      return;
    }
    if (blockedRules) {
      st.textContent = "Some event triggers reference disabled pairs.";
      return;
    }
    st.textContent = "";
  };

  const renderJobs = () => {
    const tbody = $("#schJobsBody"); if (!tbody) return;
    tbody.innerHTML = "";
    if (!_jobs.length) _jobs.push(defaultJob());
    _jobs.forEach(j => j._blocked = j.active !== false && j.pair_id && !isEnabled(j.pair_id));
    _jobs.forEach(j => tbody.appendChild(jobRow(j)));
    updateAdvancedStatus();
    try { window.cwSchedSettingsHubUpdate?.(); } catch {}
  };

  const renderEventRules = () => {
    const tbody = $("#schEventRulesBody"); if (!tbody) return;
    tbody.innerHTML = "";
    if (!_eventRules.length) _eventRules.push(defaultEventRule());
    _eventRules.forEach(syncRuleRoute);
    _eventRules.forEach(r => {
      r._blocked = r.active !== false && r.action?.pair_id && !isEnabled(r.action.pair_id);
      r._route_blocked = r.active !== false && !!r.action?.pair_id && !findEventRoute(r.source, r.filters?.route_id || "");
    });
    _eventRules.forEach(r => tbody.appendChild(eventRuleRow(r)));
    const addBtn = $("#btnAddEventRule");
    if (addBtn) {
      addBtn.disabled = !eventSourceReady("watcher") && !eventSourceReady("webhook");
      addBtn.title = addBtn.disabled ? (_eventRouteError || "No enabled watcher or webhook routes configured.") : "";
    }
    updateAdvancedStatus();
    try { window.cwSchedSettingsHubUpdate?.(); } catch {}
  };

  // load
  const loadScheduling = async () => {
    if (authSetupPending()) return;
    if (_loading) return; _loading = true;
    try {
      ensureUI();
      decorateStandardPanel();
      await fetchPairs();
      await fetchEventRoutes();

      let saved = {};
      try { saved = await fetch(`/api/scheduling?t=${Date.now()}`, { cache: "no-store" }).then(r => r.json()); } catch {}

      setBooleanSelect($("#schEnabled"), !!saved.enabled);
      ensureStdEnabledToggle();
      try { $("#schEnabled")?.__toggleSync?.(); } catch {}
      $("#schMode") && ($("#schMode").value = saved.mode || "hourly");
      $("#schN")    && ($("#schN").value = String(saved.every_n_hours || 2));
      $("#schTime") && ($("#schTime").value = saved.daily_time || "03:30");

      const adv = saved?.advanced || {};
      _advEnabled = !!adv.enabled;
      $("#schAdvEnabled") && ($("#schAdvEnabled").checked = _advEnabled);
      _jobs = Array.isArray(adv.jobs) ? adv.jobs.map(j => ({
        id: j.id || genId(),
        pair_id: j.pair_id || null,
        at: j.at || null,
        days: Array.isArray(j.days) ? j.days.filter(n => n >= 1 && n <= 7) : [],
        after: j.after || null,
        active: j.active !== false
      })) : [];
      _eventRules = Array.isArray(adv.event_rules || adv.eventRules) ? (adv.event_rules || adv.eventRules).map(normalizeEventRule) : [];
      _eventRules.forEach(syncRuleRoute);
      renderJobs();
      renderEventRules();

      applyModeLocks();

      try { window.cwSchedSettingsHubInit?.(); } catch {}
      try { window.cwSchedSettingsHubUpdate?.(); } catch {}

      try { typeof window.refreshSchedulingBanner === "function" && window.refreshSchedulingBanner(); } catch {}
    } finally { _loading = false; }
  };
  window.loadScheduling = loadScheduling;

  // serialize advanced
  const serializeAdvanced = () => ({
    enabled: !!_advEnabled,
    jobs: _jobs.map(j => ({
      id: j.id,
      pair_id: j.pair_id || null,
      at: j.at || null,
      days: Array.isArray(j.days) ? j.days.slice() : [],
      after: j.after || null,
      active: j.active !== false
    })),
    event_rules: serializableEventRules().rules.map(r => ({
      id: r.id,
      source: r.source || "watcher",
      event: r.event || "stop",
      filters: {
        route_id: r.filters?.route_id || "",
        media_type: r.filters?.media_type || "",
        min_progress: r.filters?.min_progress == null || r.filters?.min_progress === "" ? null : Math.max(0, Math.min(100, parseInt(r.filters.min_progress, 10) || 0))
      },
      action: {
        kind: "sync_pair",
        pair_id: r.action?.pair_id || null
      },
      guardrails: {
        cooldown_minutes: Math.max(0, parseInt(r.guardrails?.cooldown_minutes || 0, 10) || 0),
        dedupe_window_seconds: Math.max(0, parseInt(r.guardrails?.dedupe_window_seconds || 0, 10) || 0),
        max_runs_per_hour: Math.max(0, parseInt(r.guardrails?.max_runs_per_hour || 0, 10) || 0)
      },
      active: r.active !== false
    }))
  });

  // public getter for current scheduling patch
  window.getSchedulingPatch = () => {
    const mode = $("#schMode")?.value || "hourly";
    const every_n_hours = parseInt($("#schN")?.value || "2", 10);
    const daily_time = $("#schTime")?.value || "03:30";
    const ruleState = serializableEventRules();
    if (ruleState.issues.length) throw new Error(ruleState.issues[0]);
    const advanced = serializeAdvanced();

    // Advanced plan disables standard scheduling
    const stdEnabled = ($("#schEnabled")?.value || "").trim() === "true";
    const enabled = advanced.enabled ? false : stdEnabled;

    return { enabled, mode, every_n_hours, daily_time, advanced };
  };

  // boot
  document.addEventListener("DOMContentLoaded", () => {
    if (authSetupPending()) return;
    loadScheduling().catch(e => console.warn("scheduler load failed", e));
    try { window.dispatchEvent(new Event("sched-banner-ready")); } catch {}
  });

  document.addEventListener("config-saved", (e) => {
    if (authSetupPending()) return;
    const section = e?.detail?.section;
    if (section && section !== "scheduling") return;
    loadScheduling().catch(err => console.warn("scheduler reload failed", err));
  });
})();
