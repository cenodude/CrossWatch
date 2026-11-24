/* assets/js/events/index.js */
/* CrossWatch - Events page */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import {pageBackLink, returnFromEvents} from '../page-return.js';
import {playbackDetail, playbackTimeline, playbackUpdatedAt} from './playback-timeline.js';
import {bindCovers, createActivityCard, dayLabel, dayRange, renderRunDetail, renderTitleDetail, runRowsHTML, titleRowsHTML} from './titles.js';

const T = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.t?.(key, vars) ?? key;
const TH = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.html?.(key, vars) ?? key;

const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => (
  { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]
));

function withEventScope(u, eventScope) {
  if (eventScope === null) return u;
  try {
    const url = new URL(u, location.origin);
    if (!url.pathname.startsWith("/api/events/")) return u;
    url.searchParams.set("user_profile", eventScope || "all");
    return `${url.pathname}${url.search}`;
  } catch {
    return u;
  }
}

const fjson = async (u, o = {}) => {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort("timeout"), 30000);
  try {
    const r = await fetch(u, { ...o, signal: ctrl.signal });
    if (!r.ok) throw new Error(r.status);
    return await r.json();
  } finally { clearTimeout(t); }
};

const Q = (s, r = document) => r.querySelector(s);

const TS = (v) => {
  const n = Number(v || 0);
  if (!n) return "";
  return new Date(n * 1000).toLocaleString((window.CW?.I18n || window.__cwI18n)?.locale);
};

const maybeTS = (v) => {
  if (v == null || v === "") return "";
  const n = Number(v);
  if (Number.isFinite(n) && n > 1000000000) return TS(n);
  return String(v);
};

const cloudResultsHTML = (e) => {
  const detail = detailOf(e);
  const rows = Array.isArray(detail.cloud_results) ? detail.cloud_results.filter(row => row && typeof row === "object") : [];
  const omitted = Number(detail.cloud_results_omitted || 0);
  const labels = {dated:T("events.cloud.dated"),present:T("events.cloud.present"),cached:T("events.cloud.cached"),plex_discover_date_pending:T("events.cloud.pending")};
  const counts = detail.cloud_outcome_counts && typeof detail.cloud_outcome_counts === "object" ? detail.cloud_outcome_counts : {};
  return rows.length || omitted ? `<span class="ev-tl-desc"><strong>${TH("events.cloud.fallback")}</strong>${Object.entries(counts).map(([outcome, count]) => `<span>${esc(labels[outcome] || outcome)}: ${esc(count)}; </span>`).join("")}${rows.map(row => `<span><br>${esc(row.title || row.key)}: ${esc(labels[row.outcome] || row.reason || row.outcome)}</span>`).join("")}${omitted ? `<span><br>${omitted} additional item details omitted; totals include all outcomes.</span>` : ""}</span>` : "";
};
const detailOf = (e) => {
  try {
    return typeof e?.detail === "string" ? (e.detail ? JSON.parse(e.detail) : {}) : (e?.detail || {});
  } catch {
    return {};
  }
};

const failureDetailsHTML = (e, events = []) => {
  const sameFailure = other => ["run_id", "item_key", "feature", "operation", "source_provider", "source_instance", "destination_provider", "destination_instance", "pair_key", "reason_code", "hint_message"].every(key => (other[key] || "") === (e[key] || "")) && JSON.stringify(other.match_details || null) === JSON.stringify(e.match_details || null);
  if (e.event_type === "unresolved_recorded" && e.run_id && e.item_key && events.some(other => other.event_type === "write_failed" && sameFailure(other))) return "";
  if (!["write_failed", "unresolved_recorded"].includes(e.event_type)) return "";
  const messages = [...new Set([e.reason_message, e.hint_message].filter(value => typeof value === "string" && value))];
  const details = e.match_details;
  const candidates = Array.isArray(details?.candidates) ? details.candidates.filter(item => item && typeof item === "object").slice(0, 10) : [];
  const ids = item => ["ids", "show_ids"].flatMap(field => Object.entries(item[field] || {}).map(([key, value]) => `${field === "show_ids" ? "show " : ""}${key}:${value}`)).join(" / ");
  const matches = candidates.length ? `<details class="ev-match-details"><summary>Possible matches (${esc(details.candidate_count)})</summary><div class="ev-match-list">${candidates.map(item => `<div class="ev-match-row"><span>${esc([item.type, item.local_id].filter(Boolean).join(" / "))}</span><span>${esc([item.title, item.year].filter(Boolean).join(" / "))}<br>${esc(ids(item))}${item.season != null && item.episode != null ? ` / S${esc(item.season)}E${esc(item.episode)}` : ""}</span></div>`).join("")}${details.truncated || details.candidates.length > 10 ? "<p>" + TH("events.matches.firstOnly") + "</p>" : ""}</div></details>` : "";
  if (!messages.length && !matches) return "";
  return `<div class="ev-failure-details"><strong>${TH("events.whatHappened")}</strong>${messages.map(message => `<p>${esc(message)}</p>`).join("")}${matches}</div>`;
};

const PAGE_SIZE = 10;
const RUN_ITEMS_PAGE_SIZE = 100;

const FEATURE_LABEL = { history: T("events.feature.history"), ratings: T("events.feature.ratings"), watchlist: T("events.feature.watchlist"), progress: T("events.feature.progress"), collection: T("events.feature.collections") };
const reasonLabel = (reason, label = "") => String(label || reason || "");

const activitySummary = group => {
  const rows = group.destinations || [group];
  const skipped = rows.filter(row => row.status === "skipped").length;
  const value = group.feature === "ratings" && group.rating != null ? ` · ${group.rating}/10` : group.progress != null ? ` · ${group.progress}%` : "";
  return `${statusLabel(group.status, group.domain)}${value}${skipped ? ` · ${T("events.count.destinationsSkipped", {count: skipped})}` : ""}`;
};

const withProviderLogos = (text) => {
  const meta = window.CW?.ProviderMeta;
  const parts = String(text ?? "").split(/\b([A-Z][A-Z0-9]{2,})\b/);
  return parts.map((part, i) => {
    if (i % 2 === 0 || meta?.get?.(part)?.key !== part) return esc(part);
    const src = meta.logoPath?.(part) || "";
    const logo = src ? `<img class="ev-prov-logo" src="${esc(src)}" alt="" width="14" height="14" loading="lazy">` : "";
    return `<span class="ev-prov">${logo}${esc(part)}</span>`;
  }).join("");
};

const provShort = (src, dst) => {
  const a = (src || "").toUpperCase();
  const b = (dst || "").toUpperCase();
  if (a && b) return `${a} → ${b}`;
  return b || a || "";
};

const instLabel = (prov, inst) => {
  const p = String(prov || "").toUpperCase();
  if (!p) return "";
  const i = String(inst || "").trim();
  if (!i || i.toLowerCase() === "default") return T("events.instDefault", {provider: p});
  const user = SYNC_USER_LABELS[p] && SYNC_USER_LABELS[p][i];
  if (user !== undefined && i.includes("~")) {
    const parent = i.split("~")[0];
    return `${instLabel(p, parent)} · ${user || i.split("~").slice(1).join("~")}`;
  }
  const label = (PROFILE_LABELS[p] && PROFILE_LABELS[p][i]) || i;
  return `${p} ${label}`;
};

let PROFILE_LABELS = {};
let SYNC_USER_LABELS = {};

async function loadProfileLabels(isCurrent = () => true) {
  try {
    const j = await fjson("/api/provider-instances", { cache: "no-store" });
    const map = j && typeof j === "object" ? j : {};
    const out = {};
    for (const [prov, raw] of Object.entries(map)) {
      const labels = {};
      for (const row of (Array.isArray(raw) ? raw : [])) {
        const id = String(typeof row === "string" ? row : row?.id || "").trim();
        if (!id || id.toLowerCase() === "default") continue;
        const label = row && typeof row === "object" ? String(row.label || "").trim() : "";
        labels[id] = label && label !== id ? label : id;
      }
      out[String(prov || "").toUpperCase()] = labels;
    }
    if (isCurrent()) PROFILE_LABELS = out;
  } catch {
    if (isCurrent()) PROFILE_LABELS = {};
  }
  try {
    const users = await fjson("/api/sync-user-labels", { cache: "no-store" });
    if (isCurrent()) SYNC_USER_LABELS = users && typeof users === "object" ? users : {};
  } catch {
    if (isCurrent()) SYNC_USER_LABELS = {};
  }
}

const routeOf = (e) => {
  const a = instLabel(e.source_provider, e.source_instance);
  const b = instLabel(e.destination_provider, e.destination_instance);
  if (a && b) return `${a} → ${b}`;
  return b || a || "";
};

const titleOf = (e) => {
  let t = e.title || e.item_key || "";
  if (e.media_type === "episode" && e.season != null && e.episode != null) {
    t = `${t} S${String(e.season).padStart(2, "0")}E${String(e.episode).padStart(2, "0")}`;
  }
  return t;
};

const isRating = (e) => e.event_type === "write_succeeded" && e.feature === "ratings" && (e.old_value != null || e.new_value != null);

const BADGE = {
  write_succeeded: T("events.badge.writeSucceeded"),
  write_attempted: T("events.badge.writeAttempted"),
  write_failed: T("events.badge.writeFailed"),
  unresolved_recorded: T("events.badge.unresolvedRecorded"),
  unresolved_cleared: T("events.badge.unresolvedCleared"),
  blackbox_promoted: T("events.badge.blackboxPromoted"),
  blackbox_blocked: T("events.badge.blackboxBlocked"),
  tombstone_created: T("events.badge.tombstoneCreated"),
  tombstone_pruned: T("events.badge.tombstonePruned"),
  cloud_history_read: T("events.badge.cloudHistory"),
  plan_created: T("events.badge.syncPlan"),
  provider_health: T("events.providerHealth"),
  sync_run_started: T("events.badge.syncStarted"),
  sync_run_finished: T("events.badge.syncFinished"),
  api_rate_limit: T("events.badge.rateLimit"),
  watcher_event: T("events.badge.watcher"),
  webhook_event: T("events.badge.webhook"),
  scrobble_started: T("events.badge.started"),
  scrobble_playback: T("events.badge.playback"),
  scrobble_paused: T("events.badge.paused"),
  scrobble_resumed: T("events.badge.resumed"),
  scrobble_stopped: T("events.badge.stopped"),
  scrobble_completed: T("events.badge.watched"),
  scrobble_failed: T("events.badge.scrobbleFailed"),
  scrobble_skipped: T("events.badge.scrobbleSkipped"),
  rating_applied: T("events.badge.rated"),
  rating_failed: T("events.badge.ratingFailed"),
  rating_skipped: T("events.badge.ratingSkipped"),
  capture_completed: T("events.badge.capture"),
  capture_failed: T("events.badge.captureFailed"),
  capture_cleanup: T("events.badge.capturesRemoved"),
  backup_completed: T("events.badge.backup"),
  backup_failed: T("events.badge.backupFailed"),
  backup_uploaded: T("events.badge.backupCopied"),
  backup_upload_failed: T("events.badge.backupCopyFailed"),
  backup_cleanup: T("events.badge.backupsRemoved"),
  audit_login: T("events.badge.login"),
  audit_login_failed: T("events.badge.loginFailed"),
  audit_login_blocked: T("events.badge.loginBlocked"),
  audit_2fa_required: T("events.badge.twoFaRequired"),
  audit_2fa_failed: T("events.badge.twoFaFailed"),
  audit_logout: T("events.badge.logout"),
  audit_logout_all: T("events.badge.logoutAll"),
  audit_logout_others: T("events.badge.logoutOthers"),
  audit_user_created: T("events.badge.userCreated"),
  audit_user_updated: T("events.badge.userUpdated"),
  audit_user_deleted: T("events.badge.userDeleted"),
  audit_credentials_updated: T("events.badge.credentialsUpdated"),
  audit_totp_setup: T("events.badge.twoFaSetup"),
  audit_totp_enabled: T("events.badge.twoFaEnabled"),
  audit_totp_disabled: T("events.badge.twoFaDisabled"),
  audit_plex_sso_linked: T("events.badge.plexLinked"),
  audit_plex_sso_unlinked: T("events.badge.plexUnlinked"),
  audit_api_action: T("events.badge.userAction"),
};

const badgeOf = (e) => {
  if (isRating(e)) return T("events.badge.ratingUpdated");
  return BADGE[e.event_type] || String(e.event_type || "").replace(/_/g, " ");
};

const ICON = {
  write_succeeded: "check_circle",
  write_attempted: "upload",
  write_failed: "cancel",
  unresolved_recorded: "shield",
  unresolved_cleared: "verified",
  blackbox_promoted: "inventory_2",
  blackbox_blocked: "block",
  tombstone_created: "delete",
  tombstone_pruned: "delete_sweep",
  plan_created: "description",
  provider_health: "favorite",
  sync_run_started: "play_circle",
  sync_run_finished: "done_all",
  api_rate_limit: "speed",
  watcher_event: "visibility",
  webhook_event: "webhook",
  scrobble_started: "play_arrow",
  scrobble_completed: "check_circle",
  scrobble_failed: "error",
  scrobble_skipped: "block",
  rating_applied: "star",
  rating_failed: "error",
  rating_skipped: "block",
  audit_login: "login",
  audit_login_failed: "error",
  audit_login_blocked: "block",
  audit_2fa_required: "password",
  audit_2fa_failed: "gpp_bad",
  audit_logout: "logout",
  audit_logout_all: "logout",
  audit_logout_others: "logout",
  audit_user_created: "person_add",
  audit_user_updated: "manage_accounts",
  audit_user_deleted: "person_remove",
  audit_credentials_updated: "admin_panel_settings",
  audit_totp_setup: "password",
  audit_totp_enabled: "verified_user",
  audit_totp_disabled: "shield_lock",
  audit_plex_sso_linked: "link",
  audit_plex_sso_unlinked: "link_off",
  audit_api_action: "touch_app",
};

const iconOf = (e) => {
  if (isRating(e)) return "edit";
  return ICON[e.event_type] || "bolt";
};

const sevOf = (e) => {
  if (e.event_type === "write_failed") return "error";
  if (e.event_type === "write_succeeded") return isRating(e) ? "rating" : "ok";
  if (["blackbox_promoted", "blackbox_blocked"].includes(e.event_type)) return "error";
  if (["unresolved_recorded", "api_rate_limit"].includes(e.event_type)) return "warn";
  if (["tombstone_created", "tombstone_pruned"].includes(e.event_type)) return "warn";
  if (["scrobble_completed", "rating_applied"].includes(e.event_type)) return "ok";
  if (["scrobble_failed", "rating_failed"].includes(e.event_type)) return "error";
  if (String(e.event_type || "").startsWith("audit_")) {
    const st = String(detailOf(e).status || e.reason_code || "").toLowerCase();
    return ["failed", "blocked", "denied"].includes(st) ? "error" : "ok";
  }
  if (e.event_type === "scrobble_started") return "info";
  if (e.event_type === "unresolved_cleared") return "ok";
  const s = String(e.severity || "").toLowerCase();
  if (s === "error" || s === "warn" || s === "ok") return s;
  return "info";
};

const planLine = (e) => {
  const d = detailOf(e);
  const route = [e.source_provider, e.destination_provider].filter(Boolean).map((p) => String(p).toUpperCase()).join(" → ");
  const parts = [["add", d.adds], ["remove", d.removes], ["update", d.updates]].filter(([, n]) => Number(n) > 0).map(([w, n]) => T(`events.plan.${w}`, {count: Number(n)}));
  const known = [d.adds, d.removes, d.updates].some((n) => n != null);
  const what = parts.length ? parts.join(", ") : known ? T("events.plan.nothing") : "";
  return [T("events.planLabel"), route, what].filter(Boolean).join(" · ");
};

const titleLine = (e) => {
  switch (e.event_type) {
    case "write_succeeded":
      return isRating(e) ? T("events.title.ratingUpdate") : T("events.badge.writeSucceeded");
    case "write_attempted": return e.operation === "tag_dropped" ? `${T("events.title.markDropped")} · ${(e.destination_provider || T("events.destination")).toUpperCase()}`
      : e.operation === "rewatch_reset" ? `${T("events.title.rewatchReset")} · ${(e.destination_provider || T("events.destination")).toUpperCase()}`
      : e.operation === "hide_continue_watching" ? `${T("events.title.hideContinue")} · ${(e.destination_provider || T("events.destination")).toUpperCase()}` : T("events.title.addAttempted");
    case "write_failed": return `${T("events.title.rejected", {provider: (e.destination_provider || T("events.destination")).toUpperCase()})} · ${reasonLabel(e.reason_code || "failed", e.reason_label)}`;
    case "unresolved_recorded": {
      const r = e.reason_code || e.operation || "";
      return r ? `${T("events.title.recordedUnresolved")} · ${reasonLabel(r, e.reason_label)}` : T("events.title.recordedUnresolved");
    }
    case "unresolved_cleared": return T("events.badge.unresolvedCleared");
    case "blackbox_promoted": return e.reason_code ? `${T("events.title.blackboxed")} · ${e.reason_code}` : T("events.title.blackboxed");
    case "blackbox_blocked": return T("events.title.blockedByBlackbox");
    case "tombstone_created": return T("events.badge.tombstoneCreated");
    case "tombstone_pruned": return T("events.badge.tombstonePruned");
    case "cloud_history_read": return T("events.title.cloudHistory", {count: Number(detailOf(e).cloud_history_entries || 0)});
    case "plan_created": return planLine(e);
    case "provider_health": return `${T("events.providerHealth")} · ${(e.source_provider || "").toUpperCase()}`;
    case "sync_run_started": return T("events.title.syncRunStarted");
    case "sync_run_finished": return T("events.title.syncRunFinished");
    case "scrobble_started": return T("events.title.startedWatching");
    case "scrobble_playback": return T("events.title.playbackSession");
    case "scrobble_paused": return T("events.title.playbackPaused");
    case "scrobble_resumed": return T("events.title.playbackResumed");
    case "scrobble_stopped": return T("events.title.playbackStopped");
    case "scrobble_completed": return T("events.badge.watched");
    case "scrobble_failed": return `${T("events.badge.scrobbleFailed")}${e.reason_code ? ` · ${e.reason_code}` : ""}`;
    case "scrobble_skipped": return `${T("events.badge.scrobbleSkipped")}${e.reason_code ? ` · ${e.reason_code}` : ""}`;
    case "rating_applied": return T("events.title.ratingForwarded");
    case "rating_skipped": return `${T("events.badge.ratingSkipped")}${e.reason_code ? ` · ${reasonLabel(e.reason_code, e.reason_label)}` : ""}`;
    case "rating_failed": return `${T("events.badge.ratingFailed")}${e.reason_code ? ` · ${e.reason_code}` : ""}`;
    default: {
      if (String(e.event_type || "").startsWith("audit_")) {
        const d = detailOf(e);
        return d.message || BADGE[e.event_type] || String(e.event_type || "").replace(/_/g, " ");
      }
      return String(e.event_type || "").replace(/_/g, " ");
    }
  }
};

const EVENT_TYPES = [
  "", "write_succeeded", "write_attempted", "write_failed", "unresolved_recorded",
  "cloud_history_read",
  "blackbox_promoted", "blackbox_blocked", "tombstone_created", "plan_created",
  "provider_health", "sync_run_started", "sync_run_finished",
];
const SCROBBLE_TYPES = ["", "scrobble_playback", "scrobble_started", "scrobble_completed", "scrobble_skipped", "scrobble_failed", "rating_applied", "rating_failed", "rating_skipped"];
const CAPTURE_TYPES = ["", "capture_completed", "capture_failed", "capture_cleanup"];
const BACKUP_TYPES = ["", "backup_completed", "backup_failed", "backup_uploaded", "backup_upload_failed", "backup_cleanup"];
const JOB_DOMAINS = ["capture", "backup"];
const CAPTURE_TRIGGERS = { scheduler: T("events.trigger.schedule"), manual: T("events.trigger.manual"), pre_restore: T("events.trigger.preRestore"), upgrade: T("events.trigger.upgrade"), maintenance_state_compact: T("events.trigger.compact"), maintenance_state_prune: T("events.trigger.prune") };
const AUDIT_TYPES = ["", "audit_login", "audit_login_failed", "audit_login_blocked", "audit_2fa_required", "audit_2fa_failed", "audit_logout", "audit_logout_all", "audit_logout_others", "audit_user_created", "audit_user_updated", "audit_user_deleted", "audit_credentials_updated", "audit_totp_setup", "audit_totp_enabled", "audit_totp_disabled", "audit_plex_sso_linked", "audit_plex_sso_unlinked", "audit_api_action"];
const OUTCOME_ITEMS = {
  sync: [{ value: "", label: T("events.outcome.any") }, { value: "successful", label: T("events.outcome.successful") }, { value: "problems", label: T("events.outcome.problems") }, { value: "informational", label: T("events.outcome.informational") }],
  scrobble: [{ value: "", label: T("events.outcome.any") }, { value: "completed", label: T("events.badge.watched") }, { value: "failed", label: T("events.outcome.failed") }, { value: "rated", label: T("events.badge.rated") }, { value: "running", label: T("events.outcome.inProgress") }, { value: "paused", label: T("events.badge.paused") }, { value: "stopped", label: T("events.outcome.stoppedWatching") }, { value: "skipped", label: T("events.outcome.skipped") }],
  audit: [{ value: "", label: T("events.outcome.any") }, { value: "completed", label: T("events.outcome.successful") }, { value: "failed", label: T("events.outcome.failed") }, { value: "informational", label: T("events.outcome.informational") }],
  capture: [{ value: "", label: T("events.outcome.any") }, { value: "completed", label: T("events.outcome.successful") }, { value: "failed", label: T("events.outcome.failed") }],
  backup: [{ value: "", label: T("events.outcome.any") }, { value: "completed", label: T("events.outcome.successful") }, { value: "failed", label: T("events.outcome.failed") }],
};
const typeItems = (dom) => (dom === "audit" ? AUDIT_TYPES : dom === "capture" ? CAPTURE_TYPES : dom === "backup" ? BACKUP_TYPES : (dom === "scrobble" ? SCROBBLE_TYPES : EVENT_TYPES)).map((t) => ({ value: t, label: t ? BADGE[t] || t.replace(/_/g, " ") : T("events.allTypes") }));

// group helpers
const GROUP_SEV = { success: "ok", warning: "warn", error: "error", info: "info" };
const groupSev = (g) => {
  if (String(g.status || "").toLowerCase() === "blackboxed") return "error";
  return GROUP_SEV[String(g.severity || "").toLowerCase()] || "info";
};
const STATUS_ICON = {
  completed: "task_alt", resolved: "check_circle", blackboxed: "inventory_2", unresolved: "shield",
  paused: "pause", stopped: "stop", skipped: "block", failed: "cancel", running: "sync", pending: "hourglass_top", informational: "bolt", rated: "star",
  warning: "warning",
};
const groupIcon = (g) => STATUS_ICON[String(g.status || "").toLowerCase()] || "bolt";
const STATUS_LABEL = { paused: T("events.badge.paused"), stopped: T("events.outcome.stoppedWatching"), running: T("events.outcome.inProgress"), warning: T("events.status.issues") };
const statusLabel = (s, domain) => { if (domain === "scrobble" && s === "completed") return T("events.badge.watched"); const k = String(s || "informational").toLowerCase(); if (STATUS_LABEL[k]) return STATUS_LABEL[k]; const key = `events.status.${k}`; const label = /^[a-z_]+$/.test(k) ? T(key) : key; return label !== key ? label : (k.charAt(0).toUpperCase() + k.slice(1)); };
const modeLabel = (m) => {
  const s = String(m || "").toLowerCase();
  if (s.includes("two")) return T("events.twoWay");
  if (s.includes("one")) return T("events.oneWay");
  return m ? String(m) : "";
};

function injectCSS() {
  if (document.getElementById("cw-events-css")) return Promise.resolve();
  const link = document.createElement("link");
  const url = new URL("./styles.css", import.meta.url);
  const v = new URL(import.meta.url).searchParams.get("v") || window.__CW_VERSION__;
  if (v) url.searchParams.set("v", v);
  link.id = "cw-events-css";
  link.rel = "stylesheet";
  link.href = url.href;
  return new Promise((res) => {
    link.addEventListener("load", res, { once: true });
    link.addEventListener("error", res, { once: true });
    document.head.appendChild(link);
  });
}

function createDropdown({ label, items, value, onChange, align = "auto" }) {
  let opts = items.slice();
  let cur = value ?? (opts[0]?.value ?? "");
  const wrap = document.createElement("div");
  wrap.className = "ev-dd";
  const btn = document.createElement("button");
  btn.type = "button";
  btn.className = "ev-dd-btn";
  wrap.appendChild(btn);

  const labelFor = (v) => opts.find((i) => i.value === v)?.label ?? label;
  const render = () => {
    btn.title = labelFor(cur);
    btn.innerHTML = `<span class="ev-dd-text">${esc(labelFor(cur))}</span><span class="material-symbols-rounded ev-dd-caret" aria-hidden="true">expand_more</span>`;
  };
  render();

  let menu = null;
  const position = () => {
    if (!menu) return;
    const r = btn.getBoundingClientRect();
    menu.style.position = "fixed";
    const availableWidth = Math.max(0, window.innerWidth - 16);
    menu.style.minWidth = `${Math.min(availableWidth, Math.max(r.width, 160))}px`;
    menu.style.width = "max-content";
    menu.style.maxWidth = `${availableWidth}px`;
    menu.style.maxHeight = "260px";
    menu.style.visibility = "hidden";
    menu.style.left = "0px";
    menu.style.top = "0px";
    const mw = menu.offsetWidth;
    const mh = menu.offsetHeight;
    let left = align === "right" ? r.right - mw : r.left;
    if (left + mw > window.innerWidth - 8) left = r.right - mw;
    left = Math.max(8, Math.min(left, window.innerWidth - mw - 8));
    let top = r.bottom + 4;
    if (top + mh > window.innerHeight - 8) {
      const up = r.top - 4 - mh;
      top = up > 8 ? up : Math.max(8, window.innerHeight - mh - 8);
    }
    menu.style.left = `${left}px`;
    menu.style.top = `${top}px`;
    menu.style.visibility = "visible";
  };
  const onDocDown = (e) => {
    if (!menu) return;
    if (btn.contains(e.target) || menu.contains(e.target)) return;
    close();
  };
  const onKey = (e) => { if (e.key === "Escape") { e.stopPropagation(); close(); } };
  const close = () => {
    if (!menu) return;
    menu.remove();
    menu = null;
    btn.setAttribute("aria-expanded", "false");
    document.removeEventListener("pointerdown", onDocDown, true);
    document.removeEventListener("keydown", onKey, true);
    window.removeEventListener("resize", close);
    window.removeEventListener("scroll", close, true);
  };
  const hasSearch = () => opts.length > 8;
  const renderItems = (filter = "") => {
    const f = filter.trim().toLowerCase();
    const list = f ? opts.filter((i) => String(i.label).toLowerCase().includes(f)) : opts;
    const body = list.map((i) => `<button type="button" class="ev-dd-item${i.value === cur ? " sel" : ""}" data-v="${esc(i.value)}"><span class="ev-dd-item-main">${esc(i.label)}</span>${i.hint ? `<span class="ev-dd-item-hint">${esc(i.hint)}</span>` : ""}${i.value === cur ? `<span class="material-symbols-rounded ev-dd-check" aria-hidden="true">check</span>` : ""}</button>`).join("");
    return body || `<div class="ev-dd-empty">${TH("events.noMatches")}</div>`;
  };
  const bindItems = () => {
    menu.querySelectorAll(".ev-dd-item").forEach((el) => el.addEventListener("click", () => {
      cur = el.dataset.v;
      render();
      close();
      onChange?.(cur);
    }));
  };
  const open = () => {
    if (menu) { close(); return; }
    menu = document.createElement("div");
    menu.className = "ev-dd-menu cw-scrollbars";
    const search = hasSearch()
      ? `<label class="ev-dd-search"><span class="material-symbols-rounded" aria-hidden="true">search</span><input type="text" placeholder="${TH("events.searchShort")}"></label>` : "";
    menu.innerHTML = `${search}<div class="ev-dd-body cw-scrollbars">${renderItems()}</div>`;
    document.body.appendChild(menu);
    btn.setAttribute("aria-expanded", "true");
    const bodyEl = menu.querySelector(".ev-dd-body");
    const searchEl = menu.querySelector(".ev-dd-search input");
    bindItems();
    searchEl?.addEventListener("input", () => { bodyEl.innerHTML = renderItems(searchEl.value); bindItems(); position(); });
    position();
    searchEl?.focus();
    document.addEventListener("pointerdown", onDocDown, true);
    document.addEventListener("keydown", onKey, true);
    window.addEventListener("resize", close);
    window.addEventListener("scroll", close, true);
  };
  btn.addEventListener("click", (e) => { e.stopPropagation(); open(); });

  return {
    el: wrap,
    get value() { return cur; },
    set value(v) { cur = v; render(); },
    reset() { cur = opts[0]?.value ?? ""; render(); },
    setItems(next) { opts = next.slice(); if (!opts.some((i) => i.value === cur)) cur = opts[0]?.value ?? ""; render(); },
    labelOf(v) { return opts.find((i) => i.value === v)?.label ?? String(v ?? ""); },
    get items() { return opts.map((i) => i.value).filter(Boolean); },
    close,
  };
}

function createSegmented({ items, value, onChange, cls = "" }) {
  const wrap = document.createElement("div");
  wrap.className = `ev-seg ${cls}`.trim();
  wrap.setAttribute("role", "tablist");
  let cur = value ?? items[0]?.value;
  const render = () => {
    wrap.innerHTML = items.map((i) => `<button type="button" role="tab" class="ev-seg-btn${i.value === cur ? " on" : ""}" data-v="${esc(i.value)}" aria-selected="${i.value === cur}">${i.icon ? `<span class="material-symbols-rounded" aria-hidden="true">${i.icon}</span>` : ""}${esc(i.label)}</button>`).join("");
    wrap.querySelectorAll(".ev-seg-btn").forEach((b) => b.addEventListener("click", () => {
      if (b.dataset.v === cur) return;
      cur = b.dataset.v; render(); onChange?.(cur);
    }));
  };
  render();
  return { el: wrap, get value() { return cur; }, set value(v) { cur = v; render(); } };
}

let cleanup = null;
let hideView = null;
let showView = null;
let mountGeneration = 0;
const requestJSON = fjson;

export default {
  async mount(root, props = {}) {
    cleanup?.();
    const generation = ++mountGeneration;
    const cssReady = injectCSS();
    root.classList.add("ev-page", "cw-page-events");
    let eventScope = null;
    const fjson = (url, options) => requestJSON(withEventScope(url, eventScope), options);
    const knownAuth = window.CW?.AuthState?.read?.();
    let isAdmin = knownAuth?.isAdmin === true;
    if (typeof knownAuth?.isAdmin !== "boolean") {
      try {
        const auth = await fjson("/api/app-auth/status", { cache: "no-store" });
        isAdmin = !auth?.enabled || !!auth?.is_admin;
      } catch {}
    }
    await cssReady;
    if (generation !== mountGeneration) return;
    PROFILE_LABELS = {};

    const ls = (k, d) => { try { return localStorage.getItem(k) || d; } catch { return d; } };
    const lset = (k, v) => { try { localStorage.setItem(k, v); } catch {} };
    const initialGroupId = String(props?.groupId || props?.eventGroupId || "").trim();
    const initialDomain = String(props?.domain || "").trim().toLowerCase();
    const initialVisibility = String(props?.visibility || "").trim().toLowerCase();
    const initialMode = String(props?.mode || "").trim().toLowerCase();
    const back = pageBackLink(props?.returnTo, location.href, 'events');
    let runFilter = String(props?.runId || props?.run_id || "").trim();
    let visibility = ["open", "acknowledged", "all"].includes(initialVisibility) ? initialVisibility : ls("cw.events.visibility", "all");
    if (runFilter || initialGroupId) visibility = ["open", "acknowledged", "all"].includes(initialVisibility) ? initialVisibility : "all";
    let order = ls("cw.events.order", "newest");
    const MODES = { sync: ["titles", "runs", "raw"], scrobble: ["titles", "raw"] };
    const MODE_ITEMS = { titles: { label: T("events.view.titles"), icon: "movie" }, runs: { label: T("events.view.runs"), icon: "sync" }, grouped: { label: T("events.view.grouped"), icon: "account_tree" }, raw: { label: T("events.view.raw"), icon: "list" } };
    const modesFor = (d) => MODES[d] || ["grouped", "raw"];
    const modeFor = (d, first = false) => {
      if (first && (initialGroupId || initialMode === "grouped")) return "grouped";
      const want = first ? initialMode : "";
      if (want && modesFor(d).includes(want)) return want;
      const saved = ls(`cw.events.mode.${d}`, "");
      return modesFor(d).includes(saved) ? saved : modesFor(d)[0];
    };
    let mode = "grouped";
    const grouped = () => mode === "grouped";
    let domain = ["sync", "scrobble", "audit", ...JOB_DOMAINS].includes(initialDomain) ? initialDomain : ls("cw.events.domain", "sync");
    let detailTab = ls("cw.events.detailtab", "details");
    lset("cw.events.order", order);

    let quick = "all";
    let selectedDay = null;
    const openQuietRuns = new Set();
    let titleTab = ls("cw.events.titletab", "path");
    let tmdbOn = false;
    const checked = new Set();
    const state = { items: [], page: 0, total: 0, selected: null, selectedPair: "", relExpanded: false, collapsed: new Set(), childLoading: new Set(), didInitialSelect: false, runs: [], preparing: false };
    const dropdowns = [];

    root.innerHTML = `
      <div class="ev-app">
        <a class="ev-back" href="${esc(back.href)}" id="ev-back"><span class="material-symbols-rounded" aria-hidden="true">arrow_back</span>${esc(back.label)}</a>
        <header class="ev-header">
          <div>
            <h1>${TH("events.pageTitle")}</h1>
            <p>${TH("events.subtitle")}</p>
          </div>
        </header>

        <div class="ev-tabsbar">
          <label class="ev-mobile-view" for="ev-mobile-view"><span>${TH("events.view.label")}</span><select id="ev-mobile-view" aria-label="${TH("events.view.eventView")}"></select></label>
          <div class="ev-viewtabs" id="ev-viewtabs" role="tablist"></div>
          <div class="ev-statsrange" id="ev-statsrange" hidden></div>
        </div>

        <div class="ev-toolbar">
          <div class="ev-toolbar-row">
            <div class="ev-search" id="ev-searchbar">
              <span class="material-symbols-rounded" aria-hidden="true">search</span>
              <span class="ev-mf-summary" id="ev-mf-summary" hidden></span>
              <input id="ev-q" type="search" aria-label="${TH("events.searchEvents")}" placeholder="${TH("events.searchPlaceholder")}" autocomplete="off">
              <button class="ev-filter-btn" id="ev-more" type="button" aria-label="${TH("events.filters")}" aria-expanded="false" aria-controls="ev-morefilters" title="${TH("events.filters")}"><span class="material-symbols-rounded" aria-hidden="true">tune</span><span class="ev-more-count" id="ev-more-dot" hidden></span></button>
            </div>
            <button class="ev-refresh" id="ev-refresh" type="button" aria-label="${TH("events.refreshEvents")}" title="${TH("events.refresh")}"><span class="material-symbols-rounded" aria-hidden="true">refresh</span></button>
          </div>
          <div class="ev-quick" id="ev-quick" role="group" aria-label="${TH("events.quickViews")}"></div>
          <div class="ev-morefilters" id="ev-morefilters" role="dialog" aria-label="${TH("events.filters")}" hidden>
            <div class="ev-mf-grid">
              <div class="ev-mf-field" data-f="range"><span class="ev-mf-label">${TH("events.f.when")}</span><span id="ev-range"></span>
                <label class="ev-date" id="ev-since-wrap" hidden><span class="material-symbols-rounded" aria-hidden="true">event</span><input id="ev-since" type="date" aria-label="${TH("events.f.since")}"></label>
                <label class="ev-date" id="ev-until-wrap" hidden><span class="material-symbols-rounded" aria-hidden="true">event</span><input id="ev-until" type="date" aria-label="${TH("events.f.until")}"></label>
              </div>
              <div class="ev-mf-field" data-f="outcome"><span class="ev-mf-label">${TH("events.f.outcome")}</span><span id="ev-cat"></span></div>
              <div class="ev-mf-field" data-f="review"><span class="ev-mf-label">${TH("events.f.review")}</span><span id="ev-review"></span></div>
              <div class="ev-mf-field" data-f="type"><span class="ev-mf-label">${TH("events.f.type")}</span><span id="ev-f-type"></span></div>
              <div class="ev-mf-field" data-f="provider"><span class="ev-mf-label" id="ev-f-provider-label">${TH("events.f.provider")}</span><span id="ev-f-provider"></span></div>
              <div class="ev-mf-field" data-f="to"><span class="ev-mf-label">${TH("events.f.to")}</span><span id="ev-f-to"></span></div>
              <div class="ev-mf-field" data-f="origin"><span class="ev-mf-label">${TH("events.f.origin")}</span><span id="ev-f-origin"></span></div>
              <div class="ev-mf-field" data-f="feature"><span class="ev-mf-label">${TH("events.f.feature")}</span><span id="ev-f-feature"></span></div>
              <div class="ev-mf-field" data-f="pair"><span class="ev-mf-label">${TH("events.f.pair")}</span><span id="ev-f-pair"></span></div>
              <div class="ev-mf-field" data-f="profile"><span class="ev-mf-label">${TH("events.f.profile")}</span><span id="ev-f-profile"></span></div>
            </div>
            <div class="ev-mf-foot">
              <button type="button" class="ev-linkbtn" id="ev-mf-reset">${TH("events.clearAll")}</button>
              <button type="button" class="ev-tbtn ev-mf-done" id="ev-mf-done">${TH("events.done")}</button>
              <span class="ev-mf-tip" id="ev-mf-tip">${TH("events.tip")}</span>
              ${isAdmin ? `<button class="ev-tbtn" id="ev-clear" type="button" title="${TH("events.clearArchiveHint")}"><span class="material-symbols-rounded" aria-hidden="true">delete_sweep</span><span>${TH("events.clearArchive")}</span></button>` : ``}
            </div>
          </div>
        </div>
        <div id="ev-activity-host"></div>

        <div class="ev-layout">
          <div class="ev-col-list">
            <div class="ev-list-head">
              <div class="ev-list-head-left"><div class="ev-count" id="ev-count">—</div><span id="ev-mode"></span></div>
              <div class="ev-list-head-right">
                <span id="ev-sort"></span>
                <div class="ev-pagemini">
                  <button class="ev-pg" id="ev-prev-top" type="button" aria-label="${TH("events.previousPage")}"><span class="material-symbols-rounded" aria-hidden="true">chevron_left</span></button>
                  <span class="ev-pagemini-label" id="ev-pagelabel">${TH("events.page1")}</span>
                  <button class="ev-pg" id="ev-next-top" type="button" aria-label="${TH("events.nextPage")}"><span class="material-symbols-rounded" aria-hidden="true">chevron_right</span></button>
                </div>
              </div>
            </div>
            <div class="ev-bulk"><label><input id="ev-select-page" type="checkbox"> ${TH("events.selectPage")}</label><span id="ev-selection-count"></span><button class="ev-tbtn" id="ev-review-selected" type="button" disabled>${TH("events.markReviewed")}</button></div>
            <div class="ev-list" id="ev-list"><div class="ev-empty">${TH("events.loading")}</div></div>
            <div class="ev-list-foot" id="ev-foot"></div>
          </div>
          <div class="ev-split" id="ev-split" role="separator" aria-orientation="vertical" title="${TH("events.dragResize")}"></div>
          <div class="ev-col-detail"><button class="ev-tbtn ev-detail-back" id="ev-detail-back" type="button"><span class="material-symbols-rounded" aria-hidden="true">arrow_back</span>${TH("events.backToEvents")}</button><div class="ev-detail" id="ev-detail"><div class="ev-empty ev-empty-detail"><span class="material-symbols-rounded" aria-hidden="true">touch_app</span><div>${TH("events.selectActivityHint")}</div></div></div></div>
        </div>

        <div class="ev-stats" id="ev-stats" hidden></div>

        <div class="ev-toast" id="ev-toast" hidden></div>
      </div>`;

    const listEl = Q("#ev-list", root);
    const detailEl = Q("#ev-detail", root);
    const footEl = Q("#ev-foot", root);
    const quickEl = Q("#ev-quick", root);
    const activityHost = Q("#ev-activity-host", root);
    const toastEl = Q("#ev-toast", root);
    const qEl = Q("#ev-q", root);
    const countEl = Q("#ev-count", root);
    const sinceEl = Q("#ev-since", root);
    const untilEl = Q("#ev-until", root);
    const sinceWrap = Q("#ev-since-wrap", root);
    const untilWrap = Q("#ev-until-wrap", root);
    const moreWrap = Q("#ev-morefilters", root);
    const moreBtn = Q("#ev-more", root);
    const moreDotEl = Q("#ev-more-dot", root);
    const summaryEl = Q("#ev-mf-summary", root);
    const pageLabelEl = Q("#ev-pagelabel", root);
    const layoutEl = Q(".ev-layout", root);
    const splitEl = Q("#ev-split", root);

    let alive = true;
    let selectionSequence = 0;
    let loadSequence = 0;
    let scrollPosition = null;
    let listReturnPosition = null;
    let detailLayer = null;
    const closeMobileDetail = () => {
      ++selectionSequence;
      const layer = detailLayer;
      detailLayer = null;
      void layer?.dismiss();
      layoutEl.classList.remove('show-detail');
      if (listReturnPosition !== null) window.scrollTo({top:listReturnPosition, behavior:'instant'});
      listEl.querySelector('.ev-row.sel, .ev-trow.sel, .ev-rrow.sel')?.focus({preventScroll:true});
    };
    const pageVisible = () => document.documentElement.dataset.tab === "events";
    const revealSection = element => {
      if (!pageVisible()) return;
      const top = element.getBoundingClientRect().top;
      const header = document.querySelector("body > header");
      const inset = Math.max(0, header?.getBoundingClientRect().bottom || 0) + 16;
      if (top < inset || top > window.innerHeight - 120) {
        window.scrollTo({ top: Math.max(0, window.scrollY + top - inset), behavior: "instant" });
      }
    };
    showView = () => {
      if (scrollPosition !== null && pageVisible()) window.scrollTo({ top: scrollPosition, behavior: "instant" });
    };
    Q("#ev-detail-back", root).addEventListener("click", closeMobileDetail);
    Q("#ev-back", root).addEventListener("click", async event => {
      if (event.ctrlKey || event.metaKey || event.shiftKey || event.altKey) return;
      event.preventDefault();
      await returnFromEvents(props);
    });

    // resizable list/detail split
    const savedSplit = ls("cw.events.split", "");
    if (savedSplit) layoutEl.style.setProperty("--ev-list-w", savedSplit);
    let splitDrag = false;
    const onSplitMove = (ev) => {
      if (!splitDrag) return;
      const r = layoutEl.getBoundingClientRect();
      if (r.width <= 0) return;
      let pct = ((ev.clientX - r.left) / r.width) * 100;
      pct = Math.max(30, Math.min(72, pct));
      layoutEl.style.setProperty("--ev-list-w", `${pct.toFixed(1)}%`);
    };
    const endSplit = () => {
      if (!splitDrag) return;
      splitDrag = false;
      splitEl.classList.remove("drag");
      document.body.style.userSelect = "";
      window.removeEventListener("pointermove", onSplitMove, true);
      window.removeEventListener("pointerup", endSplit, true);
      lset("cw.events.split", layoutEl.style.getPropertyValue("--ev-list-w"));
    };
    splitEl.addEventListener("pointerdown", (ev) => {
      splitDrag = true;
      splitEl.classList.add("drag");
      document.body.style.userSelect = "none";
      window.addEventListener("pointermove", onSplitMove, true);
      window.addEventListener("pointerup", endSplit, true);
      ev.preventDefault();
    });

    let dateRange = runFilter || initialGroupId ? "all" : ls("cw.events.range", "all");
    const ddRange = createDropdown({
      label: T("events.range.all"), value: dateRange,
      items: [
        { value: "all", label: T("events.range.all") },
        { value: "24h", label: T("events.range.24h") },
        { value: "7d", label: T("events.range.7d") },
        { value: "30d", label: T("events.range.30d") },
        { value: "custom", label: T("events.range.custom") },
      ],
      onChange: (v) => { dateRange = v; lset("cw.events.range", v); syncRangeUI(); load(0); },
    });
    const ddType = createDropdown({ label: T("events.allTypes"), items: typeItems(domain), onChange: () => load(0) });
    const ddProvider = createDropdown({ label: T("events.anyProvider"), items: [{ value: "", label: T("events.anyProvider") }], onChange: () => load(0) });
    const ddOrigin = createDropdown({ label: T("events.anyOrigin"), items: [{ value: "", label: T("events.anyOrigin") }], onChange: () => load(0) });
    const ddFeature = createDropdown({ label: T("events.anyFeature"), items: [{ value: "", label: T("events.anyFeature") }, ...["history", "ratings", "watchlist", "progress", "collection"].map((f) => ({ value: f, label: FEATURE_LABEL[f] }))], onChange: () => load(0) });
    const ddPair = createDropdown({ label: T("events.anyPair"), items: [{ value: "", label: T("events.anyPair") }], onChange: () => load(0) });
    const overviewScopes = [{ value: "", label: T("events.allProfiles") }, { value: "__default__", label: T("events.admin") }];
    const ddProfile = createDropdown({
      label: T("events.allProfiles"),
      items: overviewScopes,
      onChange: (v) => {
        eventScope = v;
        clearDetail();
        state.didInitialSelect = false;
        if (view === "statistics") loadStats();
        else load(0);
      },
    });
    const ddCategory = createDropdown({
      label: T("events.outcome.any"), align: "right",
      items: OUTCOME_ITEMS[domain] || OUTCOME_ITEMS.sync,
      onChange: () => load(0),
    });
    Q("#ev-cat", root).appendChild(ddCategory.el);
    Q("#ev-range", root).appendChild(ddRange.el);
    if (isAdmin) {
      eventScope = String(window.CW?.OverviewProfile?.id || "").trim();
      const activeLabel = String(window.CW?.OverviewProfile?.label || "").trim();
      if (eventScope && activeLabel && !overviewScopes.some((row) => row.value === eventScope)) {
        ddProfile.setItems([...overviewScopes, { value: eventScope, label: activeLabel }]);
      }
      ddProfile.value = eventScope;
      (async () => {
        try {
          const data = await fjson("/api/user-profiles", { cache: "no-store" });
          if (!alive) return;
          const rows = (Array.isArray(data?.items) ? data.items : [])
            .map((row) => ({ value: String(row?.id || "").trim(), label: String(row?.label || row?.id || "").trim() }))
            .filter((row) => row.value && row.label);
          const items = [...overviewScopes, ...rows];
          ddProfile.setItems(items);
          if (eventScope && !items.some((row) => row.value === eventScope)) {
            eventScope = "";
            ddProfile.value = "";
            clearDetail();
            state.didInitialSelect = false;
            if (view === "statistics") loadStats();
            else load(0);
          } else {
            ddProfile.value = eventScope;
          }
          updateHidden();
        } catch {}
      })();
    }

    const ddTo = createDropdown({ label: T("events.anyDestination"), items: [{ value: "", label: T("events.anyDestination") }], onChange: () => load(0) });
    Q("#ev-f-type", root).appendChild(ddType.el);
    Q("#ev-f-provider", root).appendChild(ddProvider.el);
    Q("#ev-f-to", root).appendChild(ddTo.el);
    Q("#ev-f-origin", root).appendChild(ddOrigin.el);
    Q("#ev-f-feature", root).appendChild(ddFeature.el);
    Q("#ev-f-pair", root).appendChild(ddPair.el);
    if (isAdmin) Q("#ev-f-profile", root).appendChild(ddProfile.el);
    const hiddenDds = [ddType, ddProvider, ddTo, ddOrigin, ddFeature, ddPair, ...(isAdmin ? [ddProfile] : [])];
    dropdowns.push(ddCategory, ddRange, ...hiddenDds);

    const ddSort = createDropdown({
      label: T("events.newestFirst"), value: order,
      items: [{ value: "newest", label: T("events.newestFirst") }, { value: "oldest", label: T("events.oldestFirst") }],
      onChange: (v) => { order = v; lset("cw.events.order", v); load(0); },
      align: "right",
    });
    Q("#ev-sort", root).appendChild(ddSort.el);

    const seg = createSegmented({
      items: [{ value: "all", label: T("events.anyReview") }, { value: "open", label: T("events.unreviewed") }, { value: "acknowledged", label: T("events.reviewed") }],
      value: visibility,
      onChange: (v) => { visibility = v; lset("cw.events.visibility", v); load(0); },
    });
    Q("#ev-review", root).appendChild(seg.el);

    const QUICK = {
      titles: [["all", T("events.quick.everything")], ["attention", T("events.quick.attention")], ["watched", T("events.badge.watched")], ["ratings", T("events.feature.ratings")], ["watchlist", T("events.feature.watchlist")], ["collection", T("events.feature.collections")], ["removals", T("events.quick.removals")]],
      grouped: [["all", T("events.quick.allActivity")], ["attention", T("events.quick.attention")]],
    };
    const needsAttention = () => quick === "attention";
    const renderQuick = () => {
      const items = (domain === "scrobble" && mode === "titles" ? QUICK.titles.filter(([v]) => ["all", "attention", "watched", "ratings"].includes(v)) : QUICK[mode]) || [];
      quickEl.hidden = !items.length;
      const attn = titles() ? Number(state.attentionTotal || 0) : 0;
      quickEl.innerHTML = items.map(([v, l]) => `<button type="button" class="ev-qchip${v === quick ? " on" : ""}" data-q="${esc(v)}" aria-pressed="${v === quick}">${v === "attention" && attn ? `<span class="ev-qdot" aria-hidden="true"></span>` : ""}${esc(l)}${v === "attention" && attn ? `<span class="ev-qcount">${esc(attn.toLocaleString())}</span>` : ""}</button>`).join("")
        + (titles() && quick === "attention" ? `<button type="button" class="ev-tbtn ev-qreview" id="ev-review-all" hidden><span class="material-symbols-rounded" aria-hidden="true">done_all</span><span>${TH("events.markAllReviewed")}</span></button>` : "");
      Q("#ev-review-all", quickEl)?.addEventListener("click", () => reviewAll());
      quickEl.querySelectorAll(".ev-qchip").forEach((b) => b.addEventListener("click", () => {
        if (b.dataset.q === quick) return;
        quick = b.dataset.q;
        renderQuick();
        load(0);
      }));
    };

    const titles = () => mode === "titles";
    const runsMode = () => mode === "runs";
    const TITLE_OUTCOMES = [{ value: "", label: T("events.outcome.any") }, { value: "done", label: T("events.done") }, { value: "failed", label: T("events.outcome.failed") }, { value: "skipped", label: T("events.outcome.skipped") }, { value: "already", label: T("events.outcome.already") }, { value: "pending", label: T("events.outcome.inProgress") }];
    const setField = (name, on) => { const f = Q(`.ev-mf-field[data-f="${name}"]`, root); if (f) f.hidden = !on; };
    const applyFieldUI = () => {
      const audit = domain === "audit";
      const capture = JOB_DOMAINS.includes(domain);
      const backup = domain === "backup";
      const sync = domain === "sync";
      const t = titles(), r = runsMode(), g = grouped(), raw = mode === "raw";
      ddCategory.setItems(t ? TITLE_OUTCOMES : (OUTCOME_ITEMS[domain] || OUTCOME_ITEMS.sync));
      ddType.setItems(typeItems(domain));
      if (!sync || t || r) { ddFeature.reset(); ddPair.reset(); }
      if (audit || backup || r) ddProvider.reset();
      if (audit || capture || t || r) ddOrigin.reset();
      if (!t) ddTo.reset();
      if (t || r) ddType.reset();
      setField("range", !r);
      setField("outcome", t || g);
      setField("review", !r);
      setField("type", g || raw);
      setField("provider", !r && !audit && !backup);
      setField("to", t);
      setField("origin", (g || raw) && !audit && !capture);
      setField("feature", (g || raw) && sync);
      setField("pair", (g || raw) && sync);
      setField("profile", isAdmin && !audit);
      Q("#ev-f-provider-label", root).textContent = t ? T("events.f.from") : T("events.f.provider");
      Q("#ev-mf-tip", root).hidden = !t;
      Q(".ev-bulk", root).hidden = t || r;
      Q("#ev-sort", root).hidden = r;
      qEl.placeholder = r ? T("events.filterRuns") : t ? T("events.searchPlaceholder") : T("events.searchItems");
    };

    const modeHost = Q("#ev-mode", root);
    const renderModeSeg = () => {
      const items = [...modesFor(domain)];
      if (mode === "grouped" && !items.includes("grouped")) items.splice(1, 0, "grouped");
      const segm = createSegmented({ cls: "ev-seg-mode", items: items.map((v) => ({ value: v, ...MODE_ITEMS[v] })), value: mode, onChange: (v) => setMode(v) });
      modeHost.replaceChildren(segm.el);
    };

    const canAudit = isAdmin && !String(window.CW?.OverviewProfile?.id || "").trim();
    let view = initialDomain && ["sync", "scrobble", "audit", ...JOB_DOMAINS].includes(initialDomain) ? initialDomain : ls("cw.events.view", domain === "scrobble" ? "scrobble" : "sync");
    if (view === "backup" && !isAdmin) view = "sync";
    if (view === "audit" && !canAudit) view = "sync";
    if (view === "sync" || view === "scrobble" || view === "audit" || JOB_DOMAINS.includes(view)) domain = view;
    let statsRange = ls("cw.events.stats.range", "30d");

    const toolbarEl = Q(".ev-toolbar", root);
    const tabsRightEl = Q("#ev-statsrange", root);
    const statsEl = Q("#ev-stats", root);
    let statsView = null;
    let statsModule = null;

    let activity = null;
    let firstDomain = true;
    const applyActivityUI = () => {
      const show = domain === "sync" && view !== "statistics";
      activityHost.hidden = !show;
      if (show && !activity) {
        activity = createActivityCard(activityHost, {
          fetchJson: fjson,
          store: { get: (k, d) => ls(`cw.events.activity.${k}`, d), set: (k, v) => lset(`cw.events.activity.${k}`, v) },
          onPickDay: (index) => pickDay(index),
        });
        activity.setSelected(selectedDay);
        void activity.load();
      }
    };

    const applyModeUI = () => {
      renderModeSeg();
      renderQuick();
      applyFieldUI();
      applyActivityUI();
    };

    const applyDomainUI = () => {
      mode = modeFor(domain, firstDomain);
      firstDomain = false;
      quick = "all";
      applyModeUI();
    };

    const setMode = (v) => {
      mode = v;
      quick = "all";
      if (MODES[domain] && v !== "grouped") lset(`cw.events.mode.${domain}`, v);
      state.selected = null;
      state.items = [];
      clearDetail();
      applyModeUI();
      load(0);
    };

    const pickDay = (index) => {
      selectedDay = index;
      activity?.setSelected(index);
      if (index != null) { dateRange = "all"; ddRange.value = "all"; syncRangeUI(); }
      load(0);
    };

    const VIEW_TABS = [
      { value: "sync", label: T("events.tabs.sync"), icon: "sync" },
      { value: "scrobble", label: T("events.tabs.scrobble"), icon: "play_circle" },
      { value: "capture", label: T("events.tabs.captures"), icon: "photo_camera" },
      ...(isAdmin ? [{ value: "backup", label: T("events.tabs.backups"), icon: "backup" }] : []),
      ...(canAudit ? [{ value: "audit", label: T("events.tabs.audits"), icon: "admin_panel_settings" }] : []),
      { value: "statistics", label: T("events.tabs.statistics"), icon: "bar_chart" },
    ];
    const RANGE_TABS = [["24h", "24H"], ["7d", "7D"], ["30d", "30D"], ["90d", "90D"]];

    const renderViewTabs = () => {
      const el = Q("#ev-viewtabs", root);
      el.innerHTML = VIEW_TABS.map((t) => `<button type="button" role="tab" class="ev-vtab${t.value === view ? " on" : ""}" data-v="${t.value}" aria-selected="${t.value === view}"><span class="material-symbols-rounded" aria-hidden="true">${t.icon}</span>${t.label}</button>`).join("");
      el.querySelectorAll(".ev-vtab").forEach((b) => b.addEventListener("click", () => setView(b.dataset.v)));
      const mobileSelect = Q("#ev-mobile-view", root);
      mobileSelect.innerHTML = VIEW_TABS.map(t => `<option value="${t.value}">${t.label}</option>`).join("");
      mobileSelect.value = view;
      mobileSelect.onchange = () => setView(mobileSelect.value);
    };

    const renderStatsRange = () => {
      tabsRightEl.innerHTML = `<span class="ev-statsrange-label" id="ev-statsrange-label"></span><div class="ev-seg ev-seg-range">${RANGE_TABS.map(([v, l]) => `<button type="button" class="ev-seg-btn${v === statsRange ? " on" : ""}" data-r="${v}">${l}</button>`).join("")}</div><button class="ev-tbtn" id="ev-stats-refresh" type="button"><span class="material-symbols-rounded" aria-hidden="true">refresh</span><span>${TH("events.refresh")}</span></button>`;
      tabsRightEl.querySelectorAll(".ev-seg-btn").forEach((b) => b.addEventListener("click", () => {
        if (b.dataset.r === statsRange) return;
        statsRange = b.dataset.r; lset("cw.events.stats.range", statsRange); renderStatsRange(); placeProfileDd(); loadStats();
      }));
      Q("#ev-stats-refresh", root)?.addEventListener("click", () => loadStats());
    };

    const setStatsLabel = (d) => {
      const el = Q("#ev-statsrange-label", root);
      if (!el || !d || !d.range) return;
      const s = new Date(d.range.since * 1000), u = new Date(d.range.until * 1000);
      el.textContent = `${s.toLocaleDateString((window.CW?.I18n || window.__cwI18n)?.locale, { month: "short", day: "numeric" })} – ${u.toLocaleDateString((window.CW?.I18n || window.__cwI18n)?.locale, { month: "short", day: "numeric", year: "numeric" })}`;
    };

    let statsSequence = 0;
    const loadStats = async () => {
      const sequence = ++statsSequence;
      try {
        if (!statsView) {
          statsEl.innerHTML = '<div class="ev-empty" role="status">' + TH("events.loadingStats") + '</div>';
          const url = new URL('./stats.js', import.meta.url);
          url.searchParams.set('v', new URL(import.meta.url).searchParams.get('v') || window.APP_VERSION || '1');
          const module = await (statsModule ||= import(url.href).catch(error => { statsModule = null; throw error; }));
          if (!alive || sequence !== statsSequence || view !== "statistics") return;
          statsView = module.default(statsEl, { fetchJson: fjson });
        }
        const d = await statsView.load({ range: statsRange, force: true });
        if (alive && sequence === statsSequence && d) setStatsLabel(d);
      } catch {
        if (alive && sequence === statsSequence) statsEl.innerHTML = '<div class="ev-empty">' + TH("events.statsFailed") + '</div>';
      }
    };

    const placeProfileDd = () => {
      if (!isAdmin) return;
      const stats = view === "statistics";
      const host = stats ? tabsRightEl : Q("#ev-f-profile", root);
      if (ddProfile.el.parentElement !== host) host.insertBefore(ddProfile.el, host.firstChild);
      ddProfile.el.style.display = domain === "audit" && !stats ? "none" : "";
    };

    const applyView = () => {
      const stats = view === "statistics";
      toolbarEl.style.display = stats ? "none" : "";
      layoutEl.hidden = stats;
      applyActivityUI();
      statsEl.hidden = !stats;
      tabsRightEl.hidden = !stats;
      renderViewTabs();
      if (stats) { renderStatsRange(); loadStats(); }
      placeProfileDd();
    };

    const setView = (v) => {
      if (v === view) return;
      view = v; lset("cw.events.view", v);
      if (v === "sync" || v === "scrobble" || v === "audit" || JOB_DOMAINS.includes(v)) {
        domain = v; lset("cw.events.domain", v);
        state.selected = null; state.didInitialSelect = false; clearDetail();
        applyDomainUI();
        applyView();
        load(0);
      } else {
        applyView();
      }
    };

    applyDomainUI();

    const dateToEpoch = (v, end = false) => {
      if (!v) return "";
      const d = new Date(v + (end ? "T23:59:59" : "T00:00:00"));
      const n = Math.floor(d.getTime() / 1000);
      return Number.isFinite(n) ? String(n) : "";
    };

    const syncRangeUI = () => {
      const custom = dateRange === "custom";
      if (custom) { moreWrap.hidden = false; moreBtn.setAttribute("aria-expanded", "true"); moreBtn.classList.add("on"); }
      sinceWrap.hidden = !custom;
      untilWrap.hidden = !custom;
    };

    const rangeEpoch = () => {
      if (selectedDay != null) { const r = dayRange(selectedDay); return { since: String(r.since), until: String(r.until) }; }
      const nowSec = Math.floor(Date.now() / 1000);
      if (dateRange === "24h") return { since: String(nowSec - 86400), until: "" };
      if (dateRange === "7d") return { since: String(nowSec - 7 * 86400), until: "" };
      if (dateRange === "30d") return { since: String(nowSec - 30 * 86400), until: "" };
      if (dateRange === "custom") return { since: dateToEpoch(sinceEl.value, false), until: dateToEpoch(untilEl.value, true) };
      return { since: "", until: "" };
    };

    const RANGE_LABEL = { "24h": T("events.range.24h"), "7d": T("events.range.7d"), "30d": T("events.range.30d") };
    const customRangeLabel = () => {
      const s = sinceEl.value, u = untilEl.value;
      if (s && u) return `${s} → ${u}`;
      if (s) return T("events.range.from", {date: s});
      if (u) return T("events.range.until", {date: u});
      return T("events.range.custom");
    };

    const outcomeOn = () => (grouped() || titles()) && ddCategory.value;
    const hiddenActive = () => !!(visibility !== "all" || outcomeOn() || eventScope || runFilter || (selectedDay != null && !runsMode()) || ddType.value || ddProvider.value || ddTo.value || ddOrigin.value || ddFeature.value || ddPair.value || (dateRange && dateRange !== "all"));
    const filtersActive = () => quick !== "all" || !!(qEl.value.trim() || hiddenActive());

    const clearHidden = (k) => {
      const all = !k;
      if (all || k === "range") { dateRange = "all"; lset("cw.events.range", "all"); ddRange.value = "all"; sinceEl.value = ""; untilEl.value = ""; syncRangeUI(); }
      if (all || k === "review") { visibility = "all"; seg.value = "all"; lset("cw.events.visibility", "all"); }
      if (all || k === "outcome") ddCategory.reset();
      if (all || k === "type") ddType.reset();
      if (all || k === "provider") ddProvider.reset();
      if (all || k === "origin") ddOrigin.reset();
      if (all || k === "feature") ddFeature.reset();
      if (all || k === "pair") ddPair.reset();
      if (all || k === "run") runFilter = "";
      if (all || k === "day") { selectedDay = null; activity?.setSelected(null); }
      if (all || k === "to") ddTo.reset();
      if (isAdmin && (all || k === "profile")) { eventScope = ""; ddProfile.value = ""; }
      if (view === "statistics") loadStats();
      else load(0);
    };

    const updateHidden = () => {
      const chips = [];
      if (selectedDay != null && !runsMode()) chips.push({ k: "day", label: dayLabel(selectedDay) });
      if (visibility !== "all" && !runsMode()) chips.push({ k: "review", label: visibility === "open" ? T("events.unreviewed") : T("events.reviewed") });
      if (outcomeOn()) chips.push({ k: "outcome", label: ddCategory.labelOf(ddCategory.value) });
      if (runFilter) chips.push({ k: "run", label: runsMode() ? T("events.chip.runSelected") : T("events.chip.oneRun") });
      if (dateRange && dateRange !== "all") chips.push({ k: "range", label: dateRange === "custom" ? customRangeLabel() : RANGE_LABEL[dateRange] });
      if (ddType.value) chips.push({ k: "type", label: ddType.labelOf(ddType.value) });
      if (ddProvider.value) chips.push({ k: "provider", label: titles() ? T("events.chip.from", {value: ddProvider.labelOf(ddProvider.value)}) : ddProvider.labelOf(ddProvider.value) });
      if (ddTo.value) chips.push({ k: "to", label: T("events.chip.to", {value: ddTo.labelOf(ddTo.value)}) });
      if (ddOrigin.value) chips.push({ k: "origin", label: T("events.chip.origin", {value: ddOrigin.labelOf(ddOrigin.value)}) });
      if (ddFeature.value) chips.push({ k: "feature", label: ddFeature.labelOf(ddFeature.value) });
      if (ddPair.value) chips.push({ k: "pair", label: ddPair.labelOf(ddPair.value) });
      if (isAdmin && eventScope) chips.push({ k: "profile", label: ddProfile.labelOf(eventScope) });
      const active = chips.length > 0;
      moreDotEl.hidden = !active;
      moreDotEl.textContent = active ? String(chips.length) : "";
      moreBtn.classList.toggle("has-active", active);
      moreBtn.setAttribute("aria-label", active ? T("events.filtersActive", {count: chips.length}) : T("events.filters"));
      if (!active) { summaryEl.hidden = true; summaryEl.innerHTML = ""; return; }
      summaryEl.hidden = false;
      summaryEl.innerHTML = chips.map((c) => `<button type="button" class="ev-chip" data-k="${esc(c.k)}" aria-label="${TH("events.removeFilter", {label: c.label})}"><span>${esc(c.label)}</span><span class="material-symbols-rounded" aria-hidden="true">close</span></button>`).join("");
      summaryEl.querySelectorAll(".ev-chip[data-k]").forEach((b) => b.addEventListener("click", () => clearHidden(b.dataset.k)));
    };

    const buildQuery = (page) => {
      const p = new URLSearchParams();
      const q = qEl.value.trim();
      if (q) p.set("q", q);
      if (ddType.value) p.set("event_type", ddType.value);
      if (ddProvider.value) p.set("provider", ddProvider.value);
      if (ddOrigin.value) p.set("origin_provider", ddOrigin.value);
      if (ddFeature.value) p.set("feature", ddFeature.value);
      if (ddPair.value) p.set("pair_key", ddPair.value);
      if (runFilter) p.set("run_id", runFilter);
      if (grouped() && ddCategory.value) p.set((domain === "scrobble" || domain === "audit" || JOB_DOMAINS.includes(domain)) ? "status" : "category", ddCategory.value);
      p.set("domain", domain);
      if (needsAttention() && grouped()) p.set("category", "problems");
      if (grouped() && domain === "scrobble") p.set("activities", "true");
      const { since, until } = rangeEpoch();
      if (since) p.set("since", since);
      if (until) p.set("until", until);
      p.set("visibility", visibility);
      p.set("order", order);
      if (grouped() && !filtersActive()) p.set("children", "false");
      p.set("limit", String(PAGE_SIZE));
      p.set("offset", String((page || 0) * PAGE_SIZE));
      return p.toString();
    };

    const TITLE_PAGE_SIZE = 25;
    const pageSize = () => titles() ? TITLE_PAGE_SIZE : PAGE_SIZE;
    const pageCount = () => Math.max(1, Math.ceil((state.total || 0) / pageSize()));

    const titlesQuery = (page) => {
      const p = new URLSearchParams();
      const q = qEl.value.trim();
      if (q) p.set("q", q);
      p.set("domain", domain);
      if (ddCategory.value) p.set("outcome", ddCategory.value);
      if (visibility === "open") p.set("review", "unreviewed");
      else if (visibility === "acknowledged") p.set("review", "reviewed");
      if (quick === "attention") p.set("attention", "true");
      else if (quick !== "all") p.set("what", quick);
      if (ddProvider.value) p.set("source", ddProvider.value);
      if (ddTo.value) p.set("destination", ddTo.value);
      if (runFilter) p.set("run_id", runFilter);
      const { since, until } = rangeEpoch();
      if (since) p.set("since", since);
      if (until) p.set("until", until);
      p.set("order", order);
      p.set("limit", String(TITLE_PAGE_SIZE));
      p.set("offset", String((page || 0) * TITLE_PAGE_SIZE));
      return p.toString();
    };

    // rows
    const groupRowHTML = (g, opts = {}) => {
      const sv = groupSev(g);
      const feat = FEATURE_LABEL[g.feature] || g.feature || "";
      const route = routeOf(g);
      const count = `${g.event_count || 1} event${g.event_count === 1 ? "" : "s"}`;
      const destinations = g.destinations || [g];
      const deliveryRoute = destinations.length > 1 ? `${instLabel(g.source_provider, g.source_instance)} → ${destinations.map(d => instLabel(d.destination_provider, d.destination_instance)).join(", ")}` : route;
      const meta = [deliveryRoute, count].filter(Boolean).join(" · ");
      const item = titleOf(g);
      const s = g.summary || "";
      let headline, detail;
      if (item) {
        headline = item;
        detail = (s && s !== item) ? s : "";
      } else {
        const i = s.indexOf(", ");
        if (i > 0) { headline = s.slice(0, i); detail = s.slice(i + 2); }
        else { headline = s; detail = ""; }
      }
      headline = headline || statusLabel(g.status, g.domain);
      const twist = opts.expandable
        ? `<button class="ev-twist${opts.expanded ? " open" : ""}" type="button" data-twist="${g.id}" aria-label="${TH(opts.expanded ? "events.collapse" : "events.expand")}"><span class="material-symbols-rounded" aria-hidden="true">chevron_right</span></button>`
        : `<span class="ev-twist-sp" aria-hidden="true"></span>`;
      return `
      <div class="ev-row${opts.child ? " ev-row-child" : ""}${g.acknowledged_at ? " acked" : ""}" data-id="${g.id}">
        <input class="ev-check" type="checkbox" aria-label="${TH("events.selectItem", {name: headline})}" data-check="${g.id}" ${checked.has(String(g.id)) ? "checked" : ""}>${twist}
        <span class="ev-ic ${sv}"><span class="material-symbols-rounded" aria-hidden="true">${groupIcon(g)}</span></span>
        <span class="ev-line">
          <span class="ev-primary"><span class="ev-ptext">${esc(headline)}</span></span>
          <span class="ev-summary">${esc(g.domain === "scrobble" ? activitySummary(g) : detail || statusLabel(g.status, g.domain))}</span>
          <span class="ev-meta"><span class="material-symbols-rounded ev-meta-ic" aria-hidden="true">format_list_bulleted</span>${withProviderLogos(meta)}</span>
        </span>
        <span class="ev-row-end"><time class="ev-time">${esc(TS(g.last_event_at))}</time><span class="ev-badge ${sv}">${esc(statusLabel(g.status, g.domain))}</span></span>
        <span class="ev-rowact">
          <button class="ev-ack" type="button" title="${g.acknowledged_at ? TH("events.reviewedUndo") : TH("events.markActivityReviewed")}" aria-label="${g.acknowledged_at ? T("events.markActivityUnreviewed") : T("events.markActivityReviewed")}" data-ack="${g.id}"><span class="material-symbols-rounded" aria-hidden="true">${g.acknowledged_at ? "check_circle" : "check"}</span></button>
        </span>
      </div>`;
    };

    const eventRowHTML = (e) => {
      const sv = sevOf(e);
      const feat = FEATURE_LABEL[e.feature] || e.feature || "";
      const route = routeOf(e);
      const meta = [feat, route].filter(Boolean).join(" · ");
      const item = titleOf(e);
      return `
      <div class="ev-row${e.acknowledged_at ? " acked" : ""}" data-id="${e.id}">
        <input class="ev-check" type="checkbox" aria-label="${TH("events.selectItem", {name: titleLine(e)})}" data-check="${e.id}" ${checked.has(String(e.id)) ? "checked" : ""}><span class="ev-twist-sp" aria-hidden="true"></span>
        <span class="ev-ic ${sv}"><span class="material-symbols-rounded" aria-hidden="true">${iconOf(e)}</span></span>
        <span class="ev-line">
          <span class="ev-primary"><span class="ev-badge ${sv}">${esc(badgeOf(e))}</span><span class="ev-ptext">${esc(titleLine(e))}</span></span>
          <span class="ev-meta">${withProviderLogos(meta)}</span>
          ${item ? `<span class="ev-item">${esc(item)}</span>` : ""}
        </span>
        <span class="ev-time">${esc(TS(e.created_at))}</span>
        <span class="ev-rowact">
          <button class="ev-ack" type="button" title="${e.acknowledged_at ? TH("events.reviewedUndo") : TH("events.acknowledge")}" aria-label="${e.acknowledged_at ? T("events.markEventUnreviewed") : T("events.markEventReviewed")}" data-ack="${e.id}"><span class="material-symbols-rounded" aria-hidden="true">${e.acknowledged_at ? "check_circle" : "check"}</span></button>
        </span>
      </div>`;
    };

    const findGroup = (id) => {
      const s = String(id);
      for (const g of state.items) {
        if (String(g.id) === s) return g;
        for (const k of (g.children || [])) if (String(k.id) === s) return k;
      }
      return null;
    };

    const buildRows = () => {
      if (!grouped()) return state.items.map(eventRowHTML).join("");
      const out = [];
      for (const g of state.items) {
        const kids = g.children || [];
        const expandable = kids.length > 0 || Number(g.children_total || 0) > 0;
        const expanded = expandable && !state.collapsed.has(String(g.id));
        out.push(groupRowHTML(g, { expandable, expanded }));
        if (expanded && state.childLoading.has(String(g.id))) out.push(`<div class="ev-row ev-row-child ev-row-loading"><span class="ev-twist-sp" aria-hidden="true"></span><span class="ev-ico neutral"><span class="material-symbols-rounded ev-spin" aria-hidden="true">progress_activity</span></span><span class="ev-main"><span class="ev-summary">${TH("events.loadingChildren")}</span></span><span></span><span></span></div>`);
        if (expanded) for (const k of kids) out.push(groupRowHTML(k, { child: true }));
      }
      return out.join("");
    };

    const applyDefaultCollapse = () => {
      state.collapsed.clear();
      for (const g of state.items) {
        if ((g.children || []).length > 0 || Number(g.children_total || 0) > 0) state.collapsed.add(String(g.id));
      }
    };

    const bindRow = (el) => {
      el.tabIndex = 0;
      el.addEventListener("keydown", event => {
        if (event.target !== el || !["Enter", " "].includes(event.key)) return;
        event.preventDefault();
        select(el.dataset.id, el);
      });
      el.addEventListener("click", (ev) => {
        if (ev.target.closest(".ev-rowact, .ev-twist, .ev-check")) return;
        select(el.dataset.id, el);
      });
      el.querySelector(".ev-check")?.addEventListener("change", event => {
        const id = String(el.dataset.id);
        if (event.target.checked) checked.add(id); else checked.delete(id);
        updateSelection();
      });
      el.querySelector(".ev-ack")?.addEventListener("click", (ev) => {
        ev.stopPropagation();
        const item = grouped() ? findGroup(el.dataset.id) : state.items.find(row => String(row.id) === el.dataset.id);
        if (item && item.acknowledged_at) unacknowledgeRow(el.dataset.id);
        else acknowledgeRow(el.dataset.id);
      });
    };

    const clearDetail = () => {
      if (detailLayer) closeMobileDetail();
      ++selectionSequence;
      layoutEl.classList.remove("show-detail");
      state.selected = null;
      state.selectedPair = "";
      const what = titles() ? "title" : runsMode() ? "run" : grouped() ? "activity" : "event";
      detailEl.innerHTML = `<div class="ev-empty ev-empty-detail"><span class="material-symbols-rounded" aria-hidden="true">touch_app</span><div>${TH(`events.selectPrompt.${what}`)}</div></div>`;
    };

    const noun = () => "event";

    const emptyState = () => {
      let icon = "inbox", head = T("events.empty.none"), hint = "", actions = "";
      const n = noun();
      if (filtersActive()) {
        icon = "filter_alt_off"; head = T("events.empty.noMatch");
        hint = T("events.empty.noMatchHint");
        actions = `<button class="ev-linkbtn" id="ev-empty-reset">${TH("events.empty.clearFilters")}</button>`;
      } else if (visibility === "open") {
        icon = "task_alt"; head = T("events.empty.noUnreviewed");
        hint = T("events.empty.caughtUp");
        actions = `<button class="ev-linkbtn" id="ev-empty-all">${TH("events.empty.switchAll")}</button>`;
      } else if (visibility === "acknowledged") {
        icon = "done_all"; head = T("events.empty.noReviewed");
        hint = T("events.empty.noReviewedHint");
        actions = `<button class="ev-linkbtn" id="ev-empty-open">${TH("events.empty.showUnreviewed")}</button>`;
      } else {
        icon = "inbox"; head = T("events.empty.archive");
        hint = domain === "audit" ? T("events.empty.audit") : domain === "capture" ? T("events.empty.capture") : domain === "backup" ? T("events.empty.backup") : (domain === "scrobble" ? T("events.empty.scrobble") : T("events.empty.sync"));
        actions = "";
      }
      return `<div class="ev-empty ev-empty-list"><span class="material-symbols-rounded" aria-hidden="true">${icon}</span><div class="ev-empty-head">${esc(head)}</div><div class="ev-empty-hint">${esc(hint)}</div><div class="ev-empty-actions">${actions}</div></div>`;
    };

    const renderPager = () => {
      const pc = pageCount();
      const total = state.total || 0;
      if (!total) { footEl.innerHTML = ""; pageLabelEl.textContent = T("events.pageOf", {page: 0, pages: 0}); return; }
      const start = state.page * pageSize() + 1;
      const end = Math.min(total, state.page * pageSize() + state.items.length);
      pageLabelEl.textContent = T("events.pageOf", {page: state.page + 1, pages: pc});

      const cur = state.page;
      const label = titles() ? "titles" : grouped() ? "activities" : "events";
      footEl.innerHTML = `
        <div class="ev-showing">${TH(`events.showing.${label}`, {start, end, total})}</div>
        <div class="ev-pagenums">
          <button class="ev-pg" id="ev-first" type="button" aria-label="${TH("events.firstPage")}" ${cur === 0 ? "disabled" : ""}><span class="material-symbols-rounded" aria-hidden="true">first_page</span></button>
          <button class="ev-pg" id="ev-prev" type="button" aria-label="${TH("events.previousPage")}" ${cur === 0 ? "disabled" : ""}><span class="material-symbols-rounded" aria-hidden="true">chevron_left</span></button>
          <label class="ev-page-jump">${TH("events.page")} <input id="ev-page-number" type="number" min="1" max="${pc}" value="${cur + 1}" aria-label="${TH("events.pageNumber")}"> ${TH("events.ofPages", {pages: pc})}</label>
          <button class="ev-pg" id="ev-next" type="button" aria-label="${TH("events.nextPage")}" ${cur >= pc - 1 ? "disabled" : ""}><span class="material-symbols-rounded" aria-hidden="true">chevron_right</span></button>
          <button class="ev-pg" id="ev-last" type="button" aria-label="${TH("events.lastPage")}" ${cur >= pc - 1 ? "disabled" : ""}><span class="material-symbols-rounded" aria-hidden="true">last_page</span></button>
        </div>`;

      const go = (p) => { const np = Math.max(0, Math.min(pc - 1, p)); if (np !== state.page) load(np); };
      Q("#ev-page-number", footEl)?.addEventListener("change", event => { const page = Number(event.target.value); if (Number.isInteger(page) && page >= 1) go(page - 1); else event.target.value = cur + 1; });
      Q("#ev-first", footEl)?.addEventListener("click", () => go(0));
      Q("#ev-prev", footEl)?.addEventListener("click", () => go(cur - 1));
      Q("#ev-next", footEl)?.addEventListener("click", () => go(cur + 1));
      Q("#ev-last", footEl)?.addEventListener("click", () => go(pc - 1));
      footEl.querySelectorAll(".ev-pg-num").forEach((b) => b.addEventListener("click", () => go(Number(b.dataset.p))));
    };

    const updateHeadPager = () => {
      const pc = pageCount();
      Q("#ev-prev-top", root).disabled = state.page <= 0;
      Q("#ev-next-top", root).disabled = state.page >= pc - 1;
    };

    const loadRunChildren = async (id) => {
      const gid = String(id);
      const g = state.items.find((x) => String(x.id) === gid);
      if (!g || g.children_loaded || state.childLoading.has(gid)) return;
      state.childLoading.add(gid);
      renderList();
      try {
        const page = await fjson(`/api/events/groups/${encodeURIComponent(gid)}/run-items?limit=500&offset=0`);
        if (!alive) return;
        g.children = Array.isArray(page?.items) ? page.items : [];
        g.children_total = Number(page?.total || g.children.length || 0);
        g.children_loaded = true;
      } catch {
        g.children = [];
        g.children_total = 0;
      } finally {
        state.childLoading.delete(gid);
        if (alive) renderList();
      }
    };

    const runMatches = (run, needle) => !needle || [run.status, run.dry_run ? "dry run" : "", ...(run.pair_rows || []).flatMap((r) => [r.feature, r.src_provider, r.dst_provider, r.src_instance, r.dst_instance])].join(" ").toLowerCase().includes(needle);
    const visibleRuns = () => state.runs.filter((run) => runMatches(run, qEl.value.trim().toLowerCase()));

    const bindTitleRows = () => {
      listEl.querySelectorAll(".ev-trow[data-tid], .ev-rrow[data-run], .ev-rghead[data-run]").forEach((el) => {
        const id = el.dataset.tid || el.dataset.run;
        const open = () => select(id, el);
        el.addEventListener("click", open);
        el.addEventListener("keydown", (event) => {
          if (event.target !== el || !["Enter", " "].includes(event.key)) return;
          event.preventDefault();
          open();
        });
      });
      listEl.querySelectorAll(".ev-rquiet[data-group]").forEach((btn) => btn.addEventListener("click", () => {
        const key = btn.dataset.group;
        if (btn.getAttribute("aria-expanded") === "true") openQuietRuns.delete(key);
        else openQuietRuns.add(key);
        renderList();
        listEl.querySelector(`.ev-rquiet[data-group="${CSS.escape(key)}"]`)?.focus();
      }));
    };

    const renderList = () => {
      if (runsMode()) {
        const runs = visibleRuns();
        countEl.textContent = selectedDay != null ? `${dayLabel(selectedDay)} · ${T("events.count.run", {count: runs.length})}` : T("events.pickDay");
        footEl.innerHTML = "";
        pageLabelEl.textContent = "";
        Q("#ev-prev-top", root).disabled = true;
        Q("#ev-next-top", root).disabled = true;
        listEl.innerHTML = selectedDay == null
          ? `<div class="ev-empty ev-empty-list"><span class="material-symbols-rounded" aria-hidden="true">calendar_month</span><div class="ev-empty-head">${TH("events.noRuns")}</div><div class="ev-empty-hint">${TH("events.noRunsHint")}</div></div>`
          : `<div class="ev-runs">${runRowsHTML(runs, { selected: state.selected, selectedPair: state.selectedPair, open: openQuietRuns, openAll: !!qEl.value.trim() })}</div>`;
        bindTitleRows();
        return;
      }
      if (titles()) {
        countEl.textContent = T("events.count.title", {count: state.total || 0});
        const reviewAllBtn = Q("#ev-review-all", quickEl);
        if (reviewAllBtn) reviewAllBtn.hidden = !state.total;
        updateHeadPager();
        if (!state.items.length) {
          listEl.innerHTML = state.preparing
            ? `<div class="ev-empty ev-empty-list"><span class="material-symbols-rounded ev-spin" aria-hidden="true">progress_activity</span><div class="ev-empty-head">${TH("events.buildingTitles")}</div><div class="ev-empty-hint">${TH("events.buildingTitlesHint")}</div></div>`
            : emptyState();
          Q("#ev-empty-reset", listEl)?.addEventListener("click", resetFilters);
          renderPager();
          return;
        }
        listEl.innerHTML = `${state.preparing ? `<div class="ev-tnotice">${TH("events.stillAdding")}</div>` : ""}<div class="ev-titles">${titleRowsHTML(state.items, { tmdbOn, selected: state.selected })}</div>`;
        bindCovers(listEl);
        bindTitleRows();
        renderPager();
        return;
      }
      countEl.textContent = T(grouped() ? "events.count.activity" : "events.count.event", {count: state.total || 0});
      updateHeadPager();
      updateSelection();
      if (!state.items.length) {
        listEl.innerHTML = emptyState();
        renderPager();
        Q("#ev-empty-reset", listEl)?.addEventListener("click", resetFilters);
        Q("#ev-empty-all", listEl)?.addEventListener("click", () => { seg.value = "all"; visibility = "all"; lset("cw.events.visibility", "all"); load(0); });
        Q("#ev-empty-open", listEl)?.addEventListener("click", () => { seg.value = "open"; visibility = "open"; lset("cw.events.visibility", "open"); load(0); });
        return;
      }
      listEl.innerHTML = buildRows();
      listEl.querySelectorAll(".ev-row[data-id]").forEach(bindRow);
      listEl.querySelectorAll(".ev-twist").forEach((b) => b.addEventListener("click", async (ev) => {
        ev.stopPropagation();
        const id = String(b.dataset.twist);
        const opening = state.collapsed.has(id);
        if (opening) state.collapsed.delete(id); else state.collapsed.add(id);
        renderList();
        if (opening) await loadRunChildren(id);
      }));
      if (state.selected) listEl.querySelector(`.ev-row[data-id="${state.selected}"]`)?.classList.add("sel");
      renderPager();
    };

    const wideDetail = () => !matchMedia("(max-width:900px)").matches;

    const load = async (page) => {
      checked.clear();
      updateSelection();
      const sequence = ++loadSequence;
      const previousPage = state.page;
      if (page != null) state.page = page;
      updateHidden();
      if (runsMode()) {
        if (selectedDay == null) {
          await activity?.load();
          if (!alive || sequence !== loadSequence) return;
          const latest = activity?.latestDay?.();
          if (latest != null) { selectedDay = latest; activity.setSelected(latest); updateHidden(); }
        }
        if (selectedDay == null) { state.runs = []; renderList(); return; }
        listEl.innerHTML = `<div class="ev-empty ev-empty-list"><span class="material-symbols-rounded ev-spin" aria-hidden="true">progress_activity</span><div class="ev-empty-head">${TH("events.loadingRuns")}</div></div>`;
        try {
          const r = dayRange(selectedDay);
          const data = await fjson(`/api/events/calendar/day?since=${r.since}&until=${r.until + 1}&limit=300`);
          if (!alive || sequence !== loadSequence) return;
          state.runs = Array.isArray(data?.runs) ? data.runs : [];
          if (state.selected && !state.runs.some((run) => String(run.run_id) === String(state.selected))) clearDetail();
          renderList();
          if (runFilter) {
            const id = runFilter;
            runFilter = "";
            updateHidden();
            if (state.runs.some((run) => String(run.run_id) === id)) await select(id, null, { reveal: true });
          }
          const firstRun = listEl.querySelector(".ev-rrow[data-run]");
          if (!state.selected && firstRun && wideDetail()) await select(firstRun.dataset.run, firstRun, { reveal: false });
        } catch (err) {
          if (!alive || sequence !== loadSequence) return;
          listEl.innerHTML = `<div class="ev-empty ev-empty-list"><span class="material-symbols-rounded" aria-hidden="true">error</span><div class="ev-empty-head">${TH("events.runsFailed")}</div><div class="ev-empty-hint">${esc(err.message)}</div></div>`;
        }
        return;
      }
      if (titles()) {
        try {
          const data = await fjson(`/api/events/titles?${titlesQuery(state.page)}`);
          if (!alive || sequence !== loadSequence) return;
          state.items = data.items || [];
          state.total = data.total || 0;
          const attn = Number(data.attention_total || 0);
          if (attn !== Number(state.attentionTotal || 0)) { state.attentionTotal = attn; renderQuick(); }
          state.preparing = !!data.preparing;
          const pc = pageCount();
          if (state.page > 0 && state.page >= pc) return load(pc - 1);
          if (state.selected && !state.items.some((row) => String(row.id) === String(state.selected))) clearDetail();
          renderList();
          if (!state.selected && state.items.length && wideDetail()) await select(state.items[0].id, null, { reveal: false });
          if (state.preparing) setTimeout(() => { if (alive && titles() && sequence === loadSequence) load(state.page); }, 4000);
          if (state.page !== previousPage) revealSection(layoutEl);
        } catch (err) {
          if (!alive || sequence !== loadSequence) return;
          listEl.innerHTML = `<div class="ev-empty ev-empty-list"><span class="material-symbols-rounded" aria-hidden="true">error</span><div class="ev-empty-head">${TH("events.loadFailed")}</div><div class="ev-empty-hint">${esc(err.message)}</div></div>`;
        }
        return;
      }
      const url = grouped()
        ? ((domain === "scrobble" || domain === "audit" || JOB_DOMAINS.includes(domain)) ? `/api/events/groups?${buildQuery(state.page)}` : `/api/events/tree?${buildQuery(state.page)}`)
        : `/api/events/search?view=events&${buildQuery(state.page)}`;
      try {
        const data = await fjson(url);
        if (!alive || sequence !== loadSequence) return;
        state.items = data.items || [];
        state.total = data.total || 0;
        if (state.selected && !(grouped() ? findGroup(state.selected) : state.items.find(row => String(row.id) === state.selected))) clearDetail();
        applyDefaultCollapse();
        const pc = pageCount();
        if (state.page > 0 && state.page >= pc) { return load(pc - 1); }
        renderList();
        if (!state.didInitialSelect && state.page === 0 && (state.items.length || initialGroupId)) {
          state.didInitialSelect = true;
          const target = initialGroupId ? { id: initialGroupId } : null;
          const latestRun = grouped() && !target ? state.items.find((x) => String(x.operation || "") === "run") : null;
          const picked = target || latestRun || state.items[0];
          if (picked?.id != null) await select(picked.id, null, { reveal: !!(initialGroupId || runFilter) });
        }
        if (state.page !== previousPage) revealSection(layoutEl);
      } catch (err) {
        if (!alive || sequence !== loadSequence) return;
        listEl.innerHTML = `<div class="ev-empty ev-empty-list"><span class="material-symbols-rounded" aria-hidden="true">error</span><div class="ev-empty-head">${TH("events.loadFailed")}</div><div class="ev-empty-hint">${esc(err.message)}</div></div>`;
      }
    };

    let toastTimer = null;
    const showToast = (msg, undoFn, tone) => {
      clearTimeout(toastTimer);
      const bad = tone === "error";
      toastEl.hidden = false;
      toastEl.classList.toggle("ev-toast-error", bad);
      toastEl.innerHTML = `<span class="material-symbols-rounded ev-toast-ic" aria-hidden="true">${bad ? "error" : "check_circle"}</span><span>${esc(msg)}</span>${undoFn ? `<button type="button" class="ev-toast-undo">${TH("events.undo")}</button>` : ""}`;
      toastEl.querySelector(".ev-toast-undo")?.addEventListener("click", async () => {
        clearTimeout(toastTimer);
        toastEl.hidden = true;
        try { await undoFn(); } catch {}
      });
      toastTimer = setTimeout(() => { toastEl.hidden = true; }, 6000);
    };

    const ackURL = (id, action) => grouped()
      ? `/api/events/groups/${encodeURIComponent(id)}/${action}`
      : `/api/events/${encodeURIComponent(id)}/${action}`;

    const reviewIds = id => grouped() ? (findGroup(id)?.destinations || [{id}]).map(row => row.id) : [id];
    const reviewRequest = async (id, action, ids = reviewIds(id)) => {
      const results = await Promise.allSettled(ids.map(gid => fjson(ackURL(gid, action), {method: "POST"})));
      const failed = results.some(result => result.status === "rejected" || !result.value?.ok);
      if (failed) throw new Error(T("events.updateFailedSome"));
      return results[0].value;
    };
    const updateSelection = () => {
      const selectPage = Q("#ev-select-page", root);
      const ids = state.items.map(row => String(row.id));
      selectPage.checked = ids.length > 0 && ids.every(id => checked.has(id));
      selectPage.indeterminate = checked.size > 0 && !selectPage.checked;
      selectPage.disabled = !ids.length;
      Q("#ev-selection-count", root).textContent = checked.size ? `${checked.size} selected` : "";
      Q("#ev-review-selected", root).disabled = !checked.size;
    };
    Q("#ev-select-page", root).addEventListener("change", event => {
      checked.clear();
      if (event.target.checked) state.items.forEach(row => checked.add(String(row.id)));
      renderList();
    });
    Q("#ev-review-selected", root).addEventListener("click", async event => {
      event.currentTarget.disabled = true;
      const results = await Promise.allSettled([...checked].map(id => reviewRequest(id, "acknowledge")));
      const failures = results.filter(result => result.status === "rejected").length;
      await load(state.page);
      if (state.selected) await select(state.selected, null, {reveal: false});
      showToast(failures ? `${failures} activities could not be marked reviewed. Please try again.` : T("events.selectedReviewed"), null, failures ? "error" : "ok");
    });

    const animateRowOut = async (id) => {
      const row = listEl.querySelector(`.ev-row[data-id="${id}"]`);
      if (!row) return;
      row.classList.add("ev-row-leave");
      await new Promise((resolve) => {
        let done = false;
        const finish = () => { if (!done) { done = true; resolve(); } };
        row.addEventListener("animationend", finish, { once: true });
        setTimeout(finish, 260);
      });
    };

    const acknowledgeRow = async (id) => {
      const ids = reviewIds(id);
      let res;
      try { res = await reviewRequest(id, "acknowledge"); } catch (error) { showToast(error.message, null, "error"); await load(state.page); return; }
      if (!res?.ok) return;
      if (visibility === "open") await animateRowOut(id);
      const idx = state.items.findIndex((x) => String(x.id) === String(id));
      if (idx < 0) {
        await load(state.page);
        showToast(grouped() ? T("events.activityReviewed") : T("events.eventReviewed"), async () => {
          try { await reviewRequest(id, "unacknowledge", ids); } catch { return; }
          await load(state.page); select(id);
        });
        return;
      }
      const item = idx >= 0 ? state.items[idx] : null;
      if (item) item.acknowledged_at = res.acknowledged_at;

      if (visibility === "open") {
        const wasSelected = String(state.selected) === String(id);
        if (idx >= 0) { state.items.splice(idx, 1); state.total = Math.max(0, state.total - 1); }
        if (!state.items.length && state.total > 0) { await load(state.page); }
        else {
          renderList();
          if (wasSelected) {
            const next = state.items[Math.min(idx, state.items.length - 1)];
            if (next) select(next.id); else clearDetail();
          }
        }
      } else {
        const row = listEl.querySelector(`.ev-row[data-id="${id}"]`);
        if (row) { row.classList.add("acked"); const b = row.querySelector(".ev-ack .material-symbols-rounded"); if (b) b.textContent = "check_circle"; }
      }
      if (String(state.selected) === String(id)) await select(id, null, {reveal: false});
      showToast(grouped() ? T("events.activityReviewed") : T("events.eventReviewed"), async () => {
        try { await reviewRequest(id, "unacknowledge", ids); } catch { return; }
        await load(state.page);
        select(id);
      });
    };

    const unacknowledgeRow = async (id) => {
      let res;
      try { res = await reviewRequest(id, "unacknowledge"); } catch (error) { showToast(error.message, null, "error"); await load(state.page); return; }
      if (!res?.ok) return;
      const idx = state.items.findIndex((x) => String(x.id) === String(id));
      if (idx < 0) { await load(state.page); showToast(T("events.restored")); return; }
      const item = idx >= 0 ? state.items[idx] : null;
      if (item) item.acknowledged_at = null;
      if (visibility === "acknowledged") {
        if (idx >= 0) { state.items.splice(idx, 1); state.total = Math.max(0, state.total - 1); }
        if (!state.items.length && state.total > 0) await load(state.page); else renderList();
      } else {
        const row = listEl.querySelector(`.ev-row[data-id="${id}"]`);
        if (row) { row.classList.remove("acked"); const b = row.querySelector(".ev-ack .material-symbols-rounded"); if (b) b.textContent = "check"; }
      }
      if (String(state.selected) === String(id)) await select(id, null, {reveal: false});
      showToast(T("events.restored"));
    };

    // context cards (shared)
    const pill = (text, cls) => `<span class="ev-pill ${cls}">${esc(text)}</span>`;
    const card = (title, pillHTML, lines, extra = "") => `
      <div class="ev-card">
        <div class="ev-card-head"><span class="ev-card-title">${esc(title)}</span>${pillHTML || ""}</div>
        <div class="ev-card-body">${lines.filter(Boolean).map((l) => `<div class="ev-card-line">${l}</div>`).join("")}</div>
        ${extra}
      </div>`;

    const renderCards = (c, ref) => {
      const cards = [];
      const ps = c.pair_state;
      if (ps && ps.matched !== false) {
        cards.push(card(T("events.ctx.pairState"), pill(ps.enabled ? T("events.ctx.active") : T("events.ctx.disabled"), ps.enabled ? "ok" : "neutral"),
          [esc(FEATURE_LABEL[ref.feature] || ref.feature || ""), esc(ps.label || ""), esc(modeLabel(ps.mode))]));
      } else {
        cards.push(card(T("events.ctx.pairState"), pill(T("events.ctx.unmatched"), "neutral"),
          [T("events.ctx.pairNotMatched"), ps && ps.label ? `<span class="mono">${esc(ps.label)}</span>` : ""]));
      }

      const ph = c.provider_health;
      if (ph && ph.status_available !== false) {
        const entries = Object.entries(ph.providers || {});
        const anyDown = entries.some(([, h]) => h.configured !== false && h.status !== "unknown" && !h.connected);
        const anyUnknown = entries.some(([, h]) => h.status === "unknown" || h.configured === false);
        const allOk = entries.length > 0 && entries.every(([, h]) => h.connected || h.status === "ok");
        const st = anyDown ? pill(T("events.ctx.degraded"), "bad") : (allOk ? pill(T("events.ctx.healthy"), "ok") : pill(anyUnknown ? T("events.ctx.unknown") : T("events.ctx.healthy"), anyUnknown ? "neutral" : "ok"));
        const names = entries.map(([n, h]) => h.configured === false ? `${esc(n)} — ${TH("events.ctx.notConfigured")}` : esc(instLabel(n, h.instance)));
        cards.push(card(T("events.providerHealth"), st, [names.join("<br>") || "—", ph.checked_at ? TH("events.ctx.lastCheck", {time: TS(ph.checked_at)}) : ""]));
      }

      const us = c.unresolved_state;
      const uPresent = !!(us && us.present);
      const meta = (us && us.meta) || {};
      const since = meta.since ?? meta.first_seen ?? meta.first_seen_at ?? meta.ts ?? meta.created_at;
      const attempts = meta.attempts ?? meta.count ?? meta.tries;
      cards.push(card(T("events.ctx.unresolvedState"), pill(uPresent ? T("events.ctx.yes") : T("events.ctx.no"), uPresent ? "warn" : "neutral"),
        uPresent ? [since ? TH("events.ctx.since", {time: maybeTS(since)}) : "", attempts != null ? TH("events.ctx.attempts", {count: String(attempts)}) : ""] : [TH("events.ctx.notUnresolved")]));

      const bb = c.blackbox_state;
      const bPresent = !!(bb && bb.present);
      cards.push(card(T("events.ctx.blackboxState"), pill(bPresent ? T("events.ctx.yes") : T("events.ctx.no"), bPresent ? "error" : "neutral"), [bPresent ? T("events.ctx.blackboxed") : T("events.ctx.notBlackboxed")]));

      const tb = c.tombstone_state;
      const tPresent = !!(tb && tb.present);
      cards.push(card(T("events.ctx.tombstoneState"), pill(tPresent ? T("events.ctx.yes") : T("events.ctx.no"), tPresent ? "warn" : "neutral"), [tPresent ? T("events.ctx.tombstoned") : T("events.ctx.noTombstone")]));

      const af = c.analyzer_findings;
      const cnt = (af && af.count) || 0;
      cards.push(card(T("events.ctx.analyzer"), pill(cnt ? String(cnt) : T("events.ctx.none"), cnt ? "ok" : "neutral"),
        [cnt ? TH("events.ctx.baselines", {count: cnt}) : TH("events.ctx.notInBaselines")],
        ref.item_key ? `<button class="ev-card-link" id="ev-open-analyzer" type="button"><span class="material-symbols-rounded" aria-hidden="true">monitoring</span>${TH("events.ctx.viewAnalyzer")}</button>` : ""));

      return `<div class="ev-cards">${cards.join("")}</div>`;
    };

    // context summary
    const EVENT_DESC = {
      write_failed: T("events.desc.writeFailed"),
      write_succeeded: T("events.desc.writeSucceeded"),
      write_attempted: T("events.desc.writeAttempted"),
      unresolved_recorded: T("events.desc.unresolvedRecorded"),
      unresolved_cleared: T("events.desc.unresolvedCleared"),
      blackbox_promoted: T("events.desc.blackboxPromoted"),
      blackbox_blocked: T("events.desc.blackboxBlocked"),
      plan_created: T("events.desc.plan"),
      provider_health: T("events.desc.health"),
      sync_run_started: T("events.desc.runStarted"),
      sync_run_finished: T("events.desc.runCompleted"),
      tombstone_created: T("events.desc.tombstoneCreated"),
      tombstone_pruned: T("events.desc.tombstonePruned"),
      scrobble_started: T("events.desc.playbackStarted"),
      scrobble_completed: T("events.desc.scrobbled"),
      scrobble_failed: T("events.desc.scrobbleRejected"),
      scrobble_skipped: T("events.desc.scrobbleSkipped"),
      rating_applied: T("events.desc.ratingForwarded"),
      rating_failed: T("events.desc.ratingRejected"),
      rating_skipped: T("events.desc.ratingSkipped"),
    };
    const eventDesc = (e) => EVENT_DESC[e.event_type] || "";

    const providerLines = (c) => {
      const ph = c.provider_health;
      if (!ph || ph.status_available === false) return T("events.statusUnavailable");
      const entries = Object.entries(ph.providers || {});
      return entries.map(([n, h]) => h.configured === false ? `${esc(n)} — ${TH("events.ctx.notConfigured")}` : esc(instLabel(n, h.instance))).join("<br>") || "–";
    };

    const ctxSummary = (c) => {
      const ps = c.pair_state;
      const pairState = (ps && ps.matched !== false) ? (ps.enabled ? T("events.ctx.active") : T("events.ctx.disabled")) : T("events.ctx.unmatched");
      const ph = c.provider_health;
      let health = T("events.ctx.unknown"), healthCls = "neutral", lastCheck = "";
      if (ph && ph.status_available !== false) {
        const entries = Object.entries(ph.providers || {});
        const anyDown = entries.some(([, h]) => h.configured !== false && h.status !== "unknown" && !h.connected);
        const allOk = entries.length > 0 && entries.every(([, h]) => h.connected || h.status === "ok");
        health = anyDown ? T("events.ctx.degraded") : (allOk ? T("events.ctx.healthy") : T("events.ctx.unknown"));
        healthCls = anyDown ? "bad" : (allOk ? "ok" : "neutral");
        lastCheck = ph.checked_at ? TS(ph.checked_at) : "";
      }
      const us = c.unresolved_state;
      const uPresent = !!(us && us.present);
      const meta = (us && us.meta) || {};
      const uSince = meta.since ?? meta.first_seen ?? meta.first_seen_at ?? meta.ts ?? meta.created_at;
      return { pairState, pairEnabled: ps && ps.enabled, pairLabel: ps && ps.label, pairMode: ps && ps.mode, health, healthCls, lastCheck, uPresent, uSince };
    };

    const flashCopy = (btn) => {
      const ic = btn?.querySelector(".material-symbols-rounded");
      if (ic) { const t = ic.textContent; ic.textContent = "check"; setTimeout(() => { ic.textContent = t; }, 1200); }
    };

    // scrobble detail
    const parseDetail = (e) => {
      try { return typeof e.detail === "string" ? (e.detail ? JSON.parse(e.detail) : {}) : (e.detail || {}); }
      catch { return {}; }
    };

    const renderScrobbleDetail = (detail) => {
      const deliveries = detail.deliveries || [detail];
      const groups = deliveries.map(result => result.group || {});
      const latest = groups.reduce((picked, row) => Number(row.last_event_at || 0) >= Number(picked.last_event_at || 0) ? row : picked, detail.group || {});
      const failed = groups.some(row => row.status === "failed" || row.severity === "error");
      const rated = groups.some(row => row.status === "rated");
      const g = {
        ...detail.group, ...latest, id: detail.group?.id,
        first_event_at: Math.min(...groups.map(row => Number(row.first_event_at || row.last_event_at || 0))),
        event_count: groups.reduce((count, row) => count + Number(row.event_count || 0), 0),
        status: failed ? "failed" : rated ? "rated" : latest.status,
        severity: failed ? "error" : rated ? "success" : latest.severity,
        acknowledged_at: deliveries.every(result => !result.unavailable && result.group?.acknowledged_at) ? latest.acknowledged_at : null,
      };
      const events = deliveries.flatMap(delivery => delivery.events || []).sort((a, b) => a.created_at - b.created_at || a.id - b.id);
      const related = (detail.related_groups || []).filter(row => !deliveries.some(result => result.group?.id === row.id));
      const sv = groupSev(g);
      const st = String(g.status || "").toLowerCase();
      const item = titleOf(g);
      const target = instLabel(g.destination_provider, g.destination_instance) || "–";
      const player = instLabel(g.source_provider, g.source_instance) || "–";
      const route = (player !== "–" && target !== "–") ? `${player} → ${target}` : (target !== "–" ? target : player);
      const isRating = String(g.feature || "") === "ratings";
      const playback = playbackDetail(events);
      const delivery = playback.delivery || {};
      const isProblem = st === "failed" || delivery.status === "failed";
      let account = "", progress = null, progressAt = -Infinity, rating = null, manualMark = false;
      for (const e of events) {
        const d = parseDetail(e);
        if (d.account) account = String(d.account);
        if (d.manual_mark) manualMark = true;
        if (d.progress != null && playbackUpdatedAt(e) >= progressAt) { progress = d.progress; progressAt = playbackUpdatedAt(e); }
        if (d.rating != null) rating = d.rating;
      }

      const kvGrid = rows => `<div class="ev-kv">${rows.filter(Boolean).map(([k, v]) => `<div class="k">${esc(k)}</div><div class="v">${v}</div>`).join("")}</div>`;
      const outcome = isRating ? T("events.outcome.rating", {value: rating != null ? `${rating}/10` : T("events.outcome.notProvided")}) : st === "stopped" ? (progress != null ? T("events.outcome.stoppedAt", {progress: `${progress}%`}) : T("events.outcome.stopped")) : statusLabel(g.status, g.domain);
      const destinationsHTML = `<section class="ev-deliveries"><h4>${TH("events.destinationResults")} <span class="ev-h4-note">${deliveries.length}</span></h4>${deliveries.map(result => {
        const row = result.group || {};
        const status = result.unavailable ? T("events.detailsUnavailable") : statusLabel(row.status, row.domain);
        const reason = reasonLabel(row.reason, row.reason_label || row.reason_code);
        return `<details class="ev-delivery"><summary>${withProviderLogos(instLabel(row.destination_provider, row.destination_instance))}<span class="ev-pill ${groupSev(row)}">${esc(status)}</span></summary><div class="ev-delivery-body">${result.unavailable ? TH("events.deliveryFailed") : `<p>${esc(reason || row.summary || status)}</p>${kvGrid([[T("events.f.profile"), esc(instLabel(row.destination_provider, row.destination_instance))], [T("events.k.lastUpdate"), esc(TS(row.last_event_at))], [T("events.k.records"), esc(row.event_count || 0)]])}<details class="ev-tech"><summary>${TH("events.deliveryRecords")}</summary><div class="ev-tech-body"><pre>${esc(JSON.stringify(result.events || [], null, 2))}</pre></div></details>`}</div></details>`;
      }).join("")}</section>`;

      const timeline = playbackTimeline(events).slice().reverse().map((e) => {
        const es = sevOf(e);
        const pct = detailOf(e).progress;
        const desc = [eventDesc(e), pct != null ? String(pct) + "%" : "", deliveries.length > 1 ? instLabel(e.destination_provider, e.destination_instance) : ""].filter(Boolean).join(" · ");
        return `<div class="ev-tl" data-id="${e.id}"><span class="ev-tl-time">${esc(TS(e.created_at))}</span><span class="ev-tl-dot ${es}"></span><div class="ev-tl-body"><span class="ev-tl-head"><span class="ev-tl-title">${esc(titleLine(e))}</span><span class="ev-badge ${es}">${esc(badgeOf(e))}</span></span>${desc ? `<span class="ev-tl-desc">${esc(desc)}</span>` : ""}${cloudResultsHTML(e)}${failureDetailsHTML(e, events)}</div></div>`;
      }).join("");

      const detailsPane = kvGrid([
        delivery.status ? [T("events.k.lastDelivery"), esc(delivery.status + " (" + delivery.action + ")")] : null,
        [T("events.k.status"), `<span class="ev-pill ${sv}">${esc(statusLabel(g.status, g.domain))}</span>`],
        [T("events.k.title"), item ? esc(item) : "–"],
        [T("events.k.player"), withProviderLogos(player)],

        account ? [T("events.k.account"), esc(account)] : null,
        manualMark ? [T("events.k.trigger"), T("events.k.manualMark")] : null,
        isRating ? (rating != null ? [T("events.k.rating"), `${esc(rating)}/10`] : null) : (progress != null ? [T("events.feature.progress"), `${esc(progress)}%`] : null),
        [T("events.k.firstSeen"), esc(TS(g.first_event_at))],
        [T("events.k.lastSeen"), esc(TS(g.last_event_at))],
        g.reason ? [T("events.k.reason"), esc(g.reason)] : null,
      ]);
      const rawPane = `<div class="ev-dsec"><h5>${TH("events.rawData")}</h5><details class="ev-tech"><summary>${TH("events.fullPayload")}</summary><div class="ev-tech-body"><pre>${esc(JSON.stringify({ group: g, deliveries }, null, 2))}</pre></div></details></div>`;

      const relRow = (r) => `<button class="ev-rel" type="button" data-gid="${r.id}"><span class="ev-rel-ic ${groupSev(r)}"><span class="material-symbols-rounded" aria-hidden="true">${groupIcon(r)}</span></span><span class="ev-rel-badge ${groupSev(r)}">${esc(statusLabel(r.status, r.domain))}</span><span class="ev-rel-title">${esc(r.summary || titleOf(r) || "")}</span><span class="ev-rel-time">${esc(TS(r.last_event_at))}</span></button>`;
      const relHTML = related.length ? related.slice(0, 12).map(relRow).join("") : "";

      const ackLabel = g.acknowledged_at ? T("events.reviewed") : T("events.markReviewed");
      const ackIcon = g.acknowledged_at ? "check_circle" : "check";
      const tabAttr = (t) => detailTab === t ? "" : " hidden";

      detailEl.innerHTML = `
        <div class="ev-dhead">
          <div class="ev-dhead-row">
            <span class="ev-badge ${sv}">${esc(statusLabel(g.status, g.domain))}</span>
            <span class="ev-dhead-spacer"></span>
            <span class="ev-dtime">${esc(TS(g.last_event_at))}</span>
            <button class="ev-hbtn ev-hbtn-accent${g.acknowledged_at ? " on" : ""}" id="ev-ack-detail" type="button"><span class="material-symbols-rounded" aria-hidden="true">${ackIcon}</span><span>${ackLabel}</span></button>
          </div>
          <div class="ev-dtitle">${esc(item || g.summary || "")}</div>
          <p class="ev-outcome">${esc(outcome)}</p>
        </div>
        <div class="ev-tabs" role="tablist">
          <button class="ev-tab${detailTab === "details" ? " on" : ""}" data-tab="details" type="button"><span class="material-symbols-rounded" aria-hidden="true">description</span>${TH("events.tab.overview")}</button>
          <button class="ev-tab${detailTab === "timeline" ? " on" : ""}" data-tab="timeline" type="button"><span class="material-symbols-rounded" aria-hidden="true">timeline</span>${TH("events.tab.timeline")}</button>
          <button class="ev-tab${detailTab === "raw" ? " on" : ""}" data-tab="raw" type="button"><span class="material-symbols-rounded" aria-hidden="true">data_object</span>${TH("events.tab.technical")}</button>
        </div>
        <div class="ev-tabpanes">
          <div class="ev-tabpane" data-pane="timeline"${tabAttr("timeline")}>${playback.omitted ? `<p>${TH("events.omitted", {count: Number(playback.omitted) || 0})}</p>` : ""}<div class="ev-timeline">${timeline || `<div class="ev-empty ev-empty-inline">${TH("events.noThreadEvents")}</div>`}</div></div>
          <div class="ev-tabpane" data-pane="details"${tabAttr("details")}>${detailsPane}${destinationsHTML}</div>
          <div class="ev-tabpane" data-pane="raw"${tabAttr("raw")}>${rawPane}</div>
        </div>
        ${relHTML ? `<h4>${TH("events.related")}</h4><div class="ev-related">${relHTML}</div>` : ""}`;

      const panes = detailEl.querySelectorAll(".ev-tabpane");
      const switchTab = (name) => {
        detailTab = name; lset("cw.events.detailtab", name);
        detailEl.querySelectorAll(".ev-tab").forEach((x) => x.classList.toggle("on", x.dataset.tab === name));
        panes.forEach((p) => { p.hidden = p.dataset.pane !== name; });
      };
      detailEl.querySelectorAll(".ev-tab").forEach((t) => t.addEventListener("click", () => switchTab(t.dataset.tab)));
      Q("#ev-ack-detail", detailEl)?.addEventListener("click", () => { if (g.acknowledged_at) unacknowledgeRow(g.id); else acknowledgeRow(g.id); });
      detailEl.querySelectorAll(".ev-rel").forEach((a) => a.addEventListener("click", () => a.dataset.gid && select(a.dataset.gid)));
    };

    // group detail
    const renderGroupDetail = (detail) => {
      const g = detail.group || {};
      const c = detail.context || {};
      const events = detail.events || [];
      const related = detail.related_groups || [];
      const sv = groupSev(g);
      const ps = c.pair_state;
      const routeShort = provShort(g.source_provider, g.destination_provider) || "–";
      const pairFull = (ps && ps.matched && ps.label) ? ps.label : (g.pair_key || "–");
      const origin = g.origin_provider ? instLabel(g.origin_provider, g.origin_instance) : "unknown";
      const dest = instLabel(g.destination_provider, g.destination_instance) || "–";
      const item = titleOf(g);
      const feat = FEATURE_LABEL[g.feature] || g.feature || "–";
      const reason = g.reason || g.reason_code || "";
      const runId = events.map((e) => e.run_id).find(Boolean) || "";
      const cx = ctxSummary(c);
      const modeTxt = modeLabel(cx.pairMode) || "–";
      const st = String(g.status || "").toLowerCase();
      const isProblem = ["failed", "unresolved", "blackboxed", "warning"].includes(st);
      const isRun = g.operation === "run" || events.some(e => ["sync_run_started", "sync_run_finished"].includes(e.event_type));
      const summary = String(g.summary || item || "");
      const summaryBreak = isRun ? summary.indexOf(", ") : -1;
      const headline = summaryBreak > 0 ? summary.slice(0, summaryBreak) : summary;
      const supporting = summaryBreak > 0 ? summary.slice(summaryBreak + 2) : "";

      const pill = (text, cls) => `<span class="ev-pill ${cls}">${esc(text)}</span>`;
      const card = (title, pillHTML, lines, extra = "") => `
        <div class="ev-card">
          <div class="ev-card-head"><span class="ev-card-title">${esc(title)}</span>${pillHTML || ""}</div>
          <div class="ev-card-body">${lines.filter(Boolean).map((l) => `<div class="ev-card-line">${l}</div>`).join("")}</div>
          ${extra}
        </div>`;

      const problemSub = st === "failed" ? T("events.problem.failed", {operation: g.operation || "sync"})
        : st === "warning" ? T("events.problem.warning")
        : st === "unresolved" ? T("events.problem.unresolved", {operation: g.operation || "sync"})
        : st === "blackboxed" ? T("events.problem.blackbox")
        : T("events.status.completed");
      const scard = (icon, cls, title, mainHTML, subs, badge = "") => `
        <div class="ev-scard">
          <div class="ev-scard-head"><span class="ev-scard-ic ${cls}"><span class="material-symbols-rounded" aria-hidden="true">${icon}</span></span><span class="ev-scard-title ${cls}">${esc(title)}</span></div>
          <div class="ev-scard-main">${mainHTML}</div>
          ${subs.filter(Boolean).map((s) => `<div class="ev-scard-sub">${s}</div>`).join("")}
          ${badge}
        </div>`;
      const summaryCards =
        ((isProblem || reason || g.reason_label) ? scard(isProblem ? "error" : "check_circle", isProblem ? "error" : "ok", T("events.f.outcome"),
          `<span>${esc(reasonLabel(reason, g.reason_label) || statusLabel(g.status, g.domain))}</span>`, isProblem ? [esc(problemSub)] : []) : "") +
        ((g.source_provider || g.destination_provider) ? scard("swap_horiz", "info", T("events.k.route"), esc(routeShort), cx.pairMode ? [esc(modeTxt)] : [],
          g.feature ? `<span class="ev-badge info">${esc(feat)}</span>` : "") : "") +
        ((g.item_key || item) ? scard("inventory_2", "info", T("events.k.item"), esc(item || g.item_key),
          g.item_key && item ? [`<span class="mono">${esc(g.item_key)}</span>`] : []) : "");

      const timeline = events.slice().reverse().map((e) => {
        const es = sevOf(e);
        const desc = eventDesc(e);
        return `
        <div class="ev-tl" data-id="${e.id}">
          <span class="ev-tl-time">${esc(TS(e.created_at))}</span>
          <span class="ev-tl-dot ${es}"></span>
          <div class="ev-tl-body">
            <span class="ev-tl-head"><span class="ev-tl-title">${esc(titleLine(e))}</span><span class="ev-badge ${es}">${esc(badgeOf(e))}</span></span>
            ${desc ? `<span class="ev-tl-desc">${esc(desc)}</span>` : ""}${cloudResultsHTML(e)}${failureDetailsHTML(e, events)}
            ${e.reason_code ? `<span class="ev-tl-code mono">${esc(e.reason_code)}</span>` : ""}
          </div>
        </div>`;
      }).join("");

      const livePill = (label, val, cls) => `<span class="ev-livepill ${cls}"><span class="ev-livepill-k">${esc(label)}</span><span class="ev-livepill-v">${esc(val)}</span></span>`;
      const liveBar = `
        <div class="ev-livebar">
          ${livePill(T("events.f.pair"), cx.pairState, cx.pairState === T("events.ctx.active") ? "ok" : "neutral")}
          ${livePill(T("events.f.provider"), cx.health, cx.healthCls)}
          ${livePill(T("events.ctx.unresolved"), cx.uPresent ? T("events.ctx.yes") : T("events.ctx.no"), cx.uPresent ? "warn" : "neutral")}
          <button class="ev-livemore" id="ev-live-details" type="button"><span>${TH("events.fullContext")}</span><span class="material-symbols-rounded" aria-hidden="true">chevron_right</span></button>
        </div>`;

      const kvGrid = (rows) => `<div class="ev-kv">${rows.map(([k, v]) => `<div class="k">${esc(k)}</div><div class="v">${v}</div>`).join("")}</div>`;

      const detailsPane = isRun
        ? `<div class="ev-dsec"><h5>${TH("events.runDetails")}</h5>${kvGrid([
          [T("events.k.records"), esc(String(g.event_count || events.length))],
          [T("events.badge.started"), esc(TS(g.first_event_at))],
          [st === "running" ? T("events.k.lastUpdate") : T("events.k.finished"), esc(TS(g.last_event_at))],
        ])}</div>`
        :
        `<div class="ev-dsec"><h5>${TH("events.k.event")}</h5>${kvGrid([
          [T("events.k.status"), `<span class="ev-pill ${sv}">${esc(statusLabel(g.status, g.domain))}</span>`],
          [T("events.k.title"), esc(g.summary || "–")],
          [T("events.k.item"), item ? esc(item) : "–"],
          [T("events.k.reason"), esc(reason || "–")],
          [T("events.k.events"), esc(String(g.event_count || events.length))],
          [T("events.k.firstSeen"), esc(TS(g.first_event_at))],
          [T("events.k.lastSeen"), esc(TS(g.last_event_at))],
        ])}</div>` +
        `<div class="ev-dsec"><h5>${TH("events.k.route")}</h5>${kvGrid([
          [T("events.f.feature"), esc(feat)],
          [T("events.k.operation"), esc(g.operation || "–")],
          [T("events.k.route"), esc(routeShort)],
          [T("events.f.origin"), esc(origin)],
          [T("events.destination"), esc(dest)],
          [T("events.f.pair"), esc(pairFull)],
          [T("events.k.mode"), esc(modeTxt)],
        ])}</div>` +
        `<div class="ev-dsec"><h5>${TH("events.currentContext")} <span class="ev-h4-note">${TH("events.liveNow")}</span></h5>${kvGrid([
          [T("events.ctx.pairState"), esc(cx.pairState)],
          [T("events.providerHealth"), esc(cx.health)],
          [T("events.ctx.unresolvedState"), cx.uPresent ? T("events.ctx.yes") : T("events.ctx.no")],
          [T("events.k.lastProviderCheck"), cx.lastCheck ? esc(cx.lastCheck) : "–"],
        ])}</div>`;

      const eventIds = events.map((e) => e.id).join(", ");
      const rawFields = {
        status: g.status, feature: g.feature, operation: g.operation, route: routeShort,
        origin, destination: dest, reason, first_seen: TS(g.first_event_at), last_seen: TS(g.last_event_at), event_count: g.event_count,
      };
      const rawPane =
        `<div class="ev-dsec"><h5>${TH("events.identifiers")}</h5>${kvGrid([
          [T("events.k.threadId"), `<span class="mono">${esc(String(g.id))}</span>`],
          [T("events.k.runId"), runId ? `<span class="mono">${esc(runId)}</span>` : "–"],
          [T("events.k.eventIds"), `<span class="mono">${esc(eventIds || "–")}</span>`],
          [T("events.k.pairId"), `<span class="mono">${esc((ps && ps.id) || g.pair_key || "–")}</span>`],
          [T("events.k.itemKey"), g.item_key ? `<span class="mono">${esc(g.item_key)}</span>` : "–"],
        ])}</div>` +
        `<div class="ev-dsec"><h5>${TH("events.rawFields")}</h5>
          <details class="ev-tech"><summary>${TH("events.fields")}</summary><div class="ev-tech-body"><pre>${esc(JSON.stringify(rawFields, null, 2))}</pre></div></details>
          <details class="ev-tech"><summary>${TH("events.fullPayload")}</summary><div class="ev-tech-body"><div class="ev-tech-kv"><span>${TH("events.groupHash")}</span><span class="mono">${esc(g.group_hash || "–")}</span></div><pre>${esc(JSON.stringify({ group: g, context: c, events }, null, 2))}</pre></div></details>
        </div>` +
        `<div class="ev-dsec"><h5>${TH("events.actions")}</h5><div class="ev-rawacts">
          <button class="ev-tbtn" id="ev-copy-raw" type="button"><span class="material-symbols-rounded" aria-hidden="true">content_copy</span><span>${TH("events.copyRaw")}</span></button>
          <button class="ev-tbtn" id="ev-copy-item" type="button"><span class="material-symbols-rounded" aria-hidden="true">content_copy</span><span>${TH("events.copyItemKey")}</span></button>
          <button class="ev-tbtn" id="ev-copy-run" type="button"><span class="material-symbols-rounded" aria-hidden="true">content_copy</span><span>${TH("events.copyRunId")}</span></button>
        </div></div>`;

      const relRow = (r, primary) => `
          <button class="ev-rel" type="button" data-gid="${r.id}">
            <span class="ev-rel-ic ${groupSev(r)}"><span class="material-symbols-rounded" aria-hidden="true">${groupIcon(r)}</span></span>
            <span class="ev-rel-badge ${groupSev(r)}">${esc(statusLabel(r.status, r.domain))}</span>
            <span class="ev-rel-title">${esc(primary || "")}</span>
            <span class="ev-rel-time">${esc(TS(r.last_event_at))}</span>
          </button>`;
      const relHTML = related.length
        ? related.slice(0, 12).map((r) => relRow(r, r.summary || titleOf(r))).join("")
        : "";

      const runItemsSectionHTML = (items, total, limit, offset) => {
        const shown = items.length;
        const safeTotal = Math.max(Number(total || 0), shown);
        if (!safeTotal) return "";
        const safeLimit = Math.max(1, Number(limit || RUN_ITEMS_PAGE_SIZE));
        const safeOffset = Math.max(0, Number(offset || 0));
        const start = shown ? safeOffset + 1 : 0;
        const end = shown ? Math.min(safeOffset + shown, safeTotal) : 0;
        const hasPrev = safeOffset > 0;
        const hasNext = safeOffset + shown < safeTotal;
        const range = safeTotal > safeLimit
          ? T("events.runItems.range", {start, end, total: safeTotal})
          : T("events.count.item", {count: safeTotal});
        return `<section id="ev-runitems-section" class="ev-runitems-section">
          <div class="ev-runitems-head">
            <h4>${TH("events.problemItems")} <span class="ev-h4-note">${esc(range)}</span></h4>
            ${safeTotal > safeLimit ? `<div class="ev-runitems-controls">
              <button class="ev-pg" id="ev-runitems-prev" type="button" aria-label="${TH("events.previousProblems")}" ${hasPrev ? "" : "disabled"}><span class="material-symbols-rounded" aria-hidden="true">chevron_left</span></button>
              <button class="ev-pg" id="ev-runitems-next" type="button" aria-label="${TH("events.nextProblems")}" ${hasNext ? "" : "disabled"}><span class="material-symbols-rounded" aria-hidden="true">chevron_right</span></button>
            </div>` : ""}
          </div>
          <div class="ev-related">${shown ? items.map((r) => relRow(r, titleOf(r) || r.summary)).join("") : `<div class="ev-empty ev-empty-inline">${TH("events.noProblems")}</div>`}</div>
        </section>`;
      };
      const runItems = detail.run_items || [];
      const runItemsHTML = runItemsSectionHTML(
        runItems,
        detail.run_items_total,
        detail.run_items_limit,
        detail.run_items_offset,
      );

      const ackLabel = g.acknowledged_at ? T("events.reviewed") : T("events.markReviewed");
      const ackIcon = g.acknowledged_at ? "check_circle" : "check";
      const tabAttr = (t) => detailTab === t ? "" : " hidden";

      detailEl.innerHTML = `
        <div class="ev-dhead">
          <div class="ev-dhead-row">
            <span class="ev-badge ${sv}">${esc(statusLabel(g.status, g.domain))}</span>
            <span class="ev-dhead-spacer"></span>
            <span class="ev-dtime">${esc(TS(g.last_event_at))}</span>
            ${runId ? `<button class="ev-hbtn" id="ev-open-logs" type="button"><span class="material-symbols-rounded" aria-hidden="true">terminal</span><span>${TH("events.viewLogs")}</span></button>` : ""}
            <button class="ev-hbtn" id="ev-copy" type="button" title="${TH("events.copyThread")}"><span class="material-symbols-rounded" aria-hidden="true">content_copy</span><span>${TH("events.copy")}</span></button>
            <button class="ev-hbtn ev-hbtn-accent${g.acknowledged_at ? " on" : ""}" id="ev-ack-detail" type="button"><span class="material-symbols-rounded" aria-hidden="true">${ackIcon}</span><span>${ackLabel}</span></button>
          </div>
          <div class="ev-dtitle">${esc(headline)}</div>
          ${supporting ? `<p class="ev-outcome">${withProviderLogos(supporting)}</p>` : ""}
          ${item ? `<div class="ev-ditem">${esc(item)}</div>` : ""}
        </div>
        ${summaryCards ? `<div class="ev-scards">${summaryCards}</div>` : ""}
        <div class="ev-tabs" role="tablist">
          <button class="ev-tab${detailTab === "details" ? " on" : ""}" data-tab="details" type="button"><span class="material-symbols-rounded" aria-hidden="true">description</span>${TH("events.tab.overview")}</button>
          <button class="ev-tab${detailTab === "timeline" ? " on" : ""}" data-tab="timeline" type="button"><span class="material-symbols-rounded" aria-hidden="true">timeline</span>${TH("events.tab.timeline")}</button>
          <button class="ev-tab${detailTab === "raw" ? " on" : ""}" data-tab="raw" type="button"><span class="material-symbols-rounded" aria-hidden="true">data_object</span>${TH("events.tab.technical")}</button>
        </div>
        <div class="ev-tabpanes">
          <div class="ev-tabpane" data-pane="timeline"${tabAttr("timeline")}>
            ${g.item_key ? liveBar : runItemsHTML}
            <div class="ev-timeline">${timeline || `<div class="ev-empty ev-empty-inline">${TH("events.noThreadEvents")}</div>`}</div>
          </div>
          <div class="ev-tabpane" data-pane="details"${tabAttr("details")}>${detailsPane}</div>
          <div class="ev-tabpane" data-pane="raw"${tabAttr("raw")}>${rawPane}</div>
        </div>
        ${relHTML ? `<h4>${TH("events.relatedThreads")}</h4><div class="ev-related">${relHTML}</div>` : ""}`;

      const panes = detailEl.querySelectorAll(".ev-tabpane");
      const switchTab = (name) => {
        detailTab = name; lset("cw.events.detailtab", name);
        detailEl.querySelectorAll(".ev-tab").forEach((x) => x.classList.toggle("on", x.dataset.tab === name));
        panes.forEach((p) => { p.hidden = p.dataset.pane !== name; });
      };
      detailEl.querySelectorAll(".ev-tab").forEach((t) => t.addEventListener("click", () => switchTab(t.dataset.tab)));
      Q("#ev-live-details", detailEl)?.addEventListener("click", () => switchTab("details"));

      Q("#ev-ack-detail", detailEl)?.addEventListener("click", () => {
        if (g.acknowledged_at) unacknowledgeRow(g.id); else acknowledgeRow(g.id);
      });
      Q("#ev-copy", detailEl)?.addEventListener("click", (ev) => {
        const button = ev.currentTarget;
        window.cwCopyText?.([
          `Thread: ${g.summary || ""}`, `Status: ${g.status}`,
          `Feature: ${g.feature || ""}`, `Route: ${routeShort}`, `Pair: ${pairFull}`,
          `Item: ${g.item_key || ""}`, `Events: ${g.event_count}`,
          `First seen: ${TS(g.first_event_at)}`, `Last seen: ${TS(g.last_event_at)}`,
          `Reason: ${reason}`, "",
          ...events.map((e) => `  ${TS(e.created_at)} · ${badgeOf(e)} · ${titleLine(e)}`),
        ].join("\n")).then((ok) => { if (ok) flashCopy(button); });
      });
      Q("#ev-copy-raw", detailEl)?.addEventListener("click", (ev) => { const button = ev.currentTarget; window.cwCopyText?.(JSON.stringify({ group: g, context: c, events }, null, 2)).then((ok) => { if (ok) flashCopy(button); }); });
      Q("#ev-copy-item", detailEl)?.addEventListener("click", (ev) => { const button = ev.currentTarget; window.cwCopyText?.(g.item_key || "").then((ok) => { if (ok) flashCopy(button); }); });
      Q("#ev-copy-run", detailEl)?.addEventListener("click", (ev) => { const button = ev.currentTarget; window.cwCopyText?.(runId).then((ok) => { if (ok) flashCopy(button); }); });
      Q("#ev-open-logs", detailEl)?.addEventListener("click", () => window.openLogs?.({runId, channel:"sync"}));
      const bindRelRows = (root = detailEl) => {
        root.querySelectorAll(".ev-rel").forEach((a) => a.addEventListener("click", () => a.dataset.gid && select(a.dataset.gid)));
      };
      const bindRunItemsPager = () => {
        const section = Q("#ev-runitems-section", detailEl);
        if (!section) return;
        const limit = Number(detail.run_items_limit || RUN_ITEMS_PAGE_SIZE);
        const offset = Number(detail.run_items_offset || 0);
        const load = async (nextOffset) => {
          section.querySelectorAll(".ev-pg").forEach((b) => { b.disabled = true; });
          try {
            const page = await fjson(`/api/events/groups/${encodeURIComponent(g.id)}/run-items?limit=${RUN_ITEMS_PAGE_SIZE}&offset=${Math.max(0, nextOffset)}`);
            if (!alive) return;
            if (!page || page.ok === false) throw new Error("not found");
            detail.run_items = page.items || [];
            detail.run_items_total = page.total || 0;
            detail.run_items_limit = page.limit || RUN_ITEMS_PAGE_SIZE;
            detail.run_items_offset = page.offset || 0;
            section.outerHTML = runItemsSectionHTML(detail.run_items, detail.run_items_total, detail.run_items_limit, detail.run_items_offset);
            const nextSection = Q("#ev-runitems-section", detailEl);
            if (nextSection) bindRelRows(nextSection);
            bindRunItemsPager();
          } catch {
            section.outerHTML = runItemsSectionHTML(detail.run_items || [], detail.run_items_total, detail.run_items_limit, detail.run_items_offset);
            const nextSection = Q("#ev-runitems-section", detailEl);
            if (nextSection) bindRelRows(nextSection);
            bindRunItemsPager();
          }
        };
        Q("#ev-runitems-prev", section)?.addEventListener("click", () => load(offset - limit));
        Q("#ev-runitems-next", section)?.addEventListener("click", () => load(offset + limit));
      };
      bindRelRows();
      bindRunItemsPager();
    };

    const auditRowsHTML = (e) => {
      const d = detailOf(e);
      const actor = d.actor && typeof d.actor === "object" ? d.actor : {};
      const target = d.target && typeof d.target === "object" ? d.target : {};
      const req = d.request && typeof d.request === "object" ? d.request : {};
      const fields = d.fields && typeof d.fields === "object" ? d.fields : {};
      const rows = [
        [T("events.k.actor"), [actor.username, actor.role].filter(Boolean).join(" - ") || "–"],
        [T("events.k.actorId"), actor.id || "–"],
        [T("events.f.profile"), actor.profile_id || "–"],
        [T("events.k.target"), [target.type, target.id].filter(Boolean).join(" ") || "–"],
        [T("events.k.status"), d.status || e.reason_code || "success"],
        [T("events.k.method"), req.method || "–"],
        [T("events.k.path"), req.path || "–"],
        ["IP", req.ip || "–"],
        [T("events.k.userAgent"), req.user_agent || "–"],
      ];
      for (const [k, v] of Object.entries(fields)) rows.push([k, v == null ? "–" : String(v)]);
      return rows.map(([k, v]) => `<div class="k">${esc(k)}</div><div class="v">${esc(v)}</div>`).join("");
    };

    const captureSize = (value) => {
      const n = Number(value || 0);
      if (!n) return "";
      const units = ["B", "KB", "MB", "GB"];
      const i = Math.min(units.length - 1, Math.floor(Math.log(n) / Math.log(1024)));
      return `${(n / 1024 ** i).toFixed(i ? 1 : 0)} ${units[i]}`;
    };

    const captureRowsHTML = (e) => {
      const d = detailOf(e);
      const backup = String(e.feature || "") === "backup";
      const failed = String(e.event_type || "").endsWith("_failed");
      const rows = [
        [T("events.f.type"), backup ? T("events.badge.backup") : T("events.badge.capture")],
        ...(backup ? [] : [[T("events.f.provider"), e.source_provider ? instLabel(e.source_provider, e.source_instance) : "–"]]),
        [backup ? T("events.k.scope") : T("events.f.feature"), (backup ? d.scope : d.capture_feature) || "–"],
        [T("events.k.triggeredBy"), CAPTURE_TRIGGERS[String(e.source_kind || "")] || String(e.source_kind || "manual").replace(/_/g, " ")],
        [T("events.k.result"), failed ? T("events.outcome.failed") : T("events.outcome.successful")],
      ];
      if (d.items != null) rows.push([T("events.k.items"), String(d.items)]);
      if (d.size) rows.push([T("events.k.size"), captureSize(d.size)]);
      if (d.removed) rows.push([T("events.k.removed"), String(d.removed)]);
      if (d.destination) rows.push([T("events.destination"), String(d.destination)]);
      if (d.label) rows.push([T("events.k.label"), String(d.label)]);
      if (d.path) rows.push([T("events.k.file"), String(d.path)]);
      if (e.reason) rows.push([T("events.k.reason"), String(e.reason)]);
      return rows.map(([k, v]) => `<div class="k">${esc(k)}</div><div class="v">${esc(v)}</div>`).join("");
    };

    const renderCaptureDetail = (detail, eventOverride = null) => {
      const g = detail.group || eventOverride || {};
      const events = Array.isArray(detail.events) && detail.events.length ? detail.events : (eventOverride ? [eventOverride] : []);
      const e = eventOverride || events[events.length - 1] || g;
      const sv = sevOf(e);
      const logRun = e.run_id || events.find(row => row.run_id)?.run_id;
      detailEl.innerHTML = `
        <div class="ev-dhead">
          <div class="ev-dhead-row">
            <span class="ev-badge ${sv}">${esc(badgeOf(e))}</span>
            <span class="ev-dhead-spacer"></span>
            <span class="ev-dtime">${esc(TS(g.last_event_at || e.created_at))}</span>
          </div>
          <div class="ev-dtitle">${esc(g.summary || e.title || titleLine(e))}</div>
        </div>
        ${logRun ? '<button class="ev-hbtn" type="button" data-job-logs><span class="material-symbols-rounded" aria-hidden="true">terminal</span>' + TH("events.viewLogs") + '</button>' : ''}
        <h4>${String(e.feature || "") === "backup" ? T("events.badge.backup") : T("events.badge.capture")}</h4><div class="ev-kv">${captureRowsHTML(e)}</div>
        <details class="ev-tech"><summary>${TH("events.rawData")}</summary><div class="ev-tech-body"><pre>${esc(JSON.stringify({ group: detail.group || null, events }, null, 2))}</pre></div></details>`;
      Q('[data-job-logs]', detailEl)?.addEventListener('click', () => window.openLogs?.({runId: logRun, channel: e.feature === 'backup' ? 'backups' : 'captures'}));
    };

    const renderAuditDetail = (detail, eventOverride = null) => {
      const g = detail.group || eventOverride || {};
      const events = Array.isArray(detail.events) && detail.events.length ? detail.events : (eventOverride ? [eventOverride] : []);
      const e = eventOverride || events[events.length - 1] || g;
      const sv = sevOf(e);
      const data = detailOf(e);
      const actor = data.actor || {};
      const overview = [[T("events.k.who"), actor.username || T("events.ctx.unknown")], [T("events.k.action"), g.summary || titleLine(e)], [T("events.k.status"), data.status || statusLabel(g.status, "audit")], [T("events.f.profile"), actor.profile_id || T("events.k.default")], [T("events.f.when"), TS(g.last_event_at || e.created_at)]];

      const acknowledged = !!(g.acknowledged_at || e.acknowledged_at);
      const ackId = g.id || e.id;
      const ackLabel = acknowledged ? T("events.reviewed") : T("events.markReviewed");
      const ackIcon = acknowledged ? "check_circle" : "check";
      const timeline = events.length
        ? events.slice().reverse().map((row) => {
          const rs = sevOf(row);
          return `<div class="ev-tl" data-id="${esc(row.id || "")}"><span class="ev-tl-time">${esc(TS(row.created_at))}</span><span class="ev-tl-dot ${rs}"></span><span class="ev-tl-body"><span class="ev-tl-head"><span class="ev-tl-title">${esc(titleLine(row))}</span><span class="ev-badge ${rs}">${esc(badgeOf(row))}</span></span></span></div>`;
        }).join("")
        : `<div class="ev-empty ev-empty-inline">${TH("events.noTimeline")}</div>`;
      detailEl.innerHTML = `
        <div class="ev-dhead">
          <div class="ev-dhead-row">
            <span class="ev-badge ${sv}">${esc(badgeOf(e))}</span>
            <span class="ev-dhead-spacer"></span>
            <span class="ev-dtime">${esc(TS(g.last_event_at || e.created_at))}</span>
            ${ackId ? `<button class="ev-hbtn ev-hbtn-accent${acknowledged ? " on" : ""}" id="ev-ack-detail" type="button"><span class="material-symbols-rounded" aria-hidden="true">${ackIcon}</span><span>${ackLabel}</span></button>` : ""}
          </div>
          <div class="ev-dtitle">${esc(g.summary || titleLine(e))}</div>
        </div>
        <div class="ev-tabs" role="tablist"><button class="ev-tab on" data-tab="details" type="button">${TH("events.tab.overview")}</button><button class="ev-tab" data-tab="timeline" type="button">${TH("events.tab.timeline")}</button><button class="ev-tab" data-tab="raw" type="button">${TH("events.tab.technical")}</button></div>
        <div class="ev-tabpane" data-pane="details"><h4>${TH("events.audit")}</h4><div class="ev-kv">${overview.map(([key, value]) => `<div class="k">${esc(key)}</div><div class="v">${esc(value)}</div>`).join("")}</div>
        </div><div class="ev-tabpane" data-pane="timeline" hidden><h4>${TH("events.tab.timeline")}</h4><div class="ev-timeline">${timeline}</div>
        </div><div class="ev-tabpane" data-pane="raw" hidden><h4>${TH("events.requestDetails")}</h4><div class="ev-kv">${auditRowsHTML(e)}</div><details class="ev-tech"><summary>${TH("events.rawData")}</summary><div class="ev-tech-body"><pre>${esc(JSON.stringify({ group: detail.group || null, events }, null, 2))}</pre></div></details></div>`;
      detailEl.querySelectorAll(".ev-tab").forEach(tab => tab.addEventListener("click", () => {
        detailEl.querySelectorAll(".ev-tab").forEach(button => button.classList.toggle("on", button === tab));
        detailEl.querySelectorAll(".ev-tabpane").forEach(pane => { pane.hidden = pane.dataset.pane !== tab.dataset.tab; });
      }));
      Q("#ev-ack-detail", detailEl)?.addEventListener("click", () => {
        if (!ackId) return;
        if (acknowledged) unacknowledgeRow(ackId); else acknowledgeRow(ackId);
      });
    };

    // event detail (raw mode)
    const renderEventDetail = (ctx, e) => {
      const d = detailOf(e);
      if (String(e.domain || "").toLowerCase() === "audit") {
        renderAuditDetail({ event: e, events: [e] }, e);
        return;
      }
      if (JOB_DOMAINS.includes(String(e.domain || "").toLowerCase())) {
        renderCaptureDetail({ event: e, events: [e] }, e);
        return;
      }
      const c = ctx.context || {};
      const rel = ctx.related || {};
      const sv = sevOf(e);
      const ps = c.pair_state;
      const routeShort = provShort(e.source_provider, e.destination_provider) || "–";
      const pairLabel = (ps && ps.matched && ps.label) ? `${ps.label}${ps.mode ? ` (${modeLabel(ps.mode).toLowerCase()})` : ""}` : (ps && ps.label ? `${ps.label} — not matched` : (e.pair_key || "–"));
      const origin = e.origin_provider ? instLabel(e.origin_provider, e.origin_instance) : "";
      const conf = e.origin_confidence ? `<span class="ev-conf">${TH("events.confidence", {value: String(e.origin_confidence)})}</span>` : "";
      const dest = instLabel(e.destination_provider, e.destination_instance) || "";
      const runTime = e.source_mtime ? TS(e.source_mtime) : "";

      const kv = [
        [T("events.f.feature"), esc(FEATURE_LABEL[e.feature] || e.feature || "")],
        [T("events.k.operation"), esc(e.operation || "")],
        [T("events.k.route"), routeShort === "–" ? "" : esc(routeShort)],
        [T("events.f.origin"), origin ? `${esc(origin)}${conf ? ` ${conf}` : ""}` : ""],
        [T("events.destination"), esc(dest)],
        [T("events.f.pair"), pairLabel === "–" ? "" : esc(pairLabel)],
        [T("events.k.run"), e.run_id ? `<span class="mono">${esc(e.run_id)}</span>` : ""],
        ...(e.event_type === "cloud_history_read" ? [[T("events.k.cloudIncluded"), esc(d.cloud_history_entries ?? 0)], [T("events.k.totalMerged"), esc(d.history_entries ?? 0)]] : []),
        ...(cloudResultsHTML(e) ? [[T("events.cloud.fallback"), cloudResultsHTML(e)]] : []),
        [T("events.k.reason"), esc(e.reason || e.reason_code || "")],
        [T("events.k.source"), e.source_file ? `<span class="mono">${esc(e.source_file)}</span>` : esc(e.source_kind || "")],
        [T("events.k.runTime"), runTime ? esc(runTime) : ""],
      ].filter(([, v]) => v !== "").map(([k, v]) => `<div class="k">${esc(k)}</div><div class="v">${v}</div>`).join("");

      const item = titleOf(e);
      const parts = [];
      parts.push(`
        <div class="ev-dhead">
          <div class="ev-dhead-row">
            <span class="ev-badge ${sv}">${esc(badgeOf(e))}</span>
            <span class="ev-dhead-spacer"></span>
            <span class="ev-dtime">${esc(TS(e.created_at))}</span>
            <button class="ev-icon-btn" id="ev-copy" type="button" title="${TH("events.copyEvent")}" aria-label="${TH("events.copyEvent")}"><span class="material-symbols-rounded" aria-hidden="true">content_copy</span></button>
          </div>
          <div class="ev-dtitle">${esc(titleLine(e))}</div>
          ${item ? `<div class="ev-ditem">${esc(item)}</div>` : ""}
        </div>`);
      parts.push(failureDetailsHTML(e));
      parts.push(`<h4>${TH("events.k.event")}</h4><div class="ev-kv">${kv}</div>`);
      if (isRating(e)) parts.push(`<div class="ev-ratline">${TH(e.origin_provider ? "events.ratingChangedOrigin" : "events.ratingChanged", {from: String(e.old_value ?? "–"), to: String(e.new_value ?? "–"), origin: e.origin_provider ? instLabel(e.origin_provider, e.origin_instance) : ""}).replace(/\[\[b\]\]/g, "<b>").replace(/\[\[\/b\]\]/g, "</b>")}</div>`);
      parts.push(`<h4>${TH("events.currentContext")}</h4>`);
      parts.push(renderCards(c, e));
      parts.push(`<details class="ev-tech"><summary>${TH("events.technicalDetails")}</summary><div class="ev-tech-body"><div class="ev-tech-kv"><span>${TH("events.pairKey")}</span><span class="mono">${esc(e.pair_key || "–")}</span></div><pre>${esc(JSON.stringify({ event: e, context: c }, null, 2))}</pre></div></details>`);

      detailEl.innerHTML = parts.join("");
      Q("#ev-copy", detailEl)?.addEventListener("click", () => {
        window.cwCopyText?.([`Event: ${titleLine(e)}`, `Type: ${e.event_type}`, `Route: ${routeShort}`, `Pair: ${pairLabel}`, `Item: ${e.item_key || ""}`, `When: ${TS(e.created_at)}`, `Reason: ${e.reason || e.reason_code || ""}`].join("\n")).then((ok) => {
          if (!ok) return;
          const b = Q("#ev-copy", detailEl); if (b) { const ic = b.querySelector(".material-symbols-rounded"); if (ic) { ic.textContent = "check"; setTimeout(() => { ic.textContent = "content_copy"; }, 1200); } }
        });
      });
      Q("#ev-open-analyzer", detailEl)?.addEventListener("click", () => { try { window.openAnalyzer?.(); } catch {} });
    };

    const titleCtx = () => ({
      tmdbOn,
      tab: titleTab,
      fetchJson: fjson,
      onTab: (name) => { titleTab = name; lset("cw.events.titletab", name); },
      openTitle: (tid) => select(tid),
      review: (item) => reviewTitle(item),
      showRun: (item) => showRun(item),
    });

    const select = async (id, el, { reveal = true, pair = el?.dataset.pair ?? (state.selected === String(id) ? state.selectedPair : "") } = {}) => {
      const sequence = ++selectionSequence;
      if (reveal) {
        if (!layoutEl.classList.contains("show-detail")) listReturnPosition = window.scrollY;
        layoutEl.classList.add("show-detail");
        if (window.CW?.MobileUI?.mobile()) detailLayer ||= window.CW.MobileUI.layer(closeMobileDetail);
      }
      state.selected = String(id);
      state.selectedPair = runsMode() ? pair : "";
      state.relExpanded = false;
      listEl.querySelectorAll(".ev-row.sel, .ev-trow.sel, .ev-rrow.sel").forEach((n) => n.classList.remove("sel"));
      const rowSel = titles() ? `.ev-trow[data-tid="${CSS.escape(String(id))}"]` : runsMode() ? `.ev-rrow[data-run="${CSS.escape(String(id))}"]` : `.ev-row[data-id="${CSS.escape(String(id))}"]`;
      if (runsMode()) listEl.querySelectorAll(rowSel).forEach((n) => n.classList.add("sel"));
      else (el || listEl.querySelector(rowSel))?.classList.add("sel");
      if (runsMode()) {
        if (pair && !listEl.querySelector(`${rowSel}[data-pair="${CSS.escape(pair)}"]`)) openQuietRuns.add(pair);
        renderList();
        const run = state.runs.find((r) => String(r.run_id) === String(id));
        if (run) renderRunDetail(detailEl, run, {
          tmdbOn,
          pair,
          selectPair: key => select(id, null, {reveal: false, pair: key}),
          fetchJson: fjson,
          showTitles: (runId) => showTitles(runId),
          openTitle: (runId, tid) => { showTitles(runId); void select(tid); },
        });
        if (reveal) revealSection(Q(".ev-col-detail", root));
        return;
      }
      detailEl.innerHTML = `<div class="ev-empty ev-empty-detail"><span class="material-symbols-rounded ev-spin" aria-hidden="true">progress_activity</span><div>${TH("events.loadingContext")}</div></div>`;
      try {
        if (titles()) {
          const data = await fjson(`/api/events/titles/${encodeURIComponent(id)}`);
          if (!alive || sequence !== selectionSequence) return;
          if (!data?.ok || !data.item) throw new Error("not found");
          renderTitleDetail(detailEl, data.item, titleCtx());
        } else if (grouped()) {
          const detail = await fjson(`/api/events/groups/${encodeURIComponent(id)}?run_items_limit=${RUN_ITEMS_PAGE_SIZE}&run_items_offset=0`);
          if (!alive || sequence !== selectionSequence) return;
          if (!detail || detail.ok === false) throw new Error("not found");
          if (domain === "scrobble") {
            const destinations = findGroup(id)?.destinations || [detail.group];
            const results = await Promise.allSettled(destinations.map(row => String(row.id) === String(id) ? Promise.resolve({...detail}) : fjson(`/api/events/groups/${encodeURIComponent(row.id)}`)));
            if (!alive || sequence !== selectionSequence) return;
            detail.deliveries = results.map((result, index) => result.status === "fulfilled" && result.value?.ok !== false ? result.value : {group: destinations[index], unavailable: true});
            renderScrobbleDetail(detail);
          } else if (domain === "audit") renderAuditDetail(detail); else if (JOB_DOMAINS.includes(domain)) renderCaptureDetail(detail); else renderGroupDetail(detail);
        } else {
          const ctx = await fjson(`/api/events/context?event_id=${encodeURIComponent(id)}`);
          if (!alive || sequence !== selectionSequence) return;
          const e = ctx.event || state.items.find((x) => String(x.id) === String(id)) || {};
          renderEventDetail(ctx, e);
        }
      } catch (err) {
        if (!alive || sequence !== selectionSequence) return;
        if (!grouped()) {
          const e = state.items.find((x) => String(x.id) === String(id));
          if (e) { renderEventDetail({ event: e, context: {}, related: {} }, e); return; }
        }
        detailEl.innerHTML = `<div class="ev-empty ev-empty-detail"><span class="material-symbols-rounded" aria-hidden="true">error</span><div>${TH("events.detailsFailed", {error: err.message})}</div></div>`;
      } finally {
        if (alive && sequence === selectionSequence && reveal) revealSection(Q(".ev-col-detail", root));
      }
    };

    const populateFilters = async () => {
      try {
        const [cfg] = await Promise.all([
          fjson("/api/config").catch(() => ({})),
          loadProfileLabels(() => alive && generation === mountGeneration),
        ]);
        if (!alive) return;
        const provs = new Set();
        const pairs = [];
        const seenPairs = new Set();
        for (const p of (cfg?.pairs || [])) {
          const src = String(p.source || "").toUpperCase();
          const tgt = String(p.target || "").toUpperCase();
          if (src) provs.add(src);
          if (tgt) provs.add(tgt);
          if (src && tgt) {
            const key = [src, tgt].sort().join("-");
            if (!seenPairs.has(key)) {
              seenPairs.add(key);
              const si = String(p.source_instance || "").trim();
              const ti = String(p.target_instance || "").trim();
              const md = String(p.mode || "").toLowerCase();
              const arrow = md.includes("two") ? "↔" : "→";
              pairs.push({ value: key, label: `${instLabel(src, si)} ${arrow} ${instLabel(tgt, ti)}`, hint: T(md.includes("two") ? "events.twoWayLower" : "events.oneWayLower") });
            }
          }
        }
        const provItems = [...provs].sort();
        const provLabel = (p) => window.CW?.ProviderMeta?.label?.(p) || p;
        ddProvider.setItems([{ value: "", label: T("events.anyProvider") }, ...provItems.map((p) => ({ value: p, label: provLabel(p) }))]);
        ddTo.setItems([{ value: "", label: T("events.anyDestination") }, ...provItems.map((p) => ({ value: p, label: provLabel(p) }))]);
        ddOrigin.setItems([{ value: "", label: T("events.anyOrigin") }, ...provItems.map((p) => ({ value: p, label: p }))]);
        ddPair.setItems([{ value: "", label: T("events.anyPair") }, ...pairs]);
      } catch {}
    };

    const resetFilters = () => {
      qEl.value = "";
      quick = "all";
      renderQuick();
      clearHidden();
    };

    const reviewTitle = async (item) => {
      const action = item.acknowledged_at ? "unacknowledge" : "acknowledge";
      try {
        const res = await fjson(`/api/events/titles/${encodeURIComponent(item.id)}/${action}`, { method: "POST" });
        if (!res?.ok) throw new Error("failed");
      } catch {
        showToast(T("events.reviewFailed"), null, "error");
        return;
      }
      await load(state.page);
      await select(item.id, null, { reveal: false });
      showToast(action === "acknowledge" ? T("events.markedReviewed") : T("events.markedUnreviewed"), action === "acknowledge" ? async () => reviewTitle({ ...item, acknowledged_at: Date.now() }) : null);
    };

    const reviewAll = async () => {
      const total = state.total || 0;
      if (!total || !(await (window.CW?.MobileUI?.ask || window.confirm)(T("events.confirmReviewAll", {count: total})))) return;
      const btn = Q("#ev-review-all", quickEl);
      if (btn) btn.disabled = true;
      const query = Object.fromEntries(new URLSearchParams(titlesQuery(0)));
      const body = { confirm: true, domain };
      for (const key of ["q", "outcome", "source", "destination", "what", "run_id", "since", "until"]) if (query[key]) body[key] = query[key];
      try {
        const res = await fjson("/api/events/titles/acknowledge", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) });
        if (!res?.ok) throw new Error("failed");
        clearDetail();
        await load(0);
        showToast(T("events.titlesReviewed", {count: Number(res.titles || 0)}));
      } catch {
        showToast(T("events.reviewAllFailed"), null, "error");
      } finally {
        if (btn) btn.disabled = false;
      }
    };

    const showTitles = (runId) => {
      runFilter = String(runId || "");
      selectedDay = null;
      activity?.setSelected(null);
      setMode("titles");
    };

    const showRun = (item) => {
      const at = Number(item?.first_event_at || item?.last_event_at || 0);
      if (at) selectedDay = Math.floor((at - new Date().getTimezoneOffset() * 60) / 86400);
      activity?.setSelected(selectedDay);
      runFilter = String(item?.run_id || "");
      setMode("runs");
    };

    const PROVIDER_TOKEN = /(^|\s)(from|to):([a-z0-9_-]+)(?=\s|$)/i;
    const applySearchTokens = () => {
      if (!titles()) return false;
      let changed = false;
      for (let guard = 0; guard < 4; guard += 1) {
        const m = qEl.value.match(PROVIDER_TOKEN);
        if (!m) break;
        const want = m[3].toUpperCase();
        const known = ddProvider.items.find((v) => v === want) || ddProvider.items.find((v) => v.startsWith(want));
        if (!known) break;
        if (m[2].toLowerCase() === "from") ddProvider.value = known; else ddTo.value = known;
        qEl.value = (qEl.value.slice(0, m.index) + qEl.value.slice(m.index + m[0].length)).replace(/\s+/g, " ").trim();
        changed = true;
      }
      return changed;
    };

    let refreshing = false;
    const doRefresh = async () => {
      if (refreshing) return;
      refreshing = true;
      const btn = Q("#ev-refresh", root);
      if (btn) { btn.disabled = true; btn.classList.add("is-spinning"); }
      try {
        await Promise.all([populateFilters(), load(state.page), activity && domain === "sync" ? activity.load() : null, new Promise((done) => setTimeout(done, 450))]);
        if (alive && (state.items.length || state.runs.length)) {
          renderList();
          if (state.selected) await select(state.selected, null, {reveal: false});
        }
      } finally {
        refreshing = false;
        if (btn) { btn.disabled = false; btn.classList.remove("is-spinning"); }
      }
    };

    let searchTimer = null;
    qEl.addEventListener("input", () => {
      clearTimeout(searchTimer);
      if (runsMode()) { renderList(); return; }
      searchTimer = setTimeout(() => { applySearchTokens(); load(0); }, 250);
    });
    qEl.addEventListener("keydown", (event) => {
      if (event.key === "Enter" && applySearchTokens()) { clearTimeout(searchTimer); load(0); }
      if (event.key === "Backspace" && !qEl.value && !summaryEl.hidden) {
        const last = [...summaryEl.querySelectorAll(".ev-chip[data-k]")].pop();
        if (last) { event.preventDefault(); clearHidden(last.dataset.k); }
      }
    });
    sinceEl.addEventListener("change", () => load(0));
    untilEl.addEventListener("change", () => load(0));

    const setFiltersOpen = (open) => {
      if (open === !moreWrap.hidden) return;
      moreWrap.hidden = !open;
      moreBtn.setAttribute("aria-expanded", String(open));
      moreBtn.classList.toggle("on", open);
      if (open && window.CW?.MobileUI?.mobile()) {
        window.CW?.DialogFocus?.activate(moreWrap, () => setFiltersOpen(false));
        moreWrap.focus({preventScroll:true});
      } else if (!open) {
        for (const dd of dropdowns) dd.close();
        window.CW?.DialogFocus?.deactivate(moreWrap);
      }
    };
    moreBtn.addEventListener("click", () => setFiltersOpen(moreWrap.hidden));
    const onFiltersDown = (event) => {
      if (moreWrap.hidden || moreWrap.contains(event.target) || moreBtn.contains(event.target)) return;
      if (event.target.closest?.(".ev-dd-menu")) return;
      setFiltersOpen(false);
    };
    const onFiltersKey = (event) => {
      if (event.key !== "Escape" || moreWrap.hidden || document.querySelector(".ev-dd-menu")) return;
      setFiltersOpen(false);
      moreBtn.focus();
    };
    document.addEventListener("pointerdown", onFiltersDown, true);
    document.addEventListener("keydown", onFiltersKey);
    Q("#ev-mf-reset", root).addEventListener("click", () => resetFilters());
    Q("#ev-mf-done", root).addEventListener("click", () => { setFiltersOpen(false); moreBtn.focus(); });

    Q("#ev-refresh", root).addEventListener("click", () => doRefresh());
    Q("#ev-clear", root)?.addEventListener("click", async () => {
      const label = domain === "audit" ? "audit" : domain === "capture" ? "capture" : domain === "backup" ? "backup" : (domain === "scrobble" ? "scrobble" : "sync");
      if (!(await (window.CW?.MobileUI?.ask || window.confirm)(T("events.confirmClear", {kind: T(`events.kinds.${label}`)})))) return;
      const btn = Q("#ev-clear", root);
      btn?.classList.add("busy");
      try {
        const res = await fjson("/api/events/clear", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ confirm: true, domain }) });
        if (!alive) return;
        state.selected = null; state.collapsed.clear(); clearDetail();
        await load(0);
        if (res?.ok) showToast(T("events.archiveCleared", {count: state.total}));
        else showToast(T("events.clearFailed"), null, "error");
      } catch (err) { showToast(T("events.clearFailedDetail", {error: err.message}), null, "error"); } finally { btn?.classList.remove("busy"); }
    });
    Q("#ev-prev-top", root).addEventListener("click", () => { if (state.page > 0) load(state.page - 1); });
    Q("#ev-next-top", root).addEventListener("click", () => { if (state.page < pageCount() - 1) load(state.page + 1); });
    hideView = () => {
      setFiltersOpen(false);
      if (detailLayer) closeMobileDetail();
      scrollPosition = window.scrollY;
      endSplit();
      for (const dd of [...dropdowns, ddSort]) dd.close();
    };

    cleanup = () => {
      setFiltersOpen(false);
      if (detailLayer) closeMobileDetail();
      alive = false;
      clearTimeout(searchTimer);
      clearTimeout(toastTimer);
      window.removeEventListener("pointermove", onSplitMove, true);
      window.removeEventListener("pointerup", endSplit, true);
      document.removeEventListener("pointerdown", onFiltersDown, true);
      document.removeEventListener("keydown", onFiltersKey);
      document.body.style.userSelect = "";
      for (const dd of [...dropdowns, ddSort]) dd.close();
      statsView?.destroy();
      activity?.destroy();
      cleanup = null;
      hideView = null;
      showView = null;
    };

    syncRangeUI();
    applyView();
    void populateFilters().then(() => {
      if (alive && state.items.length) renderList();
    });
    void fjson("/api/config/meta", { cache: "no-store" }).then((m) => {
      tmdbOn = m?.tmdb_configured === true;
      if (alive && tmdbOn && titles() && state.items.length) renderList();
    }).catch(() => {});
    if (view !== "statistics") await load(0);
  },
  hide() { hideView?.(); },
  show() { showView?.(); },
  unmount() { ++mountGeneration; cleanup?.(); },
};
