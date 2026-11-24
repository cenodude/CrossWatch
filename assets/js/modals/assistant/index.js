/* assets/js/modals/assistant/index.js */
/* CrossWatch - Ask CrossWatch assistant modal */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

const T = (key, vars) => (globalThis.window?.CW?.I18n || globalThis.window?.__cwI18n)?.t?.(key, vars) ?? key;
const TH = (key, vars) => (globalThis.window?.CW?.I18n || globalThis.window?.__cwI18n)?.html?.(key, vars) ?? key;
const REQUEST_TIMEOUT_MS = 600_000;
const POLL_MS = 1500;
const POLL_MISSES = 3;
const MAX_QUESTION = 1000;
const SUGGESTIONS = [
  T("assistant.modal.q.healthy"),
  T("assistant.modal.q.lastSync"),
  T("assistant.modal.q.scrobbling"),
  T("assistant.modal.q.pairs"),
];
const WIKI_SUGGESTIONS = [
  T("assistant.modal.q.traktPair"),
  T("assistant.modal.q.notified"),
  T("assistant.modal.q.unresolved"),
  T("assistant.modal.q.captures"),
];
const WIKI_NOTE = T("assistant.modal.wikiNote");
const AI_NOTE = T("assistant.modal.aiNote");
const SCOPES = [
  ["all", T("assistant.modal.scope.all")],
  ["sync", T("assistant.modal.scope.sync")],
  ["scrobble", T("assistant.modal.scope.scrobble")],
  ["connections", T("assistant.modal.scope.connections")],
  ["settings", T("assistant.modal.scope.settings")],
];
const TOOL_LABELS = {
  compare_captures: "capture changes",
  get_scheduler_details: "scheduler",
  list_sync_reviews: "pending reviews",
  list_sync_context: "profiles and pairs",
  search_sync_items: "stored sync items",
  explain_sync_item: "item sync evidence",
  list_anime_overrides: "custom anime mappings",
  list_saved_mappings: "mappings and blocks",
  get_health: "health",
  get_last_sync: "last sync",
  list_pairs: "sync pairs",
  get_config: "settings",
  list_log_sessions: "log list",
  read_logs: "logs",
  read_log_context: "logs",
  search_events: "events",
  get_item_events: "item history",
  get_watcher_status: "watcher",
  get_analyzer_problems: "analyzer",
  get_anime_mapping_status: "anime mapping",
  list_captures: "captures",
  search_docs: "wiki",
  read_doc: "wiki",
  ask_wiki: "wiki",
  search_code: "source code",
  read_code: "source code",
};
const URL_RE = /https:\/\/(?:wiki\.crosswatch\.app|github\.com\/cenodude\/CrossWatch)(?![\w.@:-])(?:\/[^\s<>"')\]]*)?/g;

function goSettings(pane) {
  if (window.cwSettingsMenuSelect) return window.cwSettingsMenuSelect(pane);
  window.showTab?.("settings");
  window.setTimeout(() => window.cwSettingsSelect?.(pane), 0);
}

export const ACTIONS = {
  open_anime_overrides: { label: T("assistant.modal.act.anime"), icon: "animation", run: () => window.openAnimeOverridesModal?.() },
  open_mappings: {
    label: T("assistant.modal.act.mappings"),
    icon: "link",
    run: async () => {
      try {
        const mappings = await import(`/assets/js/editor/saved-mappings.js?v=${encodeURIComponent(window.APP_VERSION || "1")}`);
        mappings.open(document.activeElement);
      } catch { toast(T("assistant.modal.act.mappingsFailed")); }
    },
  },
  run_sync: {
    label: T("assistant.modal.act.runSync"),
    icon: "sync",
    run: async () => {
      if (window.syncBar?.isRunning?.()) return toast(T("assistant.modal.act.alreadyRunning"));
      if (!(await (window.CW?.MobileUI?.ask || window.confirm)(T("assistant.modal.act.confirmSync")))) return;
      window.showTab?.("main");
      window.runSync?.();
    },
  },
  open_logs: { label: T("assistant.modal.act.logs"), icon: "receipt_long", run: () => window.openLogs?.({ channel: "sync", latest: true }) },
  open_events: { label: T("assistant.modal.act.events"), icon: "event_note", run: () => window.openEvents?.() },
  open_analyzer: { label: T("assistant.modal.act.analyzer"), icon: "troubleshoot", run: () => window.openAnalyzer?.() },
  open_connections: { label: T("assistant.modal.act.connections"), icon: "hub", run: () => goSettings("providers") },
  open_pairs: { label: T("assistant.modal.act.pairs"), icon: "sync_alt", run: () => goSettings("pairs") },
  open_scrobbler: { label: T("assistant.modal.act.scrobbler"), icon: "sensors", run: () => goSettings("scrobbler") },
  open_scheduling: { label: T("assistant.modal.act.scheduling"), icon: "schedule", run: () => goSettings("scheduling") },
  open_backups: { label: T("assistant.modal.act.backups"), icon: "backup", run: () => window.showTab?.("backups") },
  open_tracker: { label: T("assistant.modal.act.tracker"), icon: "fact_check", run: () => window.showTab?.("tracker") },
  open_playlists: { label: T("assistant.modal.act.playlists"), icon: "playlist_play", run: () => window.showTab?.("playlists") },
};

const history = [];
const view = { root: null, ready: false, loaded: false, mode: "off", scope: "all", context: {} };
let busy = false;

const $ = (sel, root = document) => root.querySelector(sel);

function toast(message, ok = false) {
  try { window._cwShowToast?.(String(message || ""), !!ok); } catch {}
}

const ESCAPES = { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" };

function esc(value) {
  return String(value ?? "").replace(/[&<>"']/g, (char) => ESCAPES[char]);
}

export const PROPOSALS = {
  run_pair: { icon: "sync" },
  create_capture: { icon: "photo_camera" },
  create_backup: { icon: "backup" },
  refresh_anime_mapping: { icon: "refresh" },
  test_connections: { icon: "hub" },
};

export function proposalCards(proposals, states = []) {
  return (Array.isArray(proposals) ? proposals : []).map((proposal, index) => {
    if (!proposal || !Object.hasOwn(PROPOSALS, proposal.action)) return "";
    const state = states[index] || proposal;
    const status = ["running", "done", "failed", "dismissed"].includes(state.status) ? state.status : "ready";
    const message = state.message || ({ ready: T("assistant.modal.st.ready"), running: T("assistant.modal.st.running"), done: T("assistant.modal.st.done"), failed: T("assistant.modal.st.failed"), dismissed: T("assistant.modal.st.dismissed") })[status];
    return `<div class="as-proposal" data-as-proposal="${index}" data-state="${status}">
      <div class="as-proposal-title"><span class="material-symbols-rounded" aria-hidden="true">${PROPOSALS[proposal.action].icon}</span><strong>${esc(proposal.title)}</strong></div>
      <div class="as-proposal-status" role="status">${esc(message)}</div>
      <div class="as-proposal-buttons"><button class="as-action" type="button" data-as-proposal-run${status !== "ready" ? " disabled" : ""}>${TH("assistant.modal.run")}</button><button class="as-action" type="button" data-as-proposal-dismiss${status !== "ready" ? " disabled" : ""}>${TH("assistant.modal.dismiss")}</button></div>
    </div>`;
  }).join("");
}

function proposalState(proposal, status, message) {
  proposal.status = status;
  proposal.message = message;
  renderLog();
}

async function actionRequest(path, body, deadline) {
  const ctrl = new AbortController();
  const remaining = deadline - Date.now();
  if (remaining <= 0) throw new Error(T("assistant.modal.timedOut"));
  const timer = window.setTimeout(() => ctrl.abort(), remaining);
  const options = { credentials: "same-origin", cache: "no-store", signal: ctrl.signal };
  if (body !== undefined) {
    options.method = "POST";
    options.headers = { "Content-Type": "application/json" };
    options.body = JSON.stringify(body);
  }
  try {
    const res = await fetch(path, options);
    const data = await res.json().catch(() => null);
    if (!res.ok || !data || data.ok === false) throw new Error(String(data?.error || data?.message || T("assistant.modal.actionFailed")));
    return data;
  } catch (err) {
    if (ctrl.signal.aborted) throw new Error(T("assistant.modal.timedOut"));
    if (err instanceof TypeError) throw new Error(T("assistant.modal.unreachable"));
    throw err;
  } finally {
    window.clearTimeout(timer);
  }
}

export async function runProposal(proposal) {
  if (!proposal || !Object.hasOwn(PROPOSALS, proposal.action) || (proposal.status && proposal.status !== "ready")) return;
  proposalState(proposal, "running", T("assistant.modal.st.running"));
  const params = proposal.params || {};
  const deadline = Date.now() + 600_000;
  try {
    let message;
    if (proposal.action === "run_pair") {
      if (window.syncBar?.isRunning?.()) throw new Error(T("assistant.modal.act.alreadyRunning"));
      if (!params.pair_id || typeof window.runSync !== "function") throw new Error(T("assistant.modal.syncUnavailable"));
      window.showTab?.("main");
      const payload = await window.runSync({ pair_id: params.pair_id, startOnly: true });
      if (payload?.ok !== true) throw new Error(payload?.error || payload?.message || T("assistant.modal.syncNotStarted"));
      if (payload.skipped) throw new Error(T("assistant.modal.syncSkipped", {reason: payload.skipped}));
      message = T("assistant.modal.syncStarted");
    } else if (proposal.action === "create_capture") {
      const data = await actionRequest("/api/snapshots/create", { provider: params.provider, instance: params.instance, feature: params.feature, label: T("assistant.ask.on"), background: true }, deadline);
      const id = data.progress_id || data.job?.progress_id;
      if (!id) throw new Error(T("assistant.modal.captureUnavailable"));
      proposalState(proposal, "running", T("assistant.modal.capturing"));
      while (true) {
        if (Date.now() >= deadline) throw new Error(T("assistant.modal.captureTimeout"));
        await new Promise(resolve => window.setTimeout(resolve, Math.min(1500, deadline - Date.now())));
        const data = await actionRequest(`/api/snapshots/capture-progress/${encodeURIComponent(id)}`, undefined, deadline);
        const progress = data.progress || {};
        if (progress.done) {
          if (progress.ok === false || progress.error) throw new Error(String(progress.error || progress.message || T("assistant.modal.captureFailed")));
          message = T("assistant.modal.captureSaved");
          break;
        }
        const percent = Math.max(0, Math.min(100, Number(progress.percent) || 0));
        proposalState(proposal, "running", T("assistant.modal.capturingPercent", {percent: `${Math.round(percent)}%`}));
      }
    } else if (proposal.action === "create_backup") {
      const data = await actionRequest("/api/backups/create", { managed: true }, deadline);
      const id = data.job?.id;
      if (!id) throw new Error(T("assistant.modal.backupUnavailable"));
      proposalState(proposal, "running", T("assistant.modal.backupQueued"));
      while (true) {
        if (Date.now() >= deadline) throw new Error(T("assistant.modal.backupRunning"));
        await new Promise(resolve => window.setTimeout(resolve, Math.min(2000, deadline - Date.now())));
        const status = await actionRequest("/api/backups/status", undefined, deadline);
        const job = (status.jobs || []).find(row => row?.id === id);
        if (!job) throw new Error(T("assistant.modal.backupGone"));
        if (job.stage === "complete") {
          message = job.warning ? T("assistant.modal.backupSavedWarning", {warning: job.warning}) : T("assistant.modal.backupSaved");
          break;
        }
        if (job.stage === "failed") throw new Error(String(job.error || T("assistant.modal.backupFailed")));
        proposalState(proposal, "running", T("assistant.modal.backupStage", {stage: String(job.stage || "running")}));
      }
    } else if (proposal.action === "refresh_anime_mapping") {
      const data = await actionRequest("/api/anime-mapping/update", { force: false }, deadline);
      message = data.updated ? (data.identity_updated && !data.mappings_updated ? T("assistant.modal.animeIdentity") : T("assistant.modal.animeUpdated"))
        : data.rebuilt ? T("assistant.modal.animeRebuilt") : T("assistant.modal.animeCurrent");
    } else {
      const data = await actionRequest("/api/status?fresh=1", undefined, deadline);
      message = Object.entries(data.providers || {}).map(([name, info]) => `${name}: ${info?.connected ? T("assistant.modal.connectedLower") : `${T("assistant.modal.notConnected")}${info?.reason ? ` (${info.reason})` : ""}`}`).join("\n") || T("assistant.modal.noConnections");
    }
    proposalState(proposal, "done", message);
  } catch (err) {
    proposalState(proposal, "failed", String(err?.message || T("assistant.modal.actionFailed")));
  }
}

export function evidenceButtons(evidence, messageIndex) {
  const kinds = new Set(["log", "event", "mappings", "anime", "analyzer", "captures", "scheduling", "reviews"]);
  return `<div class="as-actions">${(Array.isArray(evidence) ? evidence : []).slice(0, 8).map((ref, index) =>
    ref && kinds.has(ref.kind) ? `<button class="as-action" type="button" data-as-evidence="${messageIndex}:${index}">${esc(ref.label)}</button>` : "").join("")}</div>`;
}

export function openEvidence(ref) {
  if (!ref) return;
  if (ref.kind === "log" && typeof ref.session_id === "string" && ["sync", "watcher", "debug", "captures", "backups", "audits", "system"].includes(ref.channel)) window.openLogs?.({sessionId: ref.session_id, channel: ref.channel, runId: ref.run_id});
  else if (ref.kind === "event" && Number.isSafeInteger(ref.group_id) && ref.group_id > 0) window.openEvents?.({groupId: ref.group_id, visibility: "all"});
  else if (ref.kind === "mappings") ACTIONS.open_mappings.run();
  else if (ref.kind === "anime") ACTIONS.open_anime_overrides.run();
  else if (ref.kind === "analyzer") window.openAnalyzer?.();
  else if (ref.kind === "captures") window.showTab?.("snapshots");
  else if (ref.kind === "scheduling") goSettings("scheduling");
  else if (ref.kind === "reviews" && typeof ref.session_id === "string" && /^[a-zA-Z0-9_-]{1,128}$/.test(ref.session_id)) {
    window.location.hash = `interactive_sync?session=${encodeURIComponent(ref.session_id)}`;
    window.showTab?.("interactive_sync");
  }
}

export function linkify(text, format = (part) => part) {
  const raw = String(text ?? "");
  let out = "";
  let last = 0;
  for (const match of raw.matchAll(URL_RE)) {
    const url = match[0].replace(/[.,;:!?*]+$/, "");
    out += format(esc(raw.slice(last, match.index)));
    out += `<a href="${esc(url)}" target="_blank" rel="noopener noreferrer">${esc(url)}</a>`;
    last = match.index + url.length;
  }
  return out + format(esc(raw.slice(last)));
}

const bold = (part) => part.replace(/\*\*([^*\n]+)\*\*/g, "<strong>$1</strong>");
const MD_LINK_RE = /\[([^\]\n]{1,200})\]\((https:\/\/[^\s)]+)\)/g;

function allowedUrl(url) {
  const found = [...String(url).matchAll(URL_RE)];
  return found.length === 1 && found[0].index === 0 && found[0][0] === url;
}

function links(text) {
  const raw = String(text);
  let out = "";
  let last = 0;
  for (const match of raw.matchAll(MD_LINK_RE)) {
    if (!allowedUrl(match[2])) continue;
    out += linkify(raw.slice(last, match.index), bold);
    out += `<a href="${esc(match[2])}" target="_blank" rel="noopener noreferrer">${bold(esc(match[1]))}</a>`;
    last = match.index + match[0].length;
  }
  return out + linkify(raw.slice(last), bold);
}

function inline(text) {
  return String(text)
    .split(/(`[^`\n]+`)/)
    .map((part) => (part.length > 2 && part.startsWith("`") && part.endsWith("`") ? `<code>${esc(part.slice(1, -1))}</code>` : links(part)))
    .join("");
}

export function followUps(items) {
  const list = [];
  for (const item of Array.isArray(items) ? items : []) {
    const question = String(item ?? "").trim().slice(0, 200);
    if (question && !list.includes(question) && list.length < 3) list.push(question);
  }
  if (!list.length) return "";
  return `<div class="as-follow"><span>${TH("assistant.modal.askNext")}</span>${list
    .map((question) => `<button class="as-chip" type="button" data-as-ask="${esc(question)}"><span>${esc(question)}</span><span class="material-symbols-rounded" aria-hidden="true">north_east</span></button>`)
    .join("")}</div>`;
}

export function renderAnswer(text) {
  let html = "";
  let list = "";
  let para = [];
  const flushPara = () => {
    if (para.length) html += `<p>${para.map(inline).join("<br>")}</p>`;
    para = [];
  };
  const flushList = () => {
    if (list) html += `</${list}>`;
    list = "";
  };
  for (const raw of String(text ?? "").replace(/\r/g, "").split("\n")) {
    const line = raw.trimEnd();
    const bullet = /^\s*[-*•]\s+(.*)$/.exec(line);
    const number = /^\s*\d+[.)]\s+(.*)$/.exec(line);
    if (bullet || number) {
      flushPara();
      const kind = bullet ? "ul" : "ol";
      if (list !== kind) {
        flushList();
        html += `<${kind}>`;
        list = kind;
      }
      html += `<li>${inline((bullet || number)[1])}</li>`;
    } else if (!line.trim()) {
      flushPara();
      flushList();
    } else {
      flushList();
      para.push(line.replace(/^#{1,6}\s+/, ""));
    }
  }
  flushPara();
  flushList();
  return html;
}

export function lookedAt(tools) {
  const names = [];
  for (const tool of Array.isArray(tools) ? tools : []) {
    const label = TOOL_LABELS[tool];
    if (label && !names.includes(label)) names.push(label);
  }
  return names.length ? T("assistant.modal.lookedAt", {names: names.join(", ")}) : "";
}

const WIKI_PAGE_RE = /^https:\/\/wiki\.crosswatch\.app\/[\w./-]*$/;

export function sourceLinks(sources) {
  const seen = new Set();
  const links = [];
  for (const item of Array.isArray(sources) ? sources : []) {
    const url = String(item?.url ?? "");
    if (!WIKI_PAGE_RE.test(url) || seen.has(url) || links.length >= 3) continue;
    seen.add(url);
    const title = String(item?.title ?? "").trim() || url;
    links.push(`<a href="${esc(url)}" target="_blank" rel="noopener noreferrer">${esc(title)}</a>`);
  }
  return links.length ? `<small class="as-sources">Wiki: ${links.join(", ")}</small>` : "";
}

export function actionButtons(actions) {
  const known = [...new Set(Array.isArray(actions) ? actions : [])].filter((name) => Object.hasOwn(ACTIONS, name));
  if (!known.length) return "";
  return `<div class="as-actions">${known
    .map((name) => `<button type="button" class="as-action" data-as-action="${esc(name)}"><span class="material-symbols-rounded" aria-hidden="true">${ACTIONS[name].icon}</span><span>${esc(ACTIONS[name].label)}</span></button>`)
    .join("")}</div>`;
}

function when(at) {
  const age = Math.max(0, Date.now() - Number(at || 0)) / 1000;
  if (age < 60) return T("assistant.modal.justNow");
  if (age < 3600) return T("playlists.ago.minutes", {count: Math.floor(age / 60)});
  return new Date(Number(at)).toLocaleTimeString((globalThis.window?.CW?.I18n || globalThis.window?.__cwI18n)?.locale, { hour: "2-digit", minute: "2-digit" });
}

function failure(status) {
  if (status === 504 || status === 524) {
    return T("assistant.modal.proxyCut", {status: String(status)});
  }
  return `HTTP ${status}`;
}

async function api(path, body) {
  const ctrl = new AbortController();
  const timeout = window.setTimeout(() => ctrl.abort(), REQUEST_TIMEOUT_MS);
  const opts = { cache: "no-store", credentials: "same-origin", signal: ctrl.signal };
  if (body !== undefined) {
    opts.method = "POST";
    opts.headers = { "Content-Type": "application/json" };
    opts.body = JSON.stringify(body);
  }
  try {
    const res = await fetch("/api/assistant" + path, opts);
    const data = await res.json().catch(() => null);
    if (!res.ok || data?.ok === false) {
      const err = new Error(String(data?.error || failure(res.status)));
      err.answered = data !== null;
      throw err;
    }
    return data;
  } catch (err) {
    if (err?.name === "AbortError") throw new Error(T("assistant.modal.tooLong"));
    throw err;
  } finally {
    window.clearTimeout(timeout);
  }
}

async function chat(payload) {
  const started = Date.now();
  const job = await api("/jobs", payload);
  let misses = 0;
  while (Date.now() - started < REQUEST_TIMEOUT_MS) {
    await new Promise((resolve) => window.setTimeout(resolve, POLL_MS));
    let data;
    try {
      data = await api(`/jobs/${encodeURIComponent(String(job.job_id || ""))}`);
    } catch (err) {
      if (err?.answered || ++misses >= POLL_MISSES) throw err;
      continue;
    }
    misses = 0;
    if (!data.pending) return data;
  }
  throw new Error(T("assistant.modal.tooLong"));
}

function injectCss() {
  if (document.getElementById("cw-assistant-css")) return Promise.resolve();
  const link = document.createElement("link");
  const cssUrl = new URL("./styles.css", import.meta.url);
  const version = new URL(import.meta.url).searchParams.get("v") || window.__CW_VERSION__;
  if (version) cssUrl.searchParams.set("v", version);
  link.id = "cw-assistant-css";
  link.rel = "stylesheet";
  link.href = cssUrl.href;
  return new Promise((resolve) => {
    link.addEventListener("load", resolve, { once: true });
    link.addEventListener("error", resolve, { once: true });
    document.head.appendChild(link);
  });
}

function closeModal() {
  if (window.cxCloseModal) { window.cxCloseModal(); return; }
  document.querySelector(".cx-modal-shell")?.dispatchEvent(new CustomEvent("cw-modal-close", { bubbles: true }));
}

const AVATAR = '<img class="as-avatar" src="/assets/img/CROSSWATCH.svg" alt="" aria-hidden="true">';

function botCard(inner, extra = "") {
  return `<div class="as-msg as-bot${extra}">${AVATAR}<div class="as-bot-body">${inner}</div></div>`;
}

function messageHtml(item, index) {
  if (item.role === "user") return `<div class="as-msg as-user">${esc(item.content)}</div>`;
  const meta = `<div class="as-meta"><strong>CrossWatch</strong><span>${esc(when(item.at))}</span></div>`;
  if (item.error) return botCard(`${meta}<div class="as-answer"><p>${esc(item.content)}</p></div>`, " as-error");
  const note = lookedAt(item.tools);
  return botCard(`${meta}<div class="as-answer">${renderAnswer(item.content)}</div>${note ? `<small class="as-looked">${esc(note)}</small>` : ""}${sourceLinks(item.sources)}${followUps(item.followups)}${actionButtons(item.actions)}${evidenceButtons(item.evidence, index)}<div class="as-proposals" data-as-message="${index}">${proposalCards(item.proposals)}</div>`);
}

function emptyHtml() {
  if (!view.ready) {
    return `
      <div class="as-empty">
        <strong>${TH("assistant.modal.notSetUp")}</strong>
        <p>${TH("assistant.modal.notSetUpHint")}</p>
        <button class="as-action" type="button" id="as-setup"><span class="material-symbols-rounded" aria-hidden="true">settings</span><span>${TH("assistant.modal.openSettings")}</span></button>
      </div>`;
  }
  return `
    <div class="as-try">
      <span>${TH("assistant.modal.tryAsking")}</span>
      ${(view.mode === "wiki" ? WIKI_SUGGESTIONS : SUGGESTIONS).map((text) => `<button class="as-chip" type="button" data-as-ask="${esc(text)}"><span>${esc(text)}</span><span class="material-symbols-rounded" aria-hidden="true">north_east</span></button>`).join("")}
    </div>`;
}

function renderLog() {
  const root = view.root;
  const log = root ? $("#as-log", root) : null;
  if (!log) return;
  const chatting = history.length > 0 || busy;
  $(".cw-assistant", root)?.classList?.toggle("is-chatting", chatting);
  const form = $("#as-form", root);
  const slot = $(chatting ? "#as-foot" : "#as-top", root);
  if (form && slot && form.parentElement !== slot) slot.appendChild?.(form);
  const fresh = $("#as-new", root);
  if (fresh) fresh.hidden = !history.length || busy;
  if (!view.loaded) {
    log.innerHTML = "";
    return;
  }
  if (!chatting) {
    log.innerHTML = emptyHtml();
    return;
  }
  const waiting = view.mode === "wiki" ? T("assistant.modal.searchingWiki") : T("assistant.modal.looking");
  const wait = busy ? botCard(`<div class="as-meta"><strong>CrossWatch</strong><span>${waiting}</span></div><div class="as-dots" aria-hidden="true"><i></i><i></i><i></i></div>`, " as-wait") : "";
  log.innerHTML = history.map(messageHtml).join("") + wait;
  const scroller = $("#as-scroll", root);
  if (scroller) scroller.scrollTop = scroller.scrollHeight;
}

function setBusy(value) {
  busy = value;
  const root = view.root;
  const input = root ? $("#as-input", root) : null;
  const send = root ? $("#as-send", root) : null;
  if (input) input.disabled = value || !view.ready;
  if (send) send.disabled = value || !view.ready;
  renderLog();
}

function updateCount() {
  const root = view.root;
  const input = root ? $("#as-input", root) : null;
  const count = root ? $("#as-count", root) : null;
  if (input && count) count.textContent = `${String(input.value || "").length}/${MAX_QUESTION}`;
}

async function ask(question) {
  const text = String(question || "").trim().slice(0, MAX_QUESTION);
  if (!text || busy || !view.ready) return;
  history.push({ role: "user", content: text, at: Date.now() });
  const input = view.root ? $("#as-input", view.root) : null;
  if (input) input.value = "";
  updateCount();
  setBusy(true);
  try {
    const messages = history.filter((item) => !item.error).map(({ role, content }) => ({ role, content }));
    const data = await chat({ messages, scope: view.scope, context: view.context });
    const proposals = (Array.isArray(data.proposals) ? data.proposals : []).filter(p => p && Object.hasOwn(PROPOSALS, p.action)).slice(0, 3).map(p => ({ action: p.action, title: p.title, params: p.params, status: "ready" }));
    history.push({ role: "assistant", content: data.reply || T("assistant.modal.empty"), tools: data.tools || [], actions: data.actions || [], sources: data.sources || [], followups: data.followups || [], evidence: data.evidence || [], proposals, at: Date.now() });
  } catch (err) {
    history.push({ role: "assistant", content: err?.message || T("assistant.modal.wrong"), error: true, at: Date.now() });
  }
  setBusy(false);
  if (view.root) $("#as-input", view.root)?.focus?.();
}

export default {
  async mount(root, props = {}) {
    view.context = props.context && typeof props.context === "object" ? {...props.context} : {};
    await injectCss();
    root.closest(".cx-modal-shell")?.classList.add("cw-assistant-shell");
    root.innerHTML = `
      <div class="cw-assistant">
        <div class="cx-head as-head">
          <span class="as-mark material-symbols-rounded" aria-hidden="true">auto_awesome</span>
          <div class="as-titles">
            <h2>${TH("assistant.modal.title")}</h2>
            <p id="as-sub">${TH("assistant.modal.subtitle")}</p>
          </div>
          <button class="as-icon" type="button" id="as-new" title="${TH("assistant.modal.newChat")}" aria-label="${TH("assistant.modal.newChat")}" hidden><span class="material-symbols-rounded" aria-hidden="true">edit_square</span></button>
          <button class="as-icon" type="button" id="as-close" title="${TH("assistant.modal.close")}" aria-label="${TH("assistant.modal.close")}"><span class="material-symbols-rounded" aria-hidden="true">close</span></button>
        </div>
        <div class="as-scroll" id="as-scroll">
          <div class="as-notice" id="as-notice" hidden>
            <span class="material-symbols-rounded" aria-hidden="true">info</span>
            <p><strong>${TH("assistant.modal.limited")}</strong> ${TH("assistant.modal.limitedHint")}</p>
            <button class="as-action" type="button" data-as-setup><span class="material-symbols-rounded" aria-hidden="true">settings</span><span>${TH("assistant.modal.setupAi")}</span></button>
          </div>
          <div class="as-top" id="as-top">
            <form class="as-composer" id="as-form" autocomplete="off">
              <textarea id="as-input" rows="3" maxlength="${MAX_QUESTION}" placeholder="${TH("assistant.modal.placeholder")}" aria-label="${TH("assistant.modal.yourQuestion")}" disabled></textarea>
              <div class="as-bar">
                <label class="as-scope"><select id="as-scope" aria-label="${TH("assistant.modal.scopeLabel")}">${SCOPES.map(([value, label]) => `<option value="${value}"${value === view.scope ? " selected" : ""}>${label}</option>`).join("")}</select></label>
                <span class="as-count" id="as-count">0/${MAX_QUESTION}</span>
                <button class="as-send" type="submit" id="as-send" disabled><span class="material-symbols-rounded" aria-hidden="true">send</span><span>${TH("assistant.modal.send")}</span></button>
              </div>
            </form>
            <small class="as-looked" id="as-context" hidden></small>
          </div>
          <div class="as-log" id="as-log" aria-live="polite"></div>
        </div>
        <div class="as-foot" id="as-foot"></div>
        <p class="as-note" id="as-note">${AI_NOTE}</p>
      </div>`;

    view.root = root;
    view.ready = false;
    view.mode = "off";
    view.loaded = false;
    setBusy(busy);
    try {
      const conf = await api("/settings");
      if (view.root !== root) return;
      view.mode = ["full", "wiki", "off"].includes(conf.mode) ? conf.mode : conf.ready ? "full" : "off";
      view.ready = view.mode !== "off";
      const wiki = view.mode === "wiki";
      if (view.mode === "full") $("#as-sub", root).textContent = T("assistant.modal.usingModel", {model: conf.model});
      if (wiki) $("#as-sub", root).textContent = T("assistant.modal.wikiAnswers");
      $("#as-notice", root).hidden = !wiki;
      $(".as-scope", root).hidden = wiki;
      $("#as-note", root).textContent = wiki ? WIKI_NOTE : AI_NOTE;
      const about = view.context.item_key ? "the selected item" : view.context.pair_id ? "the selected pair" : "";
      $("#as-context", root).textContent = about ? T("assistant.modal.askingAbout", {about}) : "";
      $("#as-context", root).hidden = view.mode !== "full" || !about;
    } catch {}
    if (view.root !== root) return;
    view.loaded = true;
    setBusy(busy);
    updateCount();

    root.addEventListener("click", (event) => {
      const target = event.target;
      const evidence = target.closest("[data-as-evidence]");
      if (evidence) {
        const [message, index] = evidence.getAttribute("data-as-evidence").split(":").map(Number);
        const ref = history[message]?.evidence?.[index];
        if (ref) { closeModal(); openEvidence(ref); }
        return;
      }
      const run = target.closest("[data-as-proposal-run]");
      const dismiss = target.closest("[data-as-proposal-dismiss]");
      if (run || dismiss) {
        const card = target.closest("[data-as-proposal]");
        const group = target.closest("[data-as-message]");
        const proposal = history[Number(group?.getAttribute("data-as-message"))]?.proposals?.[Number(card?.getAttribute("data-as-proposal"))];
        if (proposal && proposal.status === "ready") {
          if (run) void runProposal(proposal);
          else proposalState(proposal, "dismissed", T("assistant.modal.st.dismissed"));
        }
        return;
      }
      if (target.closest("#as-close")) closeModal();
      if (target.closest("#as-new") && !busy) {
        history.length = 0;
        renderLog();
        $("#as-input", root)?.focus?.();
      }
      if (target.closest("#as-setup, [data-as-setup]")) {
        closeModal();
        goSettings("app");
        window.setTimeout(() => window.cwUiSettingsJump?.("security"), 60);
      }
      const chip = target.closest("[data-as-ask]");
      if (chip) ask(chip.getAttribute("data-as-ask"));
      const action = target.closest("[data-as-action]");
      const name = action ? action.getAttribute("data-as-action") : "";
      if (name && Object.hasOwn(ACTIONS, name)) {
        closeModal();
        ACTIONS[name].run();
      }
    });
    $("#as-form", root).addEventListener("submit", (event) => {
      event.preventDefault();
      ask($("#as-input", root).value);
    });
    $("#as-input", root).addEventListener("keydown", (event) => {
      if (event.key === "Enter" && !event.shiftKey) {
        event.preventDefault();
        ask(event.target.value);
      }
    });
    $("#as-input", root).addEventListener("input", updateCount);
    $("#as-scope", root).addEventListener("change", (event) => {
      view.scope = String(event.target.value || "all");
    });
    if (view.mode === "full" && props?.question) ask(props.question);
    else if (view.ready && !busy) $("#as-input", root).focus?.();
  },
  unmount() {
    view.root = null;
  },
};
