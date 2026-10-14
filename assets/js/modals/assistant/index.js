/* assets/js/modals/assistant/index.js */
/* CrossWatch - Ask CrossWatch assistant modal */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

const REQUEST_TIMEOUT_MS = 600_000;
const MAX_QUESTION = 1000;
const SUGGESTIONS = [
  "Is everything healthy?",
  "Why did my last sync fail?",
  "Is scrobbling working right now?",
  "Are my sync pairs set up sensibly?",
];
const SCOPES = [
  ["all", "All of CrossWatch"],
  ["sync", "Syncing"],
  ["scrobble", "Scrobbling"],
  ["connections", "Connections"],
  ["settings", "Settings"],
];
const TOOL_LABELS = {
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
  run_sync: {
    label: "Run sync",
    icon: "sync",
    run: () => {
      if (window.syncBar?.isRunning?.()) return toast("A sync is already running");
      if (!window.confirm("Start a sync of all pairs now?")) return;
      window.showTab?.("main");
      window.runSync?.();
    },
  },
  open_logs: { label: "Open logs", icon: "receipt_long", run: () => window.openLogs?.({ channel: "sync", latest: true }) },
  open_events: { label: "Open events", icon: "event_note", run: () => window.openEvents?.() },
  open_analyzer: { label: "Open analyzer", icon: "troubleshoot", run: () => window.openAnalyzer?.() },
  open_connections: { label: "Open connections", icon: "hub", run: () => goSettings("providers") },
  open_pairs: { label: "Open sync pairs", icon: "sync_alt", run: () => goSettings("pairs") },
  open_scrobbler: { label: "Open scrobbler", icon: "sensors", run: () => goSettings("scrobbler") },
  open_scheduling: { label: "Open scheduling", icon: "schedule", run: () => goSettings("scheduling") },
};

const history = [];
const view = { root: null, ready: false, loaded: false, scope: "all" };
let busy = false;

const $ = (sel, root = document) => root.querySelector(sel);

function toast(message, ok = false) {
  try { window._cwShowToast?.(String(message || ""), !!ok); } catch {}
}

function esc(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
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

function inline(text) {
  return String(text)
    .split(/(`[^`\n]+`)/)
    .map((part) => (part.length > 2 && part.startsWith("`") && part.endsWith("`") ? `<code>${esc(part.slice(1, -1))}</code>` : linkify(part, bold)))
    .join("");
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
  return names.length ? `Looked at: ${names.join(", ")}` : "";
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
  if (age < 60) return "Just now";
  if (age < 3600) return `${Math.floor(age / 60)} min ago`;
  return new Date(Number(at)).toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit" });
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
    if (!res.ok || data?.ok === false) throw new Error(String(data?.error || `HTTP ${res.status}`));
    return data;
  } catch (err) {
    if (err?.name === "AbortError") throw new Error("That took too long. Try a narrower question.");
    throw err;
  } finally {
    window.clearTimeout(timeout);
  }
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

function messageHtml(item) {
  if (item.role === "user") return `<div class="as-msg as-user">${esc(item.content)}</div>`;
  const meta = `<div class="as-meta"><strong>CrossWatch</strong><span>${esc(when(item.at))}</span></div>`;
  if (item.error) return botCard(`${meta}<div class="as-answer"><p>${esc(item.content)}</p></div>`, " as-error");
  const note = lookedAt(item.tools);
  return botCard(`${meta}<div class="as-answer">${renderAnswer(item.content)}</div>${note ? `<small class="as-looked">${esc(note)}</small>` : ""}${actionButtons(item.actions)}`);
}

function emptyHtml() {
  if (!view.ready) {
    return `
      <div class="as-empty">
        <strong>The assistant is not set up yet</strong>
        <p>Choose an AI provider and model under Settings, UI and Security, Security.</p>
        <button class="as-action" type="button" id="as-setup"><span class="material-symbols-rounded" aria-hidden="true">settings</span><span>Open settings</span></button>
      </div>`;
  }
  return `
    <div class="as-try">
      <span>Try asking</span>
      ${SUGGESTIONS.map((text) => `<button class="as-chip" type="button" data-as-ask="${esc(text)}"><span>${esc(text)}</span><span class="material-symbols-rounded" aria-hidden="true">north_east</span></button>`).join("")}
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
  const wait = busy ? botCard(`<div class="as-meta"><strong>CrossWatch</strong><span>Looking at your CrossWatch</span></div><div class="as-dots" aria-hidden="true"><i></i><i></i><i></i></div>`, " as-wait") : "";
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
    const data = await api("/chat", { messages, scope: view.scope });
    history.push({ role: "assistant", content: data.reply || "The model gave an empty answer.", tools: data.tools || [], actions: data.actions || [], at: Date.now() });
  } catch (err) {
    history.push({ role: "assistant", content: err?.message || "Something went wrong.", error: true, at: Date.now() });
  }
  setBusy(false);
  if (view.root) $("#as-input", view.root)?.focus?.();
}

export default {
  async mount(root, props = {}) {
    await injectCss();
    root.closest(".cx-modal-shell")?.classList.add("cw-assistant-shell");
    root.innerHTML = `
      <div class="cw-assistant">
        <div class="cx-head as-head">
          <span class="as-mark material-symbols-rounded" aria-hidden="true">auto_awesome</span>
          <div class="as-titles">
            <h2>Ask CrossWatch</h2>
            <p id="as-sub">Questions about your syncs, scrobbling, connections or settings.</p>
          </div>
          <button class="as-icon" type="button" id="as-new" title="New chat" aria-label="New chat" hidden><span class="material-symbols-rounded" aria-hidden="true">edit_square</span></button>
          <button class="as-icon" type="button" id="as-close" title="Close" aria-label="Close"><span class="material-symbols-rounded" aria-hidden="true">close</span></button>
        </div>
        <div class="as-scroll" id="as-scroll">
          <div class="as-top" id="as-top">
            <form class="as-composer" id="as-form" autocomplete="off">
              <textarea id="as-input" rows="3" maxlength="${MAX_QUESTION}" placeholder="Ask a question about CrossWatch" aria-label="Your question" disabled></textarea>
              <div class="as-bar">
                <label class="as-scope"><select id="as-scope" aria-label="What the question is about">${SCOPES.map(([value, label]) => `<option value="${value}"${value === view.scope ? " selected" : ""}>${label}</option>`).join("")}</select></label>
                <span class="as-count" id="as-count">0/${MAX_QUESTION}</span>
                <button class="as-send" type="submit" id="as-send" disabled><span class="material-symbols-rounded" aria-hidden="true">send</span><span>Send</span></button>
              </div>
            </form>
          </div>
          <div class="as-log" id="as-log" aria-live="polite"></div>
        </div>
        <div class="as-foot" id="as-foot"></div>
        <p class="as-note">Answers come from the AI model you chose and can be wrong. It can only read, never change anything.</p>
      </div>`;

    view.root = root;
    view.ready = false;
    view.loaded = false;
    setBusy(busy);
    try {
      const conf = await api("/settings");
      if (view.root !== root) return;
      view.ready = !!conf.ready;
      if (view.ready) $("#as-sub", root).textContent = `Read-only help, using ${conf.model}`;
    } catch {}
    if (view.root !== root) return;
    view.loaded = true;
    setBusy(busy);
    updateCount();

    root.addEventListener("click", (event) => {
      const target = event.target;
      if (target.closest("#as-close")) closeModal();
      if (target.closest("#as-new") && !busy) {
        history.length = 0;
        renderLog();
        $("#as-input", root)?.focus?.();
      }
      if (target.closest("#as-setup")) {
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
    if (view.ready && props?.question) ask(props.question);
    else if (view.ready && !busy) $("#as-input", root).focus?.();
  },
  unmount() {
    view.root = null;
  },
};
