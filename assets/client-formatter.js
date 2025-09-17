(function (w, d) {
  // Client-side log formatter used by the UI to render server log streams.
  // It injects lightweight styles, prettifies known JSON events, filters
  // unhelpful plain lines, and outputs concise, readable HTML blocks. The
  // implementation is side-effect free aside from DOM insertion.

  // ---------- styles (once) ----------
  // Insert a small stylesheet exactly once to style the rendered log output.
  if (!d.getElementById("cf-styles")) {
    const style = d.createElement("style");
    style.id = "cf-styles";
    style.textContent = `
.cf-log{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;font-size:13px;line-height:1.35}
.cf-line,.cf-event{display:block;margin:1px 0}
.cf-event{padding:4px 6px;border-left:2px solid rgba(255,255,255,.12);border-radius:4px;background:rgba(255,255,255,.02)}
.cf-event .cf-ico{margin-right:8px}
.cf-event .cf-meta{opacity:.85;font-size:12px;margin-left:4px}
.cf-event .cf-meta b{opacity:1}
.cf-sep{opacity:.6;margin:0 4px}
.cf-event.start{border-color:#9aa0a6}
.cf-event.pair{border-color:#8ab4f8}
.cf-event.plan{border-color:#cfcfcf}
.cf-event.remove{border-color:#ef5350}
.cf-event.add{border-color:#66bb6a}
.cf-event.done{border-color:#2fb170}
.cf-event.complete{border-color:#25a05f;font-weight:700;position:relative;overflow:hidden}
.cf-muted{opacity:.72}
.cf-ok{color:#2fb170;font-weight:600}
.cf-ok-strong{color:#25a05f;font-weight:700}
.cf-arrow{opacity:.9;margin:0 6px}

/* badges */
.cf-badge{display:inline-flex;align-items:center;gap:6px;padding:2px 8px;border-radius:999px;font-weight:700;font-size:12px;line-height:1.2;border:1px solid rgba(255,255,255,.15);margin:0 2px;vertical-align:baseline;box-shadow:0 0 .5px rgba(255,255,255,.15) inset,0 0 6px rgba(255,255,255,.03) inset}
.cf-badge img{width:14px;height:14px;display:block;filter:drop-shadow(0 0 1px rgba(0,0,0,.25))}
.cf-plex{background:#2b240a;color:#ffbf3a;border-color:rgba(255,191,58,.28)}
.cf-simkl{background:#072430;color:#35d1ff;border-color:rgba(53,209,255,.28)}
.cf-trakt{background:#2b0a0a;color:#ff6470;border-color:rgba(255,100,112,.28)}
.cf-generic{background:#1b1b1b;color:#eaeaea}

/* animations */
.cf-fade-in{animation:cfFade .14s ease-out}
.cf-pop{animation:cfPop .18s ease-out}
.cf-slide-in{animation:cfSlide .18s ease-out}
.cf-pulse{animation:cfPulse .6s ease-out}
.cf-complete-shimmer:after{content:"";position:absolute;inset:0;pointer-events:none;background:linear-gradient(110deg,transparent 0%,rgba(255,255,255,.05) 40%,transparent 60%);transform:translateX(-120%);animation:cfShimmer 900ms ease-out 1}
@keyframes cfFade{from{opacity:0;transform:translateY(1px)}to{opacity:1;transform:none}}
@keyframes cfPop{from{transform:scale(.98)}to{transform:scale(1)}}
@keyframes cfSlide{from{transform:translateX(-6px);opacity:.0}to{transform:none;opacity:1}}
@keyframes cfPulse{0%{box-shadow:0 0 0 0 rgba(138,180,248,.35)}100%{box-shadow:0 0 0 10px rgba(138,180,248,0)}}
@keyframes cfShimmer{to{transform:translateX(120%)}}
@media (prefers-reduced-motion: reduce){
  .cf-fade-in,.cf-pop,.cf-slide-in,.cf-pulse{animation:none}
  .cf-complete-shimmer:after{display:none}
}
`.trim();
    d.head.appendChild(style);
  }

  // ---------- utils ----------
  // Small helpers for HTML-escaping and generating consistent labels/icons.
  /**
   * Escape a string for safe HTML insertion.
   * @param {any} s
   * @returns {string}
   */
  const esc = s => String(s ?? "").replace(/[&<>]/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;'}[m]));
  // Map provider keys to badge CSS classes.
  const BADGE_CLASS = { PLEX: "cf-plex", SIMKL: "cf-simkl", TRAKT: "cf-trakt" };
  // Map provider keys to small logo images.
  const LOGO_SRC    = { PLEX: "/assets/PLEX-log.svg", SIMKL: "/assets/SIMKL-log.svg", TRAKT: "/assets/TRAKT-log.svg" };
  // Simple icon set used across event types.
  const ICON        = { start:"▶", pair:"🔗", plan:"📝", add:"➕", remove:"➖", done:"✅", complete:"🏁" };
  /**
   * Choose an arrow based on mode (two-way uses a bidirectional glyph).
   * @param {string} m
   */
  const arrowFor = m => (String(m||"").toLowerCase().startsWith("two") ? "⇄" : "→");
  /**
   * Capitalize the first character (for labels).
   * @param {string} s
   */
  const capitalize = s => String(s||"").replace(/^./, c => c.toUpperCase());
  /**
   * Render a small provider badge with optional logo.
   * @param {string} name
   * @returns {string} HTML string
   */
  const badge = name => {
    const key = String(name||"").toUpperCase();
    const cls = BADGE_CLASS[key] || "cf-generic";
    const src = LOGO_SRC[key] || "";
    const icon = src ? `<img src="${src}" alt="" aria-hidden="true">` : "";
    return `<span class="cf-badge ${cls}">${icon}${esc(key)}</span>`;
  };
  /**
   * Build a standardized HTML block for a log event.
   * @param {"start"|"pair"|"plan"|"add"|"remove"|"done"|"complete"} type
   * @param {string} titleHTML already-escaped title HTML
   * @param {string} metaText plain metadata text (escaped internally)
   * @param {string=} extra optional extra class names
   * @returns {string}
   */
  const htmlBlock = (type, titleHTML, metaText, extra) => {
    const add = (extra ? ` ${extra}` : "");
    const base = type === "start" ? "cf-slide-in cf-pulse" :
                 type === "complete" ? "cf-fade-in cf-complete-shimmer" :
                 "cf-fade-in";
    const cls = `cf-event ${type} ${base}${add}`;
    const meta = metaText ? `<span class="cf-meta">${metaText}</span>` : "";
    const sep  = metaText ? `<span class="cf-sep">·</span>` : "";
    return `<div class="${cls}"><span class="cf-ico"></span>${titleHTML}${sep}${meta}</div>`;
  };

  // ---------- state ----------
  // Local state tracking to enrich summaries across multiple events.
  let pendingRunId = null;
  let opCounts = { add: { PLEX:0, SIMKL:0 }, remove: { PLEX:0, SIMKL:0 } };
  /**
   * Infer destination provider name from an event when not explicitly given.
   * @param {any} ev
   * @returns {"PLEX"|"SIMKL"}
   */
  const dstNameFrom = (ev) => ev.dst || (ev.event.includes(":A:") ? "PLEX" : "SIMKL");
  let squelchPlain = 0;

  // ---------- pretty JSON ----------
  /**
   * Convert a structured JSON event line into a styled HTML fragment.
   * Returns null for unknown/unhandled events so callers can fallback to plain rendering.
   * @param {string} line
   * @returns {string|null}
   */
  function formatFriendlyLog(line) {
    if (!line || line[0] !== "{") return null;
    let ev; try { ev = JSON.parse(line); } catch { return null; }
    if (!ev || !ev.event) return null;

    switch (ev.event) {
      case "run:start": {
        const dry = !!ev.dry_run;
        const conf = esc(ev.conflict || "source");
        let meta = `dry_run=${dry} · conflict=${conf}`;
        if (pendingRunId) { meta += ` · run_id=${pendingRunId}`; pendingRunId = null; }
        return htmlBlock("start", `${ICON.start} Sync started`, meta);
      }
      case "run:pair": {
        const i = ev.i|0, n = ev.n|0;
        const src = badge(ev.src), dst = badge(ev.dst), arr = arrowFor(ev.mode);
        const idx = (i && n) ? ` ${i}/${n}` : "";
        const meta = `feature=<b>${esc(ev.feature||"watchlist")}</b> · mode=${esc(ev.mode||"one-way")}` + (ev.dry_run ? " · dry_run=true" : "");
        return htmlBlock("pair", `${ICON.pair} Pair${idx}: ${src} <span class="cf-arrow">${arr}</span> ${dst}`, meta);
      }
      case "pair:start": return null;
      case "two:start":
        return htmlBlock("start", `⇄ Two-way sync`, `feature=${esc(ev.feature)} · removals=${!!ev.removals}`);
      case "snapshot:start": {
        const a = esc(ev.a || "");
        const b = esc(ev.b || "");
        const feat = esc(ev.feature || "");
        return htmlBlock("plan", `📸 Snapshot`, `${a} vs ${b} · ${feat}`);
      }
      case "debug": {
        const msg = esc(ev.msg || "debug");
        const meta = Object.entries(ev).filter(([k]) => k !== "event" && k !== "msg").map(([k,v]) => `${k}=${v}`).join(", ");
        return htmlBlock("plan", `🐞 ${msg}`, meta, "cf-muted");
      }
      case "two:plan": {
        const aA = ev.add_to_A|0, aB = ev.add_to_B|0, rA = ev.rem_from_A|0, rB = ev.rem_from_B|0;
        const has = aA + aB + rA + rB;
        return htmlBlock("plan", `${ICON.plan} Plan`,
          has ? `add A=${aA}, add B=${aB}, remove A=${rA}, remove B=${rB}` : `nothing to do`,
          has ? "" : "cf-muted");
      }
      case "two:apply:add:A:start":
      case "two:apply:add:B:start":
      case "two:apply:remove:A:start":
      case "two:apply:remove:B:start":
        return null;
      case "two:apply:add:A:done":
      case "two:apply:add:B:done":
      case "two:apply:remove:A:done":
      case "two:apply:remove:B:done": {
        const isAdd = ev.event.includes("add");
        const kind = isAdd ? "add" : "remove";
        const who  = dstNameFrom(ev);
        const cnt  = Number(ev.result?.count ?? ev.count ?? 0);
        opCounts[kind][who] = (opCounts[kind][who] || 0) + cnt;
        return null;
      }
      case "two:done": {
        const rP = Number(opCounts.remove.PLEX || 0);
        const rS = Number(opCounts.remove.SIMKL || 0);
        const aP = Number(opCounts.add.PLEX || 0);
        const aS = Number(opCounts.add.SIMKL || 0);
        const row = (kind, p, s) => {
          const ico  = kind === "add" ? ICON.add : ICON.remove;
          const type = kind === "add" ? "add" : "remove";
          const muted = (p + s) === 0 ? " cf-muted" : "";
          const meta = `PLEX·${p} / SIMKL·${s}`;
          return htmlBlock(type, `${ico} ${capitalize(kind)}`, meta, muted);
        };
        const out = [ row("remove", rP, rS), row("add", aP, aS) ].join("");
        opCounts = { add:{PLEX:0,SIMKL:0}, remove:{PLEX:0,SIMKL:0} };
        return out;
      }
      case "run:done": {
        const adds = ev.added|0, rems = ev.removed|0, pairs = ev.pairs|0;
        return htmlBlock("complete", `${ICON.complete} Sync complete`, `+${adds} / -${rems} · pairs=${pairs}`);
      }
      default: return null;
    }
  }

  // ---------- host/plain filtering ----------
  /**
   * Render plain text lines into friendlier output when possible (non-JSON path).
   * Applies small heuristics to detect orchestrator/scheduler milestones.
   * @param {string} line
   * @param {boolean} isDebug when true, bypasses filters (handled upstream)
   * @returns {string|null} HTML or plain text; null if the line should be dropped
   */
  function filterPlainLine(line, isDebug) {
    if (!line) return null;
    const t = String(line).trim();
    if (!t) return null;

    // pretty: the orchestrator start line -> "Start: orchestrator PAIR: <run_id>"
    const mOrch = t.match(/^>\s*SYNC start:\s*orchestrator\s+pairs\s+run_id=(\d+)/i);
    if (mOrch) {
      const id = mOrch[1];
      pendingRunId = id; // keep for JSON 'run:start' enrichment
      return htmlBlock("start", `${ICON.start} Start: orchestrator PAIR: ${id}`);
    }

    // capture other SYNC start lines (no render)
    const mRun = t.match(/^>\s*SYNC start:.*?\brun_id=(\d+)/i);
    if (mRun) { pendingRunId = mRun[1]; return null; }

    if (!isDebug) {
      if (/^sync start:\s*orchestrator/i.test(t)) return null;
      if (/^\[i]\s*triggered sync run/i.test(t)) return null;
      if (/^\[i]\s*orchestrator module:/i.test(t)) return null;
      if (/^\[i]\s*providers:/i.test(t)) return null;
      if (/^\[i]\s*features:/i.test(t)) return null;
      if (/^\[\d+\/\d+]\s+/i.test(t)) return null;
      if (/^•\s*feature=/i.test(t)) return null;
    }

    const mDone = t.match(/^\[i]\s*Done\.\s*Total added:\s*(\d+),\s*Total removed:\s*(\d+)/i);
    if (mDone) {
      const adds = Number(mDone[1]||0), rems = Number(mDone[2]||0);
      return htmlBlock("complete", `${ICON.complete} Sync complete`, `+${adds} / -${rems}`);
    }

    // scheduler pretty lines
    const mSched1 = t.match(/^\s*(?:\[?INFO]?)\s*\[?SCHED]?\s*scheduler\s+(started|stopped|refreshed)\s*\((enabled|disabled)\)/i);
    if (mSched1) {
      const state = mSched1[1].toLowerCase();
      const mode  = mSched1[2].toLowerCase();
      const cls   = mode === "enabled" ? "start" : "remove";
      return htmlBlock(cls, `⏱️ Scheduler`, `${state} · ${mode}`);
    }
    const mSched2 = t.match(/^\s*(?:\[?INFO]?)\s*scheduler:\s*started\s*(?:&|&amp;)\s*refreshed\s*$/i);
    if (mSched2) {
      return htmlBlock("start", `⏱️ Scheduler`, `started · refreshed`);
    }

    if (/^\[SYNC]\s*exit code/i.test(t)) {
      return `<div class="cf-line cf-fade-in">${esc(t)}</div>`;
    }

    return t;
  }

  // ---------- chunk split + JSON extract ----------
  /**
   * Split host buffer into separate lines while keeping known markers on their own lines.
   * @param {string} s
   * @returns {string[]} lines
   */
  function splitHost(s) {
    return String(s)
      .replace(/\r\n/g, "\n")
      .replace(/(?<!\n)(>\s*SYNC start:[^\n]*)/g, "\n$1")
      .replace(/(?<!\n)(\[\s*i\s*]\s*[^\n]*)/gi, "\n$1")
      .replace(/(?<!\n)(\[SYNC]\s*exit code:[^\n]*)/g, "\n$1")
      .replace(/(?<!\n)(▶\s*Sync started[^\n]*)/g, "\n$1")
      .replace(/(?<!\n)(🔗\s*Pair:[^\n]*)/g, "\n$1")
      .replace(/(?<!\n)(📝\s*Plan[^\n]*)/g, "\n$1")
      .replace(/(?<!\n)(✅\s*Pair finished[^\n]*)/g, "\n$1")
      .replace(/(?<!\n)(🏁\s*Sync complete[^\n]*)/g, "\n$1")
      .replace(/}\s*(?=\{")/g, "}\n")
      .split(/\n+/);
  }

  /**
   * Extract JSON tokens from a streaming chunk while preserving any trailing buffer.
   * The parser is tolerant to plain text around JSON objects.
   * @param {string} buf prior trailing buffer
   * @param {string} chunk new appended data
   * @returns {{tokens: string[], buf: string}}
   */
  function processChunk(buf, chunk) {
    let s = (buf || "") + String(chunk || "");
    const tokens = [];
    let i = 0;

    const emitPlain = (piece) => {
      if (!piece) return;
      for (const line of splitHost(piece)) if (line.trim()) tokens.push(line);
    };

    while (i < s.length) {
      if (s[i] !== "{") {
        const j = s.indexOf("{", i);
        if (j === -1) { emitPlain(s.slice(i)); i = s.length; break; }
        emitPlain(s.slice(i, j));
        i = j;
      }
      // JSON extractor
      let depth = 0, inStr = false, escp = false, k = i;
      for (; k < s.length; k++) {
        const ch = s[k];
        if (inStr) {
          if (escp) escp = false;
          else if (ch === "\\") escp = true;
          else if (ch === "\"") inStr = false;
        } else {
          if (ch === "\"") inStr = true;
          else if (ch === "{") depth++;
          else if (ch === "}") { depth--; if (depth === 0) { k++; break; } }
        }
      }
      if (depth === 0 && k <= s.length) { tokens.push(s.slice(i, k)); i = k; }
      else { break; }
    }

    return { tokens, buf: s.slice(i) };
  }

  // ---------- squelch helpers ----------
  /**
   * Heuristic: whether a line looks like a continuation (part of a block),
   * used to drop follow-up lines after a "providers:" or "features:" header.
   * @param {string} t
   */
  const isContinuationLine = t =>
    /^[\{\[]/.test(t) ||
    /^['"]?[A-Za-z0-9_]+['"]?\s*:/.test(t) ||
    /^\s{2,}\S/.test(t) ||
    /[}\]]$/.test(t);

  /**
   * Decide whether to drop a line and potentially squelch subsequent lines.
   * @param {string} t
   * @param {boolean} isDebug
   * @returns {boolean}
   */
  function shouldDropAndMaybeSquelch(t, isDebug) {
    if (isDebug) return false;
    if (/^\[i]\s*providers:/i.test(t)) { squelchPlain = 2; return true; }
    if (/^\[i]\s*features:/i.test(t))  { squelchPlain = 3; return true; }
    if (/^>\s*SYNC start:/i.test(t))   return true;
    if (/^\[i]\s*triggered sync run/i.test(t)) return true;
    if (/^\[i]\s*orchestrator module:/i.test(t)) return true;
    if (/^\[\d+\/\d+]\s+/i.test(t))    return true;
    if (/^•\s*feature=/i.test(t))      return true;
    if (/^\[SYNC]\s*exit code:/i.test(t)) return true;
    return false;
  }

  // ---------- render helper ----------
  /**
   * Render a line into a container element, using friendly formatting for
   * known JSON events and filtered plain text for others. In debug mode, emits
   * raw lines to aid troubleshooting.
   * @param {HTMLElement} el container element to insert into
   * @param {string} line a log line or HTML fragment
   * @param {boolean=} isDebug force debug rendering
   */
  function renderInto(el, line, isDebug) {
    if (!el || !line) return;

    isDebug = !!(isDebug ?? (typeof window !== "undefined" && window.appDebug));

    if (isDebug) {
      const raw = String(line);
      if (!raw) return;
      const div = document.createElement("div");
      div.className = "cf-line";
      div.textContent = raw;
      el.appendChild(div);
      return;
    }

    const html = formatFriendlyLog(line);
    if (html != null) { el.insertAdjacentHTML("beforeend", html); return; }

    if (String(line).trim().startsWith("{")) return;

    const t = String(line).trim();
    if (!t) return;

    if (squelchPlain > 0 && isContinuationLine(t)) { squelchPlain--; return; }
    if (squelchPlain > 0 && !isContinuationLine(t)) { squelchPlain = 0; }

    if (shouldDropAndMaybeSquelch(t, false)) return;

    const out = filterPlainLine(t, false);
    if (!out) return;

    if (/^<.+>/.test(out)) el.insertAdjacentHTML("beforeend", out);
    else {
      const div = document.createElement("div");
      div.className = "cf-line cf-fade-in";
      div.textContent = out;
      el.appendChild(div);
    }
  }

  // ---------- export ----------
  // Public API: used by the UI to format incoming log streams and render them.
  w.ClientFormatter = { formatFriendlyLog, filterPlainLine, splitHost, processChunk, renderInto };
})(window, document);
