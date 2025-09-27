// Client-side log formatter for CrossWatch UI.
//* Refactoring project: main.js (v0.1) */
//*-------------------------------------*/

(function (w, d) {
  "use strict";

  // -- styles (once) --
  if (!d.getElementById("cf-styles")) {
    d.head.insertAdjacentHTML("beforeend", `<style id="cf-styles">
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
.cf-jellyfin{background:#15102b;color:#9aa5ff;border-color:rgba(154,165,255,.28)}
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
@media (prefers-reduced-motion:reduce){.cf-fade-in,.cf-pop,.cf-slide-in,.cf-pulse{animation:none}.cf-complete-shimmer:after{display:none}}
</style>`);
  }

  // -- utils --
  const esc = s => String(s ?? "").replace(/[&<>]/g, m => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" }[m]));
  const ICON = { start: "▶", pair: "🔗", plan: "📝", add: "➕", remove: "➖", done: "✅", complete: "🏁" };
  const PROV = {
    PLEX: { cls: "cf-plex", logo: "/assets/img/PLEX-log.svg" },
    SIMKL: { cls: "cf-simkl", logo: "/assets/img/SIMKL-log.svg" },
    TRAKT: { cls: "cf-trakt", logo: "/assets/img/TRAKT-log.svg" },
    JELLYFIN: { cls: "cf-jellyfin", logo: "/assets/img/JELLYFIN-log.svg" }
  };
  const arrowFor = m => String(m || "").toLowerCase().startsWith("two") ? "⇄" : "→";
  const cap = s => String(s || "").replace(/^./, c => c.toUpperCase());
  const badge = name => {
    const key = String(name || "").toUpperCase(), p = PROV[key] || { cls: "cf-generic" };
    const img = p.logo ? `<img src="${p.logo}" alt="" aria-hidden="true">` : "";
    return `<span class="cf-badge ${p.cls}">${img}${esc(key)}</span>`;
  };
  const block = (type, titleHTML, metaText, extra) => {
    const base = type === "start" ? "cf-slide-in cf-pulse" : type === "complete" ? "cf-fade-in cf-complete-shimmer" : "cf-fade-in";
    const cls = `cf-event ${type} ${base}${extra ? ` ${extra}` : ""}`;
    const meta = metaText ? `<span class="cf-sep">·</span><span class="cf-meta">${metaText}</span>` : "";
    return `<div class="${cls}"><span class="cf-ico"></span>${titleHTML}${meta}</div>`;
  };

  // -- state --
  let pendingRunId = null;
  let pair = { A: "A", B: "B" };
  let counts = { add: {}, remove: {} };
  const resetCounts = (a, b) => (counts = { add: { [a]: 0, [b]: 0 }, remove: { [a]: 0, [b]: 0 } });
  const dstNameFrom = ev => ev?.dst ? String(ev.dst).toUpperCase() : (String(ev?.event || "").includes(":A:") ? pair.A : pair.B);
  let squelchPlain = 0;

  // -- JSON → pretty --
  function formatFriendlyLog(line) {
    if (!line || line[0] !== "{") return null;
    let ev; try { ev = JSON.parse(line); } catch { return null; }
    if (!ev?.event) return null;

    switch (ev.event) {
      case "run:start": {
        const meta = [`dry_run=${!!ev.dry_run}`, `conflict=${esc(ev.conflict || "source")}`];
        if (pendingRunId) { meta.push(`run_id=${pendingRunId}`); pendingRunId = null; }
        return block("start", `${ICON.start} Sync started`, meta.join(" · "));
      }
      case "run:pair": {
        const i = ev.i | 0, n = ev.n | 0, A = String(ev.src || "").toUpperCase(), B = String(ev.dst || "").toUpperCase();
        pair = { A, B }; resetCounts(A, B);
        const idx = (i && n) ? ` ${i}/${n}` : "";
        const meta = `feature=<b>${esc(ev.feature || "watchlist")}</b> · mode=${esc(ev.mode || "one-way")}${ev.dry_run ? " · dry_run=true" : ""}`;
        return block("pair", `${ICON.pair} Pair${idx}: ${badge(A)} <span class="cf-arrow">${arrowFor(ev.mode)}</span> ${badge(B)}`, meta);
      }
      case "pair:start": return null;
      case "two:start":  return block("start", `⇄ Two-way sync`, `feature=${esc(ev.feature)} · removals=${!!ev.removals}`);
      case "snapshot:start": return block("plan", `📸 Snapshot`, `${esc(ev.a || "")} vs ${esc(ev.b || "")} · ${esc(ev.feature || "")}`);
      case "debug": {
        const msg = esc(ev.msg || "debug");
        const meta = Object.entries(ev).filter(([k]) => !["event", "msg"].includes(k)).map(([k, v]) => `${k}=${v}`).join(", ");
        return block("plan", `🐞 ${msg}`, meta, "cf-muted");
      }
      case "two:plan": {
        const aA = ev.add_to_A | 0, aB = ev.add_to_B | 0, rA = ev.rem_from_A | 0, rB = ev.rem_from_B | 0, has = aA + aB + rA + rB;
        return block("plan", `${ICON.plan} Plan`, has ? `add A=${aA}, add B=${aB}, remove A=${rA}, remove B=${rB}` : `nothing to do`, has ? "" : "cf-muted");
      }
      case "two:apply:add:A:done":
      case "two:apply:add:B:done":
      case "two:apply:remove:A:done":
      case "two:apply:remove:B:done": {
        const kind = ev.event.includes("add") ? "add" : "remove";
        const who = dstNameFrom(ev);
        const cnt = Number(ev.result?.count ?? ev.count ?? 0);
        counts[kind][who] = (counts[kind][who] || 0) + cnt;
        return null;
      }
      case "two:done": {
        const { A, B } = pair;
        const row = (kind, aCnt, bCnt) => block(kind, `${ICON[kind]} ${cap(kind)}`, `${A}·${aCnt} / ${B}·${bCnt}`, (aCnt + bCnt) ? "" : "cf-muted");
        return [row("remove", counts.remove[A] | 0, counts.remove[B] | 0), row("add", counts.add[A] | 0, counts.add[B] | 0)].join("");
      }
      case "run:done": return block("complete", `${ICON.complete} Sync complete`, `+${(ev.added | 0)} / -${(ev.removed | 0)} · pairs=${(ev.pairs | 0)}`);
      default: return null;
    }
  }

  // -- host/plain filtering --
  function filterPlainLine(line, isDebug) {
    const t = String(line || "").trim(); if (!t) return null;

    const mOrch = t.match(/^>\s*SYNC start:\s*orchestrator\s+pairs\s+run_id=(\d+)/i);
    if (mOrch) { pendingRunId = mOrch[1]; return block("start", `${ICON.start} Start: orchestrator PAIR: ${pendingRunId}`); }

    const mRun = t.match(/^>\s*SYNC start:.*?\brun_id=(\d+)/i);
    if (mRun) { pendingRunId = mRun[1]; return null; }

    if (!isDebug) {
      if (/^sync start:\s*orchestrator/i.test(t)) return null;
      if (/^\[i]\s*triggered sync run/i.test(t)) return null;
      if (/^\[i]\s*orchestrator module:/i.test(t)) return null;
      if (/^\[i]\s*providers:/i.test(t)) { squelchPlain = 2; return null; }
      if (/^\[i]\s*features:/i.test(t))  { squelchPlain = 3; return null; }
      if (/^\[\d+\/\d+]\s+/i.test(t)) return null;
      if (/^•\s*feature=/i.test(t)) return null;
      if (/^\[SYNC]\s*exit code:/i.test(t)) return null;
    }

    const mDone = t.match(/^\[i]\s*Done\.\s*Total added:\s*(\d+),\s*Total removed:\s*(\d+)/i);
    if (mDone) return block("complete", `${ICON.complete} Sync complete`, `+${Number(mDone[1] || 0)} / -${Number(mDone[2] || 0)}`);

    const mSched1 = t.match(/^\s*(?:\[?INFO]?)\s*\[?SCHED]?\s*scheduler\s+(started|stopped|refreshed)\s*\((enabled|disabled)\)/i);
    if (mSched1) return block(mSched1[2].toLowerCase() === "enabled" ? "start" : "remove", `⏱️ Scheduler`, `${mSched1[1].toLowerCase()} · ${mSched1[2].toLowerCase()}`);

    const mSched2 = t.match(/^\s*(?:\[?INFO]?)\s*scheduler:\s*started\s*(?:&|&amp;)\s*refreshed\s*$/i);
    if (mSched2) return block("start", `⏱️ Scheduler`, `started · refreshed`);

    return t; // pass-through (will be wrapped downstream)
  }

  // -- chunk split + JSON extract --
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

  function processChunk(buf, chunk) {
    let s = (buf || "") + String(chunk || ""), tokens = [], i = 0;
    const emitPlain = piece => { if (!piece) return; for (const ln of splitHost(piece)) if (ln.trim()) tokens.push(ln); };

    while (i < s.length) {
      if (s[i] !== "{") { const j = s.indexOf("{", i); if (j === -1) { emitPlain(s.slice(i)); i = s.length; break; } emitPlain(s.slice(i, j)); i = j; }
      let depth = 0, inStr = false, escp = false, k = i;
      for (; k < s.length; k++) {
        const ch = s[k];
        if (inStr) { escp ? escp = false : ch === "\\" ? escp = true : ch === `"` && (inStr = false); }
        else { ch === `"` ? inStr = true : ch === "{" ? depth++ : ch === "}" && (--depth === 0 && (k++, 1)); }
        if (depth === 0 && !inStr && k > i && s[k - 1] === "}") break;
      }
      if (depth === 0 && k <= s.length) { tokens.push(s.slice(i, k)); i = k; } else break;
    }
    return { tokens, buf: s.slice(i) };
  }

  // -- squelch helpers --
  const isContinuationLine = t => /^[\{\[]/.test(t) || /^['"]?[A-Za-z0-9_]+['"]?\s*:/.test(t) || /^\s{2,}\S/.test(t) || /[}\]]$/.test(t);
  const shouldDropAndMaybeSquelch = (t, isDebug) => {
    if (isDebug) return false;
    if (/^\[i]\s*providers:/i.test(t)) { squelchPlain = 2; return true; }
    if (/^\[i]\s*features:/i.test(t))  { squelchPlain = 3; return true; }
    if (/^>\s*SYNC start:/i.test(t)) return true;
    if (/^\[i]\s*triggered sync run/i.test(t)) return true;
    if (/^\[i]\s*orchestrator module:/i.test(t)) return true;
    if (/^\[\d+\/\d+]\s+/i.test(t)) return true;
    if (/^•\s*feature=/i.test(t)) return true;
    if (/^\[SYNC]\s*exit code:/i.test(t)) return true;
    return false;
  };

  // -- render --
  function renderInto(el, line, isDebug) {
    if (!el || !line) return;
    isDebug = !!(isDebug ?? (typeof window !== "undefined" && window.appDebug));

    if (isDebug) {
      const div = d.createElement("div"); div.className = "cf-line"; div.textContent = String(line); el.appendChild(div); return;
    }

    const fancy = formatFriendlyLog(line);
    if (fancy != null) { el.insertAdjacentHTML("beforeend", fancy); return; }

    if (String(line).trim().startsWith("{")) return; // broken JSON → skip

    const t = String(line).trim(); if (!t) return;
    if (squelchPlain > 0 && isContinuationLine(t)) { squelchPlain--; return; }
    if (squelchPlain > 0 && !isContinuationLine(t)) squelchPlain = 0;
    if (shouldDropAndMaybeSquelch(t, false)) return;

    const out = filterPlainLine(t, false); if (!out) return;
    if (/^<.+>/.test(out)) el.insertAdjacentHTML("beforeend", out);
    else { const div = d.createElement("div"); div.className = "cf-line cf-fade-in"; div.textContent = out; el.appendChild(div); }
  }

  // -- export --
  w.ClientFormatter = { formatFriendlyLog, filterPlainLine, splitHost, processChunk, renderInto };
})(window, document);
