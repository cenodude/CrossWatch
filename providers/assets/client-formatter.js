(function (w, d) {
  // ---------- styles (once) ----------
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
  const esc = s => String(s ?? "").replace(/[&<>]/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;'}[m]));
  const BADGE_CLASS = { PLEX: "cf-plex", SIMKL: "cf-simkl", TRAKT: "cf-trakt" };
  const LOGO_SRC    = { PLEX: "/assets/PLEX-log.svg", SIMKL: "/assets/SIMKL-log.svg", TRAKT: "/assets/TRAKT-log.svg" };
  const ICON        = { start:"▶", pair:"🔗", plan:"📝", add:"➕", remove:"➖", done:"✅", complete:"🏁" };
  const arrowFor = m => (String(m||"").toLowerCase().startsWith("two") ? "⇄" : "→");
  const capitalize = s => String(s||"").replace(/^./, c => c.toUpperCase());
  const badge = name => {
    const key = String(name||"").toUpperCase();
    const cls = BADGE_CLASS[key] || "cf-generic";
    const src = LOGO_SRC[key] || "";
    const icon = src ? `<img src="${src}" alt="" aria-hidden="true">` : "";
    return `<span class="cf-badge ${cls}">${icon}${esc(key)}</span>`;
  };
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
  // run_id uit eerste plain regel; toevoegen aan run:start
  let pendingRunId = null;

  // aggregatie voor apply-steps
  let opCounts = { add: { PLEX:0, SIMKL:0 }, remove: { PLEX:0, SIMKL:0 } };
  const dstNameFrom = (ev) => ev.dst || (ev.event.includes(":A:") ? "PLEX" : "SIMKL");

  // squelch voor vervolgregels van gedropte meta
  let squelchPlain = 0;

  // ---------- pretty JSON ----------
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
      // Toon juist run:pair (met index); onderdruk pair:start om dubbele tegels te voorkomen
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

      // Snapshot zonder provider-badges (dus geen logo’s)
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

      // verberg starts
      case "two:apply:add:A:start":
      case "two:apply:add:B:start":
      case "two:apply:remove:A:start":
      case "two:apply:remove:B:start":
        return null;

      // done → tel op; render pas bij two:done
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

        // reset voor volgende pair
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
  function filterPlainLine(line, isDebug) {
    if (!line) return null;
    const t = String(line).trim();
    if (!t) return null;

    // vang SYNC start; pak run_id en render niet
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

    // pretty scheduler line
    if (/^\s*(?:\[?INFO]?)\s*scheduler:\s*started\s*(?:&|&amp;)\s*refreshed\s*$/i.test(t)) {
      return htmlBlock("start", `⏱️ Scheduler`, `started & refreshed`);
    }
    
    if (/^\[SYNC]\s*exit code/i.test(t)) {
      return `<div class="cf-line cf-fade-in">${esc(t)}</div>`;
    }

    return t;
  }

  // ---------- chunk split + JSON extract ----------
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
  const isContinuationLine = t =>
    /^[\{\[]/.test(t) ||
    /^['"]?[A-Za-z0-9_]+['"]?\s*:/.test(t) ||
    /^\s{2,}\S/.test(t) ||
    /[}\]]$/.test(t);

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
  function renderInto(el, line, isDebug) {
    if (!el || !line) return;

    // Neem globale vlag als fallback als 3e arg niet is meegegeven
    isDebug = !!(isDebug ?? (typeof window !== "undefined" && window.appDebug));

    if (isDebug) {
      // RAW: niets formatteren / filteren
      const raw = String(line);
      if (!raw) return;
      const div = document.createElement("div");
      div.className = "cf-line";
      div.textContent = raw; // exact zoals binnenkomt
      el.appendChild(div);
      return;
    }

    // Normal mode: pretty blocks + filters
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
  w.ClientFormatter = { formatFriendlyLog, filterPlainLine, splitHost, processChunk, renderInto };
})(window, document);
