/* assets/js/syncbar.js */
/* refactored */
/* SyncBar UI component for showing sync progress in the header. */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(() => {
  (document.getElementById("syncbar-css") || {}).remove?.();
  document.head.appendChild(Object.assign(document.createElement("style"), {
    id: "syncbar-css", textContent: `
#ux-progress{margin-top:12px;position:relative;z-index:1;padding:12px 14px 10px;border-radius:18px;background:radial-gradient(120% 150% at 10% 0%,rgba(76,61,168,.08) 0%,rgba(76,61,168,0) 34%),radial-gradient(90% 140% at 100% 100%,rgba(31,48,94,.07) 0%,rgba(31,48,94,0) 44%),linear-gradient(180deg,rgba(8,11,18,.985),rgba(4,6,10,.975));border:1px solid rgba(255,255,255,.07);box-shadow:inset 0 1px 0 rgba(255,255,255,.03),0 14px 30px rgba(0,0,0,.22);backdrop-filter:blur(14px) saturate(108%);-webkit-backdrop-filter:blur(14px) saturate(108%)}
.sb-head{display:flex;align-items:center;justify-content:space-between;gap:10px;margin-bottom:10px}
.sb-head-main{display:flex;align-items:center;gap:0;min-width:0}
.sb-label{font-size:12px;font-weight:700;letter-spacing:.02em;color:rgba(244,247,255,.94)}
.sb-phase{min-width:0}
.sb-phase-text{font-size:11px;color:rgba(190,199,215,.72);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.sb-pair{display:flex;align-items:center;gap:8px;min-width:0;margin-top:5px}
.sb-node{display:inline-flex;align-items:center;gap:7px;min-width:0;height:28px;padding:0 10px;border-radius:999px;border:1px solid rgba(255,255,255,.07);background:linear-gradient(180deg,rgba(255,255,255,.045),rgba(255,255,255,.02));box-shadow:inset 0 1px 0 rgba(255,255,255,.028);backdrop-filter:blur(10px) saturate(108%);-webkit-backdrop-filter:blur(10px) saturate(108%)}
.sb-node-logo{display:inline-flex;align-items:center;justify-content:center;width:18px;height:18px;flex:0 0 auto}
.sb-node-logo img{display:block;max-width:18px;max-height:14px;width:auto;height:auto;object-fit:contain;filter:brightness(1.02)}
.sb-node-logo-text{display:inline-flex;align-items:center;justify-content:center;min-width:18px;height:18px;padding:0 5px;border-radius:999px;background:rgba(255,255,255,.05);font-size:9px;font-weight:800;letter-spacing:.04em;color:rgba(242,247,255,.9)}
.sb-node-label{min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:11px;font-weight:700;color:rgba(237,242,250,.92)}
.sb-link{display:inline-flex;align-items:center;justify-content:center;width:28px;height:28px;flex:0 0 auto;color:rgba(170,191,235,.84)}
.sb-link .material-symbol{font-family:"Material Symbols Rounded";font-size:20px;line-height:1;-webkit-text-fill-color:currentColor;font-variation-settings:"FILL" 0,"wght" 500,"GRAD" 0,"opsz" 20}
.sb-feat{display:inline-flex;align-items:center;justify-content:center;height:22px;max-width:110px;padding:0 8px;border-radius:999px;border:1px solid rgba(255,255,255,.06);background:linear-gradient(180deg,rgba(255,255,255,.04),rgba(255,255,255,.018));font-size:10px;font-weight:800;letter-spacing:.04em;text-transform:uppercase;color:rgba(196,208,228,.82);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.sb-badge{display:inline-flex;align-items:center;justify-content:center;min-width:74px;height:26px;padding:0 10px;border-radius:999px;border:1px solid rgba(255,255,255,.07);background:linear-gradient(180deg,rgba(255,255,255,.045),rgba(255,255,255,.02));box-shadow:inset 0 1px 0 rgba(255,255,255,.02);font-size:11px;font-weight:800;letter-spacing:.04em;text-transform:uppercase;color:rgba(235,241,252,.88)}
.sb-badge.running{background:linear-gradient(180deg,rgba(88,104,170,.2),rgba(28,35,58,.24));border-color:rgba(132,149,214,.18);color:#edf4ff}
.sb-badge.done{background:linear-gradient(180deg,rgba(48,92,78,.22),rgba(17,45,38,.24));border-color:rgba(109,176,147,.16);color:#e9fff8}
.sb-badge.error{background:linear-gradient(180deg,rgba(106,48,56,.24),rgba(53,20,26,.26));border-color:rgba(214,128,141,.14);color:#fff0f0}
.sb-rail{position:relative;height:12px;border-radius:999px;overflow:visible;background:linear-gradient(180deg,rgba(7,10,16,.98),rgba(12,16,24,.96));border:1px solid rgba(255,255,255,.045);box-shadow:inset 0 1px 0 rgba(255,255,255,.018),inset 0 -8px 14px rgba(0,0,0,.34)}
.sb-rail::before{content:"";position:absolute;inset:1px;border-radius:inherit;pointer-events:none;background:linear-gradient(180deg,rgba(255,255,255,.025),rgba(255,255,255,0))}
.sb-rail.error{background:linear-gradient(180deg,rgba(44,14,17,.95),rgba(57,22,28,.92))}
.sb-fill{position:absolute;inset:1px auto 1px 1px;width:0%;height:calc(100% - 2px);border-radius:inherit;background:linear-gradient(90deg,#6d61ff 0%,#5f84ee 52%,#5a78cf 100%);box-shadow:inset 0 0 12px rgba(160,193,255,.12);transition:width .28s ease,filter .22s ease,opacity .22s ease;z-index:1;overflow:hidden}
@keyframes sbShift{0%{background-position:0% 50%}100%{background-position:100% 50%}}
@keyframes sbShimmer{to{transform:translateX(100%)}}
@keyframes sbPulse{from{opacity:.9}to{opacity:.75}}
.sb-rail.running .sb-fill{background-size:200% 100%}
.sb-rail.starting .sb-fill{animation:sbPulse .9s ease-in-out infinite alternate}
.sb-rail.finishing .sb-fill{filter:saturate(1.2) brightness(1.05)}
#ux-progress .sb-steps{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:8px;margin-top:10px}
#ux-progress .sb-steps span{display:flex;align-items:center;justify-content:center;height:28px;padding:0 8px;border-radius:999px;white-space:nowrap;font-size:10px;font-weight:700;letter-spacing:.03em;text-transform:uppercase;position:relative;isolation:isolate;color:rgba(176,188,206,.82);border:1px solid rgba(255,255,255,.055);background:linear-gradient(180deg,rgba(255,255,255,.04),rgba(255,255,255,.016));box-shadow:inset 0 1px 0 rgba(255,255,255,.016);opacity:1;transition:color .2s ease,border-color .2s ease,background .2s ease,box-shadow .2s ease,transform .2s ease,filter .2s ease}
#ux-progress .sb-steps span::before{content:"";position:absolute;inset:1px;border-radius:inherit;pointer-events:none;background:linear-gradient(180deg,rgba(255,255,255,.035),rgba(255,255,255,0));z-index:-1}
#ux-progress .sb-steps span.current{color:rgba(249,251,255,.98)!important;border-color:rgba(122,137,194,.2)!important;background:linear-gradient(180deg,rgba(78,88,134,.24),rgba(26,31,48,.34))!important;box-shadow:inset 0 1px 0 rgba(255,255,255,.045),0 0 0 1px rgba(91,103,154,.06)!important;transform:none;filter:none}
#ux-progress .sb-steps span.current::after{content:"";position:absolute;left:10px;top:50%;width:6px;height:6px;border-radius:999px;transform:translateY(-50%);background:rgba(171,185,233,.8);box-shadow:none}
#ux-progress .sb-steps span.done{color:rgba(234,241,255,.94)!important;border-color:rgba(255,255,255,.075)!important;background:linear-gradient(180deg,rgba(255,255,255,.06),rgba(255,255,255,.024))!important;box-shadow:inset 0 1px 0 rgba(255,255,255,.03)}
#ux-progress .sb-steps span.done:not(.current){color:rgba(221,231,245,.86)!important}
.sb-fly{position:absolute;top:-30px;left:0;transform:translateX(-50%);font-size:11px;line-height:1;padding:7px 10px;border-radius:999px;white-space:nowrap;color:rgba(242,246,255,.92);background:rgba(9,13,21,.72);backdrop-filter:blur(10px) saturate(120%);-webkit-backdrop-filter:blur(10px) saturate(120%);border:1px solid rgba(255,255,255,.08);box-shadow:0 10px 24px rgba(0,0,0,.24);opacity:.92;pointer-events:none;transition:left .25s ease,opacity .15s ease;z-index:2}
.sb-fly.hide{opacity:0}
.sb-rail.indet .sb-fill{background-size:200% 100%;animation:sbShift 1.2s ease-in-out infinite}
.sb-rail.indet .sb-fill::after{content:"";position:absolute;inset:0;background:linear-gradient(120deg,transparent 0%,rgba(255,255,255,.09) 20%,transparent 40%);transform:translateX(-100%);animation:sbShimmer 1.4s linear infinite;pointer-events:none}
.sb-rail.apply.indet .sb-fill::after{animation-duration:1s}
@media (prefers-reduced-motion:reduce){.sb-rail.running.indet::after,.sb-fill.indet,.sb-rail.starting .sb-fill{animation:none}}
@media (max-width:640px){#ux-progress{padding:11px 12px 10px}.sb-head{align-items:flex-start;flex-direction:column}.sb-badge{min-width:0}.sb-pair{gap:6px;flex-wrap:wrap}.sb-node{max-width:calc(50% - 18px);padding:0 8px}.sb-link{width:22px}.sb-link .material-symbol{font-size:18px}.sb-feat{max-width:100%}#ux-progress .sb-steps{gap:6px}#ux-progress .sb-steps span{height:26px;padding:0 6px;font-size:9px}#ux-progress .sb-steps span.current::after{left:8px}}
` }));

  const Anch = Object.freeze({ start0: 0, preStart: 35, preEnd: 57, postEnd: 67, done: 100 });
  const clamp = (n, lo = 0, hi = 100) => Math.max(lo, Math.min(hi, Math.round(n)));
  const POST_DONE_GRACE_MS = 20000;

  const PhaseAgg = {
    snap: { done: 0, total: 0, started: false, finished: false },
    apply: { done: 0, total: 0, started: false, finished: false }
  };

  const SnapAgg = {
    buckets: Object.create(null),
    reset() { this.buckets = Object.create(null); },
    update(d) {
      const k = `${(d.dst || "ALL").toUpperCase()}:${(d.feature || "all").toLowerCase()}`;
      this.buckets[k] = { done: +(d.done || 0), total: +(d.total || 0), final: !!d.final };
      let tot = 0;
      let don = 0;
      let allFinal = true;
      for (const v of Object.values(this.buckets)) {
        const dn = Math.min(+v.done || 0, +v.total || 0);
        don += dn;
        tot += (+v.total || 0);
        allFinal = allFinal && (!!v.final || dn >= (+v.total || 0));
      }
      PhaseAgg.snap.total = tot;
      PhaseAgg.snap.done = don;
      PhaseAgg.snap.started = tot > 0;
      PhaseAgg.snap.finished = allFinal && tot > 0 && don >= tot;
    }
  };

  const ApplyAgg = {
    buckets: Object.create(null),
    reset() {
      this.buckets = Object.create(null);
      PhaseAgg.apply = { done: 0, total: 0, started: false, finished: false };
    },
    _ensure(k) { return (this.buckets[k] ||= { done: 0, total: 0, final: false }); },
    start({ feature = "__global__", total = 0 }) {
      const b = this._ensure(feature);
      b.total = +total || 0;
      this._recalc();
    },
    prog({ feature = "__global__", done, total }) {
      const b = this._ensure(feature);
      if (typeof done === "number") b.done = +done || 0;
      if (typeof total === "number") b.total = +total || 0;
      this._recalc();
    },
    done({ feature = "__global__", count }) {
      const b = this._ensure(feature);
      const c = +(count || b.total || 0);
      b.done = c;
      b.total = Math.max(b.total, c);
      b.final = true;
      this._recalc();
    },
    _recalc() {
      let tot = 0;
      let don = 0;
      let allFinal = true;
      let any = false;
      for (const v of Object.values(this.buckets)) {
        any = true;
        tot += +v.total || 0;
        don += Math.min(+v.done || 0, +v.total || 0);
        allFinal = allFinal && (!!v.final || ((+v.total || 0) > 0 && (+v.done || 0) >= (+v.total || 0)));
      }
      PhaseAgg.apply.total = tot;
      PhaseAgg.apply.done = don;
      PhaseAgg.apply.started = any && tot > 0;
      PhaseAgg.apply.finished = any && allFinal && tot > 0 && don >= tot;
    }
  };

  const phaseIdx = (tl) => tl?.done ? 3 : tl?.post ? 2 : tl?.pre ? 1 : tl?.start ? 0 : -1;
  const asPctFromTimeline = (tl, allowDone = false) => ((allowDone && tl?.done) ? Anch.done : tl?.post ? Anch.postEnd : tl?.pre ? Anch.preStart : tl?.start ? Anch.start0 : 0);
  const pctFromPhase = () => {
    const sTot = PhaseAgg.snap.total | 0;
    const sDone = PhaseAgg.snap.done | 0;
    const aTot = PhaseAgg.apply.total | 0;
    const aDone = PhaseAgg.apply.done | 0;
    const snapPct = sTot > 0 ? (Anch.preStart + (Anch.preEnd - Anch.preStart) * Math.max(0, Math.min(1, sDone / sTot))) : null;
    const appPct = (PhaseAgg.snap.finished && aTot > 0)
      ? (Anch.preEnd + (Anch.postEnd - Anch.preEnd) * Math.max(0, Math.min(1, aDone / aTot)))
      : null;
    return appPct != null ? clamp(appPct) : snapPct != null ? clamp(snapPct) : null;
  };

  class SyncBar {
    constructor({ el, onStart, onStop } = {}) {
      this.el = el || document.getElementById("ux-progress");
      this._dom = null;
      this.timeline = { start: false, pre: false, post: false, done: false };
      this._pctMemo = 0;
      this._phaseMemo = -1;
      this._holdAtTen = false;
      this._optimistic = false;
      this._lastPhaseAt = Date.now();
      this._lastEventTs = Date.now();
      this._onStart = onStart;
      this._onStop = onStop;
      this._runKey = null;
      this._pairText = "";
      this._pairMeta = null;
      this._streamArmed = false;
      this._pendingDone = false;
      this._pendingDoneTimer = null;
      this._doneAt = 0;
      this._successExit0Seen = false;
      this._exitCode = null;
      this._hadError = false;
      this.render();
    }

    _runKeyOf(s) { return s?.run_id || s?.run_uuid || s?.raw_started_ts || (s?.started_at ? Date.parse(s.started_at) : null) || null; }
    lastEvent() { return this._lastEventTs; }
    isRunning() { return !!(this._running || (this.timeline.start && !this.timeline.done)); }
    state() { return { timeline: { ...this.timeline }, running: this.isRunning() }; }
    _stamp() { this._lastEventTs = Date.now(); }
    _afterUpdate(scheduleDone = false) {
      this._stamp();
      if (scheduleDone && this._pendingDone) this._scheduleDone();
      this.render();
    }
    _finishState({ done = true, error = false, exitCode = null } = {}) {
      clearTimeout(this._pendingDoneTimer);
      this._pendingDone = false;
      this._doneAt = 0;
      this.timeline = { start: true, pre: true, post: true, done };
      this._running = false;
      this._streamArmed = false;
      this._hadError = error;
      this._exitCode = exitCode;
      this._successExit0Seen = !error && exitCode === 0;
      try { this._onStop?.(); } catch {}
      this._afterUpdate();
    }
    _touchApply(d, fn) {
      if (!this._streamArmed && !this._pendingDone) return;
      this._reopenForLateWork();
      this._maybePair(d);
      fn(d || {});
      this.timeline.post = true;
      this._afterUpdate(true);
    }

    reset() {
      clearTimeout(this._pendingDoneTimer);
      Object.assign(this, {
        _pendingDone: false,
        _doneAt: 0,
        _pctMemo: 0,
        _phaseMemo: -1,
        _holdAtTen: false,
        _pairText: "",
        _pairMeta: null,
        _streamArmed: false,
        _successExit0Seen: false,
        _exitCode: null,
        _hadError: false
      });
      PhaseAgg.snap = { done: 0, total: 0, started: false, finished: false };
      PhaseAgg.apply = { done: 0, total: 0, started: false, finished: false };
      SnapAgg.reset();
      ApplyAgg.reset();
      this.timeline = { start: false, pre: false, post: false, done: false };
      this.render();
    }

    markInit() {
      if (this.timeline.start && !this.timeline.done) {
        this._running = true;
        this._streamArmed = true;
        this._lastEventTs = Date.now();
        return;
      }
      this.reset();
      this._optimistic = true;
      this._holdAtTen = true;
      this._streamArmed = true;
      this.timeline = { start: true, pre: false, post: false, done: false };
      this._afterUpdate();
      this._onStart?.();
    }

    setPair(d) {
      this._pairMeta = this._pairData((d && (d.pair || d)) || {});
      this._pairText = this._pairMeta.text;
      this.render();
    }

    _pairData(d) {
      if (!d) return { src: "", dst: "", feat: "", text: "" };
      const src = (d.src || d.provider_src || d.source || d.src_name || "").toString().trim().toUpperCase();
      const dst = (d.dst || d.provider_dst || d.target || d.dst_name || "").toString().trim().toUpperCase();
      const feat = (d.feature || d.lane || d.kind || "").toString().trim().toLowerCase();
      const parts = [];
      if (src || dst) parts.push([src, dst].filter(Boolean).join(" -> "));
      if (feat) parts.push(feat);
      return { src, dst, feat, text: parts.join(" - ") };
    }

    _pairString(d) {
      return this._pairData(d).text;
    }

    _maybePair(d) {
      if (!this._streamArmed) return;
      const pair = this._pairData(d);
      if (pair.text) {
        this._pairMeta = pair;
        this._pairText = pair.text;
      }
    }

    _providerLogo(name) {
      return window.CW?.ProviderMeta?.logoPath?.(name || "") || "";
    }

    _providerNode(name) {
      const node = document.createElement("span");
      node.className = "sb-node";
      const logoWrap = document.createElement("span");
      logoWrap.className = "sb-node-logo";
      const logo = this._providerLogo(name);
      if (logo) {
        const img = document.createElement("img");
        img.src = logo;
        img.alt = `${name || ""} logo`;
        logoWrap.appendChild(img);
      } else {
        const fallback = document.createElement("span");
        fallback.className = "sb-node-logo-text";
        fallback.textContent = String(name || "?").slice(0, 3);
        logoWrap.appendChild(fallback);
      }
      const label = document.createElement("span");
      label.className = "sb-node-label";
      label.textContent = name || "?";
      node.append(logoWrap, label);
      return node;
    }

    success() {
      this._finishState({ exitCode: 0 });
    }

    fail(code = 1) {
      this._finishState({ error: true, exitCode: code });
    }

    _reopenForLateWork() {
      if (this.timeline.done && !this._pendingDone && !this._successExit0Seen) {
        this.timeline.done = false;
        this._pendingDone = true;
        this._doneAt = this._doneAt || Date.now();
        this._streamArmed = true;
      }
    }

    _scheduleDone(delay = 900) {
      this._pendingDone = true;
      clearTimeout(this._pendingDoneTimer);
      this._pendingDoneTimer = setTimeout(() => {
        const now = Date.now();
        const elapsed = now - (this._doneAt || now);
        const quietFor = now - (this._lastEventTs || 0);
        if (elapsed < POST_DONE_GRACE_MS || quietFor < delay) return this._scheduleDone(delay);
        this._finishState();
      }, delay);
    }

    snap(d) {
      if (!this._streamArmed && !this._pendingDone) return;
      this._reopenForLateWork();
      this._holdAtTen = false;
      this._maybePair(d);
      SnapAgg.update(d || {});
      this.timeline.pre = true;
      this._afterUpdate(true);
    }

    applyStart(d) {
      this._touchApply(d, (x) => ApplyAgg.start(x));
    }

    applyProg(d) {
      this._touchApply(d, (x) => ApplyAgg.prog(x));
    }

    applyDone(d) {
      this._touchApply(d, (x) => ApplyAgg.done(x));
    }

    done() {
      this._running = false;
      this.timeline = { start: true, pre: true, post: true, done: false };
      this._doneAt = Date.now();
      this._stamp();
      this._scheduleDone();
      this.render();
    }

    error() {
      this._finishState({ error: true, exitCode: 1 });
    }

    fromSummary(sum) {
      const prevRunning = this.isRunning();
      const prevTL = { ...this.timeline };
      if (!sum) return { running: prevRunning, justStarted: false, justFinished: false };

      const key = this._runKeyOf(sum);
      const running = sum?.running === true || sum?.state === "running";
      if (!running && !this.timeline.start && !this.timeline.pre && !this.timeline.post && !this.timeline.done
        && (sum?.exit_code != null || sum?.finished || sum?.end || sum?.state === "idle")) {
        this.reset();
        return { running: false, justStarted: false, justFinished: false };
      }

      const mappedRaw = {
        start: !!(sum?.timeline?.start || sum?.timeline?.started || sum?.timeline?.[0] || sum?.started),
        pre: !!(sum?.timeline?.pre || sum?.timeline?.discovery || sum?.timeline?.discovering || sum?.timeline?.[1]),
        post: !!(sum?.timeline?.post || sum?.timeline?.syncing || sum?.timeline?.apply || sum?.timeline?.[2]),
        done: !!(sum?.timeline?.done || sum?.timeline?.finished || sum?.timeline?.complete || sum?.timeline?.[3])
      };
      let mapped = { ...mappedRaw };
      if (sum?.phase) {
        const p = String(sum.phase).toLowerCase();
        if (p === "snapshot") mapped.pre = true;
        if (p === "apply" || p === "sync" || p === "syncing") mapped.post = true;
      }
      const exitCode = sum?.exit_code != null ? Number(sum.exit_code) : null;

      if (exitCode != null) {
        if (exitCode === 0) this.success();
        else this.fail(exitCode);
        return { running: false, justStarted: false, justFinished: true };
      }

      if (key && key !== this._runKey) {
        this._runKey = key;
        this.markInit();
      }
      this._streamArmed = !!(running || (mapped.start && !mapped.done) || this._pendingDone);

      const ph = sum?._phase || {};
      if (ph.snapshot && PhaseAgg.snap.total === 0) {
        PhaseAgg.snap.total = +ph.snapshot.total || 0;
        PhaseAgg.snap.done = +ph.snapshot.done || 0;
        PhaseAgg.snap.started = PhaseAgg.snap.total > 0;
        PhaseAgg.snap.finished = !!ph.snapshot.final || (PhaseAgg.snap.total > 0 && PhaseAgg.snap.done >= PhaseAgg.snap.total);
      }
      if (ph.apply) {
        PhaseAgg.apply.total = +ph.apply.total || 0;
        PhaseAgg.apply.done = +ph.apply.done || 0;
        PhaseAgg.apply.started = PhaseAgg.apply.total > 0;
        PhaseAgg.apply.finished = !!ph.apply.final || (PhaseAgg.apply.total > 0 && PhaseAgg.apply.done >= PhaseAgg.apply.total);
      }

      this._running = running;
      const clampTL = (next) => (phaseIdx(next) < phaseIdx(this.timeline)) ? this.timeline : next;
      mapped = clampTL(mapped);
      if (mapped.start !== prevTL.start || mapped.pre !== prevTL.pre || mapped.post !== prevTL.post || mapped.done !== prevTL.done) {
        this._lastPhaseAt = Date.now();
      }
      this.timeline = mapped;

      const logicalDone = (PhaseAgg.snap.finished && (PhaseAgg.apply.finished || PhaseAgg.apply.total === 0));
      const nowInProgress = running || (this.timeline.start && !this.timeline.done);
      const wasInProgress = prevRunning || (prevTL.start && !prevTL.done) || this._optimistic;
      const justFinished = wasInProgress && !nowInProgress && (this.timeline.done || logicalDone);

      if (!nowInProgress && !this._pendingDone && (sum.exit_code != null || this.timeline.done)) this._streamArmed = false;

      this._lastEventTs = Date.now();
      this.render();
      return { running: nowInProgress, justStarted: (!prevRunning && nowInProgress), justFinished };
    }

    updateTimeline(tl) {
      const clampTL = (next) => (phaseIdx(next) < phaseIdx(this.timeline)) ? this.timeline : next;
      this.timeline = clampTL({ start: !!tl.start, pre: !!tl.pre, post: !!tl.post, done: !!tl.done });
      this.render();
    }

    updatePct(p) {
      if (typeof p === "number") {
        this._pctMemo = Math.max(this._pctMemo, clamp(p));
        this.render();
      }
    }

    _ensureDom() {
      const host = this.el;
      if (!host) return null;
      let dom = this._dom;
      if (dom && host.contains(dom.rail) && host.contains(dom.steps)) return dom;

      host.innerHTML = "";
      const head = document.createElement("div");
      head.className = "sb-head";
      const headMain = document.createElement("div");
      headMain.className = "sb-head-main";
      const meta = document.createElement("div");
      const label = document.createElement("div");
      label.className = "sb-label";
      label.textContent = "Sync status";
      const phase = document.createElement("div");
      phase.className = "sb-phase";
      const phaseText = document.createElement("div");
      phaseText.className = "sb-phase-text";
      const pair = document.createElement("div");
      pair.className = "sb-pair";
      pair.hidden = true;
      const srcNode = document.createElement("span");
      srcNode.className = "sb-pair-src";
      const link = document.createElement("span");
      link.className = "sb-link";
      link.innerHTML = `<span class="material-symbol" aria-hidden="true">trending_flat</span>`;
      const dstNode = document.createElement("span");
      dstNode.className = "sb-pair-dst";
      const feat = document.createElement("span");
      feat.className = "sb-feat";
      pair.append(srcNode, link, dstNode, feat);
      phase.append(phaseText, pair);
      meta.append(label, phase);
      headMain.append(meta);
      const badge = document.createElement("div");
      badge.className = "sb-badge";
      badge.textContent = "Idle";
      head.append(headMain, badge);

      const rail = document.createElement("div");
      rail.className = "sb-rail";
      const fill = document.createElement("div");
      fill.className = "sb-fill";
      const fly = document.createElement("div");
      fly.className = "sb-fly hide";
      const steps = document.createElement("div");
      steps.className = "sb-steps";
      [["Start", "start"], ["Discovering", "discovering"], ["Syncing", "syncing"], ["Done", "done"]].forEach(([txt, key]) => {
        const s = document.createElement("span");
        s.textContent = txt;
        s.dataset.step = key;
        steps.appendChild(s);
      });
      rail.append(fill, fly);
      host.append(head, rail, steps);
      dom = { head, badge, phaseText, pair, srcNode, dstNode, feat, rail, fill, fly, steps };
      this._dom = dom;
      return dom;
    }

    render() {
      const host = this.el;
      if (!host) return;
      const dom = this._ensureDom();
      if (!dom) return;
      const { badge, phaseText: phaseTextEl, pair, srcNode, dstNode, feat, rail, fill, fly, steps } = dom;
      fly.textContent = this._pairText || "";

      const allowDone = !!this._successExit0Seen;
      const logicalDone = (PhaseAgg.snap.finished && (PhaseAgg.apply.finished || PhaseAgg.apply.total === 0));
      const hardDone = allowDone && (!this._pendingDone) && (this.timeline.done || logicalDone);

      const byPhases = pctFromPhase();
      let base = byPhases;

      if (hardDone) {
        base = Anch.done;
      } else {
        if (base == null || (this.timeline.post && !PhaseAgg.apply.started)) base = asPctFromTimeline(this.timeline, allowDone);
        if (this._holdAtTen && !PhaseAgg.snap.started) base = Math.max(base, 10);
        base = Math.min(base, Anch.postEnd);
      }

      const idx = phaseIdx(this.timeline);
      if (idx < this._phaseMemo) base = this._pctMemo;
      if (idx > this._phaseMemo) this._phaseMemo = idx;

      this._pctMemo = Math.max(this._pctMemo, clamp(base));
      fill.style.width = this._pctMemo + "%";

      const isRunning = this.isRunning();
      const shouldFlow = isRunning && !hardDone;
      const currentStep = hardDone ? "done" : this.timeline.post ? "syncing" : this.timeline.pre ? "discovering" : this.timeline.start ? "start" : "";
      const statusText = this._hadError ? "Error" : hardDone ? "Synced" : this._pendingDone ? "Finalizing" : this.timeline.post ? "Syncing" : this.timeline.pre ? "Discovering" : isRunning ? "Starting" : "Idle";
      const phaseLabel = (this._hadError
        ? `Sync failed${this._exitCode != null ? ` (code ${this._exitCode})` : ""}`
        : hardDone ? "Completed successfully"
        : this._pendingDone ? "Wrapping up final tasks"
        : this.timeline.post ? "Applying changes across enabled features"
        : this.timeline.pre ? "Scanning current state before applying changes"
        : "Waiting for the next sync run");

      badge.textContent = statusText;
      badge.classList.toggle("running", isRunning && !hardDone && !this._hadError);
      badge.classList.toggle("done", hardDone && !this._hadError);
      badge.classList.toggle("error", this._hadError);
      phaseTextEl.textContent = phaseLabel;

      const pairMeta = this._pairMeta;
      const showPair = isRunning && !!(pairMeta?.src || pairMeta?.dst);
      pair.hidden = !showPair;
      pair.style.display = showPair ? "flex" : "none";
      if (showPair) {
        srcNode.replaceChildren(this._providerNode(pairMeta.src || "?"));
        dstNode.replaceChildren(this._providerNode(pairMeta.dst || "?"));
        feat.textContent = pairMeta.feat || "";
        feat.hidden = !pairMeta.feat;
      }

      rail.classList.toggle("running", isRunning && !this.timeline.done);
      rail.classList.toggle("indet", shouldFlow);
      rail.classList.toggle("apply", PhaseAgg.apply.started && !PhaseAgg.apply.finished);
      rail.classList.toggle("starting", isRunning && !(this.timeline.pre || this.timeline.post));
      rail.classList.toggle("finishing", !isRunning && !this.timeline.done && (logicalDone || this._pendingDone));
      rail.classList.toggle("error", this._hadError);

      for (const step of steps.children) {
        const key = step.dataset.step || "";
        const doneState = key === "start" ? !!this.timeline.start
          : key === "discovering" ? !!this.timeline.pre
          : key === "syncing" ? !!this.timeline.post
          : key === "done" ? hardDone
          : false;
        step.classList.toggle("done", doneState);
        step.classList.toggle("current", key === currentStep);
      }

      const pct = this._pctMemo / 100;
      const railW = host.clientWidth || 1;
      const left = Math.max(8, Math.min(railW - 8, railW * pct));
      fly.style.left = left + "px";
      fly.classList.toggle("hide", !(isRunning && this._pairText));
    }
  }

  window.SyncBar = SyncBar;
})();
