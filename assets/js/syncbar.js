/* assets/js/syncbar.js */
/* refactored */
/* SyncBar UI component for showing sync progress in the header. */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(() => {
  const Anch = Object.freeze({ start0: 0, preStart: 35, preEnd: 57, postEnd: 67, done: 100 });
  const clamp = (n, lo = 0, hi = 100) => Math.max(lo, Math.min(hi, Math.round(n)));
  const POST_DONE_GRACE_MS = 20000;
  const AGE_REFRESH_MS = 30000;
  const asEpochSec = (v) => {
    if (v == null || v === "") return 0;
    const n = typeof v === "number" ? v : Date.parse(v);
    if (!Number.isFinite(n) || n <= 0) return 0;
    return Math.floor(n > 1e12 ? n / 1000 : n);
  };

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
      this._unresolved = 0;
      this._finishedAt = 0;
      this._ageTimer = null;
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
        _hadError: false,
        _unresolved: 0,
        _finishedAt: 0
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
      const unresolved = Number(sum?.unresolved || 0);
      if (Number.isFinite(unresolved) && unresolved >= 0) this._unresolved = unresolved;

      const finishedAt = asEpochSec(sum?.finished_at);
      if (finishedAt) this._finishedAt = finishedAt;

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

    _trackAge(active) {
      if (active && !this._ageTimer) this._ageTimer = setInterval(() => this.render(), AGE_REFRESH_MS);
      else if (!active && this._ageTimer) {
        clearInterval(this._ageTimer);
        this._ageTimer = null;
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
      phase.append(phaseText);
      meta.append(label, phase);
      headMain.append(meta);
      const headSide = document.createElement("div");
      headSide.className = "sb-head-side";
      const note = document.createElement("div");
      note.className = "sb-note hide";
      const badge = document.createElement("div");
      badge.className = "sb-badge";
      badge.textContent = "Idle";
      headSide.append(note, badge);
      head.append(headMain, headSide);

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
      dom = { head, badge, phaseText, note, rail, fill, fly, steps };
      this._dom = dom;
      return dom;
    }

    render() {
      const host = this.el;
      if (!host) return;
      const dom = this._ensureDom();
      if (!dom) return;
      const { badge, phaseText: phaseTextEl, note: noteEl, rail, fill, fly, steps } = dom;
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
      const settled = hardDone || this._hadError;
      const doneAge = settled && this._finishedAt ? (window.relTimeFromEpoch?.(this._finishedAt) || "") : "";
      const phaseLabel = (this._hadError
        ? `Sync failed${this._exitCode != null ? ` (code ${this._exitCode})` : ""}`
        : hardDone ? "Completed successfully"
        : this._pendingDone ? "Wrapping up final tasks"
        : this.timeline.post ? "Applying changes across enabled features"
        : this.timeline.pre ? "Scanning current state before applying changes"
        : "Waiting for the next sync run") + (doneAge ? ` · ${doneAge}` : "");
      this._trackAge(!!doneAge);

      badge.textContent = statusText;
      badge.classList.toggle("running", isRunning && !hardDone && !this._hadError);
      badge.classList.toggle("done", hardDone && !this._hadError);
      badge.classList.toggle("error", this._hadError);
      phaseTextEl.textContent = phaseLabel;

      const unresolved = Number(this._unresolved || 0);
      const showNote = unresolved > 0 && (hardDone || this._hadError);
      if (noteEl) {
        noteEl.classList.toggle("hide", !showNote);
        if (showNote) {
          noteEl.textContent = `${unresolved} unresolved`;
          noteEl.title = `${unresolved} item${unresolved === 1 ? "" : "s"} could not be written to the destination. See the sync log for details.`;
        }
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
