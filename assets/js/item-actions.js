/* assets/js/item-actions.js */
/* CrossWatch - add and remove one title on providers from its provider icons */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const esc = (value) => String(value ?? "").replace(/[&<>"']/g, (ch) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[ch]));
  const icon = (name) => `<span class="material-symbols-rounded" aria-hidden="true">${esc(name)}</span>`;
  const numberFmt = new Intl.NumberFormat(window.__CW_LOCALE || navigator.language || undefined);
  const FEATURES = {
    watchlist: { noun: "watchlist", present: "On your watchlist", missing: "Not on this watchlist", undo: true },
    history: { noun: "history", present: "Watched here", missing: "Not watched here", undo: false },
    ratings: { noun: "ratings", present: "Rated here", missing: "Not rated here", undo: true },
    collection: { noun: "collection", present: "In your collection", missing: "Not in this collection", undo: true },
    progress: { noun: "resume point", present: "Has a resume point", missing: "No resume point", undo: false },
  };
  const FEATURE_ICONS = { watchlist: "bookmark", history: "history", ratings: "star", collection: "video_library", progress: "resume" };
  const FEATURE_LABELS = { watchlist: "Watchlist", history: "History", ratings: "Ratings", collection: "Collection" };
  const ARM_MS = 4200;
  const OK_MS = 900;
  const ID_KEYS = ["tmdb", "imdb", "tvdb", "trakt", "simkl", "anilist", "mal"];
  const targetCache = new Map();
  let seq = 0;
  let removeChain = Promise.resolve();

  const refKey = (ref) => `${String(ref?.provider || "").toUpperCase()}:${String(ref?.instance || "default") || "default"}`;
  const cleanRef = (ref) => ({ provider: String(ref?.provider || "").toUpperCase(), instance: String(ref?.instance || "default") || "default" });
  const providerName = (provider) => window.CW?.ProviderMeta?.label?.(provider) || String(provider || "");
  const providerLogo = (provider) => window.CW?.ProviderMeta?.logoPath?.(provider) || "";
  const plural = (count, word) => `${numberFmt.format(count)} ${word}${count === 1 ? "" : "s"}`;
  const listNames = (names) => (names.length <= 1 ? names.join("") : `${names.slice(0, -1).join(", ")} and ${names[names.length - 1]}`);

  function canWrite() {
    if (window.CW?.ProfileViewAs?.active) return false;
    const data = document.documentElement?.dataset || {};
    return data.cwRole !== "user" || data.cwPermWrite === "on";
  }

  async function request(url, body, ms = 180000) {
    const controller = typeof AbortController === "function" ? new AbortController() : null;
    const timer = controller ? window.setTimeout(() => controller.abort("timeout"), ms) : 0;
    const init = body === undefined
      ? { cache: "no-store", credentials: "same-origin", signal: controller?.signal }
      : { method: "POST", cache: "no-store", credentials: "same-origin", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body), signal: controller?.signal };
    try {
      const res = await fetch(url, init);
      const data = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error((typeof data?.detail === "string" && data.detail) || data?.message || data?.error || `Request failed (${res.status})`);
      return data;
    } catch (e) {
      if (e?.name === "AbortError") throw new Error("The provider took too long to answer");
      throw e;
    } finally {
      if (timer) window.clearTimeout(timer);
    }
  }

  function targets(feature) {
    if (!targetCache.has(feature)) {
      const pending = request(`/api/editor/send/providers?kind=${encodeURIComponent(feature)}`)
        .then((data) => (Array.isArray(data?.providers) ? data.providers : []).map((row) => ({ ...row, ...cleanRef(row) })))
        .catch(() => {
          targetCache.delete(feature);
          return [];
        });
      targetCache.set(feature, pending);
    }
    return targetCache.get(feature);
  }

  function refLabel(ref, list = []) {
    const row = list.find((entry) => refKey(entry) === refKey(ref));
    if (row?.display) return row.display;
    return ref.instance && ref.instance !== "default" ? `${providerName(ref.provider)} (${ref.instance})` : providerName(ref.provider);
  }

  function scalarIds(raw) {
    const out = {};
    for (const [key, value] of Object.entries(raw && typeof raw === "object" ? raw : {})) {
      if (value != null && value !== "" && typeof value !== "object") out[key] = value;
    }
    return out;
  }

  function identity(item) {
    const out = {};
    for (const field of ["type", "title", "year", "season", "episode"]) {
      if (item?.[field] != null && item[field] !== "") out[field] = item[field];
    }
    if (out.type === "tv") out.type = "show";
    const ids = scalarIds(item?.ids);
    for (const key of ID_KEYS) {
      if (ids[key] == null && item?.[key] != null && item[key] !== "" && typeof item[key] !== "object") ids[key] = item[key];
    }
    const showIds = { ...scalarIds(item?.ids?.show_ids), ...scalarIds(item?.show_ids) };
    if (Object.keys(ids).length) out.ids = ids;
    if (Object.keys(showIds).length) out.show_ids = showIds;
    return out;
  }

  const FOCUSABLE = "button:not([disabled]),input:not([disabled]),select:not([disabled]),textarea:not([disabled]),a[href],[tabindex]:not([tabindex='-1'])";

  function trapTab(event, container) {
    if (event.key !== "Tab" || !container) return;
    const nodes = [...container.querySelectorAll(FOCUSABLE)].filter((node) => node.offsetParent !== null || node === document.activeElement);
    if (!nodes.length) {
      event.preventDefault();
      container.focus?.({ preventScroll: true });
      return;
    }
    const first = nodes[0];
    const last = nodes[nodes.length - 1];
    const active = document.activeElement;
    if (!container.contains(active)) {
      event.preventDefault();
      (event.shiftKey ? last : first).focus({ preventScroll: true });
    } else if (event.shiftKey && (active === first || active === container)) {
      event.preventDefault();
      last.focus({ preventScroll: true });
    } else if (!event.shiftKey && active === last) {
      event.preventDefault();
      first.focus({ preventScroll: true });
    }
  }

  const isoFromEpoch = (epoch) => new Date(Number(epoch) * 1000).toISOString().replace(/\.\d{3}Z$/, "Z");
  const noonIso = (date) => `${String(date).slice(0, 10)}T12:00:00Z`;
  const fmtDate = (epoch) => {
    const date = new Date(Number(epoch) * 1000);
    const format = window.CW?.ProfileDateTime?.format;
    const options = { day: "numeric", month: "short", year: "numeric" };
    return typeof format === "function" ? format(date, options) : date.toLocaleDateString(undefined, options);
  };
  const fmtWhen = (epoch) => {
    if (!Number(epoch)) return "Date unknown";
    const date = new Date(Number(epoch) * 1000);
    const format = window.CW?.ProfileDateTime?.format;
    const options = { day: "numeric", month: "short", year: "numeric", hour: "2-digit", minute: "2-digit" };
    return typeof format === "function" ? format(date, options) : date.toLocaleString(undefined, options);
  };

  async function sendAdd(feature, ref, payload, name, rewatch = false) {
    const data = await request("/api/editor/send", {
      kind: feature,
      items: [payload],
      providers: [cleanRef(ref)],
      ...(rewatch ? { rewatch: true } : {}),
    });
    const row = (Array.isArray(data?.results) ? data.results : [])[0] || {};
    if (row.error === "rewatch_not_supported") return { ok: false, skipped: true, error: `${name} keeps one play per title` };
    if (row.error === "undated_not_supported") return { ok: false, skipped: true, error: `${name} needs a watch date` };
    if (row.error === "undated_rewatch_not_supported") return { ok: false, skipped: true, error: `Already watched on ${name}, another play needs a date` };
    if (row.error === "provider_not_allowed") return { ok: false, error: `You cannot change ${name}` };
    if (row.error) return { ok: false, error: `${name} could not be updated` };
    const result = row.result || {};
    if (Number(result.confirmed) > 0) return { ok: true };
    if (Number(result.unresolved) > 0) return { ok: false, error: `${name} could not match this title` };
    if (Number(result.errors) > 0) return { ok: false, error: `${name} returned an error` };
    if (Number(result.skipped) > 0) return { ok: true, note: "already there" };
    return row.ok ? { ok: true } : { ok: false, error: `${name} did not confirm the change` };
  }

  function queued(job) {
    const run = removeChain.then(job, job);
    removeChain = run.catch(() => {});
    return run;
  }

  function editorRemove(feature, ref, item, name) {
    return queued(async () => {
      const items = [item];
      const preview = await request("/api/editor/send/preview", { kind: feature, items }, 60000);
      const target = (Array.isArray(preview?.providers) ? preview.providers : []).find((row) => refKey(row) === refKey(ref));
      if (!target) {
        return {
          ok: false,
          error: preview?.empty_reason === "pair_baseline_unavailable"
            ? "Run your sync pairs first. CrossWatch only removes records it has seen in a sync"
            : `CrossWatch has no synced ${FEATURES[feature]?.noun || feature} record for this title on ${name}`,
        };
      }
      const data = await request("/api/editor/send", {
        kind: feature,
        operation: "remove",
        confirmed: true,
        preview_id: preview.preview_id,
        providers: [cleanRef(ref)],
        items,
      });
      const row = (Array.isArray(data?.results) ? data.results : [])[0] || {};
      const result = row.result || {};
      if (row.error) return { ok: false, error: `Removal failed on ${name}` };
      if (Number(result.removed) > 0) return { ok: true };
      if (Number(result.unresolved) > 0) return { ok: false, error: `${name} still reports this title` };
      if (Number(result.skipped) > 0) return { ok: true, note: "already gone" };
      return { ok: false, error: `${name} did not confirm the removal` };
    });
  }

  function watchlistKeys(item) {
    const ids = identity(item).ids || {};
    const show = ["show", "tv", "anime"].includes(String(item?.type || "").toLowerCase());
    const fromIds = ["imdb", "tmdb", "tvdb", "trakt", "simkl"]
      .filter((name) => ids[name] != null && ids[name] !== "")
      .map((name) => (show && name === "tmdb" ? `tmdb:${ids[name]}#show` : `${name}:${ids[name]}`));
    return [...new Set([item?.key, ...(Array.isArray(item?.aliases) ? item.aliases : []), ...fromIds].filter(Boolean).map(String))];
  }

  async function watchlistRemove(ref, item, name) {
    const [key, ...aliases] = watchlistKeys(item);
    if (!key) return { ok: false, error: "This title has no watchlist record to remove" };
    const data = await request("/api/watchlist/delete", {
      provider: ref.provider,
      provider_instance: ref.instance,
      keys: [aliases.length ? { key, aliases } : { key }],
    });
    if (Number(data?.deleted_ok) > 0) return { ok: true };
    const reason = data?.results?.[0]?.per_key?.[0]?.reason;
    if (reason === "not_in_state") return { ok: false, error: `CrossWatch has no watchlist record for this title on ${name}` };
    return { ok: false, error: data?.error ? `Removal failed on ${name}` : `${name} did not confirm the removal` };
  }

  function addTask(feature, ref, item, name, extra = {}) {
    const payload = { ...identity(item), ...extra.fields };
    return { ref: cleanRef(ref), play: extra.play, op: "add", name, exec: () => sendAdd(feature, ref, payload, name, !!extra.rewatch) };
  }

  function removeTask(feature, ref, item, name, extra = {}) {
    const exec = feature === "watchlist"
      ? () => watchlistRemove(cleanRef(ref), item, name)
      : () => editorRemove(feature, cleanRef(ref), { ...(item.key ? { key: item.key } : {}), ...identity(item) }, name);
    return { ref: cleanRef(ref), play: extra.play, scope: extra.scope, op: "remove", name, exec };
  }

  function progressTask(action, ref, record, name) {
    const url = action === "mark_watched" ? "/api/playback_progress/actions/mark_watched" : "/api/playback_progress/actions/remove";
    const exec = async () => {
      if (!record) return { ok: false, error: `No resume point on ${name}` };
      const data = await request(url, { provider: record.provider, instance_id: record.instance_id, remote_id: record.remote_id, canonical_key: record.canonical_key, record });
      return data?.ok ? { ok: true } : { ok: false, error: data?.message || `${name} did not confirm the change` };
    };
    return { ref: cleanRef(ref), op: action, name, exec };
  }

  const toast = (() => {
    let el = null;
    let timer = 0;
    let current = 0;

    function ensure() {
      if (el) return el;
      el = document.createElement("div");
      el.className = "cw-ia-toast cw-page-item-actions";
      el.setAttribute("role", "status");
      el.setAttribute("aria-live", "polite");
      el.hidden = true;
      document.body.appendChild(el);
      el.addEventListener("click", (event) => {
        const button = event.target?.closest?.("[data-ia-toast]");
        if (!button) return;
        const action = el.__actions?.[button.dataset.iaToast];
        if (button.dataset.iaToast === "close") hide();
        else if (action) {
          hide();
          action();
        }
      });
      return el;
    }

    function hide() {
      window.clearTimeout(timer);
      if (el) el.hidden = true;
    }

    function show(id, { tone = "work", text = "", pct = 0, actions = {} }) {
      const node = ensure();
      current = id;
      window.clearTimeout(timer);
      node.__actions = actions;
      node.dataset.tone = tone;
      const symbol = tone === "work" ? "progress_activity" : tone === "ok" ? "check_circle" : "error";
      const buttons = [
        actions.retry ? `<button type="button" data-ia-toast="retry">${esc(actions.retryLabel || "Retry")}</button>` : "",
        actions.undo ? `<button type="button" data-ia-toast="undo">${icon("undo")}Undo</button>` : "",
        tone !== "work" ? `<button type="button" class="cw-ia-toast-close" data-ia-toast="close" aria-label="Dismiss">${icon("close")}</button>` : "",
      ].join("");
      const bar = tone === "work" ? `<span class="cw-ia-toast-bar is-progress" style="width:${Math.max(4, Math.min(100, pct))}%"></span>` : `<span class="cw-ia-toast-bar is-timer${tone === "bad" ? " is-long" : ""}"></span>`;
      node.innerHTML = `<span class="cw-ia-toast-icon${tone === "work" ? " is-spinning" : ""}">${icon(symbol)}</span><span class="cw-ia-toast-text">${esc(text)}</span>${buttons}${bar}`;
      node.hidden = false;
      if (tone !== "work") timer = window.setTimeout(() => { if (current === id) hide(); }, tone === "bad" ? 10000 : 6000);
    }

    return { show, hide, get current() { return current; } };
  })();

  function chipNodes(job, task) {
    const scope = task.scope || job.scope;
    const scopes = (typeof scope === "function" ? scope() : [scope]).filter(Boolean);
    const selector = `[data-ia-ref="${CSS.escape(refKey(task.ref))}"]${task.play != null ? `[data-ia-play="${CSS.escape(String(task.play))}"]` : ""}`;
    return scopes.flatMap((scope) => [...(scope.querySelectorAll?.(selector) || [])]);
  }

  function mark(job, task, phase, message = "") {
    for (const node of chipNodes(job, task)) {
      if (!node.dataset.iaTitle) node.dataset.iaTitle = node.getAttribute("title") || "";
      node.dataset.iaState = phase;
      if (phase === "work") {
        node.setAttribute("aria-busy", "true");
        node.setAttribute("title", `${task.name}: sending…`);
      } else {
        node.removeAttribute("aria-busy");
        node.setAttribute("title", phase === "fail" ? `${task.name}: ${message}` : node.dataset.iaTitle);
      }
      if (phase === "ok") {
        window.setTimeout(() => {
          if (node.dataset.iaState === "ok") delete node.dataset.iaState;
        }, OK_MS);
      }
    }
  }

  async function run(job) {
    const tasks = (job.tasks || []).filter(Boolean);
    if (!tasks.length) return { ok: [], failed: [] };
    const id = ++seq;
    const total = tasks.length;
    let done = 0;
    const ok = [];
    const failed = [];
    const progress = () => toast.show(id, { tone: "work", text: `${job.working} ${done}/${total}`, pct: ((done + 0.35) / total) * 100 });
    tasks.forEach((task) => mark(job, task, "work"));
    progress();
    await Promise.all(tasks.map(async (task) => {
      let result;
      try {
        result = await task.exec();
      } catch (e) {
        result = { ok: false, error: e?.message || "Request failed" };
      }
      done += 1;
      (result.ok ? ok : failed).push({ task, result });
      mark(job, task, result.ok ? "ok" : "fail", result.error || "");
      if (done < total && toast.current === id) progress();
    }));
    const names = (rows) => listNames(rows.map((row) => row.task.name));
    let text;
    if (!failed.length) text = job.done(ok.map((row) => row.task));
    else if (!ok.length) text = failed.length === 1 ? failed[0].result.error : `Failed on ${names(failed)}`;
    else text = `${ok.length} of ${total} done · ${names(failed)} failed`;
    const actions = {};
    if (failed.length && failed.some((row) => !row.result.skipped)) {
      const retry = failed.filter((row) => !row.result.skipped).map((row) => row.task);
      actions.retry = () => void run({ ...job, tasks: retry });
      actions.retryLabel = retry.length === 1 ? `Retry ${retry[0].name}` : "Retry";
    }
    if (ok.length && typeof job.undo === "function") {
      const undoJob = job.undo(ok.map((row) => row.task));
      if (undoJob) actions.undo = () => void run(undoJob);
    }
    if (toast.current === id) toast.show(id, { tone: failed.length ? "bad" : "ok", text, actions });
    if (ok.length) {
      try {
        window.dispatchEvent(new CustomEvent("cw:item-changed", { detail: { feature: job.feature, item: job.item || null } }));
      } catch {}
      try {
        await job.onChanged?.();
      } catch {}
    }
    return { ok, failed };
  }

  const count = (tasks, single) => (tasks.length === 1 ? tasks[0].name : single(tasks.length));

  function addJob(ctx, refs, list, options = {}) {
    const { feature, item } = ctx;
    const fields = options.fields || {};
    const tasks = refs.map((ref) => addTask(feature, ref, item, refLabel(ref, list), { fields: options.fieldsFor ? options.fieldsFor(ref) : fields, rewatch: options.rewatchFor?.(ref), play: options.play }));
    const verb = options.verb || (feature === "ratings" ? "Rating on" : feature === "history" ? "Adding the watch to" : "Adding to");
    return {
      feature,
      item,
      scope: ctx.scope,
      onChanged: ctx.onChanged,
      tasks,
      working: `${verb} ${count(tasks, (n) => `${n} providers`)}…`,
      done: options.done || ((okTasks) => `${feature === "ratings" ? "Rated on" : feature === "history" ? "Watch added to" : "Added to"} ${count(okTasks, (n) => `${n} providers`)}`),
      undo: FEATURES[feature]?.undo && !options.noUndo ? (okTasks) => removeJob(ctx, okTasks.map((task) => task.ref), list, { noUndo: true, play: options.play }) : null,
    };
  }

  function removeJob(ctx, refs, list, options = {}) {
    const { feature, item } = ctx;
    const tasks = refs.map((ref) => removeTask(feature, ref, options.itemFor ? options.itemFor(ref) : item, refLabel(ref, list), { play: options.play }));
    return {
      feature,
      item,
      scope: ctx.scope,
      onChanged: ctx.onChanged,
      tasks,
      working: `${options.verb || "Removing from"} ${count(tasks, (n) => `${n} providers`)}…`,
      done: options.done || ((okTasks) => `Removed from ${count(okTasks, (n) => `${n} providers`)}`),
      undo: FEATURES[feature]?.undo && !options.noUndo
        ? (okTasks) => addJob(ctx, okTasks.map((task) => task.ref), list, {
          noUndo: true,
          fieldsFor: feature === "ratings" ? (ref) => ({ rating: ratingOf(ctx, ref) }) : null,
        })
        : null,
    };
  }

  function ratingOf(ctx, ref) {
    const hit = (ctx.refs?.present || []).find((entry) => refKey(entry) === refKey(ref));
    const value = Number(hit?.rating ?? ctx.rating);
    return Number.isFinite(value) && value >= 1 && value <= 10 ? Math.round(value) : null;
  }

  const menu = (() => {
    let el = null;
    let ctx = null;
    let list = [];
    let armedUntil = 0;
    let armTimer = 0;
    let score = 0;
    let lastFocus = null;

    function ensure() {
      if (el) return el;
      el = document.createElement("div");
      el.className = "cw-ia-pop cw-page-item-actions";
      el.setAttribute("role", "dialog");
      el.hidden = true;
      document.body.appendChild(el);
      el.addEventListener("click", onClick);
      document.addEventListener("pointerdown", (event) => {
        if (el.hidden || el.contains(event.target) || ctx?.anchor?.contains?.(event.target)) return;
        close(false);
      }, true);
      document.addEventListener("keydown", (event) => {
        if (el.hidden) return;
        if (event.key === "Tab") {
          if (el.classList.contains("is-sheet") || el.contains(document.activeElement)) trapTab(event, el);
          return;
        }
        if (event.key !== "Escape") return;
        event.preventDefault();
        event.stopPropagation();
        close(true);
      }, true);
      window.addEventListener("resize", () => {
        if (!el.hidden) place();
      });
      document.addEventListener("scroll", (event) => {
        if (!el.hidden && !el.contains(event.target)) close(false);
      }, true);
      return el;
    }

    function close(restoreFocus) {
      if (!el || el.hidden) return;
      el.hidden = true;
      window.clearTimeout(armTimer);
      armedUntil = 0;
      armedRow = "";
      ctx?.anchor?.classList?.remove("is-ia-open");
      if (restoreFocus && lastFocus?.isConnected) lastFocus.focus({ preventScroll: true });
      ctx = null;
    }

    function place() {
      const anchor = ctx?.anchor;
      if (!anchor?.isConnected) return close(false);
      const narrow = window.matchMedia?.("(max-width: 560px)")?.matches;
      el.classList.toggle("is-sheet", !!narrow);
      if (narrow) {
        el.style.left = "";
        el.style.top = "";
        return;
      }
      const rect = anchor.getBoundingClientRect();
      const width = el.offsetWidth;
      const height = el.offsetHeight;
      const gap = 8;
      let left = Math.min(Math.max(12, rect.left), window.innerWidth - width - 12);
      let top = rect.bottom + gap;
      if (top + height > window.innerHeight - 12 && rect.top - gap - height > 12) top = rect.top - gap - height;
      left = Math.max(12, left);
      el.style.left = `${Math.round(left)}px`;
      el.style.top = `${Math.round(Math.max(12, top))}px`;
    }

    let lists = {};
    let armedRow = "";
    const isNarrow = () => !!window.matchMedia?.("(max-width: 560px), (pointer: coarse)")?.matches;
    const writable = (ref) => list.some((row) => refKey(row) === refKey(ref));
    const progressRows = () => (Array.isArray(ctx?.progressRows) ? ctx.progressRows : []);
    const focusRow = () => progressRows().find((row) => row.rid === ctx?.focusRow) || null;
    const rowAllows = (row, kind) => !!(kind === "mark_watched" ? row?.record?.can_mark_watched : row?.record?.can_remove_progress);
    const groupRefs = (group) => (group.present || []).map(cleanRef).filter((ref) => (lists[group.feature] || []).some((row) => refKey(row) === refKey(ref)));
    const activeGroups = () => (ctx?.groups || []).filter((group) => !ctx.off.has(group.feature) && groupRefs(group).length);

    function progressButtonsHtml() {
      const armed = Date.now() < armedUntil && !armedRow;
      const out = [];
      const row = focusRow();
      if (ctx.focusRow != null) {
        if (!row) return `<p class="cw-ia-note">${icon("info")}This resume point is gone.</p>`;
        if (rowAllows(row, "mark_watched")) out.push(`<button type="button" class="cw-ia-act is-primary" data-ia-act="pg-watched-one">${icon("check_circle")}Mark as watched on ${esc(row.provider_label)}</button>`);
        if (rowAllows(row, "remove")) out.push(`<button type="button" class="cw-ia-act is-danger" data-ia-act="pg-clear-one">${icon("delete")}Remove the resume point</button>`);
        if (!out.length) out.push(`<p class="cw-ia-note">${icon("lock")}${esc(row.provider_label)} does not allow changes to this resume point.</p>`);
        return out.join("");
      }
      const watchable = progressRows().filter((entry) => rowAllows(entry, "mark_watched"));
      const clearable = progressRows().filter((entry) => rowAllows(entry, "remove"));
      if (watchable.length) out.push(`<button type="button" class="cw-ia-act is-primary" data-ia-act="pg-watched-all">${icon("check_circle")}${esc(watchable.length === 1 ? `Mark as watched: ${watchable[0].label}` : `Mark all ${watchable.length} as watched`)}</button>`);
      if (clearable.length) {
        const label = clearable.length === 1 ? "Remove the resume point" : `Remove all ${clearable.length} resume points`;
        out.push(`<button type="button" class="cw-ia-act is-danger${armed ? " is-armed" : ""}" data-ia-act="pg-clear-all">${icon(armed ? "warning" : "delete")}${esc(armed ? `Click again: ${label.toLowerCase()}` : label)}</button>`);
      }
      if (!out.length) out.push(`<p class="cw-ia-note">${icon("lock")}These resume points cannot be changed from here.</p>`);
      return out.join("");
    }

    function everythingHtml() {
      const armed = Date.now() < armedUntil && !armedRow;
      const rows = ctx.groups.map((group) => {
        const refs = groupRefs(group);
        const on = refs.length > 0 && !ctx.off.has(group.feature);
        const note = refs.length ? listNames(refs.map((ref) => refLabel(ref, lists[group.feature] || []))) : (group.present || []).length ? "Not in a sync pair CrossWatch can remove from" : "Not on any provider";
        return `<label class="cw-ia-ev${refs.length ? "" : " is-empty"}">
          <input type="checkbox" data-ia-act="ev-toggle" data-ia-feature="${esc(group.feature)}"${on ? " checked" : ""}${refs.length ? "" : " disabled"}>
          <span class="cw-ia-ev-icon">${icon(FEATURE_ICONS[group.feature] || "check")}</span>
          <span class="cw-ia-ev-copy"><strong>${esc(FEATURE_LABELS[group.feature] || group.feature)}</strong><small>${esc(note)}</small></span>
          <b>${esc(refs.length)}</b>
        </label>`;
      }).join("");
      const total = activeGroups().reduce((sum, group) => sum + groupRefs(group).length, 0);
      const label = total ? `Remove ${plural(total, "record")}` : "Nothing selected";
      return `${rows}<p class="cw-ia-note">${icon("info")}History removal clears every watch on that provider. Other sync sources can add records back.</p>
        <button type="button" class="cw-ia-act is-danger${armed ? " is-armed" : ""}" data-ia-act="ev-remove"${total ? "" : " disabled"}>${icon(armed ? "warning" : "delete_sweep")}${esc(armed ? `Click again: ${label.toLowerCase()}` : label)}</button>`;
    }

    function providerRowsHtml(present, missing) {
      const { feature } = ctx;
      const rows = [...present.map((ref) => ({ ref, on: true })), ...missing.map((ref) => ({ ref, on: false }))];
      if (!rows.length) return "";
      const html = rows.map(({ ref, on }) => {
        const key = refKey(ref);
        const logo = providerLogo(ref.provider);
        const rating = on && feature === "ratings" ? ratingOf(ctx, ref) : null;
        const status = on ? (rating ? `Rated ${rating}/10` : FEATURES[feature].present) : FEATURES[feature].missing;
        const armed = armedRow === key && Date.now() < armedUntil;
        let control = `<span class="cw-ia-prow-lock" title="CrossWatch cannot change this provider">${icon("lock")}</span>`;
        if (writable(ref)) {
          const label = on ? (armed ? "Confirm" : "Remove") : feature === "ratings" ? `Rate ${score}` : "Add";
          control = `<button type="button" class="cw-ia-mini ${on ? "is-danger" : "is-primary"}${armed ? " is-armed" : ""}" data-ia-act="row" data-ia-ref="${esc(key)}" data-ia-on="${on ? "1" : "0"}" aria-label="${esc(`${label} ${refLabel(ref, list)}`)}">${esc(label)}</button>`;
        }
        return `<div class="cw-ia-prow${on ? " is-on" : ""}" data-ia-ref="${esc(key)}">
          <span class="cw-ia-logo">${logo ? `<img src="${esc(logo)}" alt="">` : `<b>${esc(providerName(ref.provider).slice(0, 2))}</b>`}<i aria-hidden="true"></i></span>
          <span class="cw-ia-head-copy"><strong>${esc(refLabel(ref, list))}</strong><small>${esc(status)}</small></span>
          ${control}
        </div>`;
      }).join("");
      return `<div class="cw-ia-prows" role="list" aria-label="Providers">${html}</div>`;
    }

    function buttonsHtml() {
      if (ctx.mode === "everything") return everythingHtml();
      if (ctx.feature === "progress") return progressButtonsHtml();
      const { feature, focus, refs } = ctx;
      const present = (refs.present || []).map(cleanRef);
      const missing = (refs.missing || []).map(cleanRef);
      const armed = Date.now() < armedUntil && !armedRow;
      const out = [];
      const stepper = feature === "ratings" ? `<div class="cw-ia-score"><span>Score</span><button type="button" data-ia-act="score-down" aria-label="Lower score">${icon("remove")}</button><strong>${esc(score)}<small>/10</small></strong><button type="button" data-ia-act="score-up" aria-label="Raise score">${icon("add")}</button></div>` : "";
      if (focus) {
        const name = refLabel(focus, list);
        const isPresent = present.some((ref) => refKey(ref) === refKey(focus));
        if (!writable(focus)) {
          out.push(`<p class="cw-ia-note">${icon("lock")}CrossWatch cannot change the ${esc(FEATURES[feature].noun)} on ${esc(name)}.</p>`);
        } else if (isPresent) {
          if (feature === "ratings") {
            out.push(stepper);
            if (score !== ratingOf(ctx, focus)) out.push(`<button type="button" class="cw-ia-act is-primary" data-ia-act="add-one">${icon("star")}Change to ${esc(score)} on ${esc(name)}</button>`);
          }
          const label = feature === "history" ? `Remove watched status on ${name}` : `Remove from ${name}`;
          const confirmLabel = feature === "history" ? `Click again to remove every watch on ${name}` : label;
          out.push(`<button type="button" class="cw-ia-act is-danger${armed ? " is-armed" : ""}" data-ia-act="remove-one">${icon(armed ? "warning" : "delete")}${esc(armed ? confirmLabel : label)}</button>`);
        } else if (feature === "ratings") {
          out.push(stepper, `<button type="button" class="cw-ia-act is-primary" data-ia-act="add-one">${icon("star")}Rate ${esc(score)} on ${esc(name)}</button>`);
        } else if (feature === "history") {
          const when = ctx.epoch ? `Copy the watch of ${fmtDate(ctx.epoch)} to ${name}` : `Mark watched now on ${name}`;
          out.push(`<button type="button" class="cw-ia-act is-primary" data-ia-act="add-one">${icon("add")}${esc(when)}</button>`);
        } else {
          out.push(`<button type="button" class="cw-ia-act is-primary" data-ia-act="add-one">${icon("add")}Add to ${esc(name)}</button>`);
        }
      } else {
        const addable = missing.filter(writable);
        const removable = present.filter(writable);
        if (addable.length) {
          const target = addable.length === 1 ? refLabel(addable[0], list) : `the ${addable.length} missing`;
          if (feature === "ratings") out.push(stepper, `<button type="button" class="cw-ia-act is-primary" data-ia-act="add-all">${icon("star")}Rate ${esc(score)} on ${esc(target)}</button>`);
          else if (feature === "history") out.push(`<button type="button" class="cw-ia-act is-primary" data-ia-act="add-all">${icon("add")}${esc(ctx.epoch ? `Copy the watch to ${target}` : `Mark watched now on ${target}`)}</button>`);
          else out.push(`<button type="button" class="cw-ia-act is-primary" data-ia-act="add-all">${icon("add")}Add to ${esc(target)}</button>`);
        }
        if (removable.length) {
          const target = removable.length === 1 ? refLabel(removable[0], list) : `all ${removable.length}`;
          const needsArm = feature === "history" || removable.length > 1;
          const label = feature === "history" ? `Remove watched status on ${target}` : `Remove from ${target}`;
          out.push(`<button type="button" class="cw-ia-act is-danger${armed ? " is-armed" : ""}" data-ia-act="remove-all" data-ia-arm="${needsArm ? "1" : ""}">${icon(armed ? "warning" : "delete")}${esc(armed ? `Click again: ${label.charAt(0).toLowerCase()}${label.slice(1)}` : label)}</button>`);
        }
        if (!addable.length && !removable.length) out.push(`<p class="cw-ia-note">${icon("lock")}None of these providers can be changed from here.</p>`);
        if (isNarrow()) out.push(providerRowsHtml(present, missing));
      }
      if (feature === "history" && typeof ctx.openHistory === "function") out.push(`<button type="button" class="cw-ia-act is-ghost" data-ia-act="history">${icon("history")}Watch history and rewatches</button>`);
      if (typeof ctx.openDetails === "function") out.push(`<button type="button" class="cw-ia-act is-ghost" data-ia-act="details">${icon("dashboard")}All providers and features</button>`);
      return out.join("");
    }

    function headHtml() {
      if (ctx.mode === "everything") {
        return `<div class="cw-ia-head"><span class="cw-ia-logo is-icon is-danger">${icon("delete_sweep")}</span><span class="cw-ia-head-copy"><strong>Remove from everything</strong><small>${esc(ctx.item?.title || "This title")} on every provider</small></span></div>`;
      }
      const { feature, focus, refs } = ctx;
      const present = refs.present || [];
      const total = present.length + (refs.missing || []).length;
      if (feature === "progress") {
        const row = focusRow();
        if (row) {
          const logo = providerLogo(row.provider);
          return `<div class="cw-ia-head"><span class="cw-ia-logo">${logo ? `<img src="${esc(logo)}" alt="">` : `<b>${esc(providerName(row.provider).slice(0, 2))}</b>`}</span><span class="cw-ia-head-copy"><strong>${esc(row.label)}</strong><small>${esc(`${Math.round(Number(row.record?.progress_percent) || 0)}% watched`)}</small></span></div>`;
        }
        const rows = progressRows();
        return `<div class="cw-ia-head"><span class="cw-ia-logo is-icon">${icon(FEATURE_ICONS.progress)}</span><span class="cw-ia-head-copy"><strong>${esc(plural(rows.length, "resume point"))}</strong><small>${esc(listNames([...new Set(rows.map((entry) => entry.provider_label))]))}</small></span></div>`;
      }
      if (focus) {
        const isPresent = present.some((ref) => refKey(ref) === refKey(focus));
        const logo = providerLogo(focus.provider);
        const rating = ratingOf(ctx, focus);
        const sub = isPresent
          ? (feature === "ratings" && rating ? `Rated ${rating}/10` : FEATURES[feature].present)
          : FEATURES[feature].missing;
        return `<div class="cw-ia-head"><span class="cw-ia-logo">${logo ? `<img src="${esc(logo)}" alt="">` : `<b>${esc(providerName(focus.provider).slice(0, 2))}</b>`}</span><span class="cw-ia-head-copy"><strong>${esc(refLabel(focus, list))}</strong><small>${esc(sub)}</small></span></div>`;
      }
      const missingNames = (refs.missing || []).map((ref) => refLabel(ref, list));
      const title = !refs.missing?.length ? `On every ${FEATURES[feature].noun === "history" ? "provider" : FEATURES[feature].noun}` : `On ${present.length} of ${total}`;
      const sub = missingNames.length ? `Missing on ${listNames(missingNames)}` : listNames(present.map((ref) => refLabel(ref, list)));
      return `<div class="cw-ia-head"><span class="cw-ia-logo is-icon">${icon(FEATURE_ICONS[feature] || "bookmark")}</span><span class="cw-ia-head-copy"><strong>${esc(title)}</strong><small>${esc(sub)}</small></span></div>`;
    }

    function render() {
      if (!ctx || !el) return;
      el.setAttribute("aria-label", ctx.mode === "everything" ? `Remove ${ctx.item?.title || "this title"} from everything` : `${ctx.item?.title || "Title"} ${FEATURES[ctx.feature].noun}`);
      el.innerHTML = `${headHtml()}<div class="cw-ia-actions">${list.loading ? `<p class="cw-ia-note">${icon("progress_activity")}Checking providers…</p>` : buttonsHtml()}</div>`;
      place();
    }

    function arm() {
      if (Date.now() < armedUntil) return false;
      armedUntil = Date.now() + ARM_MS;
      window.clearTimeout(armTimer);
      armTimer = window.setTimeout(() => {
        armedUntil = 0;
        armedRow = "";
        render();
      }, ARM_MS);
      render();
      return true;
    }

    function onClick(event) {
      const button = event.target?.closest?.("[data-ia-act]");
      if (!button || !ctx) return;
      const action = button.dataset.iaAct;
      const current = ctx;
      if (action === "ev-toggle") {
        if (button.checked) current.off.delete(button.dataset.iaFeature);
        else current.off.add(button.dataset.iaFeature);
        armedUntil = 0;
        render();
        return;
      }
      if (action === "ev-remove") {
        if (arm()) return;
        const groups = activeGroups();
        const tasks = groups.flatMap((group) => groupRefs(group).map((ref) => {
          const task = removeTask(group.feature, ref, group.item, `${refLabel(ref, lists[group.feature] || [])} ${FEATURES[group.feature].noun}`, { scope: group.scope });
          return task;
        }));
        close(true);
        void run({
          feature: "everything",
          item: current.item,
          scope: null,
          onChanged: current.onChanged,
          tasks,
          working: `Removing ${plural(tasks.length, "record")}…`,
          done: (okTasks) => `Removed ${plural(okTasks.length, "record")}`,
        });
        return;
      }
      if (action.startsWith("pg-")) {
        const kind = action.includes("watched") ? "mark_watched" : "remove";
        const rows = action.endsWith("-one") ? [focusRow()].filter(Boolean) : progressRows().filter((row) => rowAllows(row, kind));
        if (action === "pg-clear-all" && rows.length > 1 && arm()) return;
        close(true);
        const tasks = rows.map((row) => ({ ...progressTask(kind, row, row.record, row.label), play: row.rid }));
        const many = (list2) => (list2.length === 1 ? list2[0].name : plural(list2.length, "resume point"));
        void run({
          feature: "progress",
          item: current.item,
          scope: current.scope,
          onChanged: current.onChanged,
          tasks,
          working: `${kind === "mark_watched" ? "Marking as watched:" : "Removing:"} ${many(tasks)}…`,
          done: (okTasks) => `${kind === "mark_watched" ? "Marked as watched:" : "Removed:"} ${many(okTasks)}`,
        });
        return;
      }
      const present = (current.refs.present || []).map(cleanRef).filter(writable);
      const missing = (current.refs.missing || []).map(cleanRef).filter(writable);
      const addOptions = () => {
        if (current.feature === "ratings") return { fields: { rating: score }, noUndo: true };
        if (current.feature === "history") return { fields: { watched_at: current.epoch ? isoFromEpoch(current.epoch) : new Date().toISOString() } };
        if (current.feature === "collection") return { fields: { collected_at: new Date().toISOString() } };
        return {};
      };
      if (action === "row") {
        const key = button.dataset.iaRef;
        const ref = [...(current.refs.present || []), ...(current.refs.missing || [])].map(cleanRef).find((entry) => refKey(entry) === key);
        if (!ref || !writable(ref)) return;
        if (button.dataset.iaOn === "1") {
          if (current.feature === "history" && !(armedRow === key && Date.now() < armedUntil)) {
            armedRow = key;
            armedUntil = 0;
            arm();
            return;
          }
          armedRow = "";
          close(true);
          void run(removeJob(current, [ref], list));
        } else {
          close(true);
          void run(addJob(current, [ref], list, addOptions()));
        }
        return;
      }
      if (action === "score-down" || action === "score-up") {
        score = Math.max(1, Math.min(10, score + (action === "score-up" ? 1 : -1)));
        render();
        return;
      }
      if (action === "history") {
        close(false);
        current.openHistory();
        return;
      }
      if (action === "details") {
        close(false);
        current.openDetails();
        return;
      }
      if (action === "add-one") {
        close(true);
        void run(addJob(current, [current.focus], list, addOptions()));
        return;
      }
      if (action === "add-all") {
        close(true);
        void run(addJob(current, missing, list, addOptions()));
        return;
      }
      if (action === "remove-one") {
        if (current.feature === "history" && armedRow) {
          armedRow = "";
          armedUntil = 0;
        }
        if (current.feature === "history" && arm()) return;
        close(true);
        void run(removeJob(current, [current.focus], list));
        return;
      }
      if (action === "remove-all") {
        if (armedRow) {
          armedRow = "";
          armedUntil = 0;
        }
        if (button.dataset.iaArm === "1" && arm()) return;
        close(true);
        void run(removeJob(current, present, list));
      }
    }

    async function open(options) {
      if (!canWrite()) return false;
      ensure();
      const sameAnchor = ctx?.anchor === options.anchor && !el.hidden;
      close(false);
      if (sameAnchor) return true;
      ctx = { ...options, refs: { present: (options.refs?.present || []).map((ref) => ({ ...ref, ...cleanRef(ref) })), missing: (options.refs?.missing || []).map(cleanRef) } };
      ctx.focus = options.focus ? cleanRef(options.focus) : null;
      ctx.off = new Set();
      lastFocus = options.anchor;
      armedUntil = 0;
      score = ratingOf(ctx, ctx.focus || {}) || Math.round(Number(options.rating)) || 8;
      list = Object.assign([], { loading: true });
      lists = {};
      options.anchor?.classList?.add("is-ia-open");
      el.hidden = false;
      render();
      const token = ctx;
      if (options.mode === "everything") {
        const features = (options.groups || []).map((group) => group.feature);
        const loaded = await Promise.all(features.map((feature) => targets(feature)));
        if (ctx !== token) return true;
        lists = Object.fromEntries(features.map((feature, index) => [feature, loaded[index]]));
        list = [];
      } else if (options.feature === "progress") {
        list = [];
      } else {
        const rows = await targets(options.feature);
        if (ctx !== token) return true;
        list = rows;
        if (options.fillMissing) {
          const seen = new Set([...ctx.refs.present, ...ctx.refs.missing].map(refKey));
          ctx.refs.missing.push(...rows.filter((row) => !seen.has(refKey(row))).map(cleanRef));
        }
      }
      render();
      el.querySelector("button")?.focus({ preventScroll: true });
      return true;
    }

    return { open, close };
  })();

  const sheet = (() => {
    let root = null;
    let ctx = null;
    let data = null;
    let list = [];
    let when = "now";
    let customDate = "";
    let picked = null;
    let armed = { key: "", until: 0 };
    let armTimer = 0;
    let lastFocus = null;
    let loadSeq = 0;
    const playNotes = new Map();

    function ensure() {
      if (root) return root;
      root = document.createElement("div");
      root.className = "cw-ia-sheet-wrap cw-page-item-actions";
      root.hidden = true;
      root.innerHTML = `<div class="cw-ia-sheet-backdrop" data-ia-sheet="close"></div><section class="cw-ia-sheet" role="dialog" aria-modal="true" aria-labelledby="cw-ia-sheet-title" tabindex="-1"></section>`;
      document.body.appendChild(root);
      root.addEventListener("click", onClick);
      root.addEventListener("change", onChange);
      document.addEventListener("keydown", (event) => {
        if (!root || root.hidden || menuIsOpen()) return;
        if (event.key === "Tab") {
          trapTab(event, dialog());
          return;
        }
        if (event.key !== "Escape") return;
        event.preventDefault();
        event.stopPropagation();
        close();
      }, true);
      return root;
    }

    const menuIsOpen = () => !!document.querySelector(".cw-ia-pop:not([hidden])");
    const dialog = () => root?.querySelector(".cw-ia-sheet");
    const writable = (ref) => list.find((row) => refKey(row) === refKey(ref)) || null;
    const plays = () => (Array.isArray(data?.plays) ? data.plays : []);
    const holders = () => (Array.isArray(data?.holders) ? data.holders : []);
    const playsOn = (ref) => plays().filter((play) => (play.present || []).some((entry) => refKey(entry) === refKey(ref)));
    const holds = (ref) => holders().some((row) => refKey(row) === refKey(ref)) || playsOn(ref).length > 0;

    function columns() {
      const map = new Map();
      for (const ref of [...list, ...(data?.endpoints || []), ...holders(), ...plays().flatMap((play) => play.present || [])]) {
        const clean = cleanRef(ref);
        if (clean.provider && !map.has(refKey(clean))) map.set(refKey(clean), clean);
      }
      return [...map.values()].sort((a, b) => refLabel(a, list).localeCompare(refLabel(b, list), undefined, { sensitivity: "base" }));
    }

    const keepsOne = (ref) => playNotes.get(refKey(ref)) === true;

    function blockReason(ref) {
      const target = writable(ref);
      if (!target) return "CrossWatch cannot write history here";
      if (when === "release" && !ctx.releaseDate) return "No release date known";
      if (when === "unknown" && !target.undated_write) return "Needs a watch date";
      if (when === "unknown" && holds(ref)) return "Already watched here, another play needs a date";
      if (holds(ref) && !target.rewatch_write) return "Keeps one play per title";
      return "";
    }

    function close() {
      if (!root || root.hidden) return;
      root.hidden = true;
      loadSeq += 1;
      window.clearTimeout(armTimer);
      document.documentElement.classList.remove("cw-ia-sheet-open");
      if (lastFocus?.isConnected) lastFocus.focus({ preventScroll: true });
      ctx = null;
    }

    function armKey(key) {
      if (armed.key === key && Date.now() < armed.until) return false;
      armed = { key, until: Date.now() + ARM_MS };
      window.clearTimeout(armTimer);
      armTimer = window.setTimeout(() => {
        armed = { key: "", until: 0 };
        render();
      }, ARM_MS);
      render();
      return true;
    }

    const isArmed = (key) => armed.key === key && Date.now() < armed.until;

    function chip(ref, play, index) {
      const entry = (play.present || []).find((row) => refKey(row) === refKey(ref));
      const name = refLabel(ref, list);
      const logo = providerLogo(ref.provider);
      const mark = logo ? `<img src="${esc(logo)}" alt="" loading="lazy">` : `<b>${esc(providerName(ref.provider).slice(0, 2))}</b>`;
      const target = writable(ref);
      let title;
      let disabled = false;
      if (entry) {
        const single = !(entry.event && target?.rewatch_read && target?.rewatch_write);
        title = !target ? `On ${name}` : single ? `Remove the watched status on ${name}` : `Remove this watch from ${name}`;
        disabled = !target;
      } else if (!target) {
        title = `Not on ${name}`;
        disabled = true;
      } else if (holds(ref) && !target.rewatch_write) {
        title = `${name} keeps one play per title`;
        disabled = true;
      } else if (!Number(play.epoch) && !target.undated_write) {
        title = `${name} needs a watch date`;
        disabled = true;
      } else if (!Number(play.epoch) && holds(ref)) {
        title = `Already watched on ${name}, another play needs a date`;
        disabled = true;
      } else {
        title = holds(ref) && keepsOne(ref) ? `Copy this watch to ${name}. ${name} keeps one play, so its older play is replaced` : `Copy this watch to ${name}`;
      }
      return `<button type="button" class="cw-ia-chip${entry ? " is-on" : ""}" data-ia-ref="${esc(refKey(ref))}" data-ia-play="${index}" data-ia-sheet="chip" title="${esc(title)}" aria-label="${esc(title)}"${disabled ? " disabled" : ""}>${mark}<i aria-hidden="true"></i></button>`;
    }

    function render() {
      const host = dialog();
      if (!host || !ctx) return;
      const cols = columns();
      const rows = plays();
      const title = ctx.item?.title || "Title";
      const loading = !data;
      const rowsHtml = loading
        ? `<p class="cw-ia-sheet-empty">${icon("progress_activity")}Loading plays…</p>`
        : data.error
          ? `<p class="cw-ia-sheet-empty is-bad">${icon("error")}${esc(data.error)}</p>`
          : rows.length
            ? rows.map((play, index) => {
              const key = `play:${index}`;
              const writableRefs = (play.present || []).map(cleanRef).filter(writable);
              const armedRow = isArmed(key);
              const wipes = writableRefs.filter((ref) => clearsAll(play, ref)).map((ref) => refLabel(ref, list));
              const label = armedRow ? "Click again to remove this watch everywhere" : "Remove this watch everywhere";
              const warning = armedRow && wipes.length
                ? `<p class="cw-ia-play-warn">${icon("warning")}${esc(`${listNames(wipes)} ${wipes.length === 1 ? "only knows" : "only know"} whether you watched this, not each watch. Removing it there clears every watch on ${wipes.length === 1 ? "that provider" : "those providers"}.`)}</p>`
                : "";
              return `<div class="cw-ia-play${armedRow ? " is-armed" : ""}">
                <span class="cw-ia-play-copy"><strong>${esc(fmtWhen(play.epoch))}</strong><small>${esc(plural((play.present || []).length, "provider"))}</small></span>
                <span class="cw-ia-play-chips">${cols.map((ref) => chip(ref, play, index)).join("")}</span>
                <button type="button" class="cw-ia-icon-btn${armedRow ? " is-armed" : ""}" data-ia-sheet="remove-play" data-ia-index="${index}" title="${esc(label)}" aria-label="${esc(label)}"${writableRefs.length ? "" : " disabled"}>${icon(armedRow ? "warning" : "delete")}</button>
                ${warning}
              </div>`;
            }).join("")
            : `<p class="cw-ia-sheet-empty">${icon("visibility_off")}Not watched on any provider yet.</p>`;
      const undated = list.some((row) => row.undated_write);
      if (when === "unknown" && !undated) when = "now";
      const options = [["now", "Now"], ["release", "On release"], ["custom", "Pick date"], ...(undated ? [["unknown", "No date"]] : [])];
      const segs = options.map(([value, label]) => {
        const disabled = value === "release" && !ctx.releaseDate;
        return `<button type="button" role="radio" aria-checked="${when === value}" class="cw-ia-seg${when === value ? " is-on" : ""}" data-ia-sheet="when" data-ia-when="${value}"${disabled ? ' disabled title="No release date known"' : ""}>${esc(label)}</button>`;
      }).join("");
      const writableCols = cols.filter((ref) => writable(ref));
      if (!picked) picked = new Set(writableCols.filter((ref) => !blockReason(ref)).map(refKey));
      const pills = writableCols.map((ref) => {
        const reason = blockReason(ref);
        const on = !reason && picked.has(refKey(ref));
        const target = writable(ref);
        const gate = target?.rewatch_gate === "pro_vip" && holds(ref) ? `${refLabel(ref, list)} rewatches need a Pro or VIP account` : "";
        const logo = providerLogo(ref.provider);
        const tip = reason || gate || (on ? "Selected" : "Skipped");
        return `<button type="button" class="cw-ia-pill${on ? " is-on" : ""}" data-ia-sheet="pick" data-ia-ref="${esc(refKey(ref))}" aria-pressed="${on}" title="${esc(tip)}"${reason ? " disabled" : ""}>
          <span class="cw-ia-pill-logo">${logo ? `<img src="${esc(logo)}" alt="">` : `<b>${esc(providerName(ref.provider).slice(0, 2))}</b>`}</span><span>${esc(refLabel(ref, list))}</span>
        </button>`;
      }).join("");
      const chosen = writableCols.filter((ref) => !blockReason(ref) && picked.has(refKey(ref)));
      const anyPlays = rows.length > 0 || holders().length > 0;
      const notes = [...new Set(writableCols.map((ref) => {
        const reason = blockReason(ref);
        return reason && reason !== "No release date known" ? `${refLabel(ref, list)}: ${reason.charAt(0).toLowerCase()}${reason.slice(1)}` : "";
      }).filter(Boolean))];
      for (const ref of chosen) {
        if (holds(ref) && keepsOne(ref)) notes.push(`${refLabel(ref, list)} is set to keep one play per title, so this replaces its older play`);
      }
      const dateOk = when !== "custom" || /^\d{4}-\d{2}-\d{2}$/.test(customDate);
      const addLabel = !chosen.length ? "Pick a provider" : `${anyPlays ? "Log a rewatch on" : "Mark watched on"} ${chosen.length === writableCols.length && chosen.length > 1 ? `all ${chosen.length}` : chosen.length} provider${chosen.length === 1 ? "" : "s"}`;
      const clearable = holders().some((ref) => writable(ref)) || rows.some((play) => (play.present || []).some(writable));
      const clearArmed = isArmed("clear");
      host.innerHTML = `<header class="cw-ia-sheet-head">
          <span class="cw-ia-sheet-poster">${ctx.poster ? `<img src="${esc(ctx.poster)}" alt="" onerror="this.remove()">` : ""}</span>
          <span class="cw-ia-sheet-title"><h2 id="cw-ia-sheet-title">${esc(title)}</h2><small>Watch history · ${esc(loading ? "loading" : data?.truncated ? `newest ${plural(rows.length, "play")} shown` : plural(rows.length, "play"))}</small></span>
          <button type="button" class="cw-ia-icon-btn" data-ia-sheet="close" aria-label="Close">${icon("close")}</button>
        </header>
        <div class="cw-ia-plays">${rowsHtml}</div>
        <div class="cw-ia-log">
          <h3>Log a play</h3>
          <div class="cw-ia-segs" role="radiogroup" aria-label="When did you watch it">${segs}</div>
          ${when === "custom" ? `<label class="cw-ia-date"><span>Watched on</span><input type="date" data-ia-sheet="date" value="${esc(customDate)}" max="${esc(new Date().toISOString().slice(0, 10))}"></label>` : ""}
          <span class="cw-ia-log-label">Send to</span>
          <div class="cw-ia-pills">${pills || `<p class="cw-ia-sheet-empty">${icon("lock")}No provider can receive history from here.</p>`}</div>
          ${notes.length ? `<p class="cw-ia-log-note">${esc(notes.join(" · "))}</p>` : ""}
          <button type="button" class="cw-ia-primary" data-ia-sheet="add"${chosen.length && dateOk && !loading ? "" : " disabled"}>${icon("add")}${esc(addLabel)}</button>
          ${clearable ? `<button type="button" class="cw-ia-quiet-danger${clearArmed ? " is-armed" : ""}" data-ia-sheet="clear">${icon(clearArmed ? "warning" : "delete")}${esc(clearArmed ? "Click again to mark as unwatched everywhere" : "Mark as unwatched everywhere")}</button>` : ""}
        </div>`;
    }

    function jobCtx() {
      const owner = ctx;
      return {
        feature: "history",
        item: owner.item,
        scope: () => (ctx === owner ? [dialog()] : []),
        onChanged: async () => {
          if (ctx === owner) await load();
          await owner.onChanged?.();
        },
      };
    }

    function clearsAll(play, ref) {
      const entry = (play.present || []).find((row) => refKey(row) === refKey(ref));
      const target = writable(ref);
      return !(entry?.event && target?.rewatch_read && target?.rewatch_write);
    }

    function playItem(play, ref) {
      const entry = (play.present || []).find((row) => refKey(row) === refKey(ref));
      const target = writable(ref);
      const base = { ...ctx.item, ...identity(play) };
      if (entry?.event && target?.rewatch_read && target?.rewatch_write) return { ...base, key: entry.key };
      const { key, ...rest } = base;
      return rest;
    }

    function onChange(event) {
      if (event.target?.matches?.("[data-ia-sheet=date]")) {
        customDate = String(event.target.value || "");
        render();
      }
    }

    function onClick(event) {
      const button = event.target?.closest?.("[data-ia-sheet]");
      if (!button || !ctx) return;
      const action = button.dataset.iaSheet;
      if (action === "close") return close();
      if (action === "when") {
        when = button.dataset.iaWhen || "now";
        if (when === "custom" && !customDate) customDate = new Date().toISOString().slice(0, 10);
        picked = null;
        render();
        return;
      }
      if (action === "pick") {
        const key = button.dataset.iaRef;
        if (picked.has(key)) picked.delete(key);
        else picked.add(key);
        render();
        return;
      }
      const job = jobCtx();
      if (action === "chip") {
        const play = plays()[Number(button.dataset.iaPlay)];
        const ref = cleanRef({ provider: button.dataset.iaRef.split(":")[0], instance: button.dataset.iaRef.split(":").slice(1).join(":") });
        if (!play) return;
        const name = refLabel(ref, list);
        const present = (play.present || []).some((row) => refKey(row) === refKey(ref));
        const index = button.dataset.iaPlay;
        if (present) {
          void run({ ...job, tasks: [removeTask("history", ref, playItem(play, ref), name, { play: index })], working: `Removing the watch from ${name}…`, done: () => `Watch removed from ${name}` });
        } else {
          const rewatch = holds(ref);
          const fields = Number(play.epoch) ? { watched_at: isoFromEpoch(play.epoch) } : { watched: true, _cw_watched_state: true };
          void run({ ...job, tasks: [addTask("history", ref, ctx.item, name, { fields, rewatch, play: index })], working: `Copying the watch to ${name}…`, done: () => `Watch copied to ${name}` });
        }
        return;
      }
      if (action === "remove-play") {
        const index = button.dataset.iaIndex;
        if (armKey(`play:${index}`)) return;
        armed = { key: "", until: 0 };
        const play = plays()[Number(index)];
        const refs = (play?.present || []).map(cleanRef).filter(writable);
        void run({
          ...job,
          tasks: refs.map((ref) => removeTask("history", ref, playItem(play, ref), refLabel(ref, list), { play: index })),
          working: `Removing the watch from ${refs.length === 1 ? refLabel(refs[0], list) : `${refs.length} providers`}…`,
          done: (okTasks) => `Watch removed from ${okTasks.length === 1 ? okTasks[0].name : `${okTasks.length} providers`}`,
        });
        return;
      }
      if (action === "clear") {
        if (armKey("clear")) return;
        armed = { key: "", until: 0 };
        const refs = columns().filter((ref) => writable(ref) && holds(ref));
        const { key, ...titleItem } = ctx.item;
        void run({
          ...job,
          tasks: refs.map((ref) => removeTask("history", ref, titleItem, refLabel(ref, list))),
          working: `Marking as unwatched on ${refs.length === 1 ? refLabel(refs[0], list) : `${refs.length} providers`}…`,
          done: (okTasks) => `Marked as unwatched on ${okTasks.length === 1 ? okTasks[0].name : `${okTasks.length} providers`}`,
        });
        return;
      }
      if (action === "add") {
        const fields = when === "unknown"
          ? { watched: true, _cw_watched_state: true }
          : { watched_at: when === "release" ? noonIso(ctx.releaseDate) : when === "custom" ? noonIso(customDate) : new Date().toISOString().replace(/\.\d{3}Z$/, "Z") };
        const refs = columns().filter((ref) => writable(ref) && !blockReason(ref) && picked.has(refKey(ref)));
        const rewatchAny = plays().length > 0 || holders().length > 0;
        void run({
          ...job,
          tasks: refs.map((ref) => addTask("history", ref, ctx.item, refLabel(ref, list), { fields, rewatch: holds(ref) })),
          working: `${rewatchAny ? "Logging the rewatch on" : "Marking watched on"} ${refs.length === 1 ? refLabel(refs[0], list) : `${refs.length} providers`}…`,
          done: (okTasks) => `${rewatchAny ? "Rewatch logged on" : "Marked watched on"} ${okTasks.length === 1 ? okTasks[0].name : `${okTasks.length} providers`}`,
        });
      }
    }

    async function loadNotes(rows, mine) {
      const pending = rows.filter((row) => !playNotes.has(refKey(row)));
      if (!pending.length) return;
      await Promise.all(pending.map(async (row) => {
        try {
          const result = await request(`/api/editor/send/history-notes?provider=${encodeURIComponent(row.provider)}&instance=${encodeURIComponent(row.instance)}`, undefined, 30000);
          playNotes.set(refKey(row), result?.keeps_one_play === true);
        } catch {
          playNotes.set(refKey(row), false);
        }
      }));
      if (mine === loadSeq && ctx) render();
    }

    async function load() {
      if (!ctx) return;
      const mine = ++loadSeq;
      const tmdb = String(ctx.item?.ids?.tmdb || ctx.item?.tmdb || "");
      try {
        const [result, rows] = await Promise.all([
          request(`/api/profile/title/plays?tmdb=${encodeURIComponent(tmdb)}`, undefined, 60000),
          targets("history"),
        ]);
        if (mine !== loadSeq || !ctx) return;
        data = result;
        list = rows;
        void loadNotes(rows.filter((row) => row.provider === "TRAKT"), mine);
      } catch (e) {
        if (mine !== loadSeq || !ctx) return;
        data = { plays: [], error: e?.message || "Plays could not be loaded" };
      }
      picked = null;
      render();
    }

    function open(options) {
      if (!canWrite()) return false;
      const tmdb = String(options?.item?.ids?.tmdb || options?.item?.tmdb || "");
      if (!tmdb) return false;
      ensure();
      menu.close(false);
      lastFocus = document.activeElement;
      ctx = { ...options, item: { ...identity(options.item), type: "movie", ...(options.item?.key ? { key: options.item.key } : {}) } };
      data = null;
      list = [];
      when = "now";
      customDate = "";
      picked = null;
      armed = { key: "", until: 0 };
      playNotes.clear();
      root.hidden = false;
      document.documentElement.classList.add("cw-ia-sheet-open");
      render();
      dialog()?.focus({ preventScroll: true });
      void load();
      return true;
    }

    return { open, close };
  })();

  (window.CW ||= {}).ItemActions = { canWrite, openMenu: menu.open, closeMenu: menu.close, openHistory: sheet.open, targets };
})();
