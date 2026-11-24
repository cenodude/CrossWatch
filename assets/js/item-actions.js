/* assets/js/item-actions.js */
/* CrossWatch - add and remove one title on providers from its provider icons */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const T = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.t?.(key, vars) ?? key;
  const TH = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.html?.(key, vars) ?? key;
  const PLURAL_KEYS = { record: "record", "resume point": "resumePoint", account: "account", play: "play" };
  const esc = (value) => String(value ?? "").replace(/[&<>"']/g, (ch) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[ch]));
  const icon = (name) => `<span class="material-symbols-rounded" aria-hidden="true">${esc(name)}</span>`;
  const numberFmt = new Intl.NumberFormat((window.CW?.I18n || window.__cwI18n)?.locale || window.__CW_LOCALE || undefined);
  const FEATURES = {
    watchlist: { noun: T("library.actions.noun.watchlist"), present: T("library.actions.present.watchlist"), missing: T("library.actions.missing.watchlist"), undo: true },
    history: { noun: T("library.actions.noun.history"), present: T("library.actions.present.history"), missing: T("library.actions.missing.history"), undo: false },
    ratings: { noun: T("library.actions.noun.ratings"), present: T("library.actions.present.ratings"), missing: T("library.actions.missing.ratings"), undo: true },
    collection: { noun: T("library.actions.noun.collection"), present: T("library.actions.present.collection"), missing: T("library.actions.missing.collection"), undo: true },
    progress: { noun: T("library.actions.noun.resumePoint"), present: T("library.actions.present.progress"), missing: T("library.actions.missing.progress"), undo: false },
  };
  const FEATURE_ICONS = { watchlist: "bookmark", history: "history", ratings: "star", collection: "video_library", progress: "resume" };
  const FEATURE_LABELS = { watchlist: T("library.actions.feature.watchlist"), history: T("library.actions.feature.history"), ratings: T("library.actions.feature.ratings"), collection: T("library.actions.feature.collection") };
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
  const plural = (count, word) => T(`library.actions.count.${PLURAL_KEYS[word] || word}`, {count: Number(count) || 0});
  const listNames = (names) => (names.length <= 1 ? names.join("") : `${names.slice(0, -1).join(", ")} ${T("library.actions.and")} ${names[names.length - 1]}`);

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
      if (!res.ok) throw new Error((typeof data?.detail === "string" && data.detail) || data?.message || data?.error || T("library.actions.requestFailed", {status: String(res.status)}));
      return data;
    } catch (e) {
      if (e?.name === "AbortError") throw new Error(T("library.actions.slow"));
      throw e;
    } finally {
      if (timer) window.clearTimeout(timer);
    }
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
    return typeof format === "function" ? format(date, options) : date.toLocaleDateString((window.CW?.I18n || window.__cwI18n)?.locale, options);
  };
  const fmtWhen = (epoch) => {
    if (!Number(epoch)) return T("library.actions.dateUnknown");
    const date = new Date(Number(epoch) * 1000);
    const format = window.CW?.ProfileDateTime?.format;
    const options = { day: "numeric", month: "short", year: "numeric", hour: "2-digit", minute: "2-digit" };
    return typeof format === "function" ? format(date, options) : date.toLocaleString((window.CW?.I18n || window.__cwI18n)?.locale, options);
  };

  async function sendAdd(feature, ref, payload, name, rewatch = false) {
    const data = await request("/api/editor/send", {
      kind: feature,
      items: [payload],
      providers: [cleanRef(ref)],
      ...(rewatch ? { rewatch: true } : {}),
    });
    const row = (Array.isArray(data?.results) ? data.results : [])[0] || {};
    const excluded = Array.isArray(row.excluded) ? row.excluded[0]?.reason : "";
    if (excluded === "unknown_watch_date_unsupported") return { ok: false, skipped: true, error: T("library.actions.err.unknownDate", {name}) };
    if (excluded === "unknown_rewatch_unsupported") return { ok: false, skipped: true, error: T("library.actions.err.unknownRewatch", {name}) };
    if (row.error === "rewatch_not_supported") return { ok: false, skipped: true, error: T("library.actions.keepsOne", {name}) };
    if (row.error === "undated_not_supported") return { ok: false, skipped: true, error: T("library.actions.needsDate", {name}) };
    if (row.error === "undated_rewatch_not_supported") return { ok: false, skipped: true, error: T("library.actions.alreadyWatched", {name}) };
    if (row.error === "provider_not_allowed") return { ok: false, error: T("library.actions.err.notAllowed", {name}) };
    if (row.error === "unsupported_rating") return { ok: false, error: T("library.actions.err.score", {name}) };
    if (row.error) return { ok: false, error: T("library.actions.err.notUpdated", {name}) };
    const result = row.result || {};
    if (Number(result.confirmed) > 0) return { ok: true };
    if (Number(result.unresolved) > 0) return { ok: false, error: T("library.actions.err.noMatch", {name}) };
    if (Number(result.errors) > 0) return { ok: false, error: T("library.actions.err.returned", {name}) };
    if (Number(result.skipped) > 0) return { ok: true, note: "already there" };
    return row.ok ? { ok: true } : { ok: false, error: T("library.actions.err.notConfirmed", {name}) };
  }

  function queued(job) {
    const run = removeChain.then(job, job);
    removeChain = run.catch(() => {});
    return run;
  }

  async function directRemove(feature, ref, items, name) {
    const data = await request("/api/editor/send", { kind: feature, operation: "remove", direct: true, providers: [cleanRef(ref)], items });
    const row = (Array.isArray(data?.results) ? data.results : [])[0] || {};
    if (row.error === "needs_live_match") return null;
    const result = row.result || {};
    if (row.error === "provider_not_allowed") return { ok: false, error: T("library.actions.err.notAllowed", {name}) };
    if (row.error) return { ok: false, error: T("library.actions.err.removalFailed", {name}) };
    if (Number(result.removed) > 0) return { ok: true };
    if (Number(result.unresolved) > 0) return { ok: false, error: T("library.actions.err.notFound", {name}) };
    if (Number(result.skipped) > 0) return { ok: false, skipped: true, error: T("library.actions.err.cannotRemoveType", {name}) };
    return { ok: false, error: T("library.actions.err.removalNotConfirmed", {name}) };
  }

  function editorRemove(feature, ref, item, name) {
    return queued(async () => {
      const items = [item];
      const direct = await directRemove(feature, ref, items, name);
      if (direct) return direct;
      const live_check = [cleanRef(ref)];
      const preview = await request("/api/editor/send/preview", { kind: feature, items, live_check }, 60000);
      const target = (Array.isArray(preview?.providers) ? preview.providers : []).find((row) => refKey(row) === refKey(ref));
      if (!target) {
        return {
          ok: false,
          error: preview?.empty_reason === "pair_baseline_unavailable"
            ? T("library.actions.runPairsFirst")
            : T("library.actions.err.noRecord", {name, noun: FEATURES[feature]?.noun || feature}),
        };
      }
      const data = await request("/api/editor/send", {
        kind: feature,
        operation: "remove",
        confirmed: true,
        preview_id: preview.preview_id,
        providers: [cleanRef(ref)],
        items,
        live_check,
      });
      const row = (Array.isArray(data?.results) ? data.results : [])[0] || {};
      const result = row.result || {};
      if (row.error) return { ok: false, error: T("library.actions.err.removalFailed", {name}) };
      if (Number(result.removed) > 0) return { ok: true };
      if (Number(result.unresolved) > 0) return { ok: false, error: T("library.actions.err.stillReports", {name}) };
      if (Number(result.skipped) > 0) return { ok: true, note: "already gone" };
      return { ok: false, error: T("library.actions.err.removalNotConfirmed", {name}) };
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
    if (!key) return { ok: false, error: T("library.actions.noWatchlistKey") };
    const data = await request("/api/watchlist/delete", {
      provider: ref.provider,
      provider_instance: ref.instance,
      keys: [aliases.length ? { key, aliases } : { key }],
    });
    if (Number(data?.deleted_ok) > 0) return { ok: true };
    const reason = data?.results?.[0]?.per_key?.[0]?.reason;
    if (reason === "not_in_state") return { ok: false, error: T("library.actions.err.noWatchlistRecord", {name}) };
    return { ok: false, error: data?.error ? T("library.actions.err.removalFailed", {name}) : T("library.actions.err.removalNotConfirmed", {name}) };
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
      if (!record) return { ok: false, error: T("library.actions.err.noResume", {name}) };
      const data = await request(url, { provider: record.provider, instance_id: record.instance_id, remote_id: record.remote_id, canonical_key: record.canonical_key, record });
      return data?.ok ? { ok: true } : { ok: false, error: data?.message || T("library.actions.err.notConfirmed", {name}) };
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
        actions.retry ? `<button type="button" data-ia-toast="retry">${esc(actions.retryLabel || T("library.actions.retry"))}</button>` : "",
        actions.undo ? `<button type="button" data-ia-toast="undo">${icon("undo")}${TH("library.actions.undo")}</button>` : "",
        tone !== "work" ? `<button type="button" class="cw-ia-toast-close" data-ia-toast="close" aria-label="${TH("library.actions.dismiss")}">${icon("close")}</button>` : "",
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
        result = { ok: false, error: e?.message || T("library.actions.requestFailedShort") };
      }
      done += 1;
      (result.ok ? ok : failed).push({ task, result });
      mark(job, task, result.ok ? "ok" : "fail", result.error || "");
      if (done < total && toast.current === id) progress();
    }));
    const names = (rows) => listNames(rows.map((row) => row.task.name));
    let text;
    if (!failed.length) text = job.done(ok.map((row) => row.task));
    else if (!ok.length) text = failed.length === 1 ? failed[0].result.error : T("library.actions.failedOn", {names: names(failed)});
    else text = T("library.actions.partial", {done: String(ok.length), total: String(total), names: names(failed)});
    const actions = {};
    if (failed.length && failed.some((row) => !row.result.skipped)) {
      const retry = failed.filter((row) => !row.result.skipped).map((row) => row.task);
      actions.retry = () => void run({ ...job, tasks: retry });
      actions.retryLabel = retry.length === 1 ? T("library.actions.retryName", {name: retry[0].name}) : T("library.actions.retry");
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
    const verbKey = feature === "ratings" ? "ratingOn" : feature === "history" ? "addingWatch" : "addingTo";
    return {
      feature,
      item,
      scope: ctx.scope,
      onChanged: ctx.onChanged,
      tasks,
      working: T(`library.actions.job.${verbKey}`, {target: count(tasks, (n) => T("library.actions.count.provider", {count: n}))}),
      done: options.done || ((okTasks) => T(`library.actions.job.${feature === "ratings" ? "ratedOn" : feature === "history" ? "watchAdded" : "addedTo"}`, {target: count(okTasks, (n) => T("library.actions.count.provider", {count: n}))})),
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
      working: T("library.actions.job.removingFrom", {target: count(tasks, (n) => T("library.actions.count.provider", {count: n}))}),
      done: options.done || ((okTasks) => T("library.actions.job.removedFrom", {target: count(okTasks, (n) => T("library.actions.count.provider", {count: n}))})),
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
    return Number.isFinite(value) && value >= 0.5 && value <= 10 ? value : null;
  }

  const REASONS = {
    read_only: T("library.actions.reason.readOnly"),
    anime_only: T("library.actions.reason.animeOnly"),
    type_not_supported: T("library.actions.reason.type"),
    provider_unavailable: T("library.actions.reason.unavailable"),
  };
  const accountName = (ref, list = []) => {
    const row = list.find((entry) => refKey(entry) === refKey(ref));
    if (row?.instance_label) return row.instance_label;
    return !ref.instance || ref.instance === "default" ? T("library.actions.default") : ref.instance;
  };
  const logoHtml = (provider, cls = "cw-ia-logo") => {
    const logo = providerLogo(provider);
    return `<span class="${cls}">${logo ? `<img src="${esc(logo)}" alt="">` : `<b>${esc(providerName(provider).slice(0, 2))}</b>`}</span>`;
  };

  function targetQuery(item) {
    const type = String(item?.type || "").toLowerCase();
    const ids = identity(item).ids || {};
    const anime = type === "anime" || ["anilist", "mal", "kitsu"].some((key) => ids[key] != null && ids[key] !== "");
    const page = document.documentElement?.dataset?.cwPage;
    const profile = window.CW?.ProfileViewAs?.id || (page === "profile" ? "" : window.CW?.OverviewProfile?.id) || "";
    return { media_type: type === "tv" ? "show" : type, anime, user_profile: profile };
  }

  function targets(feature, query = {}) {
    const params = new URLSearchParams({ kind: feature });
    if (query.media_type) params.set("media_type", query.media_type);
    if (query.anime) params.set("anime", "true");
    if (query.user_profile) params.set("user_profile", query.user_profile);
    const key = params.toString();
    if (!targetCache.has(key)) {
      const pending = request(`/api/editor/send/item-targets?${key}`)
        .then((data) => ({
          rows: (Array.isArray(data?.providers) ? data.providers : []).map((row) => ({ ...row, ...cleanRef(row) })),
          hidden: (Array.isArray(data?.hidden) ? data.hidden : []).map((row) => ({ ...row, ...cleanRef(row) })),
          others: (Array.isArray(data?.others) ? data.others : []).map((row) => ({ ...row, ...cleanRef(row) })),
          hasPair: data?.has_pair !== false,
        }))
        .catch(() => {
          targetCache.delete(key);
          return { rows: [], hidden: [], others: [], hasPair: true };
        });
      targetCache.set(key, pending);
    }
    return targetCache.get(key);
  }

  const menu = (() => {
    let el = null;
    let ctx = null;
    let list = [];
    let hidden = [];
    let others = [];
    let lists = {};
    let armedUntil = 0;
    let armedRow = "";
    let armTimer = 0;
    let score = 0;
    let lastFocus = null;
    let when = "now";
    let customDate = "";

    const isNarrow = () => !!window.matchMedia?.("(max-width: 560px)")?.matches;
    const writable = (ref) => list.some((row) => refKey(row) === refKey(ref));
    const isOther = (ref) => others.some((row) => refKey(row) === refKey(ref));
    const missingRefs = () => (ctx.refs.missing || []).map(cleanRef).filter((ref) => writable(ref) && !isOther(ref));
    const pairNote = () => T("library.actions.notInPair", {noun: FEATURES[ctx.feature].noun});
    const hiddenReason = (ref) => {
      const row = hidden.find((entry) => refKey(entry) === refKey(ref));
      return row ? REASONS[row.reason] || T("library.actions.cannotChange") : "";
    };
    const progressRows = () => (Array.isArray(ctx?.progressRows) ? ctx.progressRows : []);
    const focusRow = () => progressRows().find((row) => row.rid === ctx?.focusRow) || null;
    const rowAllows = (row, kind) => !!(kind === "mark_watched" ? row?.record?.can_mark_watched : row?.record?.can_remove_progress);
    const groupRefs = (group) => (group.present || []).map(cleanRef).filter((ref) => (lists[group.feature] || []).some((row) => refKey(row) === refKey(ref)));
    const activeGroups = () => (ctx?.groups || []).filter((group) => !ctx.off.has(group.feature) && groupRefs(group).length);
    const isArmed = (key) => Date.now() < armedUntil && armedRow === key;

    function ensure() {
      if (el) return el;
      el = document.createElement("div");
      el.className = "cw-ia-pop cw-page-item-actions";
      el.setAttribute("role", "dialog");
      el.hidden = true;
      document.body.appendChild(el);
      el.addEventListener("click", onClick);
      el.addEventListener("pointerover", event => {
        const star = event.target.closest("[data-ia-score]");
        if (star && !star.disabled) previewScore(Number(star.dataset.iaScore));
      });
      el.addEventListener("pointerout", event => {
        if (event.target.closest(".cw-ia-rating-choices") && !event.relatedTarget?.closest?.(".cw-ia-rating-choices")) previewScore(score);
      });
      el.addEventListener("focusin", event => {
        const star = event.target.closest("[data-ia-score]");
        if (star && !star.disabled) previewScore(Number(star.dataset.iaScore));
      });
      el.addEventListener("focusout", event => {
        if (event.target.closest(".cw-ia-rating-choices") && !event.relatedTarget?.closest?.(".cw-ia-rating-choices")) previewScore(score);
      });
      el.addEventListener("keydown", event => {
        const star = event.target.closest("[data-ia-score]");
        if (!star) return;
        const values = scoreScale().values;
        const index = values.indexOf(Number(star.dataset.iaScore));
        const next = event.key === "ArrowRight" || event.key === "ArrowUp" ? values[Math.min(values.length - 1, index + 1)]
          : event.key === "ArrowLeft" || event.key === "ArrowDown" ? values[Math.max(0, index - 1)]
          : event.key === "Home" ? values[0] : event.key === "End" ? values.at(-1) : null;
        if (next === null) return;
        event.preventDefault();
        el.querySelector(`[data-ia-score="${next}"]`)?.focus();
      });
      el.addEventListener("change", (event) => {
        if (!event.target?.matches?.("[data-ia-act=date]")) return;
        customDate = String(event.target.value || "");
        render();
      });
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
      el.classList.toggle("is-sheet", isNarrow());
      if (isNarrow()) {
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

    function arm(key) {
      armedRow = key;
      armedUntil = Date.now() + ARM_MS;
      window.clearTimeout(armTimer);
      armTimer = window.setTimeout(() => {
        armedUntil = 0;
        armedRow = "";
        render();
      }, ARM_MS);
      render();
    }

    const item = (action, symbol, label, tone = "", extra = "") => `<button type="button" class="cw-ia-item${tone ? ` is-${tone}` : ""}" data-ia-act="${action}"${extra}>${icon(symbol)}<span>${esc(label)}</span></button>`;
    const section = (label) => `<div class="cw-ia-section">${esc(label)}</div>`;
    function scoreScale() {
      const empty = { values: [], max: 10, display_factor: 1 };
      const refs = ctx?.focus ? [ctx.focus] : missingRefs();
      const scales = refs.map(scaleFor);
      if (scales.some(scale => scale.unavailable)) return {...empty, unavailable: true};
      const first = scales[0] || empty;
      const display = scale => JSON.stringify([scale.max, scale.display_factor, scale.labels]);
      const shared = scales.every(scale => display(scale) === display(first));
      return {...(shared ? first : empty), values: first.values.filter(value => scales.every(scale => scale.values.includes(value)))};
    }

    function scaleFor(ref) {
      const scale = list.find(row => refKey(row) === refKey(ref))?.rating_scale;
      return Array.isArray(scale?.values) ? scale : {values:[],max:10,display_factor:1,unavailable:true};
    }

    function projectedScore(value, scale) {
      if (value == null) return null;
      if (scale.bands) return scale.bands.find(band => value >= band.min && value < band.max)?.value ?? null;
      if (scale.round_step) return Math.max(scale.values[0], Math.min(scale.values.at(-1), Math.floor(value / scale.round_step + 0.5) * scale.round_step));
      return value;
    }

    function selectableScore(value, scale) {
      if (!scale.values.length || value == null) return null;
      const projected = projectedScore(value, scale) ?? value;
      return scale.values.includes(projected) ? projected : scale.values.reduce((best, choice) => {
        const distance = Math.abs(choice - projected) - Math.abs(best - projected);
        return distance < 0 || (distance === 0 && choice > best) ? choice : best;
      });
    }

    function scoreText(value, scale = scoreScale()) {
      const projected = projectedScore(value, scale);
      return projected == null ? T("library.actions.notRated") : scale.labels?.[projected] || `${projected * scale.display_factor}/${scale.max}`;
    }

    function ratingStars() {
      const scale = scoreScale();
      if (scale.unavailable) return '<p class="cw-ia-note">' + TH("library.actions.ratingUnavailable") + '</p>';
      const half = scale.values.some(value => !Number.isInteger(value * scale.display_factor));
      const label = value => value == null ? T("library.actions.notRated") : scale.labels?.[value] || `${value * scale.display_factor}`;
      const slots = half && !scale.labels ? Array.from({length: 20}, (_, i) => (i + 1) / 2) : scale.values;
      const buttons = slots.map(value => `<button type="button" class="${scale.labels ? "cw-ia-seg" : "cw-ia-star"}${half && Number.isInteger(value) ? " is-right" : ""}${(scale.labels ? value === score : value <= score) ? " is-lit" : ""}${value === score ? " is-on" : ""}" data-ia-act="score" data-ia-score="${value}"${scale.values.includes(value) ? "" : " disabled"} aria-label="${scale.labels ? esc(label(value)) : TH("library.actions.rateOutOf", {value: String(label(value)), max: String(scale.max)})}" aria-pressed="${value === score}">${scale.labels ? esc(label(value)) : icon("star")}</button>`).join("");
      return `<div class="cw-ia-score"><div class="cw-ia-score-summary"><span>${TH("library.actions.yourScore")}</span><strong><b data-ia-score-value>${esc(label(score))}</b>${scale.labels || score == null ? "" : `<small>/${scale.max}</small>`}</strong></div><div class="cw-ia-rating-choices ${scale.labels ? "cw-ia-segs" : `cw-ia-stars${half ? "" : " is-whole"}`}" style="--ia-score-count:${scale.values.length}" role="group" aria-label="${TH("library.actions.yourRating")}">${buttons}</div>${scale.values.length ? "" : '<p class="cw-ia-note">' + TH("library.actions.noShared") + '</p>'}</div>`;
    }

    function previewScore(value) {
      const scale = scoreScale();
      el.querySelectorAll("[data-ia-score]").forEach(button => {
        button.classList.toggle("is-lit", scale.labels ? Number(button.dataset.iaScore) === value : Number(button.dataset.iaScore) <= value);
        if (scale.labels) button.classList.toggle("is-on", Number(button.dataset.iaScore) === value);
      });
      const label = el.querySelector("[data-ia-score-value]");
      if (label) label.textContent = value == null ? T("library.actions.notRated") : scale.labels?.[value] || String(value * scale.display_factor);
    }

    function head(symbol, title, sub, logoProvider = "") {
      const mark = logoProvider ? logoHtml(logoProvider) : `<span class="cw-ia-logo is-icon">${icon(symbol)}</span>`;
      return `<div class="cw-ia-head">${mark}<span class="cw-ia-head-copy"><strong>${esc(title)}</strong>${sub ? `<small>${esc(sub)}</small>` : ""}</span></div>`;
    }

    function links() {
      const out = [];
      if (ctx.feature === "history" && typeof ctx.openHistory === "function") out.push(item("history", "history", T("library.actions.historyRewatches"), "accent"));
      if (typeof ctx.openDetails === "function") out.push(item("details", "dashboard", T("library.actions.openDetails"), "accent"));
      return out.length ? `<div class="cw-ia-list">${out.join("")}</div>` : "";
    }

    function addLabel(target) {
      if (ctx.feature === "ratings") return T("library.actions.rateOn", {score: String(scoreText(score)), target});
      if (ctx.feature === "history") return when === "copy" ? T("library.actions.copyWatch", {date: fmtDate(ctx.epoch), target}) : T("library.actions.markWatchedOn", {target});
      if (ctx.feature === "collection") return T("library.actions.addCollection", {target});
      return T("library.actions.addWatchlist", {target});
    }

    function removeLabel(target) {
      if (ctx.feature === "ratings") return T("library.actions.removeRating", {target});
      if (ctx.feature === "history") return T("library.actions.removeAllFrom", {target});
      if (ctx.feature === "collection") return T("library.actions.removeCollection", {target});
      return T("library.actions.removeWatchlist", {target});
    }

    const dateReady = () => ctx.feature !== "history" || when !== "custom" || /^\d{4}-\d{2}-\d{2}$/.test(customDate);

    function whenHtml() {
      if (ctx.feature !== "history") return "";
      const options = [...(ctx.epoch ? [["copy", T("library.actions.sameDate"), T("library.actions.sameDateHint", {date: fmtDate(ctx.epoch)})]] : []), ["now", T("library.actions.now"), T("library.actions.watchedNow")],
        ...(ctx.releaseDate ? [["release", T("library.actions.release"), T("library.actions.onReleaseDate", {date: ctx.releaseDate})]] : []), ["custom", T("library.actions.pickDate"), T("library.actions.pickWatchDate")]];
      const segs = options.map(([value, label, title]) => `<button type="button" role="radio" aria-checked="${when === value}" class="cw-ia-seg${when === value ? " is-on" : ""}" data-ia-act="when" data-ia-when="${value}" title="${esc(title)}">${esc(label)}</button>`).join("");
      const date = when === "custom" ? `<label class="cw-ia-date"><span>${TH("library.actions.watchedOn")}</span><input type="date" data-ia-act="date" value="${esc(customDate)}" max="${esc(new Date().toISOString().slice(0, 10))}"></label>` : "";
      return `<div class="cw-ia-when"><div class="cw-ia-segs" role="radiogroup" aria-label="${TH("library.actions.whenWatched")}">${segs}</div>${date}</div>`;
    }

    function focusHtml() {
      const { feature, focus, refs } = ctx;
      const present = (refs.present || []).some((ref) => refKey(ref) === refKey(focus));
      const name = providerName(focus.provider);
      const rating = present && feature === "ratings" ? ratingOf(ctx, focus) : null;
      const sub = feature === "ratings" ? "" : present ? FEATURES[feature].present : FEATURES[feature].missing;
      const top = head("", `${name} · ${accountName(focus, list)}`, sub, focus.provider);
      const body = [];
      const unpaired = list.some((row) => refKey(row) === refKey(focus) && row.in_pair === false);
      if (!writable(focus)) {
        body.push(`<p class="cw-ia-note">${icon("lock")}${esc(hiddenReason(focus) || T("library.actions.notYours"))}</p>`);
      } else if (present) {
        if (feature === "ratings") {
          body.push(ratingStars());
        }
        const armed = isArmed("one");
        body.push(item("remove-one", armed ? "warning" : "delete", armed ? T("library.actions.clickRemoveAll") : removeLabel(name), "danger"));
      } else {
        if (feature === "ratings") body.push(ratingStars());
        body.push(whenHtml());
        if (feature !== "ratings") body.push(item("add-one", "add", addLabel(name), "accent", dateReady() ? "" : " disabled"));
      }
      if (unpaired) body.push(`<p class="cw-ia-note">${icon("info")}${esc(pairNote())}</p>`);
      return `${top}<div class="cw-ia-list">${body.join("")}</div>${links()}`;
    }

    function coverageHtml() {
      const { feature, refs } = ctx;
      const present = (refs.present || []).map((ref) => ({ ...ref, ...cleanRef(ref) }));
      const missing = missingRefs();
      const total = present.length + missing.length;
      const title = !missing.length ? (feature === "history" ? T("library.actions.watchedEvery") : T("library.actions.onEvery")) : T("library.actions.onOf", {present: String(present.length), total: String(total)});
      const sub = missing.length ? T("library.actions.missingOn", {names: listNames(missing.map((ref) => providerName(ref.provider)))}) : FEATURES[feature].present;
      const rows = [...present.map((ref) => ({ ref, on: true })), ...missing.map((ref) => ({ ref, on: false }))].map(({ ref, on }) => {
        const key = refKey(ref);
        const canWrite = writable(ref);
        const rating = on && feature === "ratings" ? ratingOf(ctx, ref) : null;
        const pill = on ? `<span class="cw-ia-pill is-on">${esc(rating != null ? scoreText(rating, scaleFor(ref)) : T("library.actions.on"))}</span>` : `<span class="cw-ia-pill is-off">${TH("library.actions.missingPill")}</span>`;
        const armed = isArmed(`row:${key}`);
        const action = !canWrite
          ? `<span class="cw-ia-lock" title="${esc(hiddenReason(ref) || T("library.actions.notYours"))}">${icon("lock")}</span>`
          : on
            ? `<button type="button" class="cw-ia-link is-danger${armed ? " is-armed" : ""}" data-ia-act="row" data-ia-ref="${esc(key)}" data-ia-on="1">${armed ? TH("library.actions.confirm") : T("library.actions.remove")}</button>`
            : `<button type="button" class="cw-ia-link" data-ia-act="${feature === "ratings" && !scoreScale().values.length ? "choose-rating" : "row"}" data-ia-ref="${esc(key)}" data-ia-on="0"${feature === "ratings" && scaleFor(ref).unavailable ? " disabled" : ""}>${feature === "ratings" ? scoreScale().values.length ? TH("library.actions.rateValue", {score: String(scoreText(score, scaleFor(ref)))}) : T("library.actions.chooseRating") : T("library.actions.add")}</button>`;
        return `<div class="cw-ia-row${on ? " is-on" : ""}">${logoHtml(ref.provider)}<span class="cw-ia-row-copy"><strong>${esc(providerName(ref.provider))}</strong><small>${esc(accountName(ref, list))}</small></span>${pill}${action}</div>`;
      }).join("");
      const removable = present.filter(writable);
      const foot = [];
      if (missing.length && (feature !== "ratings" || scoreScale().values.length)) foot.push(`<button type="button" class="cw-ia-btn is-primary" data-ia-act="add-all"${dateReady() && (feature !== "ratings" || scoreScale().values.includes(score)) ? "" : " disabled"}>${esc(missing.length === 1 ? addLabel(providerName(missing[0].provider)).replace(/ (watchlist|collection)$/, "") : T("library.actions.addMissing", {count: missing.length}))}</button>`);
      if (removable.length > 1) {
        const armed = isArmed("all");
        foot.push(`<button type="button" class="cw-ia-btn is-danger${armed ? " is-armed" : ""}" data-ia-act="remove-all">${esc(armed ? T("library.actions.clickConfirm") : T("library.actions.removeFromAll"))}</button>`);
      }
      return `${head(FEATURE_ICONS[feature], title, sub)}${feature === "ratings" && missing.length ? `<div class="cw-ia-list">${ratingStars()}</div>` : ""}${missing.length ? whenHtml() : ""}${section(T("library.actions.yourAccounts"))}<div class="cw-ia-rows">${rows || `<p class="cw-ia-note">${icon("info")}${TH("library.actions.noneCanHold")}</p>`}</div>${links()}${foot.length ? `<div class="cw-ia-foot">${foot.join("")}</div>` : ""}`;
    }

    function progressHtml() {
      const row = focusRow();
      if (ctx.focusRow != null) {
        if (!row) return `${head("resume", T("library.actions.resumePoint"), T("library.actions.resumeGone"))}`;
        const body = [];
        if (rowAllows(row, "mark_watched")) body.push(item("pg-watched-one", "check_circle", T("library.actions.markWatched"), "accent"));
        if (rowAllows(row, "remove")) body.push(item("pg-clear-one", "delete", T("library.actions.removeResume"), "danger"));
        if (!body.length) body.push(`<p class="cw-ia-note">${icon("lock")}${TH("library.actions.noChanges", {name: row.provider_label})}</p>`);
        return `${head("", row.label, `${Math.round(Number(row.record?.progress_percent) || 0)}% watched`, row.provider)}<div class="cw-ia-list">${body.join("")}</div>`;
      }
      const rows = progressRows();
      const list2 = rows.map((entry) => {
        const pct = Math.round(Number(entry.record?.progress_percent) || 0);
        const actions = [
          rowAllows(entry, "mark_watched") ? `<button type="button" class="cw-ia-link" data-ia-act="pg-watched-row" data-ia-rid="${esc(entry.rid)}">${TH("library.actions.watched")}</button>` : "",
          rowAllows(entry, "remove") ? `<button type="button" class="cw-ia-link is-danger" data-ia-act="pg-clear-row" data-ia-rid="${esc(entry.rid)}">${TH("library.actions.remove")}</button>` : "",
        ].join("");
        return `<div class="cw-ia-row is-on">${logoHtml(entry.provider)}<span class="cw-ia-row-copy"><strong>${esc(entry.label)}</strong><small>${esc(`${pct}% watched`)}</small></span>${actions || `<span class="cw-ia-lock">${icon("lock")}</span>`}</div>`;
      }).join("");
      const clearable = rows.filter((entry) => rowAllows(entry, "remove"));
      const watchable = rows.filter((entry) => rowAllows(entry, "mark_watched"));
      const foot = [];
      if (watchable.length > 1) foot.push(`<button type="button" class="cw-ia-btn is-primary" data-ia-act="pg-watched-all">${TH("library.actions.markAllWatched")}</button>`);
      if (clearable.length > 1) {
        const armed = isArmed("all");
        foot.push(`<button type="button" class="cw-ia-btn is-danger${armed ? " is-armed" : ""}" data-ia-act="pg-clear-all">${esc(armed ? T("library.actions.clickConfirm") : T("library.actions.removeAll"))}</button>`);
      }
      return `${head("resume", plural(rows.length, "resume point"), listNames([...new Set(rows.map((entry) => entry.provider_label))]))}${section(T("library.actions.resumePoints"))}<div class="cw-ia-rows">${list2}</div>${foot.length ? `<div class="cw-ia-foot">${foot.join("")}</div>` : ""}`;
    }

    function everythingHtml() {
      const rows = ctx.groups.map((group) => {
        const refs = groupRefs(group);
        const on = refs.length > 0 && !ctx.off.has(group.feature);
        const note = refs.length ? listNames(refs.map((ref) => providerName(ref.provider))) : T("library.actions.nothingToRemove");
        return `<label class="cw-ia-row cw-ia-check${refs.length ? "" : " is-empty"}">
          <input type="checkbox" data-ia-act="ev-toggle" data-ia-feature="${esc(group.feature)}"${on ? " checked" : ""}${refs.length ? "" : " disabled"}>
          <span class="cw-ia-logo is-icon">${icon(FEATURE_ICONS[group.feature] || "check")}</span>
          <span class="cw-ia-row-copy"><strong>${esc(FEATURE_LABELS[group.feature] || group.feature)}</strong><small>${esc(note)}</small></span>
          <span class="cw-ia-pill">${esc(refs.length)}</span>
        </label>`;
      }).join("");
      const total = activeGroups().reduce((sum, group) => sum + groupRefs(group).length, 0);
      const armed = isArmed("all");
      return `${head("delete_sweep", T("library.actions.removeEverything"), ctx.item?.title || T("library.actions.thisTitle"))}<div class="cw-ia-rows">${rows}</div><p class="cw-ia-note">${icon("info")}${TH("library.actions.clearsEvery")}</p><div class="cw-ia-foot"><button type="button" class="cw-ia-btn is-danger${armed ? " is-armed" : ""}" data-ia-act="ev-remove"${total ? "" : " disabled"}>${esc(!total ? T("library.actions.nothingSelected") : armed ? T("library.actions.clickConfirm") : T("library.actions.removeRecords", {what: plural(total, "record")}))}</button></div>`;
    }

    function render() {
      if (!ctx || !el) return;
      el.setAttribute("aria-label", ctx.mode === "everything" ? T("library.actions.removeEverything") : `${ctx.item?.title || T("library.actions.title")} ${FEATURES[ctx.feature]?.noun || ""}`);
      if (list.loading) {
        el.innerHTML = `${head("progress_activity", T("library.actions.checkingAccounts"), ctx.item?.title || "")}`;
      } else if (ctx.mode === "everything") {
        el.innerHTML = everythingHtml();
      } else if (ctx.feature === "progress") {
        el.innerHTML = progressHtml();
      } else {
        el.innerHTML = ctx.focus ? focusHtml() : coverageHtml();
      }
      place();
    }

    function runProgress(current, kind, rows) {
      close(true);
      const tasks = rows.map((row) => ({ ...progressTask(kind, row, row.record, row.label), play: row.rid }));
      const many = (items) => (items.length === 1 ? items[0].name : plural(items.length, "resume point"));
      void run({
        feature: "progress",
        item: current.item,
        scope: current.scope,
        onChanged: current.onChanged,
        tasks,
        working: T(kind === "mark_watched" ? "library.actions.job.markingWatched" : "library.actions.job.removing", {target: many(tasks)}),
        done: (okTasks) => T(kind === "mark_watched" ? "library.actions.job.markedWatched" : "library.actions.job.removed", {target: many(okTasks)}),
      });
    }

    function onClick(event) {
      const button = event.target?.closest?.("[data-ia-act]");
      if (!button || button.disabled || !ctx) return;
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
        if (!isArmed("all")) return arm("all");
        const tasks = activeGroups().flatMap((group) => groupRefs(group).map((ref) => removeTask(group.feature, ref, group.item, `${providerName(ref.provider)} ${FEATURES[group.feature].noun}`, { scope: group.scope })));
        close(true);
        void run({ feature: "everything", item: current.item, scope: null, onChanged: current.onChanged, tasks, working: T("library.actions.job.removing", {target: plural(tasks.length, "record")}), done: (okTasks) => T("library.actions.job.removed", {target: plural(okTasks.length, "record")}) });
        return;
      }
      if (action.startsWith("pg-")) {
        const kind = action.includes("watched") ? "mark_watched" : "remove";
        if (action.endsWith("-one")) return runProgress(current, kind, [focusRow()].filter(Boolean));
        if (action.endsWith("-row")) return runProgress(current, kind, progressRows().filter((row) => row.rid === button.dataset.iaRid));
        const rows = progressRows().filter((row) => rowAllows(row, kind));
        if (kind === "remove" && !isArmed("all")) return arm("all");
        return runProgress(current, kind, rows);
      }
      if (action === "when") {
        when = button.dataset.iaWhen || "now";
        if (when === "custom" && !customDate) customDate = new Date().toISOString().slice(0, 10);
        render();
        return;
      }
      if (action === "score") {
        if (!scoreScale().values.includes(Number(button.dataset.iaScore))) return;
        score = Number(button.dataset.iaScore);
        if (current.focus && writable(current.focus)) {
          const present = (current.refs.present || []).some(ref => refKey(ref) === refKey(current.focus));
          const unchanged = present && score === projectedScore(ratingOf(current, current.focus), scoreScale());
          close(true);
          if (!unchanged) void run(addJob(current, [current.focus], list, { fields: { rating: score }, noUndo: true }));
          return;
        }
        render();
        el.querySelector(`[data-ia-score="${score}"]`)?.focus({ preventScroll: true });
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
      const addOptions = () => {
        if (current.feature === "ratings") return { fields: { rating: score }, noUndo: true };
        if (current.feature === "history") {
          const watched = when === "copy" ? isoFromEpoch(current.epoch) : when === "release" ? noonIso(current.releaseDate) : when === "custom" ? noonIso(customDate) : new Date().toISOString().replace(/\.\d{3}Z$/, "Z");
          return { fields: { watched_at: watched } };
        }
        if (current.feature === "collection") return { fields: { collected_at: new Date().toISOString() } };
        return {};
      };
      const all = [...(current.refs.present || []), ...(current.refs.missing || [])].map(cleanRef);
      if (action === "choose-rating") {
        const ref = all.find(entry => refKey(entry) === button.dataset.iaRef);
        if (!ref || !writable(ref)) return;
        ctx.focus = ref;
        const scale = scoreScale();
        score = selectableScore(score ?? ratingOf(ctx, ref) ?? 8, scale);
        render();
        el.querySelector(`[data-ia-score="${score}"]`)?.focus();
        return;
      }
      if (action === "row") {
        const ref = all.find((entry) => refKey(entry) === button.dataset.iaRef);
        if (!ref || !writable(ref)) return;
        if (button.dataset.iaOn === "1") {
          if (current.feature === "history" && !isArmed(`row:${refKey(ref)}`)) return arm(`row:${refKey(ref)}`);
          close(true);
          void run(removeJob(current, [ref], list));
        } else {
          if (current.feature === "ratings" && !scoreScale().values.includes(score)) return;
          close(true);
          void run(addJob(current, [ref], list, addOptions()));
        }
        return;
      }
      if (action === "add-one") {
        if (current.feature === "ratings" && !scoreScale().values.includes(score)) return;
        close(true);
        void run(addJob(current, [current.focus], list, addOptions()));
        return;
      }
      if (action === "add-all") {
        if (current.feature === "ratings" && !scoreScale().values.includes(score)) return;
        const refs = missingRefs();
        close(true);
        void run(addJob(current, refs, list, addOptions()));
        return;
      }
      if (action === "remove-one") {
        if (current.feature === "history" && !isArmed("one")) return arm("one");
        close(true);
        void run(removeJob(current, [current.focus], list));
        return;
      }
      if (action === "remove-all") {
        if (!isArmed("all")) return arm("all");
        close(true);
        void run(removeJob(current, (current.refs.present || []).map(cleanRef).filter(writable), list));
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
      when = options.feature === "history" && Number(options.epoch) ? "copy" : "now";
      customDate = "";
      armedUntil = 0;
      armedRow = "";
      score = ratingOf(ctx, ctx.focus || {}) || ratingOf({ rating: options.rating }, {}) || 8;
      list = Object.assign([], { loading: true });
      hidden = [];
      others = [];
      lists = {};
      options.anchor?.classList?.add("is-ia-open");
      el.hidden = false;
      render();
      const token = ctx;
      const query = targetQuery(options.item);
      if (options.mode === "everything") {
        const features = (options.groups || []).map((group) => group.feature);
        const loaded = await Promise.all(features.map((feature) => targets(feature, query)));
        if (ctx !== token) return true;
        lists = Object.fromEntries(features.map((feature, index) => [feature, [...loaded[index].rows, ...loaded[index].others]]));
        list = [];
      } else if (options.feature === "progress") {
        list = [];
      } else {
        const result = await targets(options.feature, query);
        if (ctx !== token) return true;
        list = [...result.rows, ...result.others];
        hidden = result.hidden;
        others = result.others;
        const seen = new Set([...ctx.refs.present, ...ctx.refs.missing].map(refKey));
        ctx.refs.missing = [...ctx.refs.missing, ...result.rows.filter((row) => !seen.has(refKey(row))).map(cleanRef)];
      }
      if (ctx.feature === "ratings") {
        const scale = scoreScale();
        const saved = ctx.focus ? ratingOf(ctx, ctx.focus) : null;
        score = selectableScore(saved ?? score, scale);
      }
      render();
      (el.querySelector(`[data-ia-score="${score}"]`) || el.querySelector("button,input"))?.focus({ preventScroll: true });
      return true;
    }

    return { open, close };
  })();

  const sheet = (() => {
    let root = null;
    let ctx = null;
    let data = null;
    let list = [];
    let hiddenRows = [];
    let when = "now";
    let customDate = "";
    let picked = new Set();
    let armed = { key: "", until: 0 };
    let armTimer = 0;
    let lastFocus = null;
    let loadSeq = 0;
    const playNotes = new Map();
    let mobileLayer = null;

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
    const keepsOne = (ref) => playNotes.get(refKey(ref)) === true;
    const isArmed = (key) => armed.key === key && Date.now() < armed.until;

    function blockReason(ref) {
      const target = writable(ref);
      if (!target) return T("library.actions.notYours");
      if (when === "release" && !ctx.releaseDate) return T("library.actions.noRelease");
      if (when === "unknown" && !target.undated_write) return T("library.actions.needsWatchDate");
      if (when === "unknown" && holds(ref)) return T("library.actions.alreadyWatchedHere");
      if (holds(ref) && !target.rewatch_write) return T("library.actions.keepsOnePlay");
      return "";
    }

    function clearsAll(play, ref) {
      const entry = (play.present || []).find((row) => refKey(row) === refKey(ref));
      const target = writable(ref);
      return !(entry?.event && target?.rewatch_read && target?.rewatch_write);
    }

    function columns() {
      const map = new Map();
      for (const ref of [...list, ...holders(), ...plays().flatMap((play) => play.present || [])]) {
        const clean = cleanRef(ref);
        if (clean.provider && !map.has(refKey(clean))) map.set(refKey(clean), clean);
      }
      return [...map.values()].sort((a, b) => providerName(a.provider).localeCompare(providerName(b.provider), undefined, { sensitivity: "base" }) || accountName(a, list).localeCompare(accountName(b, list)));
    }

    function close(fromBack = false) {
      if (!fromBack && mobileLayer) return mobileLayer.dismiss();
      mobileLayer = null;
      if (!root || root.hidden) return;
      root.hidden = true;
      loadSeq += 1;
      window.clearTimeout(armTimer);
      document.documentElement.classList.remove("cw-ia-sheet-open");
      if (lastFocus?.isConnected) lastFocus.focus({ preventScroll: true });
      ctx = null;
    }

    function armKey(key) {
      if (isArmed(key)) return false;
      armed = { key, until: Date.now() + ARM_MS };
      window.clearTimeout(armTimer);
      armTimer = window.setTimeout(() => {
        armed = { key: "", until: 0 };
        render();
      }, ARM_MS);
      render();
      return true;
    }

    function chip(ref, play, index) {
      const entry = (play.present || []).find((row) => refKey(row) === refKey(ref));
      const name = `${providerName(ref.provider)} · ${accountName(ref, list)}`;
      const target = writable(ref);
      let title;
      let disabled = false;
      if (entry) {
        title = !target ? T("library.actions.onName", {name}) : clearsAll(play, ref) ? T("library.actions.removeAllFrom", {target: name}) : T("library.actions.removePlayFrom", {name});
        disabled = !target;
      } else if (!target) {
        return "";
      } else if (holds(ref) && !target.rewatch_write) {
        title = T("library.actions.keepsOne", {name});
        disabled = true;
      } else if (!Number(play.epoch) && !target.undated_write) {
        title = T("library.actions.needsDate", {name});
        disabled = true;
      } else if (!Number(play.epoch) && holds(ref)) {
        title = T("library.actions.alreadyWatched", {name});
        disabled = true;
      } else {
        title = holds(ref) && keepsOne(ref) ? T("library.actions.copyReplace", {name}) : T("library.actions.copyTo", {name});
      }
      return `<button type="button" class="cw-ia-chip${entry ? " is-on" : ""}" data-ia-ref="${esc(refKey(ref))}" data-ia-play="${index}" data-ia-sheet="chip" title="${esc(title)}" aria-label="${esc(title)}"${disabled ? " disabled" : ""}>${logoHtml(ref.provider, "cw-ia-chip-logo")}<i aria-hidden="true"></i></button>`;
    }

    function playsHtml(rows, cols) {
      if (!data) return `<p class="cw-ia-empty">${icon("progress_activity")}${TH("library.actions.loadingPlays")}</p>`;
      if (data.error) return `<p class="cw-ia-empty is-bad">${icon("error")}${esc(data.error)}</p>`;
      if (!rows.length) return `<p class="cw-ia-empty">${icon("visibility_off")}${TH("library.actions.notWatchedAny")}</p>`;
      return rows.map((play, index) => {
        const key = `play:${index}`;
        const refs = (play.present || []).map(cleanRef);
        const writableRefs = refs.filter(writable);
        const armedRow = isArmed(key);
        const wipes = writableRefs.filter((ref) => clearsAll(play, ref)).map((ref) => `${providerName(ref.provider)} · ${accountName(ref, list)}`);
        const action = !wipes.length ? T("library.actions.removeEverywhere")
          : wipes.length === writableRefs.length ? T("library.actions.removeAllTitle")
          : T("library.actions.removePlayAndAll", {names: listNames(wipes)});
        const label = armedRow ? T("library.actions.clickAgainTo", {action: `${action[0].toLowerCase()}${action.slice(1)}`}) : action;
        const where = refs.map((ref) => `${providerName(ref.provider)} · ${accountName(ref, list)}`).join(", ");
        const warning = armedRow && wipes.length
          ? `<p class="cw-ia-warn">${icon("warning")}${esc(`${T("library.actions.removesAllFrom", {names: listNames(wipes)})}${wipes.length < writableRefs.length ? ` ${T("library.actions.onlyThisPlay")}` : ""}`)}</p>`
          : "";
        return `<div class="cw-ia-play${armedRow ? " is-armed" : ""}">
          <span class="cw-ia-play-copy"><strong>${esc(fmtWhen(play.epoch))}</strong><small>${esc(where)}</small></span>
          <span class="cw-ia-play-chips">${cols.map((ref) => chip(ref, play, index)).join("")}</span>
          <button type="button" class="cw-ia-icon-btn is-danger${armedRow ? " is-armed" : ""}" data-ia-sheet="remove-play" data-ia-index="${index}" title="${esc(label)}" aria-label="${esc(label)}"${writableRefs.length ? "" : " disabled"}>${icon(armedRow ? "warning" : "delete")}</button>
          ${warning}
        </div>`;
      }).join("");
    }

    function sendToHtml(cols) {
      const own = cols.filter((ref) => writable(ref));
      if (!own.length) return `<p class="cw-ia-empty">${icon("lock")}${TH("library.actions.noneReceive")}</p>`;
      const groups = new Map();
      for (const ref of own) {
        if (!groups.has(ref.provider)) groups.set(ref.provider, []);
        groups.get(ref.provider).push(ref);
      }
      const state = (ref) => {
        const reason = blockReason(ref);
        const gate = writable(ref)?.rewatch_gate === "pro_vip" && holds(ref) ? T("library.actions.proVip") : "";
        return { reason, gate, on: !reason && picked.has(refKey(ref)) };
      };
      const box = (on) => `<span class="cw-ia-acc-box" aria-hidden="true">${on ? icon("check") : ""}</span>`;
      return [...groups.entries()].map(([provider, refs]) => {
        if (refs.length === 1) {
          const ref = refs[0];
          const { reason, gate, on } = state(ref);
          const account = accountName(ref, list);
          const sub = reason || (account === T("library.actions.default") ? "" : account);
          return `<button type="button" class="cw-ia-dest${on ? " is-on" : ""}" data-ia-sheet="pick" data-ia-ref="${esc(refKey(ref))}" aria-pressed="${on}" title="${esc(reason || gate || `${providerName(provider)} · ${account}`)}"${reason ? " disabled" : ""}>${logoHtml(provider)}<span class="cw-ia-dest-copy"><strong>${esc(providerName(provider))}</strong>${sub ? `<small>${esc(sub)}</small>` : ""}</span>${box(on)}</button>`;
        }
        const accounts = refs.map((ref) => {
          const { reason, gate, on } = state(ref);
          return `<button type="button" class="cw-ia-acc${on ? " is-on" : ""}" data-ia-sheet="pick" data-ia-ref="${esc(refKey(ref))}" aria-pressed="${on}" title="${esc(reason || gate || accountName(ref, list))}"${reason ? " disabled" : ""}>${box(on)}${esc(accountName(ref, list))}${reason ? `<small>${esc(reason)}</small>` : ""}</button>`;
        }).join("");
        return `<div class="cw-ia-dest is-multi">${logoHtml(provider)}<span class="cw-ia-dest-copy"><strong>${esc(providerName(provider))}</strong></span><div class="cw-ia-accs">${accounts}</div></div>`;
      }).join("");
    }

    function render() {
      const host = dialog();
      if (!host || !ctx) return;
      const cols = columns();
      const rows = plays();
      const title = ctx.item?.title || T("library.actions.title");
      const loading = !data;
      const undated = list.some((row) => row.undated_write);
      if (when === "unknown" && !undated) when = "now";
      const options = [["now", T("library.actions.now")], ["release", T("library.actions.onRelease")], ["custom", T("library.actions.pickDate")], ...(undated ? [["unknown", T("library.actions.noDate")]] : [])];
      const segs = options.map(([value, label]) => {
        const disabled = value === "release" && !ctx.releaseDate;
        return `<button type="button" role="radio" aria-checked="${when === value}" class="cw-ia-seg${when === value ? " is-on" : ""}" data-ia-sheet="when" data-ia-when="${value}"${disabled ? ' disabled title="' + TH("library.actions.noRelease") + '"' : ""}>${esc(label)}</button>`;
      }).join("");
      const own = cols.filter((ref) => writable(ref));
      const pickable = own.filter((ref) => !blockReason(ref));
      const chosen = pickable.filter((ref) => picked.has(refKey(ref)));
      const anyPlays = rows.length > 0 || holders().length > 0;
      const notes = [];
      for (const ref of chosen) {
        if (holds(ref) && keepsOne(ref)) notes.push(T("library.actions.replacesOlder", {name: providerName(ref.provider)}));
      }
      const leftOut = hiddenRows.map((row) => `${providerName(row.provider)} (${(REASONS[row.reason] || "not available").toLowerCase()})`);
      const dateOk = when !== "custom" || /^\d{4}-\d{2}-\d{2}$/.test(customDate);
      const addLabel = !chosen.length ? T("library.actions.pickWhere") : T(anyPlays ? "library.actions.logRewatchOn" : "library.actions.markWatchedOn", {target: plural(chosen.length, "account")});
      const clearable = holders().some((ref) => writable(ref)) || rows.some((play) => (play.present || []).some(writable));
      const clearArmed = isArmed("clear");
      const allPicked = pickable.length > 0 && pickable.every((ref) => picked.has(refKey(ref)));
      const count = loading ? T("library.actions.loadingLower") : data?.truncated ? T("library.actions.newestShown", {what: plural(rows.length, "play")}) : plural(rows.length, "play");
      host.innerHTML = `<header class="cw-ia-sheet-head">
          <span class="cw-ia-sheet-poster">${ctx.poster ? `<img src="${esc(ctx.poster)}" alt="" onerror="this.remove()">` : ""}</span>
          <span class="cw-ia-sheet-title"><h2 id="cw-ia-sheet-title">${esc(title)}</h2><small>Watch history · ${esc(count)}</small></span>
          <button type="button" class="cw-ia-icon-btn" data-ia-sheet="close" aria-label="${TH("library.actions.close")}">${icon("close")}</button>
        </header>
        <div class="cw-ia-plays${data && !data.error && !rows.length ? " is-empty" : ""}">${playsHtml(rows, cols)}</div>
        <div class="cw-ia-log">
          <div class="cw-ia-section">${TH("library.actions.logPlay")}</div>
          <div class="cw-ia-segs" role="radiogroup" aria-label="${TH("library.actions.whenWatched")}">${segs}</div>
          ${when === "custom" ? `<label class="cw-ia-date"><span>${TH("library.actions.watchedOn")}</span><input type="date" data-ia-sheet="date" value="${esc(customDate)}" max="${esc(new Date().toISOString().slice(0, 10))}"></label>` : ""}
          <div class="cw-ia-section cw-ia-section-row"><span>${TH("library.actions.sendTo")}</span>${pickable.length ? `<button type="button" class="cw-ia-link" data-ia-sheet="pick-all">${allPicked ? T("library.actions.clear") : T("library.actions.selectAll")}</button>` : ""}</div>
          <div class="cw-ia-groups">${loading ? "" : sendToHtml(cols)}</div>
          ${leftOut.length ? `<p class="cw-ia-muted">${esc(`Not listed: ${listNames(leftOut)}.`)}</p>` : ""}
          ${notes.length ? `<p class="cw-ia-hint">${esc(notes.join(" · "))}</p>` : ""}
          <button type="button" class="cw-ia-btn is-primary is-wide" data-ia-sheet="add"${chosen.length && dateOk && !loading ? "" : " disabled"}>${esc(addLabel)}</button>
          ${clearable ? `<button type="button" class="cw-ia-btn is-quiet-danger is-wide${clearArmed ? " is-armed" : ""}" data-ia-sheet="clear">${TH(clearArmed ? "library.actions.clearAllArmed" : "library.actions.clearAll")}</button>` : ""}
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
        picked = new Set([...picked].filter((key) => {
          const ref = columns().find((entry) => refKey(entry) === key);
          return ref && !blockReason(ref);
        }));
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
      if (action === "pick-all") {
        const pickable = columns().filter((ref) => writable(ref) && !blockReason(ref));
        const all = pickable.every((ref) => picked.has(refKey(ref)));
        picked = all ? new Set() : new Set(pickable.map(refKey));
        render();
        return;
      }
      const job = jobCtx();
      const label = (ref) => `${providerName(ref.provider)} · ${accountName(ref, list)}`;
      if (action === "chip") {
        const play = plays()[Number(button.dataset.iaPlay)];
        const [provider, ...instance] = String(button.dataset.iaRef || "").split(":");
        const ref = cleanRef({ provider, instance: instance.join(":") });
        if (!play) return;
        const name = label(ref);
        const present = (play.present || []).some((row) => refKey(row) === refKey(ref));
        const index = button.dataset.iaPlay;
        if (present) {
          const all = clearsAll(play, ref);
          void run({ ...job, tasks: [removeTask("history", ref, playItem(play, ref), name, { play: index })], working: T(all ? "library.actions.job.removingAllFrom" : "library.actions.job.removingPlayFrom", {target: name}), done: () => T(all ? "library.actions.job.allRemovedFrom" : "library.actions.job.playRemovedFrom", {target: name}) });
        } else {
          const fields = Number(play.epoch) ? { watched_at: isoFromEpoch(play.epoch) } : { watched: true, _cw_watched_state: true };
          void run({ ...job, tasks: [addTask("history", ref, ctx.item, name, { fields, rewatch: holds(ref), play: index })], working: T("library.actions.job.copying", {target: name}), done: () => T("library.actions.job.copied", {target: name}) });
        }
        return;
      }
      if (action === "remove-play") {
        const index = button.dataset.iaIndex;
        if (armKey(`play:${index}`)) return;
        armed = { key: "", until: 0 };
        const play = plays()[Number(index)];
        const refs = (play?.present || []).map(cleanRef).filter(writable);
        const removal = refs.every((ref) => clearsAll(play, ref)) ? T("library.actions.what.all")
          : refs.some((ref) => clearsAll(play, ref)) ? T("library.actions.what.selected") : T("library.actions.what.play");
        void run({
          ...job,
          tasks: refs.map((ref) => removeTask("history", ref, playItem(play, ref), label(ref), { play: index })),
          working: T("library.actions.job.removingWhat", {what: removal, target: refs.length === 1 ? label(refs[0]) : T("library.actions.count.account", {count: refs.length})}),
          done: (okTasks) => {
            const all = okTasks.filter((task) => clearsAll(play, task.ref)).map((task) => task.name);
            const single = okTasks.filter((task) => !clearsAll(play, task.ref)).map((task) => task.name);
            return [all.length ? T("library.actions.job.allRemovedFrom", {target: listNames(all)}) : "", single.length ? T("library.actions.job.playRemovedFrom", {target: listNames(single)}) : ""].filter(Boolean).join(". ");
          },
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
          tasks: refs.map((ref) => removeTask("history", ref, titleItem, label(ref))),
          working: T("library.actions.job.removingAllFrom", {target: refs.length === 1 ? label(refs[0]) : T("library.actions.count.account", {count: refs.length})}),
          done: (okTasks) => T("library.actions.job.allRemovedFrom", {target: okTasks.length === 1 ? okTasks[0].name : T("library.actions.count.account", {count: okTasks.length})}),
        });
        return;
      }
      if (action === "add") {
        const fields = when === "unknown"
          ? { watched: true, _cw_watched_state: true }
          : { watched_at: when === "release" ? noonIso(ctx.releaseDate) : when === "custom" ? noonIso(customDate) : new Date().toISOString().replace(/\.\d{3}Z$/, "Z") };
        const refs = columns().filter((ref) => writable(ref) && !blockReason(ref) && picked.has(refKey(ref)));
        const rewatchAny = plays().length > 0 || holders().length > 0;
        picked = new Set();
        void run({
          ...job,
          tasks: refs.map((ref) => addTask("history", ref, ctx.item, label(ref), { fields, rewatch: holds(ref) })),
          working: T(rewatchAny ? "library.actions.job.loggingRewatch" : "library.actions.job.markingOn", {target: refs.length === 1 ? label(refs[0]) : T("library.actions.count.account", {count: refs.length})}),
          done: (okTasks) => T(rewatchAny ? "library.actions.job.rewatchLogged" : "library.actions.job.markedOn", {target: okTasks.length === 1 ? okTasks[0].name : T("library.actions.count.account", {count: okTasks.length})}),
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
      const scope = window.CW?.ProfileViewAs?.scope || String;
      try {
        const [result, found] = await Promise.all([
          request(scope(`/api/profile/title/plays?tmdb=${encodeURIComponent(tmdb)}`), undefined, 60000),
          targets("history", targetQuery(ctx.item)),
        ]);
        if (mine !== loadSeq || !ctx) return;
        data = result;
        list = [...found.rows, ...found.others.filter(holds)];
        hiddenRows = found.hidden;
        void loadNotes(found.rows.filter((row) => row.provider === "TRAKT"), mine);
      } catch (e) {
        if (mine !== loadSeq || !ctx) return;
        data = { plays: [], error: e?.message || T("library.actions.playsFailed") };
      }
      picked = new Set([...picked].filter((key) => list.some((row) => refKey(row) === key)));
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
      hiddenRows = [];
      when = "now";
      customDate = "";
      picked = new Set();
      armed = { key: "", until: 0 };
      playNotes.clear();
      root.hidden = false;
      document.documentElement.classList.add("cw-ia-sheet-open");
      if (!mobileLayer && window.CW?.MobileUI?.mobile()) mobileLayer = window.CW.MobileUI.layer(() => close(true));
      render();
      dialog()?.focus({ preventScroll: true });
      void load();
      return true;
    }

    return { open, close };
  })();

  (window.CW ||= {}).ItemActions = { canWrite, openMenu: menu.open, closeMenu: menu.close, openHistory: sheet.open, targets, targetQuery };
})();
