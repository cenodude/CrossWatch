/* assets/helpers/i18n.js */
/* CrossWatch - UI translation lookup, plural rules, locale formatting and language switching */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const meta = window.__cwLocaleMeta || {};
  const bundles = window.__cwLocales || {};
  const requested = window.__cwLanguage || {};
  const DEFAULT = String(meta.default || "en");
  const COOKIE = String(meta.cookie || "cw_lang");
  const ATTRS = ["title", "placeholder", "aria-label", "alt", "aria-description"];
  const VAR_RE = /\{\{\s*([A-Za-z0-9_]+)\s*\}\}/g;
  const languages = (Array.isArray(meta.languages) && meta.languages.length ? meta.languages : [{ code: DEFAULT, name: "English" }])
    .map((row) => ({ code: String(row.code), name: String(row.name || row.code) }));
  const known = new Set(languages.map((row) => row.code));
  const wanted = String(requested.code || document.documentElement?.getAttribute("lang") || DEFAULT).toLowerCase();
  const failed = new Set(Array.isArray(window.__cwLocaleFailed) ? window.__cwLocaleFailed : []);
  const lang = known.has(wanted) && bundles[wanted] && !failed.has(wanted) ? wanted : DEFAULT;
  const primary = bundles[lang]?.messages || {};
  const fallback = lang === DEFAULT ? {} : bundles[DEFAULT]?.messages || {};
  const missing = new Set();
  const fallbacks = new Set();
  const unsavedChecks = new Set();
  const baseOf = (code) => String(code || "").toLowerCase().split("-")[0];

  const debug = (() => {
    try {
      return new URLSearchParams(location.search).get("i18n") === "debug" || localStorage.getItem("cw.i18n.debug") === "1";
    } catch {
      return false;
    }
  })();

  const locale = (() => {
    const browser = (navigator.languages && navigator.languages.length ? navigator.languages : [navigator.language]).filter(Boolean);
    const regional = browser.find((code) => baseOf(code) === baseOf(lang));
    try {
      return Intl.getCanonicalLocales(regional || lang)[0];
    } catch {
      return lang;
    }
  })();

  const rules = (() => {
    try {
      return new Intl.PluralRules(locale);
    } catch {
      return { select: (n) => (Number(n) === 1 ? "one" : "other") };
    }
  })();
  const fallbackRules = (() => {
    try {
      return new Intl.PluralRules(DEFAULT);
    } catch {
      return rules;
    }
  })();

  function warn(kind, key) {
    const bucket = kind === "missing" ? missing : fallbacks;
    if (bucket.has(key)) return;
    bucket.add(key);
    if (kind === "missing" || debug) {
      try { console.warn(`[i18n] ${kind === "missing" ? "missing key" : `no ${lang} text, using ${DEFAULT}`}: ${key}`); } catch {}
    }
  }

  function pick(table, key, count, pluralRules) {
    if (count !== undefined) {
      const exact = table[`${key}_${pluralRules.select(count)}`];
      if (typeof exact === "string") return exact;
      if (typeof table[`${key}_other`] === "string") return table[`${key}_other`];
    }
    return typeof table[key] === "string" ? table[key] : null;
  }

  function find(key, count) {
    const own = pick(primary, key, count, rules);
    if (own !== null) return own;
    if (lang !== DEFAULT) {
      const english = pick(fallback, key, count, fallbackRules);
      if (english !== null) {
        warn("fallback", key);
        return english;
      }
    }
    warn("missing", key);
    return null;
  }

  function number(value, options) {
    const n = Number(value);
    if (!Number.isFinite(n)) return String(value ?? "");
    try {
      return new Intl.NumberFormat(locale, options).format(n);
    } catch {
      return String(n);
    }
  }

  function toDate(value) {
    if (value instanceof Date) return value;
    if (typeof value === "number") return new Date(value < 1e11 ? value * 1000 : value);
    return new Date(value);
  }

  function date(value, options = { dateStyle: "medium", timeStyle: "short" }) {
    const parsed = toDate(value);
    if (Number.isNaN(parsed.getTime())) return "";
    const zoned = window.CW?.ProfileDateTime?.formatter;
    try {
      return zoned ? zoned(options, locale).format(parsed) : new Intl.DateTimeFormat(locale, options).format(parsed);
    } catch {
      return parsed.toISOString();
    }
  }

  const RELATIVE_UNITS = [["year", 31536e6], ["month", 2592e6], ["week", 6048e5], ["day", 864e5], ["hour", 36e5], ["minute", 6e4], ["second", 1e3]];

  function relative(value, options = {}) {
    const parsed = toDate(value);
    const delta = parsed.getTime() - (options.now ?? Date.now());
    if (Number.isNaN(delta)) return "";
    const abs = Math.abs(delta);
    if (abs < 45e3 && options.numeric !== "always") return t("common.time.justNow");
    const [unit, size] = RELATIVE_UNITS.find(([, span]) => abs >= span) || RELATIVE_UNITS[RELATIVE_UNITS.length - 1];
    try {
      return new Intl.RelativeTimeFormat(locale, { numeric: options.numeric || "auto", style: options.style || "long" }).format(Math.round(delta / size), unit);
    } catch {
      return parsed.toISOString();
    }
  }

  function duration(seconds, options = {}) {
    const total = Math.max(0, Math.round(Number(seconds) || 0));
    const parts = [["day", 86400], ["hour", 3600], ["minute", 60], ["second", 1]];
    const out = [];
    let rest = total;
    for (const [unit, size] of parts) {
      const amount = Math.floor(rest / size);
      rest -= amount * size;
      if (amount && out.length < (options.parts || 2)) out.push(number(amount, { style: "unit", unit, unitDisplay: options.unitDisplay || "short" }));
    }
    return out.length ? out.join(" ") : number(0, { style: "unit", unit: options.zeroUnit || "second", unitDisplay: options.unitDisplay || "short" });
  }

  function list(items, options = { style: "long", type: "conjunction" }) {
    const values = Array.from(items || [], (item) => String(item));
    try {
      return new Intl.ListFormat(locale, options).format(values);
    } catch {
      return values.join(", ");
    }
  }

  function format(text, vars) {
    if (!vars) return text;
    return text.replace(VAR_RE, (match, name) => {
      if (!Object.prototype.hasOwnProperty.call(vars, name)) return match;
      const value = vars[name];
      return typeof value === "number" ? number(value) : String(value ?? "");
    });
  }

  function t(key, vars) {
    const name = String(key || "");
    const count = vars && typeof vars.count === "number" ? vars.count : undefined;
    const text = find(name, count);
    if (text === null) return name;
    return format(text, vars);
  }

  function has(key) {
    return typeof primary[key] === "string" || typeof fallback[key] === "string" || typeof primary[`${key}_other`] === "string" || typeof fallback[`${key}_other`] === "string";
  }

  const ESCAPES = { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" };
  const escape = (value) => String(value ?? "").replace(/[&<>"']/g, (ch) => ESCAPES[ch]);
  const html = (key, vars) => escape(t(key, vars));
  const RICH = /\[\[(\/?)(b|strong|i|em|code|br)\]\]/g;
  const rich = (key, vars) => html(key, vars).replace(RICH, (_, close, tag) => `<${close}${tag}>`);

  function readVars(el) {
    const raw = el.getAttribute("data-i18n-vars");
    if (!raw) return undefined;
    try {
      const parsed = JSON.parse(raw);
      return parsed && typeof parsed === "object" ? parsed : undefined;
    } catch {
      return undefined;
    }
  }

  function apply(root = document) {
    if (!root?.querySelectorAll) return root;
    const nodes = root.matches?.("[data-i18n]") ? [root, ...root.querySelectorAll("[data-i18n]")] : root.querySelectorAll("[data-i18n]");
    nodes.forEach((el) => {
      const key = el.getAttribute("data-i18n");
      if (key && has(key)) el.textContent = t(key, readVars(el));
    });
    ATTRS.forEach((attr) => {
      root.querySelectorAll(`[data-i18n-${attr}]`).forEach((el) => {
        const key = el.getAttribute(`data-i18n-${attr}`);
        if (key && has(key)) el.setAttribute(attr, t(key, readVars(el)));
      });
    });
    return root;
  }

  function writeCookie(code) {
    try {
      const secure = location.protocol === "https:" ? "; Secure" : "";
      document.cookie = `${COOKIE}=${encodeURIComponent(code)}; Path=/; Max-Age=31536000; SameSite=Lax${secure}`;
    } catch {}
    try { localStorage.setItem("cw.ui.language", code); } catch {}
  }

  function hasUnsavedWork() {
    for (const check of unsavedChecks) {
      try {
        if (check()) return true;
      } catch {}
    }
    return false;
  }

  function reloadWhenSafe() {
    if (hasUnsavedWork()) {
      window.dispatchEvent(new CustomEvent("cw-language-pending", { detail: { lang } }));
      return false;
    }
    window.location.reload();
    return true;
  }

  async function setLanguage(code, options = {}) {
    const next = String(code || "").toLowerCase();
    if (!known.has(next)) throw new Error(t("common.language.invalid"));
    const account = options.account ?? !!requested.account;
    if (account) {
      const res = await fetch("/api/profile", {
        method: "POST",
        credentials: "same-origin",
        cache: "no-store",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ preferences: { language: next } }),
      });
      if (!res.ok) throw new Error(t("common.language.saveFailed"));
    } else {
      writeCookie(next);
    }
    if (options.reload !== false && next !== lang) reloadWhenSafe();
    return next;
  }

  let edited = false;
  const TRACKED = "#page-settings, form";
  function tracksEdit(el) {
    if (!el?.closest || el.matches?.("[data-cw-language-select], [type=search]")) return false;
    if (el.closest("[data-cw-autosave], [data-cw-language-select]")) return false;
    return !!el.closest(TRACKED);
  }
  ["input", "change"].forEach((type) => document.addEventListener(type, (event) => {
    if (event.isTrusted && tracksEdit(event.target)) edited = true;
  }, true));
  window.addEventListener("settings-changed", () => { edited = false; });
  unsavedChecks.add(() => edited);

  function statusFor(select) {
    const ids = String(select.getAttribute("aria-describedby") || "").split(/\s+/).filter(Boolean);
    for (const id of ids) {
      const node = document.getElementById(id);
      if (node?.hasAttribute("data-cw-language-status")) return node;
    }
    return select.closest("label, div, section")?.querySelector("[data-cw-language-status]") || null;
  }

  function showStatus(node, text, reload) {
    if (!node) return;
    node.textContent = text;
    if (!reload) return;
    const button = document.createElement("button");
    button.type = "button";
    button.className = "cw-language-reload";
    button.textContent = t("common.language.reloadNow");
    button.addEventListener("click", () => window.location.reload());
    node.append(" ", button);
  }

  function themeSelect(select) {
    try { window.CW?.IconSelect?.enhance?.(select, { className: "cw-plain-select" }); } catch {}
  }

  function wireSelectors(root = document) {
    root.querySelectorAll?.("select[data-cw-language-select]").forEach((select) => {
      if (select.dataset.cwLanguageWired === "1") return;
      select.dataset.cwLanguageWired = "1";
      if (!select.options.length) languages.forEach((row) => select.add(new Option(row.name, row.code)));
      select.value = lang;
      themeSelect(select);
      const status = statusFor(select);
      const idle = status ? status.textContent : "";
      select.addEventListener("change", async () => {
        const next = select.value;
        select.disabled = true;
        showStatus(status, t("common.language.saving"));
        try {
          await setLanguage(next, { reload: false });
          if (next === lang) showStatus(status, idle);
          else if (!reloadWhenSafe()) showStatus(status, t("common.language.reloadPending"), true);
        } catch (error) {
          select.value = lang;
          themeSelect(select);
          showStatus(status, error?.message || t("common.language.saveFailed"));
        } finally {
          select.disabled = false;
        }
      });
    });
  }

  if (lang !== wanted && wanted !== DEFAULT) {
    try { console.warn(`[i18n] ${wanted} catalog unavailable, using ${DEFAULT}`); } catch {}
  }
  const markDocument = () => {
    const root = document.documentElement;
    if (!root) return;
    root.lang = lang;
    root.dataset.cwLang = lang;
  };
  markDocument();

  (window.CW ||= {});
  window.CW.I18n = {
    lang,
    requested: wanted,
    locale,
    defaultLanguage: DEFAULT,
    languages,
    account: !!requested.account,
    debug,
    t,
    has,
    html,
    rich,
    escape,
    apply,
    number,
    date,
    relative,
    duration,
    list,
    plural: (count) => rules.select(Number(count) || 0),
    setLanguage,
    reloadWhenSafe,
    hasUnsavedWork,
    registerUnsavedCheck(fn) { if (typeof fn === "function") unsavedChecks.add(fn); return () => unsavedChecks.delete(fn); },
    report: () => ({ lang, missing: [...missing], fallbacks: [...fallbacks] }),
  };

  window.CW.I18n.wireSelectors = wireSelectors;
  window.__cwI18n = window.CW.I18n;
  if (typeof window.relTimeFromEpoch !== "function") window.relTimeFromEpoch = (seconds) => relative(Number(seconds) * 1000);
  window.T = t;
  window.TH = html;

  const ready = () => {
    markDocument();
    apply(document);
    wireSelectors(document);
  };
  window.addEventListener("load", () => document.querySelectorAll?.("select[data-cw-language-select]").forEach(themeSelect), { once: true });
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", ready, { once: true });
  } else {
    ready();
  }
})();
