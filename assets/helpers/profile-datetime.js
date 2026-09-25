/* assets/helpers/profile-datetime.js */
/* CrossWatch - Profile date and time display */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude */
(function () {
  let preferences = {};
  let resolvedZone = "";
  const cache = new Map();
  const locale = () => navigator.language || window.__CW_LOCALE || undefined;
  const timeZone = () => {
    if (resolvedZone) return resolvedZone;
    const name = preferences.timezone;
    try {
      resolvedZone = new Intl.DateTimeFormat(undefined, name && name !== "auto" ? { timeZone: name } : {}).resolvedOptions().timeZone || "UTC";
    } catch {
      resolvedZone = new Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC";
    }
    return resolvedZone;
  };
  const formatter = (options = {}, language = locale()) => {
    const settings = { timeZone: timeZone(), ...options };
    if (settings.hour || settings.timeStyle) {
      if (preferences.time_format === "12h") settings.hour12 = true;
      if (preferences.time_format === "24h") settings.hourCycle = "h23";
    }
    const key = JSON.stringify([language, settings]);
    if (!cache.has(key)) {
      try {
        cache.set(key, new Intl.DateTimeFormat(language, settings));
      } catch {
        cache.set(key, new Intl.DateTimeFormat(undefined, settings));
      }
    }
    return cache.get(key);
  };
  const dayKey = (epoch) => {
    if (!epoch) return "";
    const parts = formatter({ year: "numeric", month: "2-digit", day: "2-digit" }, "en-US").formatToParts(new Date(epoch * 1000));
    const values = Object.fromEntries(parts.map(({ type, value }) => [type, value]));
    return `${values.year}-${values.month}-${values.day}`;
  };
  window.CW = window.CW || {};
  window.CW.ProfileDateTime = {
    configure(value) { preferences = value || {}; resolvedZone = ""; cache.clear(); },
    timeZone,
    formatter,
    format: (date, options) => formatter(options).format(date),
    dayKey,
    monthKey: (epoch) => dayKey(epoch).slice(0, 7),
  };
})();
