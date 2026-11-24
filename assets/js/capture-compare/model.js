/* CrossWatch - Capture comparison data and record display */
const I18N = () => globalThis.window?.CW?.I18n || globalThis.window?.__cwI18n;
const T = (key, vars) => I18N()?.t?.(key, vars) ?? key;
const TH = (key, vars) => I18N()?.html?.(key, vars) ?? key;
export const esc = (s) => String(s ?? "").replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
function kindOf(r) {
  if (!r || typeof r !== "object") return "unknown";
  const t = String(r.type || r.media_type || r.entity || "").toLowerCase();
  if (t === "episode" || r.episode != null) return "episode";
  if (t === "season" || r.season != null) return "season";
  if (["tv", "show", "shows", "series", "anime"].includes(t)) return "show";
  if (["movie", "movies", "film", "films"].includes(t)) return "movie";
  return t || "unknown";
}

function displayTitle(r) {
  if (!r || typeof r !== "object") return T("snapshots.compare.item");
  const t = String(r.type || "").toLowerCase();
  const series = String(r.series_title || r.show_title || r.series || "");
  const season = r.season != null ? String(r.season).padStart(2, "0") : "";
  const episode = r.episode != null ? String(r.episode).padStart(2, "0") : "";
  if (t === "episode" && series && season && episode) return `${series} - S${season}E${episode}`;
  if (t === "season" && series && season) return `${series} - S${season}`;
  return `${String(r.title || series || kindOf(r))}${r.year ? ` (${r.year})` : ""}`;
}

function displaySub(r) {
  if (!r || typeof r !== "object") return "";
  const out = [];
  const k = kindOf(r);
  if (k !== "unknown") out.push(k);
  if (r.year) out.push(String(r.year));
  if (k === "episode" && r.season != null && r.episode != null) out.push(`S${String(r.season).padStart(2, "0")}E${String(r.episode).padStart(2, "0")}`);
  else if (k === "season" && r.season != null) out.push(`S${String(r.season).padStart(2, "0")}`);
  if (r.watched_at) out.push("watched");
  if (r.record_type === "title_status") out.push("title status", String(r.status || "unknown").replaceAll("_", " "));
  return out.join(" • ");
}

const stringify = (v) => {
  if (v === null) return "null";
  if (v === undefined) return "—";
  if (["string", "number", "boolean"].includes(typeof v)) return String(v);
  try { return JSON.stringify(v); } catch { return String(v); }
};
const pretty = (v) => { try { return JSON.stringify(v, null, 2); } catch { return String(v); } };

async function copyText(t) {
  const s = String(t ?? "");
  if (!s) return false;
  return (await window.cwCopyText?.(s)) === true;
}

function renderRecordCard(label, rec, missingText = T("snapshots.compare.missing")) {
  if (!rec) return `<div class="card"><div class="ttl"><div class="name">${esc(label)}</div><div class="mini">${esc(missingText)}</div></div><div class="empty">${TH("snapshots.compare.noRecord")}</div></div>`;
  const kv = [[T("snapshots.compare.f.type"), kindOf(rec)], [T("snapshots.compare.f.title"), rec.title], [T("snapshots.compare.f.series"), rec.series_title || rec.show_title], [T("snapshots.compare.f.year"), rec.year], [T("snapshots.compare.f.season"), rec.season], [T("snapshots.compare.f.episode"), rec.episode], [T("snapshots.compare.f.watched"), rec.watched_at], [T("snapshots.compare.f.added"), rec.added_at], [T("snapshots.compare.f.updated"), rec.updated_at]].filter(([, v]) => v != null && v !== "");
  if (rec.record_type === "title_status") {
    const flag = value => value === true ? T("snapshots.compare.yes") : value === false ? T("snapshots.compare.no") : T("snapshots.compare.unknown");
    kv.push([T("snapshots.compare.f.titleStatus"), String(rec.status || "unknown").replaceAll("_", " ")], [T("snapshots.compare.f.providerStatus"), rec.native_status || "unknown"], [T("snapshots.compare.f.rewatching"), flag(rec.rewatching)], [T("snapshots.compare.f.watchlisted"), flag(rec.watchlisted)]);
  }
  const chips = [];
  for (const [prefix, obj] of [["", rec.ids], ["show.", rec.show_ids]]) {
    if (!obj || typeof obj !== "object") continue;
    for (const [k, v] of Object.entries(obj)) {
      if (v == null || v === "" || v === 0 || v === false) continue;
      chips.push(`<button type="button" class="chip" data-copy="${esc(v)}" title="${TH("snapshots.compare.copy")}"><span class="k">${esc(prefix + k)}</span><span class="v mono">${esc(v)}</span></button>`);
    }
  }
  return `<div class="card"><div class="ttl"><div class="name">${esc(label)}</div><div class="mini">${esc(displayTitle(rec))}</div></div><div class="kv">${kv.map(([k, v]) => `<div class="k">${esc(k)}</div><div class="v">${esc(stringify(v))}</div>`).join("")}</div>${chips.length ? `<div class="chips">${chips.join("")}</div>` : ""}<details data-cc-raw="1"><summary>${TH("snapshots.compare.rawJson")}</summary><pre class="mono">${esc(pretty(rec))}</pre></details></div>`;
}

const renderChanges = (changes) => !Array.isArray(changes) || !changes.length ? `<div class="empty">${TH("snapshots.compare.noChanges")}</div>` : changes.map((c) => `<div class="chg"><div class="p mono">${esc(String(c.path || ""))}</div><div><div class="lab">A</div><div class="v mono">${esc(stringify(c.old))}</div></div><div><div class="lab">B</div><div class="v mono">${esc(stringify(c.new))}</div></div></div>`).join("");


export {kindOf, displayTitle, displaySub, pretty, copyText, renderRecordCard, renderChanges};
