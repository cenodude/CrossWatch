/* CrossWatch - Capture comparison data and record display */
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
  if (!r || typeof r !== "object") return "Item";
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
  try {
    await navigator.clipboard.writeText(s);
    return true;
  } catch {
    try {
      const ta = Object.assign(document.createElement("textarea"), { value: s });
      ta.style.cssText = "position:fixed;left:-9999px";
      document.body.appendChild(ta);
      ta.focus({ preventScroll: true });
      ta.select();
      ta.setSelectionRange(0, ta.value.length);
      document.execCommand("copy");
      ta.remove();
      return true;
    } catch {
      return false;
    }
  }
}

function renderRecordCard(label, rec, missingText = "Missing") {
  if (!rec) return `<div class="card"><div class="ttl"><div class="name">${esc(label)}</div><div class="mini">${esc(missingText)}</div></div><div class="empty">No record in this file.</div></div>`;
  const kv = [["Type", kindOf(rec)], ["Title", rec.title], ["Series", rec.series_title || rec.show_title], ["Year", rec.year], ["Season", rec.season], ["Episode", rec.episode], ["Watched", rec.watched_at], ["Added", rec.added_at], ["Updated", rec.updated_at]].filter(([, v]) => v != null && v !== "");
  const chips = [];
  for (const [prefix, obj] of [["", rec.ids], ["show.", rec.show_ids]]) {
    if (!obj || typeof obj !== "object") continue;
    for (const [k, v] of Object.entries(obj)) {
      if (v == null || v === "" || v === 0 || v === false) continue;
      chips.push(`<button type="button" class="chip" data-copy="${esc(v)}" title="Click to copy"><span class="k">${esc(prefix + k)}</span><span class="v mono">${esc(v)}</span></button>`);
    }
  }
  return `<div class="card"><div class="ttl"><div class="name">${esc(label)}</div><div class="mini">${esc(displayTitle(rec))}</div></div><div class="kv">${kv.map(([k, v]) => `<div class="k">${esc(k)}</div><div class="v">${esc(stringify(v))}</div>`).join("")}</div>${chips.length ? `<div class="chips">${chips.join("")}</div>` : ""}<details data-cc-raw="1"><summary>Raw JSON</summary><pre class="mono">${esc(pretty(rec))}</pre></details></div>`;
}

const renderChanges = (changes) => !Array.isArray(changes) || !changes.length ? `<div class="empty">No field-level changes for this item.</div>` : changes.map((c) => `<div class="chg"><div class="p mono">${esc(String(c.path || ""))}</div><div><div class="lab">A</div><div class="v mono">${esc(stringify(c.old))}</div></div><div><div class="lab">B</div><div class="v mono">${esc(stringify(c.new))}</div></div></div>`).join("");


export {kindOf, displayTitle, displaySub, pretty, copyText, renderRecordCard, renderChanges};
