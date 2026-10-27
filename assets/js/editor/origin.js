/* assets/js/editor/origin.js */
/* CrossWatch - Editor item origin through IDs, saved fixes, anime mapping and custom mappings */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
const LABELS = {anilist: "AniList", mal: "MAL", anidb: "AniDB", kitsu: "Kitsu", simkl: "SIMKL",
  tmdb: "TMDB", tvdb: "TVDB", imdb: "IMDb", trakt: "Trakt"};
const VIA = {mapping: "Anime ID Mapping", custom: "Your custom mapping", direct: "Same anime ID", ids: "Same IDs"};
const ID_ORDER = ["tmdb", "imdb", "tvdb", "trakt", "simkl", "mal", "anilist"];
const openOrigins = new WeakMap();

const label = ns => LABELS[String(ns || "").toLowerCase()] || String(ns || "").toUpperCase();

function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text != null) node.textContent = text;
  return node;
}

function season(value) {
  return value == null ? "" : ` S${value}`;
}

function linkText(link) {
  if (!link || !link.namespace) return "";
  const kind = link.kind === "movie" ? " (movie)" : "";
  return `${label(link.namespace)} ${link.id}${season(link.season)}${kind}`;
}

function animeText(anime) {
  return `${label(anime.namespace)} #${anime.id}`;
}

function idsText(item) {
  const ids = (["episode", "season"].includes(item?.type) ? item?.show_ids : item?.ids) || item?.ids || {};
  return ID_ORDER.filter(key => ids[key]).slice(0, 2).map(key => `${label(key)} ${ids[key]}`).join(", ");
}

function itemText(item) {
  if (!item) return "";
  const title = item.series_title || item.show_title || item.title || item.name || "Untitled";
  const year = item.year ? ` (${item.year})` : "";
  const coord = item.season != null ? ` S${item.season}${item.episode != null ? `E${item.episode}` : ""}` : "";
  const ids = idsText(item);
  return `${title}${year}${coord}${ids ? ` · ${ids}` : ""}`;
}

function fixText(fix) {
  const scope = fix.scope === "pair" ? "this pair" : "all pairs";
  if (!fix.original_key) return `Added by you in the Editor (${scope})`;
  const was = itemText(fix.original) || fix.original_key;
  return `Your fix (${scope}), was: ${was}`;
}

function rowFixText(fix) {
  const scope = fix.scope === "pair" ? "this pair" : "all pairs";
  if (!fix.original_key) return `This row was added by you in the Editor (${scope}).`;
  return `This row is your fix (${scope}). Original: ${itemText(fix.original) || fix.original_key}`;
}

function episodesText(group) {
  if (group.first_episode == null) return group.count > 1 ? `${group.count} items` : "";
  const span = group.first_episode === group.last_episode ? `E${group.first_episode}` : `E${group.first_episode}-${group.last_episode}`;
  return `${span} · ${group.count} episode${group.count === 1 ? "" : "s"}`;
}

function chainLine(arrow, text, muted) {
  const line = el("div", "cw-origin-step");
  line.append(el("span", "cw-origin-arrow", arrow), el("span", muted ? "cw-origin-muted" : "", text));
  return line;
}

function renderSource(group) {
  const card = el("div", "cw-origin-card");
  card.append(el("div", "cw-origin-provider", label(group.provider)));
  const head = el("div", "cw-origin-card-head");
  head.append(el("strong", "", group.title || "Untitled"));
  if (group.year) head.append(el("span", "cw-origin-muted", ` (${group.year})`));
  card.append(head);
  const where = [label(group.provider), group.instance && group.instance !== "default" ? group.instance : "",
    group.pair_label ? `· ${group.pair_label}` : ""].filter(Boolean).join(" ");
  card.append(el("div", "cw-origin-muted", where));
  if (group.fix) card.append(chainLine("→", fixText(group.fix), true));
  const coord = [linkText(group.link) || (group.season != null ? `S${group.season}` : ""), episodesText(group)]
    .filter(Boolean).join(" · ");
  if (coord) card.append(chainLine("→", coord, false));
  card.append(chainLine("→", VIA[group.via] || group.via, true));
  if (group.anime) card.append(chainLine("→", animeText(group.anime), false));
  if (group.kind_mismatch) {
    const own = group.kind === "movie" ? "movie" : "show";
    const other = own === "movie" ? "show" : "movie";
    card.append(el("div", "cw-origin-warn",
      `Matched through a ${label(group.link?.namespace)} ${other} with the same ID as this ${own}. Movie and show IDs are numbered separately, so this match is likely wrong.`));
  }
  return card;
}

function renderLinks(links, total) {
  const box = el("div", "cw-origin-links");
  box.append(el("div", "cw-origin-muted", "The mapping links this entry to:"));
  const list = el("ul", "cw-origin-list");
  links.forEach(link => {
    const via = link.via === "custom" ? " (your custom mapping)" : "";
    const eps = link.episodes ? ` E${link.episodes}` : "";
    list.append(el("li", "", `${linkText(link)}${eps}${via}`));
  });
  box.append(list);
  if (total > links.length) box.append(el("div", "cw-origin-muted", `${total - links.length} more not shown.`));
  return box;
}

function render(body, data) {
  body.replaceChildren();
  const animeMode = data.mode === "anime";
  const anime = data.anime || [];
  const head = el("div", "cw-origin-entry");
  head.append(el("span", "cw-origin-label", "Destination"));
  head.append(el("strong", "", data.title || "This item"));
  if (data.row_fix) head.append(el("div", "cw-origin-muted", rowFixText(data.row_fix)));
  if (anime.length) {
    const prefix = animeMode ? "" : "On anime trackers: ";
    head.append(el("div", "cw-origin-muted", prefix + anime.map(entry => {
      const eps = entry.episodes?.length ? ` episode ${entry.episodes.join(", ")}` : "";
      const via = {mapping: " (via Anime ID Mapping)", custom: " (via your custom mapping)"}[entry.via] || "";
      return `${animeText(entry)}${eps}${via}`;
    }).join(" · ")));
  }
  body.append(head);

  if (animeMode && !anime.length) {
    const why = data.mapping_ready
      ? "No anime ID found for this item, and Anime ID Mapping does not link it to an anime entry."
      : "No anime ID found for this item. Anime ID Mapping is not loaded, so CrossWatch cannot look further.";
    body.append(el("p", "cw-origin-note", why));
    return;
  }
  if (animeMode && !data.mapping_ready) {
    body.append(el("p", "cw-origin-note", "Anime ID Mapping is not loaded. Only direct IDs and custom mappings were checked."));
  }

  if (data.search_truncated) {
    body.append(el("p", "cw-origin-warn", "The mapping search hit its limit, so this list may be incomplete."));
  }

  const sources = data.sources || [];
  if (sources.length) {
    body.append(el("div", "cw-origin-label", "Comes from"));
    const list = el("div", "cw-origin-sources");
    sources.forEach(group => list.append(renderSource(group)));
    body.append(list);
    if (data.sources_total > sources.length) {
      body.append(el("div", "cw-origin-muted", `${data.sources_total - sources.length} more not shown.`));
    }
    return;
  }

  const pairs = data.pairs || [];
  let note = "No sync pair sends this feature to this provider, so there are no sources to check.";
  if (pairs.length) {
    note = animeMode
      ? "No title in your sources maps to this entry. It may come from an older mapping or a title match."
      : "No title in your sources has the same IDs. It may have been added on this provider directly, or matched by title.";
  }
  body.append(el("p", "cw-origin-note", note));
  if (animeMode && (data.links || []).length) body.append(renderLinks(data.links, data.links_total || data.links.length));
}

export function openItemOrigin(row, anchor, ctx) {
  const current = openOrigins.get(anchor);
  if (current?.pop.isConnected) {
    openOrigins.delete(anchor);
    current.close();
    return;
  }
  const {state} = ctx;
  const original = structuredClone(row.raw || {});
  original.type = row.type || original.type;
  original.title = row.title || original.title;
  ctx.openPopup(anchor, (pop, close) => {
    openOrigins.set(anchor, {pop, close});
    pop.classList.add("cw-origin-pop");
    ctx.appendPopupTitle(pop, "Where does this come from?");
    const body = el("div", "cw-origin-body", "Looking this up...");
    pop.append(body);
    const finish = data => {
      const actions = [];
      if (ctx.canReplaceRow?.(row) && ctx.openMapping) {
        actions.push({label: "Edit mapping", kind: "primary", onClick: () => { close(); ctx.openMapping(row); }});
      }
      if (data && (data.mode === "anime" || (data.anime || []).length)) {
        actions.push({label: "Custom anime mappings", onClick: () => { close(); window.openAnimeOverridesModal?.(); }});
      }
      actions.push({label: "Close", kind: "ghost", onClick: close});
      ctx.appendPopupActions(pop, actions);
      ctx.positionPopup?.(pop, anchor);
    };
    ctx.fetchJSON("/api/editor/origin", {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({provider: state.snapshot, instance: state.instance || "default", feature: state.kind,
        key: String(row.key || ""), pair_id: state.mappingPair || "", original}),
    }).then(data => {
      if (!pop.isConnected) return;
      render(body, data);
      finish(data);
    }).catch(error => {
      if (!pop.isConnected) return;
      body.textContent = error?.message || "Lookup failed.";
      finish(null);
    });
  }, {trackAnchorOnScroll: true});
}
