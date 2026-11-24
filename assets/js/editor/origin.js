/* assets/js/editor/origin.js */
/* CrossWatch - Editor item origin through IDs, saved fixes, anime mapping and custom mappings */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
const LABELS = {anilist: "AniList", mal: "MAL", anidb: "AniDB", kitsu: "Kitsu", simkl: "SIMKL",
  tmdb: "TMDB", tvdb: "TVDB", imdb: "IMDb", trakt: "Trakt"};
const I18N = () => globalThis.window?.CW?.I18n || globalThis.window?.__cwI18n;
const T = (key, vars) => I18N()?.t?.(key, vars) ?? key;
const TH = (key, vars) => I18N()?.html?.(key, vars) ?? key;
const VIA_KEYS = new Set(["mapping", "custom", "direct", "ids"]);
const viaText = via => VIA_KEYS.has(via) ? T(`editor.origin.via.${via}`) : via;
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
  const title = item.series_title || item.show_title || item.title || item.name || T("editor.table.untitled");
  const year = item.year ? ` (${item.year})` : "";
  const coord = item.season != null ? ` S${item.season}${item.episode != null ? `E${item.episode}` : ""}` : "";
  const ids = idsText(item);
  return `${title}${year}${coord}${ids ? ` · ${ids}` : ""}`;
}

function fixText(fix) {
  const scope = T(fix.scope === "pair" ? "editor.origin.thisPair" : "editor.origin.allPairs");
  if (!fix.original_key) return T("editor.origin.addedByYou", {scope});
  const was = itemText(fix.original) || fix.original_key;
  return T("editor.origin.yourFix", {scope, was});
}

function rowFixText(fix) {
  const scope = T(fix.scope === "pair" ? "editor.origin.thisPair" : "editor.origin.allPairs");
  if (!fix.original_key) return T("editor.origin.rowAdded", {scope});
  return T("editor.origin.rowFix", {scope, original: itemText(fix.original) || fix.original_key});
}

function episodesText(group) {
  if (group.first_episode == null) return group.count > 1 ? T("editor.origin.items", {count: group.count}) : "";
  const span = group.first_episode === group.last_episode ? `E${group.first_episode}` : `E${group.first_episode}-${group.last_episode}`;
  return `${span} · ${T("profile.media.count.episode", {count: group.count})}`;
}

function providerMeta() {
  return window.CW?.ProviderMeta || {};
}

function profileText(provider, instance) {
  const meta = providerMeta();
  const id = String(instance || "default").trim() || "default";
  const named = id => meta.instanceLabel?.(provider, id) || (id.toLowerCase() === "default" ? T("editor.groups.defaultInstance") : id);
  if (!id.includes("~")) return named(id);
  const [account, ...user] = id.split("~");
  return `${named(account)} · ${user.join("~")}`;
}

function providerChip(provider, instance) {
  const meta = providerMeta();
  const chip = el("span", "cw-origin-provider");
  const tone = meta.tone?.(provider);
  if (tone?.solid) chip.style.setProperty("--cw-origin-tone", tone.solid);
  const logo = meta.logLogoPath?.(provider) || meta.logoPath?.(provider) || "";
  if (logo) {
    const img = el("img", "cw-origin-provider-logo");
    img.src = logo;
    img.alt = "";
    img.setAttribute("aria-hidden", "true");
    chip.append(img);
  }
  chip.append(el("span", "cw-origin-provider-name", meta.label?.(provider) || label(provider)),
    el("span", "cw-origin-profile", profileText(provider, instance)));
  return chip;
}

function chainLine(arrow, text, muted) {
  const line = el("div", "cw-origin-step");
  line.append(el("span", "cw-origin-arrow", arrow), el("span", muted ? "cw-origin-muted" : "", text));
  return line;
}

function renderSource(group) {
  const card = el("div", "cw-origin-card");
  card.append(providerChip(group.provider, group.instance));
  const head = el("div", "cw-origin-card-head");
  head.append(el("strong", "", group.title || T("editor.table.untitled")));
  if (group.year) head.append(el("span", "cw-origin-muted", ` (${group.year})`));
  card.append(head);
  if (group.pair_label) card.append(el("div", "cw-origin-muted", group.pair_label));
  if (group.fix) card.append(chainLine("→", fixText(group.fix), true));
  const coord = [linkText(group.link) || (group.season != null ? `S${group.season}` : ""), episodesText(group)]
    .filter(Boolean).join(" · ");
  if (coord) card.append(chainLine("→", coord, false));
  card.append(chainLine("→", viaText(group.via), true));
  if (group.anime) card.append(chainLine("→", animeText(group.anime), false));
  if (group.kind_mismatch) {
    card.append(el("div", "cw-origin-warn",
      T(group.kind === "movie" ? "editor.origin.mismatchMovie" : "editor.origin.mismatchShow", {ns: label(group.link?.namespace)})));
  }
  return card;
}

function renderLinks(links, total) {
  const box = el("div", "cw-origin-links");
  box.append(el("div", "cw-origin-muted", T("editor.origin.mappingLinks")));
  const list = el("ul", "cw-origin-list");
  links.forEach(link => {
    const via = link.via === "custom" ? ` (${T("editor.origin.yourCustom")})` : "";
    const eps = link.episodes ? ` E${link.episodes}` : "";
    list.append(el("li", "", `${linkText(link)}${eps}${via}`));
  });
  box.append(list);
  if (total > links.length) box.append(el("div", "cw-origin-muted", T("editor.origin.more", {count: total - links.length})));
  return box;
}

export function renderItemOrigin(body, data, state = {}) {
  body.replaceChildren();
  const animeMode = data.mode === "anime";
  const anime = data.anime || [];
  const head = el("div", "cw-origin-entry");
  head.append(el("span", "cw-origin-label", T("editor.origin.destination")));
  if (state.snapshot) head.append(providerChip(state.snapshot, state.instance || "default"));
  head.append(el("strong", "", data.title || T("editor.origin.thisItem")));
  if (data.row_fix) head.append(el("div", "cw-origin-muted", rowFixText(data.row_fix)));
  if (anime.length) {
    const prefix = animeMode ? "" : `${T("editor.origin.onTrackers")} `;
    head.append(el("div", "cw-origin-muted", prefix + anime.map(entry => {
      const eps = entry.episodes?.length ? ` ${T("editor.origin.episodeList", {list: entry.episodes.join(", ")})}` : "";
      return `${animeText(entry)}${eps}`;
    }).join(" · ")));
  }
  body.append(head);

  if (data.import_sources?.length) {
    const imports = el("div", "cw-origin-sources");
    imports.append(el("div", "cw-origin-label", T("editor.origin.importSources")));
    data.import_sources.forEach(source => imports.append(providerChip(source.provider, source.instance)));
    imports.append(el("p", "cw-origin-note", T("editor.origin.importNote")));
    body.append(imports);
  }

  if (animeMode && !anime.length) {
    const why = data.mapping_ready
      ? T("editor.origin.noAnimeLinked")
      : T("editor.origin.noAnimeNotLoaded");
    body.append(el("p", "cw-origin-note", why));
    return;
  }
  if (animeMode && !data.mapping_ready) {
    body.append(el("p", "cw-origin-note", T("editor.origin.notLoaded")));
  }

  if (data.search_truncated) {
    body.append(el("p", "cw-origin-warn", T("editor.origin.limit")));
  }

  const sources = data.sources || [];
  if (sources.length) {
    body.append(el("div", "cw-origin-label", T("editor.origin.comesFrom")));
    const list = el("div", "cw-origin-sources");
    sources.forEach(group => list.append(renderSource(group)));
    body.append(list);
    if (data.sources_total > sources.length) {
      body.append(el("div", "cw-origin-muted", T("editor.origin.more", {count: data.sources_total - sources.length})));
    }
    return;
  }

  const pairs = data.pairs || [];
  let note = T("editor.origin.noPair");
  if (pairs.length) {
    note = data.import_sources?.length
      ? T("editor.origin.noSnapshot")
      : animeMode
      ? T("editor.origin.noMaps")
      : T("editor.origin.noIds");
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
    ctx.appendPopupTitle(pop, T("editor.table.origin"));
    const body = el("div", "cw-origin-body", T("editor.origin.looking"));
    pop.append(body);
    const finish = data => {
      const actions = [];
      if (ctx.canReplaceRow?.(row) && ctx.openMapping) {
        actions.push({label: T("editor.table.editMapping"), kind: "primary", onClick: () => { close(); ctx.openMapping(row); }});
      }
      if (data && (data.mode === "anime" || (data.anime || []).length)) {
        actions.push({label: T("editor.origin.custom"), onClick: () => { close(); window.openAnimeOverridesModal?.(); }});
      }
      actions.push({label: T("editor.raw.close"), kind: "ghost", onClick: close});
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
      renderItemOrigin(body, data, state);
      finish(data);
    }).catch(error => {
      if (!pop.isConnected) return;
      body.textContent = error?.message || T("editor.origin.failed");
      finish(null);
    });
  }, {trackAnchorOnScroll: true});
}
