# services/episode_alignment.py
# CrossWatch - Episode numbering alignment proposals for one show in a History sync pair
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import re
import secrets
from difflib import SequenceMatcher
import statistics
import time
from typing import Any

import requests
from fastapi import HTTPException
from pydantic import BaseModel, ConfigDict, Field

from cw_platform.episode_groups import endpoint, tokens, validate_groups, check_corrections
from cw_platform.episode_identity import unique_ids
from cw_platform.id_map import ID_KEYS, canonical_key
from cw_platform.local_db import manual_policy
from cw_platform.mapping_policy import effective_policy, update_mappings
from cw_platform.orchestrator._pairs_utils import manual_policy as provider_policy, merge_manual_adds
from cw_platform.provider_instances import normalize_instance_id

NAMESPACES = ("tmdb", "tvdb", "imdb", "trakt", "simkl")
LONG_RUNTIME = 1.6
MAX_EXTRA_PARTS = 3
GENERIC_TITLE = (r"(?:episode|chapter|part|aflevering|folge|episodio|épisode)?"
                 r"(?:\d*|one|two|three|four|five|six|seven|eight|nine|ten)|s\d+e\d+")


class AlignmentRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    pair_id: str = Field(min_length=1, max_length=256)
    show_ids: dict[str, str] = Field(min_length=1, max_length=16)


class Correction(BaseModel):
    model_config = ConfigDict(extra="forbid")
    key: str = Field(min_length=1, max_length=1024)
    season: int = Field(ge=0, le=999, strict=True)
    episode: int = Field(ge=1, le=99999, strict=True)
    show_ids: dict[str, str] | None = Field(default=None, max_length=16)


class GroupChoice(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str = Field(min_length=1, max_length=200)
    scrobble: bool = Field(default=False, strict=True)
    source: list[str] = Field(min_length=1, max_length=20)
    target: list[str] = Field(min_length=1, max_length=20)


class ApplyRequest(AlignmentRequest):
    groups: list[GroupChoice] = Field(default_factory=list, max_length=50)
    corrections: list[Correction] = Field(default_factory=list, max_length=500)


def _coords(item) -> tuple[int, int]:
    try:
        return int(item["season"]), int(item["episode"])
    except (KeyError, TypeError, ValueError):
        return -1, -1


def _base_title(item):
    text = re.sub(r"\(\s*\d+\s*\)|\b(?:part|pt)\.?\s*\d+\b", "", str(item.get("title") or ""), flags=re.I)
    return re.sub(r"\W+", "", text).casefold()


def _titles_differ(left, right):
    mine, theirs = _base_title(left), _base_title(right)
    if not mine or not theirs or re.fullmatch(GENERIC_TITLE, mine) or re.fullmatch(GENERIC_TITLE, theirs):
        return None
    return re.sub(r"\D", "", mine) != re.sub(r"\D", "", theirs) or SequenceMatcher(None, mine, theirs).ratio() < 0.8


def _doubtful(views, forward):
    verdicts = {key: _titles_differ(views[0][key], views[1][hit]) for key, hit in forward.items()}
    known = [value for value in verdicts.values() if value is not None]
    if len(known) < 4 or known.count(False) / len(known) < 0.6:
        return set()
    return {key for key, value in verdicts.items()
            if value and _coords(views[0][key]) != _coords(views[1][forward[key]])}


def _title_key(item):
    text = re.sub(r"\W+", "", str(item.get("title") or "")).casefold()
    return "" if re.fullmatch(GENERIC_TITLE, text) else text


def _with_catalog(view, layout, id_titles, show_ids, add):
    entries = {(season, row["episode"]): row for season, rows in ((layout or {}).get("seasons") or {}).items() for row in rows}
    by_id = lambda ids, own: next((id_titles[(key, str(value))] for key, value in (ids or {}).items()
                                   if (key, str(value)) in id_titles and str((own or {}).get(key) or "") != str(value)), "")
    out = {}
    for key, item in view.items():
        if not _title_key(item):
            title = (entries.get(_coords(item)) or {}).get("title") or by_id(item.get("ids"), item.get("show_ids"))
            if title:
                item = {**item, "title": title}
        out[key] = item
    catalog = set()
    if add and entries:
        have = {_coords(item) for item in out.values()}
        ids = next((dict(item["show_ids"]) for item in out.values() if item.get("show_ids")), dict(show_ids))
        for coords, row in sorted(entries.items()):
            if coords in have:
                continue
            item = dict(type="episode", title=row["title"] or by_id(row["ids"], None), series_title=layout.get("title") or "",
                        show_ids=ids, season=coords[0], episode=coords[1], ids=dict(row["ids"]))
            key = canonical_key(item)
            if key not in out:
                out[key] = item
                catalog.add(key)
    return out, catalog


def _show_episodes(index, show_ids):
    wanted = {(str(key).lower(), str(value).lower()) for key, value in show_ids.items() if value}
    return {key: item for key, item in index.items() if isinstance(item, dict) and item.get("type") == "episode"
            and _coords(item) != (-1, -1) and wanted & {(str(k).lower(), str(v).lower()) for k, v in (item.get("show_ids") or {}).items()}}


def _matches(left, right):
    ours, others = unique_ids(left), unique_ids(right)
    lookup = {}
    for key, item in right.items():
        for pair in others(item).items():
            lookup.setdefault(pair, set()).add(key)
    forward = {}
    for key, item in left.items():
        found = {hit for pair in ours(item).items() for hit in lookup.get(pair, ())}
        if len(found) == 1:
            forward[key] = next(iter(found))
    taken = {}
    for key, hit in forward.items():
        taken.setdefault(hit, []).append(key)
    result = {key: hit for key, hit in forward.items() if len(taken[hit]) == 1}
    names, free = [{}, {}], [{}, {}]
    for index, (view, done) in enumerate(((left, result), (right, set(result.values())))):
        for key, item in view.items():
            name = _title_key(item)
            if name:
                names[index][name] = names[index].get(name, 0) + 1
                if key not in done:
                    free[index][name] = key
    titled = {key: free[1][name] for name, key in free[0].items()
              if name in free[1] and names[0][name] == 1 and names[1][name] == 1}
    return {**result, **titled}, ours, set(titled)


def tmdb_season(key, show_id, season):
    try:
        response = requests.get(f"https://api.themoviedb.org/3/tv/{int(show_id)}/season/{int(season)}",
                                params={"api_key": key}, timeout=8)
        if response.status_code == 404:
            return {}
        rows = response.json().get("episodes") if response.status_code == 200 else None
    except Exception:
        return None
    if not isinstance(rows, list):
        return None
    return {int(row["episode_number"]): row for row in rows if isinstance(row, dict) and isinstance(row.get("episode_number"), int)}


def _follows_catalog(items, catalog, show_tmdb):
    agreed = False
    for item in items:
        _, number = _coords(item)
        entry = catalog.get(number)
        if not entry:
            return False
        own = str((item.get("ids") or {}).get("tmdb") or "")
        if own and own != str(show_tmdb):
            if own != str(entry.get("id")):
                return False
            agreed = True
            continue
        mine, theirs = _base_title(item), _base_title({"title": entry.get("name")})
        if mine and theirs:
            if mine != theirs:
                return False
            agreed = True
    return agreed


def _long(catalog, item):
    runtimes = [row.get("runtime") for row in catalog.values() if isinstance(row.get("runtime"), (int, float)) and row["runtime"] > 0]
    entry = catalog.get(_coords(item)[1]) or {}
    return bool(runtimes) and isinstance(entry.get("runtime"), (int, float)) and entry["runtime"] >= LONG_RUNTIME * statistics.median(runtimes)


def _orphans(split, combined, forward, has_ids, season, catalog, skip):
    reverse = {hit: key for key, hit in forward.items()}
    by_coords = {_coords(item): key for key, item in combined.items()}
    ordered = sorted((key for key, item in split.items() if _coords(item)[0] == season), key=lambda key: _coords(split[key]))
    runs, offset, run = [], 0, []
    for key in ordered:
        if key in skip:
            if run:
                runs.append(run)
                run = []
            continue
        number = _coords(split[key])[1]
        hit = forward.get(key)
        if hit and _coords(combined[hit])[0] == season:
            offset = _coords(combined[hit])[1] - number
        expected = number + offset
        occupied = by_coords.get((season, expected))
        orphan = not hit and bool(has_ids(split[key])) and (
            (occupied is not None and reverse.get(occupied) not in (None, key))
            or (catalog is not None and occupied is None and expected not in catalog))
        if orphan:
            run.append(key)
            offset -= 1
        elif run:
            runs.append(run)
            run = []
    if run:
        runs.append(run)
    return runs, ordered


def _side(key, item):
    season, episode = _coords(item)
    return dict(key=key, season=season, episode=episode, title=str(item.get("title") or ""))


def align(source, target, show_ids, *, tmdb_key="", fetch=tmdb_season, layouts=None):
    views = [_show_episodes(source, show_ids), _show_episodes(target, show_ids)]
    listed = set()
    if layouts:
        pool = [layouts.get("source"), layouts.get("target"), *(layouts.get("extra") or [])]
        id_titles = {(key, value): row["title"] for layout in pool if layout for rows in layout["seasons"].values()
                     for row in rows if row["title"] for key, value in row["ids"].items()}
        views[0], _ = _with_catalog(views[0], layouts.get("source"), id_titles, show_ids, False)
        views[1], listed = _with_catalog(views[1], layouts.get("target"), id_titles, show_ids, True)
    matched, source_ids, titled = _matches(*views)
    doubtful = _doubtful(views, matched)
    forward = {key: hit for key, hit in matched.items()
               if key not in doubtful and _coords(views[0][key])[0] == _coords(views[1][hit])[0]}
    backward = {hit: key for key, hit in forward.items()}
    target_ids = unique_ids(views[1])
    show_tmdb = next((str((item.get("show_ids") or {}).get("tmdb")) for view in views for item in view.values()
                      if (item.get("show_ids") or {}).get("tmdb")), str(show_ids.get("tmdb") or ""))
    metadata = "used" if tmdb_key and show_tmdb else "missing_key" if not tmdb_key else "no_tmdb_id"
    seasons = sorted({_coords(item)[0] for view in views for item in view.values()})
    catalogs = {}
    suggestions = []
    for season in seasons[:40]:
        unmatched = [any(_coords(item)[0] == season and key not in links and key not in listed and ids(item)
                         for key, item in view.items())
                     for view, links, ids in ((views[0], forward, source_ids), (views[1], backward, target_ids))]
        catalog = None
        if metadata == "used" and any(unmatched):
            catalog = fetch(tmdb_key, show_tmdb, season)
            if catalog is None:
                metadata = "unavailable"
        catalogs[season] = catalog
        for split_index, (links, ids) in enumerate(((forward, source_ids), (backward, target_ids))):
            split, combined = views[split_index], views[1 - split_index]
            trusted = catalog if catalog and _follows_catalog(
                [item for item in combined.values() if _coords(item)[0] == season], catalog, show_tmdb) else None
            skip = doubtful if split_index == 0 else {matched[key] for key in doubtful} | listed
            runs, ordered = _orphans(split, combined, links, ids, season, trusted, skip)
            for run in runs:
                if len(run) > MAX_EXTRA_PARTS:
                    continue
                first, last = ordered.index(run[0]), ordered.index(run[-1])
                near = {"previous": ordered[first - 1] if first else None,
                        "next": ordered[last + 1] if last + 1 < len(ordered) else None}
                options = {}
                for where, neighbour in near.items():
                    hit = links.get(neighbour)
                    if not hit or (hit if split_index == 0 else neighbour) in listed or _coords(combined[hit])[0] != season:
                        continue
                    parts = [neighbour, *run] if where == "previous" else [*run, neighbour]
                    members = [parts, [hit]] if split_index == 0 else [[hit], parts]
                    options[where] = dict(source=members[0], target=members[1], combined=hit, neighbour=neighbour)
                if not options:
                    continue
                suggested, reason = None, ""
                if trusted:
                    longer = [where for where, option in options.items() if _long(trusted, combined[option["combined"]])]
                    if len(longer) == 1:
                        suggested, reason = longer[0], "TMDb lists the combined episode at about double length."
                if not suggested:
                    titled = [where for where, option in options.items()
                              if _base_title(split[run[0]]) and _base_title(split[run[0]]) == _base_title(split[option["neighbour"]])]
                    if len(titled) == 1:
                        suggested, reason = titled[0], "The episode titles match."
                label = str(split[run[0]].get("series_title") or "").strip()
                anchor = views[0][options[suggested or next(iter(options))]["source"][0]]
                suggestions.append(dict(
                    name=" ".join(part for part in (label, "S%02dE%02d" % _coords(anchor)) if part),
                    season=season, orphans=run, split="source" if split_index == 0 else "target", suggested=suggested, reason=reason,
                    options={where: dict(source=option["source"], target=option["target"]) for where, option in options.items()}))
    placed = set()
    steps = {}
    for key, hit in matched.items():
        here, there = _coords(views[0][key]), _coords(views[1][hit])
        if here[0] != there[0] and key not in doubtful:
            steps.setdefault((here[0], there[0]), []).append(there[1] - here[1])
    slots = {_coords(item): key for key, item in views[1].items()}
    for (season, other), offsets in steps.items():
        if len(offsets) < 2 or len(set(offsets)) != 1:
            continue
        for key, item in views[0].items():
            slot = slots.get((other, _coords(item)[1] + offsets[0]))
            if _coords(item)[0] == season and key not in matched and slot and slot not in set(matched.values()):
                matched[key] = slot
                placed.add(key)
    rows = []
    for key, item in views[0].items():
        hit = matched.get(key)
        other = views[1].get(hit) if hit else None
        status = ("unmatched" if not other else "same" if _coords(other) == _coords(item)
                  else "shifted" if _coords(other)[0] == _coords(item)[0] else "moved")
        rows.append(dict(source=_side(key, item), target=_side(hit, other) if other else None, status=status,
                         doubt=key in doubtful, via="position" if key in placed else "title" if key in titled else "id" if other else ""))
    paired = set(matched.values())
    rows.extend(dict(source=None, target=_side(key, item), status="unmatched", doubt=False, via="")
                for key, item in views[1].items() if key not in paired and key not in listed)
    rows.sort(key=lambda row: ((row["source"] or row["target"])["season"], (row["source"] or row["target"])["episode"], row["source"] is None))
    moved = {}
    for row in rows:
        if row["status"] == "moved":
            pair = (row["source"]["season"], row["target"]["season"])
            moved[pair] = moved.get(pair, 0) + 1
    corrections = []
    for row in rows:
        if row["status"] not in ("shifted", "moved"):
            continue
        entry = dict(key=row["source"]["key"], season=row["target"]["season"], episode=row["target"]["episode"], doubt=row["doubt"])
        if row["status"] == "moved" and moved[(row["source"]["season"], row["target"]["season"])] < 2:
            entry.update(doubt=True, reason="Only one episode links these two seasons.")
        there = views[1][row["target"]["key"]]
        if canonical_key({**views[0][entry["key"]], "season": entry["season"], "episode": entry["episode"]}) != canonical_key(there):
            entry["show_ids"] = {str(key): str(value) for key, value in (there.get("show_ids") or {}).items() if value}
        corrections.append(entry)
    layout = [dict(source_season=pair[0], target_season=pair[1], episodes=count) for pair, count in sorted(moved.items())]
    diagnosis = "layout" if layout else "numbering" if corrections or suggestions else "aligned"
    if layout:
        suggestions = []
    return dict(rows=rows, corrections=corrections, groups=suggestions, metadata=metadata, diagnosis=diagnosis, layout=layout)


def _load(request, payload):
    from api import editorAPI as api
    from cw_platform.local_db import state
    from cw_platform.pair_scope import pair_feature_scope
    from services.episode_groups import authorized

    cfg, pairs = authorized(request)
    pair = next((pair for pair in pairs if str(pair["id"]) == payload.pair_id), None)
    if pair is None:
        raise HTTPException(404, "History sync pair not found")
    ends = [(str(pair[side]).upper(), normalize_instance_id(pair.get(f"{side}_instance"))) for side in ("source", "target")]
    for provider, instance in ends:
        api._require_instance_scope(cfg, request, provider, instance)
    raw = state.load_pair_state(api._STATE_BASE, pair_feature_scope(cfg, pair, "history", cfg["pairs"].index(pair) + 1), {"history"})
    policy = api._load_policy()
    effective = effective_policy(policy, payload.pair_id)
    grouped = [{token for group in ((policy.get("pairs") or {}).get(payload.pair_id) or {}).get("episode_groups") or []
                for side in ("source", "target") if endpoint(group[side]) == end
                for member in group[side]["episodes"] for token in tokens(member)} for end in ends]
    views = []
    for index, (provider, instance) in enumerate(ends):
        adds, blocked = ({}, set()) if index else provider_policy(effective, provider, "history", instance)
        items = api._load_state_items("history", provider, instance, raw_state=raw)
        items = {key: item for key, item in items.items() if isinstance(item, dict) and not grouped[index] & tokens(item)}
        views.append({key: item for key, item in merge_manual_adds(items, adds).items()
                      if isinstance(item, dict) and str(key).lower() not in blocked})
    return cfg, pair, ends, views


def _lookups(cfg, ends, views, show_ids):
    from services.catalog_lookup import LABELS, available, show_layout

    shows = [_show_episodes(view, show_ids) for view in views]
    seasons = {_coords(item)[0] for view in shows for item in view.values()}
    wanted: list[tuple[str, str, str, dict[str, Any]]] = [
        (side, str(ends[index][0]), str(ends[index][1]), shows[index]) for index, side in enumerate(("source", "target"))]
    if not {"TMDB", "FLICKLIST"} & {end[0] for end in ends}:
        wanted.append(("extra", "TMDB", "default", {}))
    extra: list[Any] = []
    layouts: dict[str, Any] = dict(source=None, target=None, extra=extra)
    sources: list[dict[str, Any]] = []
    for side, provider, instance, view in wanted:
        if provider not in LABELS or not available(cfg, provider, instance):
            continue
        known: dict[str, Any] = {}
        for item in view.values():
            if isinstance(item.get("show_ids"), dict) and item["show_ids"]:
                known = dict(item["show_ids"])
                break
        layout = show_layout(cfg, provider, instance, {**show_ids, **{str(key): str(value) for key, value in known.items() if value}}, seasons)
        sources.append(dict(label=LABELS[provider], ok=bool(layout), title=(layout or {}).get("title") or "",
                            seasons=[[season, count] for season, count in sorted(((layout or {}).get("counts") or {}).items()) if season > 0]))
        if side == "extra":
            extra.append(layout)
        else:
            layouts[side] = layout
    return layouts, sources


def propose(payload, request):
    cfg, pair, ends, views = _load(request, payload)
    layouts, sources = _lookups(cfg, ends, views, payload.show_ids)
    result = align(views[0], views[1], payload.show_ids, tmdb_key=str((cfg.get("tmdb") or {}).get("api_key") or "").strip(),
                   layouts=layouts)
    return dict(ok=True, pair_id=payload.pair_id, source=dict(provider=ends[0][0], instance=ends[0][1]),
                target=dict(provider=ends[1][0], instance=ends[1][1]), sources=sources, **result)


def _member(item):
    ids = item.get("show_ids") or {}
    namespace = next((key for key in NAMESPACES if ids.get(key)), None)
    if not namespace:
        raise HTTPException(400, "A grouped episode has no usable show ID")
    season, episode = _coords(item)
    return dict(type="episode", title=str(item.get("series_title") or item.get("title") or "")[:300],
                show_ids={namespace: str(ids[namespace])}, season=season, episode=episode)


def apply(payload, request):
    from api import editorAPI as api
    from services.saved_mappings import mapping_details

    cfg, pair, ends, views = _load(request, payload)
    scoped = [_show_episodes(view, payload.show_ids) for view in views]
    groups = []
    for choice in payload.groups:
        sides = {}
        for side, keys, view, end in zip(("source", "target"), (choice.source, choice.target), scoped, ends):
            if any(key not in view for key in keys):
                raise HTTPException(409, "The History data changed. Reopen Fix matching.")
            sides[side] = dict(provider=end[0], instance=end[1], episodes=[_member(view[key]) for key in keys])
        groups.append(dict(id=secrets.token_hex(16), name=choice.name, scrobble=choice.scrobble, **sides))
    grouped = {key for choice in payload.groups for key in choice.source}
    additions, records, blocks = {}, {}, []
    for change in payload.corrections:
        original = scoped[0].get(change.key)
        if original is None or change.key in grouped:
            raise HTTPException(409, "The History data changed. Reopen Fix matching.")
        corrected = {**original, "season": change.season, "episode": change.episode}
        if change.show_ids:
            if any(key not in ID_KEYS or not value.strip() or len(value) > 128 for key, value in change.show_ids.items()):
                raise HTTPException(400, "Use valid show identifiers")
            corrected["show_ids"] = dict(change.show_ids)
        target = canonical_key(corrected)
        additions[target] = corrected
        records[target] = dict(original_key=change.key, original=mapping_details(original), saved_at=int(time.time()), origin="alignment")
        blocks.append(change.key)

    def save(policy):
        node = policy.setdefault("pairs", {}).setdefault(payload.pair_id, {"version": 1, "providers": {}})
        merged = validate_groups([*(node.get("episode_groups") or []), *groups])
        if len(merged) > 500:
            raise ValueError("A sync pair supports up to 500 episode groups")
        node["episode_groups"] = merged
        if additions:
            where = ("history", ends[0][0], ends[0][1])
            update_mappings(policy, [("history", ends[0][0], additions, blocks, ends[0][1])],
                            mappings={where: records}, pair_id=payload.pair_id, merge=True)
        check_corrections(policy, payload.pair_id, merged)

    try:
        manual_policy.update_policy(api._STATE_BASE, save)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    return dict(ok=True, groups=len(groups), corrections=len(additions))
