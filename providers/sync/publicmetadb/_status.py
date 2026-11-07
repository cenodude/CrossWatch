# providers/sync/publicmetadb/_status.py
# CrossWatch - PublicMetaDB dropped-show mappings with verified episode progress.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Mapping
from datetime import date, datetime, timezone
from urllib.parse import quote

from cw_platform.run_control import raise_if_cancelled
from cw_platform.status_mapping import Capabilities, Status
from providers.sync._status_mapping import NativeStatusAdapter, Record, digest, observation
from ._common import _tmdb_metadata_api_key, _tmdb_metadata_provider, paged_items


BASE = "/api/external"


def title(row, *, show=False):
    media = "tv" if show else row.get("media_type")
    ident = row.get("tmdb_id")
    if media not in {"tv", "movie"} or type(ident) is not int or ident <= 0:
        raise RuntimeError("publicmetadb_status_invalid_identity")
    kind = "show" if media == "tv" else "movie"
    return f"{kind}:{ident}", {"type": kind, "title": row.get("title") or row.get("name") or f"TMDb {ident}", "ids": {"tmdb": str(ident)}}


def snapshot(adapter):
    result = {name: paged_items(adapter, BASE + "/" + name, per_page=500, what="status", strict=True)
              for name in ("watched", "watched/shows", "resume", "dropped", "lists")}
    ids: list[str] = []
    for row in result["lists"]:
        ident = row.get("id")
        if not isinstance(ident, str) or not ident:
            raise RuntimeError("publicmetadb_status_invalid_lists")
        ids.append(ident)
    if len(ids) != len(set(ids)):
        raise RuntimeError("publicmetadb_status_invalid_lists")
    for ident in ids:
        name = f"lists/{quote(ident, safe='')}/items"
        result[name] = paged_items(adapter, BASE + "/" + name, per_page=500, what="status", strict=True)
    for name, rows in result.items():
        if name == "watched/shows":
            keys = [title(row, show=True)[0] for row in rows]
        elif name == "dropped" or name.endswith("/items"):
            keys = [title(row)[0] for row in rows]
        else:
            keys = [row.get("id") for row in rows]
        if any(not isinstance(key, str) or not key for key in keys) or len(keys) != len(set(keys)):
            raise RuntimeError("publicmetadb_status_duplicate_or_missing_identity")
    return result


def fingerprints(data):
    return {name: sorted(digest(row) for row in rows) for name, rows in data.items()}


def watchlists(adapter, rows):
    cfg = adapter.config.get("publicmetadb") or {}
    configured = str(cfg.get("watchlist_list_id") or "").strip()
    if configured and configured not in {row["id"] for row in rows}:
        raise RuntimeError("publicmetadb_status_missing_watchlist")
    name = str(cfg.get("watchlist_name") or "Watchlist").strip().lower()
    matches = [row["id"] for row in rows if str(row.get("name") or "").strip().lower() == name]
    typed = [row["id"] for row in rows if row.get("type") == "watchlist"]
    fallback = [row["id"] for row in rows if str(row.get("name") or "").strip().lower() in {"watchlist", "crosswatch", "crosswatch watchlist"}]
    candidates = [configured] if configured else matches or typed or fallback
    if len(candidates) > 1:
        raise RuntimeError("publicmetadb_status_ambiguous_watchlist")
    return set(candidates) | set(typed)


def progress(adapter, ident, watched):
    if not watched or not _tmdb_metadata_api_key(adapter):
        return None
    try:
        provider = _tmdb_metadata_provider(adapter)
        details = provider.lookup("show", ident, fresh=True).data
        seasons = details.get("seasons")
        if not isinstance(seasons, list) or not isinstance(details.get("status"), str):
            return None
        numbers: list[int] = []
        for row in seasons:
            number = row.get("season_number") if isinstance(row, Mapping) else None
            if type(number) is not int or number < 0:
                return None
            numbers.append(number)
        if len(set(numbers)) != len(numbers):
            return None
        if type(details.get("number_of_seasons")) is not int or len([n for n in numbers if n > 0]) != details["number_of_seasons"]:
            return None
        known, aired = set(), set()
        today = datetime.now(timezone.utc).date()
        for number in numbers:
            if number == 0:
                continue
            raise_if_cancelled()
            season = provider.lookup("show", ident, season=number, fresh=True).data
            episodes = season.get("episodes")
            expected = next(row.get("episode_count") for row in seasons if row["season_number"] == number)
            if not isinstance(episodes, list) or type(expected) is not int or len(episodes) != expected:
                return None
            for row in episodes:
                episode = row.get("episode_number") if isinstance(row, Mapping) else None
                coord = (number, episode)
                if type(episode) is not int or episode <= 0 or coord in known:
                    return None
                known.add(coord)
                if row.get("air_date") and date.fromisoformat(row["air_date"]) < today:
                    aired.add(coord)
        if not watched <= known or not aired or type(details.get("number_of_episodes")) is not int or len(known) != details["number_of_episodes"]:
            return None
        return {"title": details.get("name"), "partial": bool(watched & aired) and bool(aired - watched),
                "completed": details["status"].lower() in {"ended", "canceled"} and known <= aired and known <= watched}
    except Exception:
        return None


class StatusAdapter(NativeStatusAdapter):
    capabilities = {
        "shows": Capabilities(frozenset({Status.PLANNED, Status.WATCHING, Status.COMPLETED, Status.DROPPED}),
                              frozenset({Status.WATCHING, Status.DROPPED})),
        "movies": Capabilities(frozenset({Status.PLANNED, Status.COMPLETED}), frozenset()),
    }
    effects = {
        "dropped": "Hides an existing partly watched show without deleting history or resume points. No entry or Watchlist item is created.",
        "watching": "Restores Dropped only with existing history and TMDb-confirmed unwatched aired episodes. Missing metadata leaves the title unchanged.",
        "completed": "Read-only: all regular episodes watched on an ended show. Completion and movie status writes are unavailable.",
    }
    statuses = {status.value: status for status in Status}

    def adapter(self):
        from providers.sync._mod_PUBLICMETADB import PUBLICMETADBModule
        return PUBLICMETADBModule(self.cfg)

    def read(self):
        adapter = self.adapter()
        data = snapshot(adapter)
        titles, hidden, listed, active = {}, set(), set(), set()
        counts, coords = Counter(), defaultdict(Counter)
        seen = set()
        for row in data["watched"]:
            key, item = title(row)
            if not row.get("id") or row["id"] in seen or "watched_at" not in row:
                raise RuntimeError("publicmetadb_status_invalid_history")
            seen.add(row["id"])
            titles[key] = item
            counts[key] += 1
            if item["type"] == "show":
                season, episode = row.get("season"), row.get("episode")
                if type(season) is not int or type(episode) is not int or season < 0 or episode <= 0:
                    raise RuntimeError("publicmetadb_status_invalid_episode")
                coords[key][(season, episode)] += 1
        for name, target in (("dropped", hidden), ("resume", active)):
            for row in data[name]:
                key, item = title(row)
                if name == "dropped" and key in target:
                    raise RuntimeError("publicmetadb_status_duplicate_title")
                titles[key] = item
                target.add(key)
        for ident in watchlists(adapter, data["lists"]):
            for row in data[f"lists/{quote(ident, safe='')}/items"]:
                key, item = title(row)
                titles[key] = item
                listed.add(key)
        aggregates = {}
        for row in data["watched/shows"]:
            key, item = title(row, show=True)
            regular = {coord for coord in coords[key] if coord[0] > 0}
            if key in aggregates or type(row.get("watched_episodes")) is not int or row["watched_episodes"] != len(regular):
                raise RuntimeError("publicmetadb_status_inconsistent_progress")
            aggregates[key] = row
            titles[key] = item
        if any(key not in aggregates for key in coords):
            raise RuntimeError("publicmetadb_status_missing_progress")
        protected = fingerprints({k: v for k, v in data.items() if k != "dropped"})
        result = {}
        for key, item in titles.items():
            show = item["type"] == "show"
            repeat = any(n > 1 for n in coords[key].values()) if show else counts[key] > 1
            detail = (progress(adapter, item["ids"]["tmdb"], {coord for coord in coords[key] if coord[0] > 0})
                      if show and key not in listed and not repeat else None)
            completed = bool(detail and detail["completed"]) if show else bool(counts[key])
            partial = bool(detail and detail["partial"])
            if detail and detail.get("title"):
                item["title"] = detail["title"]
            native = ("planned" if key in listed else "completed" if completed else "dropped" if key in hidden and show else
                      "watching" if partial else "unknown")
            obs = observation(native, self.statuses, protected, watchlisted=key in listed, rewatching=repeat)
            result[key] = Record(key, "shows" if show else "movies", item, obs,
                                 {"partial": partial, "completed": completed, "has_history": bool(counts[key]), "has_playback": key in active})
        if fingerprints(data) != fingerprints(snapshot(adapter)):
            raise RuntimeError("publicmetadb_status_snapshot_changed")
        return result

    def blockers(self, record, desired):
        if record.observation.watchlisted or record.observation.status == Status.PLANNED:
            return ("Watchlisted titles are preserved.",)
        if record.raw.get("completed") or record.observation.status == Status.COMPLETED:
            return ("Existing completion is preserved.",)
        if record.raw.get("has_playback"):
            return ("This title has playback in progress; its status is left unchanged.",)
        if not record.raw.get("partial") or not record.raw.get("has_history"):
            return ("This transition requires existing history and verified incomplete aired-episode progress.",)
        if desired == Status.WATCHING and record.observation.status != Status.DROPPED:
            return ("Watching can only restore an existing Dropped show.",)
        return super().blockers(record, desired)

    def write(self, record, desired):
        if desired not in self.capabilities[record.media].writable or self.blockers(record, desired):
            raise RuntimeError("publicmetadb_status_transition_blocked")
        ident = int(record.item["ids"]["tmdb"])
        method, path, kwargs = ("DELETE", f"{BASE}/dropped/{ident}/tv", {}) if desired == Status.WATCHING else (
            "POST", BASE + "/dropped", {"json": {"tmdb_id": ident, "media_type": "tv"}})
        response = self.adapter().client.request(method, path, max_retries=1, **kwargs)
        response.raise_for_status()
        body = response.json()
        if not isinstance(body, Mapping) or body.get("success") is not True or body.get("error") or body.get("errors"):
            raise RuntimeError("publicmetadb_status_write_rejected")
