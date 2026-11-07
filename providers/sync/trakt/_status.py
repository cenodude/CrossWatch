# providers/sync/trakt/_status.py
# CrossWatch - Trakt dropped-show mapping with preserved watched progress.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from typing import Any

from cw_platform.status_mapping import Capabilities, Observation, ReadState, Status
from providers.sync._pagination import page_numbers
from providers.sync._status_mapping import NativeStatusAdapter, Record, digest, observation
from ._common import TraktPager


BASE = "https://api.trakt.tv"


def object_read(adapter, path, **kwargs):
    response = adapter.client.get(BASE + path, **kwargs)
    response.raise_for_status()
    value = response.json()
    if not isinstance(value, Mapping):
        raise RuntimeError("trakt_status_invalid_object")
    return value


def read_pages(adapter, path, **params) -> list[Mapping[str, Any]]:
    pager = TraktPager("status")
    result: list[Mapping[str, Any]] = []
    expected_pages = expected_items = 0
    for page in page_numbers():
        response = adapter.client.get(BASE + path, params={**params, "page": page, "limit": 250})
        rows = pager.read(response, page)
        if any(not isinstance(row, Mapping) for row in rows):
            raise RuntimeError("trakt_status_invalid_row")
        headers = {str(k).lower(): v for k, v in response.headers.items()}
        try:
            current = (int(headers["x-pagination-page-count"]), int(headers["x-pagination-item-count"]))
            actual = int(headers["x-pagination-page"])
        except (KeyError, TypeError, ValueError):
            raise RuntimeError("trakt_status_missing_pagination") from None
        if actual != page or min(current) < 0:
            raise RuntimeError("trakt_status_snapshot_changed")
        expected_pages = max(expected_pages, current[0])
        expected_items = max(expected_items, current[1])
        result.extend(rows)
        if not rows:
            if page < expected_pages or len(result) != expected_items:
                raise RuntimeError("trakt_status_incomplete_read")
            return result
    raise RuntimeError("trakt_status_incomplete_read")


def title(row):
    kind = "show" if isinstance(row.get("show"), Mapping) else "movie"
    media = row.get(kind)
    if not isinstance(media, Mapping):
        raise RuntimeError("trakt_status_missing_identity")
    ids = media.get("ids")
    if not isinstance(ids, Mapping):
        raise RuntimeError("trakt_status_missing_identity")
    ident = str(ids.get("trakt") or "")
    if not ident.isdigit():
        raise RuntimeError("trakt_status_missing_identity")
    return f"{kind}:{ident}", {"type": kind, "title": media.get("title"),
        "ids": {key: str(value) for key, value in ids.items() if key in {"trakt", "tmdb", "tvdb", "imdb"} and value}}, media


def preserved_row(row):
    result = dict(row)
    for kind in ("movie", "show", "season", "episode"):
        if isinstance(result.get(kind), Mapping):
            result[kind] = {key: value for key, value in result[kind].items() if key in {"ids", "season", "number"}}
    return result


def failures(value):
    return any(failures(child) for child in value.values()) if isinstance(value, Mapping) else bool(value)


class StatusAdapter(NativeStatusAdapter):
    capabilities = {
        "shows": Capabilities(frozenset({Status.PLANNED, Status.WATCHING, Status.COMPLETED, Status.DROPPED}),
                              frozenset({Status.WATCHING, Status.DROPPED})),
        "movies": Capabilities(frozenset({Status.PLANNED, Status.COMPLETED}), frozenset()),
    }
    effects = {
        "watching": "Restores a Dropped show only when its existing aired-episode progress is incomplete. No watches are added.",
        "dropped": "Uses Trakt's Dropped flag for partly watched shows. Watchlisted, completed and rewatching titles are preserved.",
        "completed": "Read from ended-series watched progress; caught-up returning shows are not Completed. Completion writes are unavailable.",
    }
    statuses = {status.value: status for status in Status}

    def adapter(self):
        from providers.sync._mod_TRAKT import TRAKTModule
        return TRAKTModule(self.cfg, connect=False)

    def read(self):
        adapter = self.adapter()
        before = object_read(adapter, "/sync/last_activities")
        if not before.get("all"):
            raise RuntimeError("trakt_status_invalid_activities")
        history = read_pages(adapter, "/sync/history", extended="full")
        watchlist = read_pages(adapter, "/sync/watchlist")
        dropped = read_pages(adapter, "/users/hidden/dropped", type="show", extended="full")
        ratings = read_pages(adapter, "/sync/ratings")
        playback = read_pages(adapter, "/sync/playback")
        notes = read_pages(adapter, "/users/me/notes/all")
        protected = {name: sorted(digest(preserved_row(row)) for row in rows) for name, rows in
                     (("history", history), ("watchlist", watchlist), ("ratings", ratings), ("playback", playback), ("notes", notes))}
        titles, listed, hidden = {}, set(), set()
        plays = Counter()
        counts = Counter()
        for rows, membership in ((history, None), (watchlist, listed), (dropped, hidden)):
            seen = set()
            for row in rows:
                key, item, media = title(row)
                titles[key] = (item, media)
                if membership is not None:
                    if membership is hidden and key in seen:
                        raise RuntimeError("trakt_status_duplicate_title")
                    seen.add(key)
                    membership.add(key)
                else:
                    if not row.get("id") or row["id"] in seen:
                        raise RuntimeError("trakt_status_invalid_history")
                    seen.add(row["id"])
                    episode = row.get("episode") or {}
                    episode_id = (episode.get("ids") or {}).get("trakt")
                    if item["type"] == "show" and not episode_id:
                        raise RuntimeError("trakt_status_missing_episode")
                    plays[(key, episode_id)] += 1
                    counts[key] += 1
        active = {title(row)[0] for row in playback}
        repeats = {key for (key, _), count in plays.items() if count > 1}
        result = {}
        for key, (item, media) in titles.items():
            progress = {}
            partial = completed = False
            repeat = key in repeats
            if item["type"] == "show":
                progress = object_read(adapter, f"/shows/{item['ids']['trakt']}/progress/watched",
                                       params={"hidden": "true", "specials": "false"})
                aired, watched = progress.get("aired"), progress.get("completed")
                if (type(aired) is not int or type(watched) is not int or aired < 0 or watched < 0
                        or watched > aired or "reset_at" not in progress or not isinstance(progress.get("seasons"), list)):
                    raise RuntimeError("trakt_status_invalid_progress")
                partial = 0 < watched < aired
                completed = aired > 0 and watched == aired and media.get("status") in {"ended", "canceled"}
                repeat |= bool(progress["reset_at"])
            else:
                completed = bool(counts[key])
            native = ("planned" if key in listed else "dropped" if key in hidden else
                      "completed" if completed else "watching" if partial else "unknown")
            obs = observation(native, self.statuses, {**protected, "progress": progress}, watchlisted=key in listed, rewatching=repeat)
            if native == "unknown":
                obs = Observation(ReadState.UNKNOWN, native_status="caught_up_or_untracked")
            result[key] = Record(key, "shows" if item["type"] == "show" else "movies", item, obs,
                                 {"partial": partial, "completed": completed, "has_history": bool(counts[key]), "has_playback": key in active})
        if before != object_read(adapter, "/sync/last_activities"):
            raise RuntimeError("trakt_status_snapshot_changed")
        return result

    def blockers(self, record, desired):
        obs = record.observation
        if obs.state != ReadState.PRESENT or obs.watchlisted or obs.status == Status.PLANNED:
            return ("Watchlisted or unknown Trakt states are preserved.",)
        if record.raw.get("completed") or obs.status == Status.COMPLETED:
            return ("Existing completion is preserved.",)
        if record.raw.get("has_playback"):
            return ("This title has playback in progress; its status is left unchanged.",)
        if not record.raw.get("partial") or not record.raw.get("has_history"):
            return ("This transition requires existing, incomplete watched progress.",)
        if desired == Status.WATCHING and obs.status != Status.DROPPED:
            return ("Watching can only restore an existing Dropped show.",)
        return super().blockers(record, desired)

    def write(self, record, desired):
        if desired not in self.capabilities[record.media].writable or self.blockers(record, desired):
            raise RuntimeError("trakt_status_transition_blocked")
        path = "/users/hidden/dropped" + ("/remove" if desired == Status.WATCHING else "")
        response = self.adapter().client.post(BASE + path, json={"shows": [{"ids": {"trakt": int(record.item["ids"]["trakt"])}}]}, max_retries=1)
        response.raise_for_status()
        body = response.json()
        expected = "deleted" if desired == Status.WATCHING else "added"
        if (not isinstance(body, Mapping) or not isinstance(body.get(expected), Mapping)
                or any(failures(body.get(key)) for key in ("not_found", "error", "errors"))):
            raise RuntimeError("trakt_status_write_rejected")
