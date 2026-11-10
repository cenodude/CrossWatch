# providers/sync/trakt/_status.py
# CrossWatch - Trakt dropped-show mapping with preserved watched progress.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import copy
import hashlib
import threading
import time
from collections import Counter, defaultdict
from collections.abc import Mapping
from typing import Any

from cw_platform.status_mapping import Capabilities, Observation, ReadState, Status
from providers.sync._pagination import page_numbers
from providers.sync._status_mapping import NativeStatusAdapter, Record, digest, observation, skipped
from ._common import TraktPager


BASE = "https://api.trakt.tv"
MAX_AGE = 86400
ENDED_PROGRESS_MAX_AGE = 7 * 86400
LISTS = {
    "history": ("/sync/history", {"extended": "full"}, (("movies", "watched_at"), ("episodes", "watched_at"))),
    "watchlist": ("/sync/watchlist", {}, (("watchlist", "updated_at"), ("movies", "watchlisted_at"), ("shows", "watchlisted_at"))),
    "dropped": ("/users/hidden/dropped", {"type": "show", "extended": "full"}, (("shows", "dropped_at"),)),
    "ratings": ("/sync/ratings", {}, (("movies", "rated_at"), ("shows", "rated_at"), ("seasons", "rated_at"), ("episodes", "rated_at"))),
    "playback": ("/sync/playback", {}, (("movies", "paused_at"), ("episodes", "paused_at"))),
    "notes": ("/users/me/notes/all", {}, (("notes", "updated_at"),)),
}
_CACHE: dict[str, dict[str, Any]] = {}
_CACHE_LOCK = threading.Lock()


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


def compact_row(row):
    result = dict(row)
    for kind in ("movie", "show", "season", "episode"):
        if isinstance(result.get(kind), Mapping):
            result[kind] = {key: value for key, value in result[kind].items() if key in {"ids", "season", "number", "title", "status"}}
    return result


def stamps(activities, fields):
    return tuple((section, field, (activities.get(section) or {}).get(field)) for section, field in fields)


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

    def cache_key(self):
        raw = self.cfg.get("trakt")
        block = raw if isinstance(raw, Mapping) else {}
        identity = f"{self.cfg.get('_cw_provider_instance') or 'default'}|{block.get('access_token') or ''}"
        return hashlib.sha256(identity.encode()).hexdigest()

    def read(self):
        adapter = self.adapter()
        now = time.time()
        before = object_read(adapter, "/sync/last_activities")
        if not before.get("all"):
            raise RuntimeError("trakt_status_invalid_activities")
        cache_id = self.cache_key()
        with _CACHE_LOCK:
            cache = copy.deepcopy(_CACHE.get(cache_id) or {"lists": {}, "progress": {}})
        fetched = False
        lists = {}
        for name, (path, params, fields) in LISTS.items():
            marker = stamps(before, fields)
            entry = cache["lists"].get(name)
            if entry and entry[0] == marker and 0 <= now - entry[1] < MAX_AGE:
                lists[name] = entry[2]
                continue
            lists[name] = [compact_row(row) for row in read_pages(adapter, path, **params)]
            cache["lists"][name] = (marker, now, lists[name])
            fetched = True
        history, watchlist, dropped = lists["history"], lists["watchlist"], lists["dropped"]
        ratings, playback, notes = lists["ratings"], lists["playback"], lists["notes"]
        protected = {name: sorted(digest(preserved_row(row)) for row in rows) for name, rows in
                     (("history", history), ("watchlist", watchlist), ("ratings", ratings), ("playback", playback), ("notes", notes))}
        titles, listed, hidden = {}, set(), set()
        plays = Counter()
        counts = Counter()
        play_ids = defaultdict(list)
        bad = {}
        for rows, membership in ((history, None), (watchlist, listed), (dropped, hidden)):
            seen = set()
            for row in rows:
                found = self.identify(row)
                if found is None:
                    continue
                key, item, media = found
                titles[key] = (item, media)
                if membership is not None:
                    if membership is hidden and key in seen:
                        bad[key] = "trakt_status_duplicate_title"
                    seen.add(key)
                    membership.add(key)
                else:
                    if not row.get("id") or row["id"] in seen:
                        bad[key] = "trakt_status_invalid_history"
                        continue
                    seen.add(row["id"])
                    episode = row.get("episode") or {}
                    episode_id = (episode.get("ids") or {}).get("trakt")
                    if item["type"] == "show" and not episode_id:
                        bad[key] = "trakt_status_missing_episode"
                        continue
                    plays[(key, episode_id)] += 1
                    counts[key] += 1
                    play_ids[key].append(row["id"])
        active = {found[0] for found in map(self.identify, playback) if found}
        repeats = {key for (key, _), count in plays.items() if count > 1}
        hidden_at = ((before.get("shows") or {}).get("hidden_at"), (before.get("seasons") or {}).get("hidden_at"))
        kept_progress = {}
        result = {}
        for key, (item, media) in titles.items():
            kind = "shows" if item["type"] == "show" else "movies"
            if key in bad:
                result[key] = skipped(key, kind, item, bad[key])
                continue
            progress = {}
            partial = completed = False
            repeat = key in repeats
            if item["type"] == "show":
                marker = (tuple(sorted(str(value) for value in play_ids.get(key, ()))), hidden_at)
                ended = media.get("status") in {"ended", "canceled"}
                entry = cache["progress"].get(key)
                if entry and entry[0] == marker and 0 <= now - entry[1] < (ENDED_PROGRESS_MAX_AGE if ended else MAX_AGE):
                    progress = copy.deepcopy(entry[2])
                    kept_progress[key] = entry
                else:
                    progress = object_read(adapter, f"/shows/{item['ids']['trakt']}/progress/watched",
                                           params={"hidden": "true", "specials": "false"})
                    kept_progress[key] = (marker, now, copy.deepcopy(progress))
                    fetched = True
                aired, watched = progress.get("aired"), progress.get("completed")
                if (type(aired) is not int or type(watched) is not int or aired < 0 or watched < 0
                        or watched > aired or "reset_at" not in progress or not isinstance(progress.get("seasons"), list)):
                    result[key] = skipped(key, kind, item, "trakt_status_invalid_progress")
                    continue
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
        if fetched and before != object_read(adapter, "/sync/last_activities"):
            raise RuntimeError("trakt_status_snapshot_changed")
        with _CACHE_LOCK:
            _CACHE[cache_id] = {"lists": cache["lists"], "progress": kept_progress}
        return result

    def identify(self, row):
        try:
            return title(row)
        except RuntimeError as exc:
            if str(exc) != "trakt_status_missing_identity":
                raise
            self.skip_unidentified(str(exc))
            return None

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
