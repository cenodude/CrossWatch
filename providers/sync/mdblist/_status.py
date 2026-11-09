# providers/sync/mdblist/_status.py
# CrossWatch - MDBList dropped-show mappings with preserved account state.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections import Counter
from collections.abc import Mapping

from cw_platform.status_mapping import Capabilities, Status
from providers.sync._status_mapping import NativeStatusAdapter, Record, digest, observation
from ._common import status_pages


BASE = "https://api.mdblist.com"


def title(bucket, row):
    kind = "movie" if bucket == "movies" else "show"
    media = row.get(kind)
    if media is None and bucket in {"episodes", "seasons"}:
        media = (row.get(bucket[:-1]) or {}).get("show")
    if media is None and bucket in {"movies", "shows"}:
        media = row
    if not isinstance(media, Mapping):
        raise RuntimeError("mdblist_status_missing_identity")
    ids = media.get("ids")
    if not isinstance(ids, Mapping):
        raise RuntimeError("mdblist_status_missing_identity")
    ident = str(ids.get("tmdb") or "")
    if not ident.isdigit() or int(ident) <= 0:
        raise RuntimeError("mdblist_status_missing_identity")
    return f"{kind}:{ident}", {"type": kind, "title": media.get("title"),
        "ids": {k: str(v) for k, v in ids.items() if k in {"tmdb", "tvdb", "imdb", "trakt"} and v}}


def preserved(row):
    result = dict(row)
    for key in ("movie", "show", "season", "episode"):
        value = result.get(key)
        if isinstance(value, Mapping):
            result[key] = {k: preserved(v) if isinstance(v, Mapping) else v for k, v in value.items()
                           if k in {"ids", "season", "number", "show"}}
    return result


def activities(client):
    response = client.get(BASE + "/sync/last_activities")
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, Mapping) or not {"watched_at", "watchlisted_at", "dropped_at"} <= data.keys():
        raise RuntimeError("mdblist_status_invalid_activities")
    return {k: v for k, v in data.items() if k != "server_time"}


def states(client, titles):
    result = {}
    for kind in ("movie", "show"):
        ids = [int(item["ids"]["tmdb"]) for item in titles.values() if item["type"] == kind]
        for start in range(0, len(ids), 100):
            batch = ids[start:start + 100]
            response = client.post(BASE + f"/sync/state/{kind}/tmdb", json={"ids": batch})
            response.raise_for_status()
            data = response.json()
            if (not isinstance(data, Mapping) or data.get("type") != kind or data.get("provider") != "tmdb"
                    or data.get("not_found") != [] or not isinstance(data.get("items"), list)):
                raise RuntimeError("mdblist_status_invalid_state")
            seen = set()
            for row in data["items"]:
                if (not isinstance(row, Mapping) or type(row.get("id")) is not int or row["id"] not in batch
                        or row["id"] in seen or type(row.get("watched")) is not bool or type(row.get("watchlist")) is not bool):
                    raise RuntimeError("mdblist_status_invalid_state")
                if kind == "show" and (any(k not in row or (row[k] is not None and type(row[k]) is not bool)
                                           for k in ("inprogress", "completed", "has_unwatched_aired"))
                        or any(k not in row or (row[k] is not None and (type(row[k]) is not int or row[k] < 0))
                               for k in ("watched_episode_count", "total_episodes_aired"))):
                    raise RuntimeError("mdblist_status_invalid_progress")
                seen.add(row["id"])
                result[f"{kind}:{row['id']}"] = row
            if seen != set(batch):
                raise RuntimeError("mdblist_status_incomplete_state")
    return result


def failures(value):
    return any(failures(v) for v in value.values()) if isinstance(value, Mapping) else bool(value)


class StatusAdapter(NativeStatusAdapter):
    capabilities = {
        "shows": Capabilities(frozenset({Status.PLANNED, Status.WATCHING, Status.COMPLETED, Status.DROPPED}),
                              frozenset({Status.WATCHING, Status.DROPPED})),
        "movies": Capabilities(frozenset({Status.PLANNED, Status.COMPLETED}), frozenset()),
    }
    effects = {
        "dropped": "Sets MDBList's Dropped flag on an existing partly watched show. No watches or Watchlist entries are created.",
        "watching": "Restores a Dropped show with verified incomplete progress. Dropped seasons, completed titles and rewatches are preserved.",
        "completed": "Read from explicit completed state. Caught-up returning shows are not Completed. Completion writes are unavailable.",
    }
    statuses = {status.value: status for status in Status}

    def adapter(self):
        from providers.sync._mod_MDBLIST import MDBLISTModule
        return MDBLISTModule(self.cfg)

    def read(self):
        client = self.adapter().client
        before = activities(client)
        watched = status_pages(client, "/sync/watched")
        plays = status_pages(client, "/sync/watched", plays="all")
        watchlist = status_pages(client, "/watchlist/items")
        dropped = status_pages(client, "/sync/dropped")
        seasons = status_pages(client, "/sync/seasons/dropped")
        ratings = status_pages(client, "/sync/ratings")
        collection = status_pages(client, "/sync/collection")
        if (any(bucket not in {"movies", "shows"} for bucket, _ in watchlist)
                or any(bucket != "shows" for bucket, _ in dropped)
                or any(bucket != "seasons" for bucket, _ in seasons)):
            raise RuntimeError("mdblist_status_invalid_bucket")
        response = client.get(BASE + "/sync/playback")
        response.raise_for_status()
        playback = response.json()
        if not isinstance(playback, list) or any(not isinstance(row, Mapping) for row in playback):
            raise RuntimeError("mdblist_status_invalid_playback")
        titles, listed, hidden, season_hidden = {}, set(), set(), set()
        for rows, membership in ((watched, None), (plays, None), (watchlist, listed), (dropped, hidden), (seasons, season_hidden)):
            for bucket, row in rows:
                key, item = title(bucket, row)
                titles[key] = item
                if membership is not None:
                    if membership is not season_hidden and key in membership:
                        raise RuntimeError("mdblist_status_duplicate_title")
                    membership.add(key)
        counts, repeated = Counter(), Counter()
        play_ids = set()
        for bucket, row in plays:
            if bucket not in {"movies", "episodes"} or not row.get("play_id") or row["play_id"] in play_ids or not row.get("watched_at"):
                raise RuntimeError("mdblist_status_invalid_play")
            play_ids.add(row["play_id"])
            key, _ = title(bucket, row)
            episode_id = ((row.get("episode") or {}).get("ids") or {}).get("tmdb")
            if bucket == "episodes" and not episode_id:
                raise RuntimeError("mdblist_status_missing_episode")
            counts[key] += 1
            repeated[(key, episode_id)] += 1
        repeats = {key for (key, _), count in repeated.items() if count > 1}
        active = {title("movies" if row.get("type") == "movie" else "episodes", row)[0] for row in playback}
        protected = {name: sorted(digest((bucket, preserved(row))) for bucket, row in rows) for name, rows in
                     (("plays", plays), ("watchlist", watchlist), ("ratings", ratings), ("collection", collection), ("seasons", seasons))}
        protected["playback"] = sorted(digest(preserved({k: v for k, v in row.items()
            if k != "progress" or "progress_at_update" not in row})) for row in playback)
        native_states = states(client, titles)
        result = {}
        for key, item in titles.items():
            state = native_states[key]
            if state["watchlist"] != (key in listed):
                raise RuntimeError("mdblist_status_watchlist_changed")
            show = item["type"] == "show"
            aired, watched_count = state.get("total_episodes_aired"), state.get("watched_episode_count")
            partial = (show and type(aired) is int and type(watched_count) is int and 0 < watched_count < aired
                       and state.get("inprogress") is True and state.get("has_unwatched_aired") is True
                       and state.get("completed") is not True)
            completed = state.get("completed") is True if show else state["watched"]
            caught_up = (show and state["watched"] and type(aired) is int and type(watched_count) is int and watched_count >= aired
                         and state.get("has_unwatched_aired") is not True and state.get("completed") is not True)
            untracked = not state["watched"] and not counts[key]
            native = ("planned" if key in listed else "dropped_seasons" if key in season_hidden else
                      "completed" if completed else "dropped" if key in hidden else
                      "watching" if partial and state["watched"] and counts[key] else
                      "caught_up" if caught_up else "untracked" if untracked else "unknown")
            obs = observation(native, self.statuses, {**protected, "state": state}, watchlisted=key in listed, rewatching=key in repeats)
            result[key] = Record(key, "shows" if show else "movies", item, obs,
                {"partial": partial, "completed": completed, "has_history": bool(counts[key]) and state["watched"],
                 "has_playback": key in active, "dropped_seasons": key in season_hidden})
        if before != activities(client):
            raise RuntimeError("mdblist_status_snapshot_changed")
        return result

    def blockers(self, record, desired):
        if record.observation.watchlisted or record.observation.status == Status.PLANNED:
            return ("Watchlisted titles are preserved.",)
        if record.raw.get("completed") or record.observation.status == Status.COMPLETED:
            return ("Existing completion is preserved.",)
        if record.raw.get("dropped_seasons"):
            return ("This show has dropped seasons; its status is left unchanged.",)
        if record.raw.get("has_playback"):
            return ("This title has playback in progress; its status is left unchanged.",)
        if not record.raw.get("partial") or not record.raw.get("has_history"):
            return ("This transition requires existing, verified incomplete watched progress.",)
        if desired == Status.WATCHING and record.observation.status != Status.DROPPED:
            return ("Watching can only restore an existing Dropped show.",)
        return super().blockers(record, desired)

    def write(self, record, desired):
        if desired not in self.capabilities[record.media].writable or self.blockers(record, desired):
            raise RuntimeError("mdblist_status_transition_blocked")
        path = "/sync/dropped" + ("/remove" if desired == Status.WATCHING else "")
        response = self.adapter().client.post(BASE + path, json={"shows": [{"ids": {"tmdb": int(record.item["ids"]["tmdb"])}}]}, max_retries=0)
        response.raise_for_status()
        body = response.json()
        expected = "deleted" if desired == Status.WATCHING else "updated"
        if (not isinstance(body, Mapping) or not isinstance(body.get(expected), Mapping)
                or any(failures(body.get(k)) for k in ("errors", "error", "not_found"))):
            raise RuntimeError("mdblist_status_write_rejected")
