# providers/sync/scrob/_status.py
# CrossWatch - Scrob dropped mappings with native progress and forwarding protection.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections import Counter
from collections.abc import Mapping

from cw_platform.status_mapping import Capabilities, Observation, ReadState, Status
from cw_platform.run_control import raise_if_cancelled
from providers.sync._status_mapping import NativeStatusAdapter, Record, digest, observation
from ._common import cfg_section, paged_get, scrob_request, status_object, status_resume, status_rows


def title(media, kind=None):
    kind = kind or media.get("type")
    show = kind in {"episode", "series", "show", "season"}
    ident = media.get("show_tmdb_id") if kind == "episode" else media.get("tmdb_id")
    if kind not in {"movie", "episode", "series", "show", "season"}:
        raise RuntimeError("scrob_status_invalid_media")
    if type(ident) is not int or ident <= 0:
        return None
    item = {"type": "show" if show else "movie", "ids": {"tmdb": str(ident)},
            "title": (media.get("show_title") if kind == "episode" else media.get("title")) or f"TMDb {ident}"}
    return f"{item['type']}:{ident}", item


def preserved(value):
    if isinstance(value, list):
        return [preserved(row) for row in value]
    if not isinstance(value, Mapping):
        return value
    result = {k: preserved(v) for k, v in value.items() if k != "media"}
    if "media" in value:
        media = value["media"]
        if not isinstance(media, Mapping):
            raise RuntimeError("scrob_status_missing_media")
        result["media"] = {k: v for k, v in media.items() if k in {
            "id", "tmdb_id", "tvdb_id", "imdb_id", "type", "show_tmdb_id", "show_tvdb_id", "season_number", "episode_number",
            "watched", "watch_started", "watch_pct", "rewatch", "play_count", "last_watched_at", "user_rating", "in_lists", "in_library"}}
    return result


def forwarding(adapter):
    settings = status_object(adapter, "auth/settings")
    keys = {"trakt_push_dropped", "simkl_push_dropped", "mdblist_push_dropped"} | {k for k in settings if k.endswith("_push_dropped")}
    return {k: settings.get(k) if type(settings.get(k)) is bool else None for k in keys}


def snapshot(adapter):
    result = {"history": paged_get(adapter, "history", feature="status", page_size=100, strict=True),
              "dropped": status_object(adapter, "history/dropped"),
              "ratings": status_rows(status_object(adapter, "ratings"), "results"),
              "resume": status_resume(adapter),
              "playing": status_rows(status_object(adapter, "history/now-playing"), "now_playing"),
              "lists": status_rows(status_object(adapter, "lists"), "lists"), "forwarding": forwarding(adapter)}
    for bucket in ("shows", "movies"):
        status_rows(result["dropped"], bucket)
    details = {}
    for row in result["lists"]:
        ident = row.get("id")
        if type(ident) is not int or ident <= 0 or ident in details:
            raise RuntimeError("scrob_status_invalid_list")
        value = status_object(adapter, f"lists/{ident}")
        rows = status_rows(value, "items")
        if value.get("id") != ident or type(value.get("item_count")) is not int or len(rows) != value["item_count"] or row.get("item_count") != len(rows):
            raise RuntimeError("scrob_status_incomplete_list")
        ids = [r.get("id") for r in rows]
        if any(type(i) is not int for i in ids) or len(ids) != len(set(ids)):
            raise RuntimeError("scrob_status_duplicate_list_item")
        details[ident] = value
    result["list_details"] = details
    return result


class StatusAdapter(NativeStatusAdapter):
    notice = "Scrob cannot report what changed, so every status sync reads your whole Scrob library. With a large library this can take a while."
    capabilities = {
        "shows": Capabilities(frozenset({Status.PLANNED, Status.WATCHING, Status.COMPLETED, Status.DROPPED}),
                              frozenset({Status.WATCHING, Status.DROPPED})),
        "movies": Capabilities(frozenset({Status.PLANNED, Status.COMPLETED, Status.DROPPED}), frozenset({Status.DROPPED})),
    }
    effects = {
        "dropped": "Sets Scrob's Dropped flag on an existing title. Show writes require incomplete progress and Scrob's Dropped forwarding switched off.",
        "watching": "Restores an existing Dropped show with verified partial progress. Active rewatches, resume points and Watchlist entries are preserved.",
        "completed": "Read-only watched state. Completion creates history and is unavailable as a destination. Caught-up returning shows remain unknown.",
    }
    statuses = {status.value: status for status in Status}

    def adapter(self):
        from providers.sync._mod_SCROB import SCROBModule
        return SCROBModule(self.cfg)

    def read(self):
        adapter = self.adapter()
        data = snapshot(adapter)
        titles, hidden, listed, active, native = {}, set(), set(), set(), {}
        counts, repeats = Counter(), Counter()
        def add(media, kind=None):
            if not isinstance(media, Mapping):
                raise RuntimeError("scrob_status_missing_media")
            value = title(media, kind)
            if value is None:
                return None
            key, item = value
            titles[key] = item
            if item["type"] == "movie":
                ident = media.get("id")
                if type(ident) is not int or ident <= 0 or (key in native and native[key]["id"] != ident):
                    raise RuntimeError("scrob_status_ambiguous_movie")
                if key not in native or "watched" in media:
                    native[key] = media
            return key
        for row in data["history"]:
            key = add(row.get("media"))
            if key is None:
                continue
            if type(row.get("completed")) is not bool or "watched_at" not in row or type(row.get("play_count")) is not int:
                raise RuntimeError("scrob_status_invalid_history")
            if row["completed"]:
                counts[key] += 1
                repeats[(key, row["media"].get("id"))] += max(1, row["play_count"])
        for bucket, kind in (("shows", "series"), ("movies", "movie")):
            for row in data["dropped"][bucket]:
                key = add(row, kind)
                if key is None:
                    continue
                if key in hidden:
                    raise RuntimeError("scrob_status_duplicate_dropped")
                hidden.add(key)
                if kind == "series":
                    native[key] = row
        name = str(cfg_section(adapter).get("watchlist_name") or "Watchlist").strip().lower()
        matches = [row["id"] for row in data["lists"] if str(row.get("name") or "").strip().lower() == name]
        if len(matches) > 1:
            raise RuntimeError("scrob_status_ambiguous_watchlist")
        for ident, detail in data["list_details"].items():
            for row in detail["items"]:
                key = add(row.get("media"))
                if key and ident in matches:
                    listed.add(key)
        for row in data["ratings"]:
            add(row.get("media"))
        for row in data["resume"] + data["playing"]:
            key = add(row.get("media"))
            if key:
                active.add(key)
        protected = digest(preserved({k: v for k, v in data.items() if k != "dropped"}))
        repeated = {key for (key, _), count in repeats.items() if count > 1}
        result = {}
        for key, item in titles.items():
            show = item["type"] == "show"
            state = status_object(adapter, f"shows/{item['ids']['tmdb']}") if show else native[key]
            if show:
                if (state.get("tmdb_id") != int(item["ids"]["tmdb"]) or type(state.get("id")) is not int or state["id"] <= 0
                        or any(type(state.get(k)) is not bool for k in ("watched", "watch_started", "dropped"))
                        or type(state.get("watch_pct")) not in (int, float) or not 0 <= state["watch_pct"] <= 100
                        or "rewatch" not in state or not isinstance(state.get("season_states"), Mapping)):
                    raise RuntimeError("scrob_status_invalid_show_state")
                if state["dropped"] != (key in hidden) or (key in native and native[key]["id"] != state["id"]):
                    raise RuntimeError("scrob_status_changed_show_identity")
            partial = show and state["watch_started"] and not state["watched"] and state["watch_pct"] < 100 and bool(counts[key])
            watched = state.get("watched") is True or bool(counts[key]) and not show
            completed = watched and (not show or str(state.get("status") or "").lower() in {"ended", "canceled"})
            repeat = key in repeated or bool(state.get("rewatch"))
            caught_up = show and state["watched"] is True and str(state.get("status") or "").strip().lower() not in {"", "ended", "canceled"}
            untracked = not state["watch_started"] and state["watched"] is False and not counts[key] if show else False
            label = ("planned" if key in listed else "completed" if completed else "dropped" if key in hidden else "watching" if partial else
                     "caught_up" if caught_up else "untracked" if untracked else "unknown")
            personal = {k: state[k] for k in ("id", "watched", "watch_pct", "watch_started", "rewatch", "season_states", "user_rating", "in_lists", "in_library", "episode_order") if k in state}
            obs = observation(label, self.statuses, {"snapshot": protected, "state": personal}, watchlisted=key in listed, rewatching=repeat)
            if not show and label == "unknown" and state.get("watched") is False and not counts[key]:
                obs = Observation(ReadState.ABSENT, protected_token=digest({"snapshot": protected, "state": personal}), watchlisted=False, rewatching=repeat)
            result[key] = Record(key, "shows" if show else "movies", item, obs,
                {"id": state["id"], "partial": partial, "watched": watched, "has_playback": key in active,
                 "forwarding": data["forwarding"], "order": state.get("episode_order")})
        if digest(preserved(data)) != digest(preserved(snapshot(adapter))):
            raise RuntimeError("scrob_status_snapshot_changed")
        return result

    def blockers(self, record, desired):
        if record.observation.watchlisted or record.observation.status == Status.PLANNED:
            return ("Watchlisted titles are preserved.",)
        if record.raw.get("watched") or record.observation.status == Status.COMPLETED:
            return ("Existing watched state is preserved.",)
        if record.raw.get("has_playback"):
            return ("This title has playback in progress, including hidden resume points.",)
        if record.media == "shows":
            if any(value is not False for value in record.raw["forwarding"].values()):
                return ("Turn off Scrob's Dropped forwarding before mapping show statuses.",)
            if not record.raw.get("partial") or record.raw.get("order") not in {"aired", "tmdb:aired"}:
                return ("This transition requires verified partial progress in aired episode order.",)
            if desired == Status.WATCHING and record.observation.status != Status.DROPPED:
                return ("Watching can only restore a Dropped show.",)
        elif record.observation.state != ReadState.ABSENT:
            return ("Movie Drop requires an existing unwatched title with no active playback.",)
        return super().blockers(record, desired)

    def write(self, record, desired):
        if desired not in self.capabilities[record.media].writable or self.blockers(record, desired):
            raise RuntimeError("scrob_status_transition_blocked")
        adapter = self.adapter()
        show = record.media == "shows"
        if show and any(value is not False for value in forwarding(adapter).values()):
            raise RuntimeError("scrob_status_forwarding_enabled")
        path = "history/drop/show" if show else "history/drop/movie"
        payload = {"show_id" if show else "media_id": record.raw["id"]}
        method, kwargs = ("DELETE", {"params": payload}) if desired == Status.WATCHING else ("POST", {"json": payload})
        raise_if_cancelled()
        response = scrob_request(adapter, method, path, **kwargs)
        if not 200 <= response.status_code < 300:
            raise RuntimeError(f"scrob_status_write_http_{response.status_code}")
        body = response.json()
        if not isinstance(body, Mapping) or body.get("status") != "ok" or body.get("error") or body.get("errors"):
            raise RuntimeError("scrob_status_write_rejected")
