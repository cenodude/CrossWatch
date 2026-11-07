# providers/sync/floppy/_status.py
# CrossWatch - Floppy consumption status reads and preservation-only updates.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Mapping

from cw_platform.status_mapping import Capabilities, Observation, ReadState, Status
from providers.sync._status_mapping import NativeStatusAdapter, Record, digest, observation
from ._common import api_patch, item_from_row, paged, watchlist_name


def identity(row):
    parts = str(row.get("item_id") or "").split("/")
    if len(parts) < 3 or parts[0] not in {"movie", "tv"} or parts[1] != "tmdb" or not parts[2].isdigit():
        return None
    inner = row.get("item") or {}
    if not isinstance(inner, Mapping):
        raise RuntimeError("floppy_status_invalid_item")
    if inner.get("library_media_type") == "anime":
        return None
    return "/".join(parts[:3])


def personal(row, *, omit_status=False):
    excluded = {"item", "image", "backdrop", "title", "next_episode", "show", "max_progress", "episodes_left", "total_episodes_left"}
    if omit_status:
        excluded |= {"status"}
    return {key: value for key, value in row.items() if key not in excluded}


def consumption_id(row):
    value = str(row.get("consumption_id") or "")
    if not value.isdigit():
        raise RuntimeError("floppy_status_missing_consumption")
    return value


def rewatching(row):
    return bool(row.get("is_rewatching") or row.get("rewatching") or row.get("rewatch_started_at"))


def read_rows(adapter, path, **kwargs):
    return paged(adapter, path, require_envelope=True, **kwargs)


class StatusAdapter(NativeStatusAdapter):
    capabilities = {media: Capabilities(frozenset(Status), frozenset({Status.WATCHING, Status.ON_HOLD, Status.DROPPED}))
                    for media in ("movies", "shows")}
    effects = {
        "watching": "Show updates require an existing active season and preserve season overrides. Only TMDb movie/TV entries with one unambiguous consumption are supported.",
        "on_hold": "Updates the selected consumption only; preserves dates, notes, ratings, plays and custom Watchlist membership.",
        "dropped": "Skipped when dropping a show would also drop an active season.",
        "completed": "Completed can create watches and change dates, so it is unavailable.",
    }
    statuses = {"0": Status.PLANNED, "1": Status.WATCHING, "2": Status.ON_HOLD, "3": Status.COMPLETED, "4": Status.DROPPED}

    def adapter(self):
        from providers.sync._mod_FLOPPY import FLOPPYModule
        return FLOPPYModule(self.cfg)

    def titles(self, adapter):
        return [row for kind in ("movie", "tv") for row in read_rows(adapter, f"media/{kind}")]

    def read(self):
        adapter = self.adapter()
        list_rows = read_rows(adapter, "lists")
        if any(not row.get("id") or not isinstance(row.get("name"), str) for row in list_rows):
            raise RuntimeError("floppy_status_invalid_lists")
        watchlists = {str(row["id"]) for row in list_rows if row["name"].casefold() == watchlist_name(adapter).casefold()}
        before = self.titles(adapter)
        groups = defaultdict(list)
        for row in before:
            key = identity(row)
            if key:
                consumption_id(row)
                groups[key].append(row)
        children = defaultdict(lambda: defaultdict(list))
        for kind in ("season", "episode"):
            seen = set()
            for row in read_rows(adapter, f"media/{kind}"):
                key = identity(row)
                if key:
                    ident = (str(row.get("item_id")), consumption_id(row))
                    if ident in seen:
                        raise RuntimeError("floppy_status_duplicate_child")
                    seen.add(ident)
                    children[key][kind].append(row)
        playback = defaultdict(list)
        for row in read_rows(adapter, "playback/progress", params={"media_type": "movie,episode", "completed": "false"}):
            kind = row.get("media_type")
            if kind not in {"movie", "episode"}:
                raise RuntimeError("floppy_status_invalid_playback")
            ids = row.get("ids") or {}
            ident = ids.get("tmdb") or (row.get("media_id") if row.get("source") == "tmdb" else None)
            if ident:
                playback[f"{'movie' if kind == 'movie' else 'tv'}/tmdb/{ident}"].append(row)
        result = {}
        for key, entries in groups.items():
            row = entries[0]
            item = item_from_row(row)
            if item is None:
                raise RuntimeError("floppy_status_missing_identity")
            media = "movies" if item["type"] == "movie" else "shows"
            history = read_rows(adapter, f"media/{key}/history")
            ids = [consumption_id(entry) for entry in history]
            if len(set(ids)) != len(ids):
                raise RuntimeError("floppy_status_duplicate_consumption")
            single = len(entries) == len(history) == 1 and ids[0] == consumption_id(row)
            fields = ("status", "progress", "score", "start_date", "end_date", "notes")
            agrees = single and all(field in row and field in history[0] and row[field] == history[0][field] for field in fields)
            lists = row.get("lists")
            if not isinstance(lists, list) or any(not isinstance(entry, Mapping) or not entry.get("list_id") for entry in lists):
                raise RuntimeError("floppy_status_incomplete_lists")
            seasons = children[key]["season"]
            episodes = children[key]["episode"]
            native = str(row.get("status"))
            protected = {
                "entry": personal(row, omit_status=True),
                "history": sorted(digest(personal(entry, omit_status=single)) for entry in history),
                "children": {kind: sorted(digest(personal(entry)) for entry in entries) for kind, entries in children[key].items()},
                "playback": sorted(digest(entry) for entry in playback[key]),
            }
            repeat = any(rewatching(entry) for entry in [row, *history, *seasons]) or any(
                count > 1 for count in Counter(str(entry.get("item_id")) for entry in episodes).values())
            listed = native == "0" or any(str(entry["list_id"]) in watchlists for entry in lists)
            obs = (observation(native, self.statuses, protected, watchlisted=listed, rewatching=repeat)
                   if agrees else Observation(ReadState.UNKNOWN))
            result[key] = Record(key, media, item, obs, {
                "consumption_id": consumption_id(row), "active_season": any(str(entry.get("status")) == "1" for entry in seasons),
                "unknown_season": any(str(entry.get("status")) not in self.statuses for entry in seasons),
                "dropped_season": any(str(entry.get("status")) == "4" for entry in seasons),
                "has_playback": bool(playback[key]),
            })
        after = self.titles(adapter)
        if sorted(digest(personal(row)) for row in before) != sorted(digest(personal(row)) for row in after):
            raise RuntimeError("floppy_status_snapshot_changed")
        return result

    def blockers(self, record, desired):
        if record.observation.state != ReadState.PRESENT:
            return ("The current Floppy consumption is ambiguous or incomplete.",)
        if record.observation.status in {Status.PLANNED, Status.COMPLETED}:
            return ("Planned and completed destinations are preserved.",)
        if record.raw.get("has_playback"):
            return ("This title has playback in progress; its status is left unchanged.",)
        if record.media == "shows":
            if record.raw.get("unknown_season"):
                return ("A season has an unknown status; season state is preserved.",)
            if desired == Status.DROPPED and record.raw.get("active_season"):
                return ("Dropping this show would also drop its active seasons.",)
            if desired == Status.WATCHING and (not record.raw.get("active_season") or record.raw.get("dropped_season")):
                return ("Starting this show could create a season or leave dropped seasons behind.",)
        return super().blockers(record, desired)

    def write(self, record, desired):
        if desired not in self.capabilities[record.media].writable or self.blockers(record, desired):
            raise RuntimeError("floppy_status_transition_blocked")
        native = next(key for key, status in self.statuses.items() if status == desired)
        response = api_patch(self.adapter(), f"media/{record.native_id}/history/{record.raw['consumption_id']}", json={"status": int(native)})
        if (not isinstance(response, Mapping) or str(response.get("consumption_id")) != record.raw["consumption_id"]
                or str(response.get("status")) != native or response.get("error") or response.get("errors")):
            raise RuntimeError("floppy_status_write_rejected")
