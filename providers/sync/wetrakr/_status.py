# providers/sync/wetrakr/_status.py
# CrossWatch - WeTrakr native status snapshots and preservation-only updates.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import copy
import threading
from collections import defaultdict
from collections.abc import Mapping

from cw_platform.status_mapping import Capabilities, Observation, ReadState, Status
from providers.sync._status_mapping import NativeStatusAdapter, Record, digest, observation
from ._common import (
    WeTrakrSyncError, _activities, account_key, body_of, commit_snapshot, identifier, media_item, pages, playing_rows,
    request, tracking_rows, write_lock,
)

_SNAPSHOTS: dict[str, tuple[dict, dict]] = {}
_SNAPSHOTS_LOCK = threading.Lock()


def owner(media):
    kind = media.get("type")
    if kind in {"episode", "season"}:
        media, kind = media.get("show") or {}, "show"
    ident = media.get("id")
    if kind not in {"movie", "show"} or not ident:
        raise WeTrakrSyncError("status_missing_identity")
    return f"{kind}:{ident}"


def user_state(row):
    interactions = row.get("interactions")
    user = interactions.get("user") if isinstance(interactions, Mapping) else None
    if not isinstance(user, Mapping):
        raise WeTrakrSyncError("status_missing_user_state")
    return user


def failures(value):
    if isinstance(value, Mapping):
        return any(failures(child) for child in value.values())
    return bool(value)


def activity_state(value):
    if not isinstance(value, Mapping) or not value.get("all"):
        raise WeTrakrSyncError("status_invalid_activities")
    return {key: item for key, item in value.items() if key != "journal_visible_until"}


class StatusAdapter(NativeStatusAdapter):
    capabilities = {
        "shows": Capabilities(frozenset(Status), frozenset({Status.WATCHING, Status.ON_HOLD, Status.DROPPED})),
        "movies": Capabilities(frozenset({Status.PLANNED, Status.COMPLETED, Status.DROPPED}), frozenset({Status.DROPPED})),
    }
    effects = {
        "completed": "Completed creates watched plays and is unavailable. Caught-up shows are not treated as Completed.",
        "watching": "Completed shows, active playback and rewatch history are preserved.",
        "on_hold": "Uses WeTrakr's Hold status for shows. Movie playback pauses are not On hold.",
        "dropped": "Skipped for completed shows or titles with playback in progress.",
    }
    statuses = {"watching": Status.WATCHING, "hold": Status.ON_HOLD, "paused": Status.ON_HOLD,
                "dropped": Status.DROPPED, "planning": Status.PLANNED, "watched": Status.COMPLETED}

    def adapter(self):
        from providers.sync._mod_WETRAKR import WETRAKRModule
        return WETRAKRModule(self.cfg, instance_id=self.cfg.get("_cw_provider_instance", "default"))

    def history_adapter(self):
        from providers.sync._mod_WETRAKR import WETRAKRModule
        return WETRAKRModule({**self.cfg, "_cw_history_rewatches": True}, instance_id=self.cfg.get("_cw_provider_instance", "default"))

    def read(self):
        adapter = self.adapter()
        cache_key = f"{account_key(adapter)}|{adapter.instance_id}"
        with write_lock(adapter):
            before = activity_state(_activities(adapter, fresh=True))
            with _SNAPSHOTS_LOCK:
                cached = _SNAPSHOTS.get(cache_key)
            if cached is not None and cached[0] == before:
                return copy.deepcopy(cached[1])
            history = self.history_adapter()
            watched = tracking_rows(history, "history")
            playing = playing_rows(adapter)
            preserved = defaultdict(lambda: defaultdict(list))
            plays = defaultdict(lambda: defaultdict(int))
            for kind in ("movie", "episode"):
                for row in watched[kind]:
                    media = row.get(kind)
                    if not isinstance(media, Mapping) or not row.get("id"):
                        raise WeTrakrSyncError("status_invalid_history")
                    key = owner(media)
                    protected = {k: v for k, v in row.items() if k != kind}
                    protected["media_id"] = media.get("id")
                    preserved[key]["history"].append(protected)
                    plays[key][str(media.get("id"))] += 1
                for row in playing[kind]:
                    if not isinstance(row.get("playback"), Mapping):
                        raise WeTrakrSyncError("status_invalid_playback")
                    preserved[owner(row)]["playback"].append({"id": row.get("id"), "playback": row["playback"]})
            for row in pages(adapter, "/sync/ratings/all"):
                if row.get("type") not in {"movie", "show", "season", "episode"}:
                    continue
                rating = user_state(row).get("rating")
                if not isinstance(rating, Mapping):
                    raise WeTrakrSyncError("status_invalid_rating")
                preserved[owner(row)]["ratings"].append({"id": row.get("id"), "type": row["type"], "rating": rating})
            for row in pages(adapter, "/sync/notes/all"):
                kind = row.get("target")
                if kind not in {"movie", "show", "season", "episode"}:
                    continue
                media = row.get(kind)
                if not isinstance(media, Mapping):
                    raise WeTrakrSyncError("status_invalid_note")
                protected = {k: v for k, v in row.items() if k != kind}
                protected["media_id"] = media.get("id")
                preserved[owner({**media, "type": kind})]["notes"].append(protected)
            grouped = defaultdict(list)
            for kind, states in (("movie", ("planning", "dropped", "watched")),
                                 ("show", ("planning", "watching", "hold", "dropped", "waiting", "watched"))):
                for state in states:
                    seen = set()
                    for row in pages(adapter, f"/sync/tracking/{state}/{kind}s", extended="interactions"):
                        item = media_item(row, kind)
                        key = owner(row)
                        if key in seen:
                            raise WeTrakrSyncError("status_duplicate_title")
                        seen.add(key)
                        grouped[key].append((state, item, user_state(row)))
            after = activity_state(_activities(adapter, fresh=True))
            if before != after:
                raise WeTrakrSyncError("status_snapshot_changed")
            commit_snapshot(history)
        result = {}
        for key, entries in grouped.items():
            states = {state for state, _, _ in entries}
            explicit = states - {"watched"}
            native = next(iter(explicit)) if len(explicit) == 1 else "watched" if not explicit else "ambiguous"
            declared = set()
            users = []
            for _, _, user in entries:
                tracking = user.get("tracking")
                last = tracking.get("last") if isinstance(tracking, Mapping) else None
                if not isinstance(last, Mapping):
                    declared.add("unknown")
                else:
                    declared.add("hold" if last.get("status") == "paused" else last.get("status"))
                users.append({**user, "tracking": {**tracking, "last": {k: v for k, v in last.items() if k not in {"status", "tracked_at"}}}
                              if isinstance(tracking, Mapping) and isinstance(last, Mapping) else {}})
            if not explicit and declared == {"none"}:
                native = "untracked"
            elif declared != {native}:
                native = "ambiguous"
            protected = {part: sorted(digest(value) for value in values) for part, values in preserved[key].items() if values}
            protected["user"] = sorted({digest(user) for user in users})
            rewatching = any(count > 1 for count in plays[key].values())
            obs = observation(native, self.statuses, protected, watchlisted="planning" in states, rewatching=rewatching)
            item = entries[0][1]
            if native == "waiting":
                obs = Observation(ReadState.UNKNOWN, native_status="waiting")
            result[key] = Record(key, "movies" if item["type"] == "movie" else "shows", item, obs,
                                 {"completed_show": item["type"] == "show" and "watched" in states,
                                  "has_playback": bool(preserved[key].get("playback"))})
        with _SNAPSHOTS_LOCK:
            _SNAPSHOTS[cache_key] = (before, copy.deepcopy(result))
        return result

    def blockers(self, record, desired):
        if record.raw.get("completed_show"):
            return ("WeTrakr still lists this show as watched; its existing completion is preserved.",)
        if record.raw.get("has_playback"):
            return ("This title has playback in progress; its status is left unchanged.",)
        return super().blockers(record, desired)

    def write(self, record, desired):
        if desired not in self.capabilities[record.media].writable or self.blockers(record, desired):
            raise WeTrakrSyncError("status_transition_blocked")
        native = "hold" if desired == Status.ON_HOLD else desired.value
        adapter = self.adapter()
        with write_lock(adapter):
            result = body_of(request(adapter, "POST", "/sync/tracking", json={
                record.media: [{**identifier(record.item["ids"]), "status": native}], "allow_rewatch": False,
            }))
        if (not isinstance(result, Mapping) or not any(key in result for key in ("added", "existing"))
                or any(failures(result.get(key)) for key in ("errored", "error", "notFound", "not_found"))):
            raise WeTrakrSyncError("status_write_rejected")
