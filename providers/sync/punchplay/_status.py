# providers/sync/punchplay/_status.py
# CrossWatch - PunchPlay interaction status reads and partial updates.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from cw_platform.status_mapping import Capabilities, Observation, ReadState, Status
from providers.sync._status_mapping import NativeStatusAdapter, Record, digest, observation
from ._common import PLATFORM_BASE, punchplay_request, snapshot_pages
from ._history import _row_data
from ._ratings import _to_minimal
from ._watchlist import build_index


class StatusAdapter(NativeStatusAdapter):
    capabilities = {
        "shows": Capabilities(frozenset(Status), frozenset(Status) - {Status.PLANNED}),
        "movies": Capabilities(frozenset(Status) - {Status.ON_HOLD}, frozenset(Status) - {Status.PLANNED, Status.ON_HOLD}),
    }
    effects = {"completed": "Updates the Completed label only; no watched plays are created."}
    statuses = {"WATCHING": Status.WATCHING, "ON_HOLD": Status.ON_HOLD, "DROPPED": Status.DROPPED,
                "WATCHED": Status.COMPLETED, "PLANNING": Status.PLANNED}

    def adapter(self):
        from providers.sync._mod_PUNCHPLAY import PUNCHPLAYModule
        return PUNCHPLAYModule(self.cfg)

    def read(self):
        adapter = self.adapter()
        watchlist = build_index(adapter)
        listed = {(item["type"], str(item["ids"]["tmdb"])) for item in watchlist.values()}
        preserved = {}
        for resource in ("history", "playback"):
            preserved[resource] = sorted(digest(row) for page in snapshot_pages(adapter, resource, feature="history:status") for row in page)
        rows = {}
        for page in snapshot_pages(adapter, "interaction", feature="history:status"):
            for raw in page:
                entry = dict(_row_data(raw))
                item = _to_minimal(entry)
                if item is None:
                    raise RuntimeError("punchplay_status_missing_identity")
                if item["type"] not in {"movie", "show"}:
                    continue
                ident = str(item["ids"]["tmdb"])
                key = f"{item['type']}:{ident}"
                if key in rows:
                    raise RuntimeError("punchplay_status_duplicate_entry")
                native = str(entry.get("showStatus") or entry.get("show_status") or "")
                membership = (item["type"], ident) in listed
                protected = {k: v for k, v in entry.items() if k not in {"showStatus", "show_status", "updatedAt", "updated_at", "changedAt"}}
                protected["preserved"] = preserved
                if not native and membership:
                    native = "PLANNING"
                obs = (observation(native, self.statuses, protected, watchlisted=membership) if native else
                       Observation(ReadState.ABSENT, protected_token=digest(protected), watchlisted=membership, rewatching=False))
                rows[key] = Record(key, "movies" if item["type"] == "movie" else "shows", item, obs, entry)
        return rows

    def blockers(self, record, desired):
        if record.observation.rewatching:
            return ("Rewatching entries are preserved.",)
        return ()

    def write(self, record, desired):
        native = next(key for key, value in self.statuses.items() if value == desired)
        response = punchplay_request(self.adapter(), "PATCH",
            f"{PLATFORM_BASE}/title/{record.item['type']}/{record.item['ids']['tmdb']}/interact", json={"showStatus": native}, max_retries=1)
        response.raise_for_status()
        if response.json().get("ok") is not True:
            raise RuntimeError("punchplay_status_write_rejected")
