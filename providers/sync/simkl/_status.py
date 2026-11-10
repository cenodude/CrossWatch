# providers/sync/simkl/_status.py
# CrossWatch - SIMKL title status snapshots and label-only updates.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from cw_platform.run_control import raise_if_cancelled
from cw_platform.status_mapping import Capabilities, Status
from providers.sync._status_mapping import ANIME_CAPS, NativeStatusAdapter, Record, observation, skipped


class StatusAdapter(NativeStatusAdapter):
    capabilities = {**ANIME_CAPS, "movies": Capabilities(frozenset({Status.PLANNED, Status.COMPLETED, Status.DROPPED}), frozenset({Status.DROPPED}))}
    effects = {"completed": "Completed can create watched history and is unavailable.", "watching": "Completed titles and active rewatches are preserved."}
    statuses = {s.value: s for s in Status} | {"plantowatch": Status.PLANNED, "hold": Status.ON_HOLD}

    def adapter(self):
        from providers.sync._mod_SIMKL import SIMKLModule
        return SIMKLModule(self.cfg)

    def read(self):
        from ._history import URL_ALL_ITEMS, _headers, _params
        from ._watchlist import _normalize_row
        adapter = self.adapter()
        headers = _headers(adapter, force_refresh=True)
        response = adapter.client.session.get(URL_ALL_ITEMS, headers=headers,
            params=_params(headers, extended="full_anime_seasons", episode_watched_at="yes"), timeout=adapter.cfg.timeout)
        response.raise_for_status()
        data = response.json()
        if (not isinstance(data, dict) or data.get("error") or data.get("errors")
                or any(not isinstance(data.get(bucket, []), list) for bucket in ("movies", "shows", "anime"))):
            raise RuntimeError("simkl_status_incomplete_read")
        rows = {}
        for bucket in ("movies", "shows", "anime"):
            for entry in data.get(bucket, []):
                item = _normalize_row(bucket, entry)
                ids = item.get("ids") or {}
                ident = str(ids.get("simkl") or ids.get("simkl_id") or "")
                if not ident:
                    self.skip_unidentified("simkl_status_missing_identity")
                    continue
                native = str(entry.get("status") or "")
                protected = {k: v for k, v in entry.items() if k not in {"status", "last_watched_at", "last_updated_at", "last_updated", "movie", "show", "anime"}}
                protected["last_watched_at"] = entry.get("last_watched_at")
                obs = observation(native, self.statuses, protected, watchlisted=native == "plantowatch", rewatching=bool(entry.get("rewatching")))
                key = f"{bucket}:{ident}"
                media = "movies" if item.get("type") == "movie" else "shows"
                if key in rows:
                    rows[key] = skipped(key, media, item, "simkl_status_duplicate_entry")
                    continue
                rows[key] = Record(key, media, item, obs, {"bucket": bucket, "entry": entry})
        return rows

    def write(self, record, desired):
        from ._history import URL_ADD_TO_LIST, _headers, _params
        adapter = self.adapter()
        headers = _headers(adapter, force_refresh=True)
        native = "hold" if desired == Status.ON_HOLD else desired.value
        body = {record.raw["bucket"]: [{"ids": {"simkl": int(record.native_id.split(":", 1)[1])}, "status": native}]}
        raise_if_cancelled()
        response = adapter.client.session.post(URL_ADD_TO_LIST, headers=headers, params=_params(headers), json=body, timeout=adapter.cfg.timeout)
        response.raise_for_status()
        result = response.json()
        if not isinstance(result, dict) or result.get("error") or result.get("errors"):
            raise RuntimeError("simkl_status_write_rejected")
        missing = result.get("not_found") or {}
        if not isinstance(missing, dict) or any(missing.values()):
            raise RuntimeError("simkl_status_write_not_found")
