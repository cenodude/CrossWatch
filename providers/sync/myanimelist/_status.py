# providers/sync/myanimelist/_status.py
# CrossWatch - MyAnimeList native title status reads and partial writes.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from cw_platform.status_mapping import Observation, ReadState, Status
from providers.sync._status_mapping import ANIME_CAPS, NativeStatusAdapter, Record, complete_fields, observation


PRESERVED_FIELDS = {"is_rewatching": (bool,), "tags": (list,), "comments": (str,),
                    **{field: (int,) for field in ("score", "num_episodes_watched", "num_times_rewatched", "priority", "rewatch_value")}}


def complete(entry):
    return (complete_fields(entry, PRESERVED_FIELDS) and entry["num_episodes_watched"] >= 0 and entry["num_times_rewatched"] >= 0
            and 0 <= entry["score"] <= 10 and 0 <= entry["priority"] <= 2 and 0 <= entry["rewatch_value"] <= 5
            and all(isinstance(tag, str) for tag in entry["tags"])
            and all(field not in entry or isinstance(entry[field], str) for field in ("start_date", "finish_date")))


class StatusAdapter(NativeStatusAdapter):
    capabilities = ANIME_CAPS
    effects = {"completed": "Completed is unavailable because history reads expand watched totals."}
    statuses = {s.value: s for s in Status} | {"plan_to_watch": Status.PLANNED}

    def client(self):
        from providers.sync._mod_MYANIMELIST import MyAnimeListClient, current_instance
        return MyAnimeListClient(self.cfg, current_instance(self.cfg))

    def read(self):
        client = self.client()
        client._shared = None
        rows = {}
        for _, media in client.entries():
            ident = str(media["id"])
            record = self.detail(ident)
            if record is None:
                raise RuntimeError("myanimelist_status_snapshot_changed")
            rows[ident] = record
        return rows

    def detail(self, ident):
        from ._common import media_item
        from types import SimpleNamespace
        media = self.client().media(ident)
        entry = media.get("my_list_status")
        if entry is None:
            return None
        item = media_item(SimpleNamespace(raw_cfg=self.cfg), media)
        native = str(entry.get("status") or "")
        protected = {k: v for k, v in entry.items() if k not in {"status", "updated_at"}}
        obs = (observation(native, self.statuses, protected, watchlisted=native == "plan_to_watch", rewatching=entry["is_rewatching"])
               if complete(entry) else Observation(ReadState.UNKNOWN))
        return Record(ident, "movies" if item["type"] == "movie" else "shows", item, obs, {"entry": entry, "native_ids": {"mal": ident}})

    def read_record(self, reference):
        return self.detail(reference.native_id)

    def write(self, record, desired):
        self.client().save(record.native_id, record.raw["entry"], {"status": desired.value})
