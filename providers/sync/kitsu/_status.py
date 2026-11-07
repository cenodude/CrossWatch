# providers/sync/kitsu/_status.py
# CrossWatch - Kitsu native title status reads and partial writes.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from cw_platform.status_mapping import Observation, ReadState, Status
from providers.sync._status_mapping import ANIME_CAPS, NativeStatusAdapter, Record, complete_fields, observation


PRESERVED_FIELDS = {"progress": (int,), "reconsuming": (bool,), "reconsumeCount": (int,), "private": (bool,),
                    "notes": (str, type(None)), "ratingTwenty": (int, type(None)),
                    **{field: (str, type(None)) for field in ("startedAt", "finishedAt", "progressedAt")}}


def complete(entry):
    return (complete_fields(entry, PRESERVED_FIELDS) and entry["progress"] >= 0 and entry["reconsumeCount"] >= 0
            and (entry["ratingTwenty"] is None or 2 <= entry["ratingTwenty"] <= 20))


class StatusAdapter(NativeStatusAdapter):
    capabilities = ANIME_CAPS
    effects = {"watching": "Skipped when Kitsu would add a missing start date.", "completed": "Completed expands progress and dates and is unavailable."}
    statuses = {s.value: s for s in Status} | {"current": Status.WATCHING}

    def client(self):
        from providers.sync._mod_KITSU import KitsuClient, current_instance
        return KitsuClient(self.cfg, current_instance(self.cfg))

    def read(self):
        from ._common import media_item
        from types import SimpleNamespace
        rows = {}
        for entry, media in self.client().entries():
            item = media_item(SimpleNamespace(raw_cfg=self.cfg), media)
            ident = str(media["id"])
            attr = entry["attributes"]
            native = str(attr.get("status") or "")
            protected = {k: v for k, v in attr.items() if k not in {"status", "updatedAt"}}
            obs = (observation(native, self.statuses, protected, watchlisted=native == "planned", rewatching=attr["reconsuming"])
                   if complete(attr) else Observation(ReadState.UNKNOWN))
            rows[ident] = Record(ident, "movies" if item["type"] == "movie" else "shows", item, obs, {"entry": entry, "native_ids": {"kitsu": ident}})
        return rows

    def blockers(self, record, desired):
        attr = record.raw["entry"]["attributes"]
        if attr.get("progress") and not (attr.get("finishedAt") or attr.get("progressedAt")):
            return ("Changing this status would change the history date inferred from Kitsu's update time.",)
        if desired == Status.WATCHING and not record.raw["entry"]["attributes"].get("startedAt"):
            return ("Watching would add a start date in Kitsu.",)
        return super().blockers(record, desired)

    def write(self, record, desired):
        self.client().save(record.native_id, record.raw["entry"], {"status": "current" if desired == Status.WATCHING else desired.value})
