# providers/sync/anilist/_status.py
# CrossWatch - AniList native status enumeration and partial mutations.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from cw_platform.status_mapping import Observation, ReadState, Status
from providers.sync._status_mapping import ANIME_CAPS, NativeStatusAdapter, Record, complete_fields, observation, skipped


FIELDS = "id mediaId status progress repeat score(format: POINT_100) notes private hiddenFromStatusLists customLists advancedScores startedAt {year month day} completedAt {year month day}"
QUERY = "query ($userId:Int!) {MediaListCollection(userId:$userId,type:ANIME) {lists {entries {" + FIELDS + " media {id idMal format title {english romaji native}}}}}}"
MUTATION = "mutation ($id:Int!, $status:MediaListStatus!) {SaveMediaListEntry(id:$id,status:$status) {id status}}"
PRESERVED_FIELDS = {**{field: (int,) for field in ("id", "mediaId", "progress", "repeat")}, "score": (int, float),
                    "notes": (str, type(None)), "private": (bool,), "hiddenFromStatusLists": (bool,),
                    "customLists": (dict, type(None)), "advancedScores": (dict, type(None)),
                    "startedAt": (dict,), "completedAt": (dict,)}


def complete(entry):
    return (complete_fields(entry, PRESERVED_FIELDS) and entry["id"] > 0 and entry["mediaId"] > 0
            and entry["progress"] >= 0 and entry["repeat"] >= 0 and 0 <= entry["score"] <= 100
            and all(complete_fields(entry[field], {key: (int, type(None)) for key in ("year", "month", "day")})
                    for field in ("startedAt", "completedAt"))
            and all(type(value) is bool for value in (entry["customLists"] or {}).values())
            and all(type(value) in (int, float) for value in (entry["advancedScores"] or {}).values()))


class StatusAdapter(NativeStatusAdapter):
    capabilities = ANIME_CAPS
    effects = {"completed": "Completed can expand progress and is unavailable.", "watching": "Repeating entries retain their native rewatch state and are skipped."}
    statuses = {"CURRENT": Status.WATCHING, "REPEATING": Status.WATCHING, "PAUSED": Status.ON_HOLD,
                "DROPPED": Status.DROPPED, "COMPLETED": Status.COMPLETED, "PLANNING": Status.PLANNED}

    def client(self):
        from providers.sync._mod_ANILIST import ANILISTModule
        return ANILISTModule(self.cfg).client

    def read(self):
        from cw_platform.anime_mapping import AnimeMappingService
        client = self.client()
        data = client.gql(QUERY, {"userId": int(client.viewer()["id"])}, feature="history:status")
        collection = data.get("MediaListCollection")
        if not isinstance(collection, dict) or not isinstance(collection.get("lists"), list):
            raise RuntimeError("anilist_status_incomplete_read")
        rows = {}
        for group in collection["lists"]:
            if not isinstance(group, dict) or not isinstance(group.get("entries"), list):
                raise RuntimeError("anilist_status_incomplete_read")
            for entry in group["entries"]:
                media = entry["media"]
                ident = str(media["id"])
                item = {"type": "movie" if media.get("format") == "MOVIE" else "show", "ids": {"anilist": ident},
                        "title": media["title"].get("english") or media["title"].get("romaji") or media["title"].get("native") or ""}
                if media.get("idMal"):
                    item["ids"]["mal"] = str(media["idMal"])
                native_ids = dict(item["ids"])
                if (self.cfg.get("anime_mapping") or {}).get("enabled"):
                    item = AnimeMappingService(self.cfg).enrich_item(item)
                native = str(entry.get("status") or "")
                protected = {k: v for k, v in entry.items() if k not in {"status", "media"}}
                obs = (observation(native, self.statuses, protected, watchlisted=native == "PLANNING", rewatching=native == "REPEATING")
                       if complete(entry) else Observation(ReadState.UNKNOWN))
                row = Record(ident, "movies" if item["type"] == "movie" else "shows", item, obs, {"entry": entry, "native_ids": native_ids})
                if ident in rows and rows[ident] != row:
                    rows[ident] = skipped(ident, row.media, item, "anilist_status_inconsistent_entry")
                    continue
                rows[ident] = row
        return rows

    def write(self, record, desired):
        native = {Status.WATCHING: "CURRENT", Status.ON_HOLD: "PAUSED", Status.DROPPED: "DROPPED"}[desired]
        self.client().gql(MUTATION, {"id": int(record.raw["entry"]["id"]), "status": native}, feature="history:status", max_retries=1)

    def blockers(self, record, desired):
        entry = record.raw["entry"]
        if entry.get("progress") and not (entry.get("completedAt") or {}).get("year"):
            return ("Changing this status would change the history date inferred from AniList's update time.",)
        return super().blockers(record, desired)
