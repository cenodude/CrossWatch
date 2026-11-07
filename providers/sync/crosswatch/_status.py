# providers/sync/crosswatch/_status.py
# CrossWatch - Tracker note status reads and compare-and-swap updates.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from cw_platform.status_mapping import Capabilities, Observation, ReadState, Status
from providers.sync._status_mapping import NativeStatusAdapter, Record, digest, observation


class StatusAdapter(NativeStatusAdapter):
    creates_missing = True
    capabilities = {media: Capabilities(frozenset(Status), frozenset(Status) - {Status.PLANNED}) for media in ("movies", "shows")}
    effects = {"completed": "Updates the tracker status only; no watched plays are created."}

    def store(self):
        from providers.sync._mod_CROSSWATCH import CROSSWATCHModule
        from cw_platform.tracker_store import TrackerStore
        return TrackerStore(CROSSWATCHModule(self.cfg).cfg.base_path)

    def read(self):
        store = self.store()
        def current(feature):
            try:
                return store.read_current(feature)["items"]
            except FileNotFoundError:
                return {}
        watchlist = current("watchlist")
        listed = set()
        items = {}
        for feature in ("history", "ratings", "watchlist"):
            for item in (watchlist if feature == "watchlist" else current(feature)).values():
                kind = "show" if item.get("type") in {"episode", "season", "show"} else "movie"
                ids = item.get("show_ids") if item.get("type") == "episode" else item.get("ids")
                ids = ids or {}
                namespace = "tmdb" if ids.get("tmdb") else "imdb" if kind == "movie" and ids.get("imdb") else ""
                if not namespace:
                    continue
                key = f"{kind}:{namespace}:{ids[namespace]}"
                items[key] = {"type": kind, "ids": ids, "title": item.get("series_title") or item.get("title") or ""}
                if feature == "watchlist":
                    listed.add(key)
        notes = {}
        offset = 0
        total = None
        while True:
            count, batch = store.notes(offset=offset, limit=200)
            if total is not None and total != count:
                raise RuntimeError("crosswatch_status_snapshot_changed")
            total = count
            for key, note, _ in batch:
                notes[key] = note
            offset += len(batch)
            if offset >= count:
                break
            if not batch:
                raise RuntimeError("crosswatch_status_incomplete_read")
        rows = {}
        for key in items.keys() | notes.keys():
            kind, namespace, ident = key.split(":", 2)
            if namespace not in {"tmdb", "imdb"}:
                continue
            note = notes.get(key)
            item = items.get(key) or {"type": kind, "ids": {namespace: ident}, "title": (note or {}).get("title") or ""}
            protected = {name: (note or {}).get(name) or default for name, default in
                         (("started", ""), ("finished", ""), ("notes", ""), ("tags", []), ("fields", {}))}
            native = str((note or {}).get("status") or "")
            obs = (observation(native, {s.value: s for s in Status}, protected, watchlisted=key in listed) if native else
                   Observation(ReadState.ABSENT, protected_token=digest(protected), watchlisted=key in listed, rewatching=False))
            rows[key] = Record(key, "movies" if kind == "movie" else "shows", item, obs, {"note": note})
        return rows

    def blockers(self, record, desired):
        return ()

    def missing_record(self, source, snapshot):
        ids = source.item.get("ids") or {}
        if source.media == "shows" and any(ids.get(key) for key in ("anilist", "mal", "kitsu", "anidb")):
            return None
        namespace = "tmdb" if ids.get("tmdb") else "imdb" if source.media == "movies" and ids.get("imdb") else ""
        if not namespace:
            return None
        kind = "movie" if source.media == "movies" else "show"
        key = f"{kind}:{namespace}:{ids[namespace]}"
        if key in snapshot:
            return snapshot[key]
        protected = {"started": "", "finished": "", "notes": "", "tags": [], "fields": {}}
        return Record(key, source.media, {**source.item, "type": kind},
                      Observation(ReadState.ABSENT, protected_token=digest(protected), watchlisted=False, rewatching=False), {"note": None})

    def write(self, record, desired):
        if self.cfg.get("_cw_readonly"):
            raise RuntimeError("This tracker is read-only.")
        note = record.raw["note"]
        updated = dict(note or {"type": record.item["type"], "title": record.item.get("title") or "", "tmdb": (record.item.get("ids") or {}).get("tmdb") or ""})
        updated["status"] = desired.value
        self.store().save_note(record.native_id, updated, expected=note)
