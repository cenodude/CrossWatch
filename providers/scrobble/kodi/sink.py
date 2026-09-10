# providers/scrobble/kodi/sink.py
# CrossWatch - Kodi Scrobble Sink
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any

from providers.scrobble._media_server import DeliveryError, MediaServerSink, ids_match
from providers.sync.kodi._common import normalize_uniqueids, path_allowed, watched_at_to_kodi

_PROPS = {
    "movie": ["title", "uniqueid", "file", "playcount", "resume", "runtime"],
    "episode": ["title", "uniqueid", "file", "playcount", "resume", "runtime", "tvshowid", "season", "episode"],
    "show": ["title", "uniqueid"],
}
_METHOD = {"movie": "Movie", "episode": "Episode", "show": "TVShow"}
_ID = {"movie": "movieid", "episode": "episodeid", "show": "tvshowid"}


def _ids(row: dict) -> dict:
    return normalize_uniqueids(row.get("uniqueid") or {})


def _key(row: dict) -> str:
    return f"{row['_kind']}:{row[_ID[row['_kind']]]}"


class KodiSink(MediaServerSink):
    name = "kodi"

    def _new_adapter(self, cfg: dict[str, Any]) -> Any:
        from providers.sync._mod_KODI import KODIModule
        return KODIModule(cfg)

    def _fetch(self, adapter: Any, key: str) -> dict:
        kind, iid = key.split(":", 1)
        method = _METHOD[kind]
        body = adapter.client.rpc(f"VideoLibrary.Get{method}Details", {_ID[kind]: int(iid), "properties": _PROPS[kind]})
        row = (body or {}).get(f"{method.lower()}details")
        if not isinstance(row, dict) or int(row.get(_ID[kind], -1)) != int(iid):
            raise DeliveryError("invalid_destination_item")
        return {**row, "_kind": kind}

    def _query(self, adapter: Any, kind: str, params: dict, *, catalog: bool = False) -> list[dict]:
        method = _METHOD[kind]
        out = []
        seen: set[str] = set()
        limit = 500 if catalog else 100
        properties = [p for p in _PROPS[kind] if p not in {"title", "runtime", "resume", "playcount"}] if catalog else _PROPS[kind]
        for start in range(0, 100_000 if catalog else 100, limit):
            body = adapter.client.rpc(f"VideoLibrary.Get{method}s", {"properties": properties, **params,
                                       "limits": {"start": start, "end": start + limit}})
            if not isinstance(body, dict):
                raise DeliveryError("invalid_destination_catalog")
            rows = body.get(f"{method.lower()}s") or []
            total = int((body.get("limits") or {}).get("total") or 0)
            if not catalog and (total > limit or (not total and len(rows) >= limit)):
                return []
            page_ids = {str(row[_ID[kind]]) for row in rows if isinstance(row, dict) and _ID[kind] in row}
            if rows and not page_ids - seen:
                raise DeliveryError("destination_catalog_pagination_failed")
            seen.update(page_ids)
            out.extend({**row, "_kind": kind} for row in rows if isinstance(row, dict) and _ID[kind] in row)
            if not rows or (total and start + len(rows) >= total) or (not total and len(rows) < limit):
                return out
        if catalog:
            raise DeliveryError("destination_catalog_limit")
        return out

    def _matches(self, adapter: Any, row: dict, item: dict, feature: str) -> bool:
        if row.get("_kind") != item["type"] or not path_allowed(adapter.config, feature, row.get("file"), self._instance_id):
            return False
        own, wanted = _ids(row), item.get("ids") or {}
        if any(wanted[k] != own[k] for k in set(wanted) & set(own)):
            return False
        if ids_match(wanted, own):
            return True
        if (item["type"] != "episode" or not item.get("show_ids") or item.get("season") is None or item.get("episode") is None
                or row.get("season") != item["season"] or row.get("episode") != item["episode"] or row.get("tvshowid") is None):
            return False
        return ids_match(item["show_ids"], _ids(self._fetch(adapter, f"show:{row['tvshowid']}")))

    def _resolve(self, adapter: Any, item: dict, feature: str, profile: str) -> dict:
        candidates = []
        title = item.get("series_title") or item.get("title")
        if title:
            if item["type"] == "movie":
                candidates = self._query(adapter, "movie", {"filter": {"field": "title", "operator": "is", "value": title}})
            elif item.get("season") is not None:
                shows = self._query(adapter, "show", {"filter": {"field": "title", "operator": "is", "value": title}})
                for show in shows:
                    if ids_match(item.get("show_ids") or {}, _ids(show)):
                        candidates.extend(self._query(adapter, "episode", {"tvshowid": show["tvshowid"], "season": item["season"]}))
        found = {_key(r): r for r in candidates if self._matches(adapter, r, item, feature)}
        if not found:
            rows = self._catalog(profile, lambda: [r for kind in ("movie", "show", "episode") for r in self._query(adapter, kind, {}, catalog=True)])
            shows = {r["tvshowid"] for r in rows if r["_kind"] == "show" and ids_match(item.get("show_ids") or {}, _ids(r))}
            candidates = [r for r in rows if r["_kind"] == item["type"] and (ids_match(item.get("ids") or {}, _ids(r))
                          or (item["type"] == "episode" and r.get("tvshowid") in shows
                              and r.get("season") == item.get("season") and r.get("episode") == item.get("episode")))]
            found = {_key(r): r for r in candidates if self._matches(adapter, r, item, feature)}
        if len(found) != 1:
            raise DeliveryError("ambiguous_ids" if found else "unmatched_in_kodi")
        row = self._fetch(adapter, next(iter(found)))
        if not self._matches(adapter, row, item, feature):
            raise DeliveryError("unmatched_in_kodi")
        return row

    def _profile(self, adapter: Any) -> str:
        body = adapter.client.rpc("Profiles.GetCurrentProfile")
        profile = (body or {}).get("profile") or {}
        if not isinstance(profile, dict) or not profile.get("label"):
            raise DeliveryError("unknown_destination_profile")
        return str(profile["label"])

    def _delivery_context(self, adapter: Any) -> str:
        adapter._cw_sink_profile = self._profile(adapter)
        return adapter._cw_sink_profile

    def _deliver(self, adapter: Any, item: dict[str, Any], complete: bool, progress: float) -> dict[str, Any]:
        profile = adapter._cw_sink_profile
        feature = "history" if complete else "progress"
        row = self._cached_target(item, [feature, profile], fetch=lambda key: self._fetch(adapter, key),
                                 matches=lambda r: self._matches(adapter, r, item, feature),
                                 resolve=lambda: self._resolve(adapter, item, feature, profile), key_of=_key)
        players = adapter.client.rpc("Player.GetActivePlayers")
        if not isinstance(players, list):
            raise DeliveryError("invalid_destination_sessions")
        for player in players:
            if player.get("type") != "video":
                continue
            playing = (adapter.client.rpc("Player.GetItem", {"playerid": player["playerid"]}) or {}).get("item") or {}
            if playing.get("id") is not None and playing.get("type") == row["_kind"] and int(playing["id"]) == int(row[_ID[row["_kind"]]]):
                return {"ok": False, "retryable": True, "error": "active_destination_session"}
        watched = int(row.get("playcount") or 0) > 0
        total = float((row.get("resume") or {}).get("total") or row.get("runtime") or 0)
        if complete:
            payload: dict[str, Any] = {"resume": {"position": 0.0, "total": max(0.0, total)}}
            if not watched:
                payload.update(playcount=1, lastplayed=watched_at_to_kodi(None))
        else:
            if watched:
                return {"ok": True, "skipped": True, "reason": "destination_already_watched"}
            if total <= 0:
                raise DeliveryError("missing_destination_duration")
            if progress <= 0:
                return {"ok": True, "skipped": True, "reason": "no_progress"}
            payload = {"resume": {"position": total * progress / 100.0, "total": total}}
        if self._profile(adapter) != profile:
            raise DeliveryError("destination_profile_changed")
        method = adapter.client.set_movie if item["type"] == "movie" else adapter.client.set_episode
        if method(int(row[_ID[row["_kind"]]]), payload) != "OK":
            raise DeliveryError("kodi_write_not_acknowledged")
        return {"ok": True}
