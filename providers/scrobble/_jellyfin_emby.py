# providers/scrobble/_jellyfin_emby.py
# CrossWatch - Jellyfin and Emby Scrobble Delivery
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from importlib import import_module
from typing import Any
from urllib.parse import quote

from providers.scrobble._media_server import DeliveryError, MediaServerSink, PUBLIC_IDS, ids_match
from providers.sync.jellyfin import _routes as jf_routes
from providers.sync.jellyfin._common import _ids_from_provider_ids

_FIELDS = "ProviderIds,ParentId"


def _ids(row: dict) -> dict[str, str]:
    return {key: str(value) for key, value in _ids_from_provider_ids(row.get("ProviderIds")).items()
            if key in PUBLIC_IDS and value}


class JellyfinEmbySink(MediaServerSink):
    def _new_adapter(self, cfg: dict[str, Any]) -> Any:
        module = import_module(f"providers.sync._mod_{self.name.upper()}")
        cfg = {**cfg, "auth": {k: v for k, v in (cfg.get("auth") or {}).items() if k != self.name}}
        return getattr(module, f"{self.name.upper()}Module")(cfg)

    def _path(self, adapter: Any, item_id: str = "", operation: str = "items") -> tuple[str, dict]:
        uid = str(adapter.cfg.user_id)
        iid = quote(str(item_id), safe="")
        if self.name == "jellyfin":
            path = {"items": jf_routes.items, "played": jf_routes.played, "data": jf_routes.user_data}[operation](iid or None)
            return path, jf_routes.user_params(uid)
        suffix = {"items": f"/Items{('/' + iid) if iid else ''}", "played": f"/PlayedItems/{iid}", "data": f"/Items/{iid}/UserData"}[operation]
        return f"/Users/{quote(uid, safe='')}{suffix}", {}

    def _body(self, response: Any, *, read: bool = True) -> Any:
        status = int(getattr(response, "status_code", 0) or 0)
        if status not in (200, 204):
            raise DeliveryError(f"{self.name}_http_{status}")
        return response.json() if read and status != 204 else None

    def _fetch(self, adapter: Any, iid: str) -> dict:
        path, params = self._path(adapter, iid)
        row = self._body(adapter.client.get(path, params={**params, "Fields": _FIELDS, "EnableUserData": True}))
        if not isinstance(row, dict) or str(row.get("Id") or "") != str(iid):
            raise DeliveryError("invalid_destination_item")
        return row

    def _query(self, adapter: Any, params: dict, *, catalog: bool = False) -> list[dict]:
        path, user = self._path(adapter)
        out = []
        seen: set[str] = set()
        limit = 500 if catalog else 100
        for start in range(0, 100_000 if catalog else 100, limit):
            body = self._body(adapter.client.get(path, params={**user, "Recursive": True,
                "Fields": _FIELDS,
                "EnableUserData": not catalog, "EnableImages": False,
                **params, "StartIndex": start, "Limit": limit, "EnableTotalRecordCount": True}))
            if not isinstance(body, dict) or not isinstance(body.get("Items"), list):
                raise DeliveryError("invalid_destination_catalog")
            rows = body["Items"]
            total = int(body.get("TotalRecordCount") or 0)
            if not catalog and (total > limit or (not total and len(rows) >= limit)):
                return []
            page_ids = {str(row["Id"]) for row in rows if isinstance(row, dict) and row.get("Id")}
            if rows and not page_ids - seen:
                raise DeliveryError("destination_catalog_pagination_failed")
            seen.update(page_ids)
            out.extend(row for row in rows if isinstance(row, dict) and row.get("Id"))
            if not rows or (total and start + len(rows) >= total) or (not total and len(rows) < limit):
                return out
        if catalog:
            raise DeliveryError("destination_catalog_limit")
        return out

    def _in_scope(self, adapter: Any, row: dict, allowed: set[str]) -> bool:
        if not allowed:
            return True
        libraries = {str(row[k]) for k in ("LibraryId", "CollectionFolderId") if row.get(k)}
        libraries.update(str(x) for x in row.get("AncestorIds") or [])
        if libraries:
            return bool(libraries & allowed)
        ancestors = self._body(adapter.client.get(f"/Items/{quote(str(row['Id']), safe='')}/Ancestors",
                                                  params={"userId": str(adapter.cfg.user_id)}))
        if not isinstance(ancestors, list):
            raise DeliveryError("invalid_destination_ancestors")
        return any(str(x.get("Id")) in allowed for x in ancestors if isinstance(x, dict))

    def _matches(self, adapter: Any, row: dict, item: dict, allowed: set[str]) -> bool:
        if str(row.get("Type") or "").lower() != item["type"] or not self._in_scope(adapter, row, allowed):
            return False
        own, wanted = _ids(row), item.get("ids") or {}
        if any(wanted[k] != own[k] for k in set(wanted) & set(own)):
            return False
        if ids_match(wanted, own):
            return True
        show_ids = item.get("show_ids") or {}
        if (item["type"] != "episode" or not show_ids or item.get("season") is None or item.get("episode") is None
                or row.get("ParentIndexNumber") != item["season"] or row.get("IndexNumber") != item["episode"] or not row.get("SeriesId")):
            return False
        return ids_match(show_ids, _ids(self._fetch(adapter, str(row["SeriesId"]))))

    def _resolve(self, adapter: Any, item: dict, allowed: set[str]) -> dict:
        candidates: dict[str, dict] = {}
        groups = [(item.get("ids") or {}, "Episode" if item["type"] == "episode" else "Movie")]
        if item["type"] == "episode":
            groups.append((item.get("show_ids") or {}, "Series"))
        if self.name == "emby":
            for ids, kind in groups:
                for key, value in ids.items():
                    for row in self._query(adapter, {"IncludeItemTypes": kind, "AnyProviderIdEquals": f"{key}.{value}"}):
                        candidates[str(row["Id"])] = row
        title = item.get("series_title") or item.get("title")
        if not candidates and title:
            for row in self._query(adapter, {"IncludeItemTypes": "Series" if item["type"] == "episode" else "Movie", "SearchTerm": title}):
                candidates[str(row["Id"])] = row

        def matches(rows: list[dict]) -> dict[str, dict]:
            result = {}
            for row in rows:
                if row.get("Type") == "Series":
                    if not ids_match(item.get("show_ids") or {}, _ids(row)) or item.get("season") is None or item.get("episode") is None:
                        continue
                    episodes = self._query(adapter, {"ParentId": str(row["Id"]), "IncludeItemTypes": "Episode",
                                                    "ParentIndexNumber": item["season"], "IndexNumber": item["episode"]})
                    for episode in episodes:
                        if self._matches(adapter, episode, item, allowed):
                            result[str(episode["Id"])] = episode
                elif self._matches(adapter, row, item, allowed):
                    result[str(row["Id"])] = row
            return result

        found = matches(list(candidates.values()))
        if not found:
            rows = self._catalog("library", lambda: self._query(adapter, {"IncludeItemTypes": "Movie,Episode,Series"}, catalog=True))
            shows = {str(r["Id"]) for r in rows if r.get("Type") == "Series" and ids_match(item.get("show_ids") or {}, _ids(r))}
            catalog_candidates = [r for r in rows if ids_match(item.get("ids") or {}, _ids(r))
                                  or (item["type"] == "episode" and r.get("Type") == "Episode"
                                      and str(r.get("SeriesId")) in shows
                                      and r.get("ParentIndexNumber") == item.get("season") and r.get("IndexNumber") == item.get("episode"))]
            found = matches(catalog_candidates)
        if len(found) != 1:
            raise DeliveryError("ambiguous_ids" if found else f"unmatched_in_{self.name}")
        row = self._fetch(adapter, next(iter(found)))
        if not self._matches(adapter, row, item, allowed):
            raise DeliveryError(f"unmatched_in_{self.name}")
        return row

    def _deliver(self, adapter: Any, item: dict[str, Any], complete: bool, progress: float) -> dict[str, Any]:
        helpers = import_module(f"providers.sync.{self.name}._common")
        scope = getattr(helpers, "jf_selected_library_ids" if self.name == "jellyfin" else "emby_selected_library_ids")
        allowed = scope(adapter.cfg, "history" if complete else "progress")
        row = self._cached_target(item, sorted(allowed), fetch=lambda iid: self._fetch(adapter, iid),
                                 matches=lambda r: self._matches(adapter, r, item, allowed),
                                 resolve=lambda: self._resolve(adapter, item, allowed), key_of=lambda r: r["Id"])
        sessions = self._body(adapter.client.get("/Sessions"))
        if not isinstance(sessions, list):
            raise DeliveryError("invalid_destination_sessions")
        if any(str(s.get("UserId") or "") == str(adapter.cfg.user_id)
               and str((s.get("NowPlayingItem") or {}).get("Id") or "") == str(row["Id"]) for s in sessions if isinstance(s, dict)):
            return {"ok": False, "retryable": True, "error": "active_destination_session"}
        data = row.get("UserData")
        if not isinstance(data, dict):
            raise DeliveryError("missing_destination_user_data")
        watched = bool(data.get("Played") or data.get("IsPlayed") or int(data.get("PlayCount") or 0) > 0)
        if complete and not watched:
            path, params = self._path(adapter, row["Id"], "played")
            self._body(adapter.client.post(path, params=params), read=False)
            data = self._fetch(adapter, row["Id"]).get("UserData") or {}
        elif not complete and watched:
            return {"ok": True, "skipped": True, "reason": "destination_already_watched"}
        if complete:
            ticks = 0
        else:
            duration = int(row.get("RunTimeTicks") or 0)
            if duration <= 0:
                raise DeliveryError("missing_destination_duration")
            ticks = round(duration * progress / 100.0)
            if ticks <= 0:
                return {"ok": True, "skipped": True, "reason": "no_progress"}
        if int(data.get("PlaybackPositionTicks") or 0) != ticks:
            if self.name == "jellyfin" and not adapter.client.progress_write_supported()[0]:
                raise DeliveryError("jellyfin_progress_write_unsupported")
            payload = {"PlaybackPositionTicks": ticks}
            if self.name == "emby":
                payload = {**{k: data[k] for k in ("Played", "PlayCount", "IsFavorite", "LastPlayedDate", "Rating") if k in data}, **payload}
            path, params = self._path(adapter, row["Id"], "data")
            self._body(adapter.client.post(path, params=params, json=payload), read=False)
        return {"ok": True}
