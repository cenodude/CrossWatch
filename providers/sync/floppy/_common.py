# providers/sync/floppy/_common.py
# CrossWatch - Floppy sync helpers
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from cw_platform.id_map import canonical_key, ids_from, minimal as id_minimal
from providers.auth._auth_FLOPPY import FloppyAuthError, is_configured as auth_is_configured, provider_block
from providers.sync._mod_common import safe_json


PLANNING = 0
COMPLETED = 3
PAGE_SIZE = 200
MAX_PAGES = 200
MAX_ROWS = 200_000
MAX_SEASONS = 60
MAX_EMPTY_SEASONS = 2

_LAYOUT_CACHE: dict[str, list[tuple[int, int]]] = {}


def int_or_none(value: Any) -> int | None:
    try:
        return int(str(value).strip())
    except Exception:
        return None


def configured_block(cfg: Mapping[str, Any] | None, instance_id: str = "default") -> dict[str, Any]:
    return provider_block(cfg or {}, instance_id)


def is_configured(cfg: Mapping[str, Any] | None, instance_id: str = "default") -> bool:
    return auth_is_configured(configured_block(cfg, instance_id))


def pair_provider_block(adapter: Any) -> dict[str, Any]:
    cfg = getattr(adapter, "config", {}) or {}
    pair = cfg.get("_pair_providers") if isinstance(cfg, Mapping) else None
    raw = pair.get("floppy") if isinstance(pair, Mapping) else None
    if isinstance(raw, Mapping):
        return dict(raw)
    return {}


def watchlist_name(adapter: Any) -> str:
    pair = pair_provider_block(adapter)
    block = configured_block(getattr(adapter, "config", None), getattr(adapter, "instance_id", "default"))
    return str(pair.get("watchlist_name") or block.get("watchlist_name") or "Watchlist").strip() or "Watchlist"


def api_request(adapter: Any, method: str, path: str, *, ok: tuple[int, ...] = (200,), **kwargs: Any) -> Any:
    client = adapter.client
    url = f"{client.api_base}/{str(path or '').strip('/')}"
    headers = dict(kwargs.pop("headers", {}) or {})
    headers.update({"Accept": "application/json", "Authorization": f"Bearer {client.api_token}", "User-Agent": "CrossWatch/1.0"})
    if method.upper() in {"POST", "PUT", "PATCH"}:
        headers.setdefault("Content-Type", "application/json")
    resp = client.session.request(method.upper(), url, headers=headers, timeout=client.timeout, verify=client.verify_ssl, **kwargs)
    status = int(getattr(resp, "status_code", 0) or 0)
    if status in (401, 403):
        raise FloppyAuthError("Floppy rejected the API token", reason="invalid_api_token", status_code=status)
    if status not in ok:
        parts: list[str] = []
        data = safe_json(resp)
        if isinstance(data, Mapping):
            for key in ("detail", "error", "errors"):
                value = data.get(key)
                if value:
                    parts.append(str(value))
        detail = " | ".join(parts)
        raise FloppyAuthError(detail or f"Floppy request failed ({status})", reason=f"http_{status}", status_code=status)
    if status == 204:
        return {}
    data = safe_json(resp)
    return data if data is not None else {}


def api_get(adapter: Any, path: str, **kwargs: Any) -> Any:
    return api_request(adapter, "GET", path, **kwargs)


def api_post(adapter: Any, path: str, **kwargs: Any) -> Any:
    return api_request(adapter, "POST", path, ok=(200, 201), **kwargs)


def api_put(adapter: Any, path: str, **kwargs: Any) -> Any:
    return api_request(adapter, "PUT", path, ok=(200, 201, 204), **kwargs)


def api_patch(adapter: Any, path: str, **kwargs: Any) -> Any:
    return api_request(adapter, "PATCH", path, ok=(200,), **kwargs)


def api_delete(adapter: Any, path: str, **kwargs: Any) -> Any:
    return api_request(adapter, "DELETE", path, ok=(200, 202, 204, 404), **kwargs)


def _page_signature(rows: list[Any]) -> str:
    parts: list[str] = []
    for row in rows:
        if not isinstance(row, Mapping):
            parts.append("")
            continue
        parts.append("|".join(str(row.get(k) or "") for k in ("item_id", "consumption_id", "id", "media_id", "end_date")))
    return "\n".join(parts)


def paged(adapter: Any, path: str, *, params: Mapping[str, Any] | None = None, max_pages: int = MAX_PAGES) -> list[Mapping[str, Any]]:
    out: list[Mapping[str, Any]] = []
    base = dict(params or {})
    limit = max(1, min(PAGE_SIZE, int(base.pop("limit", PAGE_SIZE) or PAGE_SIZE)))
    offset = max(0, int(base.pop("offset", 0) or 0))
    previous: str | None = None
    for _ in range(max(1, int(max_pages or 1))):
        data = api_get(adapter, path, params={**base, "limit": limit, "offset": offset})
        enveloped = isinstance(data, Mapping)
        rows = data.get("results") if enveloped else data
        if not isinstance(rows, list) or not rows:
            break
        before = len(out)
        out.extend(row for row in rows if isinstance(row, Mapping))
        if not enveloped:
            break
        signature = _page_signature(rows)
        if previous is not None and signature == previous:
            del out[before:]
            break
        previous = signature
        count = data.get("count")
        offset += len(rows)
        if len(rows) < limit:
            break
        if count is not None:
            try:
                if offset >= int(count):
                    break
            except Exception:
                pass
        if len(out) >= MAX_ROWS:
            break
    return out


def media_parts_from_item_id(value: Any) -> tuple[str | None, str | None, str | None, int | None, int | None]:
    parts = [p for p in str(value or "").strip("/").split("/") if p]
    if len(parts) < 3:
        return None, None, None, None, None
    typ, source, media_id = parts[0], parts[1], parts[2]
    season = episode = None
    if len(parts) >= 5:
        try:
            season = int(parts[3])
            episode = int(parts[4])
        except Exception:
            season = episode = None
    return typ, source, media_id, season, episode


def reset_layout_cache() -> None:
    _LAYOUT_CACHE.clear()


def _row_episode_number(row: Mapping[str, Any]) -> int | None:
    for key in ("episode_number", "number", "episode"):
        number = int_or_none(row.get(key))
        if number is not None and number > 0:
            return number
    _, _, _, _, episode = media_parts_from_item_id(row.get("item_id"))
    return episode if isinstance(episode, int) and episode > 0 else None


def _season_episodes(adapter: Any, tmdb_id: str, season: int) -> list[int]:
    try:
        rows = paged(adapter, f"media/tv/tmdb/{tmdb_id}/{season}/episodes")
    except Exception:
        return []
    numbers = {n for n in (_row_episode_number(row) for row in rows) if n}
    return sorted(numbers)


def show_layout(adapter: Any, tmdb_id: str) -> list[tuple[int, int]]:
    key = str(tmdb_id or "").strip()
    if not key:
        return []
    cached = _LAYOUT_CACHE.get(key)
    if cached is not None:
        return cached
    layout: list[tuple[int, int]] = []
    empty = 0
    for season in range(1, MAX_SEASONS + 1):
        numbers = _season_episodes(adapter, key, season)
        if not numbers:
            empty += 1
            if empty >= MAX_EMPTY_SEASONS:
                break
            continue
        empty = 0
        layout.extend((season, number) for number in numbers)
    _LAYOUT_CACHE[key] = layout
    return layout


def absolute_to_coord(layout: list[tuple[int, int]], absolute: int) -> tuple[int, int] | None:
    if not layout or absolute is None or absolute <= 0 or absolute > len(layout):
        return None
    return layout[absolute - 1]


def has_coord(layout: list[tuple[int, int]], season: int, episode: int) -> bool:
    try:
        return (int(season), int(episode)) in layout
    except Exception:
        return False


def floppy_type_for_item(item: Mapping[str, Any]) -> str | None:
    typ = str(item.get("type") or id_minimal(item).get("type") or "").strip().lower()
    if typ == "movie":
        return "movie"
    if typ in {"show", "series", "tv"}:
        return "tv"
    if typ == "episode":
        return "episode"
    return None


def tmdb_id_for_item(item: Mapping[str, Any], *, episode_show: bool = False) -> str | None:
    src = item.get("show_ids") if episode_show and isinstance(item.get("show_ids"), Mapping) else item
    ids = ids_from(src if isinstance(src, Mapping) else item)
    val = str(ids.get("tmdb") or "").strip()
    return val or None


def canonical_item_key(item: Mapping[str, Any]) -> str:
    return canonical_key(id_minimal(item))


def item_from_row(row: Mapping[str, Any], *, force_type: str | None = None) -> dict[str, Any] | None:
    item_id = row.get("item_id")
    typ, source, media_id, season, episode = media_parts_from_item_id(item_id)
    inner_raw = row.get("item")
    inner: Mapping[str, Any] = inner_raw if isinstance(inner_raw, Mapping) else {}
    source = str(row.get("source") or source or inner.get("source") or "").strip()
    media_id = str(row.get("media_id") or media_id or inner.get("media_id") or "").strip()
    typ = str(force_type or row.get("media_type") or typ or inner.get("media_type") or "").strip().lower()
    if source != "tmdb" or not media_id:
        return None
    if typ == "episode" or (season is not None and episode is not None):
        if season is None or episode is None:
            return None
        out = {"type": "episode", "show_ids": {"tmdb": media_id}, "season": season, "episode": episode}
    elif typ in {"tv", "show", "series"}:
        out: dict[str, Any] = {"type": "show", "ids": {"tmdb": media_id}}
    elif typ == "movie":
        out = {"type": "movie", "ids": {"tmdb": media_id}}
    else:
        return None
    title = str(row.get("title") or inner.get("title") or "").strip()
    if title:
        out["title"] = title
    image = str(row.get("image") or inner.get("image") or "").strip()
    if image:
        out["poster"] = image
    return out


def ensure_media(adapter: Any, media_type: str, tmdb_id: str, *, payload: Mapping[str, Any] | None = None) -> None:
    track_media(adapter, media_type, tmdb_id, payload=payload)


def track_media(adapter: Any, media_type: str, tmdb_id: str, *, payload: Mapping[str, Any] | None = None, patch_payload: Mapping[str, Any] | None = None) -> None:
    body = {"source": "tmdb", "media_id": str(tmdb_id)}
    body.update({k: v for k, v in dict(payload or {}).items() if v is not None})
    try:
        api_post(adapter, f"media/{media_type}", json=body)
        return
    except FloppyAuthError as exc:
        if getattr(exc, "status_code", None) not in {400, 409}:
            raise
        post_exc = exc
    try:
        patch_body = payload if patch_payload is None else patch_payload
        api_patch(adapter, f"media/{media_type}/tmdb/{tmdb_id}", json={k: v for k, v in dict(patch_body or {}).items() if v is not None})
    except FloppyAuthError as exc:
        if getattr(exc, "status_code", None) == 404:
            raise post_exc
        raise


def unresolved(item: Mapping[str, Any], reason: str) -> dict[str, Any]:
    return {"status": "unresolved", "reason": reason, "item": id_minimal(item), "canonical_key": canonical_item_key(item)}


def failure_reason(exc: Exception) -> str:
    status = getattr(exc, "status_code", None)
    reason = f"floppy_http_{status}" if status else str(getattr(exc, "reason", "") or "floppy_error")
    detail = str(exc or "").strip().replace("\n", " ")
    return f"{reason}:{detail[:160]}" if detail else reason


def rating_number(value: Any) -> float | None:
    try:
        number = float(str(value).strip())
    except Exception:
        return None
    if 10 < number <= 100:
        number /= 10.0
    if not 0.0 <= number <= 10.0:
        return None
    return round(max(0.0, min(10.0, number)), 1)
