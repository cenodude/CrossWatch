# _metaAPI.py
# CrossWatch - Metadata API for media information
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable

import requests
from fastapi import APIRouter, Body, Path as FPath, Query
from fastapi.encoders import jsonable_encoder
from fastapi.responses import (
    FileResponse,
    HTMLResponse,
    JSONResponse,
    PlainTextResponse,
)
from pydantic import BaseModel

from cw_platform.config_base import load_config

router = APIRouter(tags=["metadata"])

try:
    from providers.metadata.registry import (
        metadata_providers_html,
        metadata_providers_manifests,
    )
except Exception:
    metadata_providers_html = (
        lambda: "<div class='sub'>No metadata providers found.</div>"
    )
    metadata_providers_manifests = lambda: []


def _env() -> tuple[Any | None, Path, Callable[..., Any]]:
    try:
        import crosswatch as CW

        return CW._METADATA, Path(CW.CACHE_DIR or "./.cache"), CW._load_state
    except Exception:
        return None, Path("./.cache"), (lambda: {})


def _norm_media_type(x: str | None) -> str:
    t = (x or "").strip().lower()
    if t in {"tv", "show", "shows", "series", "season", "episode"}:
        return "show"
    if t in {"movie", "movies", "film", "films"}:
        return "movie"
    return "movie"


def _shorten(txt: str, limit: int = 280) -> str:
    if not txt or len(txt) <= limit:
        return txt or ""
    cut = txt[:limit].rsplit(" ", 1)[0].rstrip(",.;:!-–—")
    return f"{cut}…"


def _cfg_meta_ttl_secs() -> int:
    try:
        md = (load_config() or {}).get("metadata") or {}
        return max(1, int(md.get("ttl_hours", 6))) * 3600
    except Exception:
        return 6 * 3600


def _meta_cache_enabled() -> bool:
    try:
        md = (load_config() or {}).get("metadata") or {}
        return bool(md.get("meta_cache_enable", True))
    except Exception:
        return True


def _meta_cache_dir() -> Path:
    _, base, _ = _env()
    d = base / "meta"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _meta_cache_path(entity: str, tmdb_id: str | int, locale: str | None) -> Path:
    t = "movie" if str(entity).lower() == "movie" else "show"
    loc = (locale or "en-US").replace("/", "_")
    sub = _meta_cache_dir() / t
    sub.mkdir(parents=True, exist_ok=True)
    return sub / f"{tmdb_id}.{loc}.json"


def _need_satisfied(meta: dict[str, Any], need: dict[str, Any] | None) -> bool:
    if not need:
        return True
    if not isinstance(meta, dict):
        return False

    def has_img(k: str) -> bool:
        return bool(((meta.get("images") or {}).get(k) or []))

    def has_nested(k: str) -> bool:
        det = meta.get("detail") or {}
        if k == "genres":
            return bool(meta.get("genres") or det.get("genres"))
        if k == "release":
            return bool(
                meta.get("release")
                or det.get("release_date")
                or det.get("first_air_date")
            )
        if k == "title":
            return bool(
                meta.get("title") or det.get("title") or det.get("name")
            )
        if k == "year":
            return bool(
                meta.get("year")
                or det.get("year")
                or det.get("release_year")
                or det.get("first_air_year")
            )
        return bool(meta.get(k))

    for k, v in (need or {}).items():
        if not v:
            continue
        if k in {"poster", "backdrop", "logo"}:
            if not has_img(k):
                return False
        elif not has_nested(k):
            return False
    return True


def _read_meta_cache(p: Path) -> dict[str, Any] | None:
    try:
        if not p.exists():
            return None
        data = json.loads(p.read_text("utf-8"))
        if not isinstance(data, dict):
            return None
        if (time.time() - float(data.get("fetched_at") or 0)) > _cfg_meta_ttl_secs():
            return None
        return data
    except Exception:
        return None


def _write_meta_cache(p: Path, payload: dict[str, Any]) -> None:
    try:
        tmp = p.with_suffix(p.suffix + ".tmp")
        data = dict(payload)
        data["fetched_at"] = time.time()
        tmp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
        tmp.replace(p)
    except Exception:
        pass


def _prune_meta_cache_if_needed() -> None:
    try:
        md = (load_config() or {}).get("metadata") or {}
        cap_mb = int(md.get("meta_cache_max_mb", 0))
        if cap_mb <= 0:
            return
        root = _meta_cache_dir()
        files = list(root.rglob("*.json"))
        total = sum(f.stat().st_size for f in files)
        cap = cap_mb * 1024 * 1024
        if total <= cap:
            return
        files.sort(key=lambda f: f.stat().st_mtime)
        target = int(cap * 0.9)
        for f in files:
            try:
                total -= f.stat().st_size
                f.unlink(missing_ok=True)
            except Exception:
                pass
            if total <= target:
                break
    except Exception:
        pass


def _ttl_bucket(seconds: int) -> int:
    return int(time.time() // max(1, seconds))


@lru_cache(maxsize=4096)
def _resolve_tmdb_cached(
    ttl_key: int,
    entity: str,
    tmdb_id: str,
    locale: str | None,
    need_key: tuple[str, ...],
) -> dict[str, Any]:
    _METADATA, _, _ = _env()
    if _METADATA is None:
        return {}
    need = {k: True for k in need_key} if need_key else None
    try:
        return (
            _METADATA.resolve(
                entity=entity,
                ids={"tmdb": tmdb_id},
                locale=locale,
                need=need,
            )
            or {}
        )
    except Exception:
        return {}


def get_meta(
    api_key: str,  # kept for compatibility, not used directly
    typ: str,
    tmdb_id: str | int,
    cache_dir: Path | str,
    *,
    need: dict[str, Any] | None = None,
    locale: str | None = None,
) -> dict[str, Any]:
    entity = "movie" if str(typ).lower() == "movie" else "show"
    eff_need = need or {"poster": True, "backdrop": True, "logo": False}
    need_key = tuple(sorted(k for k, v in eff_need.items() if v))
    eff_locale = locale

    if _meta_cache_enabled():
        p = _meta_cache_path(entity, tmdb_id, eff_locale or "en-US")
        cached = _read_meta_cache(p)
        if cached and _need_satisfied(cached, eff_need):
            return cached

    ttl_key = _ttl_bucket(_cfg_meta_ttl_secs())
    res = _resolve_tmdb_cached(
        ttl_key, entity, str(tmdb_id), eff_locale, need_key
    ) or {}

    if res and _meta_cache_enabled():
        try:
            payload = dict(res)
            payload["locale"] = eff_locale or payload.get("locale") or None
            _write_meta_cache(
                _meta_cache_path(entity, tmdb_id, eff_locale or "en-US"),
                payload,
            )
            _prune_meta_cache_if_needed()
        except Exception:
            pass

    return res or {}


def get_runtime(
    api_key: str,
    typ: str,
    tmdb_id: str | int,
    cache_dir: Path | str,
) -> int | None:
    meta = get_meta(
        api_key,
        typ,
        tmdb_id,
        cache_dir,
        need={"runtime_minutes": True},
    )
    return meta.get("runtime_minutes")


def _cache_download(
    url: str,
    dest_path: Path,
    timeout: float = 15.0,
) -> tuple[Path, str]:
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    if not dest_path.exists():
        r = requests.get(url, stream=True, timeout=timeout)
        r.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(64 * 1024):
                if chunk:
                    f.write(chunk)
    ext = dest_path.suffix.lower()
    if ext in (".jpg", ".jpeg"):
        mime = "image/jpeg"
    elif ext == ".png":
        mime = "image/png"
    else:
        mime = "application/octet-stream"
    return dest_path, mime


def get_poster_file(
    api_key: str,
    typ: str,
    tmdb_id: str | int,
    size: str,
    cache_dir: Path | str,
) -> tuple[str, str]:
    meta = get_meta(
        api_key,
        typ,
        tmdb_id,
        cache_dir,
        need={"poster": True},
    ) or {}
    posters = ((meta.get("images") or {}).get("poster") or [])
    if not posters:
        raise FileNotFoundError("No poster found")
    src_url = posters[0]["url"]
    ext = ".jpg" if (".jpg" in src_url or ".jpeg" in src_url) else ".png"
    size_tag = (size or "w780").lower().strip()
    _, base, _ = _env()
    cache_root = Path(cache_dir or (base / "posters"))
    if cache_root == base:
        cache_root = base / "posters"
    dest = cache_root / f"{typ}_{tmdb_id}_{size_tag}{ext}"
    path, mime = _cache_download(src_url, dest)
    return str(path), mime


def _tmdb_external_ids(entity: str, tmdb_id: str | int) -> dict[str, str]:
    try:
        cfg = load_config() or {}
        tmdb_cfg = cfg.get("tmdb") or {}
        api_key = str(tmdb_cfg.get("api_key") or "").strip()
        if not api_key:
            return {}

        base = "movie" if str(entity).lower() == "movie" else "tv"
        url = f"https://api.themoviedb.org/3/{base}/{tmdb_id}/external_ids"

        r = requests.get(url, params={"api_key": api_key}, timeout=8)
        r.raise_for_status()
        data = r.json() or {}
    except Exception:
        return {}

    out: dict[str, str] = {}
    imdb_id = data.get("imdb_id")
    if imdb_id:
        out["imdb"] = imdb_id

    tvdb_id = data.get("tvdb_id")
    if tvdb_id:
        out["tvdb"] = str(tvdb_id)

    return out


@router.get("/api/metadata/providers", tags=["metadata"])
def api_metadata_providers() -> JSONResponse:
    return JSONResponse(jsonable_encoder(metadata_providers_manifests()))


@router.get("/api/metadata/providers/html", tags=["metadata"])
def api_metadata_providers_html() -> HTMLResponse:
    return HTMLResponse(metadata_providers_html())


@router.get("/art/tmdb/{typ}/{tmdb_id}", tags=["metadata"])
def api_tmdb_art(
    typ: str = FPath(...),
    tmdb_id: int = FPath(...),
    size: str = Query("w342"),
):
    t = typ.lower()
    if t == "show":
        t = "tv"
    if t not in {"movie", "tv"}:
        return PlainTextResponse("Bad type", status_code=400)

    cfg = load_config() or {}
    api_key = str(((cfg.get("tmdb") or {}).get("api_key") or "")).strip()
    if not api_key:
        return PlainTextResponse("TMDb key missing", status_code=404)

    try:
        _, base, _ = _env()
        local_path, mime = get_poster_file(api_key, t, tmdb_id, size, base)
        return FileResponse(
            str(local_path),
            media_type=mime,
            headers={
                "Cache-Control": "public, max-age=86400, stale-while-revalidate=86400"
            },
        )
    except Exception as e:
        return PlainTextResponse(
            f"Poster not available: {e}",
            status_code=404,
        )


class MetadataResolveIn(BaseModel):
    entity: str | None = None
    ids: dict[str, Any]
    locale: str | None = None
    need: dict[str, Any] | None = None
    strategy: str | None = None  # e.g., first_success


@router.get("/api/metadata/search", tags=["metadata"])
def api_metadata_search(
    q: str = Query(..., min_length=2),
    typ: str = Query("movie"),
    year: int | None = Query(None),
    limit: int = Query(10, ge=1, le=20),
) -> JSONResponse:
    cfg = load_config() or {}
    api_key = ((cfg.get("tmdb") or {}).get("api_key") or "").strip()
    if not api_key:
        return JSONResponse(
            {"ok": False, "error": "TMDb key missing"},
            status_code=200,
        )

    entity = _norm_media_type(typ)
    base = "movie" if entity == "movie" else "tv"

    url = f"https://api.themoviedb.org/3/search/{base}"
    params: dict[str, Any] = {
        "api_key": api_key,
        "query": q,
        "include_adult": False,
        "language": (cfg.get("ui") or {}).get("locale") or "en-US",
        "page": 1,
    }
    if year:
        if base == "movie":
            params["year"] = year
        else:
            params["first_air_date_year"] = year

    try:
        r = requests.get(url, params=params, timeout=8)
        r.raise_for_status()
        data = r.json() or {}
    except Exception as e:
        return JSONResponse(
            {"ok": False, "error": f"search failed: {e}"},
            status_code=200,
        )

    out: list[dict[str, Any]] = []
    for raw in (data.get("results") or [])[:limit]:
        tmdb_id = raw.get("id")
        if not tmdb_id:
            continue
        title = raw.get("title") or raw.get("name") or ""
        date = raw.get("release_date") or raw.get("first_air_date") or ""
        year_val = int(date.split("-", 1)[0]) if date else None

        out.append(
            {
                "tmdb": tmdb_id,
                "type": entity,
                "title": title,
                "year": year_val,
                "overview": _shorten(raw.get("overview") or "", 240),
                "poster_path": raw.get("poster_path"),
            }
        )

    return JSONResponse({"ok": True, "results": out})


@router.post("/api/metadata/resolve", tags=["metadata"])
def api_metadata_resolve(payload: MetadataResolveIn = Body(...)) -> JSONResponse:
    _METADATA, _, _ = _env()
    if _METADATA is None:
        return JSONResponse(
            {"ok": False, "error": "MetadataManager not available"},
            status_code=500,
        )
    try:
        entity = _norm_media_type(payload.entity)
        base_ids: dict[str, Any] = payload.ids or {}
        res = (
            _METADATA.resolve(
                entity=entity,
                ids=base_ids,
                locale=payload.locale,
                need=payload.need,
                strategy=payload.strategy or "first_success",
            )
            or {}
        )

        if not isinstance(res, dict):
            res = {}
        res.setdefault("type", entity)

        ids = res.get("ids") or {}
        if not isinstance(ids, dict):
            ids = {}
        tmdb_id = None
        if isinstance(base_ids, dict):
            tmdb_id = base_ids.get("tmdb")
        if not tmdb_id:
            tmdb_id = ids.get("tmdb")

        if tmdb_id and not ids.get("imdb"):
            extra_ids = _tmdb_external_ids(entity, tmdb_id)
            imdb_id = extra_ids.get("imdb")
            if imdb_id:
                ids["imdb"] = imdb_id
            tvdb_id = extra_ids.get("tvdb")
            if tvdb_id and not ids.get("tvdb"):
                ids["tvdb"] = tvdb_id

        res["ids"] = ids

        return JSONResponse({"ok": True, "result": res})
    except Exception as e:
        return JSONResponse(
            {"ok": False, "error": str(e)},
            status_code=500,
        )


@router.post("/api/metadata/bulk", tags=["metadata"])
def api_metadata_bulk(
    payload: dict[str, Any] = Body(
        ..., description="items[] with {type|entity|media_type, tmdb}; need{} optional"
    ),
    overview: str | None = Query(
        "full",
        description="none|short|full",
    ),
    locale: str | None = Query(
        None,
        description="e.g., 'nl-NL'",
    ),
) -> JSONResponse:
    cfg = load_config() or {}
    _METADATA, base_cache, _load_state = _env()
    st = _load_state()
    api_key = ((cfg.get("tmdb") or {}).get("api_key") or "").strip()
    md_cfg = (cfg.get("metadata") or {})
    bulk_max = int(md_cfg.get("bulk_max", 300))
    default_workers = 6

    items = (payload or {}).get("items") or []
    if not isinstance(items, list) or not items:
        return JSONResponse(
            {
                "ok": False,
                "error": "Body must include a non-empty 'items' array.",
                "missing_tmdb_key": not bool(api_key),
            },
            status_code=200,
        )
    items = items[:bulk_max]

    req_need = (payload or {}).get("need") or {
        "overview": True,
        "tagline": True,
        "runtime_minutes": True,
        "score": True,
    }
    req_need = dict(req_need, overview=(overview != "none"))
    eff_locale = (
        locale
        or md_cfg.get("locale")
        or (cfg.get("ui") or {}).get("locale")
        or None
    )

    try:
        requested_workers = int((payload or {}).get("concurrency") or default_workers)
    except Exception:
        requested_workers = default_workers
    workers = max(1, min(requested_workers, 12))

    def _fetch_one(item: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        typ = _norm_media_type(
            item.get("type")
            or item.get("entity")
            or item.get("media_type")
        )
        tmdb_id = str(item.get("tmdb") or item.get("id") or "").strip()
        key = f"{typ}:{tmdb_id or 'UNKNOWN'}"
        if not tmdb_id:
            return key, {"ok": False, "error": "missing tmdb id"}
        item["type"] = typ
        try:
            meta = (
                get_meta(
                    api_key,
                    typ,
                    tmdb_id,
                    base_cache,
                    need=req_need,
                    locale=eff_locale,
                )
                or {}
            )
        except Exception as e:
            return key, {"ok": False, "error": f"resolver failed: {e}"}
        if not meta:
            return key, {"ok": False, "error": "no metadata"}
        keep = {
            "type",
            "title",
            "year",
            "ids",
            "runtime_minutes",
            "overview",
            "tagline",
            "images",
            "genres",
            "videos",
            "score",
            "certification",
            "release",
            "detail",
        }
        out: dict[str, Any] = {"type": meta.get("type") or typ}
        for k in keep:
            if k != "type" and k in meta:
                out[k] = meta[k]
        if overview == "short" and out.get("overview"):
            out["overview"] = _shorten(out["overview"], 280)
        if "score" not in out:
            va: Any = (out.get("detail") or {}).get("vote_average") or meta.get(
                "vote_average"
            )
            try:
                out["score"] = int(round(float(va) * 10))
            except Exception:
                pass

        return key, {"ok": True, "meta": out}

    results: dict[str, Any] = {}
    fetched = 0
    if len(items) <= 8:
        for it in items:
            k, v = _fetch_one(it)
            results[k] = v
            if v.get("ok"):
                fetched += 1
    else:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            for fut in as_completed([ex.submit(_fetch_one, it) for it in items]):
                try:
                    k, v = fut.result()
                except Exception as e:
                    k, v = "unknown:0", {
                        "ok": False,
                        "error": f"worker error: {e}",
                    }
                results[k] = v
                if v.get("ok"):
                    fetched += 1

    return JSONResponse(
        {
            "ok": True,
            "count": len(items),
            "fetched": fetched,
            "missing_tmdb_key": not bool(api_key),
            "results": results,
            "last_sync_epoch": st.get("last_sync_epoch")
            if isinstance(st, dict)
            else None,
        },
        status_code=200,
    )
