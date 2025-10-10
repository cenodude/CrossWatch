# _metaAPI.py

# --- stdlib ---
from typing import Dict, Any, Tuple, Optional, List
from functools import lru_cache
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import time, json, requests

# --- third-party ---
from fastapi import APIRouter, Query, Path as FPath, Body
from fastapi.responses import FileResponse, PlainTextResponse, JSONResponse, HTMLResponse
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel

# --- app config ---
from cw_platform.config_base import load_config

# Router: metadata surface
router = APIRouter(tags=["metadata"])

# Provider registry (best-effort; keeps API up even if providers are missing)
try:
    from providers.metadata.registry import metadata_providers_html, metadata_providers_manifests
except Exception:
    metadata_providers_html = lambda: "<div class='sub'>No metadata providers found.</div>"
    metadata_providers_manifests = lambda: []

# ----- Public: Providers -----

@router.get("/api/metadata/providers", tags=["metadata"])
def api_metadata_providers():
    # JSON-safe provider manifests
    return JSONResponse(jsonable_encoder(metadata_providers_manifests()))

@router.get("/api/metadata/providers/html", tags=["metadata"])
def api_metadata_providers_html():
    # Simple HTML listing
    return HTMLResponse(metadata_providers_html())

# ----- Runtime bridge (lazy to avoid import cycles) -----

def _env():
    try:
        import crosswatch as CW
        return CW._METADATA, Path(CW.CACHE_DIR or "./.cache"), CW._load_state
    except Exception:
        return None, Path("./.cache"), (lambda: {})

# ----- Small utils -----

def _norm_media_type(x: Optional[str]) -> str:
    t = (x or "").strip().lower()
    if t in {"tv","show","shows","series","season","episode"}: return "show"
    if t in {"movie","movies","film","films"}:                 return "movie"
    return "movie"

def _shorten(txt: str, limit: int = 280) -> str:
    if not txt or len(txt) <= limit: return txt or ""
    cut = txt[:limit].rsplit(" ", 1)[0].rstrip(",.;:!-–—")
    return f"{cut}…"

# ----- Cache/TTL helpers -----

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
    d = base / "meta"; d.mkdir(parents=True, exist_ok=True); return d

def _meta_cache_path(entity: str, tmdb_id: str | int, locale: str | None) -> Path:
    t = "movie" if str(entity).lower() == "movie" else "show"
    loc = (locale or "en-US").replace("/", "_")
    sub = _meta_cache_dir() / t; sub.mkdir(parents=True, exist_ok=True)
    return sub / f"{tmdb_id}.{loc}.json"

def _need_satisfied(meta: dict, need: dict | None) -> bool:
    if not need: return True
    if not isinstance(meta, dict): return False
    def has_img(k: str) -> bool: return bool(((meta.get("images") or {}).get(k) or []))
    for k, v in need.items():
        if not v: continue
        if k in {"poster","backdrop","logo"}:
            if not has_img(k): return False
        elif not meta.get(k):
            return False
    return True

def _read_meta_cache(p: Path) -> dict | None:
    try:
        if not p.exists(): return None
        data = json.loads(p.read_text("utf-8"))
        if not isinstance(data, dict): return None
        if (time.time() - float(data.get("fetched_at") or 0)) > _cfg_meta_ttl_secs(): return None
        return data
    except Exception:
        return None

def _write_meta_cache(p: Path, payload: dict) -> None:
    try:
        tmp = p.with_suffix(p.suffix + ".tmp")
        data = dict(payload); data["fetched_at"] = time.time()
        tmp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
        tmp.replace(p)
    except Exception:
        pass

def _prune_meta_cache_if_needed() -> None:
    try:
        md = (load_config() or {}).get("metadata") or {}
        cap_mb = int(md.get("meta_cache_max_mb", 0))
        if cap_mb <= 0: return
        root = _meta_cache_dir()
        files = list(root.rglob("*.json"))
        total = sum(f.stat().st_size for f in files)
        cap = cap_mb * 1024 * 1024
        if total <= cap: return
        files.sort(key=lambda f: f.stat().st_mtime)
        target = int(cap * 0.9)
        for f in files:
            try:
                total -= f.stat().st_size
                f.unlink(missing_ok=True)
            except Exception:
                pass
            if total <= target: break
    except Exception:
        pass

# ----- Manager bridge -----

def _ttl_bucket(seconds: int) -> int:
    return int(time.time() // max(1, seconds))

@lru_cache(maxsize=4096)
def _resolve_tmdb_cached(ttl_key: int, entity: str, tmdb_id: str, locale: str | None, need_key: tuple) -> dict:
    _METADATA, _, _ = _env()
    if _METADATA is None: return {}
    need = {k: True for k in need_key} if need_key else None
    try:
        return _METADATA.resolve(entity=entity, ids={"tmdb": tmdb_id}, locale=locale, need=need) or {}
    except Exception:
        return {}

# ----- Public helpers (import-safe) -----

def get_meta(api_key: str, typ: str, tmdb_id: str | int, cache_dir: Path | str, *, need: dict | None = None, locale: str | None = None) -> dict:
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
    res = _resolve_tmdb_cached(ttl_key, entity, str(tmdb_id), eff_locale, need_key) or {}

    if res and _meta_cache_enabled():
        try:
            payload = dict(res)
            payload["locale"] = eff_locale or payload.get("locale") or None
            _write_meta_cache(_meta_cache_path(entity, tmdb_id, eff_locale or "en-US"), payload)
            _prune_meta_cache_if_needed()
        except Exception:
            pass

    return res or {}

def get_runtime(api_key: str, typ: str, tmdb_id: str | int, cache_dir: Path | str) -> Optional[int]:
    meta = get_meta(api_key, typ, tmdb_id, cache_dir, need={"runtime_minutes": True})
    return meta.get("runtime_minutes")

def _cache_download(url: str, dest_path: Path, timeout: float = 15.0) -> Tuple[Path, str]:
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    if not dest_path.exists():
        r = requests.get(url, stream=True, timeout=timeout); r.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(64 * 1024):
                if chunk: f.write(chunk)
    ext = dest_path.suffix.lower()
    mime = "image/jpeg" if ext in (".jpg",".jpeg") else ("image/png" if ext==".png" else "application/octet-stream")
    return dest_path, mime

def get_poster_file(api_key: str, typ: str, tmdb_id: str | int, size: str, cache_dir: Path | str) -> tuple[str, str]:
    meta = get_meta(api_key, typ, tmdb_id, cache_dir, need={"poster": True}) or {}
    posters = ((meta.get("images") or {}).get("poster") or [])
    if not posters: raise FileNotFoundError("No poster found")
    src_url = posters[0]["url"]
    ext = ".jpg" if (".jpg" in src_url or ".jpeg" in src_url) else ".png"
    size_tag = (size or "w780").lower().strip()
    _, base, _ = _env()
    cache_root = Path(cache_dir or (base / "posters"))
    if cache_root == base: cache_root = base / "posters"
    dest = cache_root / f"{typ}_{tmdb_id}_{size_tag}{ext}"
    path, mime = _cache_download(src_url, dest)
    return str(path), mime

# ----- Artwork proxy -----

@router.get("/art/tmdb/{typ}/{tmdb_id}", tags=["artwork"])
def api_tmdb_art(typ: str = FPath(...), tmdb_id: int = FPath(...), size: str = Query("w342")):
    t = typ.lower()
    if t == "show": t = "tv"
    if t not in {"movie","tv"}:
        return PlainTextResponse("Bad type", status_code=400)
    cfg = load_config()
    api_key = str(((cfg.get("tmdb") or {}).get("api_key") or "")).strip()
    if not api_key:
        return PlainTextResponse("TMDb key missing", status_code=404)
    try:
        _, base, _ = _env()
        local_path, mime = get_poster_file(api_key, t, tmdb_id, size, base)
        return FileResponse(str(local_path), media_type=mime,
                            headers={"Cache-Control":"public, max-age=86400, stale-while-revalidate=86400"})
    except Exception as e:
        return PlainTextResponse(f"Poster not available: {e}", status_code=404)

# ----- Resolve (single) -----

class MetadataResolveIn(BaseModel):
    entity: Optional[str] = None
    ids: Dict[str, Any]
    locale: Optional[str] = None
    need: Optional[Dict[str, Any]] = None
    strategy: Optional[str] = None  # e.g., first_success

@router.post("/api/metadata/resolve", tags=["metadata"])
def api_metadata_resolve(payload: MetadataResolveIn = Body(...)):
    _METADATA, _, _ = _env()
    if _METADATA is None:
        return JSONResponse({"ok": False, "error": "MetadataManager not available"}, status_code=500)
    try:
        entity = _norm_media_type(payload.entity)
        res = _METADATA.resolve(entity=entity, ids=payload.ids, locale=payload.locale,
                                need=payload.need, strategy=payload.strategy or "first_success")
        if isinstance(res, dict):
            res.setdefault("type", entity)
        return JSONResponse({"ok": True, "result": res})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

# ----- Resolve (bulk) -----

@router.post("/api/metadata/bulk", tags=["metadata"])
def api_metadata_bulk(
    payload: Dict[str, Any] = Body(..., description="items[] with {type|entity|media_type, tmdb}; need{} optional"),
    overview: Optional[str] = Query("full", description="none|short|full"),
    locale: Optional[str] = Query(None, description="e.g., 'nl-NL'"),
) -> JSONResponse:
    cfg = load_config()
    _, base_cache, _load_state = _env()
    st = _load_state()
    api_key = ((cfg.get("tmdb") or {}).get("api_key") or "").strip()
    md_cfg = (cfg.get("metadata") or {})
    bulk_max = int(md_cfg.get("bulk_max", 300))
    default_workers = 6

    items = (payload or {}).get("items") or []
    if not isinstance(items, list) or not items:
        return JSONResponse({"ok": False, "error": "Body must include a non-empty 'items' array.", "missing_tmdb_key": not bool(api_key)}, status_code=200)
    items = items[:bulk_max]

    req_need = (payload or {}).get("need") or {"overview": True, "tagline": True, "runtime_minutes": True, "score": True}
    req_need = dict(req_need, overview=(overview != "none"))
    eff_locale = locale or md_cfg.get("locale") or (cfg.get("ui") or {}).get("locale") or None

    try:
        requested_workers = int((payload or {}).get("concurrency") or default_workers)
    except Exception:
        requested_workers = default_workers
    workers = max(1, min(requested_workers, 12))

    def _fetch_one(item: Dict[str, Any]) -> tuple[str, Dict[str, Any]]:
        typ = _norm_media_type(item.get("type") or item.get("entity") or item.get("media_type"))
        tmdb_id = str(item.get("tmdb") or item.get("id") or "").strip()
        key = f"{typ}:{tmdb_id or 'UNKNOWN'}"
        if not tmdb_id:
            return key, {"ok": False, "error": "missing tmdb id"}
        item["type"] = typ
        try:
            meta = get_meta(api_key, typ, tmdb_id, base_cache, need=req_need, locale=eff_locale) or {}
        except Exception as e:
            return key, {"ok": False, "error": f"resolver failed: {e}"}
        if not meta:
            return key, {"ok": False, "error": "no metadata"}
        keep = {
            "type","title","year","ids","runtime_minutes","overview","tagline",
            "images","genres","videos","score","certification","release","detail",
        }
        out = {"type": meta.get("type") or typ}
        for k in keep:
            if k != "type" and k in meta: out[k] = meta[k]
        if overview == "short" and out.get("overview"):
            out["overview"] = _shorten(out["overview"], 280)
        if "score" not in out:
            va = (out.get("detail") or {}).get("vote_average") or meta.get("vote_average")
            try: out["score"] = int(round(float(va) * 10))
            except Exception: pass
        return key, {"ok": True, "meta": out}

    results: Dict[str, Any] = {}; fetched = 0
    if len(items) <= 8:
        for it in items:
            k, v = _fetch_one(it); results[k] = v
            if v.get("ok"): fetched += 1
    else:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            for fut in as_completed([ex.submit(_fetch_one, it) for it in items]):
                try: k, v = fut.result()
                except Exception as e: k, v = "unknown:0", {"ok": False, "error": f"worker error: {e}"}
                results[k] = v
                if v.get("ok"): fetched += 1

    return JSONResponse(
        {"ok": True, "count": len(items), "fetched": fetched,
         "missing_tmdb_key": not bool(api_key), "results": results,
         "last_sync_epoch": st.get("last_sync_epoch") if isinstance(st, dict) else None},
        status_code=200,
    )
