# _TMDB.py
from __future__ import annotations

from pathlib import Path
from typing import Optional, Tuple, Dict, Any, List
import json
import urllib.request
import time

TMDB_IMG = "https://image.tmdb.org/t/p"
TMDB_API = "https://api.themoviedb.org/3"

def _urlopen(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=15) as r:
        return r.read()

def get_meta(api_key: str, typ: str, tmdb_id: int, cache_dir: Path) -> Dict[str, Any]:
    meta_file = cache_dir / "tmdb_meta" / f"{typ}-{tmdb_id}.json"
    meta_file.parent.mkdir(parents=True, exist_ok=True)
    if meta_file.exists():
        try:
            return json.loads(meta_file.read_text(encoding="utf-8"))
        except Exception:
            pass
    url = f"{TMDB_API}/{typ}/{tmdb_id}?language=en-US&api_key={api_key}"
    j = json.loads(_urlopen(url).decode("utf-8", errors="ignore"))
    genres: List[str] = [g.get("name", "") for g in (j.get("genres") or []) if g.get("name")]
    out = {
        "id": tmdb_id,
        "type": typ,
        "title": j.get("title") or j.get("name") or "",
        "overview": j.get("overview") or "",
        "year": (j.get("release_date") or j.get("first_air_date") or "0000")[:4],
        "poster_path": j.get("poster_path"),
        "genres": genres,
    }
    meta_file.write_text(json.dumps(out, indent=2), encoding="utf-8")
    return out

def get_poster_file(api_key: str, typ: str, tmdb_id: int, size: str, cache_dir: Path) -> Tuple[Path, str]:
    meta = get_meta(api_key, typ, tmdb_id, cache_dir)
    poster_path = meta.get("poster_path")
    if not poster_path:
        raise RuntimeError("no poster")
    safe_size = size if size.startswith("w") or size == "original" else "w342"
    local = cache_dir / "tmdb" / typ / str(tmdb_id) / f"{safe_size}.jpg"
    local.parent.mkdir(parents=True, exist_ok=True)
    if not local.exists():
        url = f"{TMDB_IMG}/{safe_size}{poster_path}"
        local.write_bytes(_urlopen(url))
    return local, "image/jpeg"

def get_runtime(api_key: str, typ: str, tmdb_id: int, cache_dir: Path, ttl_days: int = 14) -> Optional[int]:
    try:
        t = "tv" if typ == "tv" else "movie"
        f = cache_dir / "tmdb_meta" / f"{t}-{int(tmdb_id)}.json"
        f.parent.mkdir(parents=True, exist_ok=True)

        data: Optional[Dict[str, Any]] = None

        # try fresh cache
        if f.exists() and (time.time() - f.stat().st_mtime) < ttl_days * 86400:
            try:
                maybe = json.loads(f.read_text(encoding="utf-8"))
                if isinstance(maybe, dict):
                    data = maybe
            except Exception:
                data = None

        # fetch if cache missing/invalid
        if data is None:
            url = f"{TMDB_API}/{t}/{int(tmdb_id)}?api_key={api_key}&language=en-US"
            raw = _urlopen(url)
            f.write_bytes(raw)
            try:
                maybe = json.loads(raw.decode("utf-8", errors="ignore"))
                if isinstance(maybe, dict):
                    data = maybe
                else:
                    return None
            except Exception:
                return None

        if t == "movie":
            rt = data.get("runtime")
            return int(rt) if isinstance(rt, (int, float)) else None

        arr = data.get("episode_run_time") or data.get("episode_run_times") or []
        if isinstance(arr, list) and arr:
            return int(sum(arr) / len(arr))
        return None

    except Exception:
        return None
