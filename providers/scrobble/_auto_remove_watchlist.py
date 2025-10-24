# providers/scrobble/_auto_remove_watchlist.py
from __future__ import annotations

import json, time
from pathlib import Path
from typing import Any, Dict

_TTL_PATH = Path("/config/.cw_state/watchlist_wl_autoremove.json")
_TTL_SECONDS = 120

def _now() -> float:
    return time.time()

def _read_json(p: Path) -> Dict[str, float]:
    try:
        if not p.exists():
            return {}
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}

def _write_json_atomic(p: Path, data: Dict[str, float]) -> None:
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, separators=(",", ":")), encoding="utf-8")
        tmp.replace(p)
    except Exception:
        pass

def _norm_ids(ids: Dict[str, Any] | None) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not isinstance(ids, dict):
        return out
    for k in ("imdb", "tmdb", "tvdb", "trakt", "slug", "jellyfin", "emby"):
        v = ids.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if not s:
            continue
        if k == "imdb" and not s.startswith("tt"):
            s = f"tt{s}" if s.isdigit() or s else s
        out[k] = s
    return out

def _dedupe_key(ids: Dict[str, str], media_type: str | None) -> str:
    parts = [media_type or ""]
    for k in ("imdb", "tmdb", "tvdb", "trakt", "slug", "jellyfin", "emby"):
        if ids.get(k):
            parts.append(f"{k}:{ids[k]}")
    return "|".join(parts)

def _once_per_ttl(key: str) -> bool:
    if not key:
        return True
    data = _read_json(_TTL_PATH)
    ts = float(data.get(key, 0.0) or 0.0)
    if _now() - ts < _TTL_SECONDS:
        return False
    data[key] = _now()
    cutoff = _now() - _TTL_SECONDS
    if len(data) > 2000:
        data = {k: v for k, v in data.items() if v >= cutoff}
    _write_json_atomic(_TTL_PATH, data)
    return True

def _log(msg: str, level: str = "INFO") -> None:
    try:
        from crosswatch import _append_log
        _append_log(level, msg)
        if level.upper() != "DEBUG":
            return
    except Exception:
        pass
    print(f"[WL-AUTO] {level} {msg}")

def remove_by_ids(ids: Dict[str, Any] | None, media_type: str | None = None) -> Dict[str, Any]:
    norm = _norm_ids(ids)
    if not norm:
        _log("auto-remove skipped: no usable IDs in payload", "DEBUG")
        return {"ok": False, "skipped": "no-ids"}
    dkey = _dedupe_key(norm, media_type)
    if not _once_per_ttl(dkey):
        _log(f"auto-remove deduped (TTL) for {dkey}", "DEBUG")
        return {"ok": True, "skipped": "ttl"}
    try:
        import _watchlistAPI as WLAPI
        res = WLAPI.remove_across_providers_by_ids(norm, media_type or "")
        ok = bool(res.get("ok"))
        if ok:
            _log(f"auto-remove OK ids={norm} media={media_type} → {res.get('results')}")
        else:
            _log(f"auto-remove NOOP ids={norm} media={media_type} → {res}", "DEBUG")
        return res if isinstance(res, dict) else {"ok": ok}
    except Exception as e:
        _log(f"auto-remove failed via _watchlistAPI: {e}", "WARN")
        return {"ok": False, "error": str(e), "ids": norm, "media_type": media_type}
