# providers/scrobble/_auto_remove_plex.py
from __future__ import annotations
import json, time
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

_TTL_PATH = Path("/config/.cw_state/plex_wl_autoremove.json")
_TTL_SECONDS = 120

def _now() -> float: return time.time()

def _read_json(p: Path) -> Dict[str, float]:
    try:
        if not p.exists(): return {}
        return json.loads(p.read_text("utf-8"))
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

def _get(ev: Any, name: str, default=None):
    try:
        if isinstance(ev, Mapping):
            return ev.get(name, default)
        return getattr(ev, name, default)
    except Exception:
        return default

def _dedupe_key(ev: Any) -> str:
    ids = _get(ev, "ids", {}) or {}
    title = _get(ev, "title", "") or ""
    year = _get(ev, "year", "") or ""
    media_type = str(_get(ev, "media_type", "") or "").lower()
    account = _get(ev, "account", "") or ""
    rk = ids.get("plex") or ""
    ext = ids.get("imdb") or ids.get("tmdb") or ids.get("tvdb") or ""
    base = rk or str(ext) or f"{title}|{year}"
    return f"{account}|{media_type}|{base}"

def _dedupe_should_run(ev: Any) -> bool:
    key = _dedupe_key(ev)
    if not key: return True
    data = _read_json(_TTL_PATH)
    ts = data.get(key, 0.0)
    if _now() - ts < _TTL_SECONDS:
        return False
    data[key] = _now()
    if len(data) > 2000:
        cutoff = _now() - _TTL_SECONDS
        data = {k: v for k, v in data.items() if v >= cutoff}
    _write_json_atomic(_TTL_PATH, data)
    return True

def _cfg_val(cfg: Mapping[str, Any], path: str, default=None):
    cur: Any = cfg
    for part in path.split("."):
        if not isinstance(cur, Mapping): return default
        cur = cur.get(part)
        if cur is None: return default
    return cur

def _username_from_event(ev: Any) -> str:
    v = _get(ev, "account", None)
    if v: return str(v)
    raw = _get(ev, "raw", {}) or {}
    try:
        acc = (raw.get("Account") or {}).get("title")
        if acc: return str(acc)
    except Exception:
        pass
    return ""

def _server_uuid_from_event(ev: Any) -> str:
    v = _get(ev, "server_uuid", None)
    if v: return str(v)
    raw = _get(ev, "raw", {}) or {}
    try:
        srv = raw.get("Server") or {}
        u = srv.get("uuid") or srv.get("machineIdentifier")
        if u: return str(u)
    except Exception:
        pass
    return ""

def _rating_key_from_event(ev: Any) -> Optional[str]:
    ids = _get(ev, "ids", {}) or {}
    rk = ids.get("plex")
    if rk: return str(rk)
    raw = _get(ev, "raw", {}) or {}
    try:
        psn = raw.get("PlaySessionStateNotification")
        if isinstance(psn, list) and psn:
            v = psn[0].get("ratingKey") or psn[0].get("ratingkey")
            if v is not None: return str(v)
        for vv in raw.values():
            if isinstance(vv, dict) and ("ratingKey" in vv or "ratingkey" in vv):
                v = vv.get("ratingKey") or vv.get("ratingkey")
                if v is not None: return str(v)
    except Exception:
        pass
    return None

def _ids_from_event(ev: Any) -> Dict[str, Any]:
    ids = dict(_get(ev, "ids", {}) or {})
    out = {}
    for k in ("imdb", "tmdb", "tvdb"):
        v = ids.get(k)
        if v: out[k] = v
    return out

def _passes_filters(ev: Any, cfg: Mapping[str, Any]) -> bool:
    u = _username_from_event(ev).strip()
    s = _server_uuid_from_event(ev).strip().lower()

    wl_watch = _cfg_val(cfg, "scrobble.watch.filters.username_whitelist", []) or []
    wl_webhook = _cfg_val(cfg, "scrobble.webhook.filters_plex.username_whitelist", []) or []
    whitelist = [str(x) for x in (list(wl_watch) + list(wl_webhook)) if x]
    if whitelist:
        lu = u.lower()
        if not any(str(x).strip().lower() == lu for x in whitelist):
            return False

    want_watch = _cfg_val(cfg, "scrobble.watch.filters.server_uuid", "") or ""
    want_webhook = _cfg_val(cfg, "scrobble.webhook.filters_plex.server_uuid", "") or ""
    want_cfg = _cfg_val(cfg, "plex.server_uuid", "") or ""
    wants = {str(x).strip().lower() for x in (want_watch, want_webhook, want_cfg) if str(x).strip()}
    if wants and s and s not in wants:
        return False

    return True

def _maybe_hydrate_ids_with_rk(cfg: Mapping[str, Any], rk: Optional[str], ids: Dict[str, Any]) -> Dict[str, Any]:
    if ids and any(ids.get(k) for k in ("imdb", "tmdb", "tvdb")):
        return ids
    tok = (_cfg_val(cfg, "plex.account_token", "") or "").strip()
    if not tok or not rk: return ids or {}
    try:
        from _common import hydrate_external_ids  # root-level
        extra = hydrate_external_ids(tok, str(rk))
        if isinstance(extra, dict):
            for k in ("imdb", "tmdb", "tvdb"):
                v = extra.get(k)
                if v and not ids.get(k): ids[k] = v
    except Exception:
        pass
    return ids

def _pick_key_for_delete(ids: Dict[str, Any], title: Any, year: Any) -> Optional[str]:
    imdb = ids.get("imdb")
    if isinstance(imdb, str) and imdb:
        return f"imdb:{imdb if imdb.startswith('tt') else 'tt'+imdb}"
    tmdb = ids.get("tmdb")
    if tmdb is not None:
        return f"tmdb:{tmdb}"
    tvdb = ids.get("tvdb")
    if tvdb is not None:
        return f"tvdb:{tvdb}"
    if title and year:
        return None
    return None

def _call_delete_via_api(key: str) -> bool:
    try:
        import _watchlistAPI as WLAPI  # root-level
        res = WLAPI._bulk_delete("PLEX", [key])  # single-key batch
        if isinstance(res, dict):
            if res.get("ok"): return True
            for r in (res.get("results") or []):
                if r.get("provider") == "PLEX" and r.get("ok"):
                    return True
        return False
    except Exception:
        return False

def auto_remove_if_config_allows(event: Any, cfg: Mapping[str, Any]) -> bool:
    sc = (cfg.get("scrobble") or {}) if isinstance(cfg, Mapping) else {}
    if not sc.get("delete_plex", False): return False
    media_type = str(_get(event, "media_type", "") or "").lower()
    types = sc.get("delete_plex_types") or ["movie"]
    if media_type not in {str(t).lower() for t in types}: return False

    force_at = int((((sc.get("trakt") or {}).get("force_stop_at")) or 95))
    progress = int(_get(event, "progress", 0) or 0)
    if progress < force_at: return False

    if not _passes_filters(event, cfg): return False
    if not _dedupe_should_run(event): return False

    title = _get(event, "title", None)
    year = _get(event, "year", None)
    ids = _ids_from_event(event)
    rk = _rating_key_from_event(event)

    ids = _maybe_hydrate_ids_with_rk(cfg, rk, ids)
    key = _pick_key_for_delete(ids, title, year)
    if not key: return False

    tok = (_cfg_val(cfg, "plex.account_token", "") or "").strip()
    if not tok: return False

    return _call_delete_via_api(key)