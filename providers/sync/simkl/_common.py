# /providers/sync/simkl/_common.py
from __future__ import annotations
import os, json, time
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence, Tuple
from datetime import datetime, timezone

from cw_platform.id_map import minimal as id_minimal, canonical_key

START_OF_TIME_ISO = "1970-01-01T00:00:00Z"
DEFAULT_DATE_FROM = START_OF_TIME_ISO
UA = os.getenv("CW_UA", "CrossWatch/3.0 (SIMKL)")

# ---------- state (watermarks)

_STATE_DIR = Path("/config/.cw_state")
_WATERMARK_PATH = _STATE_DIR / "simkl.watermarks.json"  # { "watchlist:shows": ISO, ... }

def _read_json(path: Path) -> Dict[str, Any]:
    try: return json.loads(path.read_text("utf-8"))
    except Exception: return {}

def _write_json(path: Path, data: Mapping[str, Any]) -> None:
    try:
        _STATE_DIR.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True), "utf-8")
        os.replace(tmp, path)
    except Exception:
        pass

def load_watermarks() -> Dict[str, str]:
    d = _read_json(_WATERMARK_PATH)
    return {k: str(v) for k, v in (d or {}).items() if isinstance(v, str) and v.strip()}

def save_watermark(feature: str, iso_ts: str) -> None:
    d = load_watermarks(); d[feature] = iso_ts; _write_json(_WATERMARK_PATH, d)

def get_watermark(feature: str) -> Optional[str]:
    return load_watermarks().get(feature)

def update_watermark_if_new(feature: str, iso_ts: Optional[str]) -> Optional[str]:
    """Advance watermark only forward."""
    if not _iso_ok(iso_ts): return get_watermark(feature)
    cur = get_watermark(feature); new = _max_iso(cur, iso_ts)
    if new and new != cur: save_watermark(feature, new)
    return new

def coalesce_date_from(
    feature: str,
    cfg_date_from: Optional[str] = None,
    *,
    hard_default: str = START_OF_TIME_ISO,
) -> str:
    """Order: watermark → env per-feature → env global → config → hard default."""
    env_any  = os.getenv("SIMKL_DATE_FROM")
    env_feat = os.getenv(f"SIMKL_{feature.upper()}_DATE_FROM")
    for cand in (get_watermark(feature), env_feat, env_any, cfg_date_from, hard_default):
        if _iso_ok(cand): return _iso_z(cand)  # normalize to Z
    return hard_default

# ---------- env flags (used by watchlist, exposed for consistency)

def shadow_ttl_seconds(default: float = 300.0) -> float:
    try: return float(os.getenv("CW_SIMKL_SHADOW_TTL", str(default)))
    except Exception: return default

def want_clear_shadow() -> bool:
    return (os.getenv("CW_SIMKL_WATCHLIST_CLEAR") or "").strip() == "1"

def force_present_bucket() -> Optional[str]:
    v = (os.getenv("CW_SIMKL_FORCE_PRESENT") or "").strip().lower()
    return v if v in ("movies", "shows", "anime", "all", "true", "1") else None

# ---------- time helpers

def _iso_ok(v: Any) -> bool:
    if not isinstance(v, str) or not v.strip(): return False
    try:
        datetime.fromisoformat(v.replace("Z", "+00:00")); return True
    except Exception:
        return False

def _iso_z(v: str) -> str:
    dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
    if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def _max_iso(a: Optional[str], b: Optional[str]) -> Optional[str]:
    if not _iso_ok(a): return _iso_z(b) if _iso_ok(b) else None
    if not _iso_ok(b): return _iso_z(a)
    A = datetime.fromisoformat(_iso_z(a).replace("Z","+00:00"))
    B = datetime.fromisoformat(_iso_z(b).replace("Z","+00:00"))
    return _iso_z(a if A >= B else b)

# ---------- headers

def build_headers(cfg: Mapping[str, Any], *, force_refresh: bool = False) -> Dict[str, str]:
    t = (cfg.get("simkl") or cfg)
    api_key = str(t.get("api_key") or t.get("client_id") or "").strip()
    token   = str(t.get("access_token") or "").strip()
    h = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": UA,
        "simkl-api-key": api_key,
    }
    if token: h["Authorization"] = f"Bearer {token}"
    if force_refresh:
        h["Cache-Control"] = "no-cache"
        h.pop("If-None-Match", None)  # kill ETag reuse
    return h

# ---------- activities + rate limits

# memo: (unix_ts, data, rate)
_ACT_MEMO: Tuple[float, Optional[Dict[str, Any]], Dict[str, Any]] = (0.0, None, {})

def fetch_activities(session, headers: Mapping[str, str], *, timeout: float = 8.0) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    """POST first (SIMKL), fallback to GET. Short-cached to avoid hammering."""
    global _ACT_MEMO
    now = time.time()
    ts, cached, rate_cached = _ACT_MEMO
    if cached is not None and (now - ts) < 10.0:
        return cached, rate_cached

    url = "https://api.simkl.com/sync/activities"
    rate: Dict[str, Any] = {}
    try:
        r = session.post(url, headers=dict(headers), timeout=timeout)
        rate = parse_rate_limit(r.headers)
        if 200 <= r.status_code < 300:
            data = r.json() if (r.text or "").strip() else {}
            _ACT_MEMO = (now, data, rate); return data, rate
        if r.status_code in (404, 405):
            r2 = session.get(url, headers=dict(headers), timeout=timeout)
            rate = parse_rate_limit(r2.headers) or rate
            if 200 <= r2.status_code < 300:
                data = r2.json() if (r2.text or "").strip() else {}
                _ACT_MEMO = (now, data, rate); return data, rate
        return None, rate
    except Exception:
        return None, rate

def parse_rate_limit(h: Mapping[str, str]) -> Dict[str, Any]:
    def _i(x):
        try: return int(x)
        except Exception: return None
    return {
        "limit":     _i(h.get("X-RateLimit-Limit") or h.get("RateLimit-Limit") or h.get("Ratelimit-Limit")),
        "remaining": _i(h.get("X-RateLimit-Remaining") or h.get("RateLimit-Remaining") or h.get("Ratelimit-Remaining")),
        "reset_ts":  _i(h.get("X-RateLimit-Reset") or h.get("RateLimit-Reset") or h.get("Ratelimit-Reset")),
    }

def extract_latest_ts(activities: Mapping[str, Any], paths: Iterable[Sequence[str]]) -> Optional[str]:
    latest: Optional[str] = None
    for p in paths or []:
        cur: Any = activities; ok = True
        for k in p:
            if isinstance(cur, Mapping) and k in cur: cur = cur[k]
            else: ok = False; break
        if ok and isinstance(cur, str) and _iso_ok(cur):
            latest = _max_iso(latest, cur)
    return latest

# ---------- ids + normalization
def _fix_imdb(ids: Mapping[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = dict(ids or {})
    imdb = out.get("imdb")
    if imdb:
        s = str(imdb).strip()
        if s and not s.startswith("tt"):
            digits = "".join(ch for ch in s if ch.isdigit())
            if digits: out["imdb"] = f"tt{digits}"
    return out

def _is_null_envelope(row: Any) -> bool:
    return isinstance(row, Mapping) and row.get("type") == "null" and row.get("body") is None

def _pick_payload(row: Mapping[str, Any]) -> Mapping[str, Any]:
    if not isinstance(row, Mapping) or _is_null_envelope(row): return {}
    t = str(row.get("type") or "").lower()
    if t and isinstance(row.get(t), Mapping): return row[t]
    for k in ("item", "entry", "media"):
        if isinstance(row.get(k), Mapping): return row[k]
    if "ids" in row or "title" in row: return row
    return {}

def normalize(obj: Mapping[str, Any]) -> Dict[str, Any]:
    if not isinstance(obj, Mapping): return id_minimal({})
    payload = _pick_payload(obj)
    t = str(obj.get("type") or "").lower()
    if not t:
        for k in ("movie", "show", "anime"):
            if isinstance(obj.get(k), Mapping) or isinstance(payload.get(k) if isinstance(payload, Mapping) else None, Mapping):
                t = k; break
    if t == "anime": t = "show"
    if t not in ("movie", "show"): t = "movie"
    ids = _fix_imdb(payload.get("ids") or {})
    base = {
        "type": t,
        "title": payload.get("title") or obj.get("title"),
        "year": payload.get("year") or obj.get("year"),
        "ids": {k: v for k, v in ids.items() if v},
    }
    return id_minimal(base)

def key_of(item: Mapping[str, Any]) -> str:
    return canonical_key(normalize(item))
