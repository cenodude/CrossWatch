# SIMKL Module for common functions
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from cw_platform.id_map import canonical_key, minimal as id_minimal

START_OF_TIME_ISO = "1970-01-01T00:00:00Z"
DEFAULT_DATE_FROM = START_OF_TIME_ISO
UA = os.getenv("CW_UA", "CrossWatch/3.2.1 (SIMKL)")

STATE_DIR = Path("/config/.cw_state")


def _pair_scope() -> str | None:
    for k in ("CW_PAIR_KEY", "CW_PAIR_SCOPE", "CW_SYNC_PAIR", "CW_PAIR"):
        v = os.getenv(k)
        if v and str(v).strip():
            return str(v).strip()
    return None




def _is_capture_mode() -> bool:
    v = str(os.getenv("CW_CAPTURE_MODE") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def _safe_scope(value: str) -> str:
    s = "".join(ch if (ch.isalnum() or ch in ("-", "_", ".")) else "_" for ch in str(value))
    s = s.strip("_ ")
    while "__" in s:
        s = s.replace("__", "_")
    return s[:96] if s else "default"


def state_file(name: str) -> Path:
    scope = _pair_scope()
    safe = _safe_scope(scope) if scope else "unscoped"
    p = Path(name)
    if p.suffix:
        return STATE_DIR / f"{p.stem}.{safe}{p.suffix}"
    return STATE_DIR / f"{name}.{safe}"


def scoped_state_path(name: str) -> Path:
    return state_file(name)


def _watermark_path() -> Path:
    return state_file("simkl.watermarks.json")


def _legacy_path(path: Path) -> Path | None:
    parts = path.stem.split(".")
    if len(parts) < 2:
        return None
    legacy_name = ".".join(parts[:-1]) + path.suffix
    legacy = path.with_name(legacy_name)
    return None if legacy == path else legacy


def _migrate_legacy_json(path: Path) -> None:
    if path.exists():
        return
    if _is_capture_mode() or _pair_scope() is None:
        return
    legacy = _legacy_path(path)
    if not legacy or not legacy.exists():
        return
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(f"{path.name}.tmp")
        tmp.write_bytes(legacy.read_bytes())
        os.replace(tmp, path)
    except Exception:
        pass


def _read_json(path: Path) -> dict[str, Any]:
    if _is_capture_mode() or _pair_scope() is None:
        return {}
    _migrate_legacy_json(path)
    try:
        return json.loads(path.read_text("utf-8"))
    except Exception:
        return {}


def _write_json(path: Path, data: Mapping[str, Any]) -> None:
    if _pair_scope() is None:
        return
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(f"{path.name}.tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True), "utf-8")
        os.replace(tmp, path)
    except Exception:
        pass


def load_watermarks() -> dict[str, str]:
    if _is_capture_mode() or _pair_scope() is None:
        return {}
    data = _read_json(_watermark_path())
    return {k: str(v) for k, v in (data or {}).items() if isinstance(v, str) and v.strip()}


def save_watermark(feature: str, iso_ts: str) -> None:
    if _pair_scope() is None:
        return
    if (_pair_scope() or "").startswith("health:"):
        return
    data = load_watermarks()
    data[feature] = iso_ts
    _write_json(_watermark_path(), data)


def get_watermark(feature: str) -> str | None:
    return load_watermarks().get(feature)


def update_watermark_if_new(feature: str, iso_ts: str | None) -> str | None:
    if not _iso_ok(iso_ts):
        return get_watermark(feature)
    current = get_watermark(feature)
    new = _max_iso(current, iso_ts)
    if new and new != current:
        save_watermark(feature, new)
    return new


def normalize_flat_watermarks() -> None:
    p = _watermark_path()
    raw = _read_json(p)
    if not isinstance(raw, dict) or not raw:
        return
    data: dict[str, Any] = {str(k): v for k, v in raw.items() if isinstance(k, str)}
    changed = False

    def _fold(base_key: str, prefix: str) -> None:
        nonlocal changed
        candidates: list[str] = []
        for k, v in list(data.items()):
            if not k.startswith(prefix):
                continue
            if _iso_ok(v):
                candidates.append(_iso_z(str(v)))
            data.pop(k, None)
            changed = True
        if _iso_ok(data.get(base_key)):
            return
        if candidates:
            data[base_key] = max(candidates)
            changed = True

    _fold("watchlist", "watchlist:")
    _fold("watchlist_removed", "watchlist_removed:")
    _fold("ratings", "ratings:")
    _fold("history", "history:")

    if changed:
        _write_json(p, data)


def coalesce_date_from(
    feature: str,
    cfg_date_from: str | None = None,
    *,
    hard_default: str = START_OF_TIME_ISO,
) -> str:
    env_any = os.getenv("SIMKL_DATE_FROM")
    env_feature = os.getenv(f"SIMKL_{feature.upper()}_DATE_FROM")
    for candidate in (get_watermark(feature), env_feature, env_any, cfg_date_from, hard_default):
        if _iso_ok(candidate):
            return _iso_z(candidate)
    return hard_default


def _iso_ok(value: Any) -> bool:
    if not isinstance(value, str) or not value.strip():
        return False
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        return True
    except Exception:
        return False


def _iso_z(value: str | None) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("invalid ISO timestamp")
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _max_iso(a: str | None, b: str | None) -> str | None:
    if not _iso_ok(a):
        return _iso_z(b) if _iso_ok(b) else None
    if not _iso_ok(b):
        return _iso_z(a)
    a_z = _iso_z(a)
    b_z = _iso_z(b)
    dt_a = datetime.fromisoformat(a_z.replace("Z", "+00:00"))
    dt_b = datetime.fromisoformat(b_z.replace("Z", "+00:00"))
    return _iso_z(a if dt_a >= dt_b else b)


def build_headers(cfg: Mapping[str, Any], *, force_refresh: bool = False) -> dict[str, str]:
    target = cfg.get("simkl") or cfg
    api_key = str(target.get("api_key") or target.get("client_id") or "").strip()
    token = str(target.get("access_token") or "").strip()
    headers: dict[str, str] = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": UA,
        "simkl-api-key": api_key,
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if force_refresh:
        headers["Cache-Control"] = "no-cache"
        headers.pop("If-None-Match", None)
    return headers


_ACT_MEMO: tuple[float, dict[str, Any] | None, dict[str, Any]] = (0.0, None, {})


def fetch_activities(
    session: Any,
    headers: Mapping[str, str],
    *,
    timeout: float = 8.0,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    global _ACT_MEMO
    now = time.time()
    ts, cached, rate_cached = _ACT_MEMO
    if cached is not None and (now - ts) < 10.0:
        return cached, rate_cached

    url = "https://api.simkl.com/sync/activities"
    rate: dict[str, Any] = {}
    try:
        resp = session.post(url, headers=dict(headers), timeout=timeout)
        rate = parse_rate_limit(resp.headers)
        if 200 <= resp.status_code < 300:
            data = resp.json() if (resp.text or "").strip() else {}
            _ACT_MEMO = (now, data, rate)
            return data, rate
        return None, rate
    except Exception:
        return None, rate


def parse_rate_limit(headers: Mapping[str, str]) -> dict[str, Any]:
    def _to_int(value: str | None) -> int | None:
        try:
            return int(value) if value is not None else None
        except Exception:
            return None

    return {
        "limit": _to_int(headers.get("X-RateLimit-Limit") or headers.get("RateLimit-Limit") or headers.get("Ratelimit-Limit")),
        "remaining": _to_int(headers.get("X-RateLimit-Remaining") or headers.get("RateLimit-Remaining") or headers.get("Ratelimit-Remaining")),
        "reset_ts": _to_int(headers.get("X-RateLimit-Reset") or headers.get("RateLimit-Reset") or headers.get("Ratelimit-Reset")),
    }


def extract_latest_ts(activities: Mapping[str, Any], paths: Iterable[Sequence[str]]) -> str | None:
    latest: str | None = None
    for path in paths or []:
        current: Any = activities
        ok = True
        for key in path:
            if isinstance(current, Mapping) and key in current:
                current = current[key]
            else:
                ok = False
                break
        if ok and isinstance(current, str) and _iso_ok(current):
            latest = _max_iso(latest, current)
    return latest


def _fix_imdb(ids: Mapping[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = dict(ids or {})
    imdb = out.get("imdb")
    if imdb:
        value = str(imdb).strip()
        if value and not value.startswith("tt"):
            digits = "".join(ch for ch in value if ch.isdigit())
            if digits:
                out["imdb"] = f"tt{digits}"
    return out


def _is_null_envelope(row: Any) -> bool:
    return isinstance(row, Mapping) and row.get("type") == "null" and row.get("body") is None


def _pick_payload(row: Mapping[str, Any]) -> Mapping[str, Any]:
    if not isinstance(row, Mapping) or _is_null_envelope(row):
        return {}
    row_type = str(row.get("type") or "").lower()
    if row_type and isinstance(row.get(row_type), Mapping):
        return row[row_type]
    for key in ("item", "entry", "media"):
        if isinstance(row.get(key), Mapping):
            return row[key]
    for key in ("movie", "show", "anime", "episode", "season"):
        if isinstance(row.get(key), Mapping):
            return row[key]
    if "ids" in row or "title" in row:
        return row
    return {}


def normalize(obj: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(obj, Mapping):
        return id_minimal({})
    payload = _pick_payload(obj)
    obj_type = str(obj.get("type") or "").lower()
    if not obj_type:
        for key in ("movie", "show", "anime"):
            if isinstance(obj.get(key), Mapping) or isinstance(
                payload.get(key) if isinstance(payload, Mapping) else None,
                Mapping,
            ):
                obj_type = key
                break

    if obj_type == "episode":
        ids = _fix_imdb((payload.get("ids") if isinstance(payload, Mapping) else None) or obj.get("ids") or {})

        def _to_int(v: Any) -> int | None:
            try:
                return int(v)
            except Exception:
                return None

        base = {
            "type": "episode",
            "title": (payload.get("title") if isinstance(payload, Mapping) else None) or obj.get("title"),
            "year": (payload.get("year") if isinstance(payload, Mapping) else None) or obj.get("year"),
            "ids": {k: v for k, v in ids.items() if v},
            "season": _to_int(obj.get("season") or obj.get("season_number")),
            "episode": _to_int(obj.get("episode") or obj.get("episode_number")),
        }
        return id_minimal(base)

    if obj_type not in ("movie", "show", "anime"):
        return id_minimal({})

    ids = _fix_imdb(payload.get("ids") or {})
    base = {
        "type": obj_type,
        "title": payload.get("title") or obj.get("title"),
        "year": payload.get("year") or obj.get("year"),
        "ids": {k: v for k, v in ids.items() if v},
    }
    return id_minimal(base)


def key_of(item: Mapping[str, Any]) -> str:
    if not isinstance(item, Mapping):
        return ""

    typ = str(item.get("type") or "").lower()
    if typ == "episode":

        def _to_int(v: Any) -> int:
            try:
                n = int(v)
                return n if n > 0 else 0
            except Exception:
                return 0

        s_num = _to_int(item.get("season") or item.get("season_number"))
        e_num = _to_int(item.get("episode") or item.get("episode_number") or item.get("number"))

        show_ids_raw = item.get("show_ids")
        show_ids = dict(show_ids_raw) if isinstance(show_ids_raw, Mapping) else {}
        if not show_ids:
            ids_raw = item.get("ids")
            ids = dict(ids_raw) if isinstance(ids_raw, Mapping) else {}
            show_ids = {k: ids[k] for k in ("tmdb", "imdb", "tvdb", "simkl") if ids.get(k)}

        if show_ids and s_num and e_num:
            show_key = canonical_key(id_minimal({"type": "show", "ids": _fix_imdb(show_ids)})) or ""
            if show_key:
                return f"{show_key}#s{s_num:02d}e{e_num:02d}"

    k = canonical_key(normalize(item))
    return k or ""


__all__ = [
    "START_OF_TIME_ISO",
    "DEFAULT_DATE_FROM",
    "UA",
    "STATE_DIR",
    "state_file",
    "scoped_state_path",
    "load_watermarks",
    "save_watermark",
    "get_watermark",
    "update_watermark_if_new",
    "coalesce_date_from",
    "build_headers",
    "fetch_activities",
    "parse_rate_limit",
    "extract_latest_ts",
    "canonical_key",
    "id_minimal",
    "key_of",
    "normalize",
    "normalize_flat_watermarks",
]
