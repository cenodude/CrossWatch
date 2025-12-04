from __future__ import annotations
from pathlib import Path
from typing import Dict, Set, Iterable, Optional, Union, Any
import json, time

STATE_DIR = Path("/config/.cw_state")

try:
    from ..id_map import canonical_key as _ck, minimal as _minimal  # type: ignore
except Exception:
    _ck = None  # type: ignore
    _minimal = None  # type: ignore


# Help help
def _read_json(path: Path) -> Dict[str, Any]:
    try:
        if not path.exists():
            return {}
        data = json.loads(path.read_text("utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}

def _atomic_write(path: Path, data: Dict[str, Any]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        tmp.replace(path)
    except Exception:
        pass

def _blocking_path(dst: str, feature: str) -> Path:
    dst_lower = str(dst).strip().lower()
    feat_lower = str(feature).strip().lower()
    return STATE_DIR / f"{dst_lower}_{feat_lower}.unresolved.json"

def _pending_path(dst: str, feature: str) -> Path:
    dst_lower = str(dst).strip().lower()
    feat_lower = str(feature).strip().lower()
    return STATE_DIR / f"{dst_lower}_{feat_lower}.unresolved.pending.json"


# BLocking

def load_unresolved_keys(dst: str,
                         feature: Optional[str] = None,
                         *,
                         cross_features: bool = True) -> Set[str]:

    keys: Set[str] = set()
    if not dst:
        return keys
    dst_lower = str(dst).strip().lower()

    if feature and not cross_features:
        p = _blocking_path(dst_lower, feature)
        if p.exists():
            keys |= set(_read_json(p).keys())
        return keys

    if not STATE_DIR.exists():
        return keys
    prefix = f"{dst_lower}_"
    suffix = ".unresolved.json"
    for p in STATE_DIR.iterdir():
        if p.is_file():
            name = p.name
            if name.startswith(prefix) and name.endswith(suffix):
                keys |= set(_read_json(p).keys())
    return keys


def load_unresolved_map(dst: str,
                        feature: Optional[str] = None,
                        *,
                        cross_features: bool = True) -> Dict[str, dict]:

    out: Dict[str, dict] = {}
    if not dst:
        return out
    dst_lower = str(dst).strip().lower()

    if feature and not cross_features:
        p = _blocking_path(dst_lower, feature)
        data = _read_json(p)
        if data:
            for k, v in data.items():
                out[k] = v if isinstance(v, dict) else {}
        return out

    if not STATE_DIR.exists():
        return out
    prefix = f"{dst_lower}_"
    suffix = ".unresolved.json"
    for p in STATE_DIR.iterdir():
        if p.is_file():
            name = p.name
            if name.startswith(prefix) and name.endswith(suffix):
                data = _read_json(p)
                for k, v in (data or {}).items():
                    out[k] = v if isinstance(v, dict) else {}
    return out


# Write helpers
def _to_ck_and_min(item: Union[str, Dict[str, Any]]) -> tuple[str, Optional[Dict[str, Any]]]:
    if isinstance(item, str):
        return item, None
    if not isinstance(item, dict):
        return "", None
    ck = ""
    if _ck:
        try:
            ck = _ck(item) or ""
        except Exception:
            ck = ""
    if not ck:
        ids = item.get("ids") or {}
        for k in ("imdb", "tmdb", "tvdb", "trakt", "ani", "mal"):
            v = ids.get(k)
            if v:
                ck = f"{k}:{str(v).lower()}"
                break
        if not ck:
            ck = str(item.get("id") or item.get("title") or "").strip().lower()

    min_item = None
    if _minimal:
        try:
            min_item = _minimal(item)
        except Exception:
            min_item = item
    else:
        min_item = item
    return ck, (min_item if isinstance(min_item, dict) else None)


def record_unresolved(dst: str,
                      feature: str,
                      items: Iterable[Union[str, Dict[str, Any]]],
                      *,
                      hint: str = "provider_down") -> Dict[str, Any]:

    path = _pending_path(dst, feature)
    now = int(time.time())

    data: Dict[str, Any] = {"keys": [], "items": {}, "hints": {}}
    cur = _read_json(path)
    if cur:
       
        try:
            data["keys"] = list(set(cur.get("keys") or []))
            data["items"] = dict(cur.get("items") or {})
            data["hints"] = dict(cur.get("hints") or {})
        except Exception:
            pass

    existing: Set[str] = set(data["keys"])
    added = 0

    for it in (items or []):
        ck, min_item = _to_ck_and_min(it)
        if not ck:
            continue
        if ck not in existing:
            data["keys"].append(ck)
            existing.add(ck)
            if min_item:
                data["items"][ck] = min_item
            added += 1
    
        if hint:
            data.setdefault("hints", {})[ck] = {"reason": str(hint), "ts": now}

    _atomic_write(path, data)
    return {"ok": True, "count": added, "path": str(path)}
