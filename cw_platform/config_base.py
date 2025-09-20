# cw_platform/config_base.py
from __future__ import annotations

import copy
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List

# ------------------------------------------------------------
# Base dir resolution
# ------------------------------------------------------------
def CONFIG_BASE() -> Path:
    """
    Determine the base directory for config files.

    Priority:
      1) $CONFIG_BASE if set
      2) /config (when running in container that mounts /config)
      3) Project root (two levels up from this file)
    """
    env = os.getenv("CONFIG_BASE")
    if env:
        return Path(env)

    if Path("/app").exists():
        # In container images we mount /config as a writable volume
        return Path("/config")

    return Path(__file__).resolve().parents[1]


# Ready-to-use Path
CONFIG: Path = CONFIG_BASE()
CONFIG.mkdir(parents=True, exist_ok=True)  # ensure exists


# ------------------------------------------------------------
# Defaults (safe, comprehensive)
# ------------------------------------------------------------
DEFAULT_CFG: Dict[str, Any] = {
    # --- Providers -----------------------------------------------------------
    "plex": {
        "server_url": "",
        "account_token": "",
        "client_id": "",
        "username": "",
        "servers": {"machine_ids": []},
    },
    "simkl": {
        "access_token": "",
        "refresh_token": "",
        "token_expires_at": 0,
        "client_id": "",
        "client_secret": "",
        "date_from": "",
    },
    "trakt": {
        "client_id": "",
        "client_secret": "",
        "access_token": "",
        "refresh_token": "",
        "scope": "public",
        "token_type": "Bearer",
        "expires_at": 0,
        "_pending_device": {
            "user_code": "",
            "device_code": "",
            "verification_url": "https://trakt.tv/activate",
            "interval": 5,
            "expires_at": 0,
            "created_at": 0,
        },
    },
    "tmdb": {"api_key": ""},

    # --- Sync / Orchestrator -------------------------------------------------
    "sync": {
        "enable_add": False,
        "enable_remove": False,
        "verify_after_write": False,
        "dry_run": False,
        "drop_guard": False,
        "allow_mass_delete": True,
        "tombstone_ttl_days": 1,
        "include_observed_deletes": True,
        "bidirectional": {
            "enabled": False,
            "mode": "two-way",
            "source_of_truth": "",
        },
    },

    # --- Runtime / Diagnostics ----------------------------------------------
    "runtime": {
        "debug": False,
        "state_dir": "",
        "telemetry": {"enabled": True},
    },

    # --- Scheduling ----------------------------------------------------------
    "scheduling": {
        "enabled": False,
        "mode": "hourly",
        "every_n_hours": 2,
        "daily_time": "03:30",
    },

    # --- Pairs (UI-driven) ---------------------------------------------------
    "pairs": [],
}


# ------------------------------------------------------------
# Helpers: paths, IO, merging, normalization
# ------------------------------------------------------------
def _cfg_file() -> Path:
    return CONFIG / "config.json"


def config_path() -> Path:
    """Public accessor kept for compatibility."""
    return _cfg_file()


def _read_json(p: Path) -> Dict[str, Any]:
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)


def _write_json_atomic(p: Path, data: Dict[str, Any]) -> None:
    """
    Atomic-ish write: write to a temp file in the same dir, then replace().
    """
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(f".{int(time.time())}.tmp")
    with tmp.open("w", encoding="utf-8", newline="\n") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.write("\n")
    tmp.replace(p)


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """
    Recursively merge dicts, with `override` taking precedence.
    Lists and scalars are replaced wholesale.
    """
    out = copy.deepcopy(base)
    for k, v in (override or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)  # type: ignore[assignment]
        else:
            out[k] = v
    return out


# -------------------- Feature normalization (pairs.*.features) ----------------
_ALLOWED_RATING_TYPES: List[str] = ["movies", "shows", "seasons", "episodes"]
_ALLOWED_RATING_MODES: List[str] = ["only_new", "from_date", "all"]


def _as_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, Iterable):
        return [str(x) for x in value if isinstance(x, (str, int, float))]
    return []


def _normalize_ratings_feature(val: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize ratings feature in pairs[n].features.ratings, preserving unknown keys.
      - enable/add/remove: bools
      - types: ["all"] or any subset of allowed types; default ["movies","shows"]
      - mode: one of _ALLOWED_RATING_MODES (default "only_new")
      - from_date: only kept when mode == "from_date"
    """
    v = dict(val or {})

    # basic toggles
    v["enable"] = bool(v.get("enable", False))
    v["add"] = bool(v.get("add", False))
    v["remove"] = bool(v.get("remove", False))

    # types
    raw_types = _as_list(v.get("types"))
    types = [str(t).strip().lower() for t in raw_types]
    if "all" in types:
        types = list(_ALLOWED_RATING_TYPES)
    else:
        # keep order per allowed list; de-dupe
        types = [t for t in _ALLOWED_RATING_TYPES if t in types]
        if not types:
            types = ["movies", "shows"]
    v["types"] = types

    # mode + from_date
    mode = str(v.get("mode", "only_new")).strip().lower()
    if mode not in _ALLOWED_RATING_MODES:
        mode = "only_new"
    v["mode"] = mode

    from_date = str(v.get("from_date", "") or "").strip()
    if mode != "from_date":
        from_date = ""
    v["from_date"] = from_date

    return v


def _normalize_features_map(features: dict | None) -> dict:
    """
    Normalize the features map for a single pair:
      - If value is bool: coerce to {enable:add:remove}
      - If value is dict: ensure enable/add/remove defaults, and preserve unknown keys
      - Ratings: apply additional normalization for scope/window (types/mode/from_date)
    """
    f = dict(features or {})
    for name, val in list(f.items()):
        if isinstance(val, bool):
            f[name] = {"enable": bool(val), "add": bool(val), "remove": False}
            continue

        if isinstance(val, dict):
            # Defaults for toggles
            v = dict(val)
            v.setdefault("enable", True)
            v.setdefault("add", True)
            v.setdefault("remove", False)

            # Ratings has extra fields we should normalize carefully
            if name == "ratings":
                v = _normalize_ratings_feature(v)

            f[name] = v
            continue

        # Unknown type -> disabled structure
        f[name] = {"enable": False, "add": False, "remove": False}
    return f


# ------------------------------------------------------------
# Public API
# ------------------------------------------------------------
def load_config() -> Dict[str, Any]:
    """
    Read config.json if present and merge it over DEFAULT_CFG.
    Also normalizes per-pair features maps.
    """
    p = _cfg_file()
    user_cfg: Dict[str, Any] = {}
    if p.exists():
        try:
            user_cfg = _read_json(p)
        except Exception:
            user_cfg = {}

    cfg = _deep_merge(DEFAULT_CFG, user_cfg)

    # normalize pair feature flags if any pairs exist
    pairs = cfg.get("pairs")
    if isinstance(pairs, list):
        for it in pairs:
            if isinstance(it, dict):
                it["features"] = _normalize_features_map(it.get("features"))

    return cfg


def save_config(cfg: Dict[str, Any]) -> None:
    """
    Write to config.json atomically.
    Performs the same feature normalization as load_config (but does not inject
    defaults beyond the pairs' feature maps).
    """
    data = dict(cfg or {})
    pairs = data.get("pairs")
    if isinstance(pairs, list):
        for it in pairs:
            if isinstance(it, dict):
                it["features"] = _normalize_features_map(it.get("features"))

    _write_json_atomic(_cfg_file(), data)
