# cw_platform/config_base.py
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict
import os
import json
import copy
import time

# ------------------------------------------------------------
# Base dir resolution
# ------------------------------------------------------------
def CONFIG_BASE() -> Path:
    """
    Resolve the writable config root. In containers, this is `/config`.
    Outside containers (dev), fall back to the repo root.
    """
    env = os.getenv("CONFIG_BASE")
    if env:
        return Path(env)

    # Container image layout: /app exists, /config is the bind-mounted RW volume
    if Path("/app").exists():
        return Path("/config")

    # Dev fallback: project root (two levels up from this file)
    return Path(__file__).resolve().parents[1]


# Ready-to-use Path
CONFIG = CONFIG_BASE()
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
        "allow_mass_delete": False,

        # Tombstones (pair-scoped in orchestrator) + observed deletes
        "tombstone_ttl_days": 30,
        "include_observed_deletes": True,

        # --------------- Global Tombstones: feature-agnostic suppression window ---------------
        "gmt_enable": False,            # opt-in; orchestrator/providers behave fine if left False
        "gmt_quarantine_days": 7,       # days to suppress re-adds for items explicitly removed elsewhere

        "bidirectional": {
            "enabled": False,
            "mode": "two-way",
            "source_of_truth": ""
        },
    },

    # --- Runtime / Diagnostics ----------------------------------------------
    "runtime": {
        "debug": False,
        # --------------- Ensure SIMKL/TRAKT cursors & shadows live under /config by default ---------------
        "state_dir": "/config",
        "telemetry": {"enabled": True},
    },

    # --- Scheduling -----------------------------------------------------------
    "scheduling": {
        "enabled": False,
        "mode": "hourly",
        "every_n_hours": 2,
        "daily_time": "03:30",
    },

    # --- airs (UI driven) ---------------------------------------------------
    "pairs": [],
}


# ------------------------------------------------------------
# Helpers: deep merge + file IO + normalization
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
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix("." + str(int(time.time())) + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    tmp.replace(p)

def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    out = copy.deepcopy(base)
    for k, v in (override or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)  # type: ignore[assignment]
        else:
            out[k] = v
    return out

def _normalize_features_map(f: dict | None) -> dict:
    f = dict(f or {})
    for name, val in list(f.items()):
        if isinstance(val, bool):
            f[name] = {"enable": bool(val), "add": bool(val), "remove": False}
        elif isinstance(val, dict):
            val.setdefault("enable", True)
            val.setdefault("add", True)
            val.setdefault("remove", False)
            f[name] = val
        else:
            f[name] = {"enable": False, "add": False, "remove": False}
    return f


# ------------------------------------------------------------
# Public API
# ------------------------------------------------------------
def load_config() -> Dict[str, Any]:
    """
    Read /config/config.json if present and merge it over DEFAULT_CFG.
    Also normalizes pairs->features and ensures runtime.state_dir is set.
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

    # --------------- Default state_dir to /config if missing/empty ---------------
    rt = cfg.get("runtime") or {}
    state_dir = str(rt.get("state_dir") or "").strip()
    if not state_dir:
        # honor CONFIG (which already points to /config in containers)
        rt["state_dir"] = str(CONFIG)
        cfg["runtime"] = rt

    # best-effort: ensure the state_dir directory exists
    try:
        Path(rt["state_dir"]).mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

    return cfg


def save_config(cfg: Dict[str, Any]) -> None:
    """
    Write to /config/config.json atomically.
    Performs the same normalization as load_config (but does not inject all defaults).
    """
    data = dict(cfg or {})

    # normalize pair feature flags to stable shape
    if isinstance(data.get("pairs"), list):
        for it in data["pairs"]:
            if isinstance(it, dict):
                it["features"] = _normalize_features_map(it.get("features"))

    # ensure runtime.state_dir is non-empty and points to a writable path
    rt = data.get("runtime") or {}
    if not str(rt.get("state_dir") or "").strip():
        rt["state_dir"] = str(CONFIG)
        data["runtime"] = rt
    try:
        Path(rt["state_dir"]).mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

    _write_json_atomic(_cfg_file(), data)
