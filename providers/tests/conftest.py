from __future__ import annotations

import os
import re
import sys
import types
from pathlib import Path
from typing import Any, Mapping

import pytest


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _ensure_sys_path() -> None:
    root = str(_project_root())
    if root not in sys.path:
        sys.path.insert(0, root)


def _get_ids(obj: Any) -> dict[str, str]:
    ids: dict[str, str] = {}
    if not obj:
        return ids
    for key in ("imdb", "tmdb", "tvdb", "anidb", "simkl", "slug"):
        val = None
        if isinstance(obj, Mapping):
            val = obj.get(key)
        else:
            val = getattr(obj, key, None)
        if val is None:
            continue
        sval = str(val).strip()
        if sval:
            ids[key] = sval
    return ids


def _canonical_key(ids: Mapping[str, Any]) -> str | None:
    for k in ("imdb", "tmdb", "tvdb", "anidb", "simkl", "slug"):
        v = ids.get(k)
        if v:
            return f"{k}:{v}"
    return None


def _minimal(ids: Mapping[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for k in ("imdb", "tmdb", "tvdb", "anidb", "simkl", "slug"):
        v = ids.get(k)
        if v:
            out[k] = str(v)
    return out


def _ids_from(obj: Any) -> dict[str, str]:
    return _minimal(_get_ids(obj))


_GUID_RE = re.compile(r"(?P<prefix>imdb|tmdb|tvdb|anidb|simkl)://(?P<id>[^/]+)", re.IGNORECASE)


def _ids_from_guid(guid: str) -> dict[str, str]:
    if not guid:
        return {}
    m = _GUID_RE.search(guid)
    if not m:
        return {}
    return {m.group("prefix").lower(): m.group("id")}


def _normalize_instance_id(instance_id: str | None) -> str:
    if not instance_id:
        return "default"
    return str(instance_id).strip() or "default"


def _ensure_instance_block(cfg: dict[str, Any], provider: str, instance_id: str) -> dict[str, Any]:
    cfg.setdefault("providers", {})
    cfg["providers"].setdefault(provider, {})
    cfg["providers"][provider].setdefault(instance_id, {})
    block = cfg["providers"][provider][instance_id]
    if not isinstance(block, dict):
        cfg["providers"][provider][instance_id] = {}
        block = cfg["providers"][provider][instance_id]
    return block


def _get_provider_block(cfg: dict[str, Any], provider: str, instance_id: str) -> dict[str, Any]:
    return _ensure_instance_block(cfg, provider, instance_id)


# Memory config test
_CONFIG_MEM: dict[str, Any] = {}


def _load_config(path: Path | None = None) -> dict[str, Any]:
    if not _CONFIG_MEM:
        _CONFIG_MEM.update({"providers": {}, "sync": {}})
    return _CONFIG_MEM


def _save_config(cfg: dict[str, Any], path: Path | None = None) -> None:
    _CONFIG_MEM.clear()
    _CONFIG_MEM.update(cfg)


def _install_stub(module_name: str) -> Any:
    mod: Any = types.ModuleType(module_name)
    sys.modules[module_name] = mod
    return mod


def _ensure_cw_platform_stubs() -> None:
    for base in ("cw_platform", "cw_platform.idutils", "cw_platform.config_base"):
        if base not in sys.modules:
            _install_stub(base)

    sys.modules["cw_platform"].__path__ = []  # type: ignore[attr-defined]

    id_map = sys.modules["cw_platform.idutils"]
    id_map.canonical_key = _canonical_key  # type: ignore[attr-defined]
    id_map.minimal = _minimal  # type: ignore[attr-defined]
    id_map.ids_from = _ids_from  # type: ignore[attr-defined]
    id_map.ids_from_guid = _ids_from_guid  # type: ignore[attr-defined]

    cfg = sys.modules["cw_platform.config_base"]
    cfg.load_config = _load_config  # type: ignore[attr-defined]
    cfg.save_config = _save_config  # type: ignore[attr-defined]
    cfg.normalize_instance_id = _normalize_instance_id  # type: ignore[attr-defined]
    cfg.ensure_instance_block = _ensure_instance_block  # type: ignore[attr-defined]
    cfg.get_provider_block = _get_provider_block  # type: ignore[attr-defined]


class _AuthProviderStub:
    def __init__(self) -> None:
        self._tokens: dict[str, dict[str, str]] = {}

    def get_auth_token(self, provider: str, instance_id: str | None = None) -> str | None:
        inst = _normalize_instance_id(instance_id)
        return self._tokens.get(provider, {}).get(inst)

    def set_auth_token(self, provider: str, token: str, instance_id: str | None = None) -> None:
        inst = _normalize_instance_id(instance_id)
        self._tokens.setdefault(provider, {})[inst] = token


def _ensure_auth_stubs() -> None:
    for base in ("providers", "providers.auth", "providers.auth._auth_TRAKT"):
        if base in sys.modules:
            continue
        _install_stub(base)
    sys.modules["providers.auth._auth_TRAKT"].PROVIDER = _AuthProviderStub()  # type: ignore[attr-defined]

    for base in ("auth", "auth._auth_TRAKT"):
        if base in sys.modules:
            continue
        _install_stub(base)
    sys.modules["auth._auth_TRAKT"].PROVIDER = _AuthProviderStub()  # type: ignore[attr-defined]


@pytest.fixture(autouse=True, scope="session")
def _bootstrap_test_env() -> None:
    _ensure_sys_path()
    _ensure_cw_platform_stubs()
    _ensure_auth_stubs()

    # Logs off
    os.environ.setdefault("CW_LOG_LEVEL", "off")
