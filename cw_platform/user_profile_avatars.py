# cw_platform/user_profile_avatars.py
# CrossWatch - Profile pictures set by the administrator
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .config_base import CONFIG as CONFIG_DIR
from .provider_instances import normalize_user_profile_id, user_profiles_root

_AVATAR_FILE = re.compile(r"[a-f0-9]{32}\.(png|jpg|webp)")


def avatar_dir() -> Path:
    path = Path(CONFIG_DIR) / "profile_avatars"
    path.mkdir(parents=True, exist_ok=True)
    return path


def avatar_file_path(filename: Any) -> Path | None:
    name = Path(str(filename or "")).name
    if not _AVATAR_FILE.fullmatch(name):
        return None
    root = avatar_dir().resolve()
    path = (root / name).resolve()
    return path if path.parent == root else None


def profile_avatar(cfg: Mapping[str, Any], profile_id: Any) -> dict[str, Any] | None:
    pid = normalize_user_profile_id(profile_id)
    block = user_profiles_root(cfg).get(pid) if pid else None
    avatar = block.get("avatar") if isinstance(block, Mapping) else None
    if not isinstance(avatar, Mapping):
        return None
    path = avatar_file_path(avatar.get("file"))
    if path is None or not path.exists():
        return None
    return {**dict(avatar), "path": path}


def profile_avatar_url(cfg: Mapping[str, Any], profile_id: Any) -> str:
    avatar = profile_avatar(cfg, profile_id)
    if avatar is None:
        return ""
    return f"/api/user-profiles/{normalize_user_profile_id(profile_id)}/avatar?ts={int(avatar.get('updated_at') or 0)}"


def remove_avatar_file(avatar: Any) -> None:
    path = avatar_file_path(avatar.get("file")) if isinstance(avatar, Mapping) else None
    if path is not None:
        try:
            path.unlink(missing_ok=True)
        except Exception:
            pass
