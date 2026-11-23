# services/hub_changes.py
# CrossWatch - Lightweight scoped Hub change notifications
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import hashlib
import threading
import uuid
from collections.abc import Mapping
from typing import Any

from cw_platform.provider_instances import normalize_instance_id

_LOCK = threading.Lock()
_EPOCH = uuid.uuid4().hex
_CHANGES: dict[tuple[str, str], int] = {}
_SEQUENCE = 0


def mark_changed(provider: str, instance_id: str = "default") -> None:
    global _SEQUENCE
    key = (str(provider or "").strip().upper(), normalize_instance_id(instance_id))
    if not key[0]:
        return
    with _LOCK:
        _SEQUENCE += 1
        _CHANGES[key] = _SEQUENCE


def revision(user_filter: Mapping[str, Any]) -> str:
    instances = user_filter.get("instances", user_filter)
    with _LOCK:
        values = [value for (provider, instance), value in _CHANGES.items()
                  if not user_filter or instance in {normalize_instance_id(item) for item in instances.get(provider, [])}]
        value = max(values, default=0)
    return hashlib.sha256(f"{_EPOCH}:{value}".encode()).hexdigest()
