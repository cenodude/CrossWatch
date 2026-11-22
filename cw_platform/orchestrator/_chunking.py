from __future__ import annotations

from collections.abc import Mapping
from typing import Any

LOCAL_CHUNK_SIZES: dict[str, int] = {"CROSSWATCH": 5000}


def _local_default(base: int, key: str) -> int:
    local = LOCAL_CHUNK_SIZES.get(key, 0)
    return max(base, local) if base > 0 and local > 0 else base


def effective_chunk_size(ctx: Any, provider_name: str) -> int:
    base = int(getattr(ctx, "apply_chunk_size", 0) or 0)
    raw = getattr(ctx, "apply_chunk_size_by_provider", None)
    key = str(provider_name or "").upper()
    if not isinstance(raw, Mapping):
        return _local_default(base, key)
    v = raw.get(key) if hasattr(raw, "get") else None
    if v is None:
        for k, vv in raw.items():
            if str(k).upper() == key:
                v = vv
                break
    try:
        n = int(v) if v is not None else 0
    except Exception:
        n = 0
    return n if n > 0 else _local_default(base, key)
