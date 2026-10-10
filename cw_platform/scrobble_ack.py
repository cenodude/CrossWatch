# cw_platform/scrobble_ack.py
# CrossWatch - In-process receipts for confirmed scrobble completions
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any

from .provider_instances import normalize_instance_id

_RECEIPT: ContextVar[dict[str, Any] | None] = ContextVar("scrobble_completion_receipt", default=None)


@contextmanager
def delivery_receipt(token, target, instance):
    receipt = {"token": token, "target": str(target).lower(), "instance": normalize_instance_id(instance), "confirmed": False}
    context = _RECEIPT.set(receipt)
    try:
        yield receipt
    finally:
        _RECEIPT.reset(context)


def confirm_scrobble(event, target, instance):
    receipt = _RECEIPT.get()
    if receipt is None:
        return
    raw = getattr(event, "raw", None) or {}
    if (raw.get("_cw_episode_group_delivery") == receipt["token"] and str(target).lower() == receipt["target"]
            and normalize_instance_id(instance) == receipt["instance"]):
        receipt["confirmed"] = True
