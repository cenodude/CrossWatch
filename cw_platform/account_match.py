# cw_platform/account_match.py
# CrossWatch - Media account matching helpers
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any


def normalize_media_account_name(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def media_account_allowed(allow: Any, account: Any = "", *, account_id: Any = "", account_uuid: Any = "", user_id: Any = "", default_allow: bool = True) -> bool:
    if not allow:
        return bool(default_allow)
    has_mapping_account = isinstance(account, Mapping)
    if has_mapping_account and isinstance(account.get("Account"), Mapping):
        acc = account.get("Account")
    elif has_mapping_account:
        acc = account
    else:
        acc = {}
    flat = "" if has_mapping_account else str(account or "").strip()
    title = str((acc.get("title") if isinstance(acc, Mapping) else "") or flat).strip()
    acc_id = str((acc.get("id") if isinstance(acc, Mapping) else "") or account_id or "").strip()
    acc_uuid = str((acc.get("uuid") if isinstance(acc, Mapping) else "") or account_uuid or "").strip().lower()
    values = allow if isinstance(allow, list) else [allow]
    for entry in values:
        raw = str(entry or "").strip()
        if not raw:
            continue
        lowered = raw.lower()
        if lowered.startswith("id:"):
            wanted = lowered.split(":", 1)[1].strip()
            if wanted and wanted == acc_id.lower():
                return True
            continue
        if lowered.startswith("uuid:"):
            wanted = lowered.split(":", 1)[1].strip()
            if wanted and wanted == acc_uuid:
                return True
            continue
        if normalize_media_account_name(raw) == normalize_media_account_name(title):
            return True
    return False
