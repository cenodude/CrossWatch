# providers/scrobble/playing_routes.py
# CrossWatch - Resolve observed playback routes for the playing card
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import logging
import time
from typing import Any

from cw_platform.event_archive.db import get_conn
from cw_platform.event_archive.scrobble_recorder import _id_token
from cw_platform.provider_instances import get_instance_block, normalize_instance_id, sanitize_instance_label


def attach_playing_routes(streams: list[dict[str, Any]], cfg: dict[str, Any], user_filter: dict[str, Any]) -> None:
    if not streams:
        return
    try:
        conn = get_conn()
        if conn is None:
            return
        for stream in streams:
            stream["routes"] = []
            session = str(stream.get("session_key") or "").strip()
            if not session:
                continue
            source = str(stream.get("source") or "").lower()
            kind = "webhook" if source in {"plextrakt", "embytrakt", "jellyfintrakt"} else "watcher"
            provider = source.removesuffix("trakt") if kind == "webhook" else source
            instance = normalize_instance_id(stream.get("provider_instance"))
            cutoff = max(int(time.time()) - 86400, int(stream.get("started") or 0) - 10)
            rows = conn.execute(
                "SELECT source_provider, source_instance, destination_provider, destination_instance, "
                "source_kind, item_key, title, year, media_type, season, episode, detail FROM events "
                "WHERE event_type='scrobble_playback' AND source_provider=? AND source_instance=? "
                "AND session_key=? AND source_kind=? AND created_at>=? ORDER BY created_at DESC, id DESC LIMIT 100",
                (provider.upper(), instance, session, kind, cutoff),
            ).fetchall()
            seen: set[tuple[str, str, str]] = set()
            for row in rows:
                try:
                    detail = json.loads(row["detail"] or "{}")
                except (ValueError, TypeError):
                    continue
                if not isinstance(detail, dict) or detail.get("state") not in {"running", "paused"}:
                    continue
                if row["media_type"] != stream.get("media_type"):
                    continue
                if any(str(row[key] if row[key] is not None else "") != str(stream.get(key) if stream.get(key) is not None else "") for key in ("season", "episode")):
                    continue
                item_key = _id_token(stream.get("ids") or {})
                if item_key and row["item_key"]:
                    if item_key != row["item_key"]:
                        continue
                elif str(row["title"] or "").casefold() != str(stream.get("title") or "").casefold():
                    continue
                account = str(stream.get("account") or "").strip().casefold()
                observed_account = str(detail.get("account") or "").strip().casefold()
                if account and observed_account and account != observed_account:
                    continue
                destination = str(row["destination_provider"] or "").lower()
                target_instance = normalize_instance_id(row["destination_instance"])
                if user_filter:
                    allowed = (user_filter.get("instances") or {}).get(destination.upper(), [])
                    if target_instance not in {normalize_instance_id(value) for value in allowed}:
                        continue
                route_id = str(detail.get("route_id") or "")
                key = (destination, target_instance, route_id)
                if not destination or key in seen:
                    continue
                seen.add(key)
                def label(name: str, inst: str) -> str:
                    return sanitize_instance_label(get_instance_block(cfg, name, inst).get("label")) or ("Default" if inst == "default" else inst)
                stream["routes"].append({
                    "id": route_id or f"{kind}:{provider}:{instance}:{destination}:{target_instance}",
                    "kind": kind,
                    "source": provider,
                    "source_instance": instance,
                    "source_label": label(provider, instance),
                    "destination": destination,
                    "destination_instance": target_instance,
                    "destination_label": label(destination, target_instance),
                })
    except Exception:
        logging.getLogger("crosswatch.scrobble").debug("playing route lookup failed", exc_info=True)
