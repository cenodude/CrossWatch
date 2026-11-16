# /providers/sync/silo/_continue.py
# Silo Continue Watching section read and home dismissal writes
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any, Iterable, Mapping
from urllib.parse import quote

from providers.sync._continue_watching import SHOWN

from ._common import SiloSyncError, iso_z, key_of, library, minimal_for, problem_of, safe_json, silo_request, _info, _warn
from ._progress import FEATURE, _float, in_progress_rows

LOG = "continue_watching"
SECTION_TYPE = "continue_watching"


def _section_id(adapter: Any) -> str:
    resp = silo_request(adapter, "GET", "/home/sections", params={"item_limit": 1})
    if resp.status_code != 200:
        raise SiloSyncError(f"continue_watching_sections_failed:{resp.status_code}:{problem_of(resp)}")
    body = safe_json(resp)
    sections = body.get("sections") if isinstance(body, Mapping) else None
    for section in sections or []:
        if isinstance(section, Mapping) and section.get("section_type") == SECTION_TYPE and section.get("id"):
            return str(section["id"])
    raise SiloSyncError("continue_watching_section_missing")


def _cards(adapter: Any) -> list[dict[str, Any]]:
    section = _section_id(adapter)
    resp = silo_request(adapter, "GET", f"/home/sections/{quote(section)}/items", params={"item_limit": 100})
    if resp.status_code != 200:
        raise SiloSyncError(f"continue_watching_read_failed:{resp.status_code}:{problem_of(resp)}")
    body = safe_json(resp)
    items = body.get("items") if isinstance(body, Mapping) else None
    if not isinstance(items, list):
        raise SiloSyncError("continue_watching_read_invalid")
    return [dict(card) for card in items if isinstance(card, Mapping) and card.get("content_id")]


def view(adapter: Any) -> dict[str, dict[str, Any]]:
    lib = library(adapter, feature=FEATURE)
    rows = in_progress_rows(adapter)
    cards = _cards(adapter)
    by_content = {str(card["content_id"]): card for card in cards}
    by_series = {str(card["series_id"]): card for card in cards if card.get("series_id")}
    out: dict[str, dict[str, Any]] = {}
    for cid, row in rows.items():
        item = minimal_for(lib, cid)
        pos = _float(row.get("position_seconds"))
        if not item or item.get("type") not in ("movie", "episode") or not pos or pos <= 0:
            continue
        item["progress_ms"] = int(round(pos * 1000.0))
        updated = iso_z(row.get("updated_at"))
        if updated:
            item["progress_at"] = updated
        if item["type"] == "movie":
            card = by_content.get(cid)
        else:
            rec = lib.episodes.get(cid) or {}
            card = by_series.get(str(rec.get("series_id") or ""))
        item[SHOWN] = card is not None
        if card is not None:
            item["_silo_card"] = {k: card.get(k) for k in ("content_id", "series_id", "item_source", "progress_updated_at")}
        key = key_of(item)
        if key:
            out[key] = item
    _info(LOG, "view_done", items=len(out), cards=len(cards), hidden=sum(1 for it in out.values() if it.get(SHOWN) is False))
    return out


def _dismissal(card: Mapping[str, Any]) -> tuple[str, dict[str, Any]] | None:
    if card.get("item_source") == "next_up" and card.get("series_id"):
        return "next_up", {"series_id": card["series_id"]}
    if card.get("progress_updated_at"):
        return "continue_watching", {"progress_updated_at": card["progress_updated_at"]}
    return None


def hide(adapter: Any, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    rows = list(items)
    confirmed: list[str] = []
    errors = 0
    done: dict[str, bool] = {}
    for item in rows:
        card = item.get("_silo_card")
        plan = _dismissal(card) if isinstance(card, Mapping) else None
        if not isinstance(card, Mapping) or not plan:
            errors += 1
            continue
        cid = str(card["content_id"])
        if cid not in done:
            surface, body = plan
            resp = silo_request(adapter, "PUT", f"/home/dismissals/{surface}/{quote(cid)}", json_body=body)
            done[cid] = resp.status_code in (200, 204)
            if not done[cid]:
                _warn(LOG, "hide_failed", content_id=cid, status=resp.status_code, problem=problem_of(resp))
        if done[cid]:
            confirmed.append(str(item.get("_cw_key") or ""))
        else:
            errors += 1
    return {"attempted": len(rows), "confirmed_keys": [k for k in confirmed if k], "errors": errors}
