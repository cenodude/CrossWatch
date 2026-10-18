# cw_platform/orchestrator/_dropped.py
# Dropped show modes and dropped tagging on the target for the orchestrator.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from ..id_map import canonical_key
from ..run_control import cancel_requested
from ._scope import provider_call

DROPPED_MODES = ("sync", "ignore", "tag")
TAG_OPERATION = "tag_dropped"
TAG_EVENT_ITEMS = 100


def provider_dropped_mode(cfg: Mapping[str, Any], provider_key: str, feature: str) -> str:
    try:
        blk = cfg.get(str(provider_key or "").strip().lower()) or {}
    except Exception:
        return "sync"
    if not isinstance(blk, Mapping):
        return "sync"
    name = str(feature or "").strip().lower()
    mode = str(blk.get(f"{name}_dropped_mode") or "").strip().lower()
    if mode in DROPPED_MODES:
        return mode
    return "ignore" if bool(blk.get(f"{name}_ignore_dropped_shows", False)) else "sync"


def reads_dropped(ops: Any) -> bool:
    return callable(getattr(ops, "dropped_show_tokens", None))


def tags_dropped(ops: Any) -> bool:
    return callable(getattr(ops, "tag_dropped_shows", None))


def show_level_tokens(item: Mapping[str, Any]) -> set[str]:
    ids = item.get("show_ids") if isinstance(item.get("show_ids"), Mapping) else None
    if not ids:
        ids = item.get("ids") if isinstance(item.get("ids"), Mapping) else None
    out: set[str] = set()
    try:
        if str(item.get("type") or "").lower() == "show":
            ck = canonical_key(item)
            if ck:
                out.add(str(ck))
    except Exception:
        pass
    if isinstance(ids, Mapping):
        for k, v in ids.items():
            if v is None:
                continue
            sv = str(v).strip()
            if not sv:
                continue
            out.add(f"{str(k).lower()}:{sv.lower()}")
    return out


def _is_movie(item: Mapping[str, Any]) -> bool:
    return str(item.get("type") or "").strip().lower() in ("movie", "movies")


def matches_dropped_show(item: Mapping[str, Any], dropped_tokens: set[str]) -> bool:
    if not dropped_tokens or not isinstance(item, Mapping):
        return False
    if _is_movie(item):
        return False
    return bool(show_level_tokens(item) & dropped_tokens)


def _show_ids(item: Mapping[str, Any]) -> dict[str, Any]:
    raw = item.get("show_ids") if isinstance(item.get("show_ids"), Mapping) and item.get("show_ids") else item.get("ids")
    return {str(k): v for k, v in dict(raw or {}).items() if v not in (None, "")}


def _show_title(item: Mapping[str, Any]) -> Any:
    if str(item.get("type") or "").strip().lower() == "show":
        return item.get("title")
    return item.get("series_title") or item.get("show_title") or item.get("grandparent_title")


def _sorted_values(index: Mapping[str, Any]) -> list[Any]:
    return [index[key] for key in sorted(index or {}, key=str)]


def _tag_pairs(
    source: Mapping[str, Any],
    source_tokens: set[str],
    target: Mapping[str, Any],
) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    if not source_tokens:
        return []
    present: dict[str, dict[str, Any]] = {}
    for item in _sorted_values(target):
        if not isinstance(item, Mapping) or _is_movie(item):
            continue
        ids = _show_ids(item)
        for token in show_level_tokens(item):
            present.setdefault(token, ids)
    out: list[tuple[dict[str, Any], dict[str, Any]]] = []
    seen: set[str] = set()
    for item in _sorted_values(source):
        if not isinstance(item, Mapping) or not matches_dropped_show(item, source_tokens):
            continue
        tokens = show_level_tokens(item)
        if tokens & seen:
            continue
        hit = next((present[token] for token in sorted(tokens) if token in present), None)
        if hit is None:
            continue
        show: dict[str, Any] = {"type": "show", "ids": {**_show_ids(item), **hit}}
        merged_tokens = show_level_tokens(show)
        if merged_tokens & seen:
            seen |= tokens | merged_tokens
            continue
        seen |= tokens | merged_tokens
        review_item: dict[str, Any] = {"type": "show", "ids": _show_ids(item)}
        title = _show_title(item)
        if title:
            show["title"] = title
            review_item["title"] = title
        out.append((review_item, show))
    return out


def dropped_tag_candidates(
    source: Mapping[str, Any],
    source_tokens: set[str],
    target: Mapping[str, Any],
) -> list[dict[str, Any]]:
    return [show for _, show in _tag_pairs(source, source_tokens, target)]


def _planned_tags(
    *,
    src: str,
    src_cfg: Mapping[str, Any],
    dst_ops: Any,
    feature: str,
    source: Mapping[str, Any],
    source_tokens: set[str],
    target: Mapping[str, Any],
    target_tokens: Callable[[], set[str]],
) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    if str(feature or "").strip().lower() != "history" or not source_tokens or not tags_dropped(dst_ops):
        return []
    if provider_dropped_mode(src_cfg, src, feature) != "tag":
        return []
    pairs = _tag_pairs(source, source_tokens, target)
    if pairs and reads_dropped(dst_ops):
        already = target_tokens()
        pairs = [(item, show) for item, show in pairs if not (show_level_tokens(show) & already)]
    return pairs


def _review_tags(review: Any, pairs: list[tuple[dict[str, Any], dict[str, Any]]], *, src: str, src_inst: str,
                 dst: str, dst_inst: str, feature: str, down: bool) -> list[dict[str, Any]]:
    items = [item for item, _ in pairs]
    kept = review.filter(feature, dst, dst_inst, TAG_OPERATION, items, source=src, source_instance=src_inst, down=down)
    chosen = {id(item) for item in kept}
    return [show for item, show in pairs if id(item) in chosen]


def preview_dropped_tags(
    *,
    review: Any,
    src: str,
    src_inst: str,
    dst: str,
    dst_inst: str,
    src_cfg: Mapping[str, Any],
    dst_ops: Any,
    feature: str,
    source: Mapping[str, Any],
    source_tokens: set[str],
    target: Mapping[str, Any],
    target_tokens: Callable[[], set[str]],
    down: bool,
) -> None:
    pairs = _planned_tags(src=src, src_cfg=src_cfg, dst_ops=dst_ops, feature=feature, source=source,
                          source_tokens=source_tokens, target=target, target_tokens=target_tokens)
    if pairs:
        _review_tags(review, pairs, src=src, src_inst=src_inst, dst=dst, dst_inst=dst_inst, feature=feature, down=down)


def apply_dropped_tags(
    *,
    src: str,
    dst: str,
    src_cfg: Mapping[str, Any],
    dst_ops: Any,
    dst_cfg: Mapping[str, Any],
    feature: str,
    source: Mapping[str, Any],
    source_tokens: set[str],
    target: Mapping[str, Any],
    target_tokens: Callable[[], set[str]],
    dry_run: bool,
    emit: Callable[..., Any],
    review: Any = None,
    src_inst: str = "default",
    dst_inst: str = "default",
) -> int:
    pairs = _planned_tags(src=src, src_cfg=src_cfg, dst_ops=dst_ops, feature=feature, source=source,
                          source_tokens=source_tokens, target=target, target_tokens=target_tokens)
    if review is not None and pairs:
        shows = _review_tags(review, pairs, src=src, src_inst=src_inst, dst=dst, dst_inst=dst_inst, feature=feature, down=False)
    else:
        shows = [show for _, show in pairs]
    if not shows:
        return 0
    if cancel_requested():
        emit("debug", msg="provider.dropped.tag_cancelled", provider=dst, source=src, feature=feature, planned=len(shows))
        return 0
    if dry_run:
        emit("debug", msg="provider.dropped.tag", provider=dst, source=src, feature=feature, planned=len(shows), dry_run=True)
        return 0
    listed = [{"title": show.get("title"), "ids": dict(show.get("ids") or {})} for show in shows[:TAG_EVENT_ITEMS]]
    try:
        res = provider_call(lambda view: dst_ops.tag_dropped_shows(view, shows), dst_cfg)
    except Exception as exc:
        emit("debug", msg="provider.dropped.tag_failed", provider=dst, source=src, feature=feature, error=str(exc)[:200])
        emit("dropped:tagged", dst=dst, destination_instance=dst_inst, feature=feature, attempted=len(shows), confirmed=0,
             skipped=0, not_found=0, errors=len(shows), error=str(exc)[:200], shows=listed)
        return 0
    res = res if isinstance(res, Mapping) else {}
    confirmed = int(res.get("confirmed") or 0)
    emit("dropped:tagged", dst=dst, destination_instance=dst_inst, feature=feature, attempted=len(shows), confirmed=confirmed,
         skipped=int(res.get("skipped") or 0), not_found=int(res.get("not_found") or 0), errors=int(res.get("errors") or 0),
         shows=listed)
    emit(
        "debug",
        msg="provider.dropped.tagged",
        provider=dst,
        source=src,
        feature=feature,
        attempted=len(shows),
        confirmed=confirmed,
        skipped=int(res.get("skipped") or 0),
        not_found=int(res.get("not_found") or 0),
        errors=int(res.get("errors") or 0),
    )
    return confirmed
