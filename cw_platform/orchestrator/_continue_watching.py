# cw_platform/orchestrator/_continue_watching.py
# Continue Watching removals between media servers: infer hides from row snapshots and hide them on the other side.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime
from typing import Any

from providers.sync import _continue_watching as registry

from ..id_map import canonical_key
from ..interactive_reads import read_once
from ..local_db import continue_rows as store
from ..run_control import cancel_requested
from ._pairs_utils import filter_manual_block
from ._scope import provider_call
from ._specials import is_special, specials_excluded

OPTION = registry.OPTION
SHOWN = registry.SHOWN
REVIEW_OPERATION = "hide_continue_watching"
POSITION_TOLERANCE_MS = 2000
SHARED_IDS = ("imdb", "tmdb", "tvdb", "trakt", "simkl", "tvmaze", "mal", "anilist", "kitsu", "anidb")
EVENT_ITEMS = 100

Side = tuple[str, str, Any, Mapping[str, Any]]


def requested(feature: str, fcfg: Mapping[str, Any] | None) -> bool:
    return str(feature or "").lower() == "progress" and bool((fcfg or {}).get(OPTION))


def pair_supported(a: str, b: str) -> bool:
    return registry.supported(a) and registry.supported(b)


def _epoch(value: Any) -> float | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        pass
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _ms(item: Mapping[str, Any]) -> int:
    try:
        return int(item.get("progress_ms") or 0)
    except (TypeError, ValueError):
        return 0


def snapshot(view: Mapping[str, Mapping[str, Any]], retry: set[str] | None = None) -> dict[str, Any]:
    items: dict[str, dict[str, Any]] = {}
    for key, item in view.items():
        if not isinstance(item, Mapping) or item.get(SHOWN) is None:
            continue
        entry: dict[str, Any] = {"shown": bool(item.get(SHOWN)), "ms": _ms(item), "at": str(item.get("progress_at") or "")}
        if str(key) in (retry or ()):
            entry["pending"] = True
        items[str(key)] = entry
    return {"items": items}


def _id_map(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _show_tokens(item: Mapping[str, Any]) -> set[str]:
    if str(item.get("type") or "").lower() != "episode":
        return set()
    show_ids = _id_map(item.get("show_ids"))
    out = {f"show:{name}:{str(show_ids.get(name)).strip().lower()}" for name in SHARED_IDS if str(show_ids.get(name) or "").strip()}
    if not out and str(item.get("series_title") or "").strip():
        out.add(f"show-title:{str(item['series_title']).strip().lower()}")
    return out


def _card(key: str, item: Mapping[str, Any]) -> str:
    tokens = _show_tokens(item)
    return min(tokens) if tokens else f"item:{key}"


def row_cutoff(view: Mapping[str, Mapping[str, Any]]) -> float | None:
    newest: dict[str, float] = {}
    for key, item in view.items():
        if item.get(SHOWN) is not True:
            continue
        at = _epoch(item.get("progress_at"))
        if at is None:
            continue
        card = _card(str(key), item)
        newest[card] = max(at, newest.get(card, at))
    return min(newest.values()) if newest else None


def detect(prev: Mapping[str, Any] | None, view: Mapping[str, Mapping[str, Any]]) -> list[str]:
    before = (prev or {}).get("items") if isinstance(prev, Mapping) else None
    if not isinstance(before, Mapping) or not before:
        return []
    oldest = row_cutoff(view)
    out: list[str] = []
    for key, item in view.items():
        if item.get(SHOWN) is not False:
            continue
        seen = before.get(key)
        if not isinstance(seen, Mapping):
            continue
        pending = seen.get("pending") is True
        if not pending and (seen.get("shown") is not True or oldest is None):
            continue
        if abs(_ms(item) - int(seen.get("ms") or 0)) > POSITION_TOLERANCE_MS:
            continue
        at = str(item.get("progress_at") or "")
        if not at or at != str(seen.get("at") or ""):
            continue
        stamp = _epoch(at)
        if stamp is None or (not pending and oldest is not None and stamp < oldest):
            continue
        out.append(str(key))
    return out


def match_tokens(item: Mapping[str, Any]) -> set[str]:
    kind = str(item.get("type") or "").lower()
    out: set[str] = set()
    ids = _id_map(item.get("ids"))
    if kind == "movie":
        for name in SHARED_IDS:
            value = str(ids.get(name) or "").strip().lower()
            if value:
                out.add(f"movie:{name}:{value}")
        return out
    if kind != "episode":
        return out
    season, episode = item.get("season"), item.get("episode")
    show_ids = _id_map(item.get("show_ids"))
    if season is not None and episode is not None:
        for name in SHARED_IDS:
            value = str(show_ids.get(name) or "").strip().lower()
            if value:
                out.add(f"episode:{name}:{value}:{int(season)}:{int(episode)}")
    for name in SHARED_IDS:
        value = str(ids.get(name) or "").strip().lower()
        if value:
            out.add(f"episode-id:{name}:{value}")
    return out


def plan(removed: list[str], source: Mapping[str, Mapping[str, Any]],
         target: Mapping[str, Mapping[str, Any]]) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    alias: dict[str, str] = {}
    show_latest: dict[str, float] = {}
    for key, item in target.items():
        for token in match_tokens(item):
            alias.setdefault(token, key)
        at = _epoch(item.get("progress_at"))
        if at is not None:
            for token in _show_tokens(item):
                show_latest[token] = max(at, show_latest.get(token, at))
    out: list[tuple[dict[str, Any], dict[str, Any]]] = []
    taken: set[str] = set()
    for key in removed:
        src = source.get(key)
        if not isinstance(src, Mapping):
            continue
        hit = next((alias[t] for t in sorted(match_tokens(src)) if t in alias), None)
        if not hit or hit in taken:
            continue
        dst = target[hit]
        if dst.get(SHOWN) is not True:
            continue
        src_at = _epoch(src.get("progress_at"))
        dst_times = [t for t in [_epoch(dst.get("progress_at"))] + [show_latest.get(tok) for tok in _show_tokens(dst)] if t is not None]
        if src_at is not None and dst_times and max(dst_times) > src_at:
            continue
        taken.add(hit)
        out.append(({**dict(src), "_cw_key": key}, {**dict(dst), "_cw_key": hit}))
    return out


def _title(item: Mapping[str, Any]) -> str:
    if str(item.get("type") or "") == "episode":
        name = str(item.get("series_title") or item.get("title") or "")
        if item.get("season") is not None and item.get("episode") is not None:
            return f"{name} S{int(item['season']):02d}E{int(item['episode']):02d}".strip()
        return name
    return str(item.get("title") or "")


def _read(provider: str, instance: str, ops: Any, cfg: Mapping[str, Any],
          emit: Callable[..., Any]) -> dict[str, dict[str, Any]] | None:
    try:
        return provider_call(lambda view: read_once(["continue_watching", provider, instance],
                                                    lambda: registry.read_view(provider, ops, view)), cfg)
    except Exception as exc:
        emit("debug", msg="continue_watching.read_failed", provider=provider, error=str(exc)[:200])
        return None


def _reviewed(review: Any, src: Side, dst: Side,
              pairs: list[tuple[dict[str, Any], dict[str, Any]]]) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    targets = [target for _source, target in pairs]
    before = {canonical_key(target): target for target in targets}
    kept = review.filter("progress", dst[0], dst[1], REVIEW_OPERATION, targets,
                         source=src[0], source_instance=src[1], before=before)
    keep = {str(target.get("_cw_key")) for target in kept}
    return [(source, target) for source, target in pairs if str(target.get("_cw_key")) in keep]


def _apply(src: Side, dst: Side, pairs: list[tuple[dict[str, Any], dict[str, Any]]], *, dry_run: bool,
           emit: Callable[..., Any]) -> set[str]:
    src_name = src[0]
    dst_name, dst_inst, dst_ops, dst_cfg = dst
    if not pairs:
        return set()
    titles = [_title(target) for _source, target in pairs[:EVENT_ITEMS]]
    if dry_run:
        emit("debug", msg="continue_watching.hide", provider=dst_name, source=src_name, planned=len(pairs), dry_run=True)
        return set()
    targets = [target for _source, target in pairs]
    try:
        res = provider_call(lambda view: registry.hide(dst_name, dst_ops, view, targets), dst_cfg)
    except Exception as exc:
        emit("debug", msg="continue_watching.hide_failed", provider=dst_name, source=src_name, error=str(exc)[:200])
        emit("continue_watching:hidden", dst=dst_name, destination_instance=dst_inst, attempted=len(pairs), confirmed=0,
             errors=len(pairs), error=str(exc)[:200], items=titles)
        return set()
    confirmed = {str(k) for k in (res.get("confirmed_keys") or []) if k}
    errors = int(res.get("errors") or 0)
    emit("continue_watching:hidden", dst=dst_name, destination_instance=dst_inst, attempted=len(pairs),
         confirmed=len(confirmed), errors=errors, items=titles)
    emit("debug", msg="continue_watching.hidden", provider=dst_name, source=src_name, attempted=len(pairs),
         confirmed=len(confirmed), errors=errors)
    return confirmed


def sync_removals(
    state_store: Any,
    *,
    src: Side,
    dst: Side,
    two_way: bool,
    dry_run: bool,
    emit: Callable[..., Any],
    fcfg: Mapping[str, Any] | None = None,
    blocked: set[str] | None = None,
    review: Any = None,
) -> int:
    scope = getattr(state_store, "pair_scope", None)
    base = getattr(state_store, "base_path", None)
    if not scope or base is None:
        emit("debug", msg="continue_watching.no_pair_scope")
        return 0
    if not pair_supported(src[0], dst[0]):
        emit("debug", msg="continue_watching.unsupported_pair", src=src[0], dst=dst[0])
        return 0
    views: dict[int, dict[str, dict[str, Any]] | None] = {}
    for idx, (name, inst, ops, cfg) in enumerate((src, dst)):
        views[idx] = _read(name, inst, ops, cfg, emit)
    if views[0] is None or views[1] is None:
        return 0
    no_specials = specials_excluded("progress", fcfg)
    sides = (src, dst)
    directions = [(0, 1), (1, 0)] if two_way else [(0, 1)]
    prev = {idx: store.load(base, scope, sides[idx][0], sides[idx][1]) for idx in (0, 1)}
    planned: list[tuple[int, int, list[tuple[dict[str, Any], dict[str, Any]]], list[tuple[dict[str, Any], dict[str, Any]]]]] = []
    for from_idx, to_idx in directions:
        removed = detect(prev[from_idx], views[from_idx] or {})
        pairs = plan(removed, views[from_idx] or {}, views[to_idx] or {})
        if no_specials:
            pairs = [(s, t) for s, t in pairs if not is_special(s) and not is_special(t)]
        if blocked:
            pairs = [(s, t) for s, t in pairs if filter_manual_block([s], blocked) and filter_manual_block([t], blocked)]
        emit("debug", msg="continue_watching.detected", provider=sides[from_idx][0], removed=len(removed), planned=len(pairs))
        chosen = _reviewed(review, sides[from_idx], sides[to_idx], pairs) if review is not None and pairs else pairs
        planned.append((from_idx, to_idx, pairs, chosen))
    if review is not None and review.preview:
        return 0
    hidden_total = 0
    retry: dict[int, set[str]] = {0: set(), 1: set()}
    hidden: dict[int, set[str]] = {0: set(), 1: set()}
    for from_idx, to_idx, pairs, chosen in planned:
        if cancel_requested():
            emit("debug", msg="continue_watching.cancelled")
            return hidden_total
        confirmed = _apply(sides[from_idx], sides[to_idx], chosen, dry_run=dry_run, emit=emit)
        hidden[to_idx] |= confirmed
        retry[from_idx] |= {str(s.get("_cw_key")) for s, t in pairs if t["_cw_key"] not in confirmed}
        hidden_total += len(confirmed)
    if dry_run:
        return hidden_total
    for idx, keys in hidden.items():
        for key in keys:
            item = (views[idx] or {}).get(key)
            if isinstance(item, dict):
                item[SHOWN] = False
    for idx in (0, 1):
        name, inst = sides[idx][0], sides[idx][1]
        try:
            store.save(base, scope, name, inst, snapshot(views[idx] or {}, retry=retry[idx]))
        except Exception as exc:
            emit("debug", msg="continue_watching.save_failed", provider=name, error=str(exc)[:200])
    return hidden_total
