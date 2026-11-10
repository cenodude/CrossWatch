# cw_platform/orchestrator/_history_resets.py
# Trakt rewatch resets: hide watches before the reset and mark them unwatched on media servers.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import datetime as _dt
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from ..anime_mapping.history_coords import coordinate_tokens
from ..history_events import history_sync_key
from ..interactive_reads import read_once
from ..run_control import cancel_requested
from ..value_coercion import coerce_bool
from ._scope import provider_call

RESET_OPTION = "trakt_rewatch_resets"
RESET_SOURCE = "TRAKT"
RESET_TARGETS = frozenset({"PLEX", "JELLYFIN", "EMBY", "KODI", "TOFA"})
RESET_EVENT = "history:rewatch_reset"
RESET_OPERATION = "rewatch_reset"
RESET_UNAVAILABLE = "history:rewatch_reset_unavailable"
RESET_EVENT_ITEMS = 100

IndexSet = tuple[dict[str, Any], ...]


def _epoch(value: Any) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    try:
        text = str(value).strip().replace("Z", "+00:00").replace(" ", "T")
        parsed = _dt.datetime.fromisoformat(text)
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=_dt.timezone.utc)
    return int(parsed.timestamp())


def _is_episode(item: Mapping[str, Any]) -> bool:
    return str(item.get("type") or "").strip().lower() in ("episode", "episodes")


def _coords(item: Mapping[str, Any]) -> tuple[int, int] | None:
    try:
        return int(item["season"]), int(item["episode"])
    except Exception:
        return None


def _label(item: Mapping[str, Any]) -> str:
    show = item.get("series_title") or item.get("show_title") or item.get("grandparent_title") or item.get("title") or ""
    coords = _coords(item)
    if coords is None:
        return str(show)
    return f"{show} S{coords[0]:02d}E{coords[1]:02d}".strip()


def resets_requested(feature: str, fcfg: Mapping[str, Any] | None, trakt: str, server: str) -> bool:
    return (
        str(feature or "").strip().lower() == "history"
        and str(trakt or "").strip().upper() == RESET_SOURCE
        and str(server or "").strip().upper() in RESET_TARGETS
        and coerce_bool((fcfg or {}).get(RESET_OPTION, False))
    )


def show_tokens(item: Mapping[str, Any]) -> set[str]:
    kind = str(item.get("type") or "").strip().lower()
    ids = item.get("ids") if kind in ("show", "shows") else item.get("show_ids")
    if not isinstance(ids, Mapping):
        return set()
    return {f"{str(k).strip().lower()}:{str(v).strip().lower()}" for k, v in ids.items()
            if str(k).strip().lower() != "slug" and isinstance(v, (str, int)) and not isinstance(v, bool) and str(v).strip()}


def reset_before(resets: Mapping[str, int], item: Any) -> bool:
    if not resets or not isinstance(item, Mapping) or not _is_episode(item):
        return False
    hits = [resets[token] for token in show_tokens(item) if token in resets]
    watched = _epoch(item.get("watched_at"))
    return bool(hits) and watched is not None and watched < max(hits)


class ResetPlan:
    def __init__(self, shows: Sequence[Mapping[str, Any]], *, trakt_side: int, trakt: str, server: str) -> None:
        self.trakt_side = trakt_side
        self.server_side = 1 - trakt_side
        self.trakt = trakt
        self.server = server
        self.reset_at: dict[str, int] = {}
        self.unwatch: list[dict[str, Any]] = []
        self._rewatched: set[str] = set()
        self._aliases: Any = None
        for show in shows:
            if not isinstance(show, Mapping):
                raise RuntimeError("trakt_rewatch_resets_invalid")
            when = _epoch(show.get("reset_at"))
            ids = show.get("ids")
            if when is None or not isinstance(ids, Mapping) or not ids:
                continue
            for token in show_tokens({"type": "show", "ids": dict(ids)}):
                self.reset_at[token] = max(when, self.reset_at.get(token, when))

    def __bool__(self) -> bool:
        return bool(self.reset_at)

    def _reset_for(self, item: Mapping[str, Any]) -> int | None:
        if not _is_episode(item):
            return None
        hits = [self.reset_at[token] for token in show_tokens(item) if token in self.reset_at]
        return max(hits) if hits else None

    def _before_reset(self, item: Any) -> bool:
        return reset_before(self.reset_at, item)

    def _episode_keys(self, item: Mapping[str, Any]) -> set[str]:
        keys = set(coordinate_tokens(item))
        if self._aliases is not None:
            keys |= set(self._aliases.tokens(item))
        coords = _coords(item)
        if coords is not None:
            keys |= {f"{token}#s{coords[0]:02d}e{coords[1]:02d}" for token in show_tokens(item)}
        return keys

    def _observe(self, index: Mapping[str, Any]) -> None:
        for item in index.values():
            if not isinstance(item, Mapping):
                continue
            when = self._reset_for(item)
            watched = _epoch(item.get("watched_at"))
            if when is not None and watched is not None and watched >= when:
                self._rewatched |= self._episode_keys(item)

    def _hidden_on_server(self, item: Any) -> bool:
        return self._before_reset(item) and not (self._episode_keys(item) & self._rewatched)

    def _candidates(self, index: Mapping[str, Any]) -> list[dict[str, Any]]:
        groups: dict[str, list[tuple[str, Mapping[str, Any]]]] = {}
        for key, item in index.items():
            if not isinstance(item, Mapping) or self._reset_for(item) is None or item.get("_plex_cloud_history"):
                continue
            base = history_sync_key(item, key, event_mode=False)
            if base:
                groups.setdefault(base, []).append((str(key), item))
        out: list[dict[str, Any]] = []
        for base in sorted(groups):
            rows = groups[base]
            if not all(self._hidden_on_server(item) for _, item in rows):
                continue
            _, latest = max(rows, key=lambda row: _epoch(row[1].get("watched_at")) or 0)
            out.append({**dict(latest), "_cw_force_clear": True})
        return out

    def view(self, trakt: IndexSet, server: IndexSet, *, emit: Callable[..., Any], aliases: Any = None) -> tuple[IndexSet, IndexSet]:
        self._aliases = aliases if getattr(aliases, "enabled", False) else None
        for index in trakt[1:]:
            self._observe(index)
        self.unwatch = self._candidates(server[-1])
        trakt_out = tuple({k: v for k, v in index.items() if not self._before_reset(v)} for index in trakt)
        server_out = tuple({k: v for k, v in index.items() if not self._hidden_on_server(v)} for index in server)
        emit("debug", msg="history.rewatch_reset.view", trakt=self.trakt, server=self.server, shows=len(self.reset_at),
             trakt_hidden=sum(len(a) - len(b) for a, b in zip(trakt, trakt_out)),
             server_hidden=sum(len(a) - len(b) for a, b in zip(server, server_out)), unwatch=len(self.unwatch))
        return trakt_out, server_out

    def view_pair(self, side_a: IndexSet, side_b: IndexSet, *, emit: Callable[..., Any], aliases: Any = None) -> tuple[IndexSet, IndexSet]:
        if self.trakt_side == 0:
            return self.view(side_a, side_b, emit=emit, aliases=aliases)
        out_b, out_a = self.view(side_b, side_a, emit=emit, aliases=aliases)
        return out_a, out_b

    def trakt_manual(self, manual: Mapping[str, Any] | None) -> dict[str, Any]:
        return {k: v for k, v in (manual or {}).items() if not self._before_reset(v)}

    def server_manual(self, manual: Mapping[str, Any] | None) -> dict[str, Any]:
        return {k: v for k, v in (manual or {}).items() if not self._hidden_on_server(v)}

    def settle(self, planned: Sequence[Mapping[str, Any]] | None, *,
               alias: Callable[[dict[str, Any]], Any],
               find: Callable[[dict[str, Any], Any, Mapping[str, Any]], Any],
               unblocked: Callable[[list[dict[str, Any]]], list[dict[str, Any]]]) -> list[dict[str, Any]]:
        keep = list(unblocked(list(self.unwatch)))
        if planned and keep:
            index = {history_sync_key(item) or f"reset:{n}": item for n, item in enumerate(keep)}
            aliases = alias(index)
            claimed: set[int] = set()
            for item in planned:
                hit = find(index, aliases, item) if isinstance(item, Mapping) else None
                if isinstance(hit, Mapping):
                    claimed.add(id(hit))
            keep = [item for item in keep if id(item) not in claimed]
        return keep


def _marked_watched_read(server: str, cfg: Mapping[str, Any]) -> bool:
    if str(server or "").upper() != "PLEX":
        return True
    history = ((cfg or {}).get("plex") or {}).get("history") or {}
    value = history.get("include_marked_watched", True) if isinstance(history, Mapping) else True
    return coerce_bool(True if value is None else value, True)


def load_reset_plan(
    feature: str,
    fcfg: Mapping[str, Any] | None,
    sides: Sequence[tuple[str, Any, Mapping[str, Any]]],
    *,
    event_mode: bool,
    one_way: bool,
    emit: Callable[..., Any] | None = None,
) -> ResetPlan | None:
    if event_mode or len(sides) != 2:
        return None
    for trakt_side in ((0,) if one_way else (0, 1)):
        trakt, ops, cfg = sides[trakt_side]
        server, _, server_cfg = sides[1 - trakt_side]
        if not resets_requested(feature, fcfg, trakt, server):
            continue
        if not _marked_watched_read(server, server_cfg):
            if emit is not None:
                emit(RESET_UNAVAILABLE, provider=str(server).upper(), src=str(trakt).upper(), dst=str(server).upper(),
                     feature=feature, reason="Trakt rewatch resets were skipped because Plex is not reading marked watched items.",
                     detail="Turn on marked watched items for Plex in this pair to follow Trakt rewatch resets.")
            return None
        getter = getattr(ops, "rewatch_resets", None)
        if not callable(getter):
            return None
        shows = provider_call(lambda view: read_once(["rewatch_resets", ops.name()], lambda: getter(view)), cfg)
        if not isinstance(shows, list):
            raise RuntimeError("trakt_rewatch_resets_invalid")
        plan = ResetPlan(shows, trakt_side=trakt_side, trakt=str(trakt).upper(), server=str(server).upper())
        return plan if plan else None
    return None


def preview_reset_unwatches(
    plan: ResetPlan | None,
    *,
    review: Any,
    feature: str,
    dst_inst: str,
    src_inst: str,
    down: bool,
) -> None:
    if plan and plan.unwatch and review is not None:
        review.filter(feature, plan.server, dst_inst, RESET_OPERATION, list(plan.unwatch),
                      source=plan.trakt, source_instance=src_inst, down=down)


def apply_reset_unwatches(
    plan: ResetPlan | None,
    *,
    dst_ops: Any,
    dst_cfg: Mapping[str, Any],
    feature: str,
    dry_run: bool,
    emit: Callable[..., Any],
    review: Any = None,
    src_inst: str = "default",
    dst_inst: str = "default",
) -> int:
    if not plan or not plan.unwatch:
        return 0
    items = list(plan.unwatch)
    if review is not None:
        items = review.filter(feature, plan.server, dst_inst, RESET_OPERATION, items,
                              source=plan.trakt, source_instance=src_inst, down=False)
    if not items:
        return 0
    if cancel_requested():
        emit("debug", msg="history.rewatch_reset.cancelled", provider=plan.server, planned=len(items))
        return 0
    if dry_run:
        emit("debug", msg="history.rewatch_reset.planned", provider=plan.server, planned=len(items), dry_run=True)
        return 0
    listed = [_label(item) for item in items[:RESET_EVENT_ITEMS]]
    route = dict(src=plan.trakt, source_instance=src_inst, dst=plan.server, destination_instance=dst_inst, feature=feature)
    try:
        res = provider_call(lambda view: dst_ops.remove(view, items, feature=feature, dry_run=False), dst_cfg)
    except Exception as exc:
        emit(RESET_EVENT, **route, attempted=len(items), confirmed=0, not_found=0, errors=len(items),
             error=str(exc)[:200], items=listed)
        return 0
    res = res if isinstance(res, Mapping) else {}
    failed = res.get("ok") is False
    confirmed = 0 if failed else int(res.get("count") or 0)
    not_found = len(res.get("unresolved") or [])
    errors = len(items) if failed else int(res.get("errors") or 0)
    emit(RESET_EVENT, **route, attempted=len(items), confirmed=confirmed, not_found=not_found, errors=errors,
         error=str(res.get("error") or "")[:200] or None, items=listed)
    emit("debug", msg="history.rewatch_reset.unwatched", provider=plan.server, attempted=len(items),
         confirmed=confirmed, not_found=not_found, errors=errors)
    return confirmed
