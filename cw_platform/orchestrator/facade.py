# cw_platform/orchestrator/_facade.py
# orchestrator facade for higher-level operations.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any

from ._types import ConflictPolicy, InventoryOps
from ._providers import load_sync_providers
from ._state_store import StateStore
from ._logging import Emitter
from ._telemetry import Stats, maybe_emit_rate_warnings
from ._pairs import run_pairs as _run_pairs
from ._tombstones import (
    prune as _tomb_prune,
    keys_for_feature as _tomb_keys_for_feature,
    filter_with as _tomb_filter_with,
    cascade_removals as _tomb_cascade,
)
from ._snapshots import (
    build_snapshots_for_feature as _build_snaps,
    allowed_providers_for_feature as _allowed_pf,
    module_checkpoint as _module_cp,
    prev_checkpoint as _prev_cp,
    coerce_suspect_snapshot as _coerce,
)
from ._planner import diff as _plan_diff
from ._applier import apply_add as _apply_add, apply_remove as _apply_remove

__all__ = ["Orchestrator"]

# config
try:  # pragma: no cover - optional import
    from .. import config_base as _config_base_mod  # type: ignore[import]
    config_base: Any = _config_base_mod
except Exception:  # pragma: no cover
    class _ConfigBaseFallback:
        @staticmethod
        def CONFIG_BASE() -> str:
            return "./"

    config_base: Any = _ConfigBaseFallback()


@dataclass
class Orchestrator:
    config: Mapping[str, Any]
    on_progress: Callable[[str], None] | None = None
    conflict: ConflictPolicy = field(default_factory=ConflictPolicy)

    dry_run: bool = False
    only_feature: str | None = None
    write_state_json: bool = True
    state_path: Path | None = None

    files: StateStore | None = field(init=False, default=None)
    providers: dict[str, InventoryOps] = field(init=False, default_factory=dict)
    stats: Stats = field(init=False)

    # internal fields (set in __post_init__)
    debug: bool = field(init=False, default=False)
    emitter: Emitter = field(init=False)
    warn_thresholds: dict[str, int] = field(init=False, default_factory=dict)
    snap_cache: dict[tuple[str, str], tuple[float, dict[str, dict[str, Any]]]] = field(
        init=False, default_factory=dict
    )
    snap_ttl_sec: int = field(init=False, default=0)
    suspect_min_prev: int = field(init=False, default=0)
    suspect_shrink_ratio: float = field(init=False, default=0.0)
    suspect_debug: bool = field(init=False, default=False)
    apply_chunk_size: int = field(init=False, default=0)
    apply_chunk_pause_ms: int = field(init=False, default=0)

    def __post_init__(self) -> None:
        self.cfg: dict[str, Any] = dict(self.config or {})
        rt = dict(self.cfg.get("runtime") or {})
        self.debug = bool(rt.get("debug", False))

        self.emitter = Emitter(self.on_progress)
        self.emit = self.emitter.emit
        self.emit_info = self.emitter.info
        self.dbg = lambda *a, **k: self.emitter.dbg(self.debug, *a, **k)

        self.state_store = StateStore(Path(config_base.CONFIG_BASE()))
        self.files = self.state_store
        self.providers = load_sync_providers()

        # stats wrapper
        raw_stats: Any | None
        try:  # pragma: no cover
            import crosswatch as CW  # type: ignore[import]
            raw_stats = getattr(CW, "STATS", None)
        except Exception:
            raw_stats = None
        self.stats = raw_stats if isinstance(raw_stats, Stats) else Stats(raw_stats)

        telem_cfg = dict(
            self.cfg.get("telemetry")
            or (self.cfg.get("runtime", {}).get("telemetry") or {})
        )
        self.warn_thresholds = (
            telem_cfg.get("warn_rate_remaining")
            or {"TRAKT": 100, "SIMKL": 50, "PLEX": 0, "JELLYFIN": 0}
        )

        self.snap_ttl_sec = int(rt.get("snapshot_ttl_sec") or 0)
        self.suspect_min_prev = int(rt.get("suspect_min_prev", 20))
        self.suspect_shrink_ratio = float(rt.get("suspect_shrink_ratio", 0.10))
        self.suspect_debug = bool(rt.get("suspect_debug", True))
        self.apply_chunk_size = int(rt.get("apply_chunk_size") or 0)
        self.apply_chunk_pause_ms = int(rt.get("apply_chunk_pause_ms") or 0)

        self.emitter.info("[i] Orchestrator v3 ready (full compat shims)")

    # Context
    @property
    def context(self) -> Any:
        from types import SimpleNamespace

        return SimpleNamespace(
            config=self.cfg,
            providers=self.providers,
            emit=self.emitter.emit,
            emit_info=self.emitter.info,
            dbg=lambda *a, **k: self.emitter.dbg(self.debug, *a, **k),
            debug=self.debug,
            dry_run=self.dry_run,
            conflict=self.conflict,
            state_store=self.state_store,
            stats=self.stats,
            emit_rate_warnings=self.emit_rate_warnings,
            tomb_prune=self.prune_tombstones,
            only_feature=self.only_feature,
            write_state_json=self.write_state_json,
            state_path=self.state_path or self.state_store.state,
            snap_cache=self.snap_cache,
            snap_ttl_sec=self.snap_ttl_sec,
            apply_chunk_size=self.apply_chunk_size,
            apply_chunk_pause_ms=self.apply_chunk_pause_ms,
        )

    # Main run
    def run(
        self,
        *,
        dry_run: bool = False,
        only_feature: str | None = None,
        write_state_json: bool = True,
        state_path: str | None = None,
        progress: Callable[[str], None] | bool | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        prev_cb = self.emitter.cb
        prev_on = self.on_progress
        try:
            if progress is not None:
                if callable(progress):
                    cb: Callable[[str], None] = progress  # type: ignore[assignment]
                    self.on_progress = cb
                    self.emitter.cb = cb
                elif isinstance(progress, bool):
                    if progress and self.on_progress is None:
                        self.emitter.cb = lambda s: print(s, flush=True)
                    elif not progress:
                        self.emitter.cb = None

            if kwargs:
                try:
                    self.dbg("run.kwargs.ignored", keys=sorted(kwargs.keys()))
                except Exception:
                    pass

            self.dry_run = bool(dry_run)
            self.only_feature = only_feature
            self.write_state_json = bool(write_state_json)
            self.state_path = Path(state_path) if state_path else None

            summary = _run_pairs(self.context)

            try:
                enabled_feats = self._enabled_features()
                if enabled_feats:
                    self._persist_feature_baselines(features=enabled_feats)
            except Exception:
                pass

            try:
                self._persist_state_wall(feature="watchlist")
            except Exception:
                pass

            try:
                self.state_store.clear_watchlist_hide()
                self.dbg(
                    "hidefile.cleared",
                    feature="watchlist",
                    scope="end-of-run",
                )
            except Exception:
                pass

            try:
                if hasattr(self.stats, "http_overview"):
                    http24 = self.stats.http_overview(hours=24)
                    self.emit("http:overview", window_hours=24, data=http24)
            except Exception:
                pass

            try:
                if hasattr(self.stats, "overview"):
                    st = self.state_store.load_state()
                    ov = self.stats.overview(st)
                    self.emit("stats:overview", overview=ov)
            except Exception:
                pass

            return summary
        finally:
            self.emitter.cb = prev_cb
            self.on_progress = prev_on

    def run_pairs(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.run(*args, **kwargs)

    def run_pair(
        self,
        pair: Mapping[str, Any],
        *,
        dry_run: bool = False,
        write_state_json: bool = True,
        state_path: str | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        saved = self.cfg
        try:
            cfg_copy = dict(saved)
            cfg_copy["pairs"] = [dict(pair)]
            self.cfg = cfg_copy
            only_feat = (pair or {}).get("feature")
            return self.run(
                dry_run=dry_run,
                only_feature=only_feat,
                write_state_json=write_state_json,
                state_path=state_path,
                **kwargs,
            )
        finally:
            self.cfg = saved

    # Snapshot helpers
    def build_snapshots(self, feature: str) -> dict[str, dict[str, Any]]:
        return _build_snaps(
            feature=feature,
            config=self.cfg,
            providers=self.providers,
            snap_cache=self.snap_cache,
            snap_ttl_sec=self.snap_ttl_sec,
            dbg=self.dbg,
            emit_info=self.emit_info,
        )

    def allowed_providers_for_feature(self, feature: str) -> set[str]:
        return _allowed_pf(self.cfg, feature)

    def module_checkpoint(self, provider_name: str, feature: str) -> str | None:
        ops = self.providers.get(str(provider_name).upper())
        return _module_cp(ops, self.cfg, feature) if ops else None

    def prev_checkpoint(self, provider_name: str, feature: str) -> str | None:
        st = self.state_store.load_state()
        return _prev_cp(st, str(provider_name).upper(), feature)

    def coerce_suspect_snapshot(
        self,
        *,
        provider: str,
        prev_idx: Mapping[str, Any],
        cur_idx: Mapping[str, Any],
        feature: str,
    ) -> tuple[dict[str, Any], bool, str]:
        prov_name = str(provider).upper()
        ops = self.providers.get(prov_name)
        if ops is None:
            # nothing to coerce against; treat as non-suspect
            return dict(cur_idx), False, "provider:missing"

        prev_cp = self.prev_checkpoint(provider, feature)
        now_cp = self.module_checkpoint(provider, feature)
        return _coerce(
            provider=prov_name,
            ops=ops,
            prev_idx=prev_idx,
            cur_idx=cur_idx,
            feature=feature,
            suspect_min_prev=self.suspect_min_prev,
            suspect_shrink_ratio=self.suspect_shrink_ratio,
            suspect_debug=self.suspect_debug,
            emit=self.emit,
            emit_info=self.emit_info,
            prev_cp=prev_cp,
            now_cp=now_cp,
        )

    def plan_diff(
        self,
        src_idx: Mapping[str, Any],
        dst_idx: Mapping[str, Any],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        return _plan_diff(src_idx, dst_idx)

    def apply_add(
        self,
        *,
        dst: str,
        feature: str,
        items: Sequence[Mapping[str, Any]],
        dry_run: bool | None = None,
    ) -> dict[str, Any]:
        dst_name = str(dst).upper()
        ops = self.providers.get(dst_name)
        if not ops:
            return {"ok": False, "count": 0, "error": f"unknown provider {dst_name}"}
        return _apply_add(
            dst_ops=ops,
            cfg=self.cfg,
            dst_name=dst_name,
            feature=feature,
            items=list(items or []),
            dry_run=self.dry_run if dry_run is None else bool(dry_run),
            emit=self.emit,
            dbg=self.dbg,
            chunk_size=self.apply_chunk_size,
            chunk_pause_ms=self.apply_chunk_pause_ms,
        )

    def apply_remove(
        self,
        *,
        dst: str,
        feature: str,
        items: Sequence[Mapping[str, Any]],
        dry_run: bool | None = None,
    ) -> dict[str, Any]:
        dst_name = str(dst).upper()
        ops = self.providers.get(dst_name)
        if not ops:
            return {"ok": False, "count": 0, "error": f"unknown provider {dst_name}"}
        return _apply_remove(
            dst_ops=ops,
            cfg=self.cfg,
            dst_name=dst_name,
            feature=feature,
            items=list(items or []),
            dry_run=self.dry_run if dry_run is None else bool(dry_run),
            emit=self.emit,
            dbg=self.dbg,
            chunk_size=self.apply_chunk_size,
            chunk_pause_ms=self.apply_chunk_pause_ms,
        )

    # Features
    def _enabled_features(self) -> list[str]:
        feats: set[str] = set()
        pairs = list(self.cfg.get("pairs") or [])
        for p in pairs:
            if not p.get("enabled", True):
                continue
            fmap = p.get("features") or {}
            for name in ("watchlist", "ratings", "history", "playlists"):
                v = fmap.get(name)
                if isinstance(v, bool):
                    if v:
                        feats.add(name)
                elif isinstance(v, dict):
                    if v.get("enable") or v.get("enabled"):
                        feats.add(name)
        if self.only_feature:
            feats &= {self.only_feature}
        if not feats:
            feats.add("watchlist")
        return sorted(feats)

    # Feature baselines
    def _persist_feature_baselines(
        self,
        *,
        features: Sequence[str] = ("watchlist",),
    ) -> dict[str, Any]:
        import time as _t
        from typing import Mapping as _MappingType, Dict as _DictType  # for fallback types

        try:
            from ..id_map import minimal
        except Exception:
            def minimal(item: _MappingType[str, Any]) -> _DictType[str, Any]:  # type: ignore[no-redef]
                return dict(item)

        state: dict[str, Any] = self.state_store.load_state() or {}
        providers = dict(state.get("providers") or {})

        for feat in (features or ()):
            if str(feat).lower() == "watchlist":
                continue
            try:
                try:
                    self.snap_cache.clear()
                except Exception:
                    pass
                snaps = self.build_snapshots(feat)
            except Exception:
                snaps = {}
            for prov, idx in (snaps or {}).items():
                items_min = {k: minimal(v) for k, v in (idx or {}).items()}
                prov_entry = providers.setdefault(str(prov).upper(), {})
                prov_entry[feat] = {
                    "baseline": {"items": items_min},
                    "checkpoint": None,
                }

        state["providers"] = providers
        state["last_sync_epoch"] = int(_t.time())
        self.state_store.save_state(state)
        self.dbg(
            "state.persisted",
            providers=len(providers),
            wall=len(state.get("wall") or []),
        )
        return state

    # Watchlist wall
    def _persist_state_wall(self, *, feature: str = "watchlist") -> dict[str, Any]:
        state: dict[str, Any] = self.state_store.load_state() or {}
        providers = dict(state.get("providers") or {})
        wall: list[dict[str, Any]] = []

        from typing import Mapping as _MappingType, Dict as _DictType  # for fallback types

        try:
            from ..id_map import minimal, canonical_key
        except Exception:
            def minimal(item: _MappingType[str, Any]) -> _DictType[str, Any]:  # type: ignore[no-redef]
                return dict(item)

            def canonical_key(item: _MappingType[str, Any]) -> str:  # type: ignore[no-redef]
                ids = item.get("ids", {})
                return str(ids.get("imdb") or "")

        for prov, fmap in providers.items():
            fentry = (fmap or {}).get(feature) or {}
            base = ((fentry.get("baseline") or {}).get("items") or {})
            if isinstance(base, dict):
                for v in base.values():
                    try:
                        wall.append(minimal(v))
                    except Exception:
                        wall.append(v)  # type: ignore[arg-type]

        seen: set[str] = set()
        uniq: list[dict[str, Any]] = []

        try:
            from ..id_map import canonical_key  # type: ignore[no-redef]
        except Exception:
            def canonical_key(item: _MappingType[str, Any]) -> str:  # type: ignore[no-redef]
                ids = item.get("ids", {})
                return str(ids.get("imdb") or "")

        for it in wall:
            k = canonical_key(it)
            if k in seen:
                continue
            seen.add(k)
            uniq.append(it)

        state["wall"] = uniq
        import time as _t

        state["last_sync_epoch"] = int(_t.time())
        self.state_store.save_state(state)
        self.dbg(
            "state.persisted",
            providers=len(providers),
            wall=len(uniq),
        )
        return state

    # Telemetry
    def emit_rate_warnings(self) -> None:
        maybe_emit_rate_warnings(self.stats, self.emitter.emit, self.warn_thresholds)

    def prune_tombstones(self, older_than_secs: int) -> int:
        return _tomb_prune(self.state_store, self.dbg, older_than_secs=older_than_secs)
