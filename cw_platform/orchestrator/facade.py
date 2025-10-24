from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Mapping, Optional, Iterable, Sequence, List

from ._types import ConflictPolicy
from ._providers import load_sync_providers
from ._state_store import StateStore
from ._logging import Emitter
from ._telemetry import Stats, maybe_emit_rate_warnings
from ._pairs import run_pairs as _run_pairs
from ._tombstones import prune as _tomb_prune, keys_for_feature as _tomb_keys_for_feature, filter_with as _tomb_filter_with, cascade_removals as _tomb_cascade
from ._snapshots import build_snapshots_for_feature as _build_snaps, allowed_providers_for_feature as _allowed_pf, module_checkpoint as _module_cp, prev_checkpoint as _prev_cp, coerce_suspect_snapshot as _coerce
from ._planner import diff as _plan_diff
from ._applier import apply_add as _apply_add, apply_remove as _apply_remove

__all__ = ["Orchestrator"]

#--- Config base import (for state store) -------------------------------------
try:
    from .. import config_base
except Exception:
    class config_base:
        @staticmethod
        def CONFIG_BASE() -> str: return "./"

#--- Orchestrator class (legacy-friendly) --------------------------------------
@dataclass
class Orchestrator:
    config: Mapping[str, Any]
    on_progress: Optional[Callable[[str], None]] = None
    conflict: ConflictPolicy = field(default_factory=ConflictPolicy)

    dry_run: bool = False
    only_feature: Optional[str] = None
    write_state_json: bool = True
    state_path: Optional[Path] = None

    # Back-compat alias
    files: StateStore | None = field(init=False, default=None)

    # Internal fields
    def __post_init__(self):
        self.cfg = dict(self.config or {})
        rt = dict(self.cfg.get("runtime") or {})
        self.debug = bool(rt.get("debug", False))
        self.emitter = Emitter(self.on_progress)
        self.emit = self.emitter.emit
        self.emit_info = self.emitter.info
        self.dbg = lambda *a, **k: self.emitter.dbg(self.debug, *a, **k)

        self.state_store = StateStore(Path(config_base.CONFIG_BASE()))
        self.files = self.state_store
        self.providers = load_sync_providers()

        self.stats = None
        try:
            import crosswatch as CW
            self.stats = getattr(CW, 'STATS', None)
        except Exception:
            self.stats = None
        if self.stats is None:
            self.stats = Stats(getattr(__import__('builtins'), 'Stats', None))

        self.warn_thresholds = (dict(self.cfg.get("telemetry") or (self.cfg.get("runtime", {}).get("telemetry") or {})).get("warn_rate_remaining") or {"TRAKT":100, "SIMKL":50, "PLEX":0, "JELLYFIN":0})
        self.snap_cache: Dict[tuple[str,str], tuple[float, Dict[str, Dict[str, Any]]]] = {}
        self.snap_ttl_sec = int(rt.get("snapshot_ttl_sec") or 0)
        self.suspect_min_prev = int(rt.get("suspect_min_prev", 20))
        self.suspect_shrink_ratio = float(rt.get("suspect_shrink_ratio", 0.10))
        self.suspect_debug = bool(rt.get("suspect_debug", True))
        self.apply_chunk_size = int(rt.get("apply_chunk_size") or 0)
        self.apply_chunk_pause_ms = int(rt.get("apply_chunk_pause_ms") or 0)
        self.emitter.info("[i] Orchestrator v3 ready (full compat shims)")

    # Context property
    @property
    def context(self):
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

    #--- Main run method ----------------------------------------------------------
    def run(self, *, dry_run: bool=False, only_feature: Optional[str]=None, write_state_json: bool=True, state_path: Optional[str]=None, progress: Optional[object]=None, **kwargs) -> Dict[str, Any]:
        prev_cb = self.emitter.cb
        prev_on = self.on_progress
        try:
            if progress is not None:
                if callable(progress):
                    self.on_progress = progress
                    self.emitter.cb = progress
                elif isinstance(progress, bool):
                    if progress and self.on_progress is None:
                        self.emitter.cb = lambda s: print(s, flush=True)
                    elif not progress:
                        self.emitter.cb = None
            if kwargs:
                try: self.dbg("run.kwargs.ignored", keys=sorted(kwargs.keys()))
                except Exception: pass

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

            try: self._persist_state_wall(feature='watchlist')
            except Exception: pass
            try:
                self.state_store.clear_watchlist_hide()
                self.dbg('hidefile.cleared', feature='watchlist', scope='end-of-run')
            except Exception: pass
            try:
                if hasattr(self.stats, 'http_overview'):
                    http24 = self.stats.http_overview(hours=24)
                    self.emit('http:overview', window_hours=24, data=http24)
            except Exception: pass

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

    def run_pairs(self, *args, **kwargs) -> Dict[str, Any]:
        return self.run(*args, **kwargs)

    def run_pair(self, pair: Mapping[str, Any], *, dry_run: bool=False, write_state_json: bool=True, state_path: Optional[str]=None, **kwargs) -> Dict[str, Any]:
        saved = self.cfg
        try:
            cfg_copy = dict(saved); cfg_copy["pairs"] = [dict(pair)]
            self.cfg = cfg_copy
            only_feat = (pair or {}).get("feature")
            return self.run(dry_run=dry_run, only_feature=only_feat, write_state_json=write_state_json, state_path=state_path, **kwargs)
        finally:
            self.cfg = saved

    #--- Snapshot / provider helpers ----------------------------------------------
    def build_snapshots(self, feature: str) -> Dict[str, Dict[str, Any]]:
        return _build_snaps(feature=feature, config=self.cfg, providers=self.providers, snap_cache=self.snap_cache, snap_ttl_sec=self.snap_ttl_sec, dbg=self.dbg, emit_info=self.emit_info)
    def allowed_providers_for_feature(self, feature: str) -> set[str]:
        return _allowed_pf(self.cfg, feature)
    def module_checkpoint(self, provider_name: str, feature: str) -> Optional[str]:
        ops = self.providers.get(str(provider_name).upper())
        return _module_cp(ops, self.cfg, feature) if ops else None
    def prev_checkpoint(self, provider_name: str, feature: str) -> Optional[str]:
        st = self.state_store.load_state()
        return _prev_cp(st, str(provider_name).upper(), feature)
    def coerce_suspect_snapshot(self, *, provider: str, prev_idx: Mapping[str, Any], cur_idx: Mapping[str, Any], feature: str) -> tuple[Dict[str, Any], bool, str]:
        ops = self.providers.get(str(provider).upper())
        prev_cp = self.prev_checkpoint(provider, feature)
        now_cp = self.module_checkpoint(provider, feature)
        return _coerce(provider=str(provider).upper(), ops=ops, prev_idx=prev_idx, cur_idx=cur_idx, feature=feature, suspect_min_prev=self.suspect_min_prev, suspect_shrink_ratio=self.suspect_shrink_ratio, suspect_debug=self.suspect_debug, emit=self.emit, emit_info=self.emit_info, prev_cp=prev_cp, now_cp=now_cp)
    def plan_diff(self, src_idx: Mapping[str, Any], dst_idx: Mapping[str, Any]) -> tuple[list[dict], list[dict]]:
        return _plan_diff(src_idx, dst_idx)
    def apply_add(self, *, dst: str, feature: str, items: List[Dict[str, Any]], dry_run: Optional[bool]=None) -> Dict[str, Any]:
        dst_name = str(dst).upper(); ops = self.providers.get(dst_name)
        if not ops: return {"ok": False, "count": 0, "error": f"unknown provider {dst_name}"}
        return _apply_add(dst_ops=ops, cfg=self.cfg, dst_name=dst_name, feature=feature, items=list(items or []), dry_run=self.dry_run if dry_run is None else bool(dry_run), emit=self.emit, dbg=self.dbg, chunk_size=self.apply_chunk_size, chunk_pause_ms=self.apply_chunk_pause_ms)
    def apply_remove(self, *, dst: str, feature: str, items: List[Dict[str, Any]], dry_run: Optional[bool]=None) -> Dict[str, Any]:
        dst_name = str(dst).upper(); ops = self.providers.get(dst_name)
        if not ops: return {"ok": False, "count": 0, "error": f"unknown provider {dst_name}"}
        return _apply_remove(dst_ops=ops, cfg=self.cfg, dst_name=dst_name, feature=feature, items=items, dry_run=self.dry_run if dry_run is None else bool(dry_run), emit=self.emit, dbg=self.dbg, chunk_size=self.apply_chunk_size, chunk_pause_ms=self.apply_chunk_pause_ms)

    #--- Persist enabled features helper -----------------------------------------
    def _enabled_features(self) -> List[str]:
        feats: set[str] = set()
        pairs = list((self.cfg.get("pairs") or []))
        for p in pairs:
            if not p.get("enabled", True):
                continue
            f = p.get("features") or {}
            for name in ("watchlist","ratings","history","playlists"):
                v = f.get(name)
                if isinstance(v, bool):
                    if v: feats.add(name)
                elif isinstance(v, dict):
                    if v.get("enable") or v.get("enabled"): feats.add(name)
        if self.only_feature:
            feats &= {self.only_feature}
        if not feats:
            feats.add("watchlist")
        return sorted(feats)

    #--- Persist provider feature baselines --------------------------------------
    def _persist_feature_baselines(self, *, features: Sequence[str] = ("watchlist",)) -> dict:
        import time as _t
        try:
            from ..id_map import minimal
        except Exception:
            def minimal(x): return x
        state = self.state_store.load_state() or {}
        providers = dict(state.get("providers") or {})
        for feat in (features or ()):
            if str(feat).lower() == "watchlist":
                continue
            try:
                try: self.snap_cache.clear()
                except Exception: pass
                snaps = self.build_snapshots(feat)
            except Exception:
                snaps = {}
            for prov, idx in (snaps or {}).items():
                items_min = {k: minimal(v) for k, v in (idx or {}).items()}
                prov_entry = providers.setdefault(str(prov).upper(), {})
                prov_entry[feat] = {"baseline": {"items": items_min}, "checkpoint": None}
        state["providers"] = providers
        state["last_sync_epoch"] = int(_t.time())
        self.state_store.save_state(state)
        self.dbg("state.persisted", providers=len(providers), wall=len((state.get("wall") or [])))
        return state

    #--- Persist watchlist wall ---------------------------------------------------
    def _persist_state_wall(self, *, feature: str = "watchlist") -> dict:
        state = self.state_store.load_state() or {}
        providers = dict(state.get("providers") or {})
        wall = []
        try:
            from ..id_map import minimal, canonical_key
        except Exception:
            def minimal(x): return x
            def canonical_key(x): return str(x.get("ids",{}).get("imdb") or "")
        for prov, fmap in providers.items():
            fentry = (fmap or {}).get(feature) or {}
            base = ((fentry.get("baseline") or {}).get("items") or {})
            for v in (base.values() if isinstance(base, dict) else []):
                try:
                    wall.append(minimal(v))
                except Exception:
                    wall.append(v)
        seen = set(); uniq = []
        try:
            from ..id_map import canonical_key
        except Exception:
            def canonical_key(x): return str(x.get("ids",{}).get("imdb") or "")
        for it in wall:
            k = canonical_key(it)
            if k in seen:
                continue
            seen.add(k); uniq.append(it)
        state["wall"] = uniq
        import time as _t
        state["last_sync_epoch"] = int(_t.time())
        self.state_store.save_state(state)
        self.dbg("state.persisted", providers=len(providers), wall=len(uniq))
        return state

    #--- Telemetry helpers ---------------------------------------------------------
    def emit_rate_warnings(self):
        return maybe_emit_rate_warnings(self.stats, self.emitter.emit, self.warn_thresholds)
    def prune_tombstones(self, older_than_secs: int) -> int:
        return _tomb_prune(self.state_store, self.dbg, older_than_secs=older_than_secs)
