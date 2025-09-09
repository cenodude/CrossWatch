from __future__ import annotations

"""
Provider-agnostic Orchestrator
------------------------------
- Pre-snapshot → plan → removals-first → adds → post-check
- Two-way sync with strict deletion guard rules (no global kill-switch)
- Bootstrap no-delete after reset
- Tombstones with TTL; observed deletions persisted (sanity-checked)
- Optional metadata enrichment (best-effort)
- Atomic state/last_sync writes
- Telemetry: integrates with _statistics.Stats for per-run summaries + HTTP 24h overview

Enhancements (this version):
- Baseline + optional checkpoint stored per provider/feature in state.json
- State-driven wall: no extra snapshot fetch at persist time
- Hooks for future delta (date_from) via optional module.activities()
"""

#-------------------- imports
import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Protocol, Sequence, Tuple

#-------------------- config base
try:
    from . import config_base
except Exception:
    class config_base:  # type: ignore
        @staticmethod
        def CONFIG_BASE() -> str:
            return "./"

#-------------------- logging shim
class _Logger:
    def __call__(self, *a): print(*a)
    def info(self, *a): print(*a)
    def warn(self, *a): print(*a)
    warning = warn
    def error(self, *a): print(*a)
    def success(self, *a): print(*a)
log = _Logger()

#-------------------- statistics (provider-agnostic)
try:
    from _statistics import Stats  # type: ignore
except Exception:  # pragma: no cover
    class Stats:  # minimal noop fallback
        def __init__(self, *a, **k): pass
        def record_summary(self, *a, **k): pass
        def overview(self, *a, **k): return {}
        def http_overview(self, *a, **k): return {}

#-------------------- provider protocol
class InventoryOps(Protocol):
    def name(self) -> str: ...
    def label(self) -> str: ...
    def features(self) -> Mapping[str, bool]: ...
    def capabilities(self) -> Mapping[str, Any]: ...
    def build_index(self, cfg: Mapping[str, Any], *, feature: str) -> Mapping[str, Dict[str, Any]]: ...
    def add(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool=False) -> Dict[str, Any]: ...
    def remove(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool=False) -> Dict[str, Any]: ...
    # Optional: modules may expose activities(cfg) → dict with timestamps
    # Optional future: build_index(..., date_from: Optional[str])

#-------------------- module loader
def _iter_sync_modules():
    import importlib, pkgutil
    import providers.sync as syncpkg  # type: ignore
    pkg_path = Path(syncpkg.__file__).parent
    for m in pkgutil.iter_modules([str(pkg_path)]):
        if not m.name.startswith("_mod_"): 
            continue
        try:
            yield importlib.import_module(f"providers.sync.{m.name}")
        except Exception as e:
            log.warn(f"provider.load_failed {m.name} {e}")

def _resolve_ops_from_module(mod) -> Optional[InventoryOps]:
    for attr in ("OPS", "ADAPTER", "ProviderOps", "InventoryOps"):
        obj = getattr(mod, attr, None)
        if obj and all(hasattr(obj, fn) for fn in ("name","label","features","capabilities","build_index","add","remove")):
            return obj  # type: ignore
    return None

def load_sync_providers() -> Dict[str, InventoryOps]:
    out: Dict[str, InventoryOps] = {}
    for mod in _iter_sync_modules():
        ops = _resolve_ops_from_module(mod)
        if not ops: 
            continue
        try:
            out[str(ops.name()).upper()] = ops  # type: ignore
        except Exception:
            continue
    return out

#-------------------- canonical helpers
_ID_KEYS = ("tmdb", "imdb", "tvdb", "trakt", "plex", "guid", "slug")

def _first_id(d: Mapping[str, Any]) -> Optional[Tuple[str, str]]:
    ids = d.get("ids") or {}
    for k in _ID_KEYS:
        v = ids.get(k)
        if v:
            return k, str(v)
    return None

def canonical_key(item: Mapping[str, Any]) -> str:
    p = _first_id(item)
    if p:
        return f"{p[0]}:{p[1]}".lower()
    t = (item.get("title") or "").strip().lower()
    y = item.get("year") or ""
    typ = (item.get("type") or "").lower()
    return f"{typ}|title:{t}|year:{y}"

def minimal(item: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "ids": {k: item.get("ids", {}).get(k) for k in _ID_KEYS if item.get("ids", {}).get(k)},
        "title": item.get("title"),
        "year": item.get("year"),
        "type": (item.get("type") or "").lower() or None,
    }

#-------------------- state files
class _Files:
    def __init__(self, base: Path):
        self.base = base
        self.state = base / "state.json"
        self.tomb  = base / "tombstones.json"
        self.last  = base / "last_sync.json"

    def _read(self, p: Path, default):
        if not p.exists(): 
            return default
        try: 
            return json.loads(p.read_text("utf-8"))
        except Exception: 
            return default

    def _write_atomic(self, p: Path, data: Any):
        tmp = p.with_suffix(p.suffix + ".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), "utf-8")
        tmp.replace(p)

    def load_state(self) -> Dict[str, Any]:
        # providers.<NAME>.<feature>.baseline.items
        # providers.<NAME>.<feature>.checkpoint
        return self._read(self.state, {"providers": {}, "wall": [], "last_sync_epoch": None})

    def save_state(self, data: Mapping[str, Any]):
        self._write_atomic(self.state, data)

    def load_tomb(self) -> Dict[str, Any]:
        return self._read(self.tomb, {"keys": {}, "pruned_at": None})

    def save_tomb(self, data: Mapping[str, Any]):
        self._write_atomic(self.tomb, data)

    def save_last(self, data: Mapping[str, Any]):
        self._write_atomic(self.last, data)

#-------------------- policy
@dataclass
class ConflictPolicy:
    prefer: str = "source"  # used only for true conflicts

#-------------------- orchestrator
@dataclass
class Orchestrator:
    config: Mapping[str, Any]
    on_progress: Optional[Callable[[str], None]] = None
    conflict: ConflictPolicy = field(default_factory=ConflictPolicy)

    def __post_init__(self):
        self.cfg = dict(self.config or {})
        self.debug = bool(((self.cfg.get("runtime") or {}).get("debug") or False))
        self.files = _Files(Path(config_base.CONFIG_BASE()))
        self.providers = load_sync_providers()
        self._emit_info(f"[i] Orchestrator module: {Path(__file__).resolve()}")

        # optional metadata
        self.meta = None
        try:
            from .metadata import MetadataManager  # type: ignore
            try: 
                self.meta = MetadataManager()  # type: ignore
            except TypeError: 
                self.meta = MetadataManager(load_cfg=lambda: self.cfg, save_cfg=lambda _c: None)  # type: ignore
        except Exception:
            self.meta = None

        # stats
        try:
            self.stats = Stats()
        except Exception:  # pragma: no cover
            self.stats = Stats()

        # thresholds for low rate warnings
        telem = dict((self.cfg.get("telemetry") or {}))
        self.warn_thresholds = (telem.get("warn_rate_remaining") or {"TRAKT": 100, "SIMKL": 50, "PLEX": 0})

    #-------------------- logging
    def _emit(self, event: str, **data):
        if self.on_progress:
            try:
                payload = {"event": event}; payload.update(data)
                self.on_progress(json.dumps(payload, separators=(",", ":")))
            except Exception: 
                pass

    def _emit_info(self, line: str):
        if self.on_progress:
            try: 
                self.on_progress(line)
            except Exception: 
                pass

    def _dbg(self, msg: str, **fields):
        if not self.debug: 
            return
        if fields: 
            self._emit("debug", msg=msg, **fields)
        else: 
            self._emit_info(f"[DEBUG] {msg}")

    #-------------------- baseline helpers
    def _ensure_pf(self, state: Dict[str, Any], prov: str, feature: str) -> Dict[str, Any]:
        p = state.setdefault("providers", {})
        pprov = p.setdefault(prov, {})
        return pprov.setdefault(feature, {"baseline": {"items": {}}, "checkpoint": None})

    def _commit_baseline(self, state: Dict[str, Any], prov: str, feature: str, items: Mapping[str, Any]) -> None:
        pf = self._ensure_pf(state, prov, feature)
        pf["baseline"] = {"items": {k: minimal(v) for k, v in items.items()}}

    def _commit_checkpoint(self, state: Dict[str, Any], prov: str, feature: str, checkpoint: Optional[str]) -> None:
        if not checkpoint: 
            return
        pf = self._ensure_pf(state, prov, feature)
        pf["checkpoint"] = checkpoint

    #-------------------- activities (optional)
    def _module_checkpoint(self, ops: InventoryOps, feature: str) -> Optional[str]:
        # If module exposes activities(cfg) we read feature-specific timestamp
        acts_fn = getattr(ops, "activities", None)
        if not callable(acts_fn):
            return None
        try:
            acts = acts_fn(self.cfg) or {}
            if feature == "watchlist":
                return acts.get("watchlist") or acts.get("ptw") or acts.get("updated_at")
            if feature == "ratings":
                return acts.get("ratings") or acts.get("updated_at")
            if feature == "history":
                return acts.get("history") or acts.get("updated_at")
            return acts.get("updated_at")
        except Exception:
            return None

    #-------------------- snapshots (full; modules may add delta later)
    def build_snapshots(self, *, feature: str) -> Dict[str, Dict[str, Any]]:
        snaps: Dict[str, Dict[str, Any]] = {}
        for name, ops in self.providers.items():
            if not ops.features().get(feature, False): 
                continue
            try:
                idx = ops.build_index(self.cfg, feature=feature) or {}
            except Exception as e:
                self._emit_info(f"[!] snapshot.failed provider={name} feature={feature} error={e}")
                idx = {}
            if isinstance(idx, list):
                canon = {canonical_key(v): v for v in idx}
            else:
                canon = {canonical_key(v): v for v in idx.values()} if idx else {}
            snaps[name] = canon
            self._dbg("snapshot", provider=name, feature=feature, count=len(canon))
        return snaps

    @staticmethod
    def diff(src_idx: Mapping[str, Any], dst_idx: Mapping[str, Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        add, rem = [], []
        for k, v in src_idx.items():
            if k not in dst_idx: 
                add.append(minimal(v))
        for k, v in dst_idx.items():
            if k not in src_idx: 
                rem.append(minimal(v))
        return add, rem

    #-------------------- tombstones
    def _tomb_add_keys(self, keys: Iterable[str]) -> int:
        t = self.files.load_tomb(); ks = t.setdefault("keys", {})
        now = int(time.time()); added = 0
        for k in keys:
            if k not in ks: 
                ks[k] = now; added += 1
        self.files.save_tomb(t)
        if added or self.debug: 
            self._dbg("tombstones.marked", added=added)
        return added

    def prune_tombstones(self, *, older_than_secs: int) -> int:
        t = self.files.load_tomb(); ks = t.get("keys", {})
        if not ks: 
            return 0
        now = int(time.time())
        keep = {k:v for k,v in ks.items() if (now - int(v)) < older_than_secs}
        removed = len(ks) - len(keep)
        t["keys"] = keep; t["pruned_at"] = now
        self.files.save_tomb(t)
        self._dbg("tombstones.pruned", removed=removed, kept=len(keep))
        return removed

    def filter_with_tombstones(self, items: Sequence[Dict[str, Any]], extra_block: Optional[set[str]] = None) -> List[Dict[str, Any]]:
        t = set((self.files.load_tomb().get("keys") or {}).keys())
        if extra_block: 
            t |= set(extra_block)
        out = [it for it in items if canonical_key(it) not in t]
        if self.debug and len(out) != len(items):
            self._dbg("tombstones.filtered", before=len(items), after=len(out))
        return out

    #-------------------- enrichment
    def maybe_enrich(self, items: List[Dict[str, Any]], *, want_ids=True) -> List[Dict[str, Any]]:
        if not items or not want_ids or not self.meta: 
            return items
        need = [it for it in items if not ((it.get("ids") or {}).get("tmdb") or (it.get("ids") or {}).get("imdb"))]
        if not need: 
            return items
        try:
            res = self.meta.resolve(need)  # type: ignore
            emap = {canonical_key(x): x for x in res or []}
            out = [emap.get(canonical_key(it), it) for it in items]
            self._dbg("enrich.done", requested=len(items), enriched=len([o for o in out if (o.get('ids') or {}).get('tmdb') or (o.get('ids') or {}).get('imdb')]))
            return out
        except Exception as e:
            self._emit_info(f"[!] metadata.enrich_failed: {e}")
            return items

    #-------------------- retry
    def _retry(self, fn: Callable[[], Any], *, attempts: int = 3, base_sleep: float = 0.5) -> Any:
        last = None
        for i in range(attempts):
            try: 
                return fn()
            except Exception as e:
                last = e; time.sleep(base_sleep * (2 ** i))
        raise last  # type: ignore

    #-------------------- effective tomb (with sanity check)
    def _effective_tomb(
        self,
        *,
        include_observed: bool,
        ttl_days: int,
        prevA: Mapping[str, Any],
        prevB: Mapping[str, Any],
        A: Mapping[str, Any],
        B: Mapping[str, Any],
    ) -> Tuple[set[str], Dict[str, str]]:
        now = int(time.time())
        t = self.files.load_tomb(); ks: Dict[str, int] = dict(t.get("keys") or {})
        ttl_secs = max(1, ttl_days) * 24 * 3600

        tomb = {k for k, ts in ks.items() if (now - int(ts)) <= ttl_secs}
        reasons: Dict[str, str] = {k: "tomb:explicit" for k in tomb}

        # Drop-detection: guard against accidental mass removals
        def _plummeted(prev: Mapping[str, Any], cur: Mapping[str, Any]) -> bool:
            return (len(prev) > 0) and (len(cur) < 0.4 * len(prev)) and (len(prev) - len(cur) >= 5)

        if include_observed:
            skip_A = _plummeted(prevA, A)
            skip_B = _plummeted(prevB, B)
            obs_del_A = set() if skip_A else {k for k in prevA.keys() if k not in A}
            obs_del_B = set() if skip_B else {k for k in prevB.keys() if k not in B}

            if skip_A: self._dbg("observed.skip_due_to_drop", side="A", prev=len(prevA), cur=len(A))
            if skip_B: self._dbg("observed.skip_due_to_drop", side="B", prev=len(prevB), cur=len(B))

            newly = set()
            for k in (obs_del_A | obs_del_B):
                if k not in tomb:
                    tomb.add(k); reasons[k] = "tomb:observed-since-last-state"; newly.add(k)
            if newly: 
                self._tomb_add_keys(newly)

            self._dbg("observed.deletions", a=len(obs_del_A), b=len(obs_del_B), tomb=len(tomb))
        else:
            self._dbg("observed.deletions", a=0, b=0, tomb=len(tomb))

        return tomb, reasons

    #-------------------- one-way
    def apply_direction(self, *, src: str, dst: str, feature: str, allow_removals: bool, dry_run: bool=False) -> Dict[str, Any]:
        src = src.upper(); dst = dst.upper()
        sops = self.providers[src]; dops = self.providers[dst]
        if not sops.features().get(feature) or not dops.features().get(feature):
            return {"ok": True, "skipped": True, "reason": "feature-not-supported", "src": src, "dst": dst}

        # 1) fresh snapshots
        self._emit("snapshot:start", src=src, dst=dst, feature=feature)
        snaps = self.build_snapshots(feature=feature)
        src_idx = snaps.get(src, {}) or {}
        dst_idx = snaps.get(dst, {}) or {}

        # 2) read PREVIOUS baseline (for bootstrap / observed-delete logic)
        prev_state = self.files.load_state() or {}
        prev_provs = (prev_state.get("providers") or {})
        prev_src = dict((((prev_provs.get(src, {}) or {}).get(feature, {}) or {}).get("baseline", {}) or {}).get("items") or {})
        prev_dst = dict((((prev_provs.get(dst, {}) or {}).get(feature, {}) or {}).get("baseline", {}) or {}).get("items") or {})

        additions, removals = self.diff(src_idx, dst_idx)

        # Bootstrap: first run → never remove
        bootstrap = (not prev_src) and (not prev_dst) and not (self.files.load_tomb().get("keys") or {})
        if bootstrap and allow_removals:
            removals = []
            self._dbg("bootstrap.no-delete", src=src, dst=dst)

        self._emit("plan", src=src, dst=dst, feature=feature, add=len(additions), rem=len(removals) if allow_removals else 0)

        # 3) apply
        res_rem = {"ok": True, "count": 0}
        if allow_removals and removals:
            self._emit("apply:remove:start", dst=dst, count=len(removals))
            res_rem = self._retry(lambda: dops.remove(self.cfg, removals, feature=feature, dry_run=dry_run))
            self._emit("apply:remove:done", dst=dst, count=len(removals), result=res_rem)
            if removals and not dry_run:
                self._tomb_add_keys([canonical_key(it) for it in removals])

        # ✅ use capabilities(), not features()
        want_ids = not bool(dops.capabilities().get("provides_ids"))
        additions = self.maybe_enrich(additions, want_ids=want_ids)

        self._emit("apply:add:start", dst=dst, count=len(additions))
        res_add = {"ok": True, "count": 0}
        if additions:
            res_add = self._retry(lambda: dops.add(self.cfg, additions, feature=feature, dry_run=dry_run))
        self._emit("apply:add:done", dst=dst, count=len(additions), result=res_add)

        # 4) commit baselines & checkpoints AFTER applying
        state = self.files.load_state()
        self._commit_baseline(state, src, feature, src_idx)
        self._commit_baseline(state, dst, feature, dst_idx)
        self._commit_checkpoint(state, src, feature, self._module_checkpoint(sops, feature))
        self._commit_checkpoint(state, dst, feature, self._module_checkpoint(dops, feature))
        state["last_sync_epoch"] = int(time.time())
        self.files.save_state(state)

        try:
            self.stats.record_summary(added=int(res_add.get("count", 0)), removed=int(res_rem.get("count", 0)))
        except Exception:
            pass

        return {
            "ok": True,
            "adds": len(additions),
            "removes": len(removals) if allow_removals else 0,
            "dst": dst, "src": src,
            "feature": feature,
            "res_add": res_add, "res_remove": res_rem,
        }

    #-------------------- two-way
    def _two_way_sync(
        self,
        a: str,
        b: str,
        *,
        feature: str,
        allow_removals: bool,
        dry_run: bool=False,
        include_observed_deletes: bool=True,
        tomb_ttl_days: int = 30,
    ) -> Dict[str, Any]:
        a = a.upper(); b = b.upper()
        aops = self.providers[a]; bops = self.providers[b]
        if not aops.features().get(feature) or not bops.features().get(feature):
            return {"ok": True, "skipped": True, "reason": "feature-not-supported", "a": a, "b": b, "feature": feature}

        self._emit("two:start", a=a, b=b, feature=feature, removals=allow_removals)

        # 1) fresh snapshots
        self._emit("snapshot:start", a=a, b=b, feature=feature)
        snaps = self.build_snapshots(feature=feature)
        A = snaps.get(a, {}) or {}
        B = snaps.get(b, {}) or {}

        # 2) read PREVIOUS baselines BEFORE touching state
        prev_state = self.files.load_state() or {}
        prev_provs = (prev_state.get("providers") or {})
        prevA = dict((((prev_provs.get(a, {}) or {}).get(feature, {}) or {}).get("baseline", {}) or {}).get("items") or {})
        prevB = dict((((prev_provs.get(b, {}) or {}).get(feature, {}) or {}).get("baseline", {}) or {}).get("items") or {})

        # optional debug visibility for observed deletions
        try:
            obsA = sum(1 for k in prevA.keys() if k not in A)
            obsB = sum(1 for k in prevB.keys() if k not in B)
            tomb_now = len(self.files.load_tomb().get("keys") or {})
            self._emit("debug", msg="observed.deletions", a=obsA, b=obsB, tomb=tomb_now)
        except Exception:
            pass

        # Bootstrap: first run (no prev & empty tomb) → never remove
        bootstrap = (not prevA) and (not prevB) and not (self.files.load_tomb().get("keys") or {})
        if bootstrap:
            self._dbg("bootstrap.no-delete", a=a, b=b)

        ttl_days = int(((self.cfg.get("sync") or {}).get("tombstone_ttl_days") or tomb_ttl_days))
        include_obs = (not bootstrap) and bool(include_observed_deletes)

        tomb, reasons = self._effective_tomb(
            include_observed=include_obs,
            ttl_days=ttl_days,
            prevA=prevA, prevB=prevB,
            A=A, B=B,
        )

        add_to_B, add_to_A, rem_from_A, rem_from_B = [], [], [], []

        # A-only
        for k, v in A.items():
            if k not in B:
                if allow_removals and (k in tomb):
                    rem_from_A.append(minimal(v))
                else:
                    add_to_B.append(minimal(v))

        # B-only
        for k, v in B.items():
            if k not in A:
                if allow_removals and (k in tomb):
                    rem_from_B.append(minimal(v))
                else:
                    add_to_A.append(minimal(v))

        # guard: don't add something we’re simultaneously removing (or tombed)
        kill_keys = {canonical_key(x) for x in rem_from_A + rem_from_B}
        add_to_A = [x for x in add_to_A if canonical_key(x) not in kill_keys and canonical_key(x) not in tomb]
        add_to_B = [x for x in add_to_B if canonical_key(x) not in kill_keys and canonical_key(x) not in tomb]

        # Bootstrap never removes
        if bootstrap and allow_removals:
            rem_from_A.clear(); rem_from_B.clear()

        if (rem_from_A or rem_from_B) and self.debug:
            detail = []
            for it in rem_from_A:
                k = canonical_key(it); detail.append({"side":"A","id":k,"reason":reasons.get(k,"tomb:unknown")})
            for it in rem_from_B:
                k = canonical_key(it); detail.append({"side":"B","id":k,"reason":reasons.get(k,"tomb:unknown")})
            self._emit("two:plan:remove:reasons", detail=detail)

        self._emit("two:plan", a=a, b=b, feature=feature,
                add_to_A=len(add_to_A), add_to_B=len(add_to_B),
                rem_from_A=len(rem_from_A) if allow_removals else 0,
                rem_from_B=len(rem_from_B) if allow_removals else 0)

        # 3) apply: removes first, then adds
        resA_rem = {"ok": True, "count": 0}; resB_rem = {"ok": True, "count": 0}
        if allow_removals:
            self._emit("two:apply:remove:A:start", dst=a, count=len(rem_from_A))
            if rem_from_A:
                resA_rem = self._retry(lambda: aops.remove(self.cfg, rem_from_A, feature=feature, dry_run=dry_run))
                if rem_from_A and not dry_run:
                    self._tomb_add_keys([canonical_key(it) for it in rem_from_A])
            self._emit("two:apply:remove:A:done", dst=a, count=len(rem_from_A), result=resA_rem)

            self._emit("two:apply:remove:B:start", dst=b, count=len(rem_from_B))
            if rem_from_B:
                resB_rem = self._retry(lambda: bops.remove(self.cfg, rem_from_B, feature=feature, dry_run=dry_run))
                if rem_from_B and not dry_run:
                    self._tomb_add_keys([canonical_key(it) for it in rem_from_B])
            self._emit("two:apply:remove:B:done", dst=b, count=len(rem_from_B), result=resB_rem)

        # ✅ use capabilities(), not features()
        want_ids_A = not bool(aops.capabilities().get("provides_ids"))
        want_ids_B = not bool(bops.capabilities().get("provides_ids"))
        add_to_A = self.maybe_enrich(add_to_A, want_ids=want_ids_A)
        add_to_B = self.maybe_enrich(add_to_B, want_ids=want_ids_B)

        resA_add = {"ok": True, "count": 0}; resB_add = {"ok": True, "count": 0}
        self._emit("two:apply:add:A:start", dst=a, count=len(add_to_A))
        if add_to_A:
            resA_add = self._retry(lambda: aops.add(self.cfg, add_to_A, feature=feature, dry_run=dry_run))
        self._emit("two:apply:add:A:done", dst=a, count=len(add_to_A), result=resA_add)

        self._emit("two:apply:add:B:start", dst=b, count=len(add_to_B))
        if add_to_B:
            resB_add = self._retry(lambda: bops.add(self.cfg, add_to_B, feature=feature, dry_run=dry_run))
        self._emit("two:apply:add:B:done", dst=b, count=len(add_to_B), result=resB_add)

        # 4) AFTER applying, commit new baselines & checkpoints (use POST-APPLY snapshots)
        if not dry_run:
            snaps2 = self.build_snapshots(feature=feature)
            A_commit = snaps2.get(a, {}) or {}
            B_commit = snaps2.get(b, {}) or {}
        else:
            # no server changes; keep originals
            A_commit = A
            B_commit = B

        state = self.files.load_state()
        self._commit_baseline(state, a, feature, A_commit)
        self._commit_baseline(state, b, feature, B_commit)
        self._commit_checkpoint(state, a, feature, self._module_checkpoint(aops, feature))
        self._commit_checkpoint(state, b, feature, self._module_checkpoint(bops, feature))
        state["last_sync_epoch"] = int(time.time())
        self.files.save_state(state)

        try:
            added_total = int(resA_add.get("count", 0)) + int(resB_add.get("count", 0))
            removed_total = int(resA_rem.get("count", 0)) + int(resB_rem.get("count", 0))
            self.stats.record_summary(added=added_total, removed=removed_total)
        except Exception:
            pass

        return {
            "ok": True, "feature": feature, "a": a, "b": b,
            "adds_to_A": len(add_to_A), "adds_to_B": len(add_to_B),
            "rem_from_A": len(rem_from_A) if allow_removals else 0,
            "rem_from_B": len(rem_from_B) if allow_removals else 0,
            "resA_add": resA_add, "resB_add": resB_add,
            "resA_remove": resA_rem, "resB_remove": resB_rem,
        }

    #-------------------- state persist (state-driven wall; no extra fetch)
    def _persist_state_wall(self, *, feature: str = "watchlist") -> Dict[str, Any]:
        state = self.files.load_state() or {}
        providers_block: Dict[str, Any] = state.get("providers") or {}

        # Build wall from committed baselines (no re-snapshot)
        wall: List[Dict[str, Any]] = []
        for prov, featmap in providers_block.items():
            fentry = (featmap or {}).get(feature) or {}
            base = (fentry.get("baseline") or {}).get("items") or {}
            for v in base.values():
                wall.append(minimal(v))

        # De-duplicate wall by canonical key
        seen = set(); uniq = []
        for it in wall:
            k = canonical_key(it)
            if k in seen: 
                continue
            seen.add(k); uniq.append(it)

        state["wall"] = uniq
        state["last_sync_epoch"] = int(time.time())
        self.files.save_state(state)
        self._dbg("state.persisted", providers=len(providers_block), wall=len(uniq))
        return state

    #-------------------- rate-limit warnings
    def _maybe_emit_rate_warnings(self):
        try:
            ov = self.stats.http_overview(hours=24) or {}
            provs = (ov.get("providers") or {})
            for prov, row in provs.items():
                last = (row.get("rate") or {})
                remaining = last.get("remaining")
                reset = last.get("reset")
                thr = int(self.warn_thresholds.get(prov, 0) or 0)
                if remaining is not None and thr and int(remaining) <= thr:
                    self._emit("rate:low", provider=prov, remaining=int(remaining), reset=reset, threshold=thr)
        except Exception:
            pass

    #-------------------- run one pair
    def run_pair(self, pair: Mapping[str, Any], *, dry_run: bool=False) -> Dict[str, Any]:
        src = str(pair.get("source") or pair.get("src") or "").upper()
        dst = str(pair.get("target") or pair.get("dst") or "").upper()
        if not src or not dst: 
            return {"ok": False, "error": "bad_pair"}

        mode = (pair.get("mode") or "one-way").lower()
        features = pair.get("features") or {"watchlist": {"enable": True, "add": True, "remove": True}}

        def f_enabled(name: str) -> Tuple[bool, bool]:
            f = features.get(name)
            if isinstance(f, bool): 
                return f, False
            if isinstance(f, dict):
                en = bool(f.get("enable", f.get("add", True)))
                rm = bool(f.get("remove", False))
                return en, rm
            return False, False

        if (pair.get("feature") or "watchlist").lower() == "multi":
            feat_names: List[str] = []
            for fname in ("watchlist", "ratings", "history", "playlists"):
                if self.providers.get(src) and self.providers.get(dst):
                    if self.providers[src].features().get(fname) and self.providers[dst].features().get(fname):
                        feat_names.append(fname)
            feat_names = feat_names or ["watchlist"]
        else:
            feat_names = [(pair.get("feature") or "watchlist").lower()]

        out_summary = {"added": 0, "removed": 0}
        self._emit("pair:start", src=src, dst=dst, mode=mode, feature=",".join(feat_names), dry_run=bool(dry_run))

        ttl_days = int(((self.cfg.get("sync") or {}).get("tombstone_ttl_days") or 30))
        include_observed_deletes_cfg = (self.cfg.get("sync") or {}).get("include_observed_deletes")
        include_observed_deletes = True if include_observed_deletes_cfg is None else bool(include_observed_deletes_cfg)

        for fname in feat_names:
            enable, allow_removals = f_enabled(fname)
            if not enable: 
                continue

            if mode == "one-way":
                self._emit_info(f"[1/1] {src} → {dst} | mode=one-way dry_run={dry_run}")
                self._emit_info(f"    • feature={fname} removals={allow_removals}")
                res = self.apply_direction(src=src, dst=dst, feature=fname, allow_removals=allow_removals, dry_run=dry_run)
                out_summary["added"] += int(res.get("adds", 0))
                out_summary["removed"] += int(res.get("removes", 0))

            elif mode == "two-way":
                self._emit_info(f"[1/1] {src} → {dst} | mode=two-way dry_run={dry_run}")
                self._emit_info(f"    • feature={fname} removals={allow_removals}")
                res2 = self._two_way_sync(
                    src, dst,
                    feature=fname,
                    allow_removals=allow_removals,
                    dry_run=dry_run,
                    include_observed_deletes=include_observed_deletes,
                    tomb_ttl_days=ttl_days,
                )
                out_summary["added"] += int(res2.get("adds_to_A", 0)) + int(res2.get("adds_to_B", 0))
                out_summary["removed"] += int(res2.get("rem_from_A", 0)) + int(res2.get("rem_from_B", 0))
                self._emit("two:done", a=src, b=dst, feature=fname, res={"adds": out_summary["added"], "removes": out_summary["removed"]})
            else:
                self._emit_info(f"[!] Unknown mode: {mode}")

        return {"ok": True, **out_summary}

    #-------------------- run all pairs
    def run_pairs(
        self,
        *,
        dry_run: bool = False,
        progress: Optional[Callable[[str], None]] = None,
        write_state_json: bool = True,
        state_path: Optional[Path] = None,
        use_snapshot: bool = True,
        **_kwargs,
    ) -> Dict[str, Any]:
        if progress is not None: 
            self.on_progress = progress

        ttl_days = int(((self.cfg.get("sync") or {}).get("tombstone_ttl_days") or 30))
        self.prune_tombstones(older_than_secs=ttl_days * 24 * 3600)

        prov_names = sorted(self.providers.keys())
        feat_map = {p: dict(self.providers[p].features()) for p in prov_names}
        self._emit_info(f"[i] Providers: {prov_names}")
        self._emit_info(f"[i] Features: {feat_map}")

        pairs = list((self.cfg or {}).get("pairs") or [])
        pairs = [p for p in pairs if p.get("enabled", True)]
        if not pairs:
            self._emit_info("[i] No pairs configured — skipping.")
            return {"ok": True, "added": 0, "removed": 0, "pairs": 0}

        self._emit("run:start", dry_run=bool(dry_run), conflict=self.conflict.prefer)

        added_total = 0; removed_total = 0
        #pre_counts = {}
        #post_counts = {}

        for i, pair in enumerate(pairs, 1):
            d = dict(pair)
            self._emit("run:pair", i=i, n=len(pairs), src=str(d.get("source")).upper(), dst=str(d.get("target")).upper(),
                       mode=(d.get("mode") or "one-way").lower(),
                       feature=(d.get("feature") or "watchlist").lower(),
                       dry_run=bool(dry_run))
            res = self.run_pair(d, dry_run=dry_run)
            added_total += int(res.get("added", 0))
            removed_total += int(res.get("removed", 0))

        if write_state_json:
            try:
                state_obj = self._persist_state_wall(feature="watchlist")
                if state_path and Path(state_path).resolve() != self.files.state.resolve():
                    tmp = Path(state_path).with_suffix(Path(state_path).suffix + ".tmp")
                    tmp.write_text(json.dumps(state_obj, indent=2), "utf-8")
                    tmp.replace(Path(state_path))
            except Exception as e:
                self._emit_info(f"[!] state.persist_failed: {e}")

        # stats: record final run summary, emit HTTP overview + rate warnings
        try:
            self.stats.record_summary(added=added_total, removed=removed_total)
            http24 = self.stats.http_overview(hours=24)
            self._emit("http:overview", window_hours=24, data=http24)
            self._maybe_emit_rate_warnings()
        except Exception:
            pass

        try:
            now = int(time.time())
            payload = {"started_at": now, "finished_at": now,
                       "result": {"added": added_total, "removed": removed_total}}
            self.files.save_last(payload)
        except Exception: 
            pass

        self._emit("run:done", added=added_total, removed=removed_total, pairs=len(pairs))
        return {"ok": True, "added": added_total, "removed": removed_total, "pairs": len(pairs)}
