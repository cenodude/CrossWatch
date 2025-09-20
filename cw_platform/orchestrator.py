from __future__ import annotations

"""
Provider-agnostic Orchestrator
------------------------------
"""

# -------------------- imports
import json
import time
import inspect
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Protocol, Sequence, Tuple

# -------------------- config base
try:
    from . import config_base
except Exception:
    class config_base:  # type: ignore
        @staticmethod
        def CONFIG_BASE() -> str:
            return "./"

# -------------------- logging shim
class _Logger:
    def __call__(self, *a): print(*a)
    def info(self, *a): print(*a)
    def warn(self, *a): print(*a)
    warning = warn
    def error(self, *a): print(*a)
    def success(self, *a): print(*a)
log = _Logger()

# -------------------- statistics (provider-agnostic)
try:
    from _statistics import Stats  # type: ignore
except Exception:  # pragma: no cover
    class Stats:  # minimal noop fallback
        def __init__(self, *a, **k): pass
        def record_summary(self, *a, **k): pass
        def overview(self, *a, **k): return {}
        def http_overview(self, *a, **k): return {}



# -------------------- provider protocol
class InventoryOps(Protocol):
    def name(self) -> str: ...
    def label(self) -> str: ...
    def features(self) -> Mapping[str, bool]: ...
    def capabilities(self) -> Mapping[str, Any]: ...
    def build_index(self, cfg: Mapping[str, Any], *, feature: str) -> Mapping[str, Dict[str, Any]]: ...
    def add(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool=False) -> Dict[str, Any]: ...
    def remove(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool=False) -> Dict[str, Any]: ...
    # Optional: modules may expose activities(cfg) → dict with timestamps

# -------------------- module loader
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
        needed = ("name","label","features","capabilities","build_index","add","remove")
        if obj and all(hasattr(obj, fn) for fn in needed):
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

# -------------------- canonical helpers
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
    out = {
        "ids": {k: item.get("ids", {}).get(k) for k in _ID_KEYS if item.get("ids", {}).get(k)},
        "title": item.get("title"),
        "year": item.get("year"),
        "type": (item.get("type") or "").lower() or None,
    }
    if item.get("rating") is not None:
        out["rating"] = item.get("rating")
    if item.get("rated_at") is not None:
        out["rated_at"] = item.get("rated_at")
    return out


# -------------------- state files
class _Files:
    def __init__(self, base: Path):
        self.base = base
        self.state = base / "state.json"
        self.tomb  = base / "tombstones.json"
        self.last  = base / "last_sync.json"
        self.hide  = base / "watchlist_hide.json"
        # NEW: compact snapshot of latest ratings changes (for UI)
        self.ratings_changes = base / "ratings_changes.json"

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
        # providers.<NAME>.<feature>.baseline.items ; checkpoint
        return self._read(self.state, {"providers": {}, "wall": [], "last_sync_epoch": None})

    def save_state(self, data: Mapping[str, Any]):
        self._write_atomic(self.state, data)

    def load_tomb(self) -> Dict[str, Any]:
        # include ttl_sec if present; keep keys/pruned_at by default
        t = self._read(self.tomb, {"keys": {}, "pruned_at": None})
        if "ttl_sec" not in t:
            t["ttl_sec"] = None
        return t

    def save_tomb(self, data: Mapping[str, Any]):
        self._write_atomic(self.tomb, data)

    def save_last(self, data: Mapping[str, Any]):
        self._write_atomic(self.last, data)

    def clear_watchlist_hide(self) -> None:
        try:
            if self.hide.exists():
                self.hide.unlink()
        except Exception:
            try:
                self.hide.write_text("[]", encoding="utf-8")
            except Exception:
                pass

    # NEW: persist ratings change snapshot
    def save_ratings_changes(self, data: Mapping[str, Any]) -> None:
        try:
            self._write_atomic(self.ratings_changes, data)
        except Exception:
            pass


# -------------------- policy
@dataclass
class ConflictPolicy:
    prefer: str = "source"  # used only for true conflicts

# -------------------- orchestrator
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

        # optional metadata (best-effort)
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

        # thresholds for rate warnings
        telem = dict((self.cfg.get("telemetry") or {}))
        self.warn_thresholds = (telem.get("warn_rate_remaining") or {"TRAKT": 100, "SIMKL": 50, "PLEX": 0})

        # NEW: in-run snapshot memo (sec). 0 disables memo.
        rt = dict(self.cfg.get("runtime") or {})
        self._snap_ttl_sec = int(rt.get("snapshot_ttl_sec") or 0)
        self._snap_cache: Dict[Tuple[str, str], Tuple[float, Dict[str, Dict[str, Any]]]] = {}

        # NEW: pair context (set in run_pair feature loop, read by _ratings_cfg)
        self._pair_ctx: Optional[Mapping[str, Any]] = None

    # -------------------- logging
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

    def _post_feature_success(self, feature: str) -> None:
        # No per-feature hidefile clearing anymore; we do it once at end-of-run.
        return

    # -------------------- ratings change snapshot helper
    def _record_ratings_changes(self, payload: Mapping[str, Any]) -> None:
        try:
            pp = dict(payload or {})
            pp.setdefault("ts", int(time.time()))
            self.files.save_ratings_changes(pp)
        except Exception:
            pass
        

    # -------------------- ratings stats + spotlight (for UI "last 3")
    def _stats_rating(self, action: str, items: Sequence[Mapping[str, Any]], source: str) -> None:
        """Record rating events for spotlights. Fallback-safe."""
        if not items:
            return
        # Stats recording (if supported)
        try:
            rec = getattr(self.stats, "record_event", None)
            if callable(rec):
                for it in items[:50]:
                    ck = canonical_key(it)
                    if not ck:
                        continue
                    title = (it.get("title") or it.get("name") or "")[:200]
                    typ = (it.get("type") or "").lower() or None
                    rec(action=action, key=ck, title=title, source=source.lower(), typ=typ, feature="ratings",
                        rating=it.get("rating"), rated_at=it.get("rated_at"))
        except Exception:
            pass
        # Optional: immediate spotlight ping for live UIs
        try:
            self._emit("spotlight", feature="ratings", action=action,
                       source=source.upper(), items=[minimal(x) for x in items[:3]])
        except Exception:
            pass

    # -------------------- generic stats for non-ratings (watchlist/history/playlists)
    def _stats_generic(self, feature: str, action: str, items: Sequence[Mapping[str, Any]], source: str) -> None:
        """Record non-rating events so Insights can build per-feature lanes."""
        if not items:
            return
        try:
            rec = getattr(self.stats, "record_event", None)
            if not callable(rec):
                return

            # Map to stable action names the UI expects.
            if feature == "history":
                mapped = "watch" if action == "add" else "unwatch"
            elif feature == "playlists":
                mapped = "playlist_add" if action == "add" else "playlist_remove"
            else:
                mapped = action  # watchlist: add/remove

            for it in items[:50]:
                ck = canonical_key(it)
                if not ck:
                    continue
                title = (it.get("title") or it.get("name") or "")[:200]
                typ = (it.get("type") or "").lower() or None
                rec(action=mapped, key=ck, title=title, source=source.lower(), typ=typ, feature=feature)
        except Exception:
            # Non-blocking by design
            pass

    # -------------------- ratings helpers
    def _ratings_cfg(self) -> Dict[str, Any]:
        """
        Effective ratings policy for the current execution context.

        Rules:
        - Start from global config.ratings.
        - Merge per-pair overrides from self._pair_ctx.features.ratings (types, mode, from_date).
        - Normalize:
            * 'types' accepts ["movies","shows","seasons","episodes"] or ["all"].
            * 'mode' in {"only_new","from_date","all"} (default: "only_new").
            * 'from_date' applies only when mode == "from_date"; otherwise cleared.
        """
        base = dict((self.cfg.get("ratings") or {}))

        # Pull per-pair overrides if a pair is currently being executed.
        ov: Dict[str, Any] = {}
        try:
            ov = (((self._pair_ctx or {}).get("features") or {}).get("ratings") or {})
        except Exception:
            ov = {}

        merged = dict(base)
        for k in ("types", "mode", "from_date"):
            if k in ov and ov[k] not in (None, ""):
                merged[k] = ov[k]

        # Enabled is decided elsewhere (pair feature toggle). Keep global value as-is.
        merged["enabled"] = bool(base.get("enabled", False))

        # Normalize types (support "all" alias)
        t = merged.get("types") or ["movies", "shows"]
        if isinstance(t, str):
            t = [t]
        t = [str(x).strip().lower() for x in t if isinstance(x, (str,))]
        if "all" in t:
            t = ["movies", "shows", "seasons", "episodes"]
        else:
            t = [x for x in ("movies","shows","seasons","episodes") if x in t] or ["movies","shows"]
        merged["types"] = t

        # Normalize mode
        m = str(merged.get("mode") or "only_new").strip().lower()
        if m not in ("only_new", "from_date", "all"):
            m = "only_new"
        merged["mode"] = m

        # Normalize from_date
        fd = (merged.get("from_date") or "").strip()
        if m != "from_date":
            fd = ""
        merged["from_date"] = fd

        return merged

    def _ratings_enabled(self) -> bool:
        return bool(self._ratings_cfg().get("enabled", False))

    def _ratings_types(self) -> List[str]:
        return list(self._ratings_cfg().get("types") or ["movies","shows"])

    def _ratings_mode(self) -> str:
        return str(self._ratings_cfg().get("mode") or "only_new")

    def _ratings_from_date(self) -> Optional[str]:
        fd = (self._ratings_cfg().get("from_date") or "").strip()
        return fd or None

    def _parse_ts(self, s: Optional[str]) -> Optional[int]:
        """Parse ISO-like timestamps or YYYY-MM-DD; return epoch seconds or None."""
        if not s or not isinstance(s, str):
            return None
        s = s.strip()
        try:
            from datetime import datetime, timezone
            if len(s) == 10 and s[4] == "-" and s[7] == "-":
                dt = datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                return int(dt.timestamp())
            # Normalize 'Z' suffix
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except Exception:
            return None

    def _filter_types(self, idx: Mapping[str, Dict[str, Any]], types: Sequence[str]) -> Dict[str, Dict[str, Any]]:
        """Keep only items whose 'type' matches one of the requested types."""
        want = set(t.lower() for t in (types or []))
        if not want:
            return dict(idx)
        out: Dict[str, Dict[str, Any]] = {}
        for k, v in idx.items():
            t = (v.get("type") or "").lower()
            if t in want:
                out[k] = v
        return out

    def _filter_mode_ratings(self, idx, *, mode: str, from_date: Optional[str], checkpoint: Optional[str]):
        if not idx:
            return {}
        if mode == "all":
            return dict(idx)

        out = {}
        cp_ts = self._parse_ts(checkpoint)
        fd_ts = self._parse_ts(from_date)

        eps = int((self.cfg.get("runtime") or {}).get("ratings_checkpoint_epsilon_sec") or 2)

        for k, v in idx.items():
            ra_ts = self._parse_ts(v.get("rated_at"))
            if ra_ts is None:
                continue  # conservative: skip unless mode == all
            if mode == "only_new":
                if cp_ts is None or ra_ts >= (cp_ts - eps):   # was: ra_ts > cp_ts
                    out[k] = v
            elif mode == "from_date":
                if fd_ts is not None and ra_ts >= fd_ts:
                    out[k] = v
        return out


    def _ratings_equal(self, a: Mapping[str, Any], b: Mapping[str, Any]) -> bool:
        """Compare rating value; timestamps are used for recency, not equality."""
        return (a.get("rating") == b.get("rating"))

    # -------------------- baseline helpers
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
        

    # -------------------- activities (optional)
    def _module_checkpoint(self, ops: InventoryOps, feature: str) -> Optional[str]:
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

    # -------------------- snapshots (modules may implement internal delta)
    def build_snapshots(self, *, feature: str) -> Dict[str, Dict[str, Any]]:
        snaps: Dict[str, Dict[str, Any]] = {}
        now = time.time()
        for name, ops in self.providers.items():
            if not ops.features().get(feature, False):
                continue
            memo_key = (name, feature)
            if self._snap_ttl_sec > 0:
                ent = self._snap_cache.get(memo_key)
                if ent and (now - ent[0]) < self._snap_ttl_sec:
                    snaps[name] = ent[1]
                    self._dbg("snapshot.memo", provider=name, feature=feature, count=len(ent[1]))
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
            if self._snap_ttl_sec > 0:
                self._snap_cache[memo_key] = (now, canon)
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

    def diff_ratings(
        self,
        src_idx: Mapping[str, Dict[str, Any]],
        dst_idx: Mapping[str, Dict[str, Any]],
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Returns (to_add_or_update, to_remove, unchanged).
        - Add: key missing on dst.
        - Update: key exists on both, rating differs, and src.rated_at >= dst.rated_at (or dst missing timestamp).
        - Remove: key exists on dst but missing on src (unrate handled upstream via removals policy).
        """
        adds_updates: List[Dict[str, Any]] = []
        removes: List[Dict[str, Any]] = []
        unchanged: List[Dict[str, Any]] = []

        for k, sv in src_idx.items():
            dv = dst_idx.get(k)
            if dv is None:
                adds_updates.append(minimal(sv))
                continue
            if not self._ratings_equal(sv, dv):
                s_ts = self._parse_ts(sv.get("rated_at"))
                d_ts = self._parse_ts(dv.get("rated_at"))
                if d_ts is None or (s_ts is not None and d_ts is not None and s_ts >= d_ts):
                    adds_updates.append(minimal(sv))
                else:
                    unchanged.append(minimal(dv))
            else:
                unchanged.append(minimal(dv))

        for k, dv in dst_idx.items():
            if k not in src_idx:
                removes.append(minimal(dv))

        return adds_updates, removes, unchanged

    # -------------------- tombstones (global, plus pair-scoped helpers)
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

    def _pair_key(self, a: str, b: str) -> str:
        return "-".join(sorted([a.upper(), b.upper()]))

    def _tomb_add_keys_for_feature(self, feature: str, keys: Iterable[str], *, pair: Optional[str] = None) -> int:
        """
        Mark tombstones for this feature both globally (feature|key) and, if provided,
        pair-scoped (feature:PAIR|key).
        """
        t = self.files.load_tomb()
        ks = t.setdefault("keys", {})
        now = int(time.time())
        added = 0

        prefixes = [feature]
        if pair:
            prefixes.append(f"{feature}:{pair}")

        for k in keys:
            for pref in prefixes:
                nk = f"{pref}|{k}"
                if nk not in ks:
                    ks[nk] = now
                    added += 1

        self.files.save_tomb(t)
        if added or self.debug:
            self._dbg("tombstones.marked", feature=feature, added=added, scope="global+pair" if pair else "global")
        return added

    def _tomb_keys_for_feature(self, feature: str, *, pair: Optional[str] = None, include_global: bool = True) -> Dict[str, int]:
        ks_all = dict((self.files.load_tomb().get("keys") or {}))
        out: Dict[str, int] = {}

        def _collect(prefix: str):
            plen = len(prefix) + 1
            for k, ts in ks_all.items():
                if isinstance(k, str) and k.startswith(prefix + "|"):
                    orig = k[plen:]
                    out[orig] = int(ts)

        if include_global:
            _collect(feature)

        if pair:
            _collect(f"{feature}:{pair}")

        return out

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
        raw = (self.files.load_tomb().get("keys") or {}).keys()
        base_keys = set()
        for k in raw:
            if isinstance(k, str):
                base_keys.add(k.split("|", 1)[-1])
        if extra_block:
            base_keys |= set(extra_block)

        out = [it for it in items if not self._keys_hit_item(base_keys, it)]
        if self.debug and len(out) != len(items):
            self._dbg("tombstones.filtered", before=len(items), after=len(out))
        return out

    def _tomb_hits_item(self, tomb: set[str], item: Mapping[str, Any]) -> bool:
        if not tomb:
            return False
        ck = canonical_key(item)
        if ck in tomb:
            return True
        ids = (item.get("ids") or {})
        for k in _ID_KEYS:
            v = ids.get(k)
            if v is not None and f"{k}:{str(v).lower()}" in tomb:
                return True
        t = (item.get("type") or "").lower()
        ttl = str(item.get("title") or "").strip().lower()
        yr  = item.get("year") or ""
        return f"{t}|title:{ttl}|year:{yr}" in tomb

    def _keys_hit_item(self, keys: set[str], item: Mapping[str, Any]) -> bool:
        if not keys:
            return False
        ck = canonical_key(item)
        if ck in keys:
            return True
        ids = (item.get("ids") or {})
        for k in _ID_KEYS:
            v = ids.get(k)
            if v is not None and f"{k}:{str(v).lower()}" in keys:
                return True
        t = (item.get("type") or "").lower()
        ttl = str(item.get("title") or "").strip().lower()
        yr  = item.get("year") or ""
        return f"{t}|title:{ttl}|year:{yr}" in keys

    # -------------------- enrichment
    def maybe_enrich(
        self,
        items: List[Dict[str, Any]],
        *,
        want_ids: bool = True,
        dst: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        if not items or not want_ids or not self.meta:
            return items

        def _has_ids_for(provider: Optional[str], ids: Mapping[str, Any]) -> bool:
            p = (provider or "").upper()
            if p == "TRAKT":
                keys = ("trakt", "tmdb", "imdb", "tvdb")
            elif p == "SIMKL":
                keys = ("imdb", "tmdb", "tvdb", "slug")
            elif p == "PLEX":
                keys = ("plex", "guid", "imdb", "tmdb", "tvdb", "trakt")
            else:
                keys = ("tmdb", "imdb", "tvdb", "trakt", "slug", "guid", "plex")
            return any((ids or {}).get(k) for k in keys)

        need: List[Dict[str, Any]] = []
        for it in items:
            ids = (it.get("ids") or {})
            if dst and _has_ids_for(dst, ids):
                continue
            if not (ids.get("tmdb") or ids.get("imdb")):
                need.append(it)

        if not need:
            return items

        try:
            if hasattr(self.meta, "resolve_many") and callable(getattr(self.meta, "resolve_many")):
                res = self.meta.resolve_many(need)  # type: ignore[attr-defined]
            else:
                try:
                    res = self.meta.resolve(need)  # type: ignore[call-arg]
                except TypeError:
                    res = self.meta.resolve(items=need)  # type: ignore[call-arg]
        except Exception as e:
            self._emit_info(f"[!] metadata.enrich_failed: {e}")
            return items

        res = res or []

        _idmap: Dict[str, Dict[str, Any]] = {}
        for r in res:
            rids = (r.get("ids") or {})
            for k, v in rids.items():
                if v is not None:
                    _idmap[f"{str(k).lower()}:{str(v).lower()}"] = r
            t = (r.get("type") or "").lower()
            ttl = str(r.get("title") or "").strip().lower()
            yr  = r.get("year") or ""
            _idmap[f"{t}|title:{ttl}|year:{yr}"] = r

        def _merge_preserving(source: Dict[str, Any], resolved: Dict[str, Any]) -> Dict[str, Any]:
            out = dict(resolved or {})
            ids = dict(source.get("ids") or {})
            ids.update((resolved or {}).get("ids") or {})
            if ids:
                out["ids"] = ids
            for k in ("rating", "rated_at", "watched_at", "watched", "playlist", "items", "type", "title", "year"):
                if source.get(k) is not None and out.get(k) is None:
                    out[k] = source[k]
            if source.get("type"):
                out["type"] = (source.get("type") or out.get("type") or "").lower() or None
            return out

        out_items: List[Dict[str, Any]] = []
        for it in items:
            if it not in need:
                out_items.append(it)
                continue
            ids = (it.get("ids") or {})
            candidate = None
            for k, v in ids.items():
                if v is None:
                    continue
                candidate = _idmap.get(f"{str(k).lower()}:{str(v).lower()}")
                if candidate:
                    break
            if not candidate:
                t = (it.get("type") or "").lower()
                ttl = str(it.get("title") or "").strip().lower()
                yr  = it.get("year") or ""
                candidate = _idmap.get(f"{t}|title:{ttl}|year:{yr}")

            out_items.append(_merge_preserving(it, candidate or {}))

        if dst:
            out_items = [it for it in out_items if _has_ids_for(dst, it.get("ids") or {})]

        self._dbg("enrich.done",
                requested=len(items),
                enriched=len([o for o in out_items if (o.get('ids') or {}).get('tmdb') or (o.get('ids') or {}).get('imdb')]),
                )
        return out_items

    # -------------------- retry
    def _retry(self, fn: Callable[[], Any], *, attempts: int = 3, base_sleep: float = 0.5) -> Any:
        last = None
        for i in range(attempts):
            try:
                return fn()
            except Exception as e:
                last = e; time.sleep(base_sleep * (2 ** i))
        raise last  # type: ignore

    # -------------------- one-way
    def apply_direction(self, *, src: str, dst: str, feature: str, allow_removals: bool, dry_run: bool=False) -> Dict[str, Any]:
        src = src.upper(); dst = dst.upper()
        sops = self.providers[src]; dops = self.providers[dst]
        if not sops.features().get(feature) or not dops.features().get(feature):
            return {"ok": True, "skipped": True, "reason": "feature-not-supported", "src": src, "dst": dst}

        # 1) fresh snapshots
        self._emit("snapshot:start", src=src, dst=dst, feature=feature)
        snaps = self.build_snapshots(feature=feature)
        src_idx = snaps.get(src, {}) or {}
        dst_delta = snaps.get(dst, {}) or {}

        # 2) previous baseline (for delta providers)
        prev_state = self.files.load_state() or {}
        prev_provs = (prev_state.get("providers") or {})
        prev_dst = dict((((prev_provs.get(dst, {}) or {}).get(feature, {}) or {}).get("baseline", {}) or {}).get("items") or {})

        # Effective destination = previous baseline + current delta
        dst_full: Dict[str, Any] = dict(prev_dst)
        dst_full.update(dst_delta)

        # Ratings-aware planning
        ratings_changes_snapshot = None
        just_adds: List[Dict[str, Any]] = []
        updates: List[Dict[str, Any]] = []
        if feature == "ratings":
            r_types = self._ratings_types()
            src_idx = self._filter_types(src_idx, r_types)
            dst_full = self._filter_types(dst_full, r_types)

            mode = self._ratings_mode()
            prev_state_cp = (((prev_provs.get(src, {}) or {}).get(feature, {}) or {}).get("checkpoint"))
            checkpoint = prev_state_cp or self._module_checkpoint(sops, feature) or None
            src_idx = self._filter_mode_ratings(src_idx, mode=mode, from_date=self._ratings_from_date(), checkpoint=checkpoint)

            additions, removals, _unchanged = self.diff_ratings(src_idx, dst_full)

            def _classify_adds(adds: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
                """Split A→B ratings into adds vs updates on B."""
                _adds: List[Dict[str, Any]] = []
                _upd: List[Dict[str, Any]] = []
                for it in adds:
                    ck = canonical_key(it)
                    if ck in dst_full:
                        old = dst_full.get(ck, {})
                        _upd.append({**minimal(it), "old_rating": old.get("rating")})
                    else:
                        _adds.append(minimal(it))
                return _adds, _upd

            just_adds, updates = _classify_adds(additions)
            ratings_changes_snapshot = {
                "pair": f"{src}->{dst}",
                "feature": "ratings",
                "mode": mode,
                "adds": just_adds,
                "updates": updates,
                "removes": [minimal(x) for x in removals] if allow_removals else [],
            }
        else:
            additions, removals = self.diff(src_idx, dst_full)

        # Bootstrap: first run → never remove
        bootstrap = (not prev_dst) and not (self.files.load_tomb().get("keys") or {})
        if bootstrap and allow_removals:
            removals = []
            self._dbg("bootstrap.no-delete", src=src, dst=dst)

        self._emit("plan", src=src, dst=dst, feature=feature, add=len(additions), rem=len(removals) if allow_removals else 0)

        # 3) apply removes
        res_rem = {"ok": True, "count": 0}
        if allow_removals and removals:
            self._emit("apply:remove:start", dst=dst, feature=feature, count=len(removals))
            res_rem = self._retry(lambda: dops.remove(self.cfg, removals, feature=feature, dry_run=dry_run))
            # include items for UI (ratings spotlight)
            try:
                items_payload = [minimal(x) for x in removals][:50] if feature == "ratings" else None
                self._emit("apply:remove:done", dst=dst, feature=feature, count=len(removals), result=res_rem, items=items_payload)
            finally:
                if removals and not dry_run:
                    pair = self._pair_key(src, dst)
                    self._tomb_add_keys_for_feature(feature, [canonical_key(it) for it in removals], pair=pair)
                    for it in removals:
                        dst_full.pop(canonical_key(it), None)
                    # stats for feature
                    if feature == "ratings":
                        self._stats_rating("unrate", removals, dst)
                    else:
                        self._stats_generic(feature, "remove", removals, dst)
        else:
            self._emit("apply:remove:done", dst=dst, feature=feature, count=0, result=res_rem)

        # 4) enrich if needed
        want_ids = not bool(dops.capabilities().get("provides_ids"))
        additions = self.maybe_enrich(additions, want_ids=want_ids, dst=dst)

        # 5) apply adds (incl. rating updates)
        self._emit("apply:add:start", dst=dst, feature=feature, count=len(additions))
        res_add = {"ok": True, "count": 0}
        if additions:
            res_add = self._retry(lambda: dops.add(self.cfg, additions, feature=feature, dry_run=dry_run))
        items_payload = [minimal(x) for x in additions][:50] if feature == "ratings" else None
        self._emit("apply:add:done", dst=dst, feature=feature, count=len(additions), result=res_add, items=items_payload)

        if additions and not dry_run:
            for it in additions:
                dst_full[canonical_key(it)] = minimal(it)
            # stats for feature
            if feature == "ratings":
                if just_adds:
                    self._stats_rating("rate", just_adds, dst)
                if updates:
                    self._stats_rating("update_rating", updates, dst)
            else:
                self._stats_generic(feature, "add", additions, dst)

        # 6) commit state
        state = self.files.load_state()
        self._commit_baseline(state, src, feature, src_idx)
        self._commit_baseline(state, dst, feature, dst_full)
        self._commit_checkpoint(state, src, feature, self._module_checkpoint(sops, feature))
        self._commit_checkpoint(state, dst, feature, self._module_checkpoint(dops, feature))
        state["last_sync_epoch"] = int(time.time())
        self.files.save_state(state)

        # 7) persist ratings change snapshot for UI
        if feature == "ratings" and ratings_changes_snapshot is not None:
            self._record_ratings_changes(ratings_changes_snapshot)

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

    # -------------------- two-way (pair-scoped tombstones; baseline+delta aware)
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
        A_cur = snaps.get(a, {}) or {}
        B_cur = snaps.get(b, {}) or {}

        # 2) previous baselines
        prev_state = self.files.load_state() or {}
        prev_provs = (prev_state.get("providers") or {})
        prevA = dict((((prev_provs.get(a, {}) or {}).get(feature, {}) or {}).get("baseline", {}) or {}).get("items") or {})
        prevB = dict((((prev_provs.get(b, {}) or {}).get(feature, {}) or {}).get("baseline", {}) or {}).get("items") or {})

        # Effective views = baseline + current delta
        A_eff: Dict[str, Any] = dict(prevA); A_eff.update(A_cur)
        B_eff: Dict[str, Any] = dict(prevB); B_eff.update(B_cur)

        # Pair-scoped tombstones with TTL (union of global + pair)
        pair = self._pair_key(a, b)
        ttl_days = int(((self.cfg.get("sync") or {}).get("tombstone_ttl_days") or tomb_ttl_days))
        now = int(time.time()); ttl_secs = max(1, ttl_days) * 24 * 3600
        tomb_map = self._tomb_keys_for_feature(feature, pair=pair, include_global=True)
        tomb = {k for k, ts in tomb_map.items() if (now - int(ts)) <= ttl_secs}
        reasons: Dict[str, str] = {k: "tomb:explicit" for k in tomb}

        # Observed deletions (scoped)
        obsA: set[str] = set()
        obsB: set[str] = set()
        bootstrap = (not prevA) and (not prevB) and not tomb
        if include_observed_deletes and not bootstrap:
            obsA = {k for k in prevA.keys() if k not in A_cur}
            obsB = {k for k in prevB.keys() if k not in B_cur}
            if obsA or obsB:
                newly = (obsA | obsB) - tomb
                if newly:
                    self._tomb_add_keys_for_feature(feature, newly, pair=pair)
                    for k in newly: reasons[k] = "tomb:observed-since-last-state"
                    tomb |= newly
            self._emit("debug", msg="observed.deletions", a=len(obsA), b=len(obsB), tomb=len(tomb))
        else:
            self._emit("debug", msg="observed.deletions", a=0, b=0, tomb=len(tomb))

        # Remove observed deletions from effective view so they become one-sided diffs
        if include_observed_deletes:
            for k in list(obsA):
                A_eff.pop(k, None)
            for k in list(obsB):
                B_eff.pop(k, None)

        # Alias-aware indices (used by non-ratings path)
        def _alias_index(idx: Mapping[str, Mapping[str, Any]]) -> Dict[str, str]:
            m: Dict[str, str] = {}
            for ck, it in idx.items():
                ids = (it.get("ids") or {})
                for k in _ID_KEYS:
                    v = ids.get(k)
                    if v:
                        m[f"{k}:{str(v)}".lower()] = ck
            return m

        A_alias = _alias_index(A_eff); B_alias = _alias_index(B_eff)
        add_to_B, add_to_A, rem_from_A, rem_from_B = [], [], [], []

        ratings_changes_snapshot = None
        adds_to_A: List[Dict[str, Any]] = []
        adds_to_B: List[Dict[str, Any]] = []
        updates_on_A: List[Dict[str, Any]] = []
        updates_on_B: List[Dict[str, Any]] = []

        if feature == "ratings":
            # Filter by requested types
            r_types = self._ratings_types()
            A_eff = self._filter_types(A_eff, r_types)
            B_eff = self._filter_types(B_eff, r_types)

            # Directional mode filtering (source-side)
            mode = self._ratings_mode()
            prevA_cp = (((prev_provs.get(a, {}) or {}).get(feature, {}) or {}).get("checkpoint"))
            prevB_cp = (((prev_provs.get(b, {}) or {}).get(feature, {}) or {}).get("checkpoint"))
            A_src = self._filter_mode_ratings(A_eff, mode=mode, from_date=self._ratings_from_date(), checkpoint=prevA_cp or self._module_checkpoint(aops, feature))
            B_src = self._filter_mode_ratings(B_eff, mode=mode, from_date=self._ratings_from_date(), checkpoint=prevB_cp or self._module_checkpoint(bops, feature))

            # Compute adds/updates per direction via value+recency diff
            to_B, remB_by_absence, _unchB = self.diff_ratings(A_src, B_eff)
            to_A, remA_by_absence, _unchA = self.diff_ratings(B_src, A_eff)

            # Respect tombstones/observed deletes for removals
            def _should_remove(item: Mapping[str, Any], obs_keys: set[str]) -> bool:
                return self._tomb_hits_item(tomb, item) or self._keys_hit_item(obs_keys, item)

            # Classify A→B
            for it in to_B:
                if allow_removals and _should_remove(it, obsB):
                    rem_from_A.append(minimal(it))
                else:
                    add_to_B.append(minimal(it))

            # Classify B→A
            for it in to_A:
                if allow_removals and _should_remove(it, obsA):
                    rem_from_B.append(minimal(it))
                else:
                    add_to_A.append(minimal(it))

            # Absence-based removals (gated)
            if allow_removals:
                for it in remA_by_absence:
                    if _should_remove(it, obsA):
                        rem_from_B.append(minimal(it))
                for it in remB_by_absence:
                    if _should_remove(it, obsB):
                        rem_from_A.append(minimal(it))

            # Bootstrap: never remove
            if bootstrap and allow_removals:
                rem_from_A.clear(); rem_from_B.clear()
                self._dbg("bootstrap.no-delete", a=a, b=b)

            # Build compact ratings change payload for UI
            def _classify(adds: List[Dict[str, Any]], dst_eff: Mapping[str, Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
                _adds: List[Dict[str, Any]] = []; _upd: List[Dict[str, Any]] = []
                for it in adds:
                    ck = canonical_key(it)
                    if ck in dst_eff:
                        _upd.append({**minimal(it), "old_rating": dst_eff.get(ck, {}).get("rating")})
                    else:
                        _adds.append(minimal(it))
                return _adds, _upd

            adds_to_A, updates_on_A = _classify(add_to_A, A_eff)
            adds_to_B, updates_on_B = _classify(add_to_B, B_eff)

            ratings_changes_snapshot = {
                "pair": f"{a}<->{b}",
                "feature": "ratings",
                "mode": mode,
                "to_A": {"adds": adds_to_A, "updates": updates_on_A, "removes": [minimal(x) for x in rem_from_B] if allow_removals else []},
                "to_B": {"adds": adds_to_B, "updates": updates_on_B, "removes": [minimal(x) for x in rem_from_A] if allow_removals else []},
            }

        else:
            # Presence-based planning (watchlist/history/etc.)
            for k, v in A_eff.items():
                ids = (v.get("ids") or {})
                in_B = (k in B_eff) or any(f"{idk}:{str(ids.get(idk))}".lower() in B_alias for idk in _ID_KEYS if ids.get(idk))
                if in_B:
                    continue
                if allow_removals and (self._tomb_hits_item(tomb, v) or self._keys_hit_item(obsB, v)):
                    rem_from_A.append(minimal(v))
                else:
                    add_to_B.append(minimal(v))

            for k, v in B_eff.items():
                ids = (v.get("ids") or {})
                in_A = (k in A_eff) or any(f"{idk}:{str(ids.get(idk))}".lower() in A_alias for idk in _ID_KEYS if ids.get(idk))
                if in_A:
                    continue
                if allow_removals and (self._tomb_hits_item(tomb, v) or self._keys_hit_item(obsA, v)):
                    rem_from_B.append(minimal(v))
                else:
                    add_to_A.append(minimal(v))

            if bootstrap and allow_removals:
                rem_from_A.clear(); rem_from_B.clear()
                self._dbg("bootstrap.no-delete", a=a, b=b)

        if (rem_from_A or rem_from_B) and self.debug:
            detail = []
            for it in rem_from_A:
                ck = canonical_key(it); detail.append({"side":"A","id":ck,"reason":reasons.get(ck,"tomb:unknown")})
            for it in rem_from_B:
                ck = canonical_key(it); detail.append({"side":"B","id":ck,"reason":reasons.get(ck,"tomb:unknown")})
            self._emit("two:plan:remove:reasons", detail=detail)

        self._emit("two:plan", a=a, b=b, feature=feature,
                add_to_A=len(add_to_A), add_to_B=len(add_to_B),
                rem_from_A=len(rem_from_A) if allow_removals else 0,
                rem_from_B=len(rem_from_B) if allow_removals else 0)

        # 3) apply removes
        resA_rem = {"ok": True, "count": 0}; resB_rem = {"ok": True, "count": 0}
        if allow_removals:
            self._emit("two:apply:remove:A:start", dst=a, feature=feature, count=len(rem_from_A))
            if rem_from_A:
                resA_rem = self._retry(lambda: aops.remove(self.cfg, rem_from_A, feature=feature, dry_run=dry_run))
                if rem_from_A and not dry_run:
                    self._tomb_add_keys_for_feature(feature, [canonical_key(it) for it in rem_from_A], pair=pair)
                    for it in rem_from_A:
                        A_eff.pop(canonical_key(it), None)
                    if feature == "ratings":
                        self._stats_rating("unrate", rem_from_A, a)
                    else:
                        self._stats_generic(feature, "remove", rem_from_A, a)
            self._emit("two:apply:remove:A:done", dst=a, feature=feature, count=len(rem_from_A), result=resA_rem,
                       items=[minimal(x) for x in rem_from_A][:50] if feature == "ratings" else None)

            self._emit("two:apply:remove:B:start", dst=b, feature=feature, count=len(rem_from_B))
            if rem_from_B:
                resB_rem = self._retry(lambda: bops.remove(self.cfg, rem_from_B, feature=feature, dry_run=dry_run))
                if rem_from_B and not dry_run:
                    self._tomb_add_keys_for_feature(feature, [canonical_key(it) for it in rem_from_B], pair=pair)
                    for it in rem_from_B:
                        B_eff.pop(canonical_key(it), None)
                    if feature == "ratings":
                        self._stats_rating("unrate", rem_from_B, b)
                    else:
                        self._stats_generic(feature, "remove", rem_from_B, b)
            self._emit("two:apply:remove:B:done", dst=b, feature=feature, count=len(rem_from_B), result=resB_rem,
                       items=[minimal(x) for x in rem_from_B][:50] if feature == "ratings" else None)

        # 4) enrich if provider can’t resolve IDs
        want_ids_A = not bool(aops.capabilities().get("provides_ids"))
        want_ids_B = not bool(bops.capabilities().get("provides_ids"))
        add_to_A = self.maybe_enrich(add_to_A, want_ids=want_ids_A, dst=a)
        add_to_B = self.maybe_enrich(add_to_B, want_ids=want_ids_B, dst=b)

        # 5) apply adds (includes updates; providers should upsert)
        resA_add = {"ok": True, "count": 0}; resB_add = {"ok": True, "count": 0}
        self._emit("two:apply:add:A:start", dst=a, feature=feature, count=len(add_to_A))
        if add_to_A:
            resA_add = self._retry(lambda: aops.add(self.cfg, add_to_A, feature=feature, dry_run=dry_run))
            if not dry_run:
                for it in add_to_A:
                    A_eff[canonical_key(it)] = minimal(it)
                if feature == "ratings":
                    if adds_to_A:
                        self._stats_rating("rate", adds_to_A, a)
                    if updates_on_A:
                        self._stats_rating("update_rating", updates_on_A, a)
                else:
                    self._stats_generic(feature, "add", add_to_A, a)
        self._emit("two:apply:add:A:done", dst=a, feature=feature, count=len(add_to_A), result=resA_add,
                   items=[minimal(x) for x in add_to_A][:50] if feature == "ratings" else None)

        self._emit("two:apply:add:B:start", dst=b, feature=feature, count=len(add_to_B))
        if add_to_B:
            resB_add = self._retry(lambda: bops.add(self.cfg, add_to_B, feature=feature, dry_run=dry_run))
            if not dry_run:
                for it in add_to_B:
                    B_eff[canonical_key(it)] = minimal(it)
                if feature == "ratings":
                    if adds_to_B:
                        self._stats_rating("rate", adds_to_B, b)
                    if updates_on_B:
                        self._stats_rating("update_rating", updates_on_B, b)
                else:
                    self._stats_generic(feature, "add", add_to_B, b)
        self._emit("two:apply:add:B:done", dst=b, feature=feature, count=len(add_to_B), result=resB_add,
                   items=[minimal(x) for x in add_to_B][:50] if feature == "ratings" else None)

        # 6) commit baselines from effective post-apply models
        state = self.files.load_state()
        self._commit_baseline(state, a, feature, A_eff)
        self._commit_baseline(state, b, feature, B_eff)
        self._commit_checkpoint(state, a, feature, self._module_checkpoint(aops, feature))
        self._commit_checkpoint(state, b, feature, self._module_checkpoint(bops, feature))
        state["last_sync_epoch"] = int(time.time())
        self.files.save_state(state)

        # Persist ratings change snapshot (UI)
        if feature == "ratings" and ratings_changes_snapshot is not None:
            self._record_ratings_changes(ratings_changes_snapshot)

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

    # -------------------- state persist (state-driven wall; no extra fetch)
    def _persist_state_wall(self, *, feature: str = "watchlist") -> Dict[str, Any]:
        """Flatten provider baselines into a simple 'wall' for UI use."""
        state = self.files.load_state() or {}
        providers_block: Dict[str, Any] = state.get("providers") or {}

        wall: List[Dict[str, Any]] = []
        for prov, featmap in providers_block.items():
            fentry = (featmap or {}).get(feature) or {}
            base = (fentry.get("baseline") or {}).get("items") or {}
            for v in base.values():
                wall.append(minimal(v))

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

    # -------------------- cascade tombstone removals
    def _cascade_tombstone_removals(self, *, feature: str, dry_run: bool = False) -> Dict[str, Any]:
        """
        End-of-run pass: for any pair-scoped tombstone (feature:PAIR|key) recorded this run,
        remove that key from all providers that still have it in their current baseline.
        """
        try:
            t = self.files.load_tomb().get("keys") or {}
        except Exception:
            t = {}

        feat_prefix = f"{feature}:"
        tomb_keys: set[str] = set()
        for k in t.keys():
            if not isinstance(k, str):
                continue
            if k.startswith(f"{feature}|"):
                tomb_keys.add(k.split("|", 1)[1])
            elif k.startswith(feat_prefix):
                tomb_keys.add(k.split("|", 1)[1])

        if not tomb_keys:
            return {"ok": True, "removed": 0, "providers": {}}

        state = self.files.load_state() or {}
        prov_block: Dict[str, Any] = state.get("providers") or {}

        # Plan removals per provider from their current baseline snapshot
        plan: Dict[str, List[Dict[str, Any]]] = {}
        for prov, fmap in prov_block.items():
            if prov not in self.providers:
                continue
            ops = self.providers[prov]
            if not ops.features().get(feature, False):
                continue
            base_items = (((fmap or {}).get(feature, {}) or {}).get("baseline", {}) or {}).get("items") or {}
            to_remove: List[Dict[str, Any]] = []
            for ck, item in list(base_items.items()):
                if ck in tomb_keys:
                    to_remove.append(minimal(item))
            if to_remove:
                plan[prov] = to_remove

        if not plan:
            return {"ok": True, "removed": 0, "providers": {}}

        self._emit("cascade:start", feature=feature, providers=len(plan), keys=len(tomb_keys))

        removed_total = 0
        per_provider_counts: Dict[str, int] = {}

        # Execute removals and update baselines in-memory
        for prov, items in plan.items():
            try:
                ops = self.providers[prov]
                self._emit("cascade:remove:start", provider=prov, feature=feature, count=len(items))
                if not dry_run and items:
                    res = self._retry(lambda: ops.remove(self.cfg, items, feature=feature, dry_run=False))
                    cnt = int((res or {}).get("count", 0))
                else:
                    cnt = len(items)
                per_provider_counts[prov] = cnt
                removed_total += cnt

                # Purge from baseline
                fmap = prov_block.get(prov, {})
                base_items = (((fmap or {}).get(feature, {}) or {}).get("baseline", {}) or {}).get("items") or {}
                for it in items:
                    base_items.pop(canonical_key(it), None)
            except Exception as e:
                self._emit_info(f"[!] cascade.remove_failed provider={prov} error={e}")

            self._emit("cascade:remove:done", provider=prov, feature=feature, count=per_provider_counts.get(prov, 0),
                       items=[minimal(x) for x in items][:50])

        # Save updated state after cascade
        try:
            state["providers"] = prov_block
            state["last_sync_epoch"] = int(time.time())
            self.files.save_state(state)
        except Exception as e:
            self._emit_info(f"[!] cascade.state_save_failed: {e}")

        return {"ok": True, "removed": removed_total, "providers": per_provider_counts}

    # -------------------- rate-limit warnings
    def _maybe_emit_rate_warnings(self):
        """Lightweight rate window sampling to warn the UI when near limits."""
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

    # -------------------- run one pair
    def run_pair(self, pair: Mapping[str, Any], *, dry_run: bool=False) -> Dict[str, Any]:
        """Runs a single pair in one-way or two-way mode."""
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

            # Set pair context so ratings helpers can read per-pair overrides.
            self._pair_ctx = pair
            try:
                if mode == "one-way":
                    self._emit_info(f"[1/1] {src} \u2192 {dst} | mode=one-way dry_run={dry_run}")
                    self._emit_info(f"    \u2022 feature={fname} removals={allow_removals}")
                    res = self.apply_direction(src=src, dst=dst, feature=fname, allow_removals=allow_removals, dry_run=dry_run)
                    out_summary["added"] += int(res.get("adds", 0))
                    out_summary["removed"] += int(res.get("removes", 0))
                    if res.get("ok"):
                        self._post_feature_success(fname)

                elif mode == "two-way":
                    self._emit_info(f"[1/1] {src} \u2192 {dst} | mode=two-way dry_run={dry_run}")
                    self._emit_info(f"    \u2022 feature={fname} removals={allow_removals}")
                    res2 = self._two_way_sync(
                        src, dst,
                        feature=fname,
                        allow_removals=allow_removals,
                        dry_run=dry_run,
                        include_observed_deletes=include_observed_deletes,
                        tomb_ttl_days=ttl_days,
                    )
                    out_summary["added"] += int((res2.get("resA_add") or {}).get("count", 0)) + int((res2.get("resB_add") or {}).get("count", 0))
                    out_summary["removed"] += int((res2.get("resA_remove") or {}).get("count", 0)) + int((res2.get("resB_remove") or {}).get("count", 0))
                    if res2.get("ok"):
                        self._post_feature_success(fname)
                    self._emit("two:done", a=src, b=dst, feature=fname,
                               res={"adds": out_summary["added"], "removes": out_summary["removed"]})
                else:
                    self._emit_info(f"[!] Unknown mode: {mode}")
            finally:
                # Always clear the pair context after a feature completes.
                self._pair_ctx = None

        return {"ok": True, **out_summary}

    # -------------------- run all pairs
    def run_pairs(
        self,
        *,
        dry_run: bool = False,
        progress: Optional[Callable[[str], None]] = None,
        write_state_json: bool = True,
        state_path: Optional[Path] = None,
        use_snapshot: bool = True,
        only_feature: Optional[str] = None,
        **_kwargs,
    ) -> Dict[str, Any]:
        """Execute all configured pairs. Optionally restrict to a single feature via only_feature."""

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

        added_total = 0
        removed_total = 0
        did_watchlist = False  # Track whether watchlist actually ran in any pair

        for i, pair in enumerate(pairs, 1):
            d = dict(pair)

            # Determine the feature for this pair.
            # Priority: explicit override -> pair.feature -> "multi"
            feat = (only_feature or d.get("feature") or "multi")
            feat = str(feat).lower().strip()
            d["feature"] = feat

            src = str(d.get("source") or "").upper()
            dst = str(d.get("target") or "").upper()

            # If feature is "multi", check whether watchlist would be included for post-run steps.
            if feat == "watchlist":
                did_watchlist = True
            elif feat == "multi":
                if (feat_map.get(src, {}).get("watchlist") and feat_map.get(dst, {}).get("watchlist")):
                    did_watchlist = True

            self._emit(
                "run:pair",
                i=i, n=len(pairs),
                src=src,
                dst=dst,
                mode=(d.get("mode") or "one-way").lower(),
                feature=feat,
                dry_run=bool(dry_run),
            )

            res = self.run_pair(d, dry_run=dry_run)
            added_total += int(res.get("added", 0))
            removed_total += int(res.get("removed", 0))

        # End-of-run cascade/state should only run when watchlist participated.
        if did_watchlist:
            try:
                self._emit("cascade:pre", note="running end-of-run tombstone cascade")
                cres = self._cascade_tombstone_removals(feature="watchlist", dry_run=dry_run)
                if self.debug:
                    self._emit("cascade:summary", removed=cres.get("removed", 0), providers=cres.get("providers", {}))
            except Exception as e:
                self._emit_info(f"[!] cascade.failed: {e}")

            if write_state_json:
                try:
                    state_obj = self._persist_state_wall(feature="watchlist")
                    if state_path and Path(state_path).resolve() != self.files.state.resolve():
                        tmp = Path(state_path).with_suffix(Path(state_path).suffix + ".tmp")
                        tmp.write_text(json.dumps(state_obj, indent=2), "utf-8")
                        tmp.replace(Path(state_path))
                except Exception as e:
                    self._emit_info(f"[!] state.persist_failed: {e}")

            # Clear hidefile once per run (post all pairs) — watchlist only.
            try:
                self.files.clear_watchlist_hide()
                self._dbg("hidefile.cleared", feature="watchlist", scope="end-of-run")
            except Exception:
                pass

        # Stats: final run summary, HTTP overview, rate warnings
        try:
            self.stats.record_summary(added=added_total, removed=removed_total)
            http24 = self.stats.http_overview(hours=24)
            self._emit("http:overview", window_hours=24, data=http24)
            self._maybe_emit_rate_warnings()
        except Exception:
            pass

        # Persist last-run stamp
        try:
            now = int(time.time())
            payload = {"started_at": now, "finished_at": now,
                    "result": {"added": added_total, "removed": removed_total}}
            self.files.save_last(payload)
        except Exception:
            pass

        self._emit("run:done", added=added_total, removed=removed_total, pairs=len(pairs))
        return {"ok": True, "added": added_total, "removed": removed_total, "pairs": len(pairs)}
