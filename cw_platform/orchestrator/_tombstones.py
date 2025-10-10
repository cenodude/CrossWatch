from __future__ import annotations
from typing import Dict, Iterable, Mapping, Optional, Sequence, List, Any
from ..id_map import canonical_key, ID_KEYS, minimal
from ._state_store import StateStore
def pair_key(a: str, b: str) -> str:
    return "-".join(sorted([a.upper(), b.upper()]))
def add_global_keys(store: StateStore, dbg, keys: Iterable[str]) -> int:
    t = store.load_tomb(); ks = t.setdefault("keys", {}); now = int(__import__("time").time()); added = 0
    for k in keys:
        if k not in ks: ks[k] = now; added += 1
    store.save_tomb(t); dbg("tombstones.marked", added=added); return added
def add_keys_for_feature(store: StateStore, dbg, feature: str, keys: Iterable[str], *, pair: Optional[str]=None) -> int:
    t = store.load_tomb(); ks = t.setdefault("keys", {}); now = int(__import__("time").time()); added = 0
    prefixes = [feature]
    if pair: prefixes.append(f"{feature}:{pair}")
    for k in keys:
        for pref in prefixes:
            nk = f"{pref}|{k}"
            if nk not in ks: ks[nk] = now; added += 1
    store.save_tomb(t); dbg("tombstones.marked", feature=feature, added=added, scope="global+pair" if pair else "global"); return added
def keys_for_feature(store: StateStore, feature: str, *, pair: Optional[str]=None, include_global: bool=True) -> Dict[str, int]:
    ks_all = dict((store.load_tomb().get("keys") or {})); out: Dict[str, int] = {}
    def _collect(prefix: str):
        plen = len(prefix) + 1
        for k, ts in ks_all.items():
            if isinstance(k, str) and k.startswith(prefix + "|"):
                orig = k[plen:]; out[orig] = int(ts)
    if include_global: _collect(feature)
    if pair: _collect(f"{feature}:{pair}")
    return out
def prune(store: StateStore, dbg, *, older_than_secs: int) -> int:
    t = store.load_tomb(); ks = t.get("keys", {})
    if not ks: return 0
    now = int(__import__("time").time())
    keep = {k:v for k,v in ks.items() if (now - int(v)) < older_than_secs}
    removed = len(ks) - len(keep)
    t["keys"] = keep; t["pruned_at"] = now; store.save_tomb(t)
    dbg("tombstones.pruned", removed=removed, kept=len(keep)); return removed
def filter_with(store: StateStore, items: Sequence[Mapping[str, Any]], extra_block: Optional[set[str]] = None) -> List[Mapping[str, Any]]:
    raw = (store.load_tomb().get("keys") or {}).keys(); base_keys = set()
    for k in raw:
        if isinstance(k, str): base_keys.add(k.split("|", 1)[-1])
    if extra_block: base_keys |= set(extra_block)
    def _hit(keys: set[str], item: Mapping[str, Any]) -> bool:
        ck = canonical_key(item)
        if ck in keys: return True
        ids = (item.get("ids") or {})
        for k in ID_KEYS:
            v = ids.get(k)
            if v is not None and f"{k}:{str(v).lower()}" in keys: return True
        t = (item.get("type") or "").lower(); ttl = str(item.get("title") or "").strip().lower(); yr = item.get("year") or ""
        return f"{t}|title:{ttl}|year:{yr}" in keys
    out = [it for it in items if not _hit(base_keys, it)]
    return out
def cascade_removals(store: StateStore, dbg, *, feature: str, removed_keys: Iterable[str]) -> Dict[str, int]:
    added = add_keys_for_feature(store, dbg, feature, removed_keys)
    return {"tombstones_added": added}
