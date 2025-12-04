# Feature-agnostic phantom guard (compact, TTL-aware, blackbox-ready)
from __future__ import annotations
from pathlib import Path
from typing import Iterable, Mapping, Any, List, Set, Dict
import json, os, time

_DIR = "/config/.cw_state"

class PhantomGuard:
    def __init__(self, src: str, dst: str, feature: str, ttl_days: int | None = None, enabled: bool = True):
        base = f"{feature.lower()}.{src.lower()}-{dst.lower()}"
        self._pf = Path(_DIR) / f"{base}.phantoms.json"
        self._lf = Path(_DIR) / f"{base}.last_success.json"
        self._ttl = int(ttl_days) if ttl_days else None
        self._enabled = bool(enabled)

    # Utils
    def _now(self) -> int: return int(time.time())

    def _read_keys(self, p: Path) -> Set[str]:
        try:
            obj = json.loads(p.read_text("utf-8"))
            if isinstance(obj, list):
                return set(obj)
            if isinstance(obj, dict):
                if isinstance(obj.get("keys"), list):
                    return set(obj["keys"])

                cutoff = (self._now() - self._ttl * 86400) if self._ttl else None
                out = set()
                for k, ts in obj.items():
                    if cutoff is None or int(ts or 0) >= cutoff:
                        out.add(k)
                return out
        except Exception:
            pass
        return set()

    def _read_map(self, p: Path) -> Dict[str, int]:
        try:
            obj = json.loads(p.read_text("utf-8"))
            if isinstance(obj, dict) and not isinstance(obj.get("keys"), list):
                return {str(k): int(obj[k] or 0) for k in obj.keys()}
            if isinstance(obj, dict) and isinstance(obj.get("keys"), list):
                now = self._now()
                return {str(k): now for k in obj["keys"]}
            if isinstance(obj, list):
                now = self._now()
                return {str(k): now for k in obj}
        except Exception:
            pass
        return {}

    def _write_map(self, p: Path, m: Mapping[str, int]) -> None:
        try:
            p.parent.mkdir(parents=True, exist_ok=True)
            tmp = p.with_suffix(".tmp")
            tmp.write_text(json.dumps(m, ensure_ascii=False, indent=2), "utf-8")
            os.replace(tmp, p)
        except Exception:
            pass

    def _save_minimals(self, items: Iterable[Mapping[str, Any]], minimal) -> None:
        try:
            self._pf.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._pf.with_suffix(".tmp")
            tmp.write_text(json.dumps([minimal(it) for it in items], ensure_ascii=False, indent=2), "utf-8")
            os.replace(tmp, self._pf)
        except Exception:
            pass

    # API
    def filter_adds(self, adds: List[Mapping[str, Any]], keyfn, minimal, emit, state_store, pair_key: str):
        if not self._enabled or not adds:
            return adds, 0
        last_ok = self._read_keys(self._lf)
        ph_file = self._read_keys(self._pf)
        planned = [keyfn(it) for it in adds]
        phantoms = (set(planned) & last_ok) | ph_file
        if not phantoms:
            return adds, 0

        blocked = [it for it in adds if keyfn(it) in phantoms]
        keep    = [it for it in adds if keyfn(it) not in phantoms]
        self._save_minimals(blocked, minimal)
        try:
            for k in {keyfn(it) for it in blocked}:
                state_store.blackbox_put(pair_key, k, reason="phantom-replan")
        except Exception:
            pass
        emit("blocked.counts", feature="*", dst=pair_key.split("-")[-1], pair=pair_key,
             blocked_global_tomb=0, blocked_pair_tomb=0, blocked_unresolved=0,
             blocked_blackbox=len(blocked), blocked_total=len(blocked))
        return keep, len(blocked)

    def record_success(self, successful_keys: Iterable[str]):
        if not self._enabled:
            return
        cur = self._read_map(self._lf)
        now = self._now()
        for k in successful_keys or []:
            cur[str(k)] = now
        self._write_map(self._lf, cur)