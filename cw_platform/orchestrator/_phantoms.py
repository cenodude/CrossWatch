# cw_platform/orchestrator/_phantoms.py
# phantom item management for orchestrator.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations
from pathlib import Path
from collections.abc import Iterable, Mapping, Sequence
from typing import Any, TypeVar
import json, os, time
import shutil

from ._scope import scope_safe

_DIR = "/config/.cw_state"
T = TypeVar("T", bound=Mapping[str, Any])

class PhantomGuard:
    def __init__(self, src: str, dst: str, feature: str, ttl_days: int | None = None, enabled: bool = True):
        legacy_base = f"{feature.lower()}.{src.lower()}-{dst.lower()}"
        scope = scope_safe()
        base = f"{legacy_base}.{scope}"
        self._pf = Path(_DIR) / f"{base}.phantoms.json"
        self._lf = Path(_DIR) / f"{base}.last_success.json"
        legacy_pf = Path(_DIR) / f"{legacy_base}.phantoms.json"
        legacy_lf = Path(_DIR) / f"{legacy_base}.last_success.json"
        if not self._pf.exists() and legacy_pf.exists():
            try:
                Path(_DIR).mkdir(parents=True, exist_ok=True)
                shutil.copy2(legacy_pf, self._pf)
            except Exception:
                pass
        if not self._lf.exists() and legacy_lf.exists():
            try:
                Path(_DIR).mkdir(parents=True, exist_ok=True)
                shutil.copy2(legacy_lf, self._lf)
            except Exception:
                pass
        self._ttl = int(ttl_days) if ttl_days else None
        self._enabled = bool(enabled)
    def _now(self) -> int: return int(time.time())

    def _read_keys(self, p: Path) -> set[str]:
        try:
            obj = json.loads(p.read_text("utf-8"))
            if isinstance(obj, list):
                return set(obj)
            if isinstance(obj, dict):
                if isinstance(obj.get("keys"), list):
                    return set(obj["keys"])
                cutoff = (self._now() - self._ttl * 86400) if self._ttl else None
                out: set[str] = set()
                for k, ts in obj.items():
                    if cutoff is None or int(ts or 0) >= cutoff:
                        out.add(k)
                return out
        except Exception:
            pass
        return set()

    def _read_map(self, p: Path) -> dict[str, int]:
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
    def filter_adds(
        self,
        adds: Sequence[T],
        keyfn,
        minimal,
        emit,
        state_store,
        pair_key: str,
    ) -> tuple[list[T], int]:
        if not self._enabled or not adds:
            return list(adds), 0
        last_ok = self._read_keys(self._lf)
        ph_file = self._read_keys(self._pf)
        planned = [keyfn(it) for it in adds]
        phantoms = (set(planned) & last_ok) | ph_file
        if not phantoms:
            return list(adds), 0
        blocked: list[T] = [it for it in adds if keyfn(it) in phantoms]
        keep: list[T] = [it for it in adds if keyfn(it) not in phantoms]
        self._save_minimals(blocked, minimal)
        try:
            for k in {keyfn(it) for it in blocked}:
                state_store.blackbox_put(pair_key, k, reason="phantom-replan")
        except Exception:
            pass
        emit(
            "blocked.counts",
            feature="*",
            dst=pair_key.split("-")[-1],
            pair=pair_key,
            blocked_global_tomb=0,
            blocked_pair_tomb=0,
            blocked_unresolved=0,
            blocked_blackbox=len(blocked),
            blocked_total=len(blocked),
        )
        return keep, len(blocked)

    def record_success(self, successful_keys: Iterable[str]) -> None:
        if not self._enabled:
            return
        cur = self._read_map(self._lf)
        now = self._now()
        for k in successful_keys or []:
            cur[str(k)] = now
        self._write_map(self._lf, cur)
        