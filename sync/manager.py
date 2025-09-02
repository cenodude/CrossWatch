from __future__ import annotations
import importlib, pkgutil, json, time
from pathlib import Path
from typing import Any, Dict, Mapping
from _logging import log

# Discover auth providers to piggy-back capabilities
def _discover_auth() -> dict:
    import auth
    out = {}
    for m in pkgutil.iter_modules([str(Path(auth.__file__).parent)]):
        if not m.name.startswith("_auth_"):
            continue
        mod = importlib.import_module(f"auth.{m.name}")
        prov = getattr(mod, "PROVIDER", None)
        if prov and getattr(prov, "name", None):
            out[prov.name.upper()] = prov
    return out

def _merge_constraints(src_caps: dict, dst_caps: dict) -> dict:
    # Keep it simple; you can expand later
    notes = {}
    for k in ("ratings.scale",):
        s = src_caps.get("features", {}).get("ratings", {}).get("scale")
        d = dst_caps.get("features", {}).get("ratings", {}).get("scale")
        if s and d and s != d:
            notes["ratings.scale_mismatch"] = f"{s} vs {d}"
    return notes

def compute_sync_options(src_caps: dict, dst_caps: dict, *, direction: str = "mirror") -> dict:
    out = {"watchlist": False, "collections": False, "ratings": False, "watched": False, "liked_lists": False}
    for feat in list(out.keys()):
        s = src_caps.get("features", {}).get(feat, {})
        d = dst_caps.get("features", {}).get(feat, {})
        if direction == "mirror":
            out[feat] = bool(s.get("read") and d.get("write"))
        else:  # two-way
            out[feat] = bool(s.get("read") and s.get("write") and d.get("read") and d.get("write"))
    out["entity_types"] = sorted(set(src_caps.get("entity_types", [])) & set(dst_caps.get("entity_types", [])))
    out["notes"] = _merge_constraints(src_caps, dst_caps)
    return out

class SyncManager:
    """Produces options, validates profiles, persists them."""
    def __init__(self, load_cfg, save_cfg, storage: Path):
        self.load_cfg = load_cfg
        self.save_cfg = save_cfg
        self.storage = storage
        self.storage.parent.mkdir(parents=True, exist_ok=True)
        self.auth = _discover_auth()

    def providers(self) -> dict[str, dict]:
        data = {}
        for name, prov in self.auth.items():
            man = prov.manifest()
            caps = getattr(prov, "capabilities", lambda: {"features": {}, "entity_types": []})()
            data[name] = {"manifest": man.__dict__, "capabilities": caps}
        return data

    def options(self, source: str, target: str, direction: str = "mirror") -> dict:
        s = self.auth[source.upper()].capabilities()
        t = self.auth[target.upper()].capabilities()
        return compute_sync_options(s, t, direction=direction)

    def _read_profiles(self) -> list[dict]:
        if not self.storage.exists():
            return []
        try:
            return json.loads(self.storage.read_text(encoding="utf-8"))
        except Exception:
            return []

    def _write_profiles(self, rows: list[dict]) -> None:
        tmp = self.storage.with_suffix(".tmp")
        tmp.write_text(json.dumps(rows, indent=2), encoding="utf-8")
        tmp.replace(self.storage)

    def validate_profile(self, profile: dict) -> dict:
        # normalize
        prof = dict(profile)
        prof["direction"] = str(prof.get("direction", "mirror")).lower()
        src = prof["source"].upper(); dst = prof["target"].upper()
        caps = self.options(src, dst, direction=prof["direction"])
        # check features
        feats = prof.get("features", {})
        for k, v in feats.items():
            if v and not caps.get(k, False):
                raise ValueError(f"Feature '{k}' not supported for {src} → {dst} ({prof['direction']})")
        # restrict entity types if needed (placeholder; extend later)
        return prof

    def list_profiles(self) -> list[dict]:
        return self._read_profiles()

    def upsert_profile(self, profile: dict) -> dict:
        prof = self.validate_profile(profile)
        rows = self._read_profiles()
        key = f"{prof['source']}→{prof['target']}"
        prof["id"] = prof.get("id") or key
        prof["updated_at"] = int(time.time())
        # upsert
        found = False
        for i, r in enumerate(rows):
            if r.get("id") == prof["id"]:
                rows[i] = prof; found = True; break
        if not found:
            rows.append(prof)
        self._write_profiles(rows)
        log("Sync profile saved", level="SUCCESS", module="SYNC", extra={"id": prof["id"]})
        return prof
