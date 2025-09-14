# --------------- Global Tombstone Store (versioned, JSON on /config/tombstones.json) ---------------
from __future__ import annotations
import json, time, os, tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from .config_base import CONFIG as CONFIG_DIR  # resolves to /config
from . import id_map
from .gmt_policy import Scope, should_suppress_write, opposing

MODEL = "global"
VERSION = 2
DEFAULT_TTL_SEC = 7 * 24 * 3600

@dataclass
class TombstoneEntry:
    keys: List[str]
    scope: Dict[str, str]      # {"list": "...", "dim": "..."}
    origin: str                # "PLEX" | "SIMKL" | "TRAKT" | etc.
    ts_iso: str
    propagate_until_iso: str
    note: Optional[str] = None
    pair_ids: Optional[List[str]] = None

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

class GlobalTombstoneStore:
    """
    Versioned, atomic JSON store under /config/tombstones.json
    Back-compat: keeps a lightweight "keys" dict for tooling that expects it.
    """
    def __init__(self, ttl_sec: Optional[int] = None, file_path: Optional[Path] = None) -> None:
        self.base = Path(CONFIG_DIR)
        self.path = file_path or (self.base / "tombstones.json")
        self.ttl_sec = int(ttl_sec or DEFAULT_TTL_SEC)
        self._ensure_file()

    # ----- public API ---------------------------------------------------------
    def model_header(self) -> Dict[str, Any]:
        j = self._read()
        return {k: j.get(k) for k in ("model", "version", "ttl_sec", "updated_at")}

    def ensure_model(self) -> None:
        j = self._read()
        if j.get("model") != MODEL or int(j.get("version") or 0) < VERSION:
            # Fresh start per your instruction: drop legacy content
            self._write(self._empty_doc())

    def record_negative_event(self, *, entity: Mapping[str, Any], scope: Scope, origin: str, pair_id: Optional[str] = None, note: Optional[str] = None) -> None:
        """
        Create/refresh a negative tombstone (remove/unrate/unscrobble) so later opposing writes are suppressed.
        """
        if scope.dim not in ("remove", "unrate", "unscrobble"):
            return  # only negative events create tombstones

        keys = sorted(id_map.keys_for_item(entity))
        now = _utc_now_iso()
        until = datetime.now(timezone.utc) + timedelta(seconds=self.ttl_sec)
        rec = TombstoneEntry(
            keys=keys,
            scope={"list": scope.list, "dim": scope.dim},
            origin=str(origin or "").upper(),
            ts_iso=now,
            propagate_until_iso=until.strftime("%Y-%m-%dT%H:%M:%SZ"),
            note=note,
            pair_ids=[pair_id] if pair_id else None,
        )
        self._upsert_entry(rec)

    def should_suppress(self, *, entity: Mapping[str, Any], scope: Scope, write_op: str) -> bool:
        """
        True if there is an active tombstone for the same list where the tombstone dim opposes write_op.
        """
        if not entity:
            return False
        active = self._active_entries_for_entity(entity, scope)
        for rec in active:
            if should_suppress_write(write_op, rec["scope"]["dim"]):
                return True
        return False

    def purge_expired(self) -> int:
        j = self._read()
        entries = j.get("entries") or []
        now = datetime.now(timezone.utc)
        keep: List[dict] = []
        removed = 0
        for rec in entries:
            try:
                until = datetime.strptime(rec.get("propagate_until_iso",""), "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            except Exception:
                # remove broken records
                removed += 1
                continue
            if until > now:
                keep.append(rec)
            else:
                removed += 1
        j["entries"] = keep
        j["updated_at"] = _utc_now_iso()
        # refresh back-compat "keys" view
        j["keys"] = self._flatten_keys_view(keep)
        self._write(j)
        return removed

    # ----- private helpers ----------------------------------------------------
    def _empty_doc(self) -> Dict[str, Any]:
        return {
            "model": MODEL,
            "version": VERSION,
            "ttl_sec": self.ttl_sec,
            "updated_at": _utc_now_iso(),
            "entries": [],
            # Back-compat for admin tools that expect a flat map:
            "keys": {},           # computed from entries; do not hand-edit
            "pruned_at": None,    # kept for older UIs
        }

    def _ensure_file(self) -> None:
        if not self.path.exists():
            self._write(self._empty_doc())
            return
        # If existing but legacy, force-reinit:
        try:
            j = self._read()
            if j.get("model") != MODEL or int(j.get("version") or 0) < VERSION:
                self._write(self._empty_doc())
        except Exception:
            self._write(self._empty_doc())

    def _read(self) -> Dict[str, Any]:
        try:
            return json.loads(self.path.read_text("utf-8"))
        except Exception:
            return self._empty_doc()

    def _write(self, data: Mapping[str, Any]) -> None:
        # atomic write compatible with volumes
        tmp = Path(tempfile.gettempdir()) / f".tomb.{os.getpid()}.{int(time.time()*1000)}.json"
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), "utf-8")
        tmp.replace(self.path)

    def _upsert_entry(self, rec: TombstoneEntry) -> None:
        j = self._read()
        # de-dup by (scope.list, scope.dim, key) with newest ts taking precedence
        entries: List[dict] = list(j.get("entries") or [])
        scope_list = rec.scope["list"]; scope_dim = rec.scope["dim"]
        keys_set = set(rec.keys or [])
        out: List[dict] = []
        for old in entries:
            if (old.get("scope", {}).get("list") == scope_list and
                old.get("scope", {}).get("dim")  == scope_dim and
                set(old.get("keys") or []).intersection(keys_set)):
                # drop older overlapping record
                continue
            out.append(old)
        out.append({
            "keys": rec.keys,
            "scope": rec.scope,
            "origin": rec.origin,
            "ts_iso": rec.ts_iso,
            "propagate_until_iso": rec.propagate_until_iso,
            "note": rec.note,
            "pair_ids": rec.pair_ids,
        })
        j["entries"] = out
        j["updated_at"] = _utc_now_iso()
        j["keys"] = self._flatten_keys_view(out)  # keep flat map in sync
        self._write(j)

    def _active_entries_for_entity(self, entity: Mapping[str, Any], scope: Scope) -> List[dict]:
        j = self._read()
        entries = j.get("entries") or []
        now = datetime.now(timezone.utc)
        e_keys = id_map.keys_for_item(entity)
        active: List[dict] = []
        for rec in entries:
            try:
                if rec.get("scope", {}).get("list") != scope.list:
                    continue
                until = datetime.strptime(rec.get("propagate_until_iso",""), "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                if until <= now:
                    continue
                if id_map.any_key_overlap(e_keys, rec.get("keys") or []):
                    active.append(rec)
            except Exception:
                continue
        return active

    def _flatten_keys_view(self, entries: Iterable[Mapping[str, Any]]) -> Dict[str, int]:
        """
        Back-compat map for admin tools: {"<feature>|<key>": epoch}
        We use the record ts for value; scoped per list.
        """
        flat: Dict[str, int] = {}
        for rec in entries or []:
            try:
                feat = str(rec.get("scope", {}).get("list") or "").lower()
                ts_iso = str(rec.get("ts_iso") or "")
                epoch = int(datetime.strptime(ts_iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp()) if ts_iso else int(time.time())
                for k in rec.get("keys") or []:
                    flat[f"{feat}|{k}"] = epoch
            except Exception:
                continue
        return flat
