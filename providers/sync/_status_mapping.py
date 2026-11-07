# providers/sync/_status_mapping.py
# CrossWatch - Native status adapter contract and capability registry.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import hashlib
import importlib
import json
from dataclasses import dataclass
from typing import Any

from cw_platform.status_mapping import Capabilities, Observation, ReadState, Status


ADAPTERS = ("crosswatch", "simkl", "anilist", "myanimelist", "kitsu", "punchplay", "wetrakr", "floppy", "trakt", "mdblist", "publicmetadb", "scrob")
LABELS = {"watching": "Watching", "on_hold": "On hold", "dropped": "Dropped", "completed": "Completed"}
ORDER = tuple(LABELS)


def digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


def complete_fields(value: Any, schema: dict[str, tuple[type, ...]]) -> bool:
    return isinstance(value, dict) and all(key in value and type(value[key]) in kinds for key, kinds in schema.items())


@dataclass
class Record:
    native_id: str
    media: str
    item: dict[str, Any]
    observation: Observation
    raw: dict[str, Any]


def observation(native: str, statuses: dict[str, Status], protected: Any, *, watchlisted: bool, rewatching: bool = False) -> Observation:
    status = statuses.get(native)
    if status is None:
        return Observation(ReadState.UNKNOWN)
    return Observation(ReadState.PRESENT, status, native, digest(protected), watchlisted, rewatching)


def adapter_class(provider: str):
    key = str(provider).lower()
    if key not in ADAPTERS:
        return None
    return importlib.import_module(f"providers.sync.{key}._status").StatusAdapter


def provider_caps(provider: str) -> dict[str, Any]:
    cls = adapter_class(provider)
    if cls is None:
        return {}
    return {
        "opt_in": "history.use_source_status", "labels": LABELS,
        "media": {media: {
            "readable": [s for s in ORDER if Status(s) in caps.readable],
            "writable": [s for s in ORDER if Status(s) in caps.writable],
            "defaults": {s: {"include": s in {"on_hold", "dropped"} and Status(s) in caps.writable, "target": s}
                         for s in ORDER if Status(s) in caps.readable and Status(s) in caps.writable},
            "effects": cls.effects,
        } for media, caps in cls.capabilities.items()},
    }


class NativeStatusAdapter:
    capabilities: dict[str, Capabilities] = {}
    effects: dict[str, str] = {}
    creates_missing = False

    def __init__(self, cfg: dict[str, Any]):
        self.cfg = cfg

    def read(self) -> dict[str, Record]:
        raise NotImplementedError

    def read_record(self, reference: Record) -> Record | None:
        snapshot = self.read()
        return snapshot.get(reference.native_id) or self.missing_record(reference, snapshot)

    def write(self, record: Record, desired: Status) -> None:
        raise NotImplementedError

    def missing_record(self, source: Record, snapshot: dict[str, Record]) -> Record | None:
        return None

    def blockers(self, record: Record, desired: Status) -> tuple[str, ...]:
        if record.observation.rewatching:
            return ("Rewatching entries are preserved.",)
        if desired == Status.COMPLETED:
            return ("Completed can create watched history or change dates.",)
        return ()


ANIME_CAPS = {media: Capabilities(frozenset(Status), frozenset({Status.WATCHING, Status.ON_HOLD, Status.DROPPED}))
              for media in ("movies", "shows")}
