# /cw_platform/anime_mapping/cw_mappings.py
# CrossWatch - CrossWatch Anime Mapping Corrections
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .coordinates import parse_source, parse_target
from .descriptors import parse_descriptor

SUPPORTED_SCHEMA = 1
FILE_NAME = "cw_mappings.json"
BUNDLED_PATH = Path(__file__).resolve().with_name(FILE_NAME)
REPO_PATH = f"cw_platform/anime_mapping/{FILE_NAME}"
ORIGIN = "crosswatch"
MAX_BYTES = 2 * 1024 * 1024
_REVISION_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})\.(\d{1,4})$")
_MAX_ENTRIES = 2000
_MAX_EDGES_PER_ENTRY = 200


class CwMappingsError(ValueError):
    pass


@dataclass(frozen=True)
class ActivePatch:
    source: str
    revision: str
    sha256: str
    entries: tuple[dict[str, Any], ...]
    error: str = ""


@dataclass
class PatchPlan:
    removed: set[tuple[str, str]] = field(default_factory=set)
    added: list[tuple[str, str, dict[str, str], str]] = field(default_factory=list)
    report: list[dict[str, str]] = field(default_factory=list)

    @property
    def redundant(self) -> list[str]:
        return [row["id"] for row in self.report if row["status"] == "redundant"]

    @property
    def applied(self) -> list[str]:
        return [row["id"] for row in self.report if row["status"] == "applied"]


def _descriptor(value: Any, where: str) -> str:
    text = str(value or "").strip()
    if not text or parse_descriptor(text) is None:
        raise CwMappingsError(f"{where}: invalid descriptor {text!r}")
    return text


def _ranges(value: Any, where: str) -> dict[str, str]:
    if not isinstance(value, Mapping) or not value:
        raise CwMappingsError(f"{where}: ranges are required")
    out: dict[str, str] = {}
    for src, dst in value.items():
        s, d = str(src or "").strip(), str(dst or "").strip()
        if parse_source(s) is None or parse_target(d) is None:
            raise CwMappingsError(f"{where}: invalid range {s!r} -> {d!r}")
        out[s] = d
    return out


def _evidence(raw: Any, where: str) -> dict[str, Any]:
    if raw in (None, {}):
        return {}
    if not isinstance(raw, Mapping):
        raise CwMappingsError(f"{where}: evidence must be an object")
    checked = str(raw.get("checked") or "").strip()
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", checked):
        raise CwMappingsError(f"{where}: evidence.checked must look like YYYY-MM-DD")
    out: dict[str, Any] = {"checked": checked}
    tmdb = raw.get("tmdb")
    if tmdb is not None:
        if not isinstance(tmdb, Mapping):
            raise CwMappingsError(f"{where}: evidence.tmdb must be an object")
        show = str(tmdb.get("show") or "").strip()
        seasons = tmdb.get("seasons")
        if not show.isdigit() or not isinstance(seasons, Mapping) or not seasons:
            raise CwMappingsError(f"{where}: evidence.tmdb needs a show id and seasons")
        clean: dict[str, int] = {}
        for season, count in seasons.items():
            s = str(season).strip()
            if not s.isdigit() or not isinstance(count, int) or isinstance(count, bool) or count < 0:
                raise CwMappingsError(f"{where}: evidence.tmdb.seasons must map season numbers to episode counts")
            clean[s] = count
        out["tmdb"] = {"show": show, "seasons": clean}
    sources = raw.get("sources")
    if sources is not None:
        if not isinstance(sources, list) or not all(isinstance(s, str) and s.strip() for s in sources):
            raise CwMappingsError(f"{where}: evidence.sources must be a list of names")
        out["sources"] = [s.strip()[:40] for s in sources][:10]
    return out


def _entry(raw: Any, index: int) -> dict[str, Any]:
    if not isinstance(raw, Mapping):
        raise CwMappingsError(f"entry {index}: must be an object")
    entry_id = str(raw.get("id") or "").strip()
    if not entry_id:
        raise CwMappingsError(f"entry {index}: id is required")
    where = f"entry {entry_id}"
    removes = raw.get("remove") or []
    adds = raw.get("add") or []
    if not isinstance(removes, list) or not isinstance(adds, list):
        raise CwMappingsError(f"{where}: remove and add must be lists")
    if not removes and not adds:
        raise CwMappingsError(f"{where}: nothing to change")
    if len(removes) + len(adds) > _MAX_EDGES_PER_ENTRY:
        raise CwMappingsError(f"{where}: too many edges")
    out_remove = []
    for row in removes:
        if not isinstance(row, Mapping):
            raise CwMappingsError(f"{where}: remove rows must be objects")
        src, dst = _descriptor(row.get("source"), where), _descriptor(row.get("target"), where)
        if src == dst:
            raise CwMappingsError(f"{where}: an edge cannot point to itself")
        out_remove.append({"source": src, "target": dst})
    out_add = []
    for row in adds:
        if not isinstance(row, Mapping):
            raise CwMappingsError(f"{where}: add rows must be objects")
        src, dst = _descriptor(row.get("source"), where), _descriptor(row.get("target"), where)
        if src == dst:
            raise CwMappingsError(f"{where}: an edge cannot point to itself")
        out_add.append({"source": src, "target": dst, "ranges": _ranges(row.get("ranges"), where)})
    return {
        "id": entry_id,
        "title": str(raw.get("title") or "").strip()[:200],
        "reason": str(raw.get("reason") or "").strip()[:2000],
        "upstream": str(raw.get("upstream") or "").strip()[:500],
        "added": str(raw.get("added") or "").strip()[:32],
        "evidence": _evidence(raw.get("evidence"), where),
        "remove": out_remove,
        "add": out_add,
    }


def revision_key(revision: str) -> tuple[str, int]:
    m = _REVISION_RE.fullmatch(str(revision or ""))
    return (m.group(1), int(m.group(2))) if m else ("", -1)


def parse_payload(raw: bytes) -> tuple[str, tuple[dict[str, Any], ...]]:
    if len(raw) > MAX_BYTES:
        raise CwMappingsError("file is too large")
    try:
        data = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        raise CwMappingsError("file is not valid JSON") from exc
    if not isinstance(data, Mapping):
        raise CwMappingsError("file must contain a JSON object")
    try:
        schema = int(data.get("schema_version") or 0)
    except (TypeError, ValueError) as exc:
        raise CwMappingsError("schema_version must be a number") from exc
    if schema != SUPPORTED_SCHEMA:
        raise CwMappingsError(f"unsupported schema_version {schema}")
    revision = str(data.get("revision") or "").strip()
    if not _REVISION_RE.fullmatch(revision):
        raise CwMappingsError("revision must look like YYYY-MM-DD.N")
    entries = data.get("entries")
    if not isinstance(entries, list):
        raise CwMappingsError("entries must be a list")
    if len(entries) > _MAX_ENTRIES:
        raise CwMappingsError("too many entries")
    parsed = tuple(_entry(row, i) for i, row in enumerate(entries))
    ids = [row["id"] for row in parsed]
    if len(ids) != len(set(ids)):
        raise CwMappingsError("entry ids must be unique")
    return revision, parsed


def _read(path: Path, source: str) -> ActivePatch:
    raw = path.read_bytes()
    revision, entries = parse_payload(raw)
    return ActivePatch(source=source, revision=revision, sha256=hashlib.sha256(raw).hexdigest(), entries=entries)


def downloaded_path(root: Path) -> Path:
    return Path(root) / FILE_NAME


def load_active(root: Path | None = None) -> ActivePatch:
    errors: list[str] = []
    downloaded: ActivePatch | None = None
    bundled: ActivePatch | None = None
    if root is not None and downloaded_path(root).exists():
        try:
            downloaded = _read(downloaded_path(root), "downloaded")
        except (OSError, CwMappingsError) as exc:
            errors.append(f"downloaded copy rejected: {exc}")
    try:
        bundled = _read(BUNDLED_PATH, "bundled")
    except (OSError, CwMappingsError) as exc:
        errors.append(f"bundled copy rejected: {exc}")
    candidates = [p for p in (bundled, downloaded) if p is not None]
    if not candidates:
        return ActivePatch(source="none", revision="", sha256="", entries=(), error="; ".join(errors))
    best = max(candidates, key=lambda p: revision_key(p.revision))
    if not errors:
        return best
    return ActivePatch(source=best.source, revision=best.revision, sha256=best.sha256, entries=best.entries, error="; ".join(errors))


def _invert(ranges: Mapping[str, str]) -> dict[str, str]:
    return {str(v): str(k) for k, v in ranges.items()}


def _pair_ranges(data: Mapping[str, Any], src: str, dst: str) -> Any:
    targets = data.get(src)
    return targets.get(dst) if isinstance(targets, Mapping) else None


def _pair_present(data: Mapping[str, Any], src: str, dst: str) -> bool:
    return _pair_ranges(data, src, dst) is not None or _pair_ranges(data, dst, src) is not None


def _pair_matches(data: Mapping[str, Any], src: str, dst: str, ranges: Mapping[str, str]) -> bool:
    forward = _pair_ranges(data, src, dst)
    backward = _pair_ranges(data, dst, src)
    if forward is None and backward is None:
        return False
    if forward is not None and dict(forward) != dict(ranges):
        return False
    if backward is not None and dict(backward) != _invert(ranges):
        return False
    return True


def plan(data: Mapping[str, Any], patch: ActivePatch) -> PatchPlan:
    out = PatchPlan()
    for entry in patch.entries:
        removed: set[tuple[str, str]] = set()
        added: list[tuple[str, str, dict[str, str], str]] = []
        changes = False
        for row in entry["remove"]:
            src, dst = row["source"], row["target"]
            if _pair_present(data, src, dst):
                changes = True
            removed.update({(src, dst), (dst, src)})
        for row in entry["add"]:
            src, dst, ranges = row["source"], row["target"], row["ranges"]
            if not _pair_matches(data, src, dst, ranges):
                changes = True
            removed.update({(src, dst), (dst, src)})
            added.append((src, dst, dict(ranges), entry["id"]))
        if changes:
            out.removed.update(removed)
            out.added.extend(added)
        out.report.append({"id": entry["id"], "status": "applied" if changes else "redundant"})
    return out
