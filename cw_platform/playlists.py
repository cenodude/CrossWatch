# cw_platform/playlists.py
# CrossWatch - Playlist domain model and provider contract
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Protocol, Sequence, runtime_checkable

from .id_map import canonical_key, minimal

PLAYLIST_KIND_REGULAR = "regular"
PLAYLIST_KIND_SMART = "smart"
RULESET_SCHEMA_VERSION = 1
BUILTIN_TRAKT_FREE_ACCOUNT_RULESET_ID = "trakt_free_account"

RULESET_ENUMS: dict[str, set[str]] = {
    "direction": {"one_way", "bidirectional"},
    "initial_sync": {"source_authoritative"},
    "read_mode": {"direct", "aggregate"},
    "write_mode": {"direct", "partition"},
    "membership": {"add_only", "managed_only", "mirror"},
    "order": {"ignore", "preserve"},
    "deduplicate": {"canonical_id"},
    "allocation": {"stable_first_fit"},
    "rebalance": {"never"},
    "overflow": {"block"},
}

RULESET_NUMERIC_FIELDS = {
    "per_endpoint_capacity": (1, 100000),
    "aggregate_capacity": (1, 1000000),
    "maximum_targets": (1, 50),
}

RULESET_BOOL_FIELDS = {"built_in", "track_assignments"}
RULESET_NAME_MAX = 10
_SAFE_NAME_CHARS = " _.'-&()"

BUILTIN_RULESETS: dict[str, dict[str, Any]] = {
    BUILTIN_TRAKT_FREE_ACCOUNT_RULESET_ID: {
        "id": BUILTIN_TRAKT_FREE_ACCOUNT_RULESET_ID,
        "name": "TraktFree",
        "schema_version": RULESET_SCHEMA_VERSION,
        "built_in": True,
        "direction": "bidirectional",
        "initial_sync": "source_authoritative",
        "read_mode": "aggregate",
        "write_mode": "partition",
        "membership": "managed_only",
        "order": "ignore",
        "deduplicate": "canonical_id",
        "allocation": "stable_first_fit",
        "rebalance": "never",
        "overflow": "block",
        "per_endpoint_capacity": 250,
        "aggregate_capacity": 1000,
        "maximum_targets": 5,
        "track_assignments": True,
    }
}

_PLAYLIST_METHODS = (
    "list_playlist_resources",
    "get_playlist_snapshot",
    "create_playlist",
    "add_playlist_items",
    "remove_playlist_items",
    "reorder_playlist_items",
)


def _clean_str(v: Any) -> str:
    return str(v).strip() if v is not None else ""


def _safe_name_error(name: Any, label: str, max_len: int) -> str:
    s = _clean_str(name)
    if not s:
        return f"{label} required"
    if len(s) > max_len:
        return f"{label} must be {max_len} characters or fewer"
    if not s[0].isalnum():
        return f"{label} must start with a letter or number"
    if any(not (ch.isalnum() or ch in _SAFE_NAME_CHARS) for ch in s):
        return f"{label} contains unsupported characters"
    return ""


def _norm_kind(v: Any) -> str:
    k = _clean_str(v).lower()
    return PLAYLIST_KIND_SMART if k == PLAYLIST_KIND_SMART else PLAYLIST_KIND_REGULAR


def _norm_media_types(v: Any) -> tuple[str, ...]:
    if v is None:
        return ()
    if isinstance(v, str):
        raw: Sequence[Any] = [x for x in v.replace(",", " ").split() if x]
    elif isinstance(v, (list, tuple, set)):
        raw = list(v)
    else:
        return ()
    out: list[str] = []
    for x in raw:
        s = _clean_str(x).lower()
        if s and s not in out:
            out.append(s)
    return tuple(out)


def _ruleset_int(v: Any, *, default: int, bounds: tuple[int, int]) -> int:
    try:
        n = int(v)
    except Exception:
        n = int(default)
    lo, hi = bounds
    if n < lo:
        return lo
    if n > hi:
        return hi
    return n


def normalize_ruleset(data: Mapping[str, Any], *, built_in: bool | None = None) -> dict[str, Any]:
    src = dict(data or {})
    rid = _clean_str(src.get("id"))
    name = _clean_str(src.get("name"))
    out: dict[str, Any] = {
        "id": rid,
        "name": name or rid,
        "description": _clean_str(src.get("description")),
        "schema_version": RULESET_SCHEMA_VERSION,
        "built_in": bool(src.get("built_in", False) if built_in is None else built_in),
    }
    for key, allowed in RULESET_ENUMS.items():
        val = _clean_str(src.get(key)).lower()
        if val not in allowed:
            val = BUILTIN_RULESETS[BUILTIN_TRAKT_FREE_ACCOUNT_RULESET_ID][key]
        out[key] = val
    for key, bounds in RULESET_NUMERIC_FIELDS.items():
        default = BUILTIN_RULESETS[BUILTIN_TRAKT_FREE_ACCOUNT_RULESET_ID][key]
        out[key] = _ruleset_int(src.get(key), default=default, bounds=bounds)
    out["track_assignments"] = bool(src.get("track_assignments", True))
    return out


def validate_ruleset(data: Mapping[str, Any], *, require_id: bool = True) -> tuple[bool, str, dict[str, Any] | None]:
    raw = dict(data or {})
    if require_id and not _clean_str(raw.get("id")):
        return False, "ruleset id required", None
    name_err = _safe_name_error(raw.get("name"), "ruleset name", RULESET_NAME_MAX)
    if name_err:
        return False, name_err, None
    if int(raw.get("schema_version") or RULESET_SCHEMA_VERSION) != RULESET_SCHEMA_VERSION:
        return False, "unsupported ruleset schema version", None
    for key, allowed in RULESET_ENUMS.items():
        val = _clean_str(raw.get(key)).lower()
        if val not in allowed:
            return False, f"invalid {key}", None
    for key, bounds in RULESET_NUMERIC_FIELDS.items():
        raw_value = raw.get(key)
        if raw_value is None:
            return False, f"invalid {key}", None
        try:
            n = int(raw_value)
        except Exception:
            return False, f"invalid {key}", None
        lo, hi = bounds
        if n < lo or n > hi:
            return False, f"{key} out of range", None
    return True, "", normalize_ruleset(raw)


def builtin_rulesets() -> list[dict[str, Any]]:
    return [normalize_ruleset(v, built_in=True) for v in BUILTIN_RULESETS.values()]


@dataclass
class PlaylistResource:
    provider: str
    id: str
    name: str = ""
    instance: str = "default"
    kind: str = PLAYLIST_KIND_REGULAR
    can_read: bool = True
    can_add: bool = False
    can_remove: bool = False
    can_reorder: bool = False
    media_types: tuple[str, ...] = ()
    extra: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.provider = _clean_str(self.provider).upper()
        self.id = _clean_str(self.id)
        self.name = _clean_str(self.name)
        self.instance = _clean_str(self.instance) or "default"
        self.kind = _norm_kind(self.kind)
        self.can_read = bool(self.can_read)
        self.can_add = bool(self.can_add)
        self.can_remove = bool(self.can_remove)
        self.can_reorder = bool(self.can_reorder)
        self.media_types = _norm_media_types(self.media_types)
        self.extra = dict(self.extra) if isinstance(self.extra, Mapping) else {}

    @property
    def is_smart(self) -> bool:
        return self.kind == PLAYLIST_KIND_SMART

    @property
    def writable(self) -> bool:
        return not self.is_smart and (self.can_add or self.can_remove)

    def to_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "instance": self.instance,
            "id": self.id,
            "name": self.name,
            "kind": self.kind,
            "smart": self.is_smart,
            "can_read": self.can_read,
            "can_add": self.can_add,
            "can_remove": self.can_remove,
            "can_reorder": self.can_reorder,
            "writable": self.writable,
            "media_types": list(self.media_types),
            "extra": dict(self.extra),
        }

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "PlaylistResource":
        return cls(
            provider=d.get("provider") or "",
            id=d.get("id") or "",
            name=d.get("name") or "",
            instance=d.get("instance") or "default",
            kind=d.get("kind") or (PLAYLIST_KIND_SMART if d.get("smart") else PLAYLIST_KIND_REGULAR),
            can_read=bool(d.get("can_read", True)),
            can_add=bool(d.get("can_add", False)),
            can_remove=bool(d.get("can_remove", False)),
            can_reorder=bool(d.get("can_reorder", False)),
            media_types=d.get("media_types") or (),
            extra=d.get("extra") or {},
        )


@dataclass
class PlaylistItem:
    key: str
    item: dict[str, Any] = field(default_factory=dict)
    playlist_item_id: str | None = None
    position: int | None = None
    provider_media_id: str | None = None

    def __post_init__(self) -> None:
        self.key = _clean_str(self.key)
        if self.playlist_item_id is not None:
            self.playlist_item_id = _clean_str(self.playlist_item_id) or None
        if self.provider_media_id is not None:
            self.provider_media_id = _clean_str(self.provider_media_id) or None
        if self.position is not None:
            try:
                self.position = int(self.position)
            except Exception:
                self.position = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "item": dict(self.item or {}),
            "playlist_item_id": self.playlist_item_id,
            "position": self.position,
            "provider_media_id": self.provider_media_id,
        }

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "PlaylistItem":
        return cls(
            key=d.get("key") or "",
            item=dict(d.get("item") or {}),
            playlist_item_id=d.get("playlist_item_id"),
            position=d.get("position"),
            provider_media_id=d.get("provider_media_id"),
        )

    @classmethod
    def from_media(
        cls,
        media: Mapping[str, Any],
        *,
        playlist_item_id: Any = None,
        position: Any = None,
        provider_media_id: Any = None,
    ) -> "PlaylistItem":
        m = minimal(media)
        key = canonical_key(media)
        return cls(
            key=key,
            item=m,
            playlist_item_id=playlist_item_id,
            position=position,
            provider_media_id=provider_media_id,
        )


@dataclass
class PlaylistSnapshot:
    resource: PlaylistResource
    items: list[PlaylistItem] = field(default_factory=list)
    checkpoint: str | None = None

    def ordered_keys(self) -> list[str]:
        return [it.key for it in self.items if it.key]

    def by_key(self) -> dict[str, PlaylistItem]:
        out: dict[str, PlaylistItem] = {}
        for it in self.items:
            if it.key and it.key not in out:
                out[it.key] = it
        return out

    def to_dict(self) -> dict[str, Any]:
        return {
            "resource": self.resource.to_dict(),
            "items": [it.to_dict() for it in self.items],
            "checkpoint": self.checkpoint,
            "count": len(self.items),
        }

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "PlaylistSnapshot":
        return cls(
            resource=PlaylistResource.from_dict(d.get("resource") or {}),
            items=[PlaylistItem.from_dict(x) for x in (d.get("items") or []) if isinstance(x, Mapping)],
            checkpoint=d.get("checkpoint"),
        )


@runtime_checkable
class PlaylistOps(Protocol):
    def list_playlist_resources(
        self, cfg: Mapping[str, Any], *, instance: str | None = None
    ) -> Sequence[PlaylistResource]: ...

    def get_playlist_snapshot(
        self, cfg: Mapping[str, Any], playlist_id: str, *, instance: str | None = None
    ) -> PlaylistSnapshot: ...

    def create_playlist(
        self,
        cfg: Mapping[str, Any],
        name: str,
        *,
        media_type: str | None = None,
        instance: str | None = None,
        dry_run: bool = False,
    ) -> PlaylistResource: ...

    def add_playlist_items(
        self,
        cfg: Mapping[str, Any],
        playlist_id: str,
        items: Sequence[Mapping[str, Any]],
        *,
        instance: str | None = None,
        dry_run: bool = False,
    ) -> dict[str, Any]: ...

    def remove_playlist_items(
        self,
        cfg: Mapping[str, Any],
        playlist_id: str,
        items: Sequence[Mapping[str, Any]],
        *,
        instance: str | None = None,
        dry_run: bool = False,
    ) -> dict[str, Any]: ...

    def reorder_playlist_items(
        self,
        cfg: Mapping[str, Any],
        playlist_id: str,
        ordered_keys: Sequence[str],
        *,
        instance: str | None = None,
        dry_run: bool = False,
    ) -> dict[str, Any]: ...


def supports_playlists(ops: Any) -> bool:
    return bool(ops) and all(callable(getattr(ops, m, None)) for m in _PLAYLIST_METHODS)


def playlist_capabilities(source: Any) -> dict[str, Any]:
    root: Any = None
    if isinstance(source, Mapping):
        root = source.get("capabilities")
    else:
        get_caps = getattr(source, "capabilities", None)
        if callable(get_caps):
            try:
                root = get_caps()
            except Exception:
                root = None
    caps = root.get("playlists") if isinstance(root, Mapping) else None
    return dict(caps) if isinstance(caps, Mapping) else {}
