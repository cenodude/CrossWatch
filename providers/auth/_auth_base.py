# providers/auth/_auth_base.py
# CrossWatch - Auth Base
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import dataclasses
from collections.abc import Mapping, MutableMapping
from dataclasses import dataclass, field
from typing import Any, Protocol, cast

@dataclass
class AuthManifest:
    name: str
    label: str
    flow: str
    fields: list[dict[str, Any]] = field(default_factory=list)
    actions: dict[str, Any] = field(default_factory=dict)
    verify_url: str | None = None
    notes: str | None = None

@dataclass
class AuthStatus:
    connected: bool
    label: str
    user: str | None = None
    expires_at: int | None = None
    scopes: list[str] | None = None
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class AuthResult:
    ok: bool
    status: str = ""
    error: str | None = None
    message: str | None = None
    data: dict[str, Any] = field(default_factory=dict)
    status_code: int | None = None


def manifest_to_dict(manifest: Any) -> dict[str, Any]:
    if dataclasses.is_dataclass(manifest) and not isinstance(manifest, type):
        return dict(dataclasses.asdict(cast(Any, manifest)))
    if isinstance(manifest, Mapping):
        return dict(manifest)
    data = getattr(manifest, "__dict__", None)
    return dict(data) if isinstance(data, dict) else {"name": str(manifest)}


def auth_status_to_dict(status: Any, *, instance_id: Any = None) -> dict[str, Any]:
    out: dict[str, Any]
    if dataclasses.is_dataclass(status) and not isinstance(status, type):
        out = dict(dataclasses.asdict(cast(Any, status)))
    elif isinstance(status, Mapping):
        out = dict(status)
    else:
        data = getattr(status, "__dict__", None)
        out = dict(data) if isinstance(data, dict) else {"connected": bool(status)}
    out["connected"] = bool(out.get("connected"))
    out.setdefault("label", "")
    if instance_id is not None:
        out.setdefault("instance", str(instance_id or "default"))
    return out


def auth_result_to_dict(result: Any, *, instance_id: Any = None) -> dict[str, Any]:
    out: dict[str, Any]
    if dataclasses.is_dataclass(result) and not isinstance(result, type):
        out = dict(dataclasses.asdict(cast(Any, result)))
    elif isinstance(result, Mapping):
        out = dict(result)
    else:
        data = getattr(result, "__dict__", None)
        out = dict(data) if isinstance(data, dict) else {"ok": bool(result)}
    out["ok"] = bool(out.get("ok"))
    if instance_id is not None:
        out.setdefault("instance", str(instance_id or "default"))
    return out


class AuthProvider(Protocol):
    name: str

    def manifest(self) -> AuthManifest | Mapping[str, Any]: ...
    def capabilities(self) -> dict[str, Any]: ...
    def get_status(self, cfg: Mapping[str, Any], *, instance_id: Any = None) -> AuthStatus | Mapping[str, Any]: ...
    def start(
        self,
        cfg: MutableMapping[str, Any],
        redirect_uri: str | None = None,
        *,
        instance_id: Any = None,
    ) -> dict[str, Any] | AuthResult: ...
    def finish(
        self,
        cfg: MutableMapping[str, Any],
        *,
        instance_id: Any = None,
        **payload: Any,
    ) -> AuthStatus | Mapping[str, Any]: ...
    def refresh(
        self,
        cfg: MutableMapping[str, Any],
        *,
        instance_id: Any = None,
    ) -> AuthStatus | Mapping[str, Any]: ...
    def disconnect(
        self,
        cfg: MutableMapping[str, Any],
        *,
        instance_id: Any = None,
    ) -> AuthStatus | Mapping[str, Any]: ...
