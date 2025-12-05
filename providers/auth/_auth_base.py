# providers/auth/_auth_base.py
# CrossWatch - Auth Base
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from dataclasses import dataclass, field
from typing import Any, Protocol

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

class AuthProvider(Protocol):
    name: str

    def manifest(self) -> AuthManifest: ...
    def capabilities(self) -> dict[str, Any]: ...
    def get_status(self, cfg: Mapping[str, Any]) -> AuthStatus: ...
    def start(self, cfg: MutableMapping[str, Any], redirect_uri: str) -> dict[str, Any]: ...
    def finish(self, cfg: MutableMapping[str, Any], **payload: Any) -> AuthStatus: ...
    def refresh(self, cfg: MutableMapping[str, Any]) -> AuthStatus: ...
    def disconnect(self, cfg: MutableMapping[str, Any]) -> AuthStatus: ...