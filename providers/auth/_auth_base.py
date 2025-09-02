from _logging import log
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Mapping, MutableMapping, Optional, Protocol

# UI schema for a provider
@dataclass
class AuthManifest:
    name: str                  # "PLEX"
    label: str                 # "Plex"
    flow: str                  # "device_pin" | "oauth" | "api_keys" | "token"
    fields: list[dict] = field(default_factory=list)   # editable fields in Settings
    actions: dict = field(default_factory=dict)        # {"start":True,"finish":True,"refresh":True,"disconnect":True}
    verify_url: Optional[str] = None                   # device_pin
    notes: Optional[str] = None                        # short UI hint

# Status for the UI
@dataclass
class AuthStatus:
    connected: bool
    label: str
    user: Optional[str] = None
    expires_at: Optional[int] = None
    scopes: Optional[list[str]] = None
    extra: dict[str, Any] = field(default_factory=dict)

# Provider interface
class AuthProvider(Protocol):
    name: str

    def manifest(self) -> AuthManifest: ...

    # optional: declare supported sync features
    def capabilities(self) -> dict: ...

    # read-only
    def get_status(self, cfg: Mapping[str, Any]) -> AuthStatus: ...
    # may mutate cfg
    def start(self, cfg: MutableMapping[str, Any], redirect_uri: str) -> dict[str, Any]: ...
    def finish(self, cfg: MutableMapping[str, Any], **payload) -> AuthStatus: ...
    def refresh(self, cfg: MutableMapping[str, Any]) -> AuthStatus: ...
    def disconnect(self, cfg: MutableMapping[str, Any]) -> AuthStatus: ...
